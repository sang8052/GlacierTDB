package s3

import (
	"archive/tar"
	"archive/zip"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// 打包下载（v1.3.13 §1.5）：GET /{bucket}?x-glacier-archive=<prefix>&format=…&mode=…
// （自定义参数不参与签名；对象锁 / WORM 对象照常包含——读侧无影响；限速计入
// per_conn_bps。）
//
// 两种模式：
//   - stream 流式下载：边打包边发送，仅允许 tar（不压缩，天然流式，无需预知总大小）；
//   - done  完成后下载：先落临时包（.s3tmp/archive-*）再整包发送，
//     支持 tar / tar.gz / zip（tar.gz 与 zip 压缩等级 1-9，默认 5 = s3.gzip_level）；
//     打包前统计总大小，超过 s3.archive_sync_max_mb（默认 512MB）直接拒绝该模式。
//
// 临时包目录弹性驱逐（空间吃紧时）：先驱逐已完整下载过的旧包（.done 标记），
// 再按最旧驱逐；目录容量 s3.archive_tmp_max_mb（默认 4096MB）。
// ---------------------------------------------------------------------------

// listAllForArchive 拉取前缀下全部对象条目（分页循环；size 降序无关，按 key 升序）。
func (s *Store) listAllForArchive(bucket, prefix string) ([]ListEntry, int64, error) {
	var out []ListEntry
	var total int64
	marker := ""
	for {
		res, err := s.ListObjects(bucket, prefix, "", marker, "", 1000)
		if err != nil {
			return nil, 0, err
		}
		for _, e := range res.Entries {
			if e.Size < 0 {
				continue
			}
			out = append(out, e)
			total += e.Size
		}
		if !res.IsTruncated || len(res.Entries) == 0 {
			return out, total, nil
		}
		marker = res.Entries[len(res.Entries)-1].Key
	}
}

// archiveEntry 打包任务条目。
type archiveEntry struct {
	Key  string
	Size int64
}

// archiveDownload 打包下载入口（handleBucket GET 路由）。
func (h *endpointHandler) archiveDownload(w http.ResponseWriter, r *http.Request, bucket, prefix, owner, reqID string,
	lim config.S3Limit, userTB *tokenBucket) {
	q := r.URL.Query()
	format := strings.ToLower(strings.TrimSpace(q.Get("format")))
	mode := strings.ToLower(strings.TrimSpace(q.Get("mode")))
	if format == "" {
		format = "tar"
	}
	switch format {
	case "tar", "tar.gz", "tgz", "zip":
	default:
		writeErr(w, r, reqID, &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: "format must be one of: tar, tar.gz, zip"})
		return
	}
	if format == "tgz" {
		format = "tar.gz"
	}
	if mode == "" {
		if format == "tar" {
			mode = "stream" // tar 天然流式：默认边打包边发
		} else {
			mode = "done" // tar.gz / zip 需要压缩器状态，走完成后下载
		}
	}
	switch mode {
	case "stream", "done":
	default:
		writeErr(w, r, reqID, &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: "mode must be stream or done"})
		return
	}
	if mode == "stream" && format != "tar" {
		writeErr(w, r, reqID, &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: "streaming download only supports format=tar (use mode=done for tar.gz / zip)"})
		return
	}

	entries, total, err := h.srv.store.listAllForArchive(bucket, prefix)
	if err != nil {
		writeErr(w, r, reqID, toS3Err(err))
		return
	}
	arc := []archiveEntry{}
	for _, e := range entries {
		arc = append(arc, archiveEntry{Key: e.Key, Size: e.Size})
	}

	switch mode {
	case "stream":
		h.archiveStream(w, r, bucket, arc, lim, userTB, reqID)
	case "done":
		maxBytes := int64(h.srv.cfg.S3.ArchiveSyncMaxMB) << 20
		if maxBytes > 0 && total > maxBytes {
			writeErr(w, r, reqID, &s3Error{Code: "ArchiveTooLarge", Status: 400,
				Msg: fmt.Sprintf("selected objects total %d bytes exceeds archive_sync_max_mb %d — use mode=stream (tar) or narrow the prefix", total, h.srv.cfg.S3.ArchiveSyncMaxMB)})
			return
		}
		h.archiveDone(w, r, bucket, arc, format, lim, userTB, reqID)
	}
}

// archiveStream 流式 tar：边读边打包边发（不压缩；Content-Length 不可知走 chunked）。
func (h *endpointHandler) archiveStream(w http.ResponseWriter, r *http.Request, bucket string,
	arc []archiveEntry, lim config.S3Limit, userTB *tokenBucket, reqID string) {
	w.Header().Set("Content-Type", "application/x-tar")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s-archive-%s.tar\"", bucket, time.Now().Format("20060102-150405")))
	w.WriteHeader(http.StatusOK)
	_, cw := newLimitedWriter(w, lim.PerConnBps, userTB)
	tw := tar.NewWriter(cw)
	defer tw.Close()
	for _, e := range arc {
		m, rc, err := h.srv.store.GetObject(bucket, e.Key, "")
		if err != nil {
			logx.Warnf("s3 archive: skip %s/%s: %v", bucket, e.Key, err) // 打包期间被删：跳过该对象
			continue
		}
		hdr := &tar.Header{
			Name:    e.Key,
			Mode:    0o644,
			Size:    m.Size,
			ModTime: time.Now(),
			Typeflag: tar.TypeReg,
		}
		if t, perr := time.ParseInLocation("2006-01-02 15:04:05", m.CreatedTS, time.Local); perr == nil {
			hdr.ModTime = t
		}
		if err := tw.WriteHeader(hdr); err != nil {
			rc.Close()
			return
		}
		_, _ = io.Copy(tw, rc)
		rc.Close()
	}
}

// archiveDone 完成后下载：先落临时包再整包发送（支持压缩）。
func (h *endpointHandler) archiveDone(w http.ResponseWriter, r *http.Request, bucket string,
	arc []archiveEntry, format string, lim config.S3Limit, userTB *tokenBucket, reqID string) {
	tmpDir := filepath.Join(h.srv.cfg.DBDir, ".s3tmp")
	_ = os.MkdirAll(tmpDir, 0o700)
	h.srv.archiveEvict(tmpDir, int64(h.srv.cfg.S3.ArchiveTmpMaxMB)<<20)

	id := genRequestUUID()
	name := fmt.Sprintf("archive-%s-%s.%s", bucket, time.Now().Format("20060102-150405"), strings.ReplaceAll(format, "/", ""))
	pkgPath := filepath.Join(tmpDir, "pkg-"+id+"."+strings.ReplaceAll(format, "/", ""))

	if err := h.buildArchive(pkgPath, bucket, arc, format); err != nil {
		_ = os.Remove(pkgPath)
		writeErr(w, r, reqID, &s3Error{Code: "InternalError", Status: 500, Msg: "archive build failed: " + err.Error()})
		return
	}
	fi, err := os.Stat(pkgPath)
	if err != nil {
		writeErr(w, r, reqID, &s3Error{Code: "InternalError", Status: 500, Msg: "archive stat failed"})
		return
	}
	w.Header().Set("Content-Type", map[string]string{
		"tar": "application/x-tar", "tar.gz": "application/gzip", "zip": "application/zip"}[format])
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", name))
	w.Header().Set("Content-Length", strconv.FormatInt(fi.Size(), 10))
	w.WriteHeader(http.StatusOK)
	_, cw := newLimitedWriter(w, lim.PerConnBps, userTB)
	f, err := os.Open(pkgPath)
	if err != nil {
		return
	}
	_, copyErr := io.Copy(cw, f)
	f.Close()
	if copyErr == nil {
		// 完整下载过 → 标记 .done（驱逐时最先淘汰）；限速层完整消费视同完成
		_ = os.Rename(pkgPath, filepath.Join(tmpDir, "done-"+id+"."+strings.ReplaceAll(format, "/", "")))
	} else {
		_ = os.Remove(pkgPath) // 中断的包直接清理（无断点续传语义）
	}
}

// buildArchive 落包：tar 直写；tar.gz gzip(level)；zip Deflate(level)。
func (h *endpointHandler) buildArchive(pkgPath, bucket string, arc []archiveEntry, format string) error {
	f, err := os.Create(pkgPath)
	if err != nil {
		return err
	}
	defer f.Close()
	level := parseGzipLevel(h.srv.cfg.S3.GzipLevel)

	switch format {
	case "tar":
		tw := tar.NewWriter(f)
		if err := h.fillTar(tw, bucket, arc); err != nil {
			return err
		}
		return tw.Close()
	case "tar.gz":
		zw, zerr := gzip.NewWriterLevel(f, level)
		if zerr != nil {
			zw = gzip.NewWriter(f)
		}
		tw := tar.NewWriter(zw)
		if err := h.fillTar(tw, bucket, arc); err != nil {
			return err
		}
		if err := tw.Close(); err != nil {
			return err
		}
		return zw.Close()
	case "zip":
		zw := zip.NewWriter(f)
		zw.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(out, level)
		})
		for _, e := range arc {
			m, rc, gerr := h.srv.store.GetObject(bucket, e.Key, "")
			if gerr != nil {
				continue // 打包期间被删：跳过
			}
			mod := time.Now()
			if t, perr := time.ParseInLocation("2006-01-02 15:04:05", m.CreatedTS, time.Local); perr == nil {
				mod = t
			}
			hdr := &zip.FileHeader{Name: e.Key, Method: zip.Deflate, Modified: mod}
			hdr.SetMode(0o644)
			fw, zerr := zw.CreateHeader(hdr)
			if zerr != nil {
				rc.Close()
				return zerr
			}
			_, _ = io.Copy(fw, rc)
			rc.Close()
		}
		return zw.Close()
	}
	return fmt.Errorf("unsupported format %q", format)
}

// fillTar 写 tar 条目。
func (h *endpointHandler) fillTar(tw *tar.Writer, bucket string, arc []archiveEntry) error {
	for _, e := range arc {
		m, rc, err := h.srv.store.GetObject(bucket, e.Key, "")
		if err != nil {
			continue
		}
		mod := time.Now()
		if t, perr := time.ParseInLocation("2006-01-02 15:04:05", m.CreatedTS, time.Local); perr == nil {
			mod = t
		}
		if err := tw.WriteHeader(&tar.Header{Name: e.Key, Mode: 0o644, Size: m.Size, ModTime: mod, Typeflag: tar.TypeReg}); err != nil {
			rc.Close()
			return err
		}
		_, _ = io.Copy(tw, rc)
		rc.Close()
	}
	return nil
}

// archiveEvict 临时包目录弹性驱逐：usage 超限时先驱逐已完整下载过的旧包
//（done-* 前缀），再按最旧驱逐构建中 / 未下完的包（pkg-*）。超过 24h 的
// 孤儿包无条件清理。
func (s *Server) archiveEvict(tmpDir string, maxBytes int64) {
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		return
	}
	type pkg struct {
		name string
		size int64
		mod  time.Time
		done bool
	}
	var pkgs []pkg
	var usage int64
	for _, e := range entries {
		if e.IsDir() || (!strings.HasPrefix(e.Name(), "pkg-") && !strings.HasPrefix(e.Name(), "done-")) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		usage += fi.Size()
		pkgs = append(pkgs, pkg{name: e.Name(), size: fi.Size(), mod: fi.ModTime(),
			done: strings.HasPrefix(e.Name(), "done-")})
	}
	// 24h 孤儿包无条件清理
	for _, p := range pkgs {
		if time.Since(p.mod) > 24*time.Hour {
			_ = os.Remove(filepath.Join(tmpDir, p.name))
			usage -= p.size
		}
	}
	if maxBytes <= 0 || usage <= maxBytes {
		return
	}
	// 弹性驱逐：先 done（完整下载过的旧包），后最旧
	sort.Slice(pkgs, func(i, j int) bool {
		if pkgs[i].done != pkgs[j].done {
			return pkgs[i].done // done 排前
		}
		return pkgs[i].mod.Before(pkgs[j].mod)
	})
	for _, p := range pkgs {
		if usage <= maxBytes {
			return
		}
		_ = os.Remove(filepath.Join(tmpDir, p.name))
		usage -= p.size
	}
}
