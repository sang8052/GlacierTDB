package s3

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ObjectMeta 对象版本元数据。
type ObjectMeta struct {
	Key               string
	VersionID         string
	IsLatest          bool
	DeleteMarker      bool
	Size              int64
	ETag              string
	ContentType       string
	ContentEncoding   string
	CacheControl      string
	ContentDisposition string
	Metadata          map[string]string
	Storage           string
	Manifest          string // 分块布局清单（JSON；空 = 旧版稠密 seq 布局，v1.3.11）
	LegalHold         bool
	RetentionMode     string // '' | GOVERNANCE | COMPLIANCE
	RetainUntil       string
	CreatedTS         string
}

// chunkManifest 分块布局清单（v1.3.11）：Range 读据此做 offset → (seq, 块内偏移)
// 的直接定位，无需线性扫描。Parts 每项 = [startSeq, nChunks, sizeBytes, chunkSize]——
// 分片上传合并零拷贝后各 part 对应一段；单 PUT 稠密布局为一段。
type chunkManifest struct {
	CS   int64     `json:"cs"`
	Parts [][4]int64 `json:"parts"`
}

// parseChunkManifest 解析清单（空串 / 非法 → nil，走旧版稠密布局）。
func parseChunkManifest(s string) []chunkSpan {
	if s == "" {
		return nil
	}
	var m chunkManifest
	if json.Unmarshal([]byte(s), &m) != nil || len(m.Parts) == 0 {
		return nil
	}
	out := make([]chunkSpan, 0, len(m.Parts))
	for _, p := range m.Parts {
		if p[1] <= 0 || p[3] <= 0 {
			return nil
		}
		out = append(out, chunkSpan{startSeq: p[0], chunks: p[1], size: p[2], cs: p[3]})
	}
	return out
}

// ObjectAttrs PUT 时的写入属性。
type ObjectAttrs struct {
	ContentType        string
	ContentEncoding    string
	CacheControl       string
	ContentDisposition string
	Metadata           map[string]string
}

// genVersionID 生成版本 ID（32 位十六进制）。
func genVersionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// lockViolation 对象锁（WORM / legal hold）违规判断。
func (m *ObjectMeta) lockViolation(bypassGovernance bool) error {
	if m.LegalHold {
		return &s3Error{Code: "AccessDenied", Status: 403,
			Msg: "Object is under legal hold and cannot be deleted or overwritten"}
	}
	if m.RetainUntil != "" {
		t, err := time.ParseInLocation("2006-01-02 15:04:05", m.RetainUntil, time.Local)
		if err == nil && time.Now().Before(t) {
			if m.RetentionMode == "COMPLIANCE" || !bypassGovernance {
				return &s3Error{Code: "AccessDenied", Status: 403,
					Msg: fmt.Sprintf("Object is retained until %s (mode %s)", m.RetainUntil, m.RetentionMode)}
			}
		}
	}
	return nil
}

// spool 上传内容暂存（v1.3.12）：小对象全部驻留内存（省去临时文件一写一读
// 两次磁盘 I/O），超过阈值溢出 <db_dir>/.s3tmp 临时文件（内存占用与对象大小
// 解耦）。全程同步计算 MD5 + SHA-256（SHA-256 供 chunk 池寻址与闪传索引，v1.3.13）。
type spool struct {
	buf    bytes.Buffer
	tmp    *os.File
	tmpDir string
	memMax int
	sz     int64
	md5h   hash.Hash
	sha256h hash.Hash
}

// spoolMemMax 内存暂存阈值（字节）：内联阈值与 1MB 取大——小对象（延迟敏感、
// 吞吐主力）全内存省两次磁盘 I/O；更大对象溢出临时文件流式处理，避免并行
// 大对象上传时数十 MB 级堆驻留推高 GC、拖慢单写临界区（实测 5MB×16 路并行
// 内存驻留使写路径降速 ~18%，v1.3.12 调优）。
func (s *Store) spoolMemMax() int {
	m := int(s.inlineMax)
	if m < 1<<20 {
		m = 1 << 20
	}
	return m
}

func newSpool(tmpDir string, memMax int) *spool {
	return &spool{tmpDir: tmpDir, memMax: memMax, md5h: md5.New(), sha256h: sha256.New()}
}

// copyFrom 流式暂存并计算 MD5，返回总字节数。
func (p *spool) copyFrom(r io.Reader) (int64, error) {
	_ = os.MkdirAll(p.tmpDir, 0o700)
	chunk := make([]byte, 256<<10)
	for {
		n, rerr := r.Read(chunk)
		if n > 0 {
			if p.tmp == nil && p.buf.Len()+n > p.memMax {
				t, err := os.CreateTemp(p.tmpDir, "put-*")
				if err != nil {
					return p.sz, err
				}
				if _, err := t.Write(p.buf.Bytes()); err != nil {
					t.Close()
					os.Remove(t.Name())
					return p.sz, err
				}
				p.buf.Reset()
				p.tmp = t
			}
			if p.tmp != nil {
				if _, err := p.tmp.Write(chunk[:n]); err != nil {
					return p.sz, err
				}
			} else {
				p.buf.Write(chunk[:n])
			}
			_, _ = p.md5h.Write(chunk[:n])
			_, _ = p.sha256h.Write(chunk[:n])
			p.sz += int64(n)
		}
		if rerr == io.EOF {
			return p.sz, nil
		}
		if rerr != nil {
			return p.sz, rerr
		}
	}
}

// md5Hex 暂存内容的 MD5（hex）。
func (p *spool) md5Hex() string { return hex.EncodeToString(p.md5h.Sum(nil)) }

// sha256Hex 暂存内容的 SHA-256（hex；chunk 池寻址 + 闪传索引，v1.3.13）。
func (p *spool) sha256Hex() string { return hex.EncodeToString(p.sha256h.Sum(nil)) }

// size 暂存字节数。
func (p *spool) size() int64 { return p.sz }

// bytes 取全部暂存字节（小对象内存态零拷贝；已溢出文件时回读）。
func (p *spool) bytes() ([]byte, error) {
	if p.tmp == nil {
		return p.buf.Bytes(), nil
	}
	if _, err := p.tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return io.ReadAll(p.tmp)
}

// reader 回读句柄（内存态或临时文件，Seek(0) 已就位）。
func (p *spool) reader() (io.Reader, error) {
	if p.tmp == nil {
		return bytes.NewReader(p.buf.Bytes()), nil
	}
	if _, err := p.tmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return p.tmp, nil
}

// close 释放暂存资源（溢出临时文件删除）。
func (p *spool) close() {
	if p.tmp != nil {
		name := p.tmp.Name()
		p.tmp.Close()
		_ = os.Remove(name)
		p.tmp = nil
	}
}

// PutObject 写对象：内容暂存（小对象内存/大对象临时文件）做 MD5 校验后，
// 删旧 + 插新 + 桶聚合 UPSERT 在**单个事务**内一次提交（v1.3.12 重写：
// 此前锁内 2 条隐式 DELETE 事务 + 主事务 + 提交后 2 条聚合/记账 UPSERT，
// 每个对象 6 次左右 commit 且全程持有全局写锁——并行上传在锁上排队；
// 现临界区 = 1 个短事务，记账走内存缓冲）。
// 返回新版本元数据与是否覆盖已有对象（versioning 关闭时）。
func (s *Store) PutObject(bucket, key string, r io.Reader, sizeHint int64, attrs ObjectAttrs) (ObjectMeta, bool, error) {
	var out ObjectMeta
	replaced := false
	if key == "" || len(key) > 1024 {
		return out, false, &s3Error{Code: "InvalidArgument", Status: 400, Msg: "Invalid object key"}
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return out, false, err
	}
	// WORM / 版本链保护：版本关闭时覆盖 = 删除旧行，须过对象锁
	if !b.Versioning {
		if cur, err := s.HeadObject(bucket, key, ""); err == nil {
			if err := cur.lockViolation(false); err != nil {
				return out, false, err
			}
		}
	}

	// 暂存（锁外完成：MD5 / 上限校验 / 内联字节备好——不占全局写锁）
	sp := newSpool(filepath.Join(s.dir, ".s3tmp"), s.spoolMemMax())
	size, err := sp.copyFrom(r)
	if err != nil {
		sp.close()
		return out, false, err
	}
	defer sp.close()
	if s.maxObject > 0 && size > s.maxObject {
		return out, false, &s3Error{Code: "EntityTooLarge", Status: 400,
			Msg: fmt.Sprintf("Object size %d exceeds max_object_mb limit %d", size, s.maxObject>>20)}
	}
	if sizeHint >= 0 && sizeHint != size {
		// Content-Length 与实际不符（协议层面 Go 已保证一致；防御）
		return out, false, &s3Error{Code: "IncompleteBody", Status: 400,
			Msg: "You did not provide the number of bytes specified by the Content-Length HTTP header"}
	}
	etag := sp.md5Hex()

	storage := "inline"
	var content []byte
	if size <= s.inlineMax {
		content, err = sp.bytes()
		if err != nil {
			return out, false, err
		}
	} else {
		storage = "chunk"
	}
	metaJSON := "{}"
	if len(attrs.Metadata) > 0 {
		metaJSON = MarshalPolicy(attrs.Metadata)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return out, false, err
	}
	defer tx.Rollback()

	oldStorage, oldCount := s.keyAggLocked(tx, b, key)
	versionID := ""
	if b.Versioning {
		versionID = genVersionID()
		if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=0 WHERE key=? AND is_latest=1`, b.TableName), key); err != nil {
			return out, false, err
		}
	} else {
		// 清理旧版本（含分块引用；只清正式行 version_id=''——不碰进行中分片上传的
		// 暂存行 (key, uploadId)，v1.3.11）
		if err := s.deleteObjectRowsTx(tx, b, key, ""); err != nil {
			return out, false, err
		}
		replaced = oldCount > 0
	}

	// 分块内容入池（v1.3.13：内容寻址 + 引用计数；重复内容跨对象/版本共享）
	manifest := ""
	var chunkHashes []string
	var chunkSizes []int64
	if storage == "chunk" {
		rc, rerr := sp.reader()
		if rerr != nil {
			return out, false, rerr
		}
		chunkHashes, _, err = s.writeChunkRefsFromReader(tx, chunkTableFor(b.TableName), key, versionID, s.scopeFor(b), 0, rc)
		if err != nil {
			return out, false, err
		}
		chunkSizes = chunkSizeSeq(size, s.chunkSize, len(chunkHashes))
		manifest = MarshalPolicy(chunkManifestV2{V: 2, CS: s.chunkSize, HS: chunkHashes, SS: chunkSizes})
	}

	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,is_latest,delete_marker,size,etag,
		content_type,content_encoding,cache_control,content_disposition,metadata,content,storage,manifest,
		legal_hold,retention_mode,retain_until,created_ts)
		VALUES(?,?,1,0,?,?,?,?,?,?,?,?,?,?,0,'','',?)`, b.TableName),
		key, versionID, size, etag,
		attrs.ContentType, attrs.ContentEncoding, attrs.CacheControl, attrs.ContentDisposition,
		metaJSON, content, storage, manifest, nowTS())
	if err != nil {
		return out, false, err
	}

	// 桶聚合按算术增量随本事务提交（省去提交后的第二次聚合 SELECT）：
	// versioning = (+size, +1)；非 versioning 覆盖后 = (size-旧, 1-旧)
	if b.Versioning {
		s.adjustAggLocked(tx, b, key, size, 1)
	} else {
		s.adjustAggLocked(tx, b, key, size-oldStorage, 1-oldCount)
	}
	if err := tx.Commit(); err != nil {
		return out, false, err
	}

	// 闪传索引登记（v1.3.13）：仅分块对象（≤内联阈值小对象无共享收益，不登记；
	// multipart 合并对象不在写路径流式，亦不登记——见 docs/S3对象存储.md）
	if storage == "chunk" {
		s.registerFlashIndex(s.scopeFor(b), sp.sha256Hex(), size, etag, chunkHashes)
	}

	s.AddUsage(b.Owner, bucket, "PUT", size, 0)

	out = ObjectMeta{
		Key: key, VersionID: versionID, IsLatest: true, Size: size, ETag: etag,
		ContentType: attrs.ContentType, ContentEncoding: attrs.ContentEncoding,
		CacheControl: attrs.CacheControl, ContentDisposition: attrs.ContentDisposition,
		Metadata: attrs.Metadata, Storage: storage, CreatedTS: nowTS(),
	}
	return out, replaced, nil
}

// deleteObjectRowsTx 删除对象行 + 分块引用行，并释放池引用（单事务内；
// versionID 空 = 该 key 的全部正式行 version_id=''）。调用方持 writeMu。
func (s *Store) deleteObjectRowsTx(tx *sql.Tx, b Bucket, key, versionID string) error {
	tbl := chunkTableFor(b.TableName)
	hashes, err := s.collectChunkHashes(tbl, key, versionID)
	if err != nil {
		return err
	}
	if versionID != "" {
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=?`, tbl), key, versionID); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=?`, b.TableName), key, versionID); err != nil {
			return err
		}
	} else {
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=''`, tbl), key); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=''`, b.TableName), key); err != nil {
			return err
		}
	}
	return releaseChunkRefs(tx, hashes)
}

// scanObject 扫描一行对象元数据（不含 content 列，内容单独取）。
func scanObject(rows *sql.Rows) (ObjectMeta, error) {
	var m ObjectMeta
	var isLatest, delMark, legalHold int
	var metaJSON, storage string
	err := rows.Scan(&m.Key, &m.VersionID, &isLatest, &delMark, &m.Size, &m.ETag,
		&m.ContentType, &m.ContentEncoding, &m.CacheControl, &m.ContentDisposition,
		&metaJSON, &storage, &m.Manifest, &legalHold, &m.RetentionMode, &m.RetainUntil, &m.CreatedTS)
	if err != nil {
		return m, err
	}
	m.IsLatest = isLatest != 0
	m.DeleteMarker = delMark != 0
	m.LegalHold = legalHold != 0
	m.Storage = storage
	m.Metadata = map[string]string{}
	if metaJSON != "" && metaJSON != "{}" {
		_ = json.Unmarshal([]byte(metaJSON), &m.Metadata)
	}
	return m, nil
}

const objectCols = `key,version_id,is_latest,delete_marker,size,etag,content_type,content_encoding,
	cache_control,content_disposition,metadata,storage,manifest,legal_hold,retention_mode,retain_until,created_ts`

// HeadObject 查对象元数据（versionID 空 = 最新版本；不存在返回 NoSuchKey）。
func (s *Store) HeadObject(bucket, key, versionID string) (ObjectMeta, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return ObjectMeta{}, err
	}
	var rows *sql.Rows
	if versionID != "" {
		rows, err = s.db.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE key=? AND version_id=?`, objectCols, b.TableName), key, versionID)
	} else {
		rows, err = s.db.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE key=? AND is_latest=1`, objectCols, b.TableName), key)
	}
	if err != nil {
		return ObjectMeta{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return ObjectMeta{}, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	m, err := scanObject(rows)
	if err != nil {
		return ObjectMeta{}, err
	}
	if m.DeleteMarker && versionID == "" {
		return ObjectMeta{}, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	return m, nil
}

// chunkSpan manifest 中一段连续分块（一个 part 或单 PUT 稠密整体）。
type chunkSpan struct {
	startSeq int64
	chunks   int64
	size     int64
	cs       int64
}

// contentReader 对象内容读取器（内联字节 / 分块惰性拉取；支持 manifest 直定位与
// Range 裁剪：skip = 起点前跳过字节，remain = 剩余输出字节，-1 = 不限）。
// 三种布局（v1.3.13）：hashes != nil = v2 清单（池键序列，逐 seq 查池）；
// spans != nil = v1.3.11 清单（seq 区间直定位）；两者皆空 = 旧版稠密 seq 布局。
// 存量行 data 非空直用（迁移前），空则回退 hash 列查池（迁移后）。
type contentReader struct {
	store *Store
	tbl   string
	key   string
	ver   string
	spans []chunkSpan // manifest 布局；nil = 旧版稠密 seq 布局
	spanIdx   int
	idxInSpan int64
	legacySeq int64
	hashes []string // v2 池键序列（v1.3.13）
	hsizes []int64  // v2 各块实际字节数（与 hashes 等长；multipart 末块不满块，seek 按累计定位）
	hidx   int
	hcs    int64
	buf   []byte
	off   int
	done  bool
	inline bool
	skip  int64
	remain int64
}

// fetchChunkRef 取一个引用行的数据：data 列非空直用（存量未迁移行），否则按
// hash 列查池（v1.3.13 迁移后形态）。
func (c *contentReader) fetchChunkRef(key, ver string, seq int64) ([]byte, error) {
	var data []byte
	var h string
	err := c.store.db.QueryRow(fmt.Sprintf(
		`SELECT data, hash FROM %s WHERE key=? AND version_id=? AND seq=?`, c.tbl),
		key, ver, seq).Scan(&data, &h)
	if err == sql.ErrNoRows {
		return nil, io.EOF
	}
	if err != nil {
		return nil, err
	}
	if len(data) > 0 {
		return data, nil
	}
	if h == "" {
		return nil, io.EOF
	}
	if err := c.store.db.QueryRow(`SELECT data FROM s3_chunks WHERE hash=?`, h).Scan(&data); err != nil {
		return nil, err
	}
	return data, nil
}

// fetchNext 拉取下一分块到 buf（v2 池键序列 / manifest seq 直取 / 旧布局 seq 升序）。
func (c *contentReader) fetchNext() error {
	if c.hashes != nil {
		for {
			if c.hidx >= len(c.hashes) {
				return io.EOF
			}
			var data []byte
			err := c.store.db.QueryRow(`SELECT data FROM s3_chunks WHERE hash=?`, c.hashes[c.hidx]).Scan(&data)
			if err == sql.ErrNoRows {
				return io.EOF
			}
			if err != nil {
				return err
			}
			c.hidx++
			if len(data) == 0 { // 池行空数据（理论不该出现）：跳过
				continue
			}
			c.buf = data
			c.off = 0
			return nil
		}
	}
	if c.spans != nil {
		for {
			if c.spanIdx >= len(c.spans) {
				return io.EOF
			}
			sp := c.spans[c.spanIdx]
			if c.idxInSpan >= sp.chunks {
				c.spanIdx++
				c.idxInSpan = 0
				continue
			}
			data, err := c.fetchChunkRef(c.key, c.ver, sp.startSeq+c.idxInSpan)
			if err != nil {
				return err
			}
			c.buf = data
			c.off = 0
			c.idxInSpan++
			return nil
		}
	}
	var seq int64
	var data []byte
	err := c.store.db.QueryRow(fmt.Sprintf(
		`SELECT seq,data FROM %s WHERE key=? AND version_id=? AND seq>? ORDER BY seq LIMIT 1`,
		c.tbl), c.key, c.ver, c.legacySeq).Scan(&seq, &data)
	if err == sql.ErrNoRows {
		return io.EOF
	}
	if err != nil {
		return err
	}
	c.legacySeq = seq
	if len(data) == 0 { // 存量布局但 data 已迁空：按 hash 列回退
		data, err = c.fetchChunkRef(c.key, c.ver, seq)
		if err != nil {
			return err
		}
	}
	c.buf = data
	c.off = 0
	return nil
}

// seekTo Range 定位：把读取位置推进到对象第 start 字节（v2 清单按块大小整除
// 直算；manifest 布局直接算出所在段与块；旧稠密布局退化为逐块跳过）。
// 调用前提：尚未读出任何字节。
func (c *contentReader) seekTo(start int64) {
	if start <= 0 {
		return
	}
	if c.hashes != nil {
		// 按各块实际字节数累计定位（multipart 各 part 末块不满块，不能均匀除法）
		var cum int64
		for i, sz := range c.hsizes {
			if start < cum+sz {
				c.hidx = i
				c.skip = start - cum
				return
			}
			cum += sz
		}
		c.hidx = len(c.hashes) // 越界：直接置完
		return
	}
	if c.spans != nil {
		for i := range c.spans {
			sp := c.spans[i]
			if start >= sp.size {
				start -= sp.size
				continue
			}
			c.spanIdx = i
			c.idxInSpan = start / sp.cs
			c.skip = start % sp.cs
			return
		}
		c.spanIdx = len(c.spans) // 越界：直接置完
		return
	}
	c.skip = start // 旧布局：fetchNext 流程内逐块消化 skip
}

func (c *contentReader) Read(p []byte) (int, error) {
	if c.inline {
		if c.off >= len(c.buf) {
			return 0, io.EOF
		}
		n := copy(p, c.buf[c.off:])
		if c.remain >= 0 && int64(n) > c.remain {
			n = int(c.remain)
		}
		c.off += n
		if c.remain >= 0 {
			c.remain -= int64(n)
		}
		return n, nil
	}
	for {
		if c.remain == 0 {
			return 0, io.EOF
		}
		if len(c.buf)-c.off == 0 {
			if c.done {
				return 0, io.EOF
			}
			if err := c.fetchNext(); err != nil {
				c.done = true
				if err == io.EOF {
					return 0, io.EOF
				}
				return 0, err
			}
		}
		if c.skip > 0 {
			n := int64(len(c.buf) - c.off)
			if n > c.skip {
				n = c.skip
			}
			c.off += int(n)
			c.skip -= n
			continue
		}
		n := copy(p, c.buf[c.off:])
		if c.remain >= 0 && int64(n) > c.remain {
			n = int(c.remain)
		}
		c.off += n
		if c.remain >= 0 {
			c.remain -= int64(n)
		}
		return n, nil
	}
}

func (c *contentReader) Close() error { return nil }

// GetObject 取对象内容（返回元数据 + 流式读取器）。
func (s *Store) GetObject(bucket, key, versionID string) (ObjectMeta, io.ReadCloser, error) {
	return s.getObjectRange(bucket, key, versionID, -1, -1)
}

// GetObjectRange 取对象 [start, start+length) 字节区间（v1.3.11 Range 读优化：
// manifest 直定位到分块，不整读）。start<0 / length<0 = 全量。
func (s *Store) GetObjectRange(bucket, key, versionID string, start, length int64) (ObjectMeta, io.ReadCloser, error) {
	if start < 0 {
		start = 0
	}
	return s.getObjectRange(bucket, key, versionID, start, length)
}

func (s *Store) getObjectRange(bucket, key, versionID string, start, length int64) (ObjectMeta, io.ReadCloser, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return ObjectMeta{}, nil, err
	}
	var rows *sql.Rows
	if versionID != "" {
		rows, err = s.db.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE key=? AND version_id=?`, objectCols, b.TableName), key, versionID)
	} else {
		rows, err = s.db.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE key=? AND is_latest=1`, objectCols, b.TableName), key)
	}
	if err != nil {
		return ObjectMeta{}, nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return ObjectMeta{}, nil, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	m, err := scanObject(rows)
	if err != nil {
		return ObjectMeta{}, nil, err
	}
	if m.DeleteMarker {
		if versionID != "" {
			return m, nil, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist (delete marker)"}
		}
		return ObjectMeta{}, nil, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	if start > m.Size {
		return m, nil, &s3Error{Code: "InvalidRange", Status: 416,
			Msg: fmt.Sprintf("Range start %d is beyond object size %d", start, m.Size)}
	}
	if length < 0 || start+length > m.Size {
		length = m.Size - start
	}
	// 内容
	if m.Storage == "inline" {
		var content []byte
		if err := s.db.QueryRow(fmt.Sprintf(`SELECT content FROM %s WHERE key=? AND version_id=?`, b.TableName),
			key, m.VersionID).Scan(&content); err != nil {
			return ObjectMeta{}, nil, err
		}
		if start > 0 || start+length < int64(len(content)) {
			content = content[start : start+length]
		}
		// remain=-1：inline 缓冲即最终字节（已按需切片），不限长输出
		return m, &contentReader{inline: true, buf: content, remain: -1}, nil
	}
	if hs, ss, hcs := parseChunkManifestV2(m.Manifest); hs != nil { // v2 池键清单（v1.3.13）
		cr := &contentReader{store: s, key: key, hashes: hs, hsizes: ss, hcs: hcs, remain: length}
		cr.seekTo(start)
		return m, cr, nil
	}
	cr := &contentReader{store: s, tbl: chunkTableFor(b.TableName), key: key, ver: m.VersionID,
		spans: parseChunkManifest(m.Manifest), legacySeq: -1, remain: length}
	cr.seekTo(start)
	return m, cr, nil
}

// GetObjectBytes 取对象完整内容（图片处理等需要整体加工的链路）。
func (s *Store) GetObjectBytes(bucket, key, versionID string) (ObjectMeta, []byte, error) {
	m, rc, err := s.GetObject(bucket, key, versionID)
	if err != nil {
		return m, nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return m, nil, err
	}
	return m, data, nil
}

// DeleteObject 删对象：
//   - versionID 非空：硬删该版本；
//   - versioning 开且无 versionID：插删除标记（版本链保留）；
//   - versioning 关：硬删。
func (s *Store) DeleteObject(bucket, key, versionID string, bypassGovernance bool) (ObjectMeta, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return ObjectMeta{}, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// 池引用释放与对象删除同事务（v1.3.13）：删引用行 + refcnt 回退一步到位
	del := ObjectMeta{Key: key, VersionID: versionID}
	delTx, err := s.db.Begin()
	if err != nil {
		return del, err
	}
	defer delTx.Rollback()

	oldStorage, oldCount := s.keyAggLocked(delTx, b, key)

	switch {
	case versionID != "":
		var rows *sql.Rows
		rows, err = s.db.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE key=? AND version_id=?`, objectCols, b.TableName), key, versionID)
		if err != nil {
			return del, err
		}
		if !rows.Next() {
			rows.Close()
			return del, &s3Error{Code: "NoSuchVersion", Status: 404, Msg: "The specified version does not exist"}
		}
		m, err := scanObject(rows)
		rows.Close()
		if err != nil {
			return del, err
		}
		if err := m.lockViolation(bypassGovernance); err != nil {
			return del, err
		}
		if err := s.deleteObjectRowsTx(delTx, b, key, versionID); err != nil {
			return del, err
		}
		// 若删的是最新版本，版本链里补最新标记
		var n int
		_ = delTx.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE key=? AND is_latest=1`, b.TableName), key).Scan(&n)
		if n == 0 {
			_, _ = delTx.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=1 WHERE key=? AND version_id=
				(SELECT version_id FROM %s WHERE key=? ORDER BY created_ts DESC, version_id DESC LIMIT 1)`,
				b.TableName, b.TableName), key, key)
		}
		del.DeleteMarker = m.DeleteMarker
		del.IsLatest = m.IsLatest

	case b.Versioning:
		// 插删除标记（检查当前最新非标记版本的对象锁）
		cur, cerr := s.headLatestLocked(b, key)
		if cerr != nil {
			return del, cerr
		}
		if err := cur.lockViolation(bypassGovernance); err != nil {
			return del, err
		}
		vid := genVersionID()
		if _, err := delTx.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=0 WHERE key=? AND is_latest=1`, b.TableName), key); err != nil {
			return del, err
		}
		if _, err := delTx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,is_latest,delete_marker,size,etag,
			content_type,content_encoding,cache_control,content_disposition,metadata,content,storage,
			legal_hold,retention_mode,retain_until,created_ts)
			VALUES(?,?,1,1,0,'','','','','','{}',NULL,'inline',0,'','',?)`, b.TableName),
			key, vid, nowTS()); err != nil {
			return del, err
		}
		del.VersionID = vid
		del.DeleteMarker = true
		del.IsLatest = true

	default:
		cur, cerr := s.headLatestLocked(b, key)
		if cerr != nil {
			return del, cerr // NoSuchKey
		}
		if err := cur.lockViolation(bypassGovernance); err != nil {
			return del, err
		}
		// 只删正式行（version_id=''），不碰进行中分片上传的暂存行（v1.3.11）
		if err := s.deleteObjectRowsTx(delTx, b, key, ""); err != nil {
			return del, err
		}
	}

	newStorage, newCount := s.keyAggLocked(delTx, b, key)
	s.adjustAggLocked(delTx, b, key, newStorage-oldStorage, newCount-oldCount)
	if err := delTx.Commit(); err != nil {
		return del, err
	}
	s.AddUsage(b.Owner, bucket, "DELETE", 0, 0)
	if s.syncHook != nil {
		s.syncHook.objectDeleted(bucket, key) // 删除传播（规则开启时；v1.3.13）
	}
	return del, nil
}

// headLatestLocked 查最新版本（调用方持 writeMu 与否均可，纯读）。
func (s *Store) headLatestLocked(b Bucket, key string) (ObjectMeta, error) {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT %s FROM %s WHERE key=? AND is_latest=1`, objectCols, b.TableName), key)
	if err != nil {
		return ObjectMeta{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return ObjectMeta{}, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	m, err := scanObject(rows)
	if err != nil {
		return ObjectMeta{}, err
	}
	if m.DeleteMarker {
		return ObjectMeta{}, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	return m, nil
}


// prefixUpperBound 前缀的字典序上界：'abc'→'abd'；高位全 0xff 时返回 ""（无上界）。
func prefixUpperBound(p string) string {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// prefixCond 前缀过滤条件（v1.3.11 性能优化：substr(key,1,?)=? 不可走索引，
// 改为 key 区间扫描——LIST/检索在 PK(key,version_id) 上从全桶扫描+排序降为
// 前缀区间直读）。
func prefixCond(p string) (string, []any) {
	if p == "" {
		return "", nil
	}
	if ub := prefixUpperBound(p); ub != "" {
		return " AND key >= ? AND key < ?", []any{p, ub}
	}
	return " AND key >= ?", []any{p}
}

// ListEntry LIST 返回行。
type ListEntry struct {
	Key          string
	Size         int64
	ETag         string
	LastModified string
	VersionID    string
	IsLatest     bool
	DeleteMarker bool
}

// ListResult LIST 结果（含分页与公共前缀）。
type ListResult struct {
	Entries        []ListEntry
	CommonPrefixes []string
	IsTruncated    bool
	NextMarker     string
	NextKeyMarker  string
	NextVerMarker  string
}

// ListObjects 列对象（ListObjects V1/V2；prefix/delimiter 分组，marker 分页）。
func (s *Store) ListObjects(bucket, prefix, delimiter, marker, startAfter string, maxKeys int) (ListResult, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return ListResult{}, err
	}
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	after := marker
	if startAfter != "" {
		after = startAfter
	}
	q := fmt.Sprintf(`SELECT %s FROM %s WHERE is_latest=1 AND delete_marker=0`, objectCols, b.TableName)
	args := []any{}
	if c, a := prefixCond(prefix); c != "" {
		q += c
		args = append(args, a...)
	}
	if after != "" {
		q += ` AND key>?`
		args = append(args, after)
	}
	q += ` ORDER BY key LIMIT ?`
	args = append(args, maxKeys*4+16) // 多取以支持 delimiter 折叠
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return ListResult{}, err
	}
	defer rows.Close()
	var res ListResult
	seenPrefix := map[string]bool{}
	count := 0
	for rows.Next() {
		m, err := scanObject(rows)
		if err != nil {
			return res, err
		}
		if delimiter != "" {
			rest := strings.TrimPrefix(m.Key, prefix)
			if i := strings.Index(rest, delimiter); i >= 0 {
				cp := prefix + rest[:i+len(delimiter)]
				if !seenPrefix[cp] {
					seenPrefix[cp] = true
					if count >= maxKeys {
						res.IsTruncated = true
						res.NextMarker = cp[:len(cp)-1] // 下一页从公共前缀前一个字符续
						break
					}
					res.CommonPrefixes = append(res.CommonPrefixes, cp)
					count++
				}
				continue
			}
		}
		if count >= maxKeys {
			res.IsTruncated = true
			res.NextMarker = m.Key
			res.NextKeyMarker = m.Key
			break
		}
		res.Entries = append(res.Entries, ListEntry{
			Key: m.Key, Size: m.Size, ETag: m.ETag,
			LastModified: m.CreatedTS, VersionID: m.VersionID, IsLatest: true,
		})
		count++
	}
	s.AddUsage(b.Owner, bucket, "LIST", 0, 0)
	return res, nil
}

// ListVersions 列对象版本链（含删除标记）。
func (s *Store) ListVersions(bucket, prefix, keyMarker, versionMarker string, maxKeys int) (ListResult, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return ListResult{}, err
	}
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	q := fmt.Sprintf(`SELECT %s FROM %s WHERE 1=1`, objectCols, b.TableName)
	args := []any{}
	if c, a := prefixCond(prefix); c != "" {
		q += c
		args = append(args, a...)
	}
	if keyMarker != "" {
		q += ` AND (key>? OR (key=? AND version_id>?))`
		args = append(args, keyMarker, keyMarker, versionMarker)
	}
	q += ` ORDER BY key, is_latest DESC, created_ts DESC, version_id DESC LIMIT ?`
	args = append(args, maxKeys+1)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return ListResult{}, err
	}
	defer rows.Close()
	var res ListResult
	n := 0
	for rows.Next() {
		m, err := scanObject(rows)
		if err != nil {
			return res, err
		}
		if n >= maxKeys {
			res.IsTruncated = true
			last := res.Entries[len(res.Entries)-1]
			res.NextKeyMarker = last.Key
			res.NextVerMarker = last.VersionID
			break
		}
		res.Entries = append(res.Entries, ListEntry{
			Key: m.Key, Size: m.Size, ETag: m.ETag, LastModified: m.CreatedTS,
			VersionID: m.VersionID, IsLatest: m.IsLatest, DeleteMarker: m.DeleteMarker,
		})
		n++
	}
	s.AddUsage(b.Owner, bucket, "LIST", 0, 0)
	return res, nil
}

// ListObjectsRaw 管理面 SHOW S3 OBJECTS：列最新对象（LIKE 前缀过滤）。
func (s *Store) ListObjectsRaw(bucket, prefix string, limit int) ([]ObjectMeta, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	q := fmt.Sprintf(`SELECT %s FROM %s WHERE is_latest=1 AND delete_marker=0`, objectCols, b.TableName)
	args := []any{}
	if c, a := prefixCond(prefix); c != "" {
		q += c
		args = append(args, a...)
	}
	q += ` ORDER BY key LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ObjectMeta
	for rows.Next() {
		if m, err := scanObject(rows); err == nil {
			m.Metadata = nil
			out = append(out, m)
		}
	}
	return out, nil
}

// SetRetain 设置对象保留（WORM）。
func (s *Store) SetRetain(bucket, key, versionID, mode, until string) error {
	b, err := s.Bucket(bucket)
	if err != nil {
		return err
	}
	if mode != "COMPLIANCE" && mode != "GOVERNANCE" {
		return fmt.Errorf("retention mode must be COMPLIANCE or GOVERNANCE")
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	where := `key=? AND is_latest=1`
	args := []any{mode, until, key}
	if versionID != "" {
		where = `key=? AND version_id=?`
		args = []any{mode, until, key, versionID}
	}
	res, err := s.db.Exec(fmt.Sprintf(`UPDATE %s SET retention_mode=?, retain_until=? WHERE %s`, b.TableName, where), args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	return nil
}

// SetLegalHold 设置对象 legal hold（WORM 例外锁）。
func (s *Store) SetLegalHold(bucket, key, versionID string, on bool) error {
	b, err := s.Bucket(bucket)
	if err != nil {
		return err
	}
	v := 0
	if on {
		v = 1
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	where := `key=? AND is_latest=1`
	args := []any{v, key}
	if versionID != "" {
		where = `key=? AND version_id=?`
		args = []any{v, key, versionID}
	}
	res, err := s.db.Exec(fmt.Sprintf(`UPDATE %s SET legal_hold=? WHERE %s`, b.TableName, where), args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	return nil
}

// CopyObject 桶内 / 跨桶复制对象（x-amz-copy-source）。
// v1.3.13 零拷贝：分块对象直接复制池引用（引用计数 +1，无数据搬运）；
// 内联小对象维持原字节复制路径（单行无共享收益）。
func (s *Store) CopyObject(srcBucket, srcKey, srcVersion, dstBucket, dstKey string, attrs ObjectAttrs) (ObjectMeta, error) {
	if dstKey == "" || len(dstKey) > 1024 {
		return ObjectMeta{}, &s3Error{Code: "InvalidArgument", Status: 400, Msg: "Invalid object key"}
	}
	m, err := s.HeadObject(srcBucket, srcKey, srcVersion)
	if err != nil {
		return ObjectMeta{}, err
	}
	if m.DeleteMarker {
		return ObjectMeta{}, &s3Error{Code: "NoSuchKey", Status: 404, Msg: "The specified key does not exist"}
	}
	if attrs.ContentType == "" {
		attrs.ContentType = m.ContentType
	}
	if attrs.ContentEncoding == "" {
		attrs.ContentEncoding = m.ContentEncoding
	}
	if attrs.CacheControl == "" {
		attrs.CacheControl = m.CacheControl
	}
	if attrs.ContentDisposition == "" {
		attrs.ContentDisposition = m.ContentDisposition
	}
	if len(attrs.Metadata) == 0 {
		attrs.Metadata = m.Metadata
	}
	if attrs.Metadata == nil {
		attrs.Metadata = map[string]string{}
	}

	if m.Storage == "chunk" {
		return s.copyObjectRefs(srcBucket, srcKey, m, dstBucket, dstKey, attrs)
	}
	_, data, err := s.GetObjectBytes(srcBucket, srcKey, srcVersion)
	if err != nil {
		return ObjectMeta{}, err
	}
	out, _, err := s.PutObject(dstBucket, dstKey, bytes.NewReader(data), int64(len(data)), attrs)
	return out, err
}

// copyObjectRefs 零拷贝复制：目标对象行引用源对象同一批池 chunk（seq 布局对齐，
// manifest v2 重建）。跨桶时目标桶 scope 不同但引用行记录的是池键本身，读取与
// 释放均按引用行进行——范围语义只影响「新内容入池时的键前缀」。
func (s *Store) copyObjectRefs(srcBucket, srcKey string, m ObjectMeta, dstBucket, dstKey string, attrs ObjectAttrs) (ObjectMeta, error) {
	var out ObjectMeta
	sb, err := s.Bucket(srcBucket)
	if err != nil {
		return out, err
	}
	db, err := s.Bucket(dstBucket)
	if err != nil {
		return out, err
	}
	// 收集源引用（v2 清单直接给池键序列；旧 span 布局取引用行——块大小须全段一致
	// 才能重建 v2 清单，混合块大小 / 无清单的稠密旧行退回字节复制，保证 Range 正确）
	var hashes []string
	var srcSizes []int64
	srcCS := s.chunkSize
	if hs, ss, cs := parseChunkManifestV2(m.Manifest); hs != nil {
		hashes = hs
		srcSizes = ss
		srcCS = cs
	} else if spans := parseChunkManifest(m.Manifest); spans != nil {
		uniform := true
		for _, sp := range spans {
			if sp.cs != spans[0].cs {
				uniform = false
				break
			}
		}
		if !uniform {
			_, data, err := s.GetObjectBytes(srcBucket, srcKey, m.VersionID)
			if err != nil {
				return ObjectMeta{}, err
			}
			out, _, err := s.PutObject(dstBucket, dstKey, bytes.NewReader(data), int64(len(data)), attrs)
			return out, err
		}
		srcCS = spans[0].cs
		hashes, err = s.collectChunkHashes(chunkTableFor(sb.TableName), srcKey, m.VersionID)
		if err != nil {
			return out, err
		}
		if len(hashes) == 0 {
			return out, &s3Error{Code: "InternalError", Status: 500,
				Msg: "source object chunk refs missing (migration in progress, retry)"}
		}
		// 旧 span 布局合成各块尺寸（每段段内均匀 cs、段末块为余数）
		srcSizes = make([]int64, 0, len(hashes))
		remaining := m.Size
		for _, sp := range spans {
			seqSizes := chunkSizeSeq(sp.size, sp.cs, int(sp.chunks))
			srcSizes = append(srcSizes, seqSizes...)
			remaining -= sp.size
		}
		_ = remaining
	} else {
		// 稠密旧布局（≤ v1.3.10）：分块大小未知，退回字节复制
		_, data, err := s.GetObjectBytes(srcBucket, srcKey, m.VersionID)
		if err != nil {
			return ObjectMeta{}, err
		}
		out, _, err := s.PutObject(dstBucket, dstKey, bytes.NewReader(data), int64(len(data)), attrs)
		return out, err
	}
	// 覆盖保护（与 PutObject 一致）：非 versioning 覆盖须过对象锁
	if !db.Versioning {
		if cur, err := s.HeadObject(dstBucket, dstKey, ""); err == nil {
			if err := cur.lockViolation(false); err != nil {
				return out, err
			}
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return out, err
	}
	defer tx.Rollback()

	oldStorage, oldCount := s.keyAggLocked(tx, db, dstKey)
	dstVersion := ""
	if db.Versioning {
		dstVersion = genVersionID()
		if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=0 WHERE key=? AND is_latest=1`, db.TableName), dstKey); err != nil {
			return out, err
		}
	} else {
		if err := s.deleteObjectRowsTx(tx, db, dstKey, ""); err != nil {
			return out, err
		}
	}
	// 引用目标 chunk（已 GC 掉 → 整体回退报错，客户端按普通 Copy 重试）
	tbl := chunkTableFor(db.TableName)
	for seq, pk := range hashes {
		ok, err := poolRefExisting(tx, pk)
		if err != nil {
			return out, err
		}
		if !ok {
			return out, &s3Error{Code: "InternalError", Status: 500,
				Msg: "source chunk evicted during copy (retry)"}
		}
		if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,hash,data) VALUES(?,?,?,?,x'')`, tbl),
			dstKey, dstVersion, seq, pk); err != nil {
			return out, err
		}
	}
	if srcSizes == nil { // 兜底（不应发生）：按均匀 cs 合成
		srcSizes = chunkSizeSeq(m.Size, srcCS, len(hashes))
	}
	manifest := MarshalPolicy(chunkManifestV2{V: 2, CS: srcCS, HS: hashes, SS: srcSizes})
	metaJSON := "{}"
	if len(attrs.Metadata) > 0 {
		metaJSON = MarshalPolicy(attrs.Metadata)
	}
	if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,is_latest,delete_marker,size,etag,
		content_type,content_encoding,cache_control,content_disposition,metadata,content,storage,manifest,
		legal_hold,retention_mode,retain_until,created_ts)
		VALUES(?,?,1,0,?,?,?,?,?,?,?,?,?,?,0,'','',?)`, db.TableName),
		dstKey, dstVersion, m.Size, m.ETag,
		attrs.ContentType, attrs.ContentEncoding, attrs.CacheControl, attrs.ContentDisposition,
		metaJSON, nil, "chunk", manifest, nowTS()); err != nil {
		return out, err
	}
	if db.Versioning {
		s.adjustAggLocked(tx, db, dstKey, m.Size, 1)
	} else {
		s.adjustAggLocked(tx, db, dstKey, m.Size-oldStorage, 1-oldCount)
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}
	s.AddUsage(db.Owner, dstBucket, "PUT", 0, 0) // 零拷贝：不计 bytes_in（内容未经过网络/磁盘重写）

	out = ObjectMeta{
		Key: dstKey, VersionID: dstVersion, IsLatest: true, Size: m.Size, ETag: m.ETag,
		ContentType: attrs.ContentType, ContentEncoding: attrs.ContentEncoding,
		CacheControl: attrs.CacheControl, ContentDisposition: attrs.ContentDisposition,
		Metadata: attrs.Metadata, Storage: "chunk", CreatedTS: nowTS(),
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 对象检索（管理面 SUBMIT S3 TEXT/FILE SEARCH / DELETE，v1.3.11）
// ---------------------------------------------------------------------------

// SearchFilter 对象检索过滤（均可选；时间串格式 'YYYY-MM-DD[ HH:MM:SS]'）。
type SearchFilter struct {
	Prefix    string
	Like      string // SQL LIKE 通配（%/_），对 key 匹配
	SizeMin   int64  // <=0 = 未设
	SizeMax   int64  // <=0 = 未设
	ModBefore string // 修改时间（= 最新版本写入时间）上界
	ModAfter  string // 修改时间下界
	CreBefore string // 创建时间（= 全版本最早写入时间）上界
	CreAfter  string // 创建时间下界
	Limit     int    // 0 = 默认 1000
}

// SearchCandidate 检索结果行（最新版本口径 + 首版本创建时间）。
type SearchCandidate struct {
	Key         string
	Size        int64
	ContentType string
	ETag        string
	ModifiedTS  string
	CreatedTS   string
}

// SearchObjects 按过滤条件检索桶内最新对象（prefix/LIKE/size/modified 下推 SQL，
// created 涉及全版本聚合在 Go 侧过滤；天然受桶级单表索引加速）。
func (s *Store) SearchObjects(bucket string, f SearchFilter) ([]SearchCandidate, error) {
	b, err := s.Bucket(bucket)
	if err != nil {
		return nil, err
	}
	limit := f.Limit
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	q := fmt.Sprintf(`SELECT key,size,content_type,etag,created_ts,
		(SELECT MIN(created_ts) FROM %s v2 WHERE v2.key=%s.key) AS first_ts
		FROM %s WHERE is_latest=1 AND delete_marker=0`, b.TableName, b.TableName, b.TableName)
	var args []any
	if c, a := prefixCond(f.Prefix); c != "" {
		q += c
		args = append(args, a...)
	}
	if f.Like != "" {
		q += ` AND key LIKE ?` // 用户通配符（%/_）原样生效；参数化绑定防注入
		args = append(args, f.Like)
	}
	if f.SizeMin > 0 {
		q += ` AND size>=?`
		args = append(args, f.SizeMin)
	}
	if f.SizeMax > 0 {
		q += ` AND size<=?`
		args = append(args, f.SizeMax)
	}
	if f.ModAfter != "" {
		q += ` AND created_ts>=?`
		args = append(args, normTS(f.ModAfter))
	}
	if f.ModBefore != "" {
		q += ` AND created_ts<?`
		args = append(args, normTS(f.ModBefore))
	}
	q += ` ORDER BY key`
	// created 过滤在 Go 侧：放大抓取窗口后裁剪
	scanCap := limit
	if f.CreAfter != "" || f.CreBefore != "" {
		scanCap = limit * 8
		if scanCap > 20000 {
			scanCap = 20000
		}
	}
	q += ` LIMIT ?`
	args = append(args, scanCap)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SearchCandidate
	for rows.Next() {
		var c SearchCandidate
		if err := rows.Scan(&c.Key, &c.Size, &c.ContentType, &c.ETag, &c.ModifiedTS, &c.CreatedTS); err != nil {
			return nil, err
		}
		if c.CreatedTS == "" {
			c.CreatedTS = c.ModifiedTS
		}
		if f.CreAfter != "" && c.CreatedTS < normTS(f.CreAfter) {
			continue
		}
		if f.CreBefore != "" && c.CreatedTS >= normTS(f.CreBefore) {
			continue
		}
		out = append(out, c)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// normTS 规范化时间过滤值：纯日期补 ' 00:00:00'，便于与 created_ts 字典序比较。
func normTS(ts string) string {
	ts = strings.TrimSpace(ts)
	if len(ts) == 10 {
		return ts + " 00:00:00"
	}
	return ts
}
