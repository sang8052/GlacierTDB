package s3

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// 分片上传（multipart，v1.3.1；v1.3.11 存储层重写）：
//
//	POST   /{bucket}/{key}?uploads               → InitiateMultipartUpload（返回 UploadId）
//	PUT    /{bucket}/{key}?partNumber=N&uploadId= → UploadPart（ETag = 该 part 内容 MD5）
//	POST   /{bucket}/{key}?uploadId=             → CompleteMultipartUpload（ETag = MD5(各 part MD5 顺序拼接)+"-N"）
//	DELETE /{bucket}/{key}?uploadId=             → AbortMultipartUpload
//	GET    /{bucket}/{key}?uploadId=             → ListParts
//	GET    /{bucket}?uploads                     → ListMultipartUploads
//
// 存储（v1.3.11）：分片内容直接以「最终对象身份」落桶分块表——行键
// (key=对象key, version_id=uploadId, seq=partNumber*stride+块内序号)，按 8MB
// 分块流式写入（内存占用与分片大小无关，单分片可达对象上限）。
// Complete 合并零数据拷贝：
//   - versioning 桶：对象行 version_id 即 uploadId，分块行原地转正；
//   - 非 versioning 桶：一条 UPDATE 把暂存行 version_id 改为 ''（删旧行之后）；
//   - 小对象（≤ inline 阈值）：装配进 content 列后清理分块行。
// 对象行的 manifest 列记录各 part 的 [startSeq, nChunks, size, chunkSize]，
// GET Range 据此直定位分块。旧版 '!mpu!' 前缀行仅在中止清理时兼容删除。

// mpuSeqStride 每 part 的 seq 地址空间（块数）。1<<20 块 × 8MB = 8TB/part 余量，
// partNumber ≤ 10000，seq 上限 ~1.05e10，int64 安全。
const mpuSeqStride = 1 << 20

// legacyMPUKey 旧版（≤ v1.3.10）分片暂存行 key 前缀，仅用于兼容清理。
const legacyMPUKey = "!mpu!"

// maxParts 单次上传分片数上限（对齐 AWS：1..10000）。
const maxParts = 10000

// UploadRow 分片上传会话行（s3_uploads + 分片聚合）。
type UploadRow struct {
	UploadID  string
	Bucket    string
	Key       string
	Parts     int
	Bytes     int64
	CreatedTS string
}

// PartRow 分片行（s3_parts）。
type PartRow struct {
	PartNumber int
	Size       int64
	ETag       string
	CreatedTS  string
}

// PartSpec CompleteMultipartUpload 请求里列出的分片（PartNumber + ETag）。
type PartSpec struct {
	PartNumber int
	ETag       string
}

// InitiateMultipart 发起分片上传，返回 UploadId。
func (s *Store) InitiateMultipart(bucket, key string, attrs ObjectAttrs) (string, error) {
	if key == "" || len(key) > 1024 {
		return "", &s3Error{Code: "InvalidArgument", Status: 400, Msg: "Invalid object key"}
	}
	if _, err := s.Bucket(bucket); err != nil {
		return "", err
	}
	uploadID := genVersionID()
	if attrs.Metadata == nil {
		attrs.Metadata = map[string]string{}
	}
	attrsJSON := MarshalPolicy(attrs)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.db.Exec(`INSERT INTO s3_uploads(upload_id,bucket,key,attrs,created_ts) VALUES(?,?,?,?,?)`,
		uploadID, bucket, key, attrsJSON, nowTS()); err != nil {
		return "", err
	}
	return uploadID, nil
}

// uploadLocked 取会话行并校验 bucket/key 归属（纯读，调用方持 writeMu 与否均可）。
func (s *Store) uploadLocked(bucket, key, uploadID string) (ObjectAttrs, error) {
	var gotBucket, gotKey, attrsJSON string
	err := s.db.QueryRow(`SELECT bucket,key,attrs FROM s3_uploads WHERE upload_id=?`, uploadID).
		Scan(&gotBucket, &gotKey, &attrsJSON)
	if err == sql.ErrNoRows {
		return ObjectAttrs{}, &s3Error{Code: "NoSuchUpload", Status: 404,
			Msg: "The specified multipart upload does not exist"}
	}
	if err != nil {
		return ObjectAttrs{}, err
	}
	if gotBucket != bucket || gotKey != key {
		return ObjectAttrs{}, &s3Error{Code: "NoSuchUpload", Status: 404,
			Msg: "The specified multipart upload does not exist"}
	}
	var attrs ObjectAttrs
	_ = json.Unmarshal([]byte(attrsJSON), &attrs)
	if attrs.Metadata == nil {
		attrs.Metadata = map[string]string{}
	}
	return attrs, nil
}

// UploadPart 上传一个分片：内容先暂存临时文件（内存有界），再按 8MB 分块直接
// 落桶分块表暂存区 (key, uploadId, partNumber*stride+i)，返回该分片 MD5（hex）。
// 同 partNumber 重传覆盖（S3 语义：最后一次为准）。
func (s *Store) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader, sizeHint int64) (string, error) {
	if partNumber < 1 || partNumber > maxParts {
		return "", &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: fmt.Sprintf("Part number must be an integer between 1 and %d", maxParts)}
	}
	if _, err := s.uploadLocked(bucket, key, uploadID); err != nil {
		return "", err
	}
	tmp, size, sum, err := s.spoolPart(r)
	if err != nil {
		return "", err
	}
	etag := hex.EncodeToString(sum)
	if sizeHint >= 0 && sizeHint != size {
		return "", &s3Error{Code: "IncompleteBody", Status: 400,
			Msg: "You did not provide the number of bytes specified by the Content-Length HTTP header"}
	}
	return s.writePartFromSpool(bucket, key, uploadID, partNumber, tmp, size, etag)
}

// spoolPart 分片内容暂存临时文件并计算 MD5（大分片不进内存；v1.3.11）。
// 返回（临时文件, 字节数, MD5 摘要）。
func (s *Store) spoolPart(r io.Reader) (*os.File, int64, []byte, error) {
	tmpDir := filepath.Join(s.dir, ".s3tmp")
	_ = os.MkdirAll(tmpDir, 0o700)
	tmp, err := os.CreateTemp(tmpDir, "part-*")
	if err != nil {
		return nil, 0, nil, err
	}
	md5h := md5.New()
	var size int64
	if s.maxObject > 0 {
		size, err = io.Copy(io.MultiWriter(tmp, md5h), io.LimitReader(r, s.maxObject+1))
	} else {
		size, err = io.Copy(io.MultiWriter(tmp, md5h), r)
	}
	if err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, 0, nil, err
	}
	if s.maxObject > 0 && size > s.maxObject {
		tmp.Close()
		os.Remove(tmp.Name())
		return nil, 0, nil, &s3Error{Code: "EntityTooLarge", Status: 400,
			Msg: fmt.Sprintf("Part size %d exceeds max_object_mb limit %d", size, s.maxObject>>20)}
	}
	return tmp, size, md5h.Sum(nil), nil
}

// writePartFromSpool 暂存文件 → 分块表暂存区 + s3_parts 元数据（调用方持 writeMu 前置检查后）。
func (s *Store) writePartFromSpool(bucket, key, uploadID string, partNumber int,
	tmp *os.File, size int64, etag string) (string, error) {
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.uploadLocked(bucket, key, uploadID); err != nil {
		return "", err
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return "", err
	}
	tbl := chunkTableFor(b.TableName)
	base := int64(partNumber) * mpuSeqStride

	tx, err := s.db.Begin()
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=? AND seq>=? AND seq<?`, tbl),
		key, uploadID, base, base+mpuSeqStride); err != nil {
		return "", err
	}
	chunks := int64(0)
	if size > 0 {
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return "", err
		}
		buf := make([]byte, s.chunkSize)
		for {
			n, rerr := io.ReadFull(tmp, buf)
			if n > 0 {
				if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,data) VALUES(?,?,?,?)`, tbl),
					key, uploadID, base+chunks, buf[:n]); err != nil {
					return "", err
				}
				chunks++
			}
			if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
				break
			}
			if rerr != nil {
				return "", rerr
			}
		}
	}
	if _, err := tx.Exec(`INSERT INTO s3_parts(upload_id,part_number,size,etag,created_ts,chunks) VALUES(?,?,?,?,?,?)
		ON CONFLICT(upload_id,part_number) DO UPDATE SET size=excluded.size, etag=excluded.etag,
		created_ts=excluded.created_ts, chunks=excluded.chunks`,
		uploadID, partNumber, size, etag, nowTS(), chunks); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	s.addUsageLocked(b.Owner, bucket, "PUT", size, 0)
	return etag, nil
}

// UploadPartCopy 拷贝分片（v1.3.5；v1.3.11 改区间流式）：从源对象读取指定字节
// 区间（versionId 可选）作为分片内容，不整读源对象、不经过网络重传。
// rng 为 nil = 整对象。返回（分片 ETag, LastModified）。
func (s *Store) UploadPartCopy(bucket, key, uploadID string, partNumber int,
	srcBucket, srcKey, srcVersion string, start, end int64, hasRange bool) (string, string, error) {
	if partNumber < 1 || partNumber > maxParts {
		return "", "", &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: fmt.Sprintf("Part number must be an integer between 1 and %d", maxParts)}
	}
	if _, err := s.uploadLocked(bucket, key, uploadID); err != nil {
		return "", "", err
	}
	meta, err := s.HeadObject(srcBucket, srcKey, srcVersion)
	if err != nil {
		return "", "", err
	}
	if !hasRange {
		start, end = 0, meta.Size-1
	}
	if start < 0 || end < start || end >= meta.Size {
		return "", "", &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: fmt.Sprintf("Range bytes=%d-%d is invalid for source size %d", start, end, meta.Size)}
	}
	_, rc, err := s.GetObjectRange(srcBucket, srcKey, srcVersion, start, end-start+1)
	if err != nil {
		return "", "", err
	}
	defer rc.Close()
	tmp, size, sum, err := s.spoolPart(rc)
	if err != nil {
		return "", "", err
	}
	etag := hex.EncodeToString(sum)
	if _, err := s.writePartFromSpool(bucket, key, uploadID, partNumber, tmp, size, etag); err != nil {
		return "", "", err
	}
	return etag, nowTS(), nil
}

// AbortStaleUploads 回收超龄未完成分片上传会话（v1.3.5：s3.abort_incomplete_days
// 自动生命周期），返回回收会话数。
func (s *Store) AbortStaleUploads(days int) int {
	if days <= 0 {
		return 0
	}
	cutoff := time.Now().AddDate(0, 0, -days).Format("2006-01-02 15:04:05")
	rows, err := s.db.Query(`SELECT bucket, key, upload_id FROM s3_uploads WHERE created_ts < ?`, cutoff)
	if err != nil {
		return 0
	}
	type stale struct{ bucket, key, id string }
	var list []stale
	for rows.Next() {
		var st stale
		if err := rows.Scan(&st.bucket, &st.key, &st.id); err == nil {
			list = append(list, st)
		}
	}
	rows.Close()
	n := 0
	for _, st := range list {
		if err := s.AbortMultipart(st.bucket, st.key, st.id); err == nil {
			n++
		}
	}
	return n
}

// CompleteMultipart 合并分片完成上传（v1.3.11 零拷贝）：分片早已按最终对象身份
// 落在分块表，versioning 桶直接以 uploadId 为版本号转正；非 versioning 桶把暂存
// 行 version_id 改写为 ''；小对象装配进 content 列。manifest 记录各 part 分块布局。
// ETag = MD5(各 part MD5 顺序拼接二进制) + "-" + N（S3 语义）。
func (s *Store) CompleteMultipart(bucket, key, uploadID string, parts []PartSpec) (ObjectMeta, bool, error) {
	var out ObjectMeta
	replaced := false
	if len(parts) == 0 {
		return out, false, &s3Error{Code: "InvalidPart", Status: 400,
			Msg: "One or more of the specified parts could not be found"}
	}
	attrs, err := s.uploadLocked(bucket, key, uploadID)
	if err != nil {
		return out, false, err
	}

	// 校验所列 part 齐全、序号严格递增、ETag 与已上传分片一致
	stored := map[int]PartRow{}
	rows, err := s.db.Query(`SELECT part_number,size,etag,created_ts FROM s3_parts WHERE upload_id=?`, uploadID)
	if err != nil {
		return out, false, err
	}
	for rows.Next() {
		var p PartRow
		if err := rows.Scan(&p.PartNumber, &p.Size, &p.ETag, &p.CreatedTS); err == nil {
			stored[p.PartNumber] = p
		}
	}
	rows.Close()
	prev := 0
	var total int64
	for _, sp := range parts {
		if sp.PartNumber <= prev {
			return out, false, &s3Error{Code: "InvalidPartOrder", Status: 400,
				Msg: "The list of parts was not in ascending order"}
		}
		prev = sp.PartNumber
		p, ok := stored[sp.PartNumber]
		if !ok {
			return out, false, &s3Error{Code: "InvalidPart", Status: 400,
				Msg: fmt.Sprintf("Part %d could not be found", sp.PartNumber)}
		}
		if got := stripETagQuotes(sp.ETag); got != "" && got != strings.ToLower(p.ETag) {
			return out, false, &s3Error{Code: "InvalidPart", Status: 400,
				Msg: fmt.Sprintf("Part %d ETag does not match", sp.PartNumber)}
		}
		total += p.Size
	}
	if s.maxObject > 0 && total > s.maxObject {
		return out, false, &s3Error{Code: "EntityTooLarge", Status: 400,
			Msg: fmt.Sprintf("Object size %d exceeds max_object_mb limit %d", total, s.maxObject>>20)}
	}
	// 复合 ETag：MD5(各 part MD5 原始 16 字节顺序拼接) + "-" + N
	etagH := md5.New()
	for _, sp := range parts {
		raw, _ := hex.DecodeString(stored[sp.PartNumber].ETag)
		etagH.Write(raw)
	}
	etag := fmt.Sprintf("%s-%d", hex.EncodeToString(etagH.Sum(nil)), len(parts))

	b, err := s.Bucket(bucket)
	if err != nil {
		return out, false, err
	}
	// WORM / 版本链保护（与 PutObject 一致）
	if !b.Versioning {
		if cur, err := s.HeadObject(bucket, key, ""); err == nil {
			if err := cur.lockViolation(false); err != nil {
				return out, false, err
			}
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	oldStorage, oldCount := s.keyAggLocked(b, key)
	// versioning 桶：uploadId 即最终版本号（分块行无需搬动）；
	// 非 versioning 桶：最终 version_id=''，暂存行随后改写。
	versionID := ""
	if b.Versioning {
		versionID = uploadID
	}

	chunkTbl := chunkTableFor(b.TableName)
	storage := "inline"
	if total > s.inlineMax {
		storage = "chunk"
	}
	metaJSON := "{}"
	if len(attrs.Metadata) > 0 {
		metaJSON = MarshalPolicy(attrs.Metadata)
	}
	manifest := ""
	if storage == "chunk" {
		mf := chunkManifest{CS: s.chunkSize}
		for _, sp := range parts {
			n := stored[sp.PartNumber].Size
			mf.Parts = append(mf.Parts, [4]int64{int64(sp.PartNumber) * mpuSeqStride, chunksFor(n, s.chunkSize), n, s.chunkSize})
		}
		manifest = MarshalPolicy(mf)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return out, false, err
	}
	defer tx.Rollback()

	if b.Versioning {
		if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=0 WHERE key=? AND is_latest=1`, b.TableName), key); err != nil {
			return out, false, err
		}
	} else {
		// 清理旧正式行（只清 version_id=''，不碰其他会话的暂存行）
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=''`, chunkTbl), key); err != nil {
			return out, false, err
		}
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=''`, b.TableName), key); err != nil {
			return out, false, err
		}
		replaced = oldCount > 0
	}
	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,is_latest,delete_marker,size,etag,
		content_type,content_encoding,cache_control,content_disposition,metadata,content,storage,manifest,
		legal_hold,retention_mode,retain_until,created_ts)
		VALUES(?,?,1,0,?,?,?,?,?,?,?,?,?,?,0,'','',?)`, b.TableName),
		key, versionID, total, etag,
		attrs.ContentType, attrs.ContentEncoding, attrs.CacheControl, attrs.ContentDisposition,
		metaJSON, nil, storage, manifest, nowTS())
	if err != nil {
		return out, false, err
	}

	if storage == "chunk" {
		if !b.Versioning { // 暂存行 version_id → ''（一条语句，SQLite 内部完成）
			if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET version_id='' WHERE key=? AND version_id=?`, chunkTbl),
				key, uploadID); err != nil {
				return out, false, err
			}
		}
	} else {
		// 小对象：按 part 顺序装配 content 列后清理分块行
		var content []byte
		for _, sp := range parts {
			base := int64(sp.PartNumber) * mpuSeqStride
			prows, qerr := tx.Query(fmt.Sprintf(
				`SELECT data FROM %s WHERE key=? AND version_id=? AND seq>=? AND seq<? ORDER BY seq`, chunkTbl),
				key, uploadID, base, base+mpuSeqStride)
			if qerr != nil {
				return out, false, qerr
			}
			for prows.Next() {
				var data []byte
				if err := prows.Scan(&data); err != nil {
					prows.Close()
					return out, false, err
				}
				content = append(content, data...)
			}
			prows.Close()
		}
		if int64(len(content)) != total {
			return out, false, &s3Error{Code: "InvalidPart", Status: 400,
				Msg: "assembled object size mismatch (part data missing)"}
		}
		if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET content=? WHERE key=? AND version_id=?`, b.TableName),
			content, key, versionID); err != nil {
			return out, false, err
		}
		if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=?`, chunkTbl), key, uploadID); err != nil {
			return out, false, err
		}
	}

	// 清理会话（顺带兼容清理旧版 '!mpu!' 暂存行）
	if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTbl), legacyMPUKey+uploadID); err != nil {
		return out, false, err
	}
	if _, err := tx.Exec(`DELETE FROM s3_parts WHERE upload_id=?`, uploadID); err != nil {
		return out, false, err
	}
	if _, err := tx.Exec(`DELETE FROM s3_uploads WHERE upload_id=?`, uploadID); err != nil {
		return out, false, err
	}
	if err := tx.Commit(); err != nil {
		return out, false, err
	}

	newStorage, newCount := s.keyAggLocked(b, key)
	s.adjustAggLocked(b, key, newStorage-oldStorage, newCount-oldCount)
	s.addUsageLocked(b.Owner, bucket, "PUT", total, 0)

	out = ObjectMeta{
		Key: key, VersionID: versionID, IsLatest: true, Size: total, ETag: etag,
		ContentType: attrs.ContentType, ContentEncoding: attrs.ContentEncoding,
		CacheControl: attrs.CacheControl, ContentDisposition: attrs.ContentDisposition,
		Metadata: attrs.Metadata, Storage: storage, Manifest: manifest, CreatedTS: nowTS(),
	}
	return out, replaced, nil
}

// chunksFor size 按 chunkSize 的分块数（向上取整；0 → 0）。
func chunksFor(size, chunkSize int64) int64 {
	if size <= 0 {
		return 0
	}
	return (size + chunkSize - 1) / chunkSize
}

// AbortMultipart 中止分片上传：清理暂存分块行、元数据与会话（幂等；含旧版布局兼容）。
func (s *Store) AbortMultipart(bucket, key, uploadID string) error {
	if _, err := s.uploadLocked(bucket, key, uploadID); err != nil {
		return err
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	tbl := chunkTableFor(b.TableName)
	if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=?`, tbl), key, uploadID); err != nil {
		return err
	}
	if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, tbl), legacyMPUKey+uploadID); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM s3_parts WHERE upload_id=?`, uploadID); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM s3_uploads WHERE upload_id=?`, uploadID); err != nil {
		return err
	}
	s.addUsageLocked(b.Owner, bucket, "DELETE", 0, 0)
	return nil
}

// ListParts 列出上传会话的分片（partNumber 升序）。
func (s *Store) ListParts(bucket, key, uploadID string) ([]PartRow, error) {
	if _, err := s.uploadLocked(bucket, key, uploadID); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT part_number,size,etag,created_ts FROM s3_parts
		WHERE upload_id=? ORDER BY part_number`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PartRow
	for rows.Next() {
		var p PartRow
		if err := rows.Scan(&p.PartNumber, &p.Size, &p.ETag, &p.CreatedTS); err == nil {
			out = append(out, p)
		}
	}
	return out, nil
}

// ListMultipartUploads 列出分片上传会话（bucket 为空 = 全部桶；prefix 过滤 key 前缀）。
func (s *Store) ListMultipartUploads(bucket, prefix string) []UploadRow {
	q := `SELECT u.upload_id,u.bucket,u.key,u.created_ts,
		(SELECT COUNT(*) FROM s3_parts p WHERE p.upload_id=u.upload_id),
		(SELECT COALESCE(SUM(size),0) FROM s3_parts p WHERE p.upload_id=u.upload_id)
		FROM s3_uploads u`
	var args []any
	var conds []string
	if bucket != "" {
		conds = append(conds, "u.bucket=?")
		args = append(args, bucket)
	}
	if prefix != "" {
		if ub := prefixUpperBound(prefix); ub != "" {
			conds = append(conds, "u.key >= ?", "u.key < ?")
			args = append(args, prefix, ub)
		} else {
			conds = append(conds, "u.key >= ?")
			args = append(args, prefix)
		}
	}
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	q += " ORDER BY u.created_ts"
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []UploadRow
	for rows.Next() {
		var u UploadRow
		if err := rows.Scan(&u.UploadID, &u.Bucket, &u.Key, &u.CreatedTS, &u.Parts, &u.Bytes); err == nil {
			out = append(out, u)
		}
	}
	return out
}

// dropBucketUploads 删桶时清理其未完成的分片上传（调用方持 writeMu；含旧版布局兼容）。
func (s *Store) dropBucketUploads(b Bucket) error {
	ids, err := s.db.Query(`SELECT key, upload_id FROM s3_uploads WHERE bucket=?`, b.Name)
	if err != nil {
		return err
	}
	type uk struct{ key, id string }
	var list []uk
	for ids.Next() {
		var u uk
		if err := ids.Scan(&u.key, &u.id); err == nil {
			list = append(list, u)
		}
	}
	ids.Close()
	tbl := chunkTableFor(b.TableName)
	for _, u := range list {
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=?`, tbl), u.key, u.id); err != nil {
			return err
		}
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, tbl), legacyMPUKey+u.id); err != nil {
			return err
		}
		if _, err := s.db.Exec(`DELETE FROM s3_parts WHERE upload_id=?`, u.id); err != nil {
			return err
		}
	}
	_, err = s.db.Exec(`DELETE FROM s3_uploads WHERE bucket=?`, b.Name)
	return err
}

// stripETagQuotes 去 ETag 引号并小写（比较用）。
func stripETagQuotes(s string) string {
	return strings.ToLower(strings.Trim(s, `"`))
}
