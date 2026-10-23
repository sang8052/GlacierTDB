package s3

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// 分片上传（multipart，v1.3.1）：
//
//	POST   /{bucket}/{key}?uploads               → InitiateMultipartUpload（返回 UploadId）
//	PUT    /{bucket}/{key}?partNumber=N&uploadId= → UploadPart（ETag = 该 part 内容 MD5）
//	POST   /{bucket}/{key}?uploadId=             → CompleteMultipartUpload（ETag = MD5(各 part MD5 顺序拼接)+"-N"）
//	DELETE /{bucket}/{key}?uploadId=             → AbortMultipartUpload
//	GET    /{bucket}/{key}?uploadId=             → ListParts
//	GET    /{bucket}?uploads                     → ListMultipartUploads
//
// 存储：s3_uploads（会话）+ s3_parts（分片元数据）；分片内容存桶分块表
// （chk_<bucket>，key='!mpu!<uploadId>'，seq=partNumber 单行 BLOB），
// Complete 按 part 顺序合并走对象两段式写入（inline/chunk），成功后清理分片。

// mpuKeyPrefix 分片内容在桶分块表里的 key 前缀。
const mpuKeyPrefix = "!mpu!"

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

// UploadPart 上传一个分片：内容单行 BLOB 落桶分块表，返回该分片 MD5（hex）。
func (s *Store) UploadPart(bucket, key, uploadID string, partNumber int, r io.Reader, sizeHint int64) (string, error) {
	if partNumber < 1 || partNumber > maxParts {
		return "", &s3Error{Code: "InvalidArgument", Status: 400,
			Msg: fmt.Sprintf("Part number must be an integer between 1 and %d", maxParts)}
	}
	if _, err := s.uploadLocked(bucket, key, uploadID); err != nil {
		return "", err
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	if s.maxObject > 0 && int64(len(data)) > s.maxObject {
		return "", &s3Error{Code: "EntityTooLarge", Status: 400,
			Msg: fmt.Sprintf("Part size %d exceeds max_object_mb limit %d", len(data), s.maxObject>>20)}
	}
	if sizeHint >= 0 && sizeHint != int64(len(data)) {
		return "", &s3Error{Code: "IncompleteBody", Status: 400,
			Msg: "You did not provide the number of bytes specified by the Content-Length HTTP header"}
	}
	sum := md5.Sum(data)
	etag := hex.EncodeToString(sum[:])

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.uploadLocked(bucket, key, uploadID); err != nil {
		return "", err
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return "", err
	}
	// 同 partNumber 重传覆盖旧分片（S3 语义：最后一次上传为准）
	tbl := chunkTableFor(b.TableName)
	if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id='' AND seq=?`, tbl),
		mpuKeyPrefix+uploadID, partNumber); err != nil {
		return "", err
	}
	if _, err := s.db.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,data) VALUES(?,'',?,?)`, tbl),
		mpuKeyPrefix+uploadID, partNumber, data); err != nil {
		return "", err
	}
	if _, err := s.db.Exec(`INSERT INTO s3_parts(upload_id,part_number,size,etag,created_ts) VALUES(?,?,?,?,?)
		ON CONFLICT(upload_id,part_number) DO UPDATE SET size=excluded.size, etag=excluded.etag,
		created_ts=excluded.created_ts`, uploadID, partNumber, len(data), etag, nowTS()); err != nil {
		return "", err
	}
	s.addUsageLocked(b.Owner, bucket, "PUT", int64(len(data)), 0)
	return etag, nil
}

// CompleteMultipart 合并分片完成上传：按 part 顺序走对象两段式写入（inline/chunk），
// ETag = MD5(各 part MD5 顺序拼接二进制) + "-" + N（S3 语义）。成功后清理分片与会话。
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
	versionID := ""
	if b.Versioning {
		versionID = genVersionID()
	} else {
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTableFor(b.TableName)), key); err != nil {
			return out, false, err
		}
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, b.TableName), key); err != nil {
			return out, false, err
		}
		replaced = oldCount > 0
	}

	storage := "inline"
	if total > s.inlineMax {
		storage = "chunk"
	}
	metaJSON := "{}"
	if len(attrs.Metadata) > 0 {
		metaJSON = MarshalPolicy(attrs.Metadata)
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
	}
	_, err = tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,is_latest,delete_marker,size,etag,
		content_type,content_encoding,cache_control,content_disposition,metadata,content,storage,
		legal_hold,retention_mode,retain_until,created_ts)
		VALUES(?,?,1,0,?,?,?,?,?,?,?,?,?,0,'','',?)`, b.TableName),
		key, versionID, total, etag,
		attrs.ContentType, attrs.ContentEncoding, attrs.CacheControl, attrs.ContentDisposition,
		metaJSON, nil, storage, nowTS())
	if err != nil {
		return out, false, err
	}

	// 按 part 顺序合并内容：inline 整体装配进 content 列，chunk 滚动缓冲写分块表
	chunkTbl := chunkTableFor(b.TableName)
	loadPart := func(partNumber int, size int64) ([]byte, error) {
		var data []byte
		if err := tx.QueryRow(fmt.Sprintf(`SELECT data FROM %s WHERE key=? AND version_id='' AND seq=?`, chunkTbl),
			mpuKeyPrefix+uploadID, partNumber).Scan(&data); err != nil {
			return nil, &s3Error{Code: "InvalidPart", Status: 400,
				Msg: fmt.Sprintf("Part %d could not be found", partNumber)}
		}
		if int64(len(data)) != size {
			return nil, &s3Error{Code: "InvalidPart", Status: 400,
				Msg: fmt.Sprintf("Part %d size mismatch", partNumber)}
		}
		return data, nil
	}
	if storage == "chunk" {
		var seq int64
		buf := make([]byte, 0, s.chunkSize)
		for _, sp := range parts {
			data, lerr := loadPart(sp.PartNumber, stored[sp.PartNumber].Size)
			if lerr != nil {
				return out, false, lerr
			}
			for len(data) > 0 {
				n := int(s.chunkSize) - len(buf)
				if n > len(data) {
					n = len(data)
				}
				buf = append(buf, data[:n]...)
				data = data[n:]
				if len(buf) >= int(s.chunkSize) {
					if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,data) VALUES(?,?,?,?)`, chunkTbl),
						key, versionID, seq, buf); err != nil {
						return out, false, err
					}
					seq++
					buf = buf[:0]
				}
			}
		}
		if len(buf) > 0 {
			if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,data) VALUES(?,?,?,?)`, chunkTbl),
				key, versionID, seq, buf); err != nil {
				return out, false, err
			}
		}
	} else {
		var content []byte
		for _, sp := range parts {
			data, lerr := loadPart(sp.PartNumber, stored[sp.PartNumber].Size)
			if lerr != nil {
				return out, false, lerr
			}
			content = append(content, data...)
		}
		if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET content=? WHERE key=? AND version_id=?`, b.TableName),
			content, key, versionID); err != nil {
			return out, false, err
		}
	}

	// 清理分片内容与元数据、会话行
	if _, err := tx.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTbl), mpuKeyPrefix+uploadID); err != nil {
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
		Metadata: attrs.Metadata, Storage: storage, CreatedTS: nowTS(),
	}
	return out, replaced, nil
}

// AbortMultipart 中止分片上传：清理分片内容、元数据与会话（幂等）。
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
	if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTableFor(b.TableName)),
		mpuKeyPrefix+uploadID); err != nil {
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
		conds = append(conds, "substr(u.key,1,?)=?")
		args = append(args, len(prefix), prefix)
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

// dropBucketUploads 删桶时清理其未完成的分片上传（调用方持 writeMu）。
func (s *Store) dropBucketUploads(b Bucket) error {
	ids, err := s.db.Query(`SELECT upload_id FROM s3_uploads WHERE bucket=?`, b.Name)
	if err != nil {
		return err
	}
	var list []string
	for ids.Next() {
		var id string
		if err := ids.Scan(&id); err == nil {
			list = append(list, id)
		}
	}
	ids.Close()
	for _, id := range list {
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTableFor(b.TableName)),
			mpuKeyPrefix+id); err != nil {
			return err
		}
		if _, err := s.db.Exec(`DELETE FROM s3_parts WHERE upload_id=?`, id); err != nil {
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
