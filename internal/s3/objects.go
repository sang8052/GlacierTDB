package s3

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
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
	LegalHold         bool
	RetentionMode     string // '' | GOVERNANCE | COMPLIANCE
	RetainUntil       string
	CreatedTS         string
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

// PutObject 写对象（内容先落临时文件做哈希校验，再事务写入内联 / 分块表）。
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

	// 暂存：小对象内存、大对象临时文件（<db_dir>/.s3tmp）
	tmpDir := filepath.Join(s.dir, ".s3tmp")
	_ = os.MkdirAll(tmpDir, 0o700)
	tmp, err := os.CreateTemp(tmpDir, "put-*")
	if err != nil {
		return out, false, err
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	md5h := md5.New()
	size, err := io.Copy(io.MultiWriter(tmp, md5h), r)
	if err != nil {
		return out, false, err
	}
	if s.maxObject > 0 && size > s.maxObject {
		return out, false, &s3Error{Code: "EntityTooLarge", Status: 400,
			Msg: fmt.Sprintf("Object size %d exceeds max_object_mb limit %d", size, s.maxObject>>20)}
	}
	if sizeHint >= 0 && sizeHint != size {
		// Content-Length 与实际不符（协议层面 Go 已保证一致；防御）
		return out, false, &s3Error{Code: "IncompleteBody", Status: 400,
			Msg: "You did not provide the number of bytes specified by the Content-Length HTTP header"}
	}
	etag := hex.EncodeToString(md5h.Sum(nil))

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	oldStorage, oldCount := s.keyAggLocked(b, key)
	versionID := ""
	if b.Versioning {
		versionID = genVersionID()
	} else {
		// 清理旧版本（含分块）
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTableFor(b.TableName)), key); err != nil {
			return out, false, err
		}
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, b.TableName), key); err != nil {
			return out, false, err
		}
		replaced = oldCount > 0
	}

	storage := "inline"
	var content []byte
	if size <= s.inlineMax {
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return out, false, err
		}
		content, err = io.ReadAll(tmp)
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
		key, versionID, size, etag,
		attrs.ContentType, attrs.ContentEncoding, attrs.CacheControl, attrs.ContentDisposition,
		metaJSON, content, storage, nowTS())
	if err != nil {
		return out, false, err
	}

	if storage == "chunk" {
		chunk := make([]byte, s.chunkSize)
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			return out, false, err
		}
		for seq := int64(0); ; seq++ {
			n, rerr := io.ReadFull(tmp, chunk)
			if n > 0 {
				if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,data) VALUES(?,?,?,?)`,
					chunkTableFor(b.TableName)), key, versionID, seq, chunk[:n]); err != nil {
					return out, false, err
				}
			}
			if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
				break
			}
			if rerr != nil {
				return out, false, rerr
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return out, false, err
	}

	newStorage, newCount := s.keyAggLocked(b, key)
	s.adjustAggLocked(b, key, newStorage-oldStorage, newCount-oldCount)
	s.addUsageLocked(b.Owner, bucket, "PUT", size, 0)

	out = ObjectMeta{
		Key: key, VersionID: versionID, IsLatest: true, Size: size, ETag: etag,
		ContentType: attrs.ContentType, ContentEncoding: attrs.ContentEncoding,
		CacheControl: attrs.CacheControl, ContentDisposition: attrs.ContentDisposition,
		Metadata: attrs.Metadata, Storage: storage, CreatedTS: nowTS(),
	}
	return out, replaced, nil
}

// scanObject 扫描一行对象元数据（不含 content 列，内容单独取）。
func scanObject(rows *sql.Rows) (ObjectMeta, error) {
	var m ObjectMeta
	var isLatest, delMark, legalHold int
	var metaJSON, storage string
	err := rows.Scan(&m.Key, &m.VersionID, &isLatest, &delMark, &m.Size, &m.ETag,
		&m.ContentType, &m.ContentEncoding, &m.CacheControl, &m.ContentDisposition,
		&metaJSON, &storage, &legalHold, &m.RetentionMode, &m.RetainUntil, &m.CreatedTS)
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
	cache_control,content_disposition,metadata,storage,legal_hold,retention_mode,retain_until,created_ts`

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

// contentReader 对象内容读取器（内联字节 / 分块惰性拉取）。
type contentReader struct {
	store *Store
	tbl   string
	key   string
	ver   string
	seq   int64
	buf   []byte
	off   int
	done  bool
	inline bool
}

func (c *contentReader) Read(p []byte) (int, error) {
	if c.inline {
		n := copy(p, c.buf[c.off:])
		c.off += n
		if c.off >= len(c.buf) && n == 0 {
			return 0, io.EOF
		}
		return n, nil
	}
	for len(c.buf)-c.off == 0 && !c.done {
		var seq int64
		var data []byte
		err := c.store.db.QueryRow(fmt.Sprintf(
			`SELECT seq,data FROM %s WHERE key=? AND version_id=? AND seq>? ORDER BY seq LIMIT 1`,
			c.tbl), c.key, c.ver, c.seq).Scan(&seq, &data)
		if err == sql.ErrNoRows {
			c.done = true
			break
		}
		if err != nil {
			return 0, err
		}
		c.seq = seq
		c.buf = data
		c.off = 0
	}
	if len(c.buf)-c.off == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.buf[c.off:])
	c.off += n
	return n, nil
}

func (c *contentReader) Close() error { return nil }

// GetObject 取对象内容（返回元数据 + 流式读取器）。
func (s *Store) GetObject(bucket, key, versionID string) (ObjectMeta, io.ReadCloser, error) {
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
	// 内容
	if m.Storage == "inline" {
		var content []byte
		if err := s.db.QueryRow(fmt.Sprintf(`SELECT content FROM %s WHERE key=? AND version_id=?`, b.TableName),
			key, m.VersionID).Scan(&content); err != nil {
			return ObjectMeta{}, nil, err
		}
		cr := &contentReader{inline: true, buf: content}
		return m, cr, nil
	}
	cr := &contentReader{store: s, tbl: chunkTableFor(b.TableName), key: key, ver: m.VersionID, seq: -1}
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

	oldStorage, oldCount := s.keyAggLocked(b, key)

	del := ObjectMeta{Key: key, VersionID: versionID}
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
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=?`, chunkTableFor(b.TableName)), key, versionID); err != nil {
			return del, err
		}
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=? AND version_id=?`, b.TableName), key, versionID); err != nil {
			return del, err
		}
		// 若删的是最新版本，版本链里补最新标记
		var n int
		_ = s.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE key=? AND is_latest=1`, b.TableName), key).Scan(&n)
		if n == 0 {
			_, _ = s.db.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=1 WHERE key=? AND version_id=
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
		if _, err := s.db.Exec(fmt.Sprintf(`UPDATE %s SET is_latest=0 WHERE key=? AND is_latest=1`, b.TableName), key); err != nil {
			return del, err
		}
		if _, err := s.db.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,is_latest,delete_marker,size,etag,
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
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTableFor(b.TableName)), key); err != nil {
			return del, err
		}
		if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, b.TableName), key); err != nil {
			return del, err
		}
	}

	newStorage, newCount := s.keyAggLocked(b, key)
	s.adjustAggLocked(b, key, newStorage-oldStorage, newCount-oldCount)
	s.addUsageLocked(b.Owner, bucket, "DELETE", 0, 0)
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
	if prefix != "" {
		q += ` AND substr(key,1,?)=?`
		args = append(args, len(prefix), prefix)
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
	if prefix != "" {
		q += ` AND substr(key,1,?)=?`
		args = append(args, len(prefix), prefix)
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
	if prefix != "" {
		q += ` AND substr(key,1,?)=?`
		args = append(args, len(prefix), prefix)
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
func (s *Store) CopyObject(srcBucket, srcKey, srcVersion, dstBucket, dstKey string, attrs ObjectAttrs) (ObjectMeta, error) {
	m, data, err := s.GetObjectBytes(srcBucket, srcKey, srcVersion)
	if err != nil {
		return ObjectMeta{}, err
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
	out, _, err := s.PutObject(dstBucket, dstKey, bytes.NewReader(data), int64(len(data)), attrs)
	return out, err
}
