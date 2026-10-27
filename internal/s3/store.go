package s3

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/sqlite"
)

// masterKeyFile SK 加密主密钥文件（<db_dir>/glacier_s3.key，首次启动自动生成）。
// 说明：AWS SigV4 校验需要可还原的 SK（HMAC 派生签名密钥），SK 因此以
// AES-256-GCM 密文落表 + SHA-256 摘要存指纹；明文仅签发 / 轮换时返回一次。
const masterKeyFile = "glacier_s3.key"

// Bucket 桶元数据。
type Bucket struct {
	Name          string
	Owner         string
	Region        string
	TableName     string
	Versioning    bool
	ObjectLock    bool
	CreatedTS     string
	CallbackURL   string
	CallbackSecret string
	PerConnBps    int64
	PerUserBps    int64
	PerUserConns  int
	StorageBytes  int64
	ObjectCount   int64
}

// LifecycleRule 生命周期规则（按天数 + 前缀到期删除）。
type LifecycleRule struct {
	Bucket   string
	Prefix   string
	Days     int
	Enabled  bool
}

// DomainRow 自定义域名 + 独立证书行。
type DomainRow struct {
	Host     string
	Bucket   string
	CertFile string
	KeyFile  string
	CreatedTS string
}

// UsageRow 计费记账行（用户 × bucket）。
type UsageRow struct {
	Owner        string
	Bucket       string
	GetCount     int64
	PutCount     int64
	ListCount    int64
	DeleteCount  int64
	BytesIn      int64
	BytesOut     int64
	StorageBytes int64
	ObjectCount  int64
	UpdatedTS    string
}

// Store 对象存储系统库（glacier_s3.db）访问层：桶 / 密钥 / 对象 / 记账 / 生命周期。
type Store struct {
	db        *sql.DB
	dir       string
	dbFile    string // glacier_s3.db 路径（空间统计 / VACUUM 用）
	masterKey []byte
	writeMu   sync.Mutex // 对象/桶写入串行化（SQLite 单写者）

	inlineMax int64
	chunkSize int64
	maxObject             int64
	abortIncompleteDays   int

	limits       config.S3Limit
	bucketLimits map[string]config.S3Limit
	limitMu      sync.RWMutex

	usageMu  sync.Mutex
	usageBuf map[string]*usageDelta // 读写操作计数内存缓冲（v1.3.11）
	usageOps int
}

// usageDelta 单 (owner,bucket) 的操作计数增量。
type usageDelta struct {
	get, put, list, del int64
	bytesIn, bytesOut   int64
}

const (
	usageFlushOps   = 256              // 缓冲满 256 次操作批量落库
	usageFlushEvery = 5 * time.Second  // 定时落库周期（崩溃至多丢 5 秒计数）
)

// Open 打开（并初始化）对象存储系统库。
func Open(backend *sqlite.Backend, cfg *config.Config) (*Store, error) {
	db, err := backend.OpenS3DB()
	if err != nil {
		return nil, err
	}
	s := &Store{
		db:           db,
		dir:          cfg.DBDir,
		dbFile:       filepath.Join(cfg.DBDir, "sys", "glacier_s3.db"),
		inlineMax:    int64(cfg.S3.InlineMaxKB) << 10,
		chunkSize:    int64(cfg.S3.ChunkKB) << 10,
		maxObject:    int64(cfg.S3.MaxObjectMB) << 20,
		abortIncompleteDays: cfg.S3.AbortIncompleteDays,
		limits:       cfg.S3.Limits,
		bucketLimits: cfg.S3.BucketLimits,
	}
	if s.chunkSize <= 0 {
		s.chunkSize = 256 << 10
	}
	if err := s.loadMasterKey(); err != nil {
		return nil, err
	}
	if err := s.initSchema(); err != nil {
		return nil, err
	}
	// 存量桶补 manifest 列（v1.3.11：ALTER 只在建桶时执行，升级部署的旧桶表
	// 缺列会导致对象写入直接 500——启动时全量幂等补跑一次）
	for _, b := range s.ListBuckets("") {
		if err := s.ensureBucketTables(b.TableName); err != nil {
			logx.Warnf("s3: migrate bucket table %s: %v", b.TableName, err)
		}
	}
	go func() { // 记账缓冲定时落库（v1.3.11）
		t := time.NewTicker(usageFlushEvery)
		defer t.Stop()
		for range t.C {
			s.flushUsage()
		}
	}()
	return s, nil
}

// loadMasterKey 加载 / 首次生成 SK 加密主密钥（<db_dir>/glacier_s3.key）。
func (s *Store) loadMasterKey() error {
	path := filepath.Join(s.dir, masterKeyFile)
	if data, err := os.ReadFile(path); err == nil {
		key, err := hex.DecodeString(strings.TrimSpace(string(data)))
		if err != nil || len(key) != 32 {
			return fmt.Errorf("s3 master key file %s is corrupted (need 64 hex chars)", path)
		}
		s.masterKey = key
		return nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return fmt.Errorf("write s3 master key %s: %w", path, err)
	}
	s.masterKey = key
	logx.Infof("s3: generated master key for AK/SK encryption at %s", path)
	return nil
}

// initSchema 建系统表（幂等）。
func (s *Store) initSchema() error {
	// auto_vacuum=INCREMENTAL（v1.3.11）：增量空间回收的前提。新库建表前生效即启用；
	// 存量库的头标志位在下次 VACUUM（S3 VACUUM FULL）后生效。
	_, _ = s.db.Exec(`PRAGMA auto_vacuum=INCREMENTAL`)
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS s3_buckets (
  name            TEXT PRIMARY KEY,
  owner           TEXT NOT NULL,
  region          TEXT NOT NULL DEFAULT '',
  table_name      TEXT NOT NULL,
  versioning      INTEGER NOT NULL DEFAULT 0,
  object_lock     INTEGER NOT NULL DEFAULT 0,
  created_ts      TEXT NOT NULL DEFAULT '',
  callback_url    TEXT NOT NULL DEFAULT '',
  callback_secret TEXT NOT NULL DEFAULT '',
  per_conn_bps    INTEGER NOT NULL DEFAULT 0,
  per_user_bps    INTEGER NOT NULL DEFAULT 0,
  per_user_conns  INTEGER NOT NULL DEFAULT 0,
  storage_bytes   INTEGER NOT NULL DEFAULT 0,
  object_count    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS s3_domains (
  host       TEXT PRIMARY KEY,
  bucket     TEXT NOT NULL,
  cert_file  TEXT NOT NULL DEFAULT '',
  key_file   TEXT NOT NULL DEFAULT '',
  created_ts TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS s3_keys (
  ak          TEXT PRIMARY KEY,
  owner       TEXT NOT NULL,
  sk_hash     TEXT NOT NULL,
  sk_enc      TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'active',
  policy      TEXT NOT NULL DEFAULT '',
  created_ts  TEXT NOT NULL DEFAULT '',
  updated_ts  TEXT NOT NULL DEFAULT '',
  last_used_ts TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS s3_usage (
  owner        TEXT NOT NULL,
  bucket       TEXT NOT NULL,
  get_count    INTEGER NOT NULL DEFAULT 0,
  put_count    INTEGER NOT NULL DEFAULT 0,
  list_count   INTEGER NOT NULL DEFAULT 0,
  delete_count INTEGER NOT NULL DEFAULT 0,
  bytes_in     INTEGER NOT NULL DEFAULT 0,
  bytes_out    INTEGER NOT NULL DEFAULT 0,
  storage_bytes INTEGER NOT NULL DEFAULT 0,
  object_count INTEGER NOT NULL DEFAULT 0,
  updated_ts   TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (owner, bucket)
);
CREATE TABLE IF NOT EXISTS s3_lifecycle (
  bucket   TEXT NOT NULL,
  prefix   TEXT NOT NULL DEFAULT '',
  days     INTEGER NOT NULL,
  enabled  INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (bucket, prefix)
);
CREATE TABLE IF NOT EXISTS s3_policies (
  scope  TEXT PRIMARY KEY,
  policy TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS s3_uploads (
  upload_id  TEXT PRIMARY KEY,
  bucket     TEXT NOT NULL,
  key        TEXT NOT NULL,
  attrs      TEXT NOT NULL DEFAULT '{}',
  created_ts TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS s3_parts (
  upload_id   TEXT NOT NULL,
  part_number INTEGER NOT NULL,
  size        INTEGER NOT NULL DEFAULT 0,
  etag        TEXT NOT NULL DEFAULT '',
  created_ts  TEXT NOT NULL DEFAULT '',
  chunks      INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (upload_id, part_number)
);`)
	if err != nil {
		return fmt.Errorf("init s3 schema: %w", err)
	}
	// 存量库补列（幂等：重复列报错忽略）
	_, _ = s.db.Exec(`ALTER TABLE s3_parts ADD COLUMN chunks INTEGER NOT NULL DEFAULT 0`)
	return nil
}

func nowTS() string { return time.Now().Format("2006-01-02 15:04:05") }

// reBucketName S3 桶命名规则：3-63 位小写字母/数字/点/连字符，字母数字开头结尾。
var reBucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// ValidBucketName 判断桶名是否合法。
func ValidBucketName(name string) bool {
	if !reBucketName.MatchString(name) {
		return false
	}
	if strings.Contains(name, "..") || strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return false
	}
	return true
}

// tableNameFor 桶名 → 对象表名（防注入：桶名仅小写字母数字点连字符）。
func tableNameFor(bucket string) string {
	san := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		default:
			return '_'
		}
	}, bucket)
	return "obj_" + san
}

// ensureBucketTables 建桶的对���表 + 分块表（幂等）。
func (s *Store) ensureBucketTables(t string) error {
	_, err := s.db.Exec(fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
  key                TEXT NOT NULL,
  version_id         TEXT NOT NULL DEFAULT '',
  is_latest          INTEGER NOT NULL DEFAULT 1,
  delete_marker      INTEGER NOT NULL DEFAULT 0,
  size               INTEGER NOT NULL DEFAULT 0,
  etag               TEXT NOT NULL DEFAULT '',
  content_type       TEXT NOT NULL DEFAULT '',
  content_encoding   TEXT NOT NULL DEFAULT '',
  cache_control      TEXT NOT NULL DEFAULT '',
  content_disposition TEXT NOT NULL DEFAULT '',
  metadata           TEXT NOT NULL DEFAULT '{}',
  content            BLOB,
  storage            TEXT NOT NULL DEFAULT 'inline',
  manifest           TEXT NOT NULL DEFAULT '',
  legal_hold         INTEGER NOT NULL DEFAULT 0,
  retention_mode     TEXT NOT NULL DEFAULT '',
  retain_until       TEXT NOT NULL DEFAULT '',
  created_ts         TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (key, version_id)
);
CREATE INDEX IF NOT EXISTS %s_latest ON %s(key, is_latest);
CREATE TABLE IF NOT EXISTS %s (
  key        TEXT NOT NULL,
  version_id TEXT NOT NULL DEFAULT '',
  seq        INTEGER NOT NULL,
  data       BLOB NOT NULL,
  PRIMARY KEY (key, version_id, seq)
);`,
		"obj_"+strings.TrimPrefix(t, "obj_"),
		"idx_"+strings.TrimPrefix(t, "obj_"),
		"obj_"+strings.TrimPrefix(t, "obj_"),
		"chk_"+strings.TrimPrefix(t, "obj_")))
	if err != nil {
		return err
	}
	// 存量桶表补 manifest 列（幂等：重复列报错忽略；常量默认值 ALTER 为瞬时操作）
	_, _ = s.db.Exec(fmt.Sprintf(`ALTER TABLE %s ADD COLUMN manifest TEXT NOT NULL DEFAULT ''`,
		"obj_"+strings.TrimPrefix(t, "obj_")))
	return nil
}

// ---------------------------------------------------------------------------
// 桶管理
// ---------------------------------------------------------------------------

// CreateBucket 建桶（bucket 从属用户 owner，命名空间隔离）。
func (s *Store) CreateBucket(name, owner, region string, versioning, objectLock bool) (Bucket, error) {
	var b Bucket
	if !ValidBucketName(name) {
		return b, fmt.Errorf("invalid bucket name %q (3-63 chars, lowercase letters/digits/./-)", name)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	var exists string
	err := s.db.QueryRow(`SELECT name FROM s3_buckets WHERE name=?`, name).Scan(&exists)
	if err == nil {
		return b, &s3Error{Code: "BucketAlreadyOwnedByYou", Status: 409,
			Msg: "Your previous request to create the named bucket succeeded and you already own it"}
	}
	if err != nil && err != sql.ErrNoRows {
		return b, err
	}
	t := tableNameFor(name)
	// 表名冲突（桶名不同但映射相同）时追加哈希后缀
	var other string
	if err := s.db.QueryRow(`SELECT name FROM s3_buckets WHERE table_name=?`, t).Scan(&other); err == nil {
		t = t + "_" + shortHash(name)
	}
	if err := s.ensureBucketTables(t); err != nil {
		return b, err
	}
	ver, ol := 0, 0
	if versioning {
		ver = 1
	}
	if objectLock {
		ol = 1
	}
	if _, err := s.db.Exec(`INSERT INTO s3_buckets(name,owner,region,table_name,versioning,object_lock,created_ts)
		VALUES(?,?,?,?,?,?,?)`, name, owner, region, t, ver, ol, nowTS()); err != nil {
		return b, err
	}
	b = Bucket{Name: name, Owner: owner, Region: region, TableName: t,
		Versioning: versioning, ObjectLock: objectLock, CreatedTS: nowTS()}
	logx.Infof("s3: bucket %s created (owner=%s, versioning=%v, object_lock=%v)", name, owner, versioning, objectLock)
	return b, nil
}

func shortHash(s string) string {
	return hexSHA256([]byte(s))[:6]
}

// DropBucket 删桶（force=false 时桶内必须为空）。
func (s *Store) DropBucket(name string, force bool) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b, err := s.bucketLocked(name)
	if err != nil {
		return err
	}
	if !force {
		var n int
		if err := s.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM %s`, b.TableName)).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return &s3Error{Code: "BucketNotEmpty", Status: 409,
				Msg: "The bucket you tried to delete is not empty"}
		}
	}
	// 先清理未完成分片上传（分块表还在），再删表
	if err := s.dropBucketUploads(b); err != nil {
		return err
	}
	if _, err := s.db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, b.TableName)); err != nil {
		return err
	}
	if _, err := s.db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, chunkTableFor(b.TableName))); err != nil {
		return err
	}
	_, _ = s.db.Exec(`DELETE FROM s3_buckets WHERE name=?`, name)
	_, _ = s.db.Exec(`DELETE FROM s3_domains WHERE bucket=?`, name)
	_, _ = s.db.Exec(`DELETE FROM s3_lifecycle WHERE bucket=?`, name)
	_, _ = s.db.Exec(`UPDATE s3_usage SET storage_bytes=0, object_count=0 WHERE bucket=?`, name)
	logx.Infof("s3: bucket %s dropped (force=%v)", name, force)
	return nil
}

func chunkTableFor(objTable string) string {
	return "chk_" + strings.TrimPrefix(objTable, "obj_")
}

func (s *Store) bucketLocked(name string) (Bucket, error) {
	var b Bucket
	var ver, ol int
	err := s.db.QueryRow(`SELECT name,owner,region,table_name,versioning,object_lock,created_ts,
		callback_url,callback_secret,per_conn_bps,per_user_bps,per_user_conns,storage_bytes,object_count
		FROM s3_buckets WHERE name=?`, name).
		Scan(&b.Name, &b.Owner, &b.Region, &b.TableName, &ver, &ol, &b.CreatedTS,
			&b.CallbackURL, &b.CallbackSecret, &b.PerConnBps, &b.PerUserBps, &b.PerUserConns,
			&b.StorageBytes, &b.ObjectCount)
	if err == sql.ErrNoRows {
		return b, &s3Error{Code: "NoSuchBucket", Status: 404, Msg: "The specified bucket does not exist"}
	}
	if err != nil {
		return b, err
	}
	b.Versioning = ver != 0
	b.ObjectLock = ol != 0
	return b, nil
}

// Bucket 查询桶（纯读，不占写锁——WAL 下读写并行）。
func (s *Store) Bucket(name string) (Bucket, error) {
	return s.bucketLocked(name)
}

// ListBuckets 全部桶（可按 owner 过滤，空 = 全部）。
func (s *Store) ListBuckets(owner string) []Bucket {
	q := `SELECT name,owner,region,table_name,versioning,object_lock,created_ts,
		callback_url,callback_secret,per_conn_bps,per_user_bps,per_user_conns,storage_bytes,object_count
		FROM s3_buckets`
	var args []any
	if owner != "" {
		q += ` WHERE owner=?`
		args = append(args, owner)
	}
	q += ` ORDER BY name`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Bucket
	for rows.Next() {
		var b Bucket
		var ver, ol int
		if err := rows.Scan(&b.Name, &b.Owner, &b.Region, &b.TableName, &ver, &ol, &b.CreatedTS,
			&b.CallbackURL, &b.CallbackSecret, &b.PerConnBps, &b.PerUserBps, &b.PerUserConns,
			&b.StorageBytes, &b.ObjectCount); err == nil {
			b.Versioning = ver != 0
			b.ObjectLock = ol != 0
			out = append(out, b)
		}
	}
	return out
}

// UpdateBucket 更新桶设置（字段为指针语义：传 nil 不改）。
func (s *Store) UpdateBucket(name string, owner *string, versioning, objectLock *bool,
	callbackURL, callbackSecret *string, perConnBps, perUserBps *int64, perUserConns *int) (Bucket, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	b, err := s.bucketLocked(name)
	if err != nil {
		return b, err
	}
	if owner != nil {
		if _, err := s.db.Exec(`UPDATE s3_buckets SET owner=? WHERE name=?`, *owner, name); err != nil {
			return b, err
		}
		b.Owner = *owner
	}
	if versioning != nil {
		v := 0
		if *versioning {
			v = 1
		}
		if _, err := s.db.Exec(`UPDATE s3_buckets SET versioning=? WHERE name=?`, v, name); err != nil {
			return b, err
		}
		b.Versioning = *versioning
	}
	if objectLock != nil {
		v := 0
		if *objectLock {
			v = 1
		}
		if _, err := s.db.Exec(`UPDATE s3_buckets SET object_lock=? WHERE name=?`, v, name); err != nil {
			return b, err
		}
		b.ObjectLock = *objectLock
	}
	if callbackURL != nil {
		if _, err := s.db.Exec(`UPDATE s3_buckets SET callback_url=? WHERE name=?`, *callbackURL, name); err != nil {
			return b, err
		}
		b.CallbackURL = *callbackURL
	}
	if callbackSecret != nil {
		if _, err := s.db.Exec(`UPDATE s3_buckets SET callback_secret=? WHERE name=?`, *callbackSecret, name); err != nil {
			return b, err
		}
		b.CallbackSecret = *callbackSecret
	}
	if perConnBps != nil {
		if _, err := s.db.Exec(`UPDATE s3_buckets SET per_conn_bps=? WHERE name=?`, *perConnBps, name); err != nil {
			return b, err
		}
		b.PerConnBps = *perConnBps
	}
	if perUserBps != nil {
		if _, err := s.db.Exec(`UPDATE s3_buckets SET per_user_bps=? WHERE name=?`, *perUserBps, name); err != nil {
			return b, err
		}
		b.PerUserBps = *perUserBps
	}
	if perUserConns != nil {
		if _, err := s.db.Exec(`UPDATE s3_buckets SET per_user_conns=? WHERE name=?`, *perUserConns, name); err != nil {
			return b, err
		}
		b.PerUserConns = *perUserConns
	}
	return b, nil
}

// LimitFor 取某桶生效的限速配置（桶覆盖非 0 字段优先，否则全局）。
func (s *Store) LimitFor(bucket string) config.S3Limit {
	s.limitMu.RLock()
	defer s.limitMu.RUnlock()
	out := s.limits
	if ov, ok := s.bucketLimits[bucket]; ok {
		if ov.PerConnBps > 0 {
			out.PerConnBps = ov.PerConnBps
		}
		if ov.PerUserBps > 0 {
			out.PerUserBps = ov.PerUserBps
		}
		if ov.PerUserConns > 0 {
			out.PerUserConns = ov.PerUserConns
		}
	}
	// 桶表内热改覆盖（管理面 ALTER BUCKET ... LIMIT）优先
	return out
}

// EffectiveLimit 取某桶生效限速：全局 < 配置桶覆盖 < 桶行热改覆盖。
func (s *Store) EffectiveLimit(bucket string) config.S3Limit {
	lim := s.LimitFor(bucket)
	if b, err := s.Bucket(bucket); err == nil {
		if b.PerConnBps > 0 {
			lim.PerConnBps = b.PerConnBps
		}
		if b.PerUserBps > 0 {
			lim.PerUserBps = b.PerUserBps
		}
		if b.PerUserConns > 0 {
			lim.PerUserConns = b.PerUserConns
		}
	}
	return lim
}

// SetConfigLimits 管理面热改全局限速（重启失效）。
func (s *Store) SetConfigLimits(lim config.S3Limit) {
	s.limitMu.Lock()
	s.limits = lim
	s.limitMu.Unlock()
}

// ---------------------------------------------------------------------------
// 生命周期（自动删除策略）
// ---------------------------------------------------------------------------

// SetLifecycle 设置生命周期规则。
func (s *Store) SetLifecycle(bucket, prefix string, days int) error {
	if _, err := s.Bucket(bucket); err != nil {
		return err
	}
	if days <= 0 {
		return fmt.Errorf("lifecycle days must be > 0")
	}
	_, err := s.db.Exec(`INSERT INTO s3_lifecycle(bucket,prefix,days,enabled) VALUES(?,?,?,1)
		ON CONFLICT(bucket,prefix) DO UPDATE SET days=excluded.days, enabled=1`, bucket, prefix, days)
	return err
}

// ClearLifecycle 清除生命周期规则（prefix 为空 = 全清）。
func (s *Store) ClearLifecycle(bucket, prefix string) error {
	if prefix == "" {
		_, err := s.db.Exec(`DELETE FROM s3_lifecycle WHERE bucket=?`, bucket)
		return err
	}
	_, err := s.db.Exec(`DELETE FROM s3_lifecycle WHERE bucket=? AND prefix=?`, bucket, prefix)
	return err
}

// ListLifecycle 生命周期规则列表。
func (s *Store) ListLifecycle(bucket string) []LifecycleRule {
	q := `SELECT bucket,prefix,days,enabled FROM s3_lifecycle`
	var args []any
	if bucket != "" {
		q += ` WHERE bucket=?`
		args = append(args, bucket)
	}
	q += ` ORDER BY bucket,prefix`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []LifecycleRule
	for rows.Next() {
		var r LifecycleRule
		var en int
		if err := rows.Scan(&r.Bucket, &r.Prefix, &r.Days, &en); err == nil {
			r.Enabled = en != 0
			out = append(out, r)
		}
	}
	return out
}

// RunLifecycle 执行一轮生命周期删除（按天数 / 前缀到期删除全部版本），返回删除对象数。
func (s *Store) RunLifecycle() int {
	rules := s.ListLifecycle("")
	now := time.Now()
	deleted := 0
	for _, r := range rules {
		if !r.Enabled {
			continue
		}
		cutoff := now.AddDate(0, 0, -r.Days).Format("2006-01-02 15:04:05")
		b, err := s.Bucket(r.Bucket)
		if err != nil {
			continue
		}
		cond, cargs := prefixCond(r.Prefix)
		rows, err := s.db.Query(fmt.Sprintf(
			`SELECT key, MIN(created_ts) FROM %s WHERE 1=1%s AND created_ts < ? GROUP BY key`,
			b.TableName, cond), append(cargs, any(cutoff))...)
		if err != nil {
			continue
		}
		var keys []string
		for rows.Next() {
			var k, ts string
			if err := rows.Scan(&k, &ts); err == nil {
				keys = append(keys, k)
			}
		}
		rows.Close()
		for _, k := range keys {
			s.writeMu.Lock()
			err := s.deleteAllVersionsLocked(b, k)
			s.writeMu.Unlock()
			if err == nil {
				deleted++
			}
		}
	}
	if deleted > 0 {
		logx.Infof("s3: lifecycle deleted %d expired objects", deleted)
	}
	// 未完成分片上传自动回收（v1.3.5：s3.abort_incomplete_days，默认 7 天）
	if n := s.AbortStaleUploads(s.abortIncompleteDays); n > 0 {
		logx.Infof("s3: lifecycle aborted %d stale multipart uploads (>%dd)", n, s.abortIncompleteDays)
		deleted += n
	}
	return deleted
}

// deleteAllVersionsLocked 删除一个 key 的全部版本（生命周期用；调用方持 writeMu）。
func (s *Store) deleteAllVersionsLocked(b Bucket, key string) error {
	oldStorage, oldCount := s.keyAggLocked(b, key)
	if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, chunkTableFor(b.TableName)), key); err != nil {
		return err
	}
	if _, err := s.db.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key=?`, b.TableName), key); err != nil {
		return err
	}
	s.adjustAggLocked(b, key, -oldStorage, -oldCount)
	return nil
}

// ---------------------------------------------------------------------------
// 计费记账（s3_usage）
// ---------------------------------------------------------------------------

// AddUsage 记账一次操作（class: GET/PUT/LIST/DELETE）。
// v1.3.11 性能：改为内存缓冲 + 批量落库——此前每次 GET/LIST 都拿全局写锁写一行
// UPSERT，全部 S3 读流量相互串行且与写竞争；现在读路径零锁零 SQL，
// 每 256 次操作或 5 秒合并落库（写路径的 addUsageLocked 仍即时落）。
func (s *Store) AddUsage(owner, bucket, class string, bytesIn, bytesOut int64) {
	s.usageMu.Lock()
	if s.usageBuf == nil {
		s.usageBuf = make(map[string]*usageDelta)
	}
	k := owner + "\x00" + bucket
	d := s.usageBuf[k]
	if d == nil {
		d = &usageDelta{}
		s.usageBuf[k] = d
	}
	switch strings.ToUpper(class) {
	case "GET":
		d.get++
	case "PUT":
		d.put++
	case "LIST":
		d.list++
	case "DELETE":
		d.del++
	}
	d.bytesIn += bytesIn
	d.bytesOut += bytesOut
	s.usageOps++
	flush := s.usageOps >= usageFlushOps
	s.usageMu.Unlock()
	if flush {
		s.flushUsage()
	}
}

// flushUsage 缓冲记账批量落库（换出缓冲 → writeMu 下逐行 UPSERT）。
func (s *Store) flushUsage() {
	s.usageMu.Lock()
	if len(s.usageBuf) == 0 {
		s.usageMu.Unlock()
		return
	}
	buf := s.usageBuf
	s.usageBuf = make(map[string]*usageDelta)
	s.usageOps = 0
	s.usageMu.Unlock()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	for k, d := range buf {
		owner, bucket, _ := strings.Cut(k, "\x00")
		_, _ = s.db.Exec(`INSERT INTO s3_usage(owner,bucket,get_count,put_count,list_count,delete_count,
			bytes_in,bytes_out,updated_ts) VALUES(?,?,?,?,?,?,?,?,?)
			ON CONFLICT(owner,bucket) DO UPDATE SET
			get_count=get_count+excluded.get_count, put_count=put_count+excluded.put_count,
			list_count=list_count+excluded.list_count, delete_count=delete_count+excluded.delete_count,
			bytes_in=bytes_in+excluded.bytes_in, bytes_out=bytes_out+excluded.bytes_out,
			updated_ts=excluded.updated_ts`,
			owner, bucket, d.get, d.put, d.list, d.del, d.bytesIn, d.bytesOut, nowTS())
	}
}

// usageSnapshot 缓冲增量快照（Usage 合并视图用）。
func (s *Store) usageSnapshot() map[string]usageDelta {
	s.usageMu.Lock()
	defer s.usageMu.Unlock()
	out := make(map[string]usageDelta, len(s.usageBuf))
	for k, v := range s.usageBuf {
		out[k] = *v
	}
	return out
}

func (s *Store) addUsageLocked(owner, bucket, class string, bytesIn, bytesOut int64) {
	getN, putN, listN, delN := int64(0), int64(0), int64(0), int64(0)
	switch strings.ToUpper(class) {
	case "GET":
		getN = 1
	case "PUT":
		putN = 1
	case "LIST":
		listN = 1
	case "DELETE":
		delN = 1
	}
	_, _ = s.db.Exec(`INSERT INTO s3_usage(owner,bucket,get_count,put_count,list_count,delete_count,
		bytes_in,bytes_out,updated_ts) VALUES(?,?,?,?,?,?,?,?,?)
		ON CONFLICT(owner,bucket) DO UPDATE SET
		get_count=get_count+excluded.get_count, put_count=put_count+excluded.put_count,
		list_count=list_count+excluded.list_count, delete_count=delete_count+excluded.delete_count,
		bytes_in=bytes_in+excluded.bytes_in, bytes_out=bytes_out+excluded.bytes_out,
		updated_ts=excluded.updated_ts`,
		owner, bucket, getN, putN, listN, delN, bytesIn, bytesOut, nowTS())
}

// adjustAggLocked 调整桶与用量的存储字节 / 对象数（delta 可为负；调用方持 writeMu）。
func (s *Store) adjustAggLocked(b Bucket, key string, dBytes, dCount int64) {
	_, _ = s.db.Exec(`UPDATE s3_buckets SET storage_bytes=MAX(0,storage_bytes+?), object_count=MAX(0,object_count+?) WHERE name=?`,
		dBytes, dCount, b.Name)
	_, _ = s.db.Exec(`INSERT INTO s3_usage(owner,bucket,storage_bytes,object_count,updated_ts) VALUES(?,?,?,?,?)
		ON CONFLICT(owner,bucket) DO UPDATE SET
		storage_bytes=MAX(0,storage_bytes+excluded.storage_bytes),
		object_count=MAX(0,object_count+excluded.object_count),
		updated_ts=excluded.updated_ts`,
		b.Owner, b.Name, dBytes, dCount, nowTS())
}

// keyAggLocked 某 key 当前的存储字节与对象计数贡献（调用方持 writeMu）。
// v1.3.11：两条聚合合并为一条（每次对象写少一半锁内查询）。
func (s *Store) keyAggLocked(b Bucket, key string) (int64, int64) {
	var bytes, nLatest int64
	_ = s.db.QueryRow(fmt.Sprintf(
		`SELECT COALESCE(SUM(size),0), COALESCE(SUM(CASE WHEN is_latest=1 AND delete_marker=0 THEN 1 ELSE 0 END),0)
		FROM %s WHERE key=?`, b.TableName), key).Scan(&bytes, &nLatest)
	return bytes, nLatest
}

// Usage 计费统计（owner 为空 = 全部用户）。
func (s *Store) Usage(owner string) []UsageRow {
	q := `SELECT owner,bucket,get_count,put_count,list_count,delete_count,bytes_in,bytes_out,
		storage_bytes,object_count,updated_ts FROM s3_usage`
	var args []any
	if owner != "" {
		q += ` WHERE owner=?`
		args = append(args, owner)
	}
	q += ` ORDER BY owner,bucket`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []UsageRow
	for rows.Next() {
		var u UsageRow
		if err := rows.Scan(&u.Owner, &u.Bucket, &u.GetCount, &u.PutCount, &u.ListCount, &u.DeleteCount,
			&u.BytesIn, &u.BytesOut, &u.StorageBytes, &u.ObjectCount, &u.UpdatedTS); err == nil {
			out = append(out, u)
		}
	}
	// 合并尚未落库的内存缓冲（v1.3.11：读视图一致）
	if buf := s.usageSnapshot(); len(buf) > 0 {
		idx := make(map[string]int, len(out))
		for i := range out {
			idx[out[i].Owner+"\x00"+out[i].Bucket] = i
		}
		for k, d := range buf {
			o, b, _ := strings.Cut(k, "\x00")
			if owner != "" && o != owner {
				continue
			}
			if i, ok := idx[k]; ok {
				u := &out[i]
				u.GetCount += d.get
				u.PutCount += d.put
				u.ListCount += d.list
				u.DeleteCount += d.del
				u.BytesIn += d.bytesIn
				u.BytesOut += d.bytesOut
			} else {
				out = append(out, UsageRow{Owner: o, Bucket: b,
					GetCount: d.get, PutCount: d.put, ListCount: d.list, DeleteCount: d.del,
					BytesIn: d.bytesIn, BytesOut: d.bytesOut, UpdatedTS: nowTS()})
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Owner != out[j].Owner {
				return out[i].Owner < out[j].Owner
			}
			return out[i].Bucket < out[j].Bucket
		})
	}
	return out
}

// UsageCSV 导出计费 CSV（供计费系统拉取）。
func (s *Store) UsageCSV(owner string) string {
	var b strings.Builder
	b.WriteString("owner,bucket,get_count,put_count,list_count,delete_count,bytes_in,bytes_out,storage_bytes,object_count,updated_ts\n")
	for _, u := range s.Usage(owner) {
		fmt.Fprintf(&b, "%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%s\n",
			u.Owner, u.Bucket, u.GetCount, u.PutCount, u.ListCount, u.DeleteCount,
			u.BytesIn, u.BytesOut, u.StorageBytes, u.ObjectCount, u.UpdatedTS)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// 自定义域名 + 独立证书
// ---------------------------------------------------------------------------

// AddDomain 注册 bucket 自定义域名（certFile/keyFile 为空 = 用自签发证书文件）。
func (s *Store) AddDomain(host, bucket, certFile, keyFile string) error {
	if host == "" || strings.ContainsAny(host, "/: ") {
		return fmt.Errorf("invalid domain %q", host)
	}
	if _, err := s.Bucket(bucket); err != nil {
		return err
	}
	_, err := s.db.Exec(`INSERT INTO s3_domains(host,bucket,cert_file,key_file,created_ts) VALUES(?,?,?,?,?)
		ON CONFLICT(host) DO UPDATE SET bucket=excluded.bucket, cert_file=excluded.cert_file,
		key_file=excluded.key_file`, host, bucket, certFile, keyFile, nowTS())
	return err
}

// DropDomain 移除自定义域名。
func (s *Store) DropDomain(host string) error {
	_, err := s.db.Exec(`DELETE FROM s3_domains WHERE host=?`, host)
	return err
}

// Domains 全部自定义域名。
func (s *Store) Domains() []DomainRow {
	rows, err := s.db.Query(`SELECT host,bucket,cert_file,key_file,created_ts FROM s3_domains ORDER BY host`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []DomainRow
	for rows.Next() {
		var d DomainRow
		if err := rows.Scan(&d.Host, &d.Bucket, &d.CertFile, &d.KeyFile, &d.CreatedTS); err == nil {
			out = append(out, d)
		}
	}
	return out
}

// DomainFor 查询域名映射。
func (s *Store) DomainFor(host string) (DomainRow, bool) {
	var d DomainRow
	err := s.db.QueryRow(`SELECT host,bucket,cert_file,key_file,created_ts FROM s3_domains WHERE host=?`, host).
		Scan(&d.Host, &d.Bucket, &d.CertFile, &d.KeyFile, &d.CreatedTS)
	if err != nil {
		return d, false
	}
	return d, true
}

// MarshalPolicy 序列化策略 JSON（防御性封装）。
func MarshalPolicy(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// ---------------------------------------------------------------------------
// 空间回收（v1.3.11）：SQLite 删除只挂 freelist 不缩文件，这里提供
// 增量回收（免长锁）与全量重建（排他）两级手段 + 后台阈值自动回收。
// ---------------------------------------------------------------------------

// SpaceStats 系统库空间统计。
type SpaceStats struct {
	PageCount     int64
	FreelistCount int64
	PageSize      int64
	FreeBytes     int64
	FileBytes     int64
	AutoVacuum    int64 // 0=none 1=full 2=incremental
}

// SpaceStats 读取当前空间统计（纯 PRAGMA 读，不锁库）。
func (s *Store) SpaceStats() SpaceStats {
	var st SpaceStats
	_ = s.db.QueryRow(`PRAGMA page_count`).Scan(&st.PageCount)
	_ = s.db.QueryRow(`PRAGMA freelist_count`).Scan(&st.FreelistCount)
	_ = s.db.QueryRow(`PRAGMA page_size`).Scan(&st.PageSize)
	_ = s.db.QueryRow(`PRAGMA auto_vacuum`).Scan(&st.AutoVacuum)
	st.FreeBytes = st.FreelistCount * st.PageSize
	if fi, err := os.Stat(s.dbFile); err == nil {
		st.FileBytes = fi.Size()
	}
	return st
}

// VacuumIncremental 增量回收：每次 PRAGMA incremental_vacuum(N) 是一个独立的
// 短写事务（默认每步 256 页 ≈ 1MB），WAL 下读完全不受影响、写仅在单步期间排队。
// 循环执行直到空闲页清空或达到单轮上限（默认 1GB）。返回释放字节数。
// 前提：auto_vacuum=INCREMENTAL（新建库默认启用；存量库先跑一次 S3 VACUUM FULL）。
func (s *Store) VacuumIncremental(maxBytes int64) int64 {
	if maxBytes <= 0 {
		maxBytes = 1 << 30 // 单轮默认回收至多 1GB
	}
	const stepPages = 256
	freed := int64(0)
	for freed < maxBytes {
		var before int64
		_ = s.db.QueryRow(`PRAGMA freelist_count`).Scan(&before)
		if before == 0 {
			break
		}
		if _, err := s.db.Exec(fmt.Sprintf(`PRAGMA incremental_vacuum(%d)`, stepPages)); err != nil {
			logx.Warnf("s3: incremental_vacuum failed: %v", err)
			break
		}
		var after int64
		_ = s.db.QueryRow(`PRAGMA freelist_count`).Scan(&after)
		if after >= before { // 未启用增量模式（auto_vacuum=0）等场景：不再空转
			break
		}
		var ps int64
		_ = s.db.QueryRow(`PRAGMA page_size`).Scan(&ps)
		freed += (before - after) * ps
	}
	if freed > 0 {
		logx.Infof("s3: incremental vacuum reclaimed %d bytes", freed)
	}
	return freed
}

// VacuumFull 全量重建（排他）：VACUUM 重写整个库文件、期间锁库（时长与库大小
// 成正比），仅在维护窗口执行；顺带把存量库切换到 auto_vacuum=INCREMENTAL，
// 之后日常空间释放都可走免长锁的增量回收。
func (s *Store) VacuumFull() error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.db.Exec(`PRAGMA auto_vacuum=INCREMENTAL`); err != nil {
		return err
	}
	if _, err := s.db.Exec(`VACUUM`); err != nil {
		return err
	}
	logx.Infof("s3: full VACUUM done (glacier_s3.db rebuilt, auto_vacuum=INCREMENTAL)")
	return nil
}

// AutoReclaim 后台阈值自动回收（生命周期扫描周期内调用一次）：
// 空闲空间超过 [128MB 且占文件 10%] 才动手，避免频繁抖动；
// 走增量路径（短事务分步），读流量与常规写基本无感。
func (s *Store) AutoReclaim() {
	st := s.SpaceStats()
	if st.AutoVacuum != 2 { // 未启用增量模式（存量库）：交给管理员 S3 VACUUM FULL 一次性切换
		return
	}
	if st.FreeBytes < 128<<20 || st.FreeBytes*10 < st.FileBytes {
		return
	}
	s.VacuumIncremental(1 << 30)
}
