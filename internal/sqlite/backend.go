// Package sqlite 管理后端 SQLite：一个库 = 目录下一个 <db>.db 文件；
// 每客户端连接一个专属物理连接（MaxOpenConns=1），USE 切库即换文件；
// 全局写互斥保证写串行；显式 BEGIN IMMEDIATE 持锁至 COMMIT/ROLLBACK/断开。
package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ncruces/go-sqlite3/driver"
	_ "github.com/ncruces/go-sqlite3/vfs/adiantum"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// authMetaFile 管理面改密的持久化文件（位于 db 目录）。
const authMetaFile = "glacier.json"

// 合法库名：字母数字下划线连字符（防路径穿越）
var reDBName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Backend 是所有会话共享的句柄。
type Backend struct {
	dir     string
	dsnOpts string
	maxRows int64
	slowMs  int64

	dbLocksMu sync.Mutex
	dbLocks   map[string]*sync.Mutex // 每库一把写锁（惰性创建）：单库单线程，多库并行

	globalSem  *dynSem // 全局线程数闸门（读写共享）
	readGlobal *dynSem // 全局默认：单库读并发上限
	readSemsMu sync.Mutex
	readSemTab map[string]*dynSem // 按库覆盖的读信号量（惰性创建）
	readOvMu   sync.RWMutex
	readOv     map[string]int // 库名 → 覆盖值（0 = 用全局默认）

	sessionSeq atomic.Uint64

	authMu       sync.RWMutex
	authPassword string // 当前生效密码（可被管理面热改）

	encrypt    bool
	passphrase string
}

// Session 是一个客户端连接专属的 SQLite 会话（独占一个物理连接，绑定当前库）。
type Session struct {
	ID        uint64
	User      string
	Remote    string
	db        *sql.DB
	backend   *Backend
	currentDB string
	InTx      bool      // 处于显式事务
	holdsLock bool      // 当前持有本库写锁
	writeMu   *sync.Mutex // AcquireWrite 时记录的锁对象
	startedAt time.Time
}

// NewBackend 创建后端并自检（目录存在/可写、默认库可用）。
func NewBackend(cfg *config.Config) (*Backend, error) {
	b := &Backend{
		dir:          cfg.DBDir,
		maxRows:      cfg.SQLite.MaxResultRows,
		slowMs:       cfg.Log.SlowQueryMs,
		authPassword: cfg.Auth.Password,
		dbLocks:      make(map[string]*sync.Mutex),
		globalSem:    newDynSem(cfg.Concurrency.GlobalThreads),
		readGlobal:   newDynSem(cfg.Concurrency.PerDBReadThreads),
		readSemTab:   make(map[string]*dynSem),
		readOv:       make(map[string]int),
	}
	for db, n := range cfg.Concurrency.PerDBReadThreadsOverride {
		b.readOv[db] = n
	}
	// 注意顺序：busy_timeout 必须最先——否则 journal_mode 撞上活动写锁时
	// 无超时保护立即 SQLITE_BUSY，新连接握手表现为「database is locked」
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", cfg.SQLite.BusyTimeoutMs))
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", fmt.Sprintf("synchronous(%d)", cfg.SQLite.SynchronousLevel()))
	q.Add("_pragma", fmt.Sprintf("cache_size(-%d)", cfg.SQLite.CacheSizeKB))
	b.dsnOpts = q.Encode()
	b.encrypt = cfg.Security.Encrypt
	b.passphrase = cfg.Security.Passphrase
	if b.encrypt && b.passphrase == "" {
		return nil, fmt.Errorf("security.encrypt 开启时必须配置 security.passphrase")
	}
	if b.encrypt {
		if bad, err := b.detectPlaintextDBs(); err == nil && len(bad) > 0 {
			return nil, fmt.Errorf("目录中存在未加密的库 %v；加密模式要求空目录或已加密库（新库自动加密，存量迁移工具见 FEATURE.md）", bad)
		}
	}

	st, err := os.Stat(b.dir)
	if err != nil || !st.IsDir() {
		return nil, fmt.Errorf("db dir %s 不存在或不是目录", b.dir)
	}
	b.loadAuthOverride() // 管理面改过的密码优先于配置文件
	b.authMu.RLock()
	cfg.Auth.Password = b.authPassword
	b.authMu.RUnlock()
	// 确保默认库存在（首次启动自动创建）
	if err := b.CreateDatabase(cfg.DefaultDB); err != nil {
		return nil, fmt.Errorf("init default db %s: %w", cfg.DefaultDB, err)
	}
	// 自检：默认库可读写
	s, err := b.OpenSession("startup", "selfcheck", cfg.DefaultDB)
	if err != nil {
		return nil, err
	}
	if _, _, err := s.Exec("SELECT 1"); err != nil {
		s.Close()
		return nil, fmt.Errorf("sqlite selfcheck failed: %w", err)
	}
	s.Close()
	return b, nil
}

func (b *Backend) fileFor(db string) string { return filepath.Join(b.dir, db+".db") }

func (b *Backend) openFile(file string) (*sql.DB, error) {
	// driver.Open（带连接初始化钩子）：每个物理连接注册 MD5/SHA/LEFT/RIGHT/
	// SUBSTRING_INDEX/DATEDIFF/TIMESTAMPDIFF/DATE_FORMAT 等 MySQL 兼容标量函数
	db, err := driver.Open(b.dsnFor(file), registerCompatFuncs)
	if err != nil {
		return nil, err
	}
	// 关键：物理连接数固定为 1，保证 BEGIN/COMMIT 不漂移
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	return db, nil
}

// OpenSession 为一个客户端连接建立专属会话（连到 defaultDB）。
func (b *Backend) OpenSession(user, remote, defaultDB string) (*Session, error) {
	if !reDBName.MatchString(defaultDB) {
		return nil, fmt.Errorf("invalid database name %q", defaultDB)
	}
	if _, err := os.Stat(b.fileFor(defaultDB)); err != nil {
		return nil, ErrDatabaseNotFound(defaultDB)
	}
	db, err := b.openFileRetry(b.fileFor(defaultDB), 2)
	if err != nil {
		return nil, err
	}
	s := &Session{
		ID:        b.sessionSeq.Add(1),
		User:      user,
		Remote:    remote,
		db:        db,
		backend:   b,
		currentDB: defaultDB,
		startedAt: time.Now(),
	}
	if _, _, err := s.Exec("SELECT 1"); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// ---------------------------------------------------------------------------
// 多库管理
// ---------------------------------------------------------------------------

type dbErr struct{ msg string }

func (e *dbErr) Error() string { return e.msg }

// ErrDatabaseNotFound 1049 Unknown database
func ErrDatabaseNotFound(name string) error {
	return &dbErr{msg: fmt.Sprintf("Unknown database '%s'", name)}
}

// IsDBNotFound 判断是否库不存在错误。
func IsDBNotFound(err error) bool {
	e, ok := err.(*dbErr)
	return ok && strings.HasPrefix(e.msg, "Unknown database")
}

// CurrentDB 返回当前库名。
func (s *Session) CurrentDB() string { return s.currentDB }

// CurrentUser 返回连接用户名（固定 glacier）。
func (s *Session) CurrentUser() string { return s.User }

// ListDatabases 列出所有库。
func (s *Session) ListDatabases() []string { return s.backend.ListDatabases() }

// CreateDatabase 创建库。
func (s *Session) CreateDatabase(name string) error { return s.backend.CreateDatabase(name) }

// DropDatabase 删除库。
func (s *Session) DropDatabase(name string) error { return s.backend.DropDatabase(name) }

// ListDatabases 列出目录下所有库（.db 文件名）。
func (b *Backend) ListDatabases() []string {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".db") {
			out = append(out, strings.TrimSuffix(name, ".db"))
		}
	}
	sort.Strings(out)
	return out
}

// CreateDatabase 创建一个库（空 .db 文件，应用 WAL）。
// glacier 为系统内置库名（管理面别名），不允许创建。
func (b *Backend) CreateDatabase(name string) error {
	if strings.EqualFold(name, "glacier") {
		return fmt.Errorf("database name 'glacier' is reserved for the built-in admin database")
	}
	if !reDBName.MatchString(name) {
		return fmt.Errorf("invalid database name %q", name)
	}
	file := b.fileFor(name)
	if _, err := os.Stat(file); err == nil {
		return nil // 已存在
	}
	db, err := b.openFile(file)
	if err != nil {
		return err
	}
	if _, err := db.Exec("SELECT 1"); err != nil { // 触发建文件 + WAL
		db.Close()
		_ = os.Remove(file)
		return err
	}
	db.Close()
	return nil
}

// DropDatabase 删除一个库（.db + -wal + -shm）。
func (b *Backend) DropDatabase(name string) error {
	if !reDBName.MatchString(name) {
		return fmt.Errorf("invalid database name %q", name)
	}
	file := b.fileFor(name)
	for _, f := range []string{file, file + "-wal", file + "-shm"} {
		if err := os.Remove(f); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Use 切换当前库（事务中禁止，MySQL 报 1192）。
func (s *Session) Use(dbName string) error {
	if dbName == s.currentDB {
		return nil
	}
	if s.InTx {
		return fmt.Errorf("can't execute the given command because you have active queries in this session (transaction open)")
	}
	if !reDBName.MatchString(dbName) {
		return ErrDatabaseNotFound(dbName)
	}
	if _, err := os.Stat(s.backend.fileFor(dbName)); err != nil {
		return ErrDatabaseNotFound(dbName)
	}
	ndb, err := s.backend.openFileRetry(s.backend.fileFor(dbName), 2)
	if err != nil {
		return err
	}
	if _, err := ndb.Exec("SELECT 1"); err != nil {
		ndb.Close()
		return err
	}
	s.db.Close()
	s.db = ndb
	s.currentDB = dbName
	return nil
}

// ---------------------------------------------------------------------------
// 查询执行
// ---------------------------------------------------------------------------

// Query 执行只读查询（受 全局闸门 + 本库读并发 限流，maxRows 行数保护）。
func (s *Session) Query(q string) ([]string, [][]any, error) {
	s.backend.globalSem.Acquire()
	defer s.backend.globalSem.Release()
	rs := s.backend.readSemFor(s.currentDB)
	rs.Acquire()
	defer rs.Release()
	rows, err := s.db.Query(q)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		out = append(out, vals)
		if int64(len(out)) > s.backend.maxRows {
			return nil, nil, fmt.Errorf("result exceeds max_result_rows(%d); add LIMIT or raise sqlite.max_result_rows", s.backend.maxRows)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return cols, out, nil
}

// Exec 执行写语句（全局闸门限流；库级写锁由调用方管理），返回 lastInsertID / affectedRows。
func (s *Session) Exec(q string) (int64, int64, error) {
	s.backend.globalSem.Acquire()
	defer s.backend.globalSem.Release()
	res, err := s.db.Exec(q)
	if err != nil {
		return 0, 0, err
	}
	lastID, _ := res.LastInsertId()
	affected, _ := res.RowsAffected()
	return lastID, affected, nil
}

// AcquireWrite 获取当前库的写锁（单库单线程；多库写入并行）。
func (s *Session) AcquireWrite() {
	m := s.backend.writeLockFor(s.currentDB)
	m.Lock()
	s.writeMu = m
	s.holdsLock = true
}

// ReleaseWrite 释放写锁（幂等）。
func (s *Session) ReleaseWrite() {
	if s.holdsLock {
		s.holdsLock = false
		m := s.writeMu
		s.writeMu = nil
		m.Unlock()
	}
}

func (b *Backend) writeLockFor(db string) *sync.Mutex {
	b.dbLocksMu.Lock()
	defer b.dbLocksMu.Unlock()
	if m, ok := b.dbLocks[db]; ok {
		return m
	}
	m := &sync.Mutex{}
	b.dbLocks[db] = m
	return m
}

// readSemFor 取某库的读信号量（有覆盖用覆盖，否则全局默认）。
func (b *Backend) readSemFor(db string) *dynSem {
	b.readOvMu.RLock()
	ov, hasOv := b.readOv[db]
	b.readOvMu.RUnlock()
	if hasOv {
		b.readSemsMu.Lock()
		defer b.readSemsMu.Unlock()
		if s, ok := b.readSemTab[db]; ok {
			return s
		}
		s := newDynSem(ov)
		b.readSemTab[db] = s
		return s
	}
	return b.readGlobal
}

// SlowMs 返回慢查询阈值。
func (s *Session) SlowMs() int64 { return s.backend.slowMs }

// Close 关闭会话：若在显式事务中先回滚，并确保写锁释放（防泄漏）。
func (s *Session) Close() {
	if s.InTx {
		if _, _, err := s.Exec("ROLLBACK"); err != nil {
			logx.Warnf("session %d rollback-on-close failed: %v", s.ID, err)
		}
		s.InTx = false
	}
	s.ReleaseWrite()
	if s.db != nil {
		s.db.Close()
	}
}

// GetAuthPassword 返回当前生效密码。
func (b *Backend) GetAuthPassword() string {
	b.authMu.RLock()
	defer b.authMu.RUnlock()
	return b.authPassword
}

// SetAuthPassword 热改密码并持久化到 <db_dir>/glacier.json。
func (b *Backend) SetAuthPassword(newPwd string) error {
	file := filepath.Join(b.dir, authMetaFile)
	data := []byte(fmt.Sprintf(`{"password": %q}`, newPwd))
	tmp := file + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, file); err != nil {
		return err
	}
	b.authMu.Lock()
	b.authPassword = newPwd
	b.authMu.Unlock()
	return nil
}

func (b *Backend) loadAuthOverride() {
	data, err := os.ReadFile(filepath.Join(b.dir, authMetaFile))
	if err != nil {
		return
	}
	var m struct {
		Password string `json:"password"`
	}
	if err := json.Unmarshal(data, &m); err == nil && m.Password != "" {
		b.authMu.Lock()
		b.authPassword = m.Password
		b.authMu.Unlock()
	}
}

// HoldsWriteLock 返回会话当前是否持有全局写锁（管理面展示用）。
func (s *Session) HoldsWriteLock() bool { return s.holdsLock }

// ---------------------------------------------------------------------------
// 动态信号量：容量可热调（管理面 SET，重启失效回到配置文件值）
// ---------------------------------------------------------------------------

type dynSem struct {
	mu   sync.Mutex
	cond *sync.Cond
	cur  int
	max  int // <=0 表示不限
}

func newDynSem(max int) *dynSem {
	s := &dynSem{max: max}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *dynSem) Acquire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.max > 0 && s.cur >= s.max {
		s.cond.Wait()
	}
	s.cur++
}

func (s *dynSem) Release() {
	s.mu.Lock()
	s.cur--
	s.cond.Signal()
	s.mu.Unlock()
}

func (s *dynSem) SetMax(n int) {
	s.mu.Lock()
	s.max = n
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *dynSem) Max() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

// SetGlobalThreads 热改全局线程数（<=0 不限）。
func (b *Backend) SetGlobalThreads(n int) { b.globalSem.SetMax(n) }

// SetPerDBReadThreads 热改全局默认单库读线程数（<=0 不限）。
func (b *Backend) SetPerDBReadThreads(n int) { b.readGlobal.SetMax(n) }

// SetDBReadThreads 设置/清除单库读线程覆盖（n<=0 删除覆盖，走全局默认）。
func (b *Backend) SetDBReadThreads(db string, n int) {
	b.readOvMu.Lock()
	if n <= 0 {
		delete(b.readOv, db)
	} else {
		b.readOv[db] = n
	}
	b.readOvMu.Unlock()
	if n > 0 {
		b.readSemsMu.Lock()
		if s, ok := b.readSemTab[db]; ok {
			s.SetMax(n)
		}
		b.readSemsMu.Unlock()
	}
}

// ConcurrencyInfo 返回当前并发配置（供 server_config 展示）。
func (b *Backend) ConcurrencyInfo() map[string]string {
	out := map[string]string{
		"concurrency.global_threads":      fmt.Sprintf("%d", b.globalSem.Max()),
		"concurrency.per_db_read_threads": fmt.Sprintf("%d", b.readGlobal.Max()),
	}
	b.readOvMu.RLock()
	for db, n := range b.readOv {
		out["concurrency.db_read_threads."+db] = fmt.Sprintf("%d", n)
	}
	b.readOvMu.RUnlock()
	return out
}

// openFileRetry 打开库文件（撞上活动写锁时退避重试）。
func (b *Backend) openFileRetry(file string, attempts int) (*sql.DB, error) {
	var db *sql.DB
	var err error
	for i := 0; i < attempts; i++ {
		db, err = b.openFile(file)
		if err == nil {
			var probe *sql.DB = db
			if _, perr := probe.Exec("SELECT 1"); perr == nil {
				return db, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return b.openFile(file)
}

// dsnFor 生成 SQLite 官方 URI 形式的 DSN（跨平台）：
//   POSIX 绝对路径  /data/x.db    → file:///data/x.db
//   Windows 绝对路径 H:/dir/x.db  → file:///H:/dir/x.db（反斜杠已 ToSlash）
//   相对路径        data/x.db     → file:data/x.db
// 路径按 RFC3986 百分号编码（空格/中文/特殊字符），避免 url.URL 把盘符当 host。
func (b *Backend) dsnFor(file string) string {
	// 无条件把反斜杠转正斜杠：filepath.ToSlash 仅在 Windows 运行时生效，
	// 而 DSN 生成必须与目标平台无关（Linux 宿主上生成 Windows 路径也要正确）
	p := strings.ReplaceAll(filepath.Clean(file), "\\", "/")
	var sb strings.Builder
	for _, r := range p {
		switch {
		case r == '/' || r == ':' || r == '-' || r == '_' || r == '.' || r == '~',
			r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			sb.WriteRune(r)
		default:
			for _, c := range []byte(string(r)) {
				fmt.Fprintf(&sb, "%%%02X", c)
			}
		}
	}
	// 统一使用无 authority 的 file: 前缀：
	//   /data/x.db  → file:/data/x.db     （SQLite: file:[//authority]path，// 可省）
	//   H:/x.db     → file:H:/x.db        （Windows API 原生接受正斜杠盘符路径）
	// 带三斜杠的 file:///H:/x 在 wazero VFS 下会被解析成 POSIX 路径 "/H:/..."，
	// 报 CreateFile /H:: 语法错误，故禁用 authority 形式。
	if b.encrypt {
		// textkey 必须先于其它 pragma（连接后立即生效）
		kv := url.Values{}
		kv.Add("vfs", "adiantum")
		kv.Add("_pragma", fmt.Sprintf("textkey('%s')", b.passphrase))
		return "file:" + sb.String() + "?" + kv.Encode() + "&" + b.dsnOpts
	}
	return "file:" + sb.String() + "?" + b.dsnOpts
}

// detectPlaintextDBs 找出目录中的明文库（文件头 == "SQLite format 3\x00"）。
func (b *Backend) detectPlaintextDBs() ([]string, error) {
	entries, err := os.ReadDir(b.dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".db") {
			continue
		}
		f, err := os.Open(filepath.Join(b.dir, name))
		if err != nil {
			continue
		}
		hdr := make([]byte, 16)
		n, _ := f.Read(hdr)
		f.Close()
		if n == 16 && string(hdr) == "SQLite format 3\x00" {
			out = append(out, name)
		}
	}
	return out, nil
}

// VacuumDB 对指定库执行 VACUUM：获取该库写锁并长持至完成（执行期间该库所有写入阻塞）。
// 返回（耗时, 释放字节数）。
func (b *Backend) VacuumDB(db string) (time.Duration, int64, int64, error) {
	if !reDBName.MatchString(db) {
		return 0, 0, 0, fmt.Errorf("invalid database name %q", db)
	}
	file := b.fileFor(db)
	s, err := b.OpenSession("vacuum", "cli", db)
	if err != nil {
		return 0, 0, 0, err
	}
	s.AcquireWrite()
	start := time.Now()
	sizeBefore := fileSize(file)
	_, _, err = s.Exec("VACUUM")
	s.ReleaseWrite()
	s.Close() // 连接关闭时 VACUUM 的文件替换/落盘才最终完成（wasm VFS 行为）
	if err != nil {
		return time.Since(start), 0, 0, err
	}
	return time.Since(start), sizeBefore, fileSize(file), nil
}

func fileSize(p string) int64 {
	st, err := os.Stat(p)
	if err != nil {
		return 0
	}
	return st.Size()
}

// CloseAll 释放后端（CLI 模式用）。
func (b *Backend) CloseAll() {}
