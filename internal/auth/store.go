// Package auth — 用户权限系统（v1.0.10）。
// 账号/权限/审计入库（<db_dir>/glacier_auth.db 系统库表，不再用配置文件存用户）；
// 兼容并自动升级旧版用户策略（config auth 段 + glacier.json → 系统表）；
// 防爆破：登录失败计数（账号 / IP / 混合三模式）、指数升级锁定、持久化、
// 可选系统层防火墙封锁（ufw / iptables / firewall-cmd）、登录失败 HTTP 异步告警。
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// 账号类型。
const (
	TypeSuper    = "super"    // 超级用户（默认 glacier）：全部权限
	TypeReadonly = "readonly" // 只读账号：仅查询/查看
	TypeUser     = "user"     // 普通用户：按 user_grants 授权
)

// User 账户行。
type User struct {
	Username  string
	Password  string
	Type      string
	Plugin    string // 登录算法：mysql_native_password | caching_sha2_password | sha256_password
	DefaultDB string
	Locked    bool
	LockedTo  string // 自动锁定到期（空 = 无）
	QPS        int64 // 账号级配额（v1.3.6；0 = 不限）
	Concurrency int64
}

// HostRow IP 授权行。
type HostRow struct {
	Username    string
	HostPattern string
}

// Grant 权限行。
type Grant struct {
	Username    string
	DB          string
	Table       string
	Privs       string
	GrantOption bool
}

// Attempt 登录审计行。
type Attempt struct {
	ID       int64
	TS       string
	IP       string
	Remote   string
	Username string
	Success  bool
	Reason   string
}

// LockRow 锁定行。
type LockRow struct {
	LockKey   string // ip:<ip> / acct:<user>
	LockType  string // ip | account
	LockedAt  string
	UntilTS   string
	FailCount int
	Level     int
}

// Bootstrap 旧版用户策略迁移输入。
type Bootstrap struct {
	AdminUser, AdminPassword       string
	ReadonlyUser, ReadonlyPassword string // 密码为空 = 不创建只读账号
}

// Store 认证/权限存储。
type Store struct {
	db     *sql.DB
	cfg    config.LockOpts
	boot   Bootstrap
	fw     *firewallCtl
	client *http.Client

	failMu sync.Mutex
	fails  map[string][]time.Time // 窗口内失败时间戳（key: ip:<ip> / acct:<user>）

	quotaMu    sync.Mutex
	quotaCache map[string][2]int64 // 账号级配额缓存（v1.3.6；[qps, concurrency]）
}

// Open 打开系统库、建表并做首次迁移（users 表为空时从 bootstrap 升级）。
func Open(db *sql.DB, boot Bootstrap, lockCfg config.LockOpts) (*Store, error) {
	s := &Store{
		db:     db,
		cfg:    lockCfg,
		boot:   boot,
		client: &http.Client{Timeout: 10 * time.Second},
		fails:  make(map[string][]time.Time),
		quotaCache: map[string][2]int64{},
	}
	s.fw = newFirewallCtl(lockCfg.Firewall)
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS users (
  username   TEXT PRIMARY KEY,
  password   TEXT NOT NULL,
  account_type TEXT NOT NULL DEFAULT 'user',
  auth_plugin TEXT NOT NULL DEFAULT 'mysql_native_password',
  default_db TEXT NOT NULL DEFAULT '',
  locked     INTEGER NOT NULL DEFAULT 0,
  locked_until TEXT NOT NULL DEFAULT '',
  created_ts TEXT NOT NULL DEFAULT '',
  updated_ts TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS user_hosts (
  username     TEXT NOT NULL,
  host_pattern TEXT NOT NULL,
  PRIMARY KEY (username, host_pattern)
);
CREATE TABLE IF NOT EXISTS user_grants (
  username    TEXT NOT NULL,
  db_name     TEXT NOT NULL,
  table_name  TEXT NOT NULL,
  privs       TEXT NOT NULL,
  grant_option INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (username, db_name, table_name)
);
CREATE TABLE IF NOT EXISTS login_attempts (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  ts       TEXT NOT NULL DEFAULT '',
  ip       TEXT NOT NULL DEFAULT '',
  remote   TEXT NOT NULL DEFAULT '',
  username TEXT NOT NULL DEFAULT '',
  success  INTEGER NOT NULL DEFAULT 0,
  reason   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS user_quotas (
  username    TEXT PRIMARY KEY,
  qps         INTEGER NOT NULL DEFAULT 0,
  concurrency INTEGER NOT NULL DEFAULT 0,
  updated_ts  TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS lock_state (
  lock_key   TEXT PRIMARY KEY,
  lock_type  TEXT NOT NULL DEFAULT '',
  locked_at  TEXT NOT NULL DEFAULT '',
  until_ts   TEXT NOT NULL DEFAULT '',
  fail_count INTEGER NOT NULL DEFAULT 0,
  level      INTEGER NOT NULL DEFAULT 0
);`); err != nil {
		return nil, fmt.Errorf("init auth schema: %w", err)
	}
	// 旧库迁移：users 表补 auth_plugin 列（v1.1.1，缓存列已存在时忽略报错）
	_, _ = db.Exec(`ALTER TABLE users ADD COLUMN auth_plugin TEXT NOT NULL DEFAULT 'mysql_native_password'`)
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

// migrate 旧版用户策略升级：users 表为空（或缺 glacier）时按 bootstrap 落表。
func (s *Store) migrate() error {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		return err
	}
	now := time.Now().Format("2006-01-02 15:04:05")
	if n == 0 {
		tx, err := s.db.Begin()
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.Exec(`INSERT INTO users(username,password,account_type,default_db,created_ts,updated_ts) VALUES(?,?,?,?,?,?)`,
			s.boot.AdminUser, s.boot.AdminPassword, TypeSuper, "", now, now); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO user_hosts(username,host_pattern) VALUES(?,?)`, s.boot.AdminUser, "%"); err != nil {
			return err
		}
		if s.boot.ReadonlyUser != "" && s.boot.ReadonlyPassword != "" {
			if _, err := tx.Exec(`INSERT INTO users(username,password,account_type,default_db,created_ts,updated_ts) VALUES(?,?,?,?,?,?)`,
				s.boot.ReadonlyUser, s.boot.ReadonlyPassword, TypeReadonly, "", now, now); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO user_hosts(username,host_pattern) VALUES(?,?)`, s.boot.ReadonlyUser, "%"); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		logx.Infof("auth: migrated legacy user policy (config/glacier.json) into %s system tables", "glacier_auth.db")
		return nil
	}
	// 存量库若缺超级用户，补回（防呆）
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO users(username,password,account_type,default_db,created_ts,updated_ts) VALUES(?,?,?,?,?,?)`,
		s.boot.AdminUser, s.boot.AdminPassword, TypeSuper, "", now, now); err != nil {
		return err
	}
	_, _ = s.db.Exec(`INSERT OR IGNORE INTO user_hosts(username,host_pattern) VALUES(?,?)`, s.boot.AdminUser, "%")
	return nil
}

// ---------------------------------------------------------------------------
// 认证查询
// ---------------------------------------------------------------------------

// Lookup 查询账户（密码明文对照存储，与旧版 glacier.json 一致）。
func (s *Store) Lookup(username string) (User, bool, error) {
	var u User
	var locked int
	var lockedUntil string
	err := s.db.QueryRow(`SELECT username,password,account_type,COALESCE(auth_plugin,''),default_db,locked,locked_until FROM users WHERE username=?`,
		username).Scan(&u.Username, &u.Password, &u.Type, &u.Plugin, &u.DefaultDB, &locked, &lockedUntil)
	if err == sql.ErrNoRows {
		return User{}, false, nil
	}
	if err != nil {
		return User{}, false, err
	}
	u.Locked = locked != 0
	u.LockedTo = lockedUntil
	return u, true, nil
}

// UserType 返回账号类型（未知 = user）。
func (s *Store) UserType(username string) string {
	u, ok, err := s.Lookup(username)
	if err != nil || !ok {
		return TypeUser
	}
	return u.Type
}

// HostOK 判断来源 IP 是否命中该账号的 host 授权（% / 前缀% / CIDR / 精确 IP）。
func (s *Store) HostOK(username, ip string) (bool, string) {
	rows, err := s.db.Query(`SELECT host_pattern FROM user_hosts WHERE username=?`, username)
	if err != nil {
		return false, ""
	}
	defer rows.Close()
	best, bestScore := "", -1
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			continue
		}
		if ok, score := hostMatchScore(p, ip); ok && score > bestScore {
			best, bestScore = p, score
		}
	}
	return bestScore >= 0, best
}

// hostMatchScore 打分匹配：精确 3 > CIDR 2 > 前缀% 1 > % 0。
func hostMatchScore(pattern, ip string) (bool, int) {
	p := strings.TrimSpace(pattern)
	switch {
	case p == "" || p == "%":
		return true, 0
	case strings.Contains(p, "/"):
		_, cidr, err := net.ParseCIDR(p)
		if err != nil {
			return false, -1
		}
		if pip := net.ParseIP(ip); pip != nil && cidr.Contains(pip) {
			return true, 2
		}
		return false, -1
	case strings.HasSuffix(p, "%"):
		if strings.HasPrefix(ip, strings.TrimSuffix(p, "%")) {
			return true, 1
		}
		return false, -1
	default:
		return p == ip, 3
	}
}

// AccountLocked 判断账号是否被锁（手动 locked 或 lock_state 自动锁未到期）。
func (s *Store) AccountLocked(username string) (bool, time.Time) {
	u, ok, err := s.Lookup(username)
	if err != nil || !ok {
		return false, time.Time{}
	}
	if u.Locked {
		if u.LockedTo != "" {
			if t, e := time.ParseInLocation("2006-01-02 15:04:05", u.LockedTo, time.Local); e == nil && time.Now().After(t) {
				return false, time.Time{} // 自动锁已到期
			}
		}
		return true, time.Time{}
	}
	return s.lockActive("acct:" + username)
}

// IPLocked 判断来源 IP 是否被锁（本机回环地址永不锁定：运维本机客户端
// 密码配错不应把自己锁死在管理面之外）。
func (s *Store) IPLocked(ip string) (bool, time.Time) {
	if isLoopbackIP(ip) {
		return false, time.Time{}
	}
	return s.lockActive("ip:" + ip)
}

// isLoopbackIP 判断是否本机回环地址（127.x.x.x / ::1 / localhost）。
func isLoopbackIP(ip string) bool {
	if ip == "" {
		return false
	}
	if strings.EqualFold(ip, "localhost") {
		return true
	}
	if p := net.ParseIP(ip); p != nil {
		return p.IsLoopback()
	}
	return false
}

func (s *Store) lockActive(key string) (bool, time.Time) {
	var until string
	err := s.db.QueryRow(`SELECT until_ts FROM lock_state WHERE lock_key=?`, key).Scan(&until)
	if err != nil {
		return false, time.Time{}
	}
	t, e := time.ParseInLocation("2006-01-02 15:04:05", until, time.Local)
	if e == nil && time.Now().After(t) {
		return false, time.Time{} // 已到期（行保留，惰性清理）
	}
	if e != nil {
		return true, time.Time{} // 无到期时间 = 永久锁
	}
	return true, t
}

// ---------------------------------------------------------------------------
// 登录审计与防爆破
// ---------------------------------------------------------------------------

// RecordLogin 记录一次登录审计；失败时做防爆破计数与锁定。
func (s *Store) RecordLogin(ip, remote, username string, success bool, reason string) {
	successInt := 0
	if success {
		successInt = 1
	}
	_, _ = s.db.Exec(`INSERT INTO login_attempts(ts,ip,remote,username,success,reason) VALUES(?,?,?,?,?,?)`,
		time.Now().Format("2006-01-02 15:04:05"), ip, remote, username, successInt, truncateStr(reason, 200))
	if !s.cfg.Enable {
		return
	}
	if success {
		s.clearFails("ip:" + ip)
		s.clearFails("acct:" + username)
		return
	}
	// 失败计数（窗口滑动）；本机回环地址豁免 IP 维度计数与锁定
	//（审计照记；账号维度计数不受影响，可用 UNLOCK ACCOUNT / CLI -unlock-user 解锁）
	if (s.cfg.Mode == "ip_only" || s.cfg.Mode == "mixed") && !isLoopbackIP(ip) {
		if s.bumpFail("ip:"+ip) >= s.cfg.MaxFailuresIP {
			s.lockKey("ip:"+ip, "ip", ip, username)
		}
	}
	if s.cfg.Mode == "account_only" || s.cfg.Mode == "mixed" {
		if s.bumpFail("acct:"+username) >= s.cfg.MaxFailuresAcct {
			s.lockKey("acct:"+username, "account", username, ip)
		}
	}
}

func (s *Store) bumpFail(key string) int {
	now := time.Now()
	win := time.Duration(s.cfg.WindowSec) * time.Second
	s.failMu.Lock()
	defer s.failMu.Unlock()
	arr := s.fails[key]
	kept := arr[:0]
	for _, t := range arr {
		if now.Sub(t) <= win {
			kept = append(kept, t)
		}
	}
	kept = append(kept, now)
	s.fails[key] = kept
	return len(kept)
}

func (s *Store) clearFails(key string) {
	s.failMu.Lock()
	delete(s.fails, key)
	s.failMu.Unlock()
}

// lockKey 落锁（指数升级）+ 防火墙封锁 + HTTP 告警。
func (s *Store) lockKey(key, lockType, subject, ip string) {
	if lockType == "ip" && isLoopbackIP(subject) {
		logx.Debugf("auth: skip locking loopback %s", subject)
		return
	}
	now := time.Now()
	var level, failCount int
	var lockedAt, untilOld string
	err := s.db.QueryRow(`SELECT level, fail_count, locked_at, until_ts FROM lock_state WHERE lock_key=?`, key).
		Scan(&level, &failCount, &lockedAt, &untilOld)
	if err != nil { // 新锁
		level = 0
	}
	// 未到期的重复触发才升级；到期后重锁回到 1 级
	if err == nil {
		if t, e := time.ParseInLocation("2006-01-02 15:04:05", untilOld, time.Local); e != nil || time.Now().Before(t) {
			level++
		}
	}
	if level < 1 {
		level = 1
	}
	lockSec := s.cfg.LockSec
	for i := 1; i < level; i++ {
		lockSec *= 2
		if lockSec >= s.cfg.LockSecMax {
			lockSec = s.cfg.LockSecMax
			break
		}
	}
	until := now.Add(time.Duration(lockSec) * time.Second)
	_, _ = s.db.Exec(`INSERT INTO lock_state(lock_key,lock_type,locked_at,until_ts,fail_count,level) VALUES(?,?,?,?,?,?)
		ON CONFLICT(lock_key) DO UPDATE SET locked_at=excluded.locked_at, until_ts=excluded.until_ts,
		fail_count=excluded.fail_count, level=excluded.level`,
		key, lockType, now.Format("2006-01-02 15:04:05"), until.Format("2006-01-02 15:04:05"),
		s.cfg.MaxFailures, level)
	logx.Warnf("auth: %s %s locked for %ds (level %d, %s)", lockType, subject, lockSec, level, key)
	if lockType == "ip" && s.fw != nil {
		s.fw.BlockIP(subject, time.Duration(lockSec)*time.Second) // 系统层封锁（尽力而为）
	}
	s.notify(map[string]any{
		"event": lockType + "_locked", "subject": subject, "ip": ip,
		"locked_at": now.Format("2006-01-02 15:04:05"),
		"until":     until.Format("2006-01-02 15:04:05"),
		"level":     level,
	})
}

// NotifyFailed 登录失败告警（异步 HTTP，fire-and-forget）。
func (s *Store) NotifyFailed(ip, username, reason string) {
	if s.cfg.NotifyURL == "" {
		return
	}
	s.notify(map[string]any{
		"event": "login_failed", "ip": ip, "username": username,
		"reason": reason, "ts": time.Now().Format("2006-01-02 15:04:05"),
	})
}

// notify 异步 POST 告警（HMAC 签名，退避重试；失败仅记日志）。
func (s *Store) notify(payload map[string]any) {
	if s.cfg.NotifyURL == "" {
		return
	}
	go func() {
		body, _ := json.Marshal(payload)
		for attempt, backoff := range []time.Duration{0, time.Second, 5 * time.Second, 15 * time.Second} {
			time.Sleep(backoff)
			req, err := http.NewRequest(http.MethodPost, s.cfg.NotifyURL, strings.NewReader(string(body)))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			if s.cfg.NotifySecret != "" {
				mac := hmac.New(sha256.New, []byte(s.cfg.NotifySecret))
				mac.Write(body)
				req.Header.Set("X-Glacier-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
			}
			resp, err := s.client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return
				}
			}
			logx.Warnf("auth notify attempt %d to %s failed", attempt+1, s.cfg.NotifyURL)
		}
	}()
}

// ---------------------------------------------------------------------------
// 解锁与视图
// ---------------------------------------------------------------------------

// UnlockAccount 解锁账号（手动锁 + 自动锁 + 失败计数）。
func (s *Store) UnlockAccount(username string) error {
	_, err := s.db.Exec(`DELETE FROM lock_state WHERE lock_key=?`, "acct:"+username)
	_, _ = s.db.Exec(`UPDATE users SET locked=0, locked_until='' WHERE username=?`, username)
	s.clearFails("acct:" + username)
	if err == nil {
		s.notify(map[string]any{"event": "unlocked", "subject": username, "lock_type": "account"})
	}
	return err
}

// UnlockIP 解锁 IP（自动锁 + 失败计数 + 防火墙解封）。
func (s *Store) UnlockIP(ip string) error {
	_, err := s.db.Exec(`DELETE FROM lock_state WHERE lock_key=?`, "ip:"+ip)
	s.clearFails("ip:" + ip)
	if s.fw != nil {
		s.fw.UnblockIP(ip) // 尽力解除系统层封锁
	}
	if err == nil {
		s.notify(map[string]any{"event": "unlocked", "subject": ip, "lock_type": "ip"})
	}
	return err
}

// Users 全部账户。
func (s *Store) Users() []User {
	rows, err := s.db.Query(`SELECT u.username,u.password,u.account_type,u.default_db,u.locked,u.locked_until,
		COALESCE(q.qps,0),COALESCE(q.concurrency,0)
		FROM users u LEFT JOIN user_quotas q ON q.username=u.username ORDER BY u.username`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []User
	for rows.Next() {
		var u User
		var locked int
		if err := rows.Scan(&u.Username, &u.Password, &u.Type, &u.DefaultDB, &locked, &u.LockedTo, &u.QPS, &u.Concurrency); err == nil {
			u.Locked = locked != 0
			out = append(out, u)
		}
	}
	return out
}

// Hosts 全部 host 授权行。
func (s *Store) Hosts() []HostRow {
	rows, err := s.db.Query(`SELECT username,host_pattern FROM user_hosts ORDER BY username,host_pattern`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []HostRow
	for rows.Next() {
		var h HostRow
		if err := rows.Scan(&h.Username, &h.HostPattern); err == nil {
			out = append(out, h)
		}
	}
	return out
}

// GrantsFor 某账号全部权限行（username 为空 = 全部用户）。
func (s *Store) GrantsFor(username string) []Grant {
	q := `SELECT username,db_name,table_name,privs,grant_option FROM user_grants`
	var args []any
	if username != "" {
		q += ` WHERE username=?`
		args = append(args, username)
	}
	q += ` ORDER BY username,db_name,table_name`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Grant
	for rows.Next() {
		var g Grant
		var gopt int
		if err := rows.Scan(&g.Username, &g.DB, &g.Table, &g.Privs, &gopt); err == nil {
			g.GrantOption = gopt != 0
			out = append(out, g)
		}
	}
	return out
}

// Attempts 最近登录审计（新→旧）。
func (s *Store) Attempts(limit int) []Attempt {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT id,ts,ip,remote,username,success,reason FROM login_attempts ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		var ok int
		if err := rows.Scan(&a.ID, &a.TS, &a.IP, &a.Remote, &a.Username, &ok, &a.Reason); err == nil {
			a.Success = ok != 0
			out = append(out, a)
		}
	}
	return out
}

// Locks 当前锁定列表。
func (s *Store) Locks() []LockRow {
	rows, err := s.db.Query(`SELECT lock_key,lock_type,locked_at,until_ts,fail_count,level FROM lock_state ORDER BY locked_at DESC`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []LockRow
	for rows.Next() {
		var r LockRow
		if err := rows.Scan(&r.LockKey, &r.LockType, &r.LockedAt, &r.UntilTS, &r.FailCount, &r.Level); err == nil {
			out = append(out, r)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// 账号/权限管理（管理面 SQL 调用）
// ---------------------------------------------------------------------------

// CreateUser 创建账户（默认 host '%'；plugin 空 = mysql_native_password）。
func (s *Store) CreateUser(username, password, host, typ, plugin, defaultDB string) error {
	if username == "" || strings.ContainsAny(username, "'\"`") {
		return fmt.Errorf("invalid user name %q", username)
	}
	switch typ {
	case TypeSuper, TypeReadonly, TypeUser:
	default:
		return fmt.Errorf("invalid account type %q", typ)
	}
	plugin, err := NormalizePlugin(plugin)
	if err != nil {
		return err
	}
	now := time.Now().Format("2006-01-02 15:04:05")
	if host == "" {
		host = "%"
	}
	if _, err := s.db.Exec(`INSERT INTO users(username,password,account_type,auth_plugin,default_db,created_ts,updated_ts) VALUES(?,?,?,?,?,?,?)`,
		username, password, typ, plugin, defaultDB, now, now); err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR IGNORE INTO user_hosts(username,host_pattern) VALUES(?,?)`, username, host)
	return err
}

// NormalizePlugin 登录算法名规范化（空 = native；非法报错）。
func NormalizePlugin(plugin string) (string, error) {
	p := strings.ToLower(strings.TrimSpace(plugin))
	p = strings.ReplaceAll(p, "`", "")
	switch p {
	case "", "mysql_native_password":
		return "mysql_native_password", nil
	case "caching_sha2_password":
		return "caching_sha2_password", nil
	case "sha256_password":
		return "sha256_password", nil
	}
	return "", fmt.Errorf("unsupported auth plugin %q (mysql_native_password|caching_sha2_password|sha256_password)", plugin)
}

// SetPlugin 修改账号登录算法（ALTER USER ... IDENTIFIED WITH ...）。
func (s *Store) SetPlugin(username, plugin string) error {
	plugin, err := NormalizePlugin(plugin)
	if err != nil {
		return err
	}
	res, err := s.db.Exec(`UPDATE users SET auth_plugin=?, updated_ts=? WHERE username=?`,
		plugin, time.Now().Format("2006-01-02 15:04:05"), username)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("there is no such account %q defined", username)
	}
	return nil
}

// PluginFor 返回账号登录算法（未知 = mysql_native_password）。
func (s *Store) PluginFor(username string) string {
	u, ok, err := s.Lookup(username)
	if err != nil || !ok || u.Plugin == "" {
		return "mysql_native_password"
	}
	return u.Plugin
}

// SetPassword 改密（管理面 SET PASSWORD / ALTER USER）。
func (s *Store) SetPassword(username, password string) error {
	if strings.TrimSpace(password) == "" {
		return fmt.Errorf("empty password not allowed")
	}
	res, err := s.db.Exec(`UPDATE users SET password=?, updated_ts=? WHERE username=?`,
		password, time.Now().Format("2006-01-02 15:04:05"), username)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("there is no such account %q defined", username)
	}
	return nil
}

// SetAccountType 修改账号类型（super / readonly / user）。
func (s *Store) SetAccountType(username, typ string) error {
	switch typ {
	case TypeSuper, TypeReadonly, TypeUser:
	default:
		return fmt.Errorf("invalid account type %q", typ)
	}
	_, err := s.db.Exec(`UPDATE users SET account_type=?, updated_ts=? WHERE username=?`,
		typ, time.Now().Format("2006-01-02 15:04:05"), username)
	return err
}

// SetLocked 手动锁/解锁账号。
func (s *Store) SetLocked(username string, locked bool) error {
	v, until := 0, ""
	if locked {
		v = 1
	}
	_, err := s.db.Exec(`UPDATE users SET locked=?, locked_until=? WHERE username=?`, v, until, username)
	return err
}

// DropUser 删除账户（含 hosts / grants）。
func (s *Store) DropUser(username string) error {
	if username == s.boot.AdminUser {
		return fmt.Errorf("cannot drop the super user %q", username)
	}
	if _, err := s.db.Exec(`DELETE FROM users WHERE username=?`, username); err != nil {
		return err
	}
	_, _ = s.db.Exec(`DELETE FROM user_hosts WHERE username=?`, username)
	_, _ = s.db.Exec(`DELETE FROM user_grants WHERE username=?`, username)
	return nil
}

// RenameUser 改名。
func (s *Store) RenameUser(oldName, newName string) error {
	if _, err := s.db.Exec(`UPDATE users SET username=? WHERE username=?`, newName, oldName); err != nil {
		return err
	}
	_, _ = s.db.Exec(`UPDATE user_hosts SET username=? WHERE username=?`, newName, oldName)
	_, _ = s.db.Exec(`UPDATE user_grants SET username=? WHERE username=?`, newName, oldName)
	return nil
}

// AddHost 增加 host 授权。
func (s *Store) AddHost(username, host string) error {
	_, err := s.db.Exec(`INSERT OR IGNORE INTO user_hosts(username,host_pattern) VALUES(?,?)`, username, host)
	return err
}

// Grant 授权（db / table 可用 *；privs 逗号分隔，ALL 覆盖全部）。
func (s *Store) Grant(username, db, table, privs string, grantOption bool) error {
	if db == "" {
		db = "*"
	}
	if table == "" {
		table = "*"
	}
	gopt := 0
	if grantOption {
		gopt = 1
	}
	_, err := s.db.Exec(`INSERT INTO user_grants(username,db_name,table_name,privs,grant_option) VALUES(?,?,?,?,?)
		ON CONFLICT(username,db_name,table_name) DO UPDATE SET privs=excluded.privs, grant_option=excluded.grant_option`,
		username, db, table, normalizePrivs(privs), gopt)
	return err
}

// Revoke 回收权限（privs 为空 = 删除整行）。
func (s *Store) Revoke(username, db, table, privs string) error {
	if db == "" {
		db = "*"
	}
	if table == "" {
		table = "*"
	}
	if strings.TrimSpace(privs) == "" {
		_, err := s.db.Exec(`DELETE FROM user_grants WHERE username=? AND db_name=? AND table_name=?`, username, db, table)
		return err
	}
	var cur string
	err := s.db.QueryRow(`SELECT privs FROM user_grants WHERE username=? AND db_name=? AND table_name=?`,
		username, db, table).Scan(&cur)
	if err != nil {
		return err
	}
	remain := subtractPrivs(cur, normalizePrivs(privs))
	if remain == "" {
		_, err = s.db.Exec(`DELETE FROM user_grants WHERE username=? AND db_name=? AND table_name=?`, username, db, table)
		return err
	}
	_, err = s.db.Exec(`UPDATE user_grants SET privs=? WHERE username=? AND db_name=? AND table_name=?`,
		remain, username, db, table)
	return err
}

// CheckPrivilege 权限判定：super 恒真；readonly 只读（由 handler 层控制）；
// 普通 user 按 user_grants（* 通配，ALL 覆盖）。
func (s *Store) CheckPrivilege(username, db, table, priv string) bool {
	u, ok, err := s.Lookup(username)
	if err != nil || !ok {
		return false
	}
	if u.Type == TypeSuper {
		return true
	}
	if u.Type == TypeReadonly {
		return priv == "SELECT"
	}
	priv = strings.ToUpper(strings.TrimSpace(priv))
	rows, err := s.db.Query(`SELECT db_name,table_name,privs FROM user_grants WHERE username=?`, username)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var gdb, gtab, gprivs string
		if err := rows.Scan(&gdb, &gtab, &gprivs); err != nil {
			continue
		}
		if gdb != "*" && !strings.EqualFold(gdb, db) {
			continue
		}
		if gtab != "*" && !strings.EqualFold(gtab, table) {
			continue
		}
		for _, p := range strings.Split(strings.ToUpper(gprivs), ",") {
			p = strings.TrimSpace(p)
			if p == "ALL" || p == priv {
				return true
			}
		}
	}
	return false
}

// normalizePrivs 规范化权限串（大写、去重、去空白；ALL PRIVILEGES → ALL）。
func normalizePrivs(privs string) string {
	privs = strings.ReplaceAll(strings.ToUpper(privs), "ALL PRIVILEGES", "ALL")
	seen := map[string]bool{}
	var out []string
	for _, p := range strings.Split(privs, ",") {
		p = strings.TrimSpace(p)
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return strings.Join(out, ",")
}

// subtractPrivs 从 cur 中移除 del 里的权限。
func subtractPrivs(cur, del string) string {
	rm := map[string]bool{}
	for _, p := range strings.Split(del, ",") {
		rm[strings.ToUpper(strings.TrimSpace(p))] = true
	}
	var out []string
	for _, p := range strings.Split(cur, ",") {
		p = strings.TrimSpace(p)
		if p == "" || rm[strings.ToUpper(p)] {
			continue
		}
		out = append(out, p)
	}
	return strings.Join(out, ",")
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ---------------------------------------------------------------------------
// go-mysql 认证钩子（每连接一个 Handler，捕获来源 IP）
// ---------------------------------------------------------------------------

// Handler 实现 server.AuthenticationHandler：
// GetCredential 阶段完成 IP/账号锁定与 host 授权检查（先于密码比对，拒绝即精确错误码）；
// OnAuthFailure/OnAuthSuccess 落审计并做防爆破计数。
type Handler struct {
	store *Store
	ip    string
}

// NewHandler 创建连接级认证处理器（remoteAddr 形如 "1.2.3.4:5678"）。
func NewHandler(store *Store, remoteAddr string) *Handler {
	ip := remoteAddr
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		ip = h
	}
	return &Handler{store: store, ip: ip}
}

// GetCredential 实现 server.AuthenticationHandler。
func (h *Handler) GetCredential(username string) (server.Credential, bool, error) {
	// IP 锁（先于一切）
	if ok, until := h.store.IPLocked(h.ip); ok {
		return server.Credential{}, false, mysql.NewError(3119,
			fmt.Sprintf("Host '%s' is blocked (brute-force protection) until %s", h.ip, until.Format("2006-01-02 15:04:05")))
	}
	u, found, err := h.store.Lookup(username)
	if err != nil {
		return server.Credential{}, false, err
	}
	if !found {
		return server.Credential{}, false, nil
	}
	// 账号锁
	if ok, until := h.store.AccountLocked(username); ok {
		return server.Credential{}, false, mysql.NewError(3119,
			fmt.Sprintf("Access denied for user '%s'. Account is blocked (brute-force protection) until %s",
				username, until.Format("2006-01-02 15:04:05")))
	}
	// host 授权
	if ok, pattern := h.store.HostOK(username, h.ip); !ok {
		return server.Credential{}, false, mysql.NewError(1045,
			fmt.Sprintf("Host '%s' is not allowed to connect to this MySQL server (user '%s'@'%s')", h.ip, username, username))
	} else {
		_ = pattern
	}
	return server.Credential{
		Passwords:      []string{u.Password},
		AuthPluginName: pluginOrDefault(u.Plugin),
	}, true, nil
}

// pluginOrDefault 空插件回退 mysql_native_password。
func pluginOrDefault(p string) string {
	if p == "" {
		return "mysql_native_password"
	}
	return p
}

// OnAuthSuccess 实现 server.AuthenticationHandler：成功审计。
func (h *Handler) OnAuthSuccess(conn *server.Conn) error {
	h.store.RecordLogin(h.ip, h.ip, conn.GetUser(), true, "")
	return nil
}

// OnAuthFailure 实现 server.AuthenticationHandler：失败审计 + 防爆破 + 告警。
func (h *Handler) OnAuthFailure(conn *server.Conn, err error) {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	user := ""
	if conn != nil {
		user = conn.GetUser()
	}
	h.store.RecordLogin(h.ip, h.ip, user, false, reason)
	h.store.NotifyFailed(h.ip, user, reason)
}

// IP 返回来源 IP。
func (h *Handler) IP() string { return h.ip }

// ---------------------------------------------------------------------------
// 系统层防火墙封锁（可选：ufw / iptables / firewall-cmd，自动识别）
// ---------------------------------------------------------------------------

// firewallCtl 系统层 IP 封锁（尽力而为：无权限/容器内自动降级为网关层软锁）。
type firewallCtl struct {
	driver string // off | ufw | iptables | firewall-cmd
}

func newFirewallCtl(driver string) *firewallCtl {
	f := &firewallCtl{driver: driver}
	if driver == "" || driver == "auto" {
		f.driver = f.detect()
	}
	if f.driver != "off" && f.driver != "" {
		logx.Infof("auth firewall driver: %s", f.driver)
	}
	return f
}

func (f *firewallCtl) detect() string {
	for _, d := range []string{"ufw", "firewall-cmd", "iptables"} {
		if _, err := exec.LookPath(d); err == nil {
			return d
		}
	}
	return "off"
}

// BlockIP 封锁 IP（异步：下发规则 + TTL 后自动解封；本机回环永不封锁）。
func (f *firewallCtl) BlockIP(ip string, ttl time.Duration) {
	if f.driver == "" || f.driver == "off" || net.ParseIP(ip) == nil || isLoopbackIP(ip) {
		return
	}
	go func() {
		if err := f.exec(true, ip); err != nil {
			logx.Warnf("auth firewall block %s failed (%s): %v", ip, f.driver, err)
			return
		}
		logx.Infof("auth firewall: blocked %s via %s for %s", ip, f.driver, ttl)
		time.AfterFunc(ttl, func() {
			if err := f.exec(false, ip); err != nil {
				logx.Warnf("auth firewall unblock %s failed (%s): %v", ip, f.driver, err)
			}
		})
	}()
}

// UnblockIP 解除封锁（尽力而为）。
func (f *firewallCtl) UnblockIP(ip string) {
	if f.driver == "" || f.driver == "off" || net.ParseIP(ip) == nil {
		return
	}
	go func() {
		_ = f.exec(false, ip)
	}()
}

func (f *firewallCtl) exec(block bool, ip string) error {
	var args []string
	switch f.driver {
	case "ufw":
		if block {
			args = []string{"deny", "from", ip, "to", "any"}
		} else {
			args = []string{"delete", "deny", "from", ip, "to", "any"}
		}
		return runCmd("ufw", args...)
	case "iptables":
		op := "-I"
		if !block {
			op = "-D"
		}
		return runCmd("iptables", op, "INPUT", "-s", ip, "-j", "DROP")
	case "firewall-cmd":
		rule := fmt.Sprintf(`rule family="ipv4" source address="%s" drop`, ip)
		if block {
			return runCmd("firewall-cmd", "--add-rich-rule="+rule)
		}
		return runCmd("firewall-cmd", "--remove-rich-rule="+rule)
	}
	return fmt.Errorf("firewall driver %q not supported", f.driver)
}

func runCmd(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %v: %v (%s)", name, args, err, truncateStr(string(out), 200))
	}
	return nil
}

// ---------------------------------------------------------------------------
// 多租户增强（v1.3.6）：DEFAULT_DB / 库级授权探测 / 账号级配额
// ---------------------------------------------------------------------------

// SetDefaultDB 设置账号默认库（CREATE USER DEFAULT_DB / SET USER DEFAULT_DB）。
func (s *Store) SetDefaultDB(username, db string) error {
	res, err := s.db.Exec(`UPDATE users SET default_db=?, updated_ts=? WHERE username=?`,
		db, time.Now().Format("2006-01-02 15:04:05"), username)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("unknown user '%s'", username)
	}
	return nil
}

// HasDBGrant 判断账号在指定库上是否有任意授权（USE 权限校验 / SHOW DATABASES 过滤用；
// db_name='*' 的通配授权对任意库生效）。
func (s *Store) HasDBGrant(username, db string) bool {
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM user_grants WHERE username=? AND (db_name=? OR db_name='*')`,
		username, db).Scan(&n)
	return err == nil && n > 0
}

// GrantedDBs 账号被授权的库集合（'*' 通配时返回 nil 表示全部可见）。
func (s *Store) GrantedDBs(username string) (map[string]bool, bool) {
	set := map[string]bool{}
	rows, err := s.db.Query(`SELECT db_name FROM user_grants WHERE username=?`, username)
	if err != nil {
		return nil, false
	}
	defer rows.Close()
	for rows.Next() {
		var db string
		if rows.Scan(&db) == nil {
			if db == "*" {
				return nil, true // 通配全库
			}
			set[db] = true
		}
	}
	return set, len(set) > 0
}

// SetQuota 设置账号级配额（qps/concurrency，0 = 不限；v1.3.6）。
func (s *Store) SetQuota(username string, qps, concurrency int64) error {
	if qps < 0 || concurrency < 0 {
		return fmt.Errorf("quota values must be >= 0")
	}
	res, err := s.db.Exec(`INSERT INTO user_quotas(username,qps,concurrency,updated_ts) VALUES(?,?,?,?)
		ON CONFLICT(username) DO UPDATE SET qps=excluded.qps, concurrency=excluded.concurrency, updated_ts=excluded.updated_ts`,
		username, qps, concurrency, time.Now().Format("2006-01-02 15:04:05"))
	if err != nil {
		return err
	}
	_ = res
	s.quotaMu.Lock()
	s.quotaCache[username] = [2]int64{qps, concurrency}
	s.quotaMu.Unlock()
	return nil
}

// ClearQuota 清除账号配额（恢复不限）。
func (s *Store) ClearQuota(username string) error {
	_, err := s.db.Exec(`DELETE FROM user_quotas WHERE username=?`, username)
	s.quotaMu.Lock()
	delete(s.quotaCache, username)
	s.quotaMu.Unlock()
	return err
}

// QuotaFor 取账号配额（内存缓存优先；0,0 = 不限）。
func (s *Store) QuotaFor(username string) (qps, concurrency int64) {
	s.quotaMu.Lock()
	if q, ok := s.quotaCache[username]; ok {
		s.quotaMu.Unlock()
		return q[0], q[1]
	}
	s.quotaMu.Unlock()
	var q, c int64
	if err := s.db.QueryRow(`SELECT qps,concurrency FROM user_quotas WHERE username=?`, username).Scan(&q, &c); err != nil {
		q, c = 0, 0
	}
	s.quotaMu.Lock()
	s.quotaCache[username] = [2]int64{q, c}
	s.quotaMu.Unlock()
	return q, c
}
