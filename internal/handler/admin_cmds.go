package handler

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/admin"
	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
)

// GlacierTDB 管理库名（USE GlacierTDB 进入管理面）。
const AdminDB = "GlacierTDB"

var (
	reAdminShowConfig = regexp.MustCompile(`(?i)^SHOW\s+SERVER\s+CONFIG$`)
	reAdminShowClient = regexp.MustCompile(`(?i)^SHOW\s+CLIENTS$`)
	reAdminShowTx     = regexp.MustCompile(`(?i)^SHOW\s+TRANSACTIONS$`)
	reAdminSetPwd     = regexp.MustCompile(`(?i)^(?:SET\s+PASSWORD(?:\s+FOR\s+(\S+?))?\s*=\s*|ALTER\s+USER\s+(\S+?)\s+IDENTIFIED\s+BY\s+)'([^']{1,64})'\s*$`)
	reAdminSelect     = regexp.MustCompile("(?i)^SELECT\\s+(.+?)\\s+FROM\\s+(server_config|clients|transactions)(?:\\s+WHERE\\s+(.+?))?(?:\\s+ORDER\\s+BY\\s+.+?)?(?:\\s+LIMIT\\s+\\d+(?:\\s*,\\s*\\d+)?)?\\s*$")
	reKillConn        = regexp.MustCompile(`(?i)^KILL\s+(?:CONNECTION\s+|QUERY\s+)?(\d+)$`)
	reUseAdmin        = regexp.MustCompile("(?i)^USE\\s+`?(GlacierTDB|glacier)`?\\s*$") // 管理库别名：glacier 为系统内置名，禁止建库
	reAdminSetConc    = regexp.MustCompile(`(?i)^SET\s+(GLOBAL_THREADS|PER_DB_READ_THREADS|DB_READ_THREADS)\s*(?:([A-Za-z0-9_-]+)\s*)?=\s*(\d+)\s*$`)
)

// IsAdminUse 判断是否进入/退出管理库的 USE 语句（handler 在探活层之前拦截）。
func IsAdminUse(q string) (adminMode bool, ok bool) {
	if reUseAdmin.MatchString(q) {
		return true, true
	}
	return false, false
}

var reVacuumDB = regexp.MustCompile("(?i)^VACUUM\\s+DATABASE\\s+`?([A-Za-z0-9_-]+)`?\\s*$")

// handleVacuumDB 管理面 VACUUM —— ⚠️ 执行期间该库全部写入被阻塞（写锁长持）。
func (h *Handler) handleVacuumDB(q string) (*mysql.Result, bool, error) {
	m := reVacuumDB.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	if h.readonly {
		return nil, true, roErr()
	}
	if h.cfg.Maintenance.NoHotVacuum {
		return nil, true, mysql.NewError(1149,
			"hot VACUUM disabled (maintenance.no_hot_vacuum); 请停止服务后使用 CLI: glaciertdb -vacuum -db-dir <dir>")
	}
	cost, sizeBefore, sizeAfter, err := h.backend.VacuumDB(m[1])
	if err != nil {
		return nil, true, MapSQLiteErr(err)
	}
	res, err2 := meta.SimpleRS([]string{"database", "cost_ms", "size_before", "size_after", "note"},
		[][]any{{m[1], cost.Milliseconds(), sizeBefore, sizeAfter, "执行期间该库所有写入被阻塞（写锁长持至 VACUUM 完成）；wasm 落盘略滞后，size_after 仅供参考"}})
	return res, true, err2
}

// handleKill 处理 KILL <id>（全局生效，不限管理面；只读账号拒绝）。
func (h *Handler) handleKill(q string) (handled bool, err error) {
	m := reKillConn.FindStringSubmatch(q)
	if m == nil {
		return false, nil
	}
	if h.readonly {
		return true, roErr()
	}
	var id uint64
	fmt.Sscanf(m[1], "%d", &id)
	if !h.registry.Kill(id) {
		return true, mysql.NewError(1094, "Unknown thread id "+m[1])
	}
	logx.Infof("conn %d killed conn %d by admin command", h.connID, id)
	return true, nil
}

// handleAdmin 管理面命令分发；handled=false 表示不是管理命令。
// 仅在 adminMode 下调用（透传 SQL 前拦截）。
func (h *Handler) handleAdmin(q string) (*mysql.Result, bool, error) {
	switch {
	case reAdminShowConfig.MatchString(q):
		rows := h.serverConfigRows()
		res, err := meta.SimpleRS([]string{"name", "value"}, rows)
		return res, true, err

	case reAdminShowClient.MatchString(q):
		var rows [][]any
		for _, c := range h.registry.List() {
			rows = append(rows, []any{
				int64(c.ID), c.User, c.Remote, c.DB, b2s(c.AdminMode),
				int64(c.ConnectedSec), b2s(c.InTx), b2s(c.HoldingWriteLock), c.LastQuery,
			})
		}
		res, err := meta.SimpleRS([]string{"Id", "User", "Host", "DB", "AdminMode", "ConnectedSec", "InTransaction", "HoldingWriteLock", "LastQuery"}, rows)
		return res, true, err

	case reAdminShowTx.MatchString(q):
		var rows [][]any
		for _, c := range h.registry.List() {
			if c.InTx {
				rows = append(rows, []any{
					int64(c.ID), c.User, c.Remote, c.DB, int64(c.TxSec),
					b2s(c.HoldingWriteLock), c.LastQuery,
				})
			}
		}
		res, err := meta.SimpleRS([]string{"Id", "User", "Host", "DB", "TxSec", "HoldingWriteLock", "LastQuery"}, rows)
		return res, true, err

	case reAdminSetPwd.MatchString(q):
		if h.readonly {
			return nil, true, roErr()
		}
		m := reAdminSetPwd.FindStringSubmatch(q)
		newPwd := m[3]
		if strings.TrimSpace(newPwd) == "" {
			return nil, true, mysql.NewError(1819, "Your password does not satisfy the current policy requirements (empty)")
		}
		// 目标账号：SET PASSWORD FOR <user> / ALTER USER <user>（缺省 = 管理员自己）；
		// 命中只读账号名则改只读密码（v1.0.9：与管理员密码同等热改、同一 glacier.json 持久化）
		target := m[1]
		if target == "" {
			target = m[2]
		}
		var err error
		whom := "admin"
		if target != "" && h.cfg.IsReadOnlyUser(target) {
			err = h.backend.SetReadonlyPassword(newPwd)
			whom = target + " (readonly)"
		} else {
			err = h.backend.SetAuthPassword(newPwd)
		}
		if err != nil {
			return nil, true, mysql.NewError(1105, "persist password failed: "+err.Error())
		}
		logx.Infof("auth password changed for %s via admin command (by conn %d)", whom, h.connID)
		return meta.OKResultPub(), true, nil

	case reAdminSetConc.MatchString(q):
		if h.readonly {
			return nil, true, roErr()
		}
		// 并发配置热改（仅内存生效，重启失效回到配置文件值）
		m := reAdminSetConc.FindStringSubmatch(q)
		val := 0
		fmt.Sscanf(m[3], "%d", &val)
		db := m[2]
		switch strings.ToUpper(m[1]) {
		case "GLOBAL_THREADS":
			h.backend.SetGlobalThreads(val)
		case "PER_DB_READ_THREADS":
			h.backend.SetPerDBReadThreads(val)
		case "DB_READ_THREADS":
			if db == "" {
				return nil, true, mysql.NewError(1064, "SET DB_READ_THREADS <db> = <n> requires a database name")
			}
			h.backend.SetDBReadThreads(db, val)
		}
		logx.Infof("concurrency %s[%s] set to %d via admin command (runtime only, by conn %d)",
			strings.ToUpper(m[1]), db, val, h.connID)
		return meta.OKResultPub(), true, nil
	}

	// SELECT * FROM server_config / clients / transactions [WHERE ...]
	if m := reAdminSelect.FindStringSubmatch(q); m != nil {
		table := strings.ToLower(m[2])
		where := m[3]
		var cols []string
		var rows [][]any
		switch table {
		case "server_config":
			cols = []string{"name", "value"}
			rows = h.serverConfigRows()
		case "clients":
			cols = []string{"Id", "User", "Host", "DB", "AdminMode", "ConnectedSec", "InTransaction", "HoldingWriteLock", "LastQuery"}
			for _, c := range h.registry.List() {
				rows = append(rows, []any{
					int64(c.ID), c.User, c.Remote, c.DB, b2s(c.AdminMode),
					int64(c.ConnectedSec), b2s(c.InTx), b2s(c.HoldingWriteLock), c.LastQuery,
				})
			}
		case "transactions":
			cols = []string{"Id", "User", "Host", "DB", "TxSec", "HoldingWriteLock", "LastQuery"}
			for _, c := range h.registry.List() {
				if c.InTx {
					rows = append(rows, []any{
						int64(c.ID), c.User, c.Remote, c.DB, int64(c.TxSec),
						b2s(c.HoldingWriteLock), c.LastQuery,
					})
				}
			}
		}
		rows = filterAdminRows(cols, rows, where)
		res, err := meta.SimpleRS(cols, rows)
		return res, true, err
	}

	return nil, false, nil
}

// serverConfigRows 汇总服务器配置键值。
func (h *Handler) serverConfigRows() [][]any {
	cfg := h.cfg
	dbs := strings.Join([]string{}, ",")
	_ = dbs
	var dbNames []string
	for _, d := range h.backend.ListDatabases() {
		dbNames = append(dbNames, d)
	}
	kv := [][2]string{
		{"server", "GlacierTDB"},
		{"glacier_version", config.GlacierVersion},
		{"listen", cfg.Listen},
		{"server_version", cfg.ServerVersion},
		{"db_dir", cfg.DBDir},
		{"default_db", cfg.DefaultDB},
		{"databases", strings.Join(dbNames, ",")},
		{"auth.user", cfg.Auth.User},
		{"auth.readonly_user", cfg.Auth.ReadOnlyUser},
		{"memory_limit_mb", fmt.Sprintf("%d", cfg.MemoryLimitMB)},
		{"max_connections", fmt.Sprintf("%d", cfg.MaxConnections)},
		{"current_connections", fmt.Sprintf("%d", h.registry.Count())},
		{"sqlite.synchronous", cfg.SQLite.Synchronous},
		{"sqlite.busy_timeout_ms", fmt.Sprintf("%d", cfg.SQLite.BusyTimeoutMs)},
		{"sqlite.cache_size_kb", fmt.Sprintf("%d", cfg.SQLite.CacheSizeKB)},
		{"sqlite.max_result_rows", fmt.Sprintf("%d", cfg.SQLite.MaxResultRows)},
		{"log.level", cfg.Log.Level},
		{"log.slow_query_ms", fmt.Sprintf("%d", cfg.Log.SlowQueryMs)},
	}
	for k, v := range h.backend.ConcurrencyInfo() {
		kv = append(kv, [2]string{k, v})
	}
	rows := make([][]any, 0, len(kv))
	for _, p := range kv {
		rows = append(rows, []any{p[0], p[1]})
	}
	return rows
}

// filterAdminRows 支持最简 WHERE：col = 'v' / col LIKE 'p%'（AND 连接）。
func filterAdminRows(cols []string, rows [][]any, where string) [][]any {
	if strings.TrimSpace(where) == "" {
		return rows
	}
	type cond struct{ col, op, val string }
	var conds []cond
	// 值支持带引号字符串与裸数字/标识符：col = 'v' / col = 123 / col LIKE 'p%'
	reCond := regexp.MustCompile(`(?i)(\w+)\s*(=|LIKE)\s*('(?:[^']|'')*'|[A-Za-z0-9_.-]+)`)
	for _, m := range reCond.FindAllStringSubmatch(where, -1) {
		val := m[3]
		if len(val) >= 2 && val[0] == '\'' && val[len(val)-1] == '\'' {
			val = strings.ReplaceAll(val[1:len(val)-1], "''", "'")
		}
		conds = append(conds, cond{strings.ToLower(m[1]), strings.ToUpper(m[2]), val})
	}
	if len(conds) == 0 {
		return rows
	}
	idx := func(name string) int {
		for i, c := range cols {
			if strings.EqualFold(c, name) {
				return i
			}
		}
		return -1
	}
	var out [][]any
	for _, r := range rows {
		match := true
		for _, c := range conds {
			i := idx(c.col)
			if i < 0 || i >= len(r) {
				match = false
				break
			}
			cell := fmt.Sprintf("%v", r[i])
			if c.op == "LIKE" {
				if !meta.LikeMatchPub(c.val, cell) {
					match = false
					break
				}
			} else if cell != c.val {
				match = false
				break
			}
		}
		if match {
			out = append(out, r)
		}
	}
	return out
}

func b2s(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

// Snapshot 实现 admin.Target。
func (h *Handler) Snapshot() admin.ConnInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	info := admin.ConnInfo{
		ID:               h.connID,
		User:             h.user,
		Remote:           h.remote,
		DB:               "GlacierTDB",
		AdminMode:        h.adminMode,
		ConnectedSec:     int64(time.Since(h.startedAt).Seconds()),
		InTx:             h.session != nil && h.session.InTx,
		HoldingWriteLock: h.session != nil && h.session.HoldsWriteLock(),
		LastQuery:        h.lastQuery,
	}
	if h.adminMode {
		info.DB = AdminDB
	} else if h.session != nil {
		info.DB = h.session.CurrentDB()
	}
	if h.session != nil && h.session.InTx && !h.txStartedAt.IsZero() {
		info.TxSec = int64(time.Since(h.txStartedAt).Seconds())
	}
	return info
}

// Kill 实现 admin.Target：主动断开该客户端。
func (h *Handler) Kill() {
	if h.mc != nil {
		h.mc.Close()
	}
}
