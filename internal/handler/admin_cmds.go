package handler

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/admin"
	"glaciertdb/internal/auth"
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
	reAdminSelect     = regexp.MustCompile("(?i)^SELECT\\s+(.+?)\\s+FROM\\s+(server_config|clients|transactions|qos_status|users|user_grants|user_hosts|login_attempts|locked|s3_usage|s3_buckets|s3_keys|s3_lifecycle|s3_domains|s3_uploads|kv_stats|mq_queues|mq_exchanges)(?:\\s+WHERE\\s+(.+?))?(?:\\s+ORDER\\s+BY\\s+.+?)?(?:\\s+LIMIT\\s+\\d+(?:\\s*,\\s*\\d+)?)?\\s*$")
	reKillConn        = regexp.MustCompile(`(?i)^KILL\s+(?:CONNECTION\s+|QUERY\s+)?(\d+)$`)
	reUseAdmin        = regexp.MustCompile("(?i)^USE\\s+`?(GlacierTDB|glacier)`?\\s*$") // 管理库别名：glacier 为系统内置名，禁止建库
	reAdminSetConc    = regexp.MustCompile(`(?i)^SET\s+(GLOBAL_THREADS|PER_DB_READ_THREADS|DB_READ_THREADS)\s*(?:([A-Za-z0-9_-]+)\s*)?=\s*(\d+)\s*$`)

	// ---- QoS 热改 / 查询（v1.0.10）----
	reAdminSetQos   = regexp.MustCompile(`(?i)^SET\s+QOS\s+(GLOBAL|DB\s+([A-Za-z0-9_-]+))\s+(QPS|CONCURRENCY|ROWS_PER_SEC)\s*=\s*(\d+)\s*$`)
	reAdminQosClear = regexp.MustCompile(`(?i)^SET\s+QOS\s+(GLOBAL|DB\s+([A-Za-z0-9_-]+))\s+CLEAR\s*$`)
	reAdminShowQos  = regexp.MustCompile(`(?i)^SHOW\s+QOS\s*$`)

	// ---- 用户权限管理（v1.0.10）----
	reAdminCreateUser = regexp.MustCompile(`(?i)^CREATE\s+USER\s+'([^']{1,64})'(?:\s*@\s*'([^']{1,64})')?\s+IDENTIFIED\s+(?:WITH\s+([A-Za-z0-9_]+)\s+)?BY\s+'([^']{1,64})'(?:\s+(SUPER|READONLY|USER))?\s*$`)
	reAdminDropUser   = regexp.MustCompile(`(?i)^DROP\s+USER\s+'([^']{1,64})'(?:\s*@\s*'[^']*')?\s*$`)
	reAdminRenameUser = regexp.MustCompile(`(?i)^RENAME\s+USER\s+'([^']{1,64})'\s+TO\s+'([^']{1,64})'\s*$`)
	reAdminAlterType  = regexp.MustCompile(`(?i)^ALTER\s+USER\s+'([^']{1,64})'\s+(SUPER|READONLY|USER)\s*$`)
	// ALTER USER ... IDENTIFIED WITH <plugin> [BY 'pwd']（v1.1.1：caching_sha2_password 等登录算法）
	reAdminAlterPlugin = regexp.MustCompile(`(?i)^ALTER\s+USER\s+'([^']{1,64})'\s+IDENTIFIED\s+WITH\s+([A-Za-z0-9_]+)(?:\s+BY\s+'([^']{1,64})')?\s*$`)
	reAdminGrant      = regexp.MustCompile(`(?i)^GRANT\s+(.+?)\s+ON\s+([^\s.]+)\s*\.\s*([^\s]+)\s+TO\s+'([^']{1,64})'(?:\s+IDENTIFIED\s+BY\s+'([^']{1,64})')?(?:\s+WITH\s+GRANT\s+OPTION)?\s*$`)
	reAdminRevoke     = regexp.MustCompile(`(?i)^REVOKE\s+(.+?)\s+ON\s+([^\s.]+)\s*\.\s*([^\s]+)\s+FROM\s+'([^']{1,64})'\s*$`)
	reAdminRevokeAll  = regexp.MustCompile(`(?i)^REVOKE\s+ALL\s+PRIVILEGES(?:\s*,\s*GRANT\s+OPTION)?\s+FROM\s+'([^']{1,64})'\s*$`)
	reAdminShowGrants = regexp.MustCompile(`(?i)^SHOW\s+GRANTS(?:\s+FOR\s+'([^']+)')?\s*$`)

	// ---- 防爆破审计 / 解锁（v1.0.10）----
	reAdminShowAtt  = regexp.MustCompile(`(?i)^SHOW\s+LOGIN\s+ATTEMPTS(?:\s+LIMIT\s+(\d+))?\s*$`)
	reAdminShowLock = regexp.MustCompile(`(?i)^SHOW\s+LOCKED\s*$`)
	reAdminUnlock   = regexp.MustCompile(`(?i)^UNLOCK\s+(ACCOUNT\s+(\S+)|IP\s+(\S+)|ALL)\s*$`)
)

// IsAdminUse 判断是否进入/退出管理库的 USE 语句（handler 在探活层之前拦截）。
func IsAdminUse(q string) (adminMode bool, ok bool) {
	if reUseAdmin.MatchString(q) {
		return true, true
	}
	return false, false
}

var reVacuumDB = regexp.MustCompile("(?i)^VACUUM\\s+DATABASE\\s+`?([A-Za-z0-9_-]+)`?\\s*$")

var reRebuildRollup = regexp.MustCompile("(?i)^REBUILD\\s+ROLLUP\\s+`?([A-Za-z0-9_-]+)`?\\s*$")

// handleRebuildRollup REBUILD ROLLUP <db>：从原始 points 回填 m1/h1/d1 汇总表（v1.1.1）。
// 回填期间该库写入持写锁；触发器保证后续写入同遍落。
func (h *Handler) handleRebuildRollup(q string) (*mysql.Result, bool, error) {
	m := reRebuildRollup.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	if h.readonly {
		return nil, true, roErr()
	}
	if h.userType != auth.TypeSuper {
		return nil, true, mysql.NewError(1142, "command denied to user '"+h.user+"' (REBUILD ROLLUP requires super user)")
	}
	if h.session.InTx {
		return nil, true, mysql.NewError(1192, "can't execute REBUILD ROLLUP with active transaction")
	}
	db := m[1]
	old := h.session.CurrentDB()
	if err := h.session.Use(db); err != nil {
		return nil, true, mysql.NewError(mysql.ER_BAD_DB_ERROR, err.Error())
	}
	defer func() { _ = h.session.Use(old) }()
	h.session.AcquireWrite()
	defer h.session.ReleaseWrite()
	cols, rows, err := h.rebuildRollup()
	if err != nil {
		return nil, true, mysql.NewError(1105, err.Error())
	}
	logx.Infof("rollup rebuilt for db %s by conn %d", db, h.connID)
	res, err2 := meta.SimpleRS(cols, rows)
	return res, true, err2
}

// handleVacuumDB 管理面 VACUUM —— ⚠️ 执行期间该库全部写入被阻塞（写锁长持）。
func (h *Handler) handleVacuumDB(q string) (*mysql.Result, bool, error) {
	m := reVacuumDB.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	if h.readonly {
		return nil, true, roErr()
	}
	if h.userType != auth.TypeSuper {
		return nil, true, mysql.NewError(1142, "command denied to user '"+h.user+"' (VACUUM requires super user)")
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

// handleKill 处理 KILL <id>（全局生效，不限管理面；只读/普通用户拒绝）。
func (h *Handler) handleKill(q string) (handled bool, err error) {
	m := reKillConn.FindStringSubmatch(q)
	if m == nil {
		return false, nil
	}
	if h.readonly {
		return true, roErr()
	}
	if h.userType != auth.TypeSuper {
		return true, mysql.NewError(1142, "command denied to user '"+h.user+"' (KILL requires super user)")
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
	// 对象存储管理命令（v1.1.0：桶 / 密钥 / 策略 / 生命周期 / 域名证书 / 用量）
	if res, handled, err := h.handleS3Admin(q); handled || err != nil {
		return res, true, err
	}

	// 键值/队列管理命令（v1.2.0：kv_stats / 内存上限 / 淘汰策略）
	if res, handled, err := h.handleKVAdmin(q); handled || err != nil {
		return res, true, err
	}

	// 消息队列管理命令（v1.3.0：SHOW QUEUES / SHOW EXCHANGES）
	if res, handled, err := h.handleMQAdmin(q); handled || err != nil {
		return res, true, err
	}

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
		if h.userType != auth.TypeSuper {
			return nil, true, mysql.NewError(1142, "command denied to user '"+h.user+"' (SET PASSWORD requires super user)")
		}
		m := reAdminSetPwd.FindStringSubmatch(q)
		newPwd := m[3]
		if strings.TrimSpace(newPwd) == "" {
			return nil, true, mysql.NewError(1819, "Your password does not satisfy the current policy requirements (empty)")
		}
		// 目标账号：SET PASSWORD FOR <user> / ALTER USER <user>（缺省 = 管理员自己）；
		// 用户系统为主（glacier_auth.db），同时镜像 legacy glacier.json（兼容旧版策略）
		target := strings.Trim(strings.TrimSpace(m[1]+m[2]), "`\"'")
		if target == "" {
			target = h.cfg.Auth.User
		}
		if h.auths != nil {
			if err := h.auths.SetPassword(target, newPwd); err != nil {
				return nil, true, mysql.NewError(1105, err.Error())
			}
		}
		h.invalidateAuthCache(target) // caching_sha2_password 缓存失效（防旧密码仍可登录）
		// legacy 镜像（仅两个内置账号有对应字段）
		if target == h.cfg.Auth.User {
			_ = h.backend.SetAuthPassword(newPwd)
		} else if h.cfg.Auth.ReadOnlyUser != "" && strings.EqualFold(target, h.cfg.Auth.ReadOnlyUser) {
			_ = h.backend.SetReadonlyPassword(newPwd)
		}
		logx.Infof("auth password changed for %s via admin command (by conn %d)", target, h.connID)
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

	// ---- QoS（v1.0.10）----
	case reAdminSetQos.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminSetQos.FindStringSubmatch(q)
		val := 0
		fmt.Sscanf(m[4], "%d", &val)
		lim := h.qosLimitFor(m[1], m[2])
		switch strings.ToUpper(m[3]) {
		case "QPS":
			lim.Qps = val
		case "CONCURRENCY":
			lim.Concurrency = val
		case "ROWS_PER_SEC":
			lim.RowsPerSec = int64(val)
		}
		if strings.EqualFold(m[1], "GLOBAL") {
			h.backend.SetQosGlobal(lim)
		} else {
			h.backend.SetQosDB(m[2], lim)
		}
		logx.Infof("qos %s %s set to %d via admin command (runtime only, by conn %d)", m[1], m[3], val, h.connID)
		return meta.OKResultPub(), true, nil

	case reAdminQosClear.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminQosClear.FindStringSubmatch(q)
		if strings.EqualFold(m[1], "GLOBAL") {
			h.backend.SetQosGlobal(sqliteQosLimitZero())
		} else {
			h.backend.ClearQosDB(m[2])
		}
		return meta.OKResultPub(), true, nil

	case reAdminShowQos.MatchString(q):
		rows := h.qosRows()
		res, err := meta.SimpleRS([]string{"scope", "qps", "concurrency", "rows_per_sec", "passed", "queued", "rejected", "waiting", "avg_wait_ms"}, rows)
		return res, true, err

	// ---- 用户权限管理（v1.0.10）----
	case reAdminCreateUser.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminCreateUser.FindStringSubmatch(q)
		typ := auth.TypeUser
		if m[5] != "" {
			switch strings.ToUpper(m[5]) {
			case "SUPER":
				typ = auth.TypeSuper
			case "READONLY":
				typ = auth.TypeReadonly
			}
		}
		if err := h.auths.CreateUser(m[1], m[4], m[2], typ, m[3]); err != nil {
			return nil, true, mysql.NewError(1396, "create user failed: "+err.Error())
		}
		logx.Infof("user %s@%s created (type=%s, plugin=%s) by conn %d", m[1], m[2], typ, m[3], h.connID)
		return meta.OKResultPub(), true, nil

	case reAdminDropUser.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminDropUser.FindStringSubmatch(q)
		if err := h.auths.DropUser(m[1]); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		logx.Infof("user %s dropped by conn %d", m[1], h.connID)
		return meta.OKResultPub(), true, nil

	case reAdminRenameUser.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminRenameUser.FindStringSubmatch(q)
		if err := h.auths.RenameUser(m[1], m[2]); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		return meta.OKResultPub(), true, nil

	case reAdminAlterType.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminAlterType.FindStringSubmatch(q)
		if err := h.auths.SetAccountType(m[1], strings.ToLower(m[2])); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		logx.Infof("user %s account_type set to %s by conn %d", m[1], m[2], h.connID)
		return meta.OKResultPub(), true, nil

	case reAdminAlterPlugin.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminAlterPlugin.FindStringSubmatch(q)
		if err := h.auths.SetPlugin(m[1], m[2]); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		if m[3] != "" {
			if err := h.auths.SetPassword(m[1], m[3]); err != nil {
				return nil, true, mysql.NewError(1105, err.Error())
			}
		}
		h.invalidateAuthCache(m[1])
		logx.Infof("user %s auth plugin set to %s by conn %d", m[1], m[2], h.connID)
		return meta.OKResultPub(), true, nil

	case reAdminGrant.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminGrant.FindStringSubmatch(q)
		if err := h.auths.Grant(m[4], unquoteGrantIdent(m[2]), unquoteGrantIdent(m[3]), m[1],
			strings.Contains(strings.ToUpper(q), "GRANT OPTION")); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		if m[5] != "" {
			_ = h.auths.SetPassword(m[4], m[5])
			h.invalidateAuthCache(m[4])
		}
		logx.Infof("granted %s ON %s.%s TO %s by conn %d", m[1], m[2], m[3], m[4], h.connID)
		return meta.OKResultPub(), true, nil

	case reAdminRevoke.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminRevoke.FindStringSubmatch(q)
		if err := h.auths.Revoke(m[4], unquoteGrantIdent(m[2]), unquoteGrantIdent(m[3]), m[1]); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		return meta.OKResultPub(), true, nil

	case reAdminRevokeAll.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminRevokeAll.FindStringSubmatch(q)
		if err := h.auths.Revoke(m[1], "*", "*", ""); err != nil {
			return nil, true, mysql.NewError(1105, err.Error())
		}
		return meta.OKResultPub(), true, nil

	case reAdminShowGrants.MatchString(q):
		m := reAdminShowGrants.FindStringSubmatch(q)
		who := m[1]
		if who == "" {
			who = h.user
		}
		rows := h.grantRows(who)
		res, err := meta.SimpleRS([]string{"Grants for " + who + "@%"}, rows)
		return res, true, err

	// ---- 防爆破审计 / 解锁（v1.0.10）----
	case reAdminShowAtt.MatchString(q):
		limit := 100
		if m := reAdminShowAtt.FindStringSubmatch(q); m != nil && m[1] != "" {
			fmt.Sscanf(m[1], "%d", &limit)
		}
		rows := h.attemptRows(limit)
		res, err := meta.SimpleRS([]string{"Id", "Time", "IP", "Remote", "User", "Success", "Reason"}, rows)
		return res, true, err

	case reAdminShowLock.MatchString(q):
		rows := h.lockRows()
		res, err := meta.SimpleRS([]string{"LockKey", "LockType", "LockedAt", "Until", "FailCount", "Level"}, rows)
		return res, true, err

	case reAdminUnlock.MatchString(q):
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		m := reAdminUnlock.FindStringSubmatch(q)
		switch {
		case m[2] != "":
			_ = h.auths.UnlockAccount(strings.Trim(m[2], "`\"'"))
		case m[3] != "":
			_ = h.auths.UnlockIP(strings.Trim(m[3], "`\"'"))
		default:
			for _, r := range h.auths.Locks() {
				if r.LockType == "ip" {
					_ = h.auths.UnlockIP(strings.TrimPrefix(r.LockKey, "ip:"))
				} else {
					_ = h.auths.UnlockAccount(strings.TrimPrefix(r.LockKey, "acct:"))
				}
			}
		}
		logx.Infof("unlock executed by conn %d: %s", h.connID, q)
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
		case "qos_status":
			cols = []string{"scope", "qps", "concurrency", "rows_per_sec", "passed", "queued", "rejected", "waiting", "avg_wait_ms"}
			rows = h.qosRows()
		case "users":
			cols = []string{"username", "account_type", "default_db", "locked", "locked_until"}
			rows = h.userRows()
		case "user_grants":
			cols = []string{"username", "db_name", "table_name", "privs", "grant_option"}
			rows = h.userGrantRows()
		case "user_hosts":
			cols = []string{"username", "host_pattern"}
			rows = h.userHostRows()
		case "login_attempts":
			cols = []string{"Id", "Time", "IP", "Remote", "User", "Success", "Reason"}
			rows = h.attemptRows(200)
		case "locked":
			cols = []string{"LockKey", "LockType", "LockedAt", "Until", "FailCount", "Level"}
			rows = h.lockRows()
		case "s3_usage":
			cols = []string{"owner", "bucket", "get_count", "put_count", "list_count", "delete_count",
				"bytes_in", "bytes_out", "storage_bytes", "object_count", "updated"}
			rows = h.s3UsageRows("")
		case "s3_buckets":
			cols = []string{"bucket", "owner", "region", "versioning", "object_lock",
				"objects", "storage_bytes", "callback_url", "created"}
			rows = h.s3BucketRows()
		case "s3_keys":
			cols = []string{"access_key", "owner", "status", "sk_fingerprint", "policy", "created", "last_used"}
			rows = h.s3KeyRows()
		case "s3_lifecycle":
			cols = []string{"bucket", "prefix", "days", "enabled"}
			rows = h.s3LifecycleRows()
		case "s3_domains":
			cols = []string{"host", "bucket", "cert_file", "key_file", "created"}
			rows = h.s3DomainRows()
		case "s3_uploads":
			cols = []string{"upload_id", "bucket", "key", "parts", "bytes", "created"}
			rows = h.s3UploadRows("")
		case "kv_stats":
			cols = kvStatCols
			rows = h.kvStatRows()
		case "mq_queues":
			cols = mqQueueCols
			rows = h.mqQueueRows()
		case "mq_exchanges":
			cols = mqExchangeCols
			rows = h.mqExchangeRows()
		case "mqtt_retains":
			cols = []string{"namespace", "topic", "bytes", "qos", "ts"}
			if h.mqstore != nil {
				rows = h.mqstore.RetainStats()
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
		{"mq.server_version", cfg.MQ.ServerVersion},
		{"mqtt.enable", b2s(cfg.MQ.Enable && cfg.MQ.MQTT.Enable)},
		{"mqtt.listen", cfg.MQ.MQTT.Listen},
		{"mqtt.ws_path", cfg.MQ.MQTT.WsPath},
		{"timeseries.cold_after_days", fmt.Sprintf("%d", cfg.Timeseries.ColdAfterDays)},
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

// ---------------------------------------------------------------------------
// v1.0.10 辅助：管理命令权限门 / QoS / 用户权限视图
// ---------------------------------------------------------------------------

// adminMutateGate 管理面变更命令统一权限门：只读 1290，普通用户 1142，仅 super 放行。
func (h *Handler) adminMutateGate() error {
	if h.readonly {
		return roErr()
	}
	if h.userType != auth.TypeSuper {
		return mysql.NewError(1142, "command denied to user '"+h.user+"' (admin mutate requires super user)")
	}
	if h.auths == nil {
		return mysql.NewError(1105, "auth store unavailable")
	}
	return nil
}

// sqliteQosLimitZero 全零配额（SET QOS GLOBAL CLEAR = 不限）。
func sqliteQosLimitZero() config.QosLimit {
	return config.QosLimit{}
}

// qosLimitFor 取当前生效的某级配额（供 SET QOS 单项改写）。
func (h *Handler) qosLimitFor(scope, db string) config.QosLimit {
	for _, r := range h.backend.QoSInfo() {
		if strings.EqualFold(r.Scope, scope) || (scope != "GLOBAL" && strings.EqualFold(r.Scope, db)) {
			return config.QosLimit{Qps: r.Lim.Qps, Concurrency: r.Lim.Concurrency, RowsPerSec: r.Lim.RowsPerSec}
		}
	}
	return config.QosLimit{}
}

// qosRows SHOW QOS / qos_status 输出。
func (h *Handler) qosRows() [][]any {
	var rows [][]any
	for _, r := range h.backend.QoSInfo() {
		rows = append(rows, []any{r.Scope, r.Lim.Qps, r.Lim.Concurrency, r.Lim.RowsPerSec,
			r.Passed, r.Queued, r.Rejected, r.Waiting, r.AvgWaitMs})
	}
	return rows
}

// userRows users 表（不含密码）。
func (h *Handler) userRows() [][]any {
	var rows [][]any
	for _, u := range h.auths.Users() {
		rows = append(rows, []any{u.Username, u.Type, u.DefaultDB, b2s(u.Locked), u.LockedTo})
	}
	return rows
}

// userGrantRows user_grants 表。
func (h *Handler) userGrantRows() [][]any {
	var rows [][]any
	for _, g := range h.auths.GrantsFor("") {
		rows = append(rows, []any{g.Username, g.DB, g.Table, g.Privs, b2s(g.GrantOption)})
	}
	return rows
}

// userHostRows user_hosts 表。
func (h *Handler) userHostRows() [][]any {
	var rows [][]any
	for _, hh := range h.auths.Hosts() {
		rows = append(rows, []any{hh.Username, hh.HostPattern})
	}
	return rows
}

// grantRows SHOW GRANTS 输出（生成 MySQL 风格 GRANT 语句）。
func (h *Handler) grantRows(username string) [][]any {
	u, ok, _ := h.auths.Lookup(username)
	if !ok {
		return [][]any{{fmt.Sprintf("There is no such grant defined for user '%s'", username)}}
	}
	var rows [][]any
	switch u.Type {
	case auth.TypeSuper:
		rows = append(rows, []any{fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%' WITH GRANT OPTION", username)})
	case auth.TypeReadonly:
		rows = append(rows, []any{fmt.Sprintf("GRANT SELECT ON *.* TO '%s'@'%%'", username)})
	default:
		if gs := h.auths.GrantsFor(username); len(gs) == 0 {
			rows = append(rows, []any{fmt.Sprintf("GRANT USAGE ON *.* TO '%s'@'%%'", username)})
		}
		for _, g := range h.auths.GrantsFor(username) {
			line := fmt.Sprintf("GRANT %s ON %s.%s TO '%s'@'%%'", g.Privs, g.DB, g.Table, username)
			if g.GrantOption {
				line += " WITH GRANT OPTION"
			}
			rows = append(rows, []any{line})
		}
	}
	return rows
}

// attemptRows SHOW LOGIN ATTEMPTS 输出。
func (h *Handler) attemptRows(limit int) [][]any {
	var rows [][]any
	for _, a := range h.auths.Attempts(limit) {
		rows = append(rows, []any{a.ID, a.TS, a.IP, a.Remote, a.Username, b2s(a.Success), a.Reason})
	}
	return rows
}

// lockRows SHOW LOCKED 输出。
func (h *Handler) lockRows() [][]any {
	var rows [][]any
	for _, r := range h.auths.Locks() {
		rows = append(rows, []any{r.LockKey, r.LockType, r.LockedAt, r.UntilTS, int64(r.FailCount), int64(r.Level)})
	}
	return rows
}

// unquoteGrantIdent 去掉 GRANT/REVOKE 目标标识符引号（* 原样）。
func unquoteGrantIdent(s string) string {
	s = strings.TrimSpace(s)
	if s == "*" {
		return s
	}
	return strings.Trim(s, "`\"'[]")
}
