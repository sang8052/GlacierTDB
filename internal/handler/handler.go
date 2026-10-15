// Package handler 实现 go-mysql 的 server.Handler：
// 探活应答 → 方言改写 → 事务/写串行 → SQLite 透传。
package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	sqlite3 "github.com/ncruces/go-sqlite3"

	"glaciertdb/internal/admin"
	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/sqlite"
)

// Handler 每个客户端连接一个实例（含连接级状态）。
type Handler struct {
	cfg       *config.Config
	backend   *sqlite.Backend
	session   *sqlite.Session
	remote    string
	readonly  bool
	connID     uint64
	startedAt  time.Time
	mc         *server.Conn
	autocommit bool // 客户端视角 autocommit（SET AUTOCOMMIT=0 后写语句惰性开隐式事务）

	registry   *admin.Registry
	adminMode  bool     // USE GlacierTDB 后进入管理面
	lastQuery  string   // 最近一条语句（管理面展示）
	txStartedAt time.Time // 当前事务开启时刻
	mu         sync.Mutex // 保护 lastQuery/adminMode/txStartedAt 的跨连接读
}

var reSetAutocommit = regexp.MustCompile(`(?i)^SET\s+(?:@@?[A-Za-z_]*\.)?AUTOCOMMIT\s*=\s*([01])`)

var _ meta.Querier = (*sqlite.Session)(nil)

// New 创建连接级 handler（连到 defaultDB）。
func New(b *sqlite.Backend, cfg *config.Config, remote, defaultDB string, reg *admin.Registry) (*Handler, error) {
	s, err := b.OpenSession(cfg.Auth.User, remote, defaultDB)
	if err != nil {
		return nil, err
	}
	return &Handler{
		cfg:        cfg,
		backend:    b,
		session:    s,
		remote:     remote,
		readonly:   cfg.IsReadOnlyUser(cfg.Auth.User),
		startedAt:  time.Now(),
		autocommit: true,
		registry:   reg,
	}, nil
}

// BindConn 在 go-mysql 握手完成后回填连接并声明 autocommit 状态位
//（真 MySQL 每个 OK/EOF 都带 SERVER_STATUS_AUTOCOMMIT，客户端据此判断事务态）。
func (h *Handler) BindConn(mc *server.Conn) {
	h.mc = mc
	h.connID = uint64(mc.ConnectionID())
	mc.SetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)
	if h.registry != nil {
		h.registry.Add(h.connID, h)
	}
}

// Close 释放会话（含事务回滚与写锁释放）并从注册表注销。
func (h *Handler) Close() {
	if h.registry != nil {
		h.registry.Remove(h.connID)
	}
	if h.session != nil {
		h.session.Close()
	}
}

// MapSQLiteErr 把 SQLite 错误包装为 MySQL 协议错误。
func MapSQLiteErr(err error) error {
	if err == nil {
		return nil
	}
	var me *mysql.MyError
	if errors.As(err, &me) {
		return err
	}
	var se *sqlite3.Error
	if errors.As(err, &se) {
		switch se.Code() {
		case sqlite3.ERROR:
			return mysql.NewError(mysql.ER_PARSE_ERROR,
				"You have an error in your SQL syntax; SQLite says: "+se.Error())
		case sqlite3.READONLY, sqlite3.PERM:
			return mysql.NewError(1290,
				"The SQLite backend is running with the read-only option and this statement cannot be executed: "+se.Error())
		case sqlite3.BUSY, sqlite3.LOCKED:
			return mysql.NewError(mysql.ER_LOCK_WAIT_TIMEOUT,
				"Lock wait timeout exceeded; database is locked: "+se.Error())
		case sqlite3.CONSTRAINT:
			return mysql.NewError(mysql.ER_DUP_ENTRY,
				"Constraint violation: "+se.Error())
		case sqlite3.CANTOPEN:
			return mysql.NewError(1105,
				"Cannot open database file (is the directory writable for WAL?): "+se.Error())
		default:
			return mysql.NewError(mysql.ER_UNKNOWN_ERROR, se.Error())
		}
	}
	return mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error())
}

// ---------------------------------------------------------------------------
// server.Handler 接口
// ---------------------------------------------------------------------------

// UseDB 处理 COM_INIT_DB / USE：真实切换后端 SQLite 文件；GlacierTDB 为管理库。
func (h *Handler) UseDB(dbName string) error {
	dbName = strings.Trim(strings.TrimSpace(dbName), "`\"'")
	if strings.EqualFold(dbName, AdminDB) {
		h.mu.Lock()
		h.adminMode = true
		h.mu.Unlock()
		return nil
	}
	h.mu.Lock()
	h.adminMode = false
	h.mu.Unlock()
	if err := h.session.Use(dbName); err != nil {
		if strings.Contains(err.Error(), "transaction open") {
			return mysql.NewError(1192, err.Error())
		}
		return mysql.NewError(mysql.ER_BAD_DB_ERROR, err.Error())
	}
	return nil
}

var reReadWord = regexp.MustCompile(`(?i)^(SELECT|WITH|EXPLAIN|VALUES|TABLE)\b`)

func isReadStmt(q string) bool { return reReadWord.MatchString(q) }

// HandleQuery 处理 COM_QUERY（统一记录 SQL 审计日志后转发）。
func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	start := time.Now()
	res, err := h.handleQueryInner(query)
	h.auditSQL(start, query, res, err)
	return res, err
}

// auditSQL 写一条 SQL 审计记录（OK/ERROR、耗时、库、连接）。
func (h *Handler) auditSQL(start time.Time, query string, res *mysql.Result, err error) {
	if !logx.SQLLogEnabled() {
		return
	}
	status := "OK"
	errMsg := ""
	if err != nil {
		status = "ERROR"
		errMsg = err.Error()
	}
	rows := int64(-1)
	if res != nil && res.Resultset != nil {
		rows = int64(len(res.Resultset.RowDatas))
	}
	affected := int64(0)
	insertID := int64(0)
	if res != nil {
		affected = int64(res.AffectedRows)
		insertID = int64(res.InsertId)
	}
	h.mu.Lock()
	admin := h.adminMode
	h.mu.Unlock()
	rec := map[string]any{
		"ts":       time.Now().Format("2006-01-02 15:04:05.000"),
		"conn":     h.connID,
		"user":     h.cfg.Auth.User,
		"remote":   h.remote,
		"db":       h.session.CurrentDB(),
		"admin":    admin,
		"cost_ms":  float64(time.Since(start).Microseconds()) / 1000.0,
		"status":   status,
		"rows":     rows,
		"affected": affected,
		"insert_id": insertID,
		"sql":      truncate(strings.ReplaceAll(query, "\n", " "), 2000),
	}
	if errMsg != "" {
		rec["error"] = truncate(errMsg, 500)
	}
	if b, jerr := json.Marshal(rec); jerr == nil {
		logx.SQLAudit(string(b))
	}
}

// handleQueryInner 原查询处理主流程。
func (h *Handler) handleQueryInner(query string) (*mysql.Result, error) {
	start := time.Now()
	logx.Debugf("query [conn %d db=%s]: %s", h.connID, h.session.CurrentDB(), truncate(query, 300))
	h.mu.Lock()
	h.lastQuery = truncate(query, 200)
	h.mu.Unlock()
	s := meta.StripComments(query)
	// 剥掉「当前库名.表」前缀（客户端工具常发 backup.tbl / `backup`.`tbl`）
	s = meta.RewriteDBPrefix(s, h.session.CurrentDB())

	// USE GlacierTDB → 进入管理面；USE 普通库 → 退出管理面（在探活层之前拦截）
	if am, ok := IsAdminUse(s); ok {
		h.mu.Lock()
		h.adminMode = am
		h.mu.Unlock()
		return meta.OKResultPub(), nil
	}
	if strings.HasPrefix(strings.ToLower(s), "use ") {
		h.mu.Lock()
		h.adminMode = false
		h.mu.Unlock()
	}

	// KILL <id>（全局生效）
	if handled, err := h.handleKill(s); handled || err != nil {
		if err != nil {
			return nil, err
		}
		return meta.OKResultPub(), nil
	}
	if s == "" {
		return meta.OKResult(), nil
	}

	// SET autocommit=0/1 实装（MySQL 语义：0 = 隐式事务；此处惰性开启避免空闲持锁）
	if m := reSetAutocommit.FindStringSubmatch(s); m != nil {
		if m[1] == "1" {
			if h.session.InTx {
				_, _, err := h.session.Exec("COMMIT")
				h.session.InTx = false
				h.session.ReleaseWrite()
				if err != nil {
					return nil, MapSQLiteErr(err)
				}
			}
			h.autocommit = true
			if h.mc != nil {
				h.mc.SetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)
			}
		} else {
			h.autocommit = false
			if h.mc != nil {
				h.mc.UnsetStatus(mysql.SERVER_STATUS_AUTOCOMMIT)
			}
		}
		return meta.OKResult(), nil
	}

	h.mu.Lock()
	adminNow := h.adminMode
	h.mu.Unlock()

	// 管理面命令优先于探活拦截（否则 SET PASSWORD / SET GLOBAL_THREADS 会被通用 SET 分支吞掉）
	if adminNow {
		if res, handled, err := h.handleAdmin(s); handled || err != nil {
			return res, err
		}
	}

	ctx := &meta.Ctx{
		DBName: func() string {
			if adminNow {
				return AdminDB
			}
			return h.session.CurrentDB()
		}(),
		User:    h.cfg.Auth.User,
		Version: h.cfg.ServerVersion,
		ConnID:  h.connID,
		Remote:  h.remote,
		Sess:    h.session,
	}

	// 1) 探活/元语句
	res, handled, err := meta.HandleProbe(ctx, s)
	if err != nil {
		h.slow(start, s)
		return nil, err
	}
	if handled {
		h.slow(start, s)
		return res, nil
	}

	// 1a) DESC[RIBE] tbl → SHOW COLUMNS
	if res, ok, err := meta.HandleDescribe(ctx, s); ok || err != nil {
		h.slow(start, s)
		return res, err
	}

	// 1b) information_schema 兼容层（客户端工具直查，不透传 SQLite）
	if res, ok, err := meta.HandleInfoSchemaQuery(ctx, s); ok || err != nil {
		h.slow(start, s)
		return res, err
	}

	// 2b) 管理面：未匹配管理命令的普通 SQL 一律拒绝（管理命令已在探活前拦截）
	if h.adminMode {
		return nil, mysql.NewError(1064, "GlacierTDB is the admin database; supported: SHOW SERVER CONFIG / SHOW CLIENTS / SHOW TRANSACTIONS / SET PASSWORD='new' / SET GLOBAL_THREADS=n / SET PER_DB_READ_THREADS=n / SET DB_READ_THREADS db=n / KILL <id>, or USE <db> to exit")
	}

	// 3) 方言改写
	sq := meta.RewriteDialect(meta.RewriteLastInsertID(s))

	// 4) 文本协议占位符拒绝：真 MySQL 对 select $$ / select ? 报 1064，
	//    SQLite 却会按未绑定参数执行返回 NULL 结果集，客户端消费失步
	if meta.HasPlaceholders(sq) {
		return nil, mysql.NewError(mysql.ER_PARSE_ERROR,
			"You have an error in your SQL syntax; placeholders ($name/?) are not supported in the text protocol")
	}

	// 5) ATTACH/DETACH 禁止
	if meta.IsAttachStmt(sq) {
		return nil, mysql.NewError(mysql.ER_NOT_SUPPORTED_YET, "ATTACH/DETACH are not allowed through this proxy")
	}

	// 6) 事务控制
	switch {
	case meta.IsBegin(sq):
		if h.readonly {
			return nil, roErr()
		}
		h.session.AcquireWrite()
		if _, _, err := h.session.Exec("BEGIN IMMEDIATE"); err != nil {
			h.session.InTx = false
			h.session.ReleaseWrite()
			h.slow(start, s)
			return nil, MapSQLiteErr(err)
		}
		h.session.InTx = true
		h.setTxStart()
		h.slow(start, s)
		return meta.OKResult(), nil

	case meta.IsCommit(sq) || meta.IsRollback(sq):
		if h.session.InTx {
			stmt := "COMMIT"
			if meta.IsRollback(sq) {
				stmt = "ROLLBACK"
			}
			_, _, err := h.session.Exec(stmt)
			h.session.InTx = false
			h.session.ReleaseWrite()
			h.slow(start, s)
			if err != nil {
				return nil, MapSQLiteErr(err)
			}
		}
		return meta.OKResult(), nil

	case meta.IsSavepoint(sq):
		if !h.session.InTx {
			return nil, mysql.NewError(1305, "SAVEPOINT does not exist")
		}
		if _, _, err := h.session.Exec(sq); err != nil {
			return nil, MapSQLiteErr(err)
		}
		return meta.OKResult(), nil
	}

	// 6) 读写分发
	if isReadStmt(sq) {
		// 多核并行查询：hint /*+ PARALLEL(n) */ 触发（不满足条件自动回退串行）
		if clean, deg := parallelHint(sq); deg > 0 {
			if res, ok, err := h.tryParallel(clean, deg); ok || err != nil {
				return res, err
			}
			sq = clean
		}
		cols, rows, err := h.session.Query(sq)
		h.slow(start, s)
		if err != nil {
			return nil, MapSQLiteErr(err)
		}
		rs, err := mysql.BuildSimpleResultset(cols, rows, false)
		if err != nil {
			return nil, MapSQLiteErr(err)
		}
		binarizeBlobFields(rs, rows)
		return mysql.NewResult(rs), nil
	}

	// 写语句
	if h.readonly {
		return nil, roErr()
	}
	if !h.session.InTx {
		if h.autocommit {
			h.session.AcquireWrite()
			defer h.session.ReleaseWrite()
		} else {
			// 隐式事务（SET autocommit=0 语义）：首条写语句开启，持锁至 COMMIT/ROLLBACK/断开
			h.session.AcquireWrite()
			if _, _, err := h.session.Exec("BEGIN IMMEDIATE"); err != nil {
				h.session.ReleaseWrite()
				h.slow(start, s)
				return nil, MapSQLiteErr(err)
			}
			h.session.InTx = true
			h.setTxStart()
		}
	}
	lastID, affected, err := h.session.Exec(sq)
	h.slow(start, s)
	if err != nil {
		return nil, MapSQLiteErr(err)
	}
	return &mysql.Result{AffectedRows: uint64(affected), InsertId: uint64(lastID)}, nil
}

func roErr() error {
	return mysql.NewError(1290, "The MySQL server is running with the --read-only option so this statement cannot be executed")
}

// HandleFieldList 处理 COM_FIELD_LIST。
func (h *Handler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	_, rows, err := h.session.Query(fmt.Sprintf(`PRAGMA main.table_info(%s)`, quoteIdent(table)))
	if err != nil {
		return nil, MapSQLiteErr(err)
	}
	var fields []*mysql.Field
	for _, r := range rows {
		name := anyToStr(r[len(r)-5]) // table_info: cid,name,type,notnull,dflt,pk —— 兜底取 name
		if len(r) >= 2 {
			name = anyToStr(r[1])
		}
		if fieldWildcard != "" && !meta.LikeMatchPub(fieldWildcard, name) {
			continue
		}
		decl := ""
		if len(r) >= 3 {
			decl = anyToStr(r[2])
		}
		f := &mysql.Field{Type: declToFieldType(decl)}
		f.Name = []byte(name)
		f.Charset = 33
		fields = append(fields, f)
	}
	return fields, nil
}

// HandleStmtPrepare 预处理语句不支持（设计红线），返回错误促客户端降级。
func (h *Handler) HandleStmtPrepare(query string) (int, int, any, error) {
	return 0, 0, nil, mysql.NewError(1295,
		"This proxy does not support prepared statements; use the text protocol (COM_QUERY)")
}

// HandleStmtExecute 同上。
func (h *Handler) HandleStmtExecute(ctx any, query string, args []any) (*mysql.Result, error) {
	return nil, mysql.NewError(1295,
		"This proxy does not support prepared statements; use the text protocol (COM_QUERY)")
}

// HandleStmtClose 无操作。
func (h *Handler) HandleStmtClose(ctx any) error { return nil }

// HandleOtherCommand 其他命令统一报不支持。
func (h *Handler) HandleOtherCommand(cmd byte, data []byte) error {
	return mysql.NewError(mysql.ER_UNKNOWN_COM_ERROR, fmt.Sprintf("command 0x%02x not supported by this proxy", cmd))
}

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

func (h *Handler) slow(start time.Time, q string) {
	if ms := time.Since(start).Milliseconds(); ms >= h.session.SlowMs() {
		logx.Infof("slow query (%d ms) [conn %d]: %s", ms, h.connID, truncate(q, 300))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func anyToStr(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func declToFieldType(decl string) byte {
	u := strings.ToUpper(decl)
	switch {
	case strings.Contains(u, "INT"):
		return mysql.MYSQL_TYPE_LONGLONG
	case strings.Contains(u, "REAL"), strings.Contains(u, "FLOA"), strings.Contains(u, "DOUB"):
		return mysql.MYSQL_TYPE_DOUBLE
	case strings.Contains(u, "BLOB"):
		return mysql.MYSQL_TYPE_BLOB
	default:
		return mysql.MYSQL_TYPE_VAR_STRING
	}
}

// binarizeBlobFields 把「非 UTF-8 的 []byte 列」标记为 BLOB/二进制字符集，
// 客户端（pymysql 等）才能按 bytes 而非 utf8 文本解码。
func binarizeBlobFields(rs *mysql.Resultset, rows [][]any) {
	if rs == nil || rs.Fields == nil {
		return
	}
	for j := range rs.Fields {
		for _, row := range rows {
			if j >= len(row) {
				break
			}
			if b, ok := row[j].([]byte); ok && len(b) > 0 && !utf8.Valid(b) {
				rs.Fields[j].Type = mysql.MYSQL_TYPE_BLOB
				rs.Fields[j].Charset = 63 // binary
				rs.Fields[j].Flag |= mysql.BINARY_FLAG
				break
			}
		}
	}
}

// setTxStart 记录事务开启时刻（管理面展示用）。
func (h *Handler) setTxStart() {
	h.mu.Lock()
	h.txStartedAt = time.Now()
	h.mu.Unlock()
}
