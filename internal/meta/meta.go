// Package meta 实现探活语句应答表（@@变量 / SHOW / SET / USE / 多库管理等）。
package meta

import (
	"fmt"
	"regexp"
	"strconv"
	"sort"
	"strings"

	"glaciertdb/internal/config"
	"glaciertdb/internal/instance"
	"sync"
)

// Querier 是 meta 对后端会话的最小依赖（含多库管理）。
type Querier interface {
	Query(sql string) ([]string, [][]any, error)
	Exec(sql string) (int64, int64, error)
	CurrentDB() string
	CurrentUser() string
	ListDatabases() []string
	Use(dbName string) error
	CreateDatabase(name string) error
	DropDatabase(name string) error
}

// Ctx 每次查询的上下文。
type Ctx struct {
	DBName  string
	User    string
	Version string
	ConnID  uint64
	Remote  string
	Sess    Querier

	// 多租户过滤（v1.3.6；nil = 不限）：USER 类型账号按授权库过滤 SHOW DATABASES /
	// information_schema.SCHEMATA；USE 切库前校验。
	DBVisible func(db string) bool
	CanUseDB  func(db string) error
	// 建库/删库门禁（v1.3.11 安全修复；nil = 放行——仅本地调用方）
	CanAdminDB func() error
}

// ---------------------------------------------------------------------------
// 系统变量默认表
// ---------------------------------------------------------------------------

func DefaultVars(version string) map[string]string {
	return map[string]string{
		"auto_increment_increment": "1",
		"auto_increment_offset":    "1",
		"autocommit":               "1",
		"character_set_client":     "utf8mb4",
		"character_set_connection": "utf8mb4",
		"character_set_database":   "utf8mb4",
		"character_set_filesystem": "binary",
		"character_set_results":    "utf8mb4",
		"character_set_server":     "utf8mb4",
		"character_set_system":     "utf8mb3",
		"character_sets_dir":       "/usr/share/mysql/charsets/",
		"collation_connection":     "utf8mb4_0900_ai_ci",
		"collation_database":       "utf8mb4_0900_ai_ci",
		"collation_server":         "utf8mb4_0900_ai_ci",
		"completion_type":          "NO_CHAIN",
		"div_precision_increment":  "4",
		"foreign_key_checks":       "1",
		"hostname":                 "sqlite-proxy",
		"init_connect":             "",
		"interactive_timeout":      "28800",
		"license":                  "GPL",
		"lower_case_file_system":   "0",
		"lower_case_table_names":   "0",
		"max_allowed_packet":       "67108864",
		"max_execution_time":       "0",
		"max_join_size":            "18446744073709551615",
		"net_buffer_length":        "16384",
		"net_read_timeout":         "30",
		"net_write_timeout":        "60",
		"performance_schema":       "0",
		"sql_auto_is_null":         "0",
		"sql_mode":                 "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		"sql_notes":                "1",
		"sql_safe_updates":         "0",
		"sql_warnings":             "0",
		"system_time_zone":         "UTC",
		"time_zone":                "SYSTEM",
		"transaction_isolation":    "REPEATABLE-READ",
		"transaction_read_only":    "0",
		"tx_isolation":             "REPEATABLE-READ",
		"tx_read_only":             "0",
		"unique_checks":            "1",
		"version":                  version,
		"version_comment":          "SQLite over MySQL protocol",
		"version_compile_machine":  "x86_64",
		"version_compile_os":       "Linux",
		"wait_timeout":             "28800",
		"warning_count":            "0",
		"error_count":              "0",
	}
}

// ---------------------------------------------------------------------------
// 语句匹配
// ---------------------------------------------------------------------------

// identAlt 匹配标识符：反引号/双引号/方括号包裹，或裸名（不含引号、空白与点）。
const identAlt = "(`[^`]+`|\"[^\"]+\"|\\[[^\\]]+\\]|[^`\\[\\]\"\\s.]+)"

// unquoteIdent 去掉标识符包裹引号。
func unquoteIdent(s string) string {
	if len(s) >= 2 {
		switch s[0] {
		case '`', '"':
			if s[len(s)-1] == s[0] {
				return s[1 : len(s)-1]
			}
		case '[':
			if s[len(s)-1] == ']' {
				return s[1 : len(s)-1]
			}
		}
	}
	return s
}

var (
	reBegin      = regexp.MustCompile(`(?i)^(BEGIN(\s+.*)?|START\s+TRANSACTION(\s+.*)?)$`)
	reCommit     = regexp.MustCompile(`(?i)^COMMIT(\s+AND\s+(NO\s+)?CHAIN|\s+NO\s+RELEASE|\s+RELEASE)?$`)
	reRollback   = regexp.MustCompile(`(?i)^ROLLBACK$`)
	reSavepoint  = regexp.MustCompile(`(?i)^(SAVEPOINT|RELEASE\s+SAVEPOINT|ROLLBACK\s+TO\s+(SAVEPOINT\s+)?)\b`)
	rePragma     = regexp.MustCompile(`(?i)^PRAGMA\b`)
	reSet        = regexp.MustCompile(`(?i)^SET\b`)
	reUse        = regexp.MustCompile(`(?i)^USE\s+(.+)$`)
	reCreateDB   = regexp.MustCompile("(?is)^CREATE\\s+(DATABASE|SCHEMA)\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?`?([A-Za-z0-9_-]+)`?\\s*(?:(?:DEFAULT|CHARACTER|CHARSET|COLLATE|DEFAULT\\s+CHARACTER)\\b.*)?$")
	reDropDB     = regexp.MustCompile("(?is)^DROP\\s+(DATABASE|SCHEMA)\\s+(?:IF\\s+EXISTS\\s+)?`?([A-Za-z0-9_-]+)`?\\s*$")
	reKill       = regexp.MustCompile(`(?i)^KILL\b`)
	reFlush      = regexp.MustCompile(`(?i)^FLUSH\b`)
	reLockTables = regexp.MustCompile(`(?i)^(UNLOCK\s+TABLES|LOCK\s+TABLES\b)`)
	reDo         = regexp.MustCompile(`(?i)^DO\b`)
	reReset      = regexp.MustCompile(`(?i)^(RESET|BINLOG|PURGE|CHANGE\s+MASTER|START\s+SLAVE|STOP\s+SLAVE)\b`)

	reShowDatabases   = regexp.MustCompile(`(?i)^SHOW\s+(?:DATABASES|SCHEMAS)(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowTables      = regexp.MustCompile(`(?i)^SHOW\s+(FULL\s+)?TABLES(?:\s+(?:FROM|IN)\s+\S+)?(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	// 标识符（RE2 不支持反向引用，用 alternation 匹配可选引号包裹的表名）
	reShowColumns     = regexp.MustCompile(`(?i)^SHOW\s+(FULL\s+)?(?:COLUMNS|FIELDS)\s+(?:FROM|IN)\s+` + identAlt + `(?:\s+(?:FROM|IN)\s+\S+)?(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowIndex       = regexp.MustCompile(`(?i)^SHOW\s+(?:INDEX|INDEXES|KEYS)\s+(?:FROM|IN)\s+` + identAlt + `(?:\s+(?:FROM|IN)\s+\S+)?\s*$`)
	reShowCreate      = regexp.MustCompile(`(?i)^SHOW\s+CREATE\s+(TABLE|VIEW|DATABASE|SCHEMA)\s+(?:IF\s+NOT\s+EXISTS\s+)?` + identAlt + `\s*$`)
	reShowTableStatus = regexp.MustCompile(`(?i)^SHOW\s+TABLE\s+STATUS(?:\s+(?:FROM|IN)\s+\S+)?(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowVariables   = regexp.MustCompile(`(?i)^SHOW\s+(?:(?:GLOBAL|SESSION|LOCAL)\s+)?VARIABLES?(?:\s+(?:GLOBAL|SESSION|LOCAL))?(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowStatus      = regexp.MustCompile(`(?i)^SHOW\s+(?:(?:GLOBAL|SESSION)\s+)?STATUS(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowCollation   = regexp.MustCompile(`(?i)^SHOW\s+COLLATION(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowCharset     = regexp.MustCompile(`(?i)^SHOW\s+(?:CHARACTER\s+SET|CHARSET)(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowEngines     = regexp.MustCompile(`(?i)^SHOW\s+(?:ENGINES|STORAGE\s+ENGINES|TYPES)\s*$`)
	reShowWarnings    = regexp.MustCompile(`(?i)^SHOW\s+(?:WARNINGS|ERRORS)(?:\s+LIMIT\s+.*)?\s*$`)
	reShowCountWarn   = regexp.MustCompile(`(?i)^SHOW\s+COUNT\(\*\)\s+(?:WARNINGS|ERRORS)\s*$`)
	reShowProcesslist = regexp.MustCompile(`(?i)^SHOW\s+(?:FULL\s+)?PROCESSLIST\s*$`)
	reShowGrants      = regexp.MustCompile(`(?i)^SHOW\s+GRANTS(?:\s+FOR\s+.+)?$`)
	reShowTriggers    = regexp.MustCompile(`(?i)^SHOW\s+TRIGGERS(?:\s+(?:FROM|IN)\s+\S+)?(?:\s+LIKE\s+'((?:[^']|'')*)')?\s*$`)
	reShowProcStatus  = regexp.MustCompile(`(?i)^SHOW\s+(?:PROCEDURE|FUNCTION)\s+STATUS`)
	reShowPlugins     = regexp.MustCompile(`(?i)^SHOW\s+PLUGINS\s*$`)
	reShowPrivileges  = regexp.MustCompile(`(?i)^SHOW\s+PRIVILEGES\s*$`)
	reShowEngineStatus = regexp.MustCompile(`(?i)^SHOW\s+ENGINE\s+\S+\s+STATUS`)
	reShowBinlog      = regexp.MustCompile(`(?i)^SHOW\s+(?:MASTER\s+STATUS|BINARY\s+LOGS|BINLOG\s+EVENTS)`)
	reShowReplica     = regexp.MustCompile(`(?i)^SHOW\s+(?:SLAVE|REPLICA)\s+STATUS`)
	reShowEvents2     = regexp.MustCompile(`(?i)^SHOW\s+EVENTS\b`)
	reShowOpenTables  = regexp.MustCompile(`(?i)^SHOW\s+OPEN\s+TABLES`)
	reShowProfiles2   = regexp.MustCompile(`(?i)^SHOW\s+PROFILES?\b`)
	reShowAnything    = regexp.MustCompile(`(?i)^SHOW\b`) // 兜底：任何未覆盖的 SHOW 不透传
	reDescribe        = regexp.MustCompile("(?i)^DESC(?:RIBE)?\\s+(?:`|\"|\\[)?([^`\"\\]\\s]+)(?:`|\"|\\])?(?:\\s+(?:FULL\\s+)?(?:COLUMNS|FIELDS))?(?:\\s+FROM\\s+\\S+)?\\s*$")
	reSelect          = regexp.MustCompile(`(?i)^SELECT\s+(.+)$`)
	reTailLimit       = regexp.MustCompile(`(?i)^(.*?)\s+LIMIT\s+(\d+)(?:\s*,\s*(\d+))?\s*$`)
	reSysVarToken     = regexp.MustCompile(`(?i)^@@(session\.|global\.|local\.)?([a-z0-9_.]+)$`)
	reFnDatabase      = regexp.MustCompile(`(?i)^(?:DATABASE|SCHEMA)\s*\(\s*\)$`)
	reFnUser          = regexp.MustCompile(`(?i)^(?:USER|CURRENT_USER|SESSION_USER|SYSTEM_USER)\s*\(\s*\)$`)
	reFnConnectionID  = regexp.MustCompile(`(?i)^CONNECTION_ID\s*\(\s*\)$`)
	reFnVersion       = regexp.MustCompile(`(?i)^VERSION\s*\(\s*\)$`)
	reFnFoundRows     = regexp.MustCompile(`(?i)^FOUND_ROWS\s*\(\s*\)$`)
	reFnRowCount      = regexp.MustCompile(`(?i)^ROW_COUNT\s*\(\s*\)$`)

	reLastInsertIDCall = regexp.MustCompile(`(?i)\bLAST_INSERT_ID\s*\(\s*\)`)
	reNow              = regexp.MustCompile(`(?i)\bNOW\s*\(\s*\)`)
	reCurdate          = regexp.MustCompile(`(?i)\bCURDATE\s*\(\s*\)`)
	reCurtime          = regexp.MustCompile(`(?i)\bCURTIME\s*\(\s*\)`)
	reUnixsw           = regexp.MustCompile(`(?i)\bUNIX_TIMESTAMP\s*\(\s*\)`)
	reTrunc            = regexp.MustCompile(`(?i)^TRUNCATE\s+(?:TABLE\s+)?`)

	// ---- MySQL 8 维护语句（v1.3.3）：返回标准四列/两列结果集，不透传 ----
	reMaintAnalyze  = regexp.MustCompile(`(?i)^ANALYZE\s+(?:NO\s+WRITE_TO_BINLOG\s+|LOCAL\s+)?TABLE\b`)
	reMaintCheck    = regexp.MustCompile(`(?i)^CHECK\s+TABLE\b`)
	reMaintOptimize = regexp.MustCompile(`(?i)^OPTIMIZE\s+(?:NO\s+WRITE_TO_BINLOG\s+|LOCAL\s+)?TABLE\b`)
	reMaintRepair   = regexp.MustCompile(`(?i)^REPAIR\s+TABLE\b`)
	reMaintChecksum = regexp.MustCompile(`(?i)^CHECKSUM\s+TABLE\b`)

	// ---- 时序改写（详见 schema/timeseries.sql 约定：ts = epoch ms）----
	reInterval   = regexp.MustCompile(`(?i)\b(NOW\s*\(\s*\)|CURRENT_TIMESTAMP)(\s*([+-])\s*INTERVAL\s+'?(\d+)\s*(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?'?)`)
	reDatePred   = regexp.MustCompile("(?i)\\b(ts|time)\\s*(>=|<=|>|<|=|!=|<>)\\s*'(2\\d{3}-\\d{2}-\\d{2}(?:[ T]\\d{2}:\\d{2}(?::\\d{2})?)?)'")
	reTimeBucket = regexp.MustCompile(`(?i)\btime\s*\(\s*(\d+)\s*\)`)
	reFromUnix   = regexp.MustCompile(`(?i)\bFROM_UNIXTIME\s*\(\s*([^()]+?)\s*\)`)
	reAttach           = regexp.MustCompile(`(?i)^(ATTACH|DETACH)\b`)
)

// 语句分类辅助（handler 使用）。
func IsBegin(q string) bool      { return reBegin.MatchString(q) }
func IsCommit(q string) bool     { return reCommit.MatchString(q) }
func IsRollback(q string) bool   { return reRollback.MatchString(q) }
func IsSavepoint(q string) bool  { return reSavepoint.MatchString(q) }
func IsPragmaStmt(q string) bool { return rePragma.MatchString(q) }
func IsAttachStmt(q string) bool { return reAttach.MatchString(q) }

// RewriteLastInsertID 把 LAST_INSERT_ID() 改写为 SQLite 等价函数。
func RewriteLastInsertID(q string) string { return reLastInsertIDCall.ReplaceAllString(q, "last_insert_rowid()") }

// RewriteDialect 少量 MySQL 函数改写。
func RewriteDialect(q string) string {
	// ---- 运算符级改写（v1.3.3 M4；必须在 CONCAT→|| 展开之前：MySQL || 是 OR）----
	q = RewriteOperators(q)
	// ---- JSON 运算符（v1.0.10）----
	q = RewriteJSONOps(q) // -> / ->> → json_extract 等价形式
	// ---- 时序改写 ----
	q = expandInterval(q) // NOW() ± INTERVAL n UNIT → epoch ms 整数表达式（防 ts 列全表扫）
	if reDatePred.MatchString(q) { // ts >= 'YYYY-MM-DD ...' → 整数化（INTEGER 列与 TEXT 字面量比较会静默恒真/恒假）
		q = reDatePred.ReplaceAllString(q, `$1 $2 strftime('%s','$3')*1000`)
	}
	if reTimeBucket.MatchString(q) { // InfluxDB 风格 GROUP BY time(N) → (ts/N*N) 整数分桶
		q = reTimeBucket.ReplaceAllString(q, `(ts/$1*$1)`)
	}
	if reFromUnix.MatchString(q) { // FROM_UNIXTIME(x) → 可读时间
		q = reFromUnix.ReplaceAllString(q, `datetime($1, 'unixepoch')`)
	}
	// ---- 通用改写 ----
	if reNow.MatchString(q) {
		q = reNow.ReplaceAllString(q, "datetime('now')")
	}
	if reCurdate.MatchString(q) {
		q = reCurdate.ReplaceAllString(q, "date('now')")
	}
	if reCurtime.MatchString(q) {
		q = reCurtime.ReplaceAllString(q, "time('now')")
	}
	if reUnixsw.MatchString(q) {
		q = reUnixsw.ReplaceAllString(q, "strftime('%s','now')")
	}
	if reTrunc.MatchString(q) {
		q = reTrunc.ReplaceAllString(q, "DELETE FROM ")
	}
	// ---- MySQL 兼容 M1（LIMIT 双参 / CONCAT / IF / RAND / UUID）----
	q = RewriteMySQLCompat(q)
	// ---- MySQL 兼容 M4（v1.3.3：CAST/CONVERT/EXTRACT/DATE_ADD 族/GROUP_CONCAT SEPARATOR…）----
	q = RewriteMySQLCompat2(q)
	// ---- DDL 类型映射（仅 CREATE/ALTER TABLE 等语句生效）----
	q = RewriteDDLTypes(q)
	return q
}

// expandInterval 把 `NOW() - INTERVAL 1 HOUR` 一类表达式改写为 epoch 毫秒整数
// 算术式（MONTH=30 天、YEAR=365 天近似，冷查询场景足够）。必须在 NOW() 通用改写前执行。
func expandInterval(q string) string {
	if !reInterval.MatchString(q) {
		return q
	}
	return reInterval.ReplaceAllStringFunc(q, func(m string) string {
		sub := reInterval.FindStringSubmatch(m)
		n, _ := strconv.ParseInt(sub[4], 10, 64)
		var ms int64
		switch strings.ToUpper(sub[5]) {
		case "SECOND":
			ms = n * 1000
		case "MINUTE":
			ms = n * 60000
		case "HOUR":
			ms = n * 3600000
		case "DAY":
			ms = n * 86400000
		case "WEEK":
			ms = n * 604800000
		case "MONTH":
			ms = n * 2592000000 // 30 天
		case "YEAR":
			ms = n * 31536000000 // 365 天
		}
		return fmt.Sprintf("(strftime('%%s','now')*1000%s%d)", sub[3], ms)
	})
}

// ---------------------------------------------------------------------------
// 探活总入口：handled=true 表示已消费
// ---------------------------------------------------------------------------

// HandleProbe 处理探活/元语句。MapSQLError 在 main 里注入（避免 import cycle）。
var MapSQLError = func(err error) error { return err }

// ColumnTypeLookuper 列类型回放钩子（handler 注入 colmeta：SQLite 声明类型 →
// 建表时的 MySQL 原始类型；无映射返回 ""，v1.3.3）。
var ColumnTypeLookuper func(db, tbl, col, sqliteType string) string

// OverrideColumnType 应用列类型回放；无映射/无钩子时返回 SQLite 声明类型。
func OverrideColumnType(db, tbl, col, sqliteType string) string {
	if ColumnTypeLookuper != nil {
		if t := ColumnTypeLookuper(db, tbl, col, sqliteType); t != "" {
			return t
		}
	}
	return sqliteType
}

func HandleProbe(ctx *Ctx, q string) (res *Result, handled bool, err error) {
	if q == "" {
		return OKResult(), true, nil
	}

	// SELECT @@xxx / DATABASE() / USER() ...（支持尾部 LIMIT，mysql CLI 探活常用）
	if m := reSelect.FindStringSubmatch(q); m != nil {
		expr := m[1]
		limit, offset := int64(-1), int64(0)
		if lm := reTailLimit.FindStringSubmatch(expr); lm != nil {
			expr = lm[1]
			if lm[3] != "" {
				offset, limit = atoiSafe(lm[2]), atoiSafe(lm[3])
			} else {
				limit = atoiSafe(lm[2])
			}
		}
		if res, ok, err := handleSelectVars(ctx, expr); ok || err != nil {
			if err == nil && res != nil && res.Resultset != nil {
				applyLimit(res.Resultset, offset, limit)
			}
			return res, true, err
		}
	}

	// SHOW 家族
	if res, ok, err := handleShow(ctx, q); ok || err != nil {
		return res, true, err
	}

	// ANALYZE / CHECK / OPTIMIZE / REPAIR / CHECKSUM TABLE（v1.3.3：MySQL 标准应答形态）
	if res, ok := handleMaintTable(ctx, q); ok {
		return res, true, nil
	}

	if reSet.MatchString(q) {
		return OKResult(), true, nil
	}
	if m := reUse.FindStringSubmatch(q); m != nil {
		db := strings.Trim(strings.TrimSpace(m[1]), "`\"';")
		if ctx.CanUseDB != nil {
			if err := ctx.CanUseDB(db); err != nil {
				return nil, true, err
			}
		}
		if err := ctx.Sess.Use(db); err != nil {
			if strings.Contains(err.Error(), "transaction open") {
				return nil, true, NewMySQLError(1192, err.Error())
			}
			return nil, true, NewMySQLError(1049, err.Error())
		}
		return OKResult(), true, nil
	}

	// CREATE / DROP DATABASE（一库一文件）
	// v1.3.11 安全修复：此前任何账号（含只读）都能建/删任意租户的库——
	// 现在统一走 CanAdminDB 门禁（super 且非只读）。
	if m := reCreateDB.FindStringSubmatch(q); m != nil {
		if ctx.CanAdminDB != nil {
			if err := ctx.CanAdminDB(); err != nil {
				return nil, true, err
			}
		}
		if err := ctx.Sess.CreateDatabase(m[2]); err != nil {
			return nil, true, NewMySQLError(1007, "Can't create database '"+m[2]+"': "+err.Error())
		}
		return OKResult(), true, nil
	}
	if m := reDropDB.FindStringSubmatch(q); m != nil {
		if ctx.CanAdminDB != nil {
			if err := ctx.CanAdminDB(); err != nil {
				return nil, true, err
			}
		}
		if err := ctx.Sess.DropDatabase(m[2]); err != nil {
			return nil, true, NewMySQLError(1008, "Can't drop database '"+m[2]+"': "+err.Error())
		}
		return OKResult(), true, nil
	}
	if reKill.MatchString(q) || reFlush.MatchString(q) || reLockTables.MatchString(q) ||
		reDo.MatchString(q) || reReset.MatchString(q) {
		return OKResult(), true, nil
	}
	if rePragma.MatchString(q) {
		// 客户端 PRAGMA 一律空应答，防止改坏 WAL 等全局设置
		return OKResult(), true, nil
	}

	return nil, false, nil
}

// ---------------------------------------------------------------------------
// SELECT @@vars / 内置函数
// ---------------------------------------------------------------------------

func handleSelectVars(ctx *Ctx, expr string) (*Result, bool, error) {	tokens := splitTopLevel(expr)
	if len(tokens) == 0 {
		return nil, false, nil
	}
	vars := DefaultVars(ctx.Version)
	vars["glaciertdb_instance_id"] = instance.UUID() // 实例唯一 ID（v1.3.13 §6.2）
	cols := make([]string, 0, len(tokens))
	var row []any
	for _, tok := range tokens {
		t := strings.TrimSpace(tok)
		if t == "" {
			return nil, false, nil
		}
		switch {
		case strings.HasPrefix(t, "@@"):
			m := reSysVarToken.FindStringSubmatch(t)
			if m == nil {
				return nil, false, nil
			}
			name := strings.ToLower(m[2])
			v, ok := vars[name]
			if !ok {
				return nil, true, NewMySQLError(1193, "Unknown system variable '"+m[2]+"'")
			}
			cols = append(cols, t)
			row = append(row, v)
		case reFnDatabase.MatchString(t):
			cols = append(cols, t)
			row = append(row, ctx.DBName) // 管理面下为 GlacierTDB
		case reFnUser.MatchString(t):
			cols = append(cols, t)
			row = append(row, ctx.User+"@"+ctx.Remote)
		case reFnConnectionID.MatchString(t):
			cols = append(cols, t)
			row = append(row, int64(ctx.ConnID))
		case reFnVersion.MatchString(t):
			cols = append(cols, t)
			row = append(row, ctx.Version)
		case strings.EqualFold(t, "glacier_version"):
			cols = append(cols, "glacier_version")
			// 版本号隐藏（v1.3.13 §6.4）：security.hide_version 开启时管理面
			// 同步隐藏（启动日志与 CLI 版本输出不隐藏，本机排障用）
			if v := config.DisplayVersion(); v != "" {
				row = append(row, v)
			} else {
				row = append(row, "hidden")
			}
		case strings.EqualFold(t, "glaciertdb_instance_id"), strings.EqualFold(strings.TrimPrefix(t, "@@"), "glaciertdb_instance_id"):
			// 实例唯一 ID（v1.3.13 §6.2）：SELECT @@glaciertdb_instance_id
			cols = append(cols, t)
			row = append(row, instance.UUID())
		case reFnFoundRows.MatchString(t):
			cols = append(cols, t)
			row = append(row, int64(0))
		case reFnRowCount.MatchString(t):
			cols = append(cols, t)
			row = append(row, int64(-1))
		default:
			// 普通表达式 → 交给 SQLite 透传
			return nil, false, nil
		}
	}
	res, err := simpleRS(cols, [][]any{row})
	return res, true, err
}

// splitTopLevel 按顶层逗号切分（忽略括号与引号内逗号）。
func splitTopLevel(s string) []string {
	var out []string
	depth := 0
	var quote byte
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	out = append(out, s[start:])
	return out
}

// ---------------------------------------------------------------------------
// SHOW 家族
// ---------------------------------------------------------------------------

func handleShow(ctx *Ctx, q string) (*Result, bool, error) {
	switch {
	case reShowDatabases.MatchString(q):
		var rows [][]any
		pat := ""
		if m := reShowDatabases.FindStringSubmatch(q); m != nil {
			pat = m[1]
		}
		for _, d := range ctx.Sess.ListDatabases() {
			if pat != "" && !likeMatch(pat, d) {
				continue
			}
			if ctx.DBVisible != nil && !ctx.DBVisible(d) {
				continue
			}
			rows = append(rows, []any{d})
		}
		if pat == "" || likeMatch(pat, "GlacierTDB") {
			rows = append(rows, []any{"GlacierTDB"}) // 管理库（USE GlacierTDB 进入管理面）
		}
		res, err := simpleRS([]string{"Database"}, rows)
		return res, true, err

	case reShowTables.MatchString(q):
		m := reShowTables.FindStringSubmatch(q)
		full := m[1] != ""
		pattern := m[2]
		cols := []string{"Tables_in_" + ctx.Sess.CurrentDB()}
		if full {
			cols = append(cols, "Table_type")
		}
		_, rows, err := ctx.Sess.Query("SELECT name, type FROM main.sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
		if err != nil {
			return nil, true, MapSQLError(err)
		}
		var out [][]any
		for _, r := range rows {
			name, _ := r[0].(string)
			typ, _ := r[1].(string)
			if pattern != "" && !likeMatch(pattern, name) {
				continue
			}
			if full {
				tt := "BASE TABLE"
				if typ == "view" {
					tt = "VIEW"
				}
				out = append(out, []any{name, tt})
			} else {
				out = append(out, []any{name})
			}
		}
		res, err := simpleRS(cols, out)
		return res, true, err

	case reShowColumns.MatchString(q):
		return showColumns(ctx, q)

	case reShowIndex.MatchString(q):
		return showIndex(ctx, q)

	case reShowCreate.MatchString(q):
		return showCreate(ctx, q)

	case reShowTableStatus.MatchString(q):
		return showTableStatus(ctx, q)

	case reShowVariables.MatchString(q):
		m := reShowVariables.FindStringSubmatch(q)
		return showVariables(ctx, m[1])

	case reShowStatus.MatchString(q):
		m := reShowStatus.FindStringSubmatch(q)
		rows := [][]any{
			{"Uptime", "1000"},
			{"Threads_connected", "1"},
			{"Threads_running", "1"},
			{"Ssl_cipher", ""},
			{"Ssl_version", ""},
		}
		if m[1] != "" {
			var out [][]any
			for _, r := range rows {
				if likeMatch(m[1], r[0].(string)) {
					out = append(out, r)
				}
			}
			rows = out
		}
		res, err := simpleRS([]string{"Variable_name", "Value"}, rows)
		return res, true, err

	case reShowCollation.MatchString(q):
		m := reShowCollation.FindStringSubmatch(q)
		all := [][]any{
			{"utf8mb4_0900_ai_ci", "utf8mb4", int64(255), "Yes", "Yes", int64(0)},
			{"utf8mb4_general_ci", "utf8mb4", int64(45), "", "Yes", int64(1)},
			{"utf8mb4_bin", "utf8mb4", int64(46), "", "Yes", int64(1)},
			{"utf8mb3_general_ci", "utf8mb3", int64(33), "", "Yes", int64(1)},
			{"latin1_swedish_ci", "latin1", int64(8), "Yes", "Yes", int64(1)},
			{"binary", "binary", int64(63), "Yes", "Yes", int64(1)},
		}
		var out [][]any
		for _, r := range all {
			if m[1] == "" || likeMatch(m[1], r[0].(string)) {
				out = append(out, r)
			}
		}
		res, err := simpleRS([]string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}, out)
		return res, true, err

	case reShowCharset.MatchString(q):
		m := reShowCharset.FindStringSubmatch(q)
		all := [][]any{
			{"utf8mb4", int64(255), "utf8mb4_0900_ai_ci", "UTF-8 Unicode", int64(4)},
			{"utf8mb3", int64(33), "utf8mb3_general_ci", "UTF-8 Unicode", int64(3)},
			{"latin1", int64(8), "latin1_swedish_ci", "cp1252 West European", int64(1)},
			{"binary", int64(63), "binary", "Binary pseudo charset", int64(1)},
			{"ascii", int64(65), "ascii_general_ci", "US ASCII", int64(1)},
		}
		var out [][]any
		for _, r := range all {
			if m[1] == "" || likeMatch(m[1], r[0].(string)) {
				out = append(out, r)
			}
		}
		res, err := simpleRS([]string{"Charset", "Id", "Default collation", "Description", "Maxlen"}, out)
		return res, true, err

	case reShowEngines.MatchString(q):
		rows := [][]any{
			{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
			{"MEMORY", "YES", "Hash based, stored in memory, useful for temporary tables", "NO", "NO", "NO"},
			{"MyISAM", "YES", "MyISAM storage engine", "NO", "NO", "NO"},
			{"CSV", "YES", "CSV storage engine", "NO", "NO", "NO"},
			{"ARCHIVE", "YES", "Archive storage engine", "NO", "NO", "NO"},
			{"BLACKHOLE", "YES", "/dev/null storage engine (anything you write to it disappears)", "NO", "NO", "NO"},
		}
		res, err := simpleRS([]string{"Engine", "Support", "Comment", "Transactions", "XA", "Savepoints"}, rows)
		return res, true, err

	case reShowWarnings.MatchString(q):
		res, err := simpleRS([]string{"Level", "Code", "Message"}, nil)
		return res, true, err

	case reShowCountWarn.MatchString(q):
		res, err := simpleRS([]string{"@@session.warning_count"}, [][]any{{int64(0)}})
		return res, true, err

	case reShowProcesslist.MatchString(q):
		row := []any{int64(ctx.ConnID), ctx.User, ctx.Remote, ctx.DBName, "Query", int64(0), "executing", nil}
		res, err := simpleRS([]string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}, [][]any{row})
		return res, true, err

	case reShowGrants.MatchString(q):
		res, err := simpleRS([]string{fmt.Sprintf("Grants for %s@%%", ctx.User)},
			[][]any{{fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO `%s`@`%%`", ctx.User)}})
		return res, true, err

	case reShowTriggers.MatchString(q):
		m := reShowTriggers.FindStringSubmatch(q)
		_, rows, err := ctx.Sess.Query("SELECT name, tbl_name, sql FROM main.sqlite_master WHERE type='trigger' ORDER BY name")
		if err != nil {
			return nil, true, MapSQLError(err)
		}
		var out [][]any
		for _, r := range rows {
			name, _ := r[0].(string)
			if m[1] != "" && !likeMatch(m[1], name) {
				continue
			}
			out = append(out, []any{r[0], "INSERT", r[1], r[2], "AFTER", nil, "", ctx.User, "utf8mb4", "utf8mb4_0900_ai_ci", "utf8mb4_0900_ai_ci"})
		}
		res, err := simpleRS([]string{"Trigger", "Event", "Table", "Statement", "Timing", "Created", "sql_mode", "Definer", "character_set_client", "collation_connection", "Database Collation"}, out)
		return res, true, err

	case reShowProcStatus.MatchString(q):
		res, err := simpleRS([]string{"Db", "Name", "Type", "Definer", "Modified", "Created", "Security_type", "Comment", "character_set_client", "collation_connection", "Database Collation"}, nil)
		return res, true, err

	case reShowPlugins.MatchString(q):
		res, err := simpleRS([]string{"Name", "Status", "Type", "Library", "License"}, nil)
		return res, true, err

	case reShowPrivileges.MatchString(q):
		res, err := simpleRS([]string{"Privilege", "Context", "Comment"},
			[][]any{{"All", "Server Admin", "All privileges"}, {"Select", "Tables", "To retrieve rows from table"}, {"Insert", "Tables", "To insert data into table"}, {"Update", "Tables", "To update existing rows"}, {"Delete", "Tables", "To delete existing rows"}})
		return res, true, err

	case reShowEngineStatus.MatchString(q), reShowBinlog.MatchString(q), reShowReplica.MatchString(q),
		reShowEvents2.MatchString(q), reShowOpenTables.MatchString(q), reShowProfiles2.MatchString(q):
		res, err := simpleRS([]string{"Name", "Value"}, nil)
		return res, true, err
	}

	// 兜底：任何其他 SHOW 变体（客户端工具层出不穷）一律空结果集，绝不透传 SQLite；
	// 例外：SHOW COLD（时序冷压缩命令，v1.3.3 在 handler 层处理，不能在此吞掉）
	if reShowAnything.MatchString(q) && !reShowColdHead.MatchString(q) {
		res, err := simpleRS([]string{"Name", "Value"}, nil)
		return res, true, err
	}
	return nil, false, nil
}

// reShowColdHead SHOW COLD 命令头（handler 的冷压缩命令，探活层放行）。
var reShowColdHead = regexp.MustCompile(`(?i)^SHOW\s+COLD\b`)

// HandleDescribe 处理 MySQL 的 DESC[RIBE] tbl 语法（等价 SHOW COLUMNS）。
func HandleDescribe(ctx *Ctx, q string) (*Result, bool, error) {
	m := reDescribe.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	return showColumns(ctx, "SHOW COLUMNS FROM "+m[1])
}

// handleMaintTable ANALYZE/CHECK/OPTIMIZE/REPAIR/CHECKSUM TABLE 的 MySQL 标准应答
//（v1.3.3：SQLite 无对应维护语义，回 OK 形态；CHECKSUM 回 0）。
func handleMaintTable(ctx *Ctx, q string) (*Result, bool) {
	var op string
	switch {
	case reMaintAnalyze.MatchString(q):
		op = "analyze"
	case reMaintCheck.MatchString(q):
		op = "check"
	case reMaintOptimize.MatchString(q):
		op = "optimize"
	case reMaintRepair.MatchString(q):
		op = "repair"
	case reMaintChecksum.MatchString(q):
		op = "checksum"
	default:
		return nil, false
	}
	targets := maintTableTargets(q)
	db := ctx.Sess.CurrentDB()
	if op == "checksum" {
		var rows [][]any
		for _, t := range targets {
			rows = append(rows, []any{db + "." + t, int64(0)})
		}
		res, err := simpleRS([]string{"Table", "Checksum"}, rows)
		if err != nil {
			return nil, true
		}
		return res, true
	}
	var rows [][]any
	for _, t := range targets {
		rows = append(rows, []any{db + "." + t, op, "status", "OK"})
	}
	res, err := simpleRS([]string{"Table", "Op", "Msg_type", "Msg_text"}, rows)
	if err != nil {
		return nil, true
	}
	return res, true
}

// maintTableTargets 取维护语句的表名清单（去维护关键字后按逗号切）。
func maintTableTargets(q string) []string {
	s := q
	for _, re := range []*regexp.Regexp{reMaintAnalyze, reMaintCheck, reMaintOptimize, reMaintRepair, reMaintChecksum} {
		if re.MatchString(s) {
			s = strings.TrimSpace(re.ReplaceAllString(s, ""))
			break
		}
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		t := strings.Trim(strings.TrimSpace(part), "`\"'")
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func showColumns(ctx *Ctx, q string) (*Result, bool, error) {
	m := reShowColumns.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	full := m[1] != ""
	table := unquoteIdent(m[2])
	pattern := m[3]

	_, rows, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.table_info(%s)", quoteIdent(table)))
	if err != nil {
		return nil, true, MapSQLError(err)
	}
	if len(rows) == 0 {
		return nil, true, NewMySQLError(1146, "Table '"+ctx.Sess.CurrentDB()+"."+table+"' doesn't exist")
	}
	// 索引列 → Key=MUL
	idxCols := map[string]bool{}
	if _, irows, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_list(%s)", quoteIdent(table))); err == nil {
		for _, r := range irows {
			name := rowGet(r, 1)
			origin := rowGet(r, 3)
			if origin == "c" {
				continue
			}
			if _, crows, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_info(%s)", quoteIdent(name))); err == nil {
				for _, cr := range crows {
					if cn := rowGet(cr, 2); cn != "" {
						idxCols[cn] = true
					}
				}
			}
		}
	}

	names := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	if full {
		names = append(names, "Collation", "Privileges", "Comment")
	}
	var out [][]any
	for _, r := range rows {
		name := rowGet(r, 1)
		if pattern != "" && !likeMatch(pattern, name) {
			continue
		}
		typ := OverrideColumnType(ctx.Sess.CurrentDB(), table, name, rowGet(r, 2))
		notnull := rowGet(r, 3)
		var dflt any
		if len(r) > 4 && r[4] != nil {
			dflt = toStr(r[4])
		}
		pk := toI64(rowGet(r, 5))
		nullable := "YES"
		if notnull == "1" {
			nullable = "NO"
		}
		key := ""
		if pk != 0 {
			key = "PRI"
		} else if idxCols[name] {
			key = "MUL"
		}
		extra := ""
		row := []any{name, typ, nullable, key, dflt, extra}
		if full {
			ut := strings.ToUpper(typ)
			numeric := strings.Contains(ut, "INT") || strings.Contains(ut, "REAL") ||
				strings.Contains(ut, "FLOA") || strings.Contains(ut, "DOUB")
			var collation any
			if !numeric && ut != "BLOB" {
				collation = "utf8mb4_0900_ai_ci"
			}
			row = append(row, collation, "select,insert,update,references", "")
		}
		out = append(out, row)
	}
	res, err := simpleRS(names, out)
	return res, true, err
}

func showIndex(ctx *Ctx, q string) (*Result, bool, error) {
	m := reShowIndex.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	table := unquoteIdent(m[1])
	names := []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"}
	_, ilist, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_list(%s)", quoteIdent(table)))
	if err != nil {
		return nil, true, MapSQLError(err)
	}
	var out [][]any
	for _, r := range ilist {
		idxName := rowGet(r, 1)
		unique := rowGet(r, 2) == "1"
		_, iinfo, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_info(%s)", quoteIdent(idxName)))
		if err != nil {
			continue
		}
		for seq, cr := range iinfo {
			colName := rowGet(cr, 2)
			var colVal any
			if colName != "" {
				colVal = colName
			}
			out = append(out, []any{table, b2i(!unique), idxName, int64(seq + 1), colVal,
				"A", int64(0), nil, nil, "", "BTREE", "", "", "YES", nil})
		}
	}
	res, err := simpleRS(names, out)
	return res, true, err
}

func showCreate(ctx *Ctx, q string) (*Result, bool, error) {
	m := reShowCreate.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	kind, name := m[1], unquoteIdent(m[2])
	lowerKind := strings.ToLower(kind)
	if lowerKind == "database" || lowerKind == "schema" {
		res, err := simpleRS([]string{"Database", "Create Database"},
			[][]any{{ctx.DBName, fmt.Sprintf("CREATE DATABASE `%s` /*!40100 DEFAULT CHARACTER SET utf8mb4 */", ctx.DBName)}})
		return res, true, err
	}
	_, rows, err := ctx.Sess.Query(fmt.Sprintf("SELECT sql FROM main.sqlite_master WHERE type IN ('table','view') AND name=%s LIMIT 1", quoteStr(name)))
	if err != nil {
		return nil, true, MapSQLError(err)
	}
	if len(rows) == 0 {
		return nil, true, NewMySQLError(1146, fmt.Sprintf("Table '%s.%s' doesn't exist", ctx.Sess.CurrentDB(), name))
	}
	sqlText := rowGet(rows[0], 0)
	title := "Table"
	if lowerKind == "view" {
		title = "View"
	} else {
		sqlText = buildMySQLCreateTable(ctx, name, name, sqlText)
	}
	res, err := simpleRS([]string{title, "Create " + kind}, [][]any{{name, sqlText}})
	return res, true, err
}

// buildMySQLCreateTable 把 SQLite 表结构转写为 MySQL 方言 DDL（SQLAlchemy 等客户端靠它反射列）。
func buildMySQLCreateTable(ctx *Ctx, _, table, sqliteSQL string) string {
	_, rows, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.table_info(%s)", quoteIdent(table)))
	if err != nil || len(rows) == 0 {
		return sqliteSQL
	}
	type colDef struct {
		line string
	}
	var lines []string
	var pk []string
	nameIdx := map[string]int{}
	for _, r := range rows {
		name := rowGet(r, 1)
		typ := OverrideColumnType(ctx.Sess.CurrentDB(), table, name, rowGet(r, 2))
		notnull := rowGet(r, 3) == "1"
		dflt := rowGet(r, 4)
		pkOrd := toI64(rowGet(r, 5))
		line := "`" + name + "` " + typ
		if notnull || pkOrd > 0 {
			line += " NOT NULL"
		}
		if dflt != "" {
			line += " DEFAULT " + dflt
		}
		nameIdx[name] = len(lines)
		lines = append(lines, line)
		if pkOrd > 0 {
			for len(pk) < int(pkOrd) {
				pk = append(pk, "")
			}
			pk[pkOrd-1] = name
		}
	}
	// 单列整型主键 → AUTO_INCREMENT
	if len(pk) == 1 {
		for _, r := range rows {
			if rowGet(r, 1) == pk[0] && strings.Contains(strings.ToUpper(rowGet(r, 2)), "INT") {
				if i, ok := nameIdx[pk[0]]; ok {
					lines[i] += " AUTO_INCREMENT"
				}
			}
		}
	}
	if len(pk) > 0 {
		quoted := make([]string, 0, len(pk))
		for _, c := range pk {
			if c != "" {
				quoted = append(quoted, "`"+c+"`")
			}
		}
		if len(quoted) > 0 {
			lines = append(lines, "PRIMARY KEY ("+strings.Join(quoted, ", ")+")")
		}
	}
	return fmt.Sprintf("CREATE TABLE `%s` (\n  %s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
		table, strings.Join(lines, ",\n  "))
}

func showTableStatus(ctx *Ctx, q string) (*Result, bool, error) {
	m := reShowTableStatus.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	pattern := m[1]
	_, rows, err := ctx.Sess.Query("SELECT name, type FROM main.sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
	if err != nil {
		return nil, true, MapSQLError(err)
	}
	names := []string{"Name", "Engine", "Version", "Row_format", "Rows", "Avg_row_length", "Data_length", "Max_data_length", "Index_length", "Data_free", "Auto_increment", "Create_time", "Update_time", "Check_time", "Collation", "Checksum", "Create_options", "Comment"}
	var out [][]any
	for _, r := range rows {
		name := toStr(r[0])
		if pattern != "" && !likeMatch(pattern, name) {
			continue
		}
		var cnt int64
		if r[1] == "table" {
			if _, crows, err := ctx.Sess.Query(fmt.Sprintf("SELECT count(*) FROM main.%s", quoteIdent(name))); err == nil && len(crows) > 0 {
				cnt = toI64(rowGet(crows[0], 0))
			}
		}
		engine := "SQLite"
		comment := ""
		if r[1] == "view" {
			engine = ""
			comment = "VIEW"
		}
		out = append(out, []any{name, engine, int64(10), "Dynamic", cnt, int64(0), int64(0), int64(0), int64(0), int64(0), nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, "", comment})
	}
	res, err := simpleRS(names, out)
	return res, true, err
}

func showVariables(ctx *Ctx, pattern string) (*Result, bool, error) {
	vars := DefaultVars(ctx.Version)
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out [][]any
	for _, k := range keys {
		if pattern != "" && !likeMatch(pattern, k) {
			continue
		}
		v := vars[k]
		if k == "autocommit" {
			v = "ON"
		}
		out = append(out, []any{k, v})
	}
	res, err := simpleRS([]string{"Variable_name", "Value"}, out)
	return res, true, err
}

// ---------------------------------------------------------------------------
// 工具
// ---------------------------------------------------------------------------

func toStr(v any) string {
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

func toI64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	case []byte:
		var n int64
		fmt.Sscanf(string(x), "%d", &n)
		return n
	case string:
		var n int64
		fmt.Sscanf(x, "%d", &n)
		return n
	}
	return 0
}

func b2i(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// quoteIdent 双引号包裹标识符。
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteStr 单引号包裹字符串字面量。
func quoteStr(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// likeRegexpCache LIKE 模式 → 编译正则缓存（v1.3.11 性能：此前每次调用现场
// 编译正则，information_schema / SHOW 过滤逐行编译，O(rows) 次编译开销）。
var likeRegexpCache sync.Map // string → *regexp.Regexp

// likeMatch 实现 MySQL LIKE（% _）语义。
func likeMatch(pattern, s string) bool {
	var re *regexp.Regexp
	if v, ok := likeRegexpCache.Load(pattern); ok {
		re = v.(*regexp.Regexp)
	} else {
		var sb strings.Builder
		sb.WriteString("(?s)\\A")
		for _, c := range pattern {
			switch c {
			case '%':
				sb.WriteString(".*")
			case '_':
				sb.WriteString(".")
			default:
				sb.WriteString(regexp.QuoteMeta(string(c)))
			}
		}
		sb.WriteString("\\z")
		re, _ = regexp.Compile(sb.String())
		likeRegexpCache.Store(pattern, re)
	}
	if re == nil {
		return false
	}
	return re.MatchString(s)
}

// reBlockComment 匹配全部块注释；StripComments 剥除时保留 /*+ ... */ 优化器 hint
// （如 /*+ PARALLEL(n) */）—— v1.0.8 前会把 hint 一并剥掉，
// 导致并行查询 hint 从未生效（静默串行回退）。
var reBlockComment = regexp.MustCompile(`(?s)/\*.*?\*/`)

// StripComments 去掉 /* */ 块注释、首尾空白与结尾分号。
func StripComments(q string) string {
	s := strings.TrimSpace(q)
	s = strings.TrimSuffix(s, ";")
	s = reBlockComment.ReplaceAllStringFunc(s, func(m string) string {
		if strings.HasPrefix(m, "/*+") {
			return m // 优化器 hint（/*+ PARALLEL(n) */ 等）保留
		}
		return ""
	})
	return strings.TrimSpace(s)
}



// rowGet 越界安全的行取值（PRAGMA 列数随版本/驱动不同）。
func rowGet(row []any, i int) string {
	if i < 0 || i >= len(row) {
		return ""
	}
	switch v := row[i].(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// RewriteDBPrefix 剥掉查询中「当前库名.表」的库名前缀（客户端工具常发
// `backup`.`blob_t` / backup.blob_t；SQLite 会把 backup 当附加库名报 no such table）。
// 仅当前缀库等于当前库名时剥除；其他库名（跨库查询不支持）保持原样交由 SQLite 报错。
func RewriteDBPrefix(q, dbName string) string {
	if dbName == "" {
		return q
	}
	esc := regexp.QuoteMeta(dbName)
	re := regexp.MustCompile("(?i)(`" + esc + "`|\"" + esc + "\"|" + esc + ")\\s*\\.\\s*(`[^`]+`|\"[^\"]+\"|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)")
	return re.ReplaceAllString(q, "$2")
}
