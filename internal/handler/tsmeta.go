// Package handler — 时序大表声明与 ts 单位自动换算（v1.3.11）。
//
// 建表谓词（特殊谓词）：
//
//	CREATE TABLE metrics (
//	  series_id BIGINT, ts BIGINT, val DOUBLE, PRIMARY KEY(series_id, ts)
//	) WITH (TIMESERIES, TS_UNIT='ms', TS_COLUMN='ts')
//
// 声明后该表享受时序大表优化红利：冷压缩 / 自动解冻 / rollup 预聚合路由
// （不再固定 points 表名——任意声明表都可用）；ts 支持秒(s)/毫秒(ms)/微秒(us)
// 三种存储精度，查询谓词按数值量纲自动检测并换算到存储精度（存秒查毫秒 /
// 存毫秒查秒均可）。声明落在库内 sidecar 表 __glacier_tsmeta（随库迁移、备份）。
//
//	SHOW TS TABLES                    —— 列当前库声明
//	SHOW TS TABLES ON <db>            —— 列指定库声明
//
// 单位检测启发式（数值字面量，epoch 附近）：
//	≥1e14 → 微秒；≥1e11 → 毫秒；≥1e9 → 秒；其余（<1e9，非 epoch 量纲）不动。
package handler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/sqlite"
)

// tsMetaTable 声明 sidecar 表（每库一张）。
const tsMetaTable = "__glacier_tsmeta"

// tsTable 一张已声明的时序大表。
type tsTable struct {
	Table  string
	Column string // ts 列名（默认 ts）
	Unit   string // s | ms | us（存储精度）
}

// reCreateTableWith 建表谓词拦截（WITH 子句须在语句尾）。
var reCreateTableWith = regexp.MustCompile(`(?is)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` +
	"(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" +
	`\s*\(.*\)\s*WITH\s*\(([^)]*)\)\s*$`)

var reShowTsTables = regexp.MustCompile(`(?is)^SHOW\s+TS\s+TABLES(?:\s+ON\s+` + "(`[^`]+`|[A-Za-z0-9_-]+)" + `)?\s*$`)

// 单位换算：1 unit = X ms。
func msToUnitValue(ms int64, unit string) int64 {
	switch unit {
	case "s":
		return ms / 1000
	case "us":
		return ms * 1000
	default:
		return ms
	}
}

func unitValueToMs(v int64, unit string) int64 {
	switch unit {
	case "s":
		return v * 1000
	case "us":
		return v / 1000
	default:
		return v
	}
}

// normalizeTsUnit 单位别名归一 → s|ms|us（空 = ms）。
func normalizeTsUnit(u string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(u)) {
	case "", "ms", "milli", "millisecond", "milliseconds":
		return "ms", true
	case "s", "sec", "second", "seconds":
		return "s", true
	case "us", "µs", "micro", "microsecond", "microseconds":
		return "us", true
	}
	return "", false
}

// detectTsLiteralUnit epoch 数值量纲检测（见文件头启发式；空 = 不转换）。
func detectTsLiteralUnit(v int64) string {
	switch {
	case v >= 100000000000000: // 1e14
		return "us"
	case v >= 100000000000: // 1e11
		return "ms"
	case v >= 1000000000: // 1e9
		return "s"
	default:
		return ""
	}
}

// convertTsLiteral 数值字面量 → 存储精度（同精度 / 量纲不明返回原值）。
func convertTsLiteral(v int64, toUnit string) (int64, bool) {
	from := detectTsLiteralUnit(v)
	if from == "" || from == toUnit {
		return v, false
	}
	return msToUnitValue(unitValueToMs(v, from), toUnit), true
}

// parseCreateTableWith 解析建表谓词；ok=false 非本形态。
// 返回（去掉 WITH 子句的建表语句, 声明, ok）。errText 非空 = 谓词非法。
func parseCreateTableWith(q string) (stripped string, decl tsTable, ok bool, errText string) {
	m := reCreateTableWith.FindStringSubmatch(q)
	if m == nil {
		return q, decl, false, ""
	}
	tbl := strings.Trim(m[1], "`")
	decl = tsTable{Table: tbl, Column: "ts", Unit: "ms"}
	timeseries := false
	for _, kv := range strings.Split(m[2], ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			errText = "empty WITH option"
			return
		}
		key, val := kv, ""
		if i := strings.IndexByte(kv, '='); i >= 0 {
			key = strings.TrimSpace(kv[:i])
			val = strings.Trim(strings.TrimSpace(kv[i+1:]), "'\"")
		}
		switch strings.ToUpper(key) {
		case "TIMESERIES":
			timeseries = val == "" || strings.EqualFold(val, "on") || val == "1" || strings.EqualFold(val, "true")
			if !timeseries && !strings.EqualFold(val, "off") && val != "0" && !strings.EqualFold(val, "false") {
				errText = "WITH (TIMESERIES) expects on/off"
				return
			}
		case "TS_UNIT", "TS_PRECISION", "UNIT":
			u, valid := normalizeTsUnit(val)
			if !valid {
				errText = "WITH TS_UNIT must be one of s|ms|us"
				return
			}
			decl.Unit = u
		case "TS_COLUMN":
			if val == "" || strings.ContainsAny(val, "`'\"() ") {
				errText = "WITH TS_COLUMN expects a plain column name"
				return
			}
			decl.Column = val
		default:
			errText = "unknown WITH option " + strconv.Quote(key) + " (TIMESERIES|TS_UNIT|TS_COLUMN)"
			return
		}
	}
	if !timeseries {
		// WITH 子句存在但未声明时序：剥掉后按普通建表执行
		stripped = strings.TrimSpace(reWithTail.ReplaceAllString(q, "$1"))
		return stripped, decl, true, ""
	}
	stripped = strings.TrimSpace(reWithTail.ReplaceAllString(q, "$1"))
	return stripped, decl, true, ""
}

// reWithTail 语句尾的 WITH (...) 子句（保留其前的列定义收尾括号）。
var reWithTail = regexp.MustCompile(`(?is)(\))\s*WITH\s*\([^)]*\)\s*$`)

// ---------------------------------------------------------------------------
// 声明存取（sidecar）
// ---------------------------------------------------------------------------

// ensureTsMetaSess 建声明 sidecar 表（幂等）。
func ensureTsMetaSess(sess *sqlite.Session) error {
	_, _, err := sess.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  tbl TEXT PRIMARY KEY, ts_column TEXT NOT NULL DEFAULT 'ts', ts_unit TEXT NOT NULL DEFAULT 'ms',
  declared_ts TEXT NOT NULL DEFAULT '')`, tsMetaTable))
	return err
}

// registerTsTableSess 写声明行（建表成功后调用）。
func registerTsTableSess(sess *sqlite.Session, t tsTable) error {
	if err := ensureTsMetaSess(sess); err != nil {
		return err
	}
	_, _, err := sess.Exec(fmt.Sprintf(`INSERT INTO %s(tbl,ts_column,ts_unit,declared_ts) VALUES(%s,%s,%s,%s)
		ON CONFLICT(tbl) DO UPDATE SET ts_column=excluded.ts_column, ts_unit=excluded.ts_unit`,
		tsMetaTable, quoteStr(t.Table), quoteStr(t.Column), quoteStr(t.Unit),
		quoteStr(time.Now().Format("2006-01-02 15:04:05"))))
	return err
}

// loadTsTablesSess 读库内全部声明（表已不存在 / sidecar 缺失 → 空表，不报错）。
func loadTsTablesSess(sess *sqlite.Session) []tsTable {
	_, rows, err := sess.Query(fmt.Sprintf(
		`SELECT m.tbl, m.ts_column, m.ts_unit FROM %s m
		 JOIN main.sqlite_master sm ON sm.type='table' AND sm.name=m.tbl`, tsMetaTable))
	if err != nil {
		return nil
	}
	var out []tsTable
	for _, r := range rows {
		t := tsTable{Table: anyToStr(r[0]), Column: anyToStr(r[1]), Unit: anyToStr(r[2])}
		if t.Table == "" {
			continue
		}
		if u, ok := normalizeTsUnit(t.Unit); ok {
			t.Unit = u
		} else {
			t.Unit = "ms"
		}
		if t.Column == "" {
			t.Column = "ts"
		}
		out = append(out, t)
	}
	return out
}

// activeTsTables 当前库生效的时序表 = 声明表 ∪ 隐式 points(ms, 表存在时)。
// Handler 级缓存（DDL 后失效）。sidecar 缺失且无 points 时零查询开销旁路。
func (h *Handler) activeTsTables() []tsTable {
	if h.tsCache != nil {
		return h.tsCache
	}
	var out []tsTable
	if decl := loadTsTablesSess(h.session); len(decl) > 0 {
		out = append(out, decl...)
	}
	// 隐式 points（未声明也保持 v1.1.1 起的 rollup/cold 行为；ms 口径）
	if _, rows, err := h.session.Query(`SELECT 1 FROM main.sqlite_master WHERE type='table' AND name='points' LIMIT 1`); err == nil && len(rows) > 0 {
		dup := false
		for _, t := range out {
			if strings.EqualFold(t.Table, "points") {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, tsTable{Table: "points", Column: "ts", Unit: "ms"})
		}
	}
	h.tsCache = out
	return out
}

// tsTableForNamed 语句中按表名找声明的时序表（找不到 = nil）。
func (h *Handler) tsTableForNamed(tbl string) *tsTable {
	tbl = strings.Trim(strings.TrimSpace(tbl), "`")
	list := h.activeTsTables()
	for i := range list {
		if strings.EqualFold(list[i].Table, tbl) {
			return &list[i]
		}
	}
	return nil
}

// invalidateTsCache DDL 后失效声明缓存。
func (h *Handler) invalidateTsCache() { h.tsCache = nil }

// reDDLStmt 表级 DDL 识别（成功后失效时序声明缓存）。
var reDDLStmt = regexp.MustCompile(`(?is)^\s*(CREATE|DROP|ALTER|RENAME)\s+(TABLE|INDEX|VIEW)`)

// invalidateTsCacheOnDDL 写语句成功后按需失效时序声明缓存（DROP/RENAME 表场景）。
func (h *Handler) invalidateTsCacheOnDDL(sq string) {
	if reDDLStmt.MatchString(sq) {
		h.invalidateTsCache()
	}
}

// ---------------------------------------------------------------------------
// ts 谓词单位自动换算（语句级重写）
// ---------------------------------------------------------------------------

var (
	reTsCmpNum    = regexp.MustCompile(`(?i)\b(ts|time)\s*(>=|<=|!=|<>|=|<|>)\s*(\d{9,19})\b`)
	reTsBetween   = regexp.MustCompile(`(?i)\b(ts|time)\s+BETWEEN\s+(\d{9,19})\s+AND\s+(\d{9,19})\b`)
	reTsCmpDate   = regexp.MustCompile(`(?i)\b(ts|time)\s*(>=|<=|=|<|>)\s*'(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)'`)
	reTsBucketMs  = regexp.MustCompile(`(?i)\bts\s*/\s*(\d+)\s*\*\s*(\d+)\b`)
	reTsTimeFn    = regexp.MustCompile(`(?i)\btime\s*\(\s*(\d+)\s*\)`)
	reTsMention   = regexp.MustCompile(`(?i)\b(ts|time)\b`)
)

// rewriteTsPredicates 对提及声明时序表的语句做 ts 单位换算：
//   - 数值/日期字面量比较、BETWEEN：自动检测量纲换算到存储精度；
//   - 桶宽表达式 ts/N*N 与 time(N)：N 按毫秒约定换算到存储精度；
//   - 日期串转存储精度整数（整数列与文本比较本无意义，转换后语义正确）。
// 未声明时序表 / 语句不含 ts 词汇时原样返回（零改动）。
func (h *Handler) rewriteTsPredicates(sq string) string {
	tables := h.activeTsTables()
	if len(tables) == 0 || !reTsMention.MatchString(sq) {
		return sq
	}
	// 语句实际提及的表（大小写不敏感词边界）；未提及时不改写（避免 JOIN 场景误伤他表 ts 列）
	var hit []tsTable
	for _, t := range tables {
		re := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(strings.Trim(t.Table, "`")) + `\b`)
		if re.MatchString(sq) {
			hit = append(hit, t)
		}
	}
	if len(hit) == 0 {
		return sq
	}
	// 多表混提且精度不一致：保守不改写（罕见；提示用 SHOW TS TABLES 排查）
	unit := hit[0].Unit
	for _, t := range hit[1:] {
		if t.Unit != unit {
			return sq
		}
	}
	changed := false
	out := reTsCmpNum.ReplaceAllStringFunc(sq, func(m string) string {
		sub := reTsCmpNum.FindStringSubmatch(m)
		v, _ := strconv.ParseInt(sub[3], 10, 64)
		if nv, did := convertTsLiteral(v, unit); did {
			changed = true
			return sub[1] + sub[2] + strconv.FormatInt(nv, 10)
		}
		return m
	})
	out = reTsBetween.ReplaceAllStringFunc(out, func(m string) string {
		sub := reTsBetween.FindStringSubmatch(m)
		a, _ := strconv.ParseInt(sub[2], 10, 64)
		b, _ := strconv.ParseInt(sub[3], 10, 64)
		na, da := convertTsLiteral(a, unit)
		nb, db := convertTsLiteral(b, unit)
		if da || db {
			changed = true
			return sub[1] + " BETWEEN " + strconv.FormatInt(na, 10) + " AND " + strconv.FormatInt(nb, 10)
		}
		return m
	})
	out = reTsCmpDate.ReplaceAllStringFunc(out, func(m string) string {
		sub := reTsCmpDate.FindStringSubmatch(m)
		ds := strings.Replace(sub[3], "T", " ", 1)
		for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02"} {
			if t, err := time.ParseInLocation(layout, ds, time.UTC); err == nil {
				changed = true
				return sub[1] + sub[2] + strconv.FormatInt(msToUnitValue(t.UnixMilli(), unit), 10)
			}
		}
		return m
	})
	if unit != "ms" { // 桶宽毫秒约定 → 存储精度（ms 表无需换算）
		out = reTsBucketMs.ReplaceAllStringFunc(out, func(m string) string {
			sub := reTsBucketMs.FindStringSubmatch(m)
			n, _ := strconv.ParseInt(sub[1], 10, 64)
			changed = true
			return fmt.Sprintf("ts/%d*%d", msToUnitValue(n, unit), msToUnitValue(n, unit))
		})
		out = reTsTimeFn.ReplaceAllStringFunc(out, func(m string) string {
			sub := reTsTimeFn.FindStringSubmatch(m)
			n, _ := strconv.ParseInt(sub[1], 10, 64)
			changed = true
			return fmt.Sprintf("time(%d)", msToUnitValue(n, unit))
		})
	}
	if changed {
		fs, ts := sq, out
		if len(fs) > 200 {
			fs = fs[:200] + "..."
		}
		if len(ts) > 200 {
			ts = ts[:200] + "..."
		}
		logx.Debugf("ts unit rewrite [db=%s]: %s => %s", h.session.CurrentDB(), fs, ts)
	}
	return out
}

// ---------------------------------------------------------------------------
// 管理命令：SHOW TS TABLES / 建表谓词执行
// ---------------------------------------------------------------------------

// handleShowTsTables SHOW TS TABLES [ON <db>]。
func (h *Handler) handleShowTsTables(q string) (*mysql.Result, bool, error) {
	m := reShowTsTables.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	logx.Debugf("tsmeta: SHOW TS TABLES dispatch (tables=%d)", len(h.activeTsTables()))
	db := ""
	if m[1] != "" {
		db = strings.Trim(m[1], "`")
		old := h.session.CurrentDB()
		if err := h.session.Use(db); err != nil {
			return nil, true, mysql.NewError(mysql.ER_BAD_DB_ERROR, err.Error())
		}
		defer func() { _ = h.session.Use(old) }()
	}
	var rows [][]any
	for _, t := range h.activeTsTables() {
		rows = append(rows, []any{h.session.CurrentDB(), t.Table, t.Column, t.Unit})
	}
	res, err := meta.SimpleRS([]string{"db", "table", "ts_column", "ts_unit"}, rows)
	return res, true, err
}

// handleCreateTableWith 建表谓词拦截执行：剥掉 WITH 子句正常建表，
// 成功后落 sidecar 声明并校验 ts 列存在。ok=false 非本形态。
func (h *Handler) handleCreateTableWith(q string) (*mysql.Result, bool, error) {
	stripped, decl, ok, errText := parseCreateTableWith(q)
	if !ok {
		return nil, false, nil
	}
	if errText != "" {
		return nil, true, mysql.NewError(1064, errText)
	}
	if h.readonly {
		return nil, true, roErr()
	}
	res, werr := h.execWriteOne(stripped)
	if werr != nil {
		return res, true, werr
	}
	// 谓词语义：列校验 + 声明落库（TIMESERIES=off 时仅剥子句）
	if reWithTail.MatchString(q) && strings.Contains(strings.ToUpper(q), "TIMESERIES") {
		if hasTsPredicateOn(q) {
			_, ti, err := h.session.Query(fmt.Sprintf(`PRAGMA main.table_info(%s)`, quoteIdent(decl.Table)))
			if err != nil || len(ti) == 0 {
				return nil, true, mysql.NewError(1105, "table created but declaration check failed: "+decl.Table)
			}
			has := false
			for _, r := range ti {
				if len(r) >= 2 && strings.EqualFold(anyToStr(r[1]), decl.Column) {
					has = true
					break
				}
			}
			if !has {
				return nil, true, mysql.NewError(1105, fmt.Sprintf(
					"table %s created but TS_COLUMN %q not found (declaration not saved)", decl.Table, decl.Column))
			}
			if err := registerTsTableSess(h.session, decl); err != nil {
				return nil, true, MapSQLiteErr(err)
			}
			h.invalidateTsCache()
		}
	}
	h.afterWriteSuccess(stripped)
	return res, true, nil
}

// hasTsPredicateOn WITH 子句里是否声明了 TIMESERIES=on。
func hasTsPredicateOn(q string) bool {
	m := reCreateTableWith.FindStringSubmatch(q)
	if m == nil {
		return false
	}
	for _, kv := range strings.Split(m[2], ",") {
		kv = strings.TrimSpace(kv)
		if i := strings.IndexByte(kv, '='); i >= 0 {
			kv = strings.TrimSpace(kv[:i])
		}
		if strings.EqualFold(kv, "TIMESERIES") {
			return true
		}
	}
	return false
}
