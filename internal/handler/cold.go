// Package handler — 时序冷压缩与固化（v1.3.3）：
//
//	points（series_id/ts/val 约定 schema）按时间步长（默认 1 天）分块，
//	完全早于边界的数据压缩进 <tbl>_cold（gzip(varint ts 增量 + 值原码)，
//	字节级无损），并从热表删除——「固化」为不可变归档。
//
//	命令：COMPRESS COLD <tbl> BEFORE <time> [STEP n DAY|HOUR]
//	      THAW COLD <tbl> [ALL | BEFORE <time> | CHUNK <ms>]
//	      SHOW COLD [<tbl>]
//	透明性：读查询触及冻结区间时自动解冻（解析 ts 谓词，无谓词保守全解冻）；
//	写入撞冻结区间的 PK 冲突自动全解冻重试一次。rollup 汇总表在解冻期间
//	临时摘除触发器避免重复计数。
package handler

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	sqlite3 "github.com/ncruces/go-sqlite3"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/sqlite"
)

const coldAlgo = "gz-varint1"

var (
	reCompressCold = regexp.MustCompile(`(?is)^COMPRESS\s+COLD\s+` + "(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" +
		`\s+BEFORE\s+('(?:[^']|'')*'|[^\s]+)(?:\s+STEP\s+(\d+)\s+(DAY|HOUR))?\s*$`)
	reThawCold = regexp.MustCompile(`(?is)^THAW\s+COLD\s+` + "(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" +
		`(?:\s+(ALL|BEFORE\s+(?:'(?:[^']|'')*'|[^\s]+)|CHUNK\s+(\d+)))?\s*$`)
	reShowCold = regexp.MustCompile(`(?is)^SHOW\s+COLD(?:\s+` + "(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" + `)?\s*$`)
	reTsLower  = regexp.MustCompile(`(?i)\bts\s*(?:>=|>|=|between)\s*('(?:[^']|'')*'|[0-9]+)`)
	reColdOK   = regexp.MustCompile(`(?i)\bpoints\b`)

	// 时序谓词求值复用（rollup.go 的 evalRollupTime 处理整数/日期串/NOW±INTERVAL）
	reColdTimeVal = regexp.MustCompile(`(?i)^(-?\d+)$|^'(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)'$|^(NOW|CURRENT_TIMESTAMP)\s*\(\s*\)(?:\s*([+-])\s*INTERVAL\s+'?(\d+)\s*(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?'?)?$`)
)

// coldTableName 归档表名。
func coldTableName(tbl string) string { return tbl + "_cold" }

// handleColdStmt COMPRESS/THAW/SHOW COLD 命令入口（普通库执行）。
// v1.3.11：表可为任意声明时序表（WITH (TIMESERIES...)），边界/步长按声明精度换算；
// COMPRESS/THAW 为破坏性写（热表删除/回插），只读账号拒绝（安全修复）。
func (h *Handler) handleColdStmt(q string) (*mysql.Result, bool, error) {
	switch {
	case reCompressCold.MatchString(q):
		if h.readonly {
			return nil, true, roErr()
		}
		m := reCompressCold.FindStringSubmatch(q)
		tbl := strings.Trim(m[1], "`")
		unit := "ms"
		if t := h.tsTableForNamed(tbl); t != nil {
			unit = t.Unit
		}
		boundaryMS, ok := parseColdTime(m[2])
		if !ok {
			return nil, true, mysql.NewError(1064, "invalid BEFORE time: "+m[2])
		}
		boundary := msToUnitValue(boundaryMS, unit)
		stepMS := int64(86400000)
		if m[3] != "" {
			n, _ := strconv.ParseInt(m[3], 10, 64)
			if n <= 0 || n > 366 {
				return nil, true, mysql.NewError(1064, "invalid STEP")
			}
			stepMS = n * 86400000
			if strings.EqualFold(m[4], "HOUR") {
				stepMS = n * 3600000
			}
		}
		step := msToUnitValue(stepMS, unit)
		st, err := h.coldCompress(tbl, boundary, step)
		if err != nil {
			return nil, true, err
		}
		res, err := meta.SimpleRS([]string{"table", "chunks", "series", "rows", "raw_bytes", "gz_bytes", "ratio", "boundary_ms"},
			[][]any{{tbl, int64(st.Chunks), int64(st.Series), st.Rows, st.RawBytes, st.GzBytes,
				fmt.Sprintf("%.2f", st.Ratio()), unitValueToMs(boundary, unit)}})
		return res, true, err

	case reThawCold.MatchString(q):
		if h.readonly {
			return nil, true, roErr()
		}
		m := reThawCold.FindStringSubmatch(q)
		tbl := strings.Trim(m[1], "`")
		unit := "ms"
		if t := h.tsTableForNamed(tbl); t != nil {
			unit = t.Unit
		}
		mode := strings.ToUpper(strings.TrimSpace(m[2]))
		chunk := int64(-1)
		before := int64(-1)
		switch {
		case mode == "" || mode == "ALL":
		case strings.HasPrefix(mode, "BEFORE"):
			bm := regexp.MustCompile(`(?is)^BEFORE\s+(.+)$`).FindStringSubmatch(mode)
			if bm == nil {
				return nil, true, mysql.NewError(1064, "invalid THAW clause")
			}
			t, ok := parseColdTime(bm[1])
			if !ok {
				return nil, true, mysql.NewError(1064, "invalid BEFORE time: "+bm[1])
			}
			before = msToUnitValue(t, unit)
		case strings.HasPrefix(mode, "CHUNK"):
			cm := regexp.MustCompile(`(?is)^CHUNK\s+(\d+)$`).FindStringSubmatch(mode)
			if cm == nil {
				return nil, true, mysql.NewError(1064, "invalid CHUNK")
			}
			chunk, _ = strconv.ParseInt(cm[1], 10, 64)
		default:
			return nil, true, mysql.NewError(1064, "invalid THAW clause")
		}
		chunks, rows, err := h.coldThaw(tbl, before, chunk)
		if err != nil {
			return nil, true, err
		}
		res, err := meta.SimpleRS([]string{"table", "chunks_thawed", "rows_restored"},
			[][]any{{tbl, chunks, rows}})
		return res, true, err

	case reShowCold.MatchString(q):
		m := reShowCold.FindStringSubmatch(q)
		tbl := ""
		if m != nil && m[1] != "" {
			tbl = strings.Trim(m[1], "`")
		}
		var rows [][]any
		for _, t := range h.coldTables(tbl) {
			unit := "ms"
			if d := h.tsTableForNamed(t); d != nil {
				unit = d.Unit
			}
			trows, err := h.coldStats(t, unit)
			if err != nil {
				continue
			}
			rows = append(rows, trows...)
		}
		res, err := meta.SimpleRS([]string{"table", "chunk_start", "chunk_ms", "series", "rows", "gz_bytes", "min_ts", "max_ts"}, rows)
		return res, true, err
	}
	return nil, false, nil
}

// coldTables 当前库有归档表的表名清单（过滤参数为空时全列）。
func (h *Handler) coldTables(filter string) []string {
	_, rows, err := h.session.Query("SELECT name FROM main.sqlite_master WHERE type='table' AND name LIKE '%\\_cold' ESCAPE '\\' ORDER BY name")
	if err != nil {
		return nil
	}
	var out []string
	for _, r := range rows {
		name := anyToStr(r[0])
		tbl := strings.TrimSuffix(name, "_cold")
		if filter != "" && !strings.EqualFold(tbl, strings.Trim(filter, "`")) {
			continue
		}
		out = append(out, tbl)
	}
	return out
}

// parseColdTime 边界时间 → epoch ms（整数 / 'YYYY-MM-DD[ HH:MM:SS]' / NOW()±INTERVAL）。
func parseColdTime(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, true
	}
	s2 := strings.Trim(s, "'\"")
	s2 = strings.Replace(s2, "T", " ", 1)
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s2, time.UTC); err == nil {
			return t.UnixMilli(), true
		}
	}
	if m := reColdTimeVal.FindStringSubmatch(s); m != nil && m[3] != "" {
		if v, ok := evalRollupTime(s); ok {
			return v, true
		}
	}
	return 0, false
}

type coldStatsResult struct {
	Chunks   int
	Series   int
	Rows     int64
	RawBytes int64
	GzBytes  int64
}

func (s coldStatsResult) Ratio() float64 {
	if s.GzBytes > 0 {
		return float64(s.RawBytes) / float64(s.GzBytes)
	}
	return 0
}

// validateTSSchema 表必须含 series_id/ts/val 三列（时序约定）。
func validateTSSchema(sess *sqlite.Session, tbl string) error {
	_, ti, err := sess.Query(fmt.Sprintf(`PRAGMA main.table_info(%s)`, quoteIdent(tbl)))
	if err != nil || len(ti) == 0 {
		return mysql.NewError(1146, "Table '"+sess.CurrentDB()+"."+tbl+"' doesn't exist")
	}
	cols := map[string]bool{}
	for _, r := range ti {
		if len(r) >= 2 {
			cols[strings.ToLower(fmt.Sprintf("%v", r[1]))] = true
		}
	}
	if !cols["series_id"] || !cols["ts"] || !cols["val"] {
		return mysql.NewError(1105, "cold compression requires columns (series_id, ts, val); table "+tbl+" does not match")
	}
	return nil
}

// coldCompress 压缩边界之前的完整块（写锁 + 单事务；固化语义）。
func (h *Handler) coldCompress(tbl string, boundary, step int64) (coldStatsResult, error) {
	return compressColdOnSession(h.session, tbl, boundary, step)
}

func compressColdOnSession(sess *sqlite.Session, tbl string, boundary, step int64) (coldStatsResult, error) {
	var st coldStatsResult
	if err := validateTSSchema(sess, tbl); err != nil {
		return st, err
	}
	if sess.InTx || sess.HoldsWriteLock() {
		return st, mysql.NewError(1192, "COMPRESS COLD cannot run inside an explicit transaction")
	}
	cold := coldTableName(tbl)
	sess.AcquireWrite()
	defer sess.ReleaseWrite()
	if _, _, err := sess.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  chunk INTEGER NOT NULL, series_id INTEGER NOT NULL,
  rows INTEGER NOT NULL, min_ts INTEGER NOT NULL, max_ts INTEGER NOT NULL,
  algo TEXT NOT NULL, zdata BLOB NOT NULL,
  PRIMARY KEY (chunk, series_id))`, quoteIdent(cold))); err != nil {
		return st, MapSQLiteErr(err)
	}
	if _, _, err := sess.Exec("BEGIN IMMEDIATE"); err != nil {
		return st, MapSQLiteErr(err)
	}
	fail := func(err error) (coldStatsResult, error) {
		_, _, _ = sess.Exec("ROLLBACK")
		return st, err
	}
	// 已有冻结块不再重压（INSERT OR REPLACE 幂等）；流式按 (series, ts) 分组
	_, rows, err := sess.Query(fmt.Sprintf(
		`SELECT series_id, ts, val FROM %s WHERE ts < %d ORDER BY series_id, ts`, quoteIdent(tbl), boundary))
	if err != nil {
		return fail(MapSQLiteErr(err))
	}
	type key struct{ chunk, series int64 }
	type acc struct {
		tss  []int64
		vals []any
	}
	accs := map[key]*acc{}
	var order []key
	for _, r := range rows {
		sid := cellInt64(r[0])
		ts := cellInt64(r[1])
		chunk := ts / step * step
		if chunk+step > boundary {
			continue // 尾部不完整块保持热态
		}
		k := key{chunk, sid}
		a := accs[k]
		if a == nil {
			a = &acc{}
			accs[k] = a
			order = append(order, k)
		}
		a.tss = append(a.tss, ts)
		a.vals = append(a.vals, r[2])
	}
	st.Chunks = len(order)
	if st.Chunks == 0 {
		_, _, _ = sess.Exec("COMMIT")
		return st, nil
	}
	var compressed []key
	for _, k := range order {
		a := accs[k]
		z, raw, err := encodeColdChunk(a.tss, a.vals)
		if err != nil {
			return fail(mysql.NewError(1105, "cold encode: "+err.Error()))
		}
		stmt := fmt.Sprintf(`INSERT OR REPLACE INTO %s(chunk,series_id,rows,min_ts,max_ts,algo,zdata)
			VALUES(%d,%d,%d,%d,%d,'%s',x'%s')`,
			quoteIdent(cold), k.chunk, k.series, len(a.tss), a.tss[0], a.tss[len(a.tss)-1],
			coldAlgo, hexEncode(z))
		if _, _, err := sess.Exec(stmt); err != nil {
			return fail(MapSQLiteErr(err))
		}
		compressed = append(compressed, k)
		st.Series++
		st.Rows += int64(len(a.tss))
		st.RawBytes += raw
		st.GzBytes += int64(len(z))
	}
	// 热表删除已固化行（按块范围，块级保证 < boundary）
	for i := 0; i < len(compressed); i += 64 {
		end := i + 64
		if end > len(compressed) {
			end = len(compressed)
		}
		var conds []string
		for _, k := range compressed[i:end] {
			conds = append(conds, fmt.Sprintf("(ts >= %d AND ts < %d AND series_id = %d)", k.chunk, k.chunk+step, k.series))
		}
		if _, _, err := sess.Exec(fmt.Sprintf(`DELETE FROM %s WHERE %s`, quoteIdent(tbl), strings.Join(conds, " OR "))); err != nil {
			return fail(MapSQLiteErr(err))
		}
	}
	if _, _, err := sess.Exec("COMMIT"); err != nil {
		return fail(MapSQLiteErr(err))
	}
	logx.Infof("cold: compressed %s.%s chunks=%d series=%d rows=%d ratio=%.2f",
		sess.CurrentDB(), tbl, st.Chunks, st.Series, st.Rows, st.Ratio())
	return st, nil
}

// coldThaw 解冻（写锁 + 单事务；BEFORE/CHUNK/ALL）。返回（块数, 行数）。
func (h *Handler) coldThaw(tbl string, before, chunk int64) (int64, int64, error) {
	unit := "ms"
	if t := h.tsTableForNamed(tbl); t != nil {
		unit = t.Unit
	}
	return thawColdOnSession(h.session, tbl, unit, before, chunk, -1)
}

// thawColdOnSession 解冻核心。过滤优先级：CHUNK > overlap（max_ts >= 值，自动
// 解冻用）> BEFORE（max_ts < 值）；全 -1 = ALL。unit 为该表 ts 存储精度
//（仅用于 rollup 触发器重建的桶宽换算，块过滤值本身已是存储精度）。
func thawColdOnSession(sess *sqlite.Session, tbl, unit string, before, chunk, overlap int64) (int64, int64, error) {
	cold := coldTableName(tbl)
	if sess.InTx || sess.HoldsWriteLock() {
		return 0, 0, mysql.NewError(1192, "THAW COLD cannot run inside an explicit transaction")
	}
	sess.AcquireWrite()
	defer sess.ReleaseWrite()
	// rollup 触发器临时摘除（避免解冻 INSERT 重复计数）
	dropRollupTriggers(sess, tbl)
	defer ensureRollupTriggersFor(sess, tbl, unit)

	if _, _, err := sess.Exec("BEGIN IMMEDIATE"); err != nil {
		return 0, 0, MapSQLiteErr(err)
	}
	fail := func(err error) (int64, int64, error) {
		_, _, _ = sess.Exec("ROLLBACK")
		return 0, 0, err
	}
	cond := ""
	switch {
	case chunk >= 0:
		cond = fmt.Sprintf(" WHERE chunk = %d", chunk)
	case overlap >= 0:
		cond = fmt.Sprintf(" WHERE max_ts >= %d", overlap)
	case before >= 0:
		cond = fmt.Sprintf(" WHERE max_ts < %d", before)
	}
	_, rows, err := sess.Query(fmt.Sprintf(`SELECT chunk, series_id, rows, min_ts, max_ts, algo, zdata FROM %s%s ORDER BY chunk, series_id`,
		quoteIdent(cold), cond))
	if err != nil {
		return fail(MapSQLiteErr(err))
	}
	var chunks, total int64
	var delConds []string
	for _, r := range rows {
		ch := cellInt64(r[0])
		sid := cellInt64(r[1])
		algo := anyToStr(r[5])
		if algo != coldAlgo {
			return fail(mysql.NewError(1105, "unknown cold algo "+algo))
		}
		blob, _ := r[6].([]byte)
		if blob == nil {
			if s, ok := r[6].(string); ok {
				blob = []byte(s)
			}
		}
		tss, vals, err := decodeColdChunk(blob)
		if err != nil {
			return fail(mysql.NewError(1105, "cold decode: "+err.Error()))
		}
		if err := insertColdRows(sess, tbl, sid, tss, vals); err != nil {
			return fail(err)
		}
		total += int64(len(tss))
		chunks++
		delConds = append(delConds, fmt.Sprintf("(chunk = %d AND series_id = %d)", ch, sid))
	}
	for i := 0; i < len(delConds); i += 64 {
		end := i + 64
		if end > len(delConds) {
			end = len(delConds)
		}
		if _, _, err := sess.Exec(fmt.Sprintf(`DELETE FROM %s WHERE %s`, quoteIdent(cold),
			strings.Join(delConds[i:end], " OR "))); err != nil {
			return fail(MapSQLiteErr(err))
		}
	}
	if _, _, err := sess.Exec("COMMIT"); err != nil {
		return fail(MapSQLiteErr(err))
	}
	logx.Infof("cold: thawed %s.%s chunks=%d rows=%d", sess.CurrentDB(), tbl, chunks, total)
	return chunks, total, nil
}

// insertColdRows 批量还原行（500 行/批；PK 冲突忽略——热表已有更新版本时以热为准）。
func insertColdRows(sess *sqlite.Session, tbl string, sid int64, tss []int64, vals []any) error {
	const batch = 500
	for off := 0; off < len(tss); off += batch {
		end := off + batch
		if end > len(tss) {
			end = len(tss)
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, `INSERT OR IGNORE INTO %s(series_id, ts, val) VALUES `, quoteIdent(tbl))
		for i := off; i < end; i++ {
			if i > off {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, "(%d, %d, %s)", sid, tss[i], coldValueLiteral(vals[i]))
		}
		if _, _, err := sess.Exec(sb.String()); err != nil {
			return MapSQLiteErr(err)
		}
	}
	return nil
}

func coldValueLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case int64:
		return strconv.FormatInt(x, 10)
	case []byte:
		return quoteStr(string(x))
	case string:
		return quoteStr(x)
	default:
		return "NULL"
	}
}

// dropRollupTriggers / ensureRollupTriggersFor rollup 触发器临时摘除与重建（任意声明时序表；
// 仅当该表已建 rollup 汇总表时才重建，避免给未启用 rollup 的表凭空建触发器）。
func dropRollupTriggers(sess *sqlite.Session, tbl string) {
	for _, g := range rollupGrains {
		_, _, _ = sess.Exec(fmt.Sprintf(`DROP TRIGGER IF EXISTS %s`,
			quoteIdent(tbl+"_rollup_ai_"+g.suffix)))
	}
}

func ensureRollupTriggersFor(sess *sqlite.Session, tbl, unit string) {
	if _, rows, err := sess.Query(fmt.Sprintf(
		`SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=%s LIMIT 1`, quoteStr(tbl+"_m1"))); err != nil || len(rows) == 0 {
		return // 该表未启用 rollup：不建触发器
	}
	for _, g := range rollupGrains {
		p := msToUnitValue(g.periodMs, unit)
		t := tbl + "_" + g.suffix
		trg := fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS %s AFTER INSERT ON %s BEGIN
  INSERT INTO %s(series_id,bucket_ts,cnt,cntv,sum_val,min_val,max_val)
  VALUES (NEW.series_id, NEW.ts/%d*%d, 1, CASE WHEN NEW.val IS NULL THEN 0 ELSE 1 END, NEW.val, NEW.val, NEW.val)
  ON CONFLICT(series_id,bucket_ts) DO UPDATE SET
    cnt = cnt + 1,
    cntv = cntv + excluded.cntv,
    sum_val = CASE WHEN excluded.sum_val IS NULL THEN sum_val WHEN sum_val IS NULL THEN excluded.sum_val ELSE sum_val + excluded.sum_val END,
    min_val = CASE WHEN excluded.min_val IS NULL THEN min_val WHEN min_val IS NULL THEN excluded.min_val WHEN excluded.min_val < min_val THEN excluded.min_val ELSE min_val END,
    max_val = CASE WHEN excluded.max_val IS NULL THEN max_val WHEN max_val IS NULL THEN excluded.max_val WHEN excluded.max_val > max_val THEN excluded.max_val ELSE max_val END;
END`, quoteIdent(tbl+"_rollup_ai_"+g.suffix), quoteIdent(tbl), quoteIdent(t), p, p)
		_, _, _ = sess.Exec(trg)
	}
}

// quoteStr 单引号字面量（handler 包内工具）。
func quoteStr(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// coldStats 归档统计行集（unit 用于块起点展示换算）。
func (h *Handler) coldStats(tbl, unit string) ([][]any, error) {
	cold := coldTableName(tbl)
	_, rows, err := h.session.Query(fmt.Sprintf(
		`SELECT chunk, COUNT(*), SUM(rows), SUM(LENGTH(zdata)), MIN(min_ts), MAX(max_ts) FROM %s GROUP BY chunk ORDER BY chunk`,
		quoteIdent(cold)))
	if err != nil {
		return nil, err
	}
	var out [][]any
	for _, r := range rows {
		ch := cellInt64(r[0])
		out = append(out, []any{
			tbl,
			time.UnixMilli(unitValueToMs(ch, unit)).UTC().Format("2006-01-02 15:04:05"),
			unitValueToMs(ch, unit),
			cellInt64(r[1]),
			cellInt64(r[2]),
			cellInt64(r[3]),
			cellInt64(r[4]),
			cellInt64(r[5]),
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// 自动解冻（读写透明性）
// ---------------------------------------------------------------------------

// maybeThawCold 语句触及冻结区间时先行解冻（v1.3.11：遍历当前库全部时序表——
// 声明表 + 隐式 points；语句提及且存在归档才处理）。
// 无 ts 谓词 → 保守全解冻；有下界 → 解冻 max_ts >= lo 的块。
// 客户端显式事务/已持写锁时跳过（避免不可重入锁死锁，查询只见热数据）。
func (h *Handler) maybeThawCold(sq string) {
	if h.session.InTx || h.session.HoldsWriteLock() {
		return
	}
	for _, t := range h.activeTsTables() {
		h.maybeThawColdTable(sq, t)
	}
}

// maybeThawColdTable 单表的冻结区间触达检查与解冻。
func (h *Handler) maybeThawColdTable(sq string, t tsTable) {
	reMention := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(strings.Trim(t.Table, "`")) + `\b`)
	if !reMention.MatchString(sq) {
		return
	}
	reLower := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(t.Column) + `\s*(?:>=|>|=|between)\s*('(?:[^']|'')*'|[0-9]+)`)
	cold := coldTableName(t.Table)
	_, rows, err := h.session.Query(fmt.Sprintf(
		`SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=%s LIMIT 1`, quoteStr(cold)))
	if err != nil || len(rows) == 0 {
		return // 无归档：零开销
	}
	_, mrows, err := h.session.Query(fmt.Sprintf(`SELECT MAX(max_ts), COUNT(*) FROM %s`, quoteIdent(cold)))
	if err != nil || len(mrows) == 0 {
		return
	}
	maxCold := cellInt64(mrows[0][0])
	cnt := cellInt64(mrows[0][1])
	if cnt == 0 || maxCold == 0 {
		return
	}
	// 查询 ts 下界（rewriteTsPredicates 已把字面量换算到存储精度；
	// evalRollupTime 对数值原样返回、日期串/NOW() 给 ms → 统一归一到存储精度）
	lo := int64(-1)
	for _, m := range reLower.FindAllStringSubmatch(sq, -1) {
		if v, ok := evalColdPredicateTimeForUnit(m[1], t.Unit); ok {
			if lo < 0 || v < lo {
				lo = v
			}
		} else {
			lo = -1
			break // 解析不了：保守全解
		}
	}
	if lo >= 0 && lo > maxCold {
		return // 查询区间在冻结区之后：无需解冻
	}
	if _, _, err := thawColdOnSession(h.session, t.Table, t.Unit, -1, -1, lo); err != nil {
		logx.Warnf("cold auto-thaw [db=%s tbl=%s]: %v (query continues on hot data)", h.session.CurrentDB(), t.Table, err)
	} else {
		logx.Debugf("cold auto-thaw [db=%s tbl=%s] for query touching frozen range", h.session.CurrentDB(), t.Table)
	}
}

// evalColdPredicateTimeForUnit 谓词时间值 → 该表存储精度
//（数值字面量原样、日期串/NOW() 由 ms 换算）。
func evalColdPredicateTimeForUnit(s, unit string) (int64, bool) {
	v, ok := evalRollupTime(strings.TrimSpace(s))
	if !ok {
		return 0, false
	}
	if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
		return n, true // 数值：已在存储精度（rewriter 已换算）
	}
	return msToUnitValue(v, unit), true
}

// retryThawOnConstraint 写时序表撞冻结区间（PK 冲突）→ 解冻后重试一次。
// 返回 true 表示已解冻（调用方应重试原语句）。
func (h *Handler) retryThawOnConstraint(err error, sq string) bool {
	if err == nil {
		return false
	}
	var se *sqlite3.Error
	if !errors.As(err, &se) || se.Code() != sqlite3.CONSTRAINT {
		return false
	}
	thawed := false
	for _, t := range h.activeTsTables() {
		reMention := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(strings.Trim(t.Table, "`")) + `\b`)
		if !reMention.MatchString(sq) {
			continue
		}
		cold := coldTableName(t.Table)
		_, rows, qerr := h.session.Query(fmt.Sprintf(
			`SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=%s LIMIT 1`, quoteStr(cold)))
		if qerr != nil || len(rows) == 0 {
			continue
		}
		if _, _, terr := thawColdOnSession(h.session, t.Table, t.Unit, -1, -1, -1); terr != nil {
			logx.Warnf("cold constraint-thaw: %v", terr)
			continue
		}
		thawed = true
	}
	return thawed
}

// ---------------------------------------------------------------------------
// 编解码：gzip(varint(ts 增量 zigzag) + 值原码)
// ---------------------------------------------------------------------------

func hexEncode(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*2)
	for _, c := range b {
		out = append(out, hexdigits[c>>4], hexdigits[c&0xF])
	}
	return string(out)
}

func encodeColdChunk(tss []int64, vals []any) ([]byte, int64, error) {
	if len(tss) != len(vals) {
		return nil, 0, fmt.Errorf("ts/val length mismatch")
	}
	var buf bytes.Buffer
	var prev int64
	for i, ts := range tss {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutVarint(tmp[:], ts-prev) // zigzag varint 增量
		buf.Write(tmp[:n])
		prev = ts
		switch v := vals[i].(type) {
		case nil:
			buf.WriteByte(0)
		case float64:
			buf.WriteByte(1)
			var b8 [8]byte
			binary.BigEndian.PutUint64(b8[:], math.Float64bits(v))
			buf.Write(b8[:])
		case int64:
			buf.WriteByte(1)
			var b8 [8]byte
			binary.BigEndian.PutUint64(b8[:], math.Float64bits(float64(v)))
			buf.Write(b8[:])
		case []byte:
			buf.WriteByte(2)
			var b4 [4]byte
			binary.BigEndian.PutUint32(b4[:], uint32(len(v)))
			buf.Write(b4[:])
			buf.Write(v)
		case string:
			buf.WriteByte(2)
			b := []byte(v)
			var b4 [4]byte
			binary.BigEndian.PutUint32(b4[:], uint32(len(b)))
			buf.Write(b4[:])
			buf.Write(b)
		default:
			buf.WriteByte(2)
			b := []byte(fmt.Sprintf("%v", v))
			var b4 [4]byte
			binary.BigEndian.PutUint32(b4[:], uint32(len(b)))
			buf.Write(b4[:])
			buf.Write(b)
		}
	}
	raw := int64(buf.Len())
	var zout bytes.Buffer
	zw := gzip.NewWriter(&zout)
	if _, err := zw.Write(buf.Bytes()); err != nil {
		return nil, 0, err
	}
	if err := zw.Close(); err != nil {
		return nil, 0, err
	}
	return zout.Bytes(), raw, nil
}

func decodeColdChunk(z []byte) ([]int64, []any, error) {
	zr, err := gzip.NewReader(bytes.NewReader(z))
	if err != nil {
		return nil, nil, err
	}
	var out bytes.Buffer
	if _, err := out.ReadFrom(zr); err != nil {
		return nil, nil, err
	}
	_ = zr.Close()
	b := out.Bytes()
	var tss []int64
	var vals []any
	var prev int64
	for len(b) > 0 {
		d, n := binary.Varint(b)
		if n <= 0 {
			return nil, nil, fmt.Errorf("bad varint")
		}
		b = b[n:]
		prev += d
		tss = append(tss, prev)
		if len(b) == 0 {
			return nil, nil, fmt.Errorf("truncated value tag")
		}
		tag := b[0]
		b = b[1:]
		switch tag {
		case 0:
			vals = append(vals, nil)
		case 1:
			if len(b) < 8 {
				return nil, nil, fmt.Errorf("truncated float")
			}
			vals = append(vals, math.Float64frombits(binary.BigEndian.Uint64(b[:8])))
			b = b[8:]
		case 2:
			if len(b) < 4 {
				return nil, nil, fmt.Errorf("truncated text len")
			}
			l := int(binary.BigEndian.Uint32(b[:4]))
			b = b[4:]
			if l < 0 || l > len(b) {
				return nil, nil, fmt.Errorf("bad text len")
			}
			vals = append(vals, string(b[:l]))
			b = b[l:]
		default:
			return nil, nil, fmt.Errorf("bad value tag %d", tag)
		}
	}
	return tss, vals, nil
}

// cellInt64 行值 → int64。
func cellInt64(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case int64:
		return x
	case float64:
		return int64(x)
	case []byte:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	default:
		return 0
	}
}

// ---------------------------------------------------------------------------
// 后台冷压缩策略（timeseries.cold_after_days）
// ---------------------------------------------------------------------------

// RunColdPolicy 周期扫描：各库的时序表（声明表 + 隐式 points）超过
// cold_after_days 的数据自动固化（v1.3.11：不再固定 points 表名）。
func RunColdPolicy(backend *sqlite.Backend, cfg *config.Config) {
	if cfg.Timeseries.ColdAfterDays <= 0 {
		return
	}
	go func() {
		for {
			time.Sleep(time.Duration(cfg.Timeseries.ColdScanMinSec) * time.Second)
			runColdPolicyOnce(backend, cfg)
		}
	}()
}

// coldPolicyTables 某库参与后台冷压缩的表：sidecar 声明表 + 隐式 points（去重）。
func coldPolicyTables(sess *sqlite.Session) []tsTable {
	out := loadTsTablesSess(sess)
	if _, rows, err := sess.Query(`PRAGMA main.table_info("points")`); err == nil && len(rows) > 0 {
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
	return out
}

func runColdPolicyOnce(backend *sqlite.Backend, cfg *config.Config) {
	defer func() {
		if r := recover(); r != nil {
			logx.Errorf("cold policy panic: %v", r)
		}
	}()
	for _, db := range backend.ListDatabases() {
		sess, err := backend.OpenSession("cold-policy", "loop", db)
		if err != nil {
			continue
		}
		func() {
			defer sess.Close()
			boundaryMS := time.Now().UnixMilli() - int64(cfg.Timeseries.ColdAfterDays)*86400000
			for _, t := range coldPolicyTables(sess) {
				boundary := msToUnitValue(boundaryMS, t.Unit)
				step := msToUnitValue(86400000, t.Unit)
				st, err := compressColdOnSession(sess, t.Table, boundary, step)
				if err != nil {
					logx.Debugf("cold policy %s.%s: %v", db, t.Table, err)
					continue
				}
				if st.Chunks > 0 {
					logx.Infof("cold policy %s.%s: chunks=%d rows=%d ratio=%.2f", db, t.Table, st.Chunks, st.Rows, st.Ratio())
				}
			}
		}()
	}
}
