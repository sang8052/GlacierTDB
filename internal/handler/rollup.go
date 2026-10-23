package handler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
)

// 时序 rollup 多粒度预聚合（v1.1.1，FEATURE §5 P0）：
//
// 回填同遍落：points 表 INSERT 触发器同步更新汇总表 points_m1 / points_h1 / points_d1
//（1 分钟 / 1 小时 / 1 天桶：cnt / cntv / sum / min / max）；存量数据 REBUILD ROLLUP 回填。
//
// 网关自动路由：单表聚合查询带时间范围时，改走「汇总表满桶 + 原始表边缘桶」UNION，
// 选取可用的最粗粒度（桶宽整除 GROUP BY 桶宽且范围内至少一个满桶），
// 结果与全量扫原始表精确一致（边缘桶回退原始行，无近似）。
//
// 适用 schema：schema/timeseries.sql 约定的 points(series_id INTEGER, ts INTEGER, val REAL)。
const (
	grainM1 = 60000
	grainH1 = 3600000
	grainD1 = 86400000
)

// rollupGrain 汇总粒度定义。
type rollupGrain struct {
	suffix   string // m1/h1/d1
	periodMs int64
}

var rollupGrains = []rollupGrain{
	{"d1", grainD1},
	{"h1", grainH1},
	{"m1", grainM1},
}

// rollupMade 已初始化（建表+触发器）的 (db, table)。
var rollupMade sync.Map

var (
	// SELECT <list> FROM points [WHERE ...] [GROUP BY ...]
	reRollSel = regexp.MustCompile("(?is)^SELECT\\s+(.+?)\\s+FROM\\s+`?points`?(?:\\s+AS\\s+\\w+)?\\s*(?:WHERE\\s+(.+?))?\\s*(?:GROUP\\s+BY\\s+(.+?))?\\s*$")
	// INSERT INTO points（写路径懒建触发器用）
	reRollInsert = regexp.MustCompile("(?i)^\\s*(?:INSERT\\s+(?:OR\\s+[A-Z]+\\s+)?INTO\\s+|REPLACE\\s+INTO\\s+)`?points`?(?:\\s|\\()")
	// GROUP BY [series_id,] (ts/N*N) | time(N)
	reRollGB = regexp.MustCompile(`(?i)^\s*(?:series_id\s*,\s*)?(?:\(\s*ts\s*/\s*(\d+)\s*\*\s*\d+\s*\)|time\s*\(\s*(\d+)\s*\))\s*$`)
	// WHERE 里的时间谓词：(ts|time) op <val>
	reRollTsPred = regexp.MustCompile(`(?i)^\s*(ts|time)\s*(>=|<=|>|<|=)\s*(.+?)\s*$`)
	// 时间字面量：整数 / 'YYYY-MM-DD[ HH:MM:SS]' / NOW()±INTERVAL / NOW()
	reRollTimeVal = regexp.MustCompile(`(?i)^(-?\d+)$|^'(\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)'$|^(NOW|CURRENT_TIMESTAMP)\s*\(\s*\)(?:\s*([+-])\s*INTERVAL\s+'?(\d+)\s*(SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?'?)?$`)
	// 选择项
	reRollSeriesCol = regexp.MustCompile(`(?i)^series_id$`)
	reRollBucketCol = regexp.MustCompile(`(?i)^(?:\(\s*ts\s*/\s*(\d+)\s*\*\s*\d+\s*\)|time\s*\(\s*(\d+)\s*\)|ts\s*/\s*(\d+)\s*\*\s*\d+)(?:\s+(?:AS\s+)?(\w+))?$`)
	reRollAggCol    = regexp.MustCompile(`(?i)^(COUNT|SUM|MIN|MAX|AVG)\s*\(\s*(\*|val|series_id)\s*\)(?:\s+(?:AS\s+)?(\w+))?$`)
)

// rollupSelItem 选择项分类。
type rollupSelItem struct {
	kind  string // series | bucket | agg
	fn    string // COUNT/SUM/MIN/MAX/AVG
	arg   string // */val/series_id
	bn    int64  // bucket 宽（ms）
	alias string // 输出列名（缺省原样文本）
}

// ensureRollupTables 确保汇总表 + INSERT 触发器存在（幂等；schema 不符返回 false）。
func (h *Handler) ensureRollupTables() bool {
	db := h.session.CurrentDB()
	key := db + "/points"
	if _, ok := rollupMade.Load(key); ok {
		// 缓存命中仍验证一次（库可能被 DROP/CREATE 重建或汇总表被删）：
		// sqlite_master 探测开销极小，缺失则回落重建
		if _, rows, err := h.session.Query(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='points_m1'`); err == nil && len(rows) > 0 {
			return true
		}
		rollupMade.Delete(key)
	}
	// schema 校验：points 必须有 series_id / ts / val 三列
	_, ti, err := h.session.Query(`PRAGMA main.table_info("points")`)
	if err != nil || len(ti) == 0 {
		return false
	}
	cols := map[string]bool{}
	for _, r := range ti {
		if len(r) >= 2 {
			cols[strings.ToLower(fmt.Sprintf("%v", r[1]))] = true
		}
	}
	if !cols["series_id"] || !cols["ts"] || !cols["val"] {
		return false
	}
	for _, g := range rollupGrains {
		t := "points_" + g.suffix
		ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  series_id INTEGER NOT NULL,
  bucket_ts INTEGER NOT NULL,
  cnt  INTEGER NOT NULL DEFAULT 0,
  cntv INTEGER NOT NULL DEFAULT 0,
  sum_val REAL,
  min_val REAL,
  max_val REAL,
  PRIMARY KEY (series_id, bucket_ts)
)`, t)
		if _, _, err := h.session.Exec(ddl); err != nil {
			logx.Warnf("rollup create %s: %v", t, err)
			return false
		}
		trg := fmt.Sprintf(`CREATE TRIGGER IF NOT EXISTS points_rollup_ai_%s AFTER INSERT ON points BEGIN
  INSERT INTO %s(series_id,bucket_ts,cnt,cntv,sum_val,min_val,max_val)
  VALUES (NEW.series_id, NEW.ts/%d*%d, 1, CASE WHEN NEW.val IS NULL THEN 0 ELSE 1 END, NEW.val, NEW.val, NEW.val)
  ON CONFLICT(series_id,bucket_ts) DO UPDATE SET
    cnt = cnt + 1,
    cntv = cntv + excluded.cntv,
    sum_val = CASE WHEN excluded.sum_val IS NULL THEN sum_val WHEN sum_val IS NULL THEN excluded.sum_val ELSE sum_val + excluded.sum_val END,
    min_val = CASE WHEN excluded.min_val IS NULL THEN min_val WHEN min_val IS NULL THEN excluded.min_val WHEN excluded.min_val < min_val THEN excluded.min_val ELSE min_val END,
    max_val = CASE WHEN excluded.max_val IS NULL THEN max_val WHEN max_val IS NULL THEN excluded.max_val WHEN excluded.max_val > max_val THEN excluded.max_val ELSE max_val END;
END`, g.suffix, t, g.periodMs, g.periodMs)
		if _, _, err := h.session.Exec(trg); err != nil {
			logx.Warnf("rollup create trigger %s: %v", t, err)
			return false
		}
	}
	rollupMade.Store(key, true)
	return true
}

// rebuildRollup 从原始表回填全部汇总表（REBUILD ROLLUP；返回各粒度行数）。
func (h *Handler) rebuildRollup() ([]string, [][]any, error) {
	if !h.ensureRollupTables() {
		return nil, nil, fmt.Errorf("points 表不存在或 schema 不符（需要 series_id/ts/val）")
	}
	var rows [][]any
	for _, g := range rollupGrains {
		t := "points_" + g.suffix
		if _, _, err := h.session.Exec("DELETE FROM " + t); err != nil {
			return nil, nil, err
		}
		q := fmt.Sprintf(`INSERT INTO %s(series_id,bucket_ts,cnt,cntv,sum_val,min_val,max_val)
			SELECT series_id, ts/%d*%d, count(*), count(val), sum(val), min(val), max(val)
			FROM points GROUP BY series_id, ts/%d*%d`, t, g.periodMs, g.periodMs, g.periodMs, g.periodMs)
		if _, _, err := h.session.Exec(q); err != nil {
			return nil, nil, err
		}
		var n int64
		_, r, err := h.session.Query("SELECT count(*) FROM " + t)
		if err == nil && len(r) > 0 {
			n = cellInt(r[0][0])
		}
		rows = append(rows, []any{t, n, int64(g.periodMs)})
	}
	return []string{"rollup_table", "rows", "bucket_ms"}, rows, nil
}

// tryRollup rollup 自动路由入口；ok=false 表示形态不匹配/无需路由（走原路径）。
func (h *Handler) tryRollup(sq string) (*mysql.Result, bool, error) {
	if h.cfg.Rollup.Disable || !reRollSel.MatchString(sq) {
		return nil, false, nil
	}
	m := reRollSel.FindStringSubmatch(sq)
	selList, where, groupBy := strings.TrimSpace(m[1]), strings.TrimSpace(m[2]), strings.TrimSpace(m[3])

	// 选择项解析
	var items []rollupSelItem
	bucketN := int64(0)
	for _, p := range splitTopLevelLocal(selList) {
		t := strings.TrimSpace(p)
		if t == "" {
			return nil, false, nil
		}
		it, ok := parseRollupSelItem(t)
		if !ok {
			return nil, false, nil // 形态不符：回退原路径
		}
		if it.kind == "bucket" {
			if it.bn <= 0 {
				return nil, false, nil
			}
			if bucketN != 0 && bucketN != it.bn {
				return nil, false, nil
			}
			bucketN = it.bn
		}
		items = append(items, it)
	}
	if len(items) == 0 {
		return nil, false, nil
	}

	// GROUP BY 解析（可空；仅支持 [series_id,] (ts/N*N)/time(N)）
	groupSeries := false
	if groupBy != "" {
		gm := reRollGB.FindStringSubmatch(groupBy)
		if gm == nil {
			return nil, false, nil
		}
		n, _ := strconv.ParseInt(gm[1]+gm[2], 10, 64)
		if n <= 0 {
			return nil, false, nil
		}
		if bucketN != 0 && bucketN != n {
			return nil, false, nil
		}
		bucketN = n
		groupSeries = strings.Contains(strings.ToLower(groupBy), "series_id")
	}

	// WHERE 解析：时间范围 + series_id 过滤（其余一票否决）
	lo, hi, seriesCond, ok := parseRollupWhere(where)
	if !ok {
		return nil, false, nil
	}
	if lo > hi {
		return nil, false, nil
	}

	if !h.ensureRollupTables() {
		return nil, false, nil
	}

	// 粒度选择：最粗的满足（GROUP BY 桶宽整除 + 至少一个满桶）的粒度
	var gr rollupGrain
	found := false
	for _, g := range rollupGrains {
		if bucketN > 0 && bucketN%g.periodMs != 0 {
			continue
		}
		loB := ceilDiv(lo, g.periodMs) * g.periodMs
		hiB := floorDiv(hi-g.periodMs+1, g.periodMs) * g.periodMs
		if loB > hiB {
			continue
		}
		gr, found = g, true
		break
	}
	if !found {
		return nil, false, nil
	}
	p := gr.periodMs
	loB := ceilDiv(lo, p) * p
	hiB := floorDiv(hi-p+1, p) * p

	// 组装 UNION SQL：汇总表满桶 + 原始表左右边缘
	cond := ""
	if seriesCond != "" {
		cond = " AND (" + seriesCond + ")"
	}
	sub := fmt.Sprintf(
		`SELECT bucket_ts AS ts, series_id, cnt, cntv, sum_val, min_val, max_val FROM points_%s WHERE bucket_ts >= %d AND bucket_ts <= %d%s
  UNION ALL
  SELECT ts, series_id, 1 AS cnt, CASE WHEN val IS NULL THEN 0 ELSE 1 END AS cntv, val AS sum_val, val AS min_val, val AS max_val FROM points WHERE ts >= %d AND ts < %d%s
  UNION ALL
  SELECT ts, series_id, 1 AS cnt, CASE WHEN val IS NULL THEN 0 ELSE 1 END AS cntv, val AS sum_val, val AS min_val, val AS max_val FROM points WHERE ts > %d AND ts <= %d%s`,
		gr.suffix, loB, hiB, cond,
		lo, loB, cond,
		hiB+p-1, hi, cond)

	var outerCols []string
	for _, it := range items {
		switch it.kind {
		case "series":
			outerCols = append(outerCols, "series_id")
		case "bucket":
			outerCols = append(outerCols, fmt.Sprintf("(ts/%d*%d)", bucketN, bucketN))
		case "agg":
			switch it.fn {
			case "COUNT":
				if it.arg == "val" {
					outerCols = append(outerCols, "SUM(cntv)")
				} else {
					outerCols = append(outerCols, "SUM(cnt)")
				}
			case "SUM":
				outerCols = append(outerCols, "SUM(sum_val)")
			case "MIN":
				outerCols = append(outerCols, "MIN(min_val)")
			case "MAX":
				outerCols = append(outerCols, "MAX(max_val)")
			case "AVG":
				outerCols = append(outerCols, "(SUM(sum_val)*1.0/SUM(cntv))")
			}
		}
	}
	q := "SELECT " + strings.Join(outerCols, ", ") + " FROM (" + sub + ")"
	if groupBy != "" {
		g := ""
		if groupSeries {
			g = "series_id, "
		}
		q += " GROUP BY " + g + fmt.Sprintf("(ts/%d*%d)", bucketN, bucketN)
	}
	cols, rows, err := h.session.Query(q)
	if err != nil {
		// 路由失败保守回退原路径（不吞错误）
		logx.Warnf("rollup routed query failed, fallback to raw: %v", err)
		return nil, false, nil
	}
	logx.Debugf("rollup route: grain=%s bucket=%dms range=[%d,%d] rows=%d", gr.suffix, bucketN, lo, hi, len(rows))
	rs, err := meta.BuildResultset(rollupColNames(items), rows)
	if err != nil {
		return nil, true, err
	}
	_ = cols
	return mysql.NewResult(rs), true, nil
}

// rollupColNames 输出列名（带别名用别名，否则原样表达式文本）。
func rollupColNames(items []rollupSelItem) []string {
	out := make([]string, len(items))
	for i, it := range items {
		if it.alias != "" {
			out[i] = it.alias
		} else {
			out[i] = it.fn + "(" + it.arg + ")"
			if it.kind == "series" {
				out[i] = "series_id"
			}
			if it.kind == "bucket" {
				out[i] = fmt.Sprintf("(ts/%d*%d)", it.bn, it.bn)
			}
		}
	}
	return out
}

// parseRollupSelItem 解析单个选择项。
func parseRollupSelItem(t string) (rollupSelItem, bool) {
	var it rollupSelItem
	if reRollSeriesCol.MatchString(t) {
		return rollupSelItem{kind: "series"}, true
	}
	if m := reRollBucketCol.FindStringSubmatch(t); m != nil {
		n, _ := strconv.ParseInt(m[1]+m[2]+m[3], 10, 64)
		it = rollupSelItem{kind: "bucket", bn: n, alias: m[4]}
		if n <= 0 {
			return it, false
		}
		return it, true
	}
	if m := reRollAggCol.FindStringSubmatch(t); m != nil {
		return rollupSelItem{kind: "agg", fn: strings.ToUpper(m[1]),
			arg: strings.ToLower(m[2]), alias: m[3]}, true
	}
	return it, false
}

// parseRollupWhere 解析 WHERE：时间范围 [lo,hi]（epoch ms，闭区间）+ series_id 条件透传。
// 其他谓词（如 val>5）出现时返回 ok=false（预聚合无法精确下推）。
func parseRollupWhere(where string) (lo, hi int64, seriesCond string, ok bool) {
	lo, hi = 0, 0
	hasLo, hasHi := false, false
	if where == "" {
		return 0, 0, "", false // 无时间范围：不路由（全量聚合走原路径即可）
	}
	terms := splitTopLevelAnd(where)
	var seriesTerms []string
	for _, t := range terms {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if m := reRollTsPred.FindStringSubmatch(t); m != nil {
			v, vok := evalRollupTime(strings.TrimSpace(m[3]))
			if !vok {
				return 0, 0, "", false
			}
			switch m[2] {
			case ">=":
				if !hasLo || v > lo {
					lo, hasLo = v, true
				}
			case ">":
				if !hasLo || v+1 > lo {
					lo, hasLo = v+1, true
				}
			case "<=":
				if !hasHi || v < hi {
					hi, hasHi = v, true
				}
			case "<":
				if !hasHi || v-1 < hi {
					hi, hasHi = v-1, true
				}
			case "=":
				if !hasLo || v > lo {
					lo, hasLo = v, true
				}
				if !hasHi || v < hi {
					hi, hasHi = v, true
				}
			}
			continue
		}
		// series_id 过滤：原样透传
		low := strings.ToLower(t)
		if strings.HasPrefix(low, "series_id") {
			seriesTerms = append(seriesTerms, t)
			continue
		}
		return 0, 0, "", false
	}
	if !hasLo || !hasHi {
		return 0, 0, "", false
	}
	return lo, hi, strings.Join(seriesTerms, " AND "), true
}

// evalRollupTime 计算时间表达式 → epoch ms（整数 / 日期串(UTC) / NOW()±INTERVAL）。
func evalRollupTime(s string) (int64, bool) {
	m := reRollTimeVal.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	switch {
	case m[1] != "":
		n, err := strconv.ParseInt(m[1], 10, 64)
		return n, err == nil
	case m[2] != "":
		ds := strings.Replace(m[2], "T", " ", 1)
		for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02"} {
			if t, err := time.ParseInLocation(layout, ds, time.UTC); err == nil {
				return t.UnixMilli(), true
			}
		}
		return 0, false
	default: // NOW()/CURRENT_TIMESTAMP [± INTERVAL n UNIT]
		base := time.Now().UnixMilli()
		if m[4] == "" {
			return base, true
		}
		n, _ := strconv.ParseInt(m[5], 10, 64)
		var ms int64
		switch strings.ToUpper(m[6]) {
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
			ms = n * 2592000000
		case "YEAR":
			ms = n * 31536000000
		default:
			return 0, false
		}
		if m[4] == "-" {
			return base - ms, true
		}
		return base + ms, true
	}
}

func ceilDiv(a, b int64) int64 {
	if a >= 0 {
		return (a + b - 1) / b
	}
	return a / b
}

func floorDiv(a, b int64) int64 {
	if a >= 0 {
		return a / b
	}
	return (a - b + 1) / b
}

// splitTopLevelLocal 顶层逗号切分（括号/引号内不切）。
func splitTopLevelLocal(s string) []string {
	var out []string
	depth := 0
	var quote byte
	last := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' && (quote == '\'' || quote == '"') {
				i++
				continue
			}
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
				out = append(out, s[last:i])
				last = i + 1
			}
		}
	}
	out = append(out, s[last:])
	return out
}

// splitTopLevelAnd 顶层 AND 切分（括号/引号内不切；OR 出现整体否决由调用方判断）。
func splitTopLevelAnd(s string) []string {
	if reOrOutside.MatchString(s) {
		return []string{s} // 含 OR：整体作为未知谓词处理 → 不路由
	}
	var out []string
	depth := 0
	var quote byte
	last := 0
	lower := strings.ToLower(s)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			if c == '\\' && (quote == '\'' || quote == '"') {
				i++
				continue
			}
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
		case 'a', 'A':
			if depth == 0 && strings.HasPrefix(lower[i:], "and ") {
				out = append(out, s[last:i])
				last = i + 3
				i += 2
			}
		}
	}
	out = append(out, s[last:])
	return out
}

var reOrOutside = regexp.MustCompile(`(?i)\bOR\b`)
