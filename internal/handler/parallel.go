// Package handler — 多核并行查询（v1.0.4 引入，v1.0.8 增强）：网关层「分片-并行-合并」，不改 SQLite。
// 触发方式：
//   1) SQL 注释 hint `/*+ PARALLEL */` 或 `/*+ PARALLEL(4) */`（显式并行度）；
//   2) 自动触发：单表可合并聚合语句，行数估算 ≥ parallel_query.scan_rows（默认 100 万），
//      并行度取 parallel_query.workers（0 = CPU 核数）；scan_rows=0 关闭自动触发。
// 适用：单表 + 顶层可合并聚合（COUNT/SUM/MIN/MAX/AVG），无 GROUP BY/ORDER/LIMIT；
// 分片键优先 rowid，其次单列主键（WITHOUT ROWID 表）；不满足条件自动回退串行。
// EXPLAIN PARALLEL <query>：查看拆分计划（分片键 / 估算行数 / 并行度 / 触发方式）。
package handler

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
)

var reParallelHint = regexp.MustCompile(`(?i)/\*\+\s*PARALLEL(?:\((\d+)\))?\s*\*/`)

// parallelHint 提取 hint，返回去 hint 后的语句与并行度（0 = 无 hint）。
func parallelHint(q string) (string, int) {
	m := reParallelHint.FindStringSubmatch(q)
	if m == nil {
		return q, 0
	}
	n := 0
	if m[1] != "" {
		fmt.Sscanf(m[1], "%d", &n)
	}
	return reParallelHint.ReplaceAllString(q, ""), n
}

var (
	reParAgg = regexp.MustCompile("(?is)^SELECT\\s+(.+?)\\s+FROM\\s+(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)(?:\\s+WHERE\\s+(.+))?$")
	reAggCol = regexp.MustCompile("(?i)^(COUNT|SUM|MIN|MAX|AVG)\\s*\\(\\s*(\\*|[A-Za-z0-9_.\\x{4e00}-\\x{9fa5}]+)\\s*\\)$")
)

type aggCol struct {
	raw string // 原列文本（作结果列名）
	fn  string // COUNT/SUM/MIN/MAX/AVG
	arg string // * 或列名
}

// parallelPlan 一次并行执行的拆分计划。
type parallelPlan struct {
	table, shardCol string
	lo, hi, estRows int64
	deg             int
	hint            bool   // true=hint 触发；false=自动触发
	cols            []aggCol
	where           string
}

// autoDeg 返回自动触发的并行度（0 = 自动触发关闭）。
func (h *Handler) autoDeg() int {
	if h.cfg.ParallelQuery.Disable || h.cfg.ParallelQuery.ScanRows <= 0 {
		return 0
	}
	n := h.cfg.ParallelQuery.Workers
	if n <= 0 {
		n = runtime.NumCPU()
	}
	if n > 32 {
		n = 32
	}
	if n < 2 {
		n = 2
	}
	return n
}

// buildParallelPlan 分析语句是否可并行拆分；不可行时返回 nil 与原因（供 EXPLAIN PARALLEL 展示）。
func (h *Handler) buildParallelPlan(sq string, deg int) (*parallelPlan, string) {
	if deg <= 0 || h.cfg.ParallelQuery.Disable {
		return nil, "parallel_query disabled"
	}
	if deg > 32 {
		deg = 32
	}
	m := reParAgg.FindStringSubmatch(sq)
	if m == nil {
		return nil, "form not supported: expect single-table top-level mergeable aggregates (COUNT/SUM/MIN/MAX/AVG, no GROUP BY/ORDER/LIMIT)"
	}
	colsExpr, table, where := strings.TrimSpace(m[1]), strings.Trim(m[2], "`"), strings.TrimSpace(m[3])
	table = strings.Trim(table, `"`)

	// 解析聚合列：任一列不是可合并聚合即回退串行
	parts := strings.Split(colsExpr, ",")
	cols := make([]aggCol, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		am := reAggCol.FindStringSubmatch(t)
		if am == nil {
			return nil, "column not mergeable: " + t
		}
		cols = append(cols, aggCol{raw: t, fn: strings.ToUpper(am[1]), arg: am[2]})
	}

	// 分片键：rowid 可用则用；否则仅支持单列主键（WITHOUT ROWID 表）
	shardCol := ""
	if _, rows, err := h.session.Query(fmt.Sprintf("SELECT min(rowid), max(rowid) FROM %s", quoteIdentLocal(table))); err == nil && len(rows) > 0 {
		shardCol = "rowid"
	} else {
		_, ti, terr := h.session.Query(fmt.Sprintf("PRAGMA main.table_info(%s)", quoteIdentLocal(table)))
		if terr != nil {
			return nil, "table not found: " + table
		}
		var pkCol string
		pkCount, pkMaxOrd := 0, 0
		for _, r := range ti {
			if len(r) >= 6 {
				if p := cellInt(r[5]); p > 0 {
					pkCount++
					if int(p) > pkMaxOrd {
						pkMaxOrd = int(p)
						pkCol = cellStr(r[1])
					}
				}
			}
		}
		if pkCount == 1 { // 仅单列主键
			shardCol = pkCol
		}
	}
	if shardCol == "" {
		return nil, "no shard key (need rowid or single-column primary key)"
	}

	var lo, hi int64
	_, rows, err := h.session.Query(fmt.Sprintf("SELECT min(%s), max(%s) FROM %s",
		quoteIdentLocal(shardCol), quoteIdentLocal(shardCol), quoteIdentLocal(table)))
	if err != nil || len(rows) == 0 {
		return nil, "cannot read shard key range"
	}
	if rows[0][0] == nil || rows[0][1] == nil {
		return nil, "empty table" // 回退串行（串行可正确返回 0/NULL）
	}
	lo, hi = cellInt(rows[0][0]), cellInt(rows[0][1])
	est := hi - lo + 1
	if est < 0 {
		est = 0
	}
	if est < int64(deg)*1000 { // 数据量太小不值得并行
		return nil, fmt.Sprintf("table too small (est_rows=%d < degree*1000)", est)
	}
	return &parallelPlan{table: table, shardCol: shardCol, lo: lo, hi: hi,
		estRows: est, deg: deg, cols: cols, where: where}, ""
}

// tryParallel 尝试并行执行；ok=false 表示不满足条件需回退串行。
func (h *Handler) tryParallel(sq string, deg int, hint bool) (*mysql.Result, bool, error) {
	p, _ := h.buildParallelPlan(sq, deg)
	if p == nil {
		return nil, false, nil
	}
	p.hint = hint
	return h.runParallelPlan(p)
}

// maybeAutoParallel 自动触发：语句形态匹配且估算行数达阈值时并行执行；
// 不满足则静默走串行（ok=false）。
func (h *Handler) maybeAutoParallel(sq string) (*mysql.Result, bool, error) {
	deg := h.autoDeg()
	if deg <= 0 || !reParAgg.MatchString(sq) {
		return nil, false, nil
	}
	p, _ := h.buildParallelPlan(sq, deg)
	if p == nil || p.estRows < h.cfg.ParallelQuery.ScanRows {
		return nil, false, nil
	}
	return h.runParallelPlan(p)
}

// runParallelPlan 按计划分片并行执行并合并部分聚合。
func (h *Handler) runParallelPlan(p *parallelPlan) (*mysql.Result, bool, error) {
	cols, deg := p.cols, p.deg
	// 子查询列：AVG 拆 sum+count 两列
	subCols := make([]string, 0, len(cols))
	for _, c := range cols {
		switch c.fn {
		case "AVG":
			subCols = append(subCols, fmt.Sprintf("sum(%s), count(%s)", c.arg, c.arg))
		default:
			subCols = append(subCols, c.raw)
		}
	}
	condExtra := ""
	if p.where != "" {
		condExtra = " AND (" + p.where + ")"
	}

	// 并行执行：每分片一个临时只读会话（WAL 读不阻塞；Query 内含 global_threads 闸门）
	step := (p.hi - p.lo + 1) / int64(deg)
	results := make([][]any, deg)
	errs := make([]error, deg)
	var wg sync.WaitGroup
	started := time.Now()
	for i := 0; i < deg; i++ {
		a := p.lo + step*int64(i)
		b := p.lo + step*(int64(i)+1) - 1
		if i == deg-1 {
			b = p.hi
		}
		wg.Add(1)
		go func(i int, a, b int64) {
			defer wg.Done()
			s, err := h.backend.OpenSession("parallel", h.remote, h.session.CurrentDB())
			if err != nil {
				errs[i] = err
				return
			}
			defer s.Close()
			q := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d AND %s <= %d%s",
				strings.Join(subCols, ", "), quoteIdentLocal(p.table),
				quoteIdentLocal(p.shardCol), a, quoteIdentLocal(p.shardCol), b, condExtra)
			_, rows, err := s.Query(q)
			if err != nil {
				errs[i] = err
				return
			}
			if len(rows) > 0 {
				results[i] = rows[0]
			}
		}(i, a, b)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return nil, true, MapSQLiteErr(e)
		}
	}

	// 合并部分聚合
	merged := make([]any, len(cols))
	idx := 0
	for j, c := range cols {
		switch c.fn {
		case "COUNT":
			var total int64
			for i := range results {
				if results[i] != nil && idx < len(results[i]) && results[i][idx] != nil {
					total += cellInt(results[i][idx])
				}
			}
			merged[j] = total
			idx++
		case "SUM":
			var iSum int64
			var fSum float64
			gotFloat := false
			for i := range results {
				if results[i] == nil || idx >= len(results[i]) || results[i][idx] == nil {
					continue
				}
				switch x := results[i][idx].(type) {
				case float64:
					gotFloat, fSum = true, fSum+x
				case int64:
					iSum += x
					fSum += float64(x)
				}
			}
			if gotFloat {
				merged[j] = fSum
			} else {
				merged[j] = iSum
			}
			idx++
		case "MIN", "MAX":
			var best *float64
			for i := range results {
				if results[i] == nil || idx >= len(results[i]) || results[i][idx] == nil {
					continue
				}
				f := cellFloat(results[i][idx])
				if best == nil || (c.fn == "MIN" && f < *best) || (c.fn == "MAX" && f > *best) {
					best = &f
				}
			}
			if best != nil {
				merged[j] = *best
			}
			idx++
		case "AVG": // 子查询输出 sum,count 两列
			var fs float64
			var ci int64
			for i := range results {
				if results[i] == nil {
					continue
				}
				if idx < len(results[i]) && results[i][idx] != nil {
					fs += cellFloat(results[i][idx])
				}
				if idx+1 < len(results[i]) && results[i][idx+1] != nil {
					ci += cellInt(results[i][idx+1])
				}
			}
			if ci > 0 {
				merged[j] = fs / float64(ci)
			}
			idx += 2
		}
	}

	logx.Infof("parallel query: table=%s degree=%d est_rows=%d hint=%v cost=%dms",
		p.table, deg, p.estRows, p.hint, time.Since(started).Milliseconds())
	rs, err := meta.BuildResultset(colNames(cols), [][]any{merged})
	if err != nil {
		return nil, true, err
	}
	return mysql.NewResult(rs), true, nil
}

// explainParallel 生成 EXPLAIN PARALLEL <query> 的拆分计划说明。
func (h *Handler) explainParallel(sq string, deg int) (*mysql.Result, error) {
	p, reason := h.buildParallelPlan(sq, deg)
	if p == nil {
		return meta.SimpleRS([]string{"table", "shard_col", "est_rows", "degree", "strategy"},
			[][]any{{"(not eligible)", "", int64(0), int64(0), reason}})
	}
	aggs := make([]string, len(p.cols))
	for i, c := range p.cols {
		aggs[i] = c.raw
	}
	return meta.SimpleRS([]string{"table", "shard_col", "est_rows", "degree", "strategy"},
		[][]any{{p.table, p.shardCol, p.estRows, int64(p.deg),
			"shard range scan; " + strings.Join(aggs, ", ")}})
}

func colNames(cols []aggCol) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.raw
	}
	return out
}

func cellInt(v any) int64 {
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

func cellFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case []byte:
		var f float64
		fmt.Sscanf(string(x), "%g", &f)
		return f
	case string:
		var f float64
		fmt.Sscanf(x, "%g", &f)
		return f
	}
	return 0
}

func cellStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

func quoteIdentLocal(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
