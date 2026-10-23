package handler

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
)

// 并行查询扩展（v1.1.1，FEATURE §5）：
//
//  1. GROUP BY 分片并行：`GROUP BY <series>, <桶表达式>` 按 series 列取模分片，
//     Go 侧按 (series, 桶) 键合并部分聚合——时序长范围分组查询的多核并行；
//  2. 明细导出并行（方案 B）：单表明细 SELECT 按 rowid/单列主键区间并行 scan，
//     行包并行读、按分片顺序顺序发送（结果顺序 = 分片键升序）。
//
// 与 v1.0.4 顶层聚合并行同机制：hint `/*+ PARALLEL(n) */` 显式触发，
// 估算行数 ≥ parallel_query.scan_rows 自动触发；不满足条件自动回退串行。

var (
	// SELECT <sel> FROM <t> [WHERE ...] GROUP BY [series,] <bucket>
	reGrpStmt = regexp.MustCompile("(?is)^SELECT\\s+(.+?)\\s+FROM\\s+(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)(?:\\s+AS\\s+\\w+)?\\s*(?:WHERE\\s+(.+?))?\\s*GROUP\\s+BY\\s+(.+?)\\s*$")
	// GROUP BY 形态：[series_col ,] 桶表达式（(ts/N*N) / ts/N*N / time(N) 原始形态）
	reGrpBy = regexp.MustCompile(`(?i)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*(?:\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*/\s*(\d+)\s*\*\s*\d+\s*\)|([A-Za-z_][A-Za-z0-9_]*)\s*/\s*(\d+)\s*\*\s*\d+|time\s*\(\s*(\d+)\s*\))\s*$`)
	// 仅桶分组（无 series 列）
	reGrpByBucketOnly = regexp.MustCompile(`(?i)^\s*(?:\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*/\s*(\d+)\s*\*\s*\d+\s*\)|([A-Za-z_][A-Za-z0-9_]*)\s*/\s*(\d+)\s*\*\s*\d+|time\s*\(\s*(\d+)\s*\))\s*$`)
	// 聚合列（与 rollup 相同族）
	reGrpAgg = regexp.MustCompile(`(?i)^(COUNT|SUM|MIN|MAX|AVG)\s*\(\s*(\*|[A-Za-z0-9_]+)\s*\)$`)
	// 明细列：标识符或 *（禁函数调用，保守）
	reDetailCol = regexp.MustCompile(`(?i)^[A-Za-z_][A-Za-z0-9_]*$|^\*$`)
	// ORDER BY 单列 [ASC]
	reOrderOne = regexp.MustCompile(`(?i)^([A-Za-z_][A-Za-z0-9_]*|rowid)(?:\s+ASC)?$`)
	// 尾部 LIMIT n
	reTailLimitN = regexp.MustCompile(`(?i)\s+LIMIT\s+(\d+)\s*$`)
	// 无 GROUP BY 的明细语句
	reDetailStmt = regexp.MustCompile("(?is)^SELECT\\s+(.+?)\\s+FROM\\s+(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)(?:\\s+AS\\s+\\w+)?\\s*(?:WHERE\\s+(.+?))?$")
	// 选择项尾部别名
	reGrpAlias = regexp.MustCompile(`(?i)\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)$`)
)

// grpKey 分组键（series 值, 桶起点）。
type grpKey struct {
	series, bucket int64
}

// groupShardPlan GROUP BY 分片并行计划。
type groupShardPlan struct {
	table      string
	seriesCol  string
	bucketCol  string
	bucketN    int64
	selSeries  bool // 输出含 series 列
	selBucket  bool // 输出含桶列
	cols       []aggCol
	where      string
	deg        int
	lo, hi     int64
	estRows    int64
	hint       bool
	aliasName  string // 桶列输出名
}

// detailPlan 明细导出并行计划（方案 B）。
type detailPlan struct {
	table, shardCol string
	selCols         string // 原样选择列表
	where           string
	limit           int64 // <=0 不限
	lo, hi          int64
	estRows         int
	deg             int
	hint            bool
}

// tryGroupShard GROUP BY 分片并行；ok=false 回退串行。
func (h *Handler) tryGroupShard(sq string, deg int, hint bool) (*mysql.Result, bool, error) {
	p := h.buildGroupShardPlan(sq, deg)
	if p == nil {
		return nil, false, nil
	}
	p.hint = hint
	return h.runGroupShard(p)
}

// tryDetailParallel 明细导出并行（方案 B）；ok=false 回退串行。
func (h *Handler) tryDetailParallel(sq string, deg int, hint bool) (*mysql.Result, bool, error) {
	p := h.buildDetailPlan(sq, deg)
	if p == nil {
		return nil, false, nil
	}
	p.hint = hint
	return h.runDetailParallel(p)
}

// maybeAutoParallelShard 自动触发的 GROUP/明细形态（maybeAutoParallel 不匹配后调用）。
func (h *Handler) maybeAutoParallelShard(sq string) (*mysql.Result, bool, error) {
	deg := h.autoDeg()
	if deg <= 0 {
		return nil, false, nil
	}
	if p := h.buildGroupShardPlan(sq, deg); p != nil && h.rowEstimate(p.table) >= h.cfg.ParallelQuery.ScanRows {
		p.hint = false
		return h.runGroupShard(p)
	}
	if p := h.buildDetailPlan(sq, deg); p != nil && h.rowEstimate(p.table) >= h.cfg.ParallelQuery.ScanRows {
		p.hint = false
		return h.runDetailParallel(p)
	}
	return nil, false, nil
}

// buildGroupShardPlan 分析 GROUP BY 分片形态。
func (h *Handler) buildGroupShardPlan(sq string, deg int) *groupShardPlan {
	if deg <= 0 || h.cfg.ParallelQuery.Disable || deg > 32 {
		return nil
	}
	m := reGrpStmt.FindStringSubmatch(sq)
	if m == nil {
		return nil
	}
	selList, table, where, groupBy := strings.TrimSpace(m[1]), strings.Trim(m[2], "`\""), strings.TrimSpace(m[3]), strings.TrimSpace(m[4])

	// GROUP BY：series, bucket | bucket
	var seriesCol, bucketCol string
	var bucketN int64
	if gm := reGrpBy.FindStringSubmatch(groupBy); gm != nil {
		seriesCol = gm[1]
		bucketCol = gm[2] + gm[4]
		bucketN = atoiSafeLocal(gm[3] + gm[5] + gm[6])
	} else if gm := reGrpByBucketOnly.FindStringSubmatch(groupBy); gm != nil {
		bucketCol = gm[1] + gm[3]
		bucketN = atoiSafeLocal(gm[2] + gm[4] + gm[5])
	} else {
		return nil
	}
	if bucketCol == "" || bucketN <= 0 {
		return nil
	}

	// 选择项：series 列 / 桶表达式 / 可合并聚合
	p := &groupShardPlan{table: table, seriesCol: seriesCol, bucketCol: bucketCol,
		bucketN: bucketN, where: where, deg: deg}
	for _, part := range splitTopLevelLocal(selList) {
		t := strings.TrimSpace(part)
		if seriesCol != "" && strings.EqualFold(t, seriesCol) {
			p.selSeries = true
			continue
		}
		// 桶列：(ts/N*N) / ts/N*N / time(N)（允许 AS 别名）
		name := t
		if loc := reGrpAlias.FindStringSubmatchIndex(t); loc != nil {
			name = strings.TrimSpace(t[:loc[0]])
			p.aliasName = t[loc[2]:loc[3]]
		}
		if isBucketExpr(name, bucketCol, bucketN) {
			p.selBucket = true
			continue
		}
		am := reGrpAgg.FindStringSubmatch(t)
		if am == nil {
			return nil // 形态不符
		}
		p.cols = append(p.cols, aggCol{raw: t, fn: strings.ToUpper(am[1]), arg: am[2]})
	}
	if len(p.cols) == 0 {
		return nil
	}

	// 分片键范围估计
	lo, hi, ok := h.shardRange(table, seriesCol)
	if !ok {
		return nil
	}
	p.lo, p.hi = lo, hi
	p.estRows = hi - lo + 1
	return p
}

// rowEstimate 便宜的行数估计（max(rowid) 代理；无 rowid 表回退 count(*)）。
func (h *Handler) rowEstimate(table string) int64 {
	if _, rows, err := h.session.Query(fmt.Sprintf("SELECT max(rowid) FROM %s", quoteIdentLocal(table))); err == nil &&
		len(rows) > 0 && rows[0][0] != nil {
		return cellInt(rows[0][0])
	}
	if _, rows, err := h.session.Query(fmt.Sprintf("SELECT count(*) FROM %s", quoteIdentLocal(table))); err == nil &&
		len(rows) > 0 {
		return cellInt(rows[0][0])
	}
	return 0
}

// isBucketExpr 判断表达式是否指定桶列。
func isBucketExpr(expr, bucketCol string, bucketN int64) bool {
	e := strings.TrimSpace(expr)
	e = strings.TrimPrefix(e, "(")
	e = strings.TrimSuffix(e, ")")
	n := fmt.Sprintf("%d", bucketN)
	patterns := []string{
		fmt.Sprintf("%s/%s*%s", bucketCol, n, n),
		fmt.Sprintf("time(%s)", n),
	}
	for _, p := range patterns {
		if strings.EqualFold(strings.ReplaceAll(e, " ", ""), strings.ReplaceAll(p, " ", "")) {
			return true
		}
	}
	return false
}

// shardRange 估计分片键 (min,max)（rowid 优先，其次给定列）。
func (h *Handler) shardRange(table, col string) (int64, int64, bool) {
	key := col
	if key == "" {
		key = "rowid"
		if _, rows, err := h.session.Query(fmt.Sprintf("SELECT min(rowid), max(rowid) FROM %s", quoteIdentLocal(table))); err != nil || len(rows) == 0 || rows[0][0] == nil {
			return 0, 0, false
		} else {
			return cellInt(rows[0][0]), cellInt(rows[0][1]), true
		}
	}
	_, rows, err := h.session.Query(fmt.Sprintf("SELECT min(%s), max(%s) FROM %s",
		quoteIdentLocal(key), quoteIdentLocal(key), quoteIdentLocal(table)))
	if err != nil || len(rows) == 0 || rows[0][0] == nil || rows[0][1] == nil {
		return 0, 0, false
	}
	return cellInt(rows[0][0]), cellInt(rows[0][1]), true
}

// runGroupShard 按 series 取模分片并行 + Go 侧按 (series, 桶) 合并。
func (h *Handler) runGroupShard(p *groupShardPlan) (*mysql.Result, bool, error) {
	deg := p.deg
	// 子查询列：series, bucket, agg…（AVG 拆 sum+count）
	subCols := []string{p.seriesCol, fmt.Sprintf("(%s/%d*%d)", p.bucketCol, p.bucketN, p.bucketN)}
	for _, c := range p.cols {
		switch c.fn {
		case "AVG":
			subCols = append(subCols, fmt.Sprintf("sum(%s), count(%s)", c.arg, c.arg))
		default:
			subCols = append(subCols, fmt.Sprintf("%s(%s)", c.fn, c.arg))
		}
	}
	cond := ""
	if p.where != "" {
		cond = " AND (" + p.where + ")"
	}

	type grpVal struct {
		out []any // 每聚合列状态：[sum,count] 或标量
	}
	groups := map[grpKey]*grpVal{}
	var gmu sync.Mutex
	errs := make([]error, deg)
	var wg sync.WaitGroup
	started := time.Now()
	for i := 0; i < deg; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := h.backend.OpenSession("group-shard", h.remote, h.session.CurrentDB())
			if err != nil {
				errs[i] = err
				return
			}
			defer s.Close()
			q := fmt.Sprintf("SELECT %s FROM %s WHERE (%s %% %d = %d)%s GROUP BY %s, %s",
				strings.Join(subCols, ", "), quoteIdentLocal(p.table),
				quoteIdentLocal(p.seriesCol), deg, i, cond,
				quoteIdentLocal(p.seriesCol),
				fmt.Sprintf("(%s/%d*%d)", p.bucketCol, p.bucketN, p.bucketN))
			_, rows, err := s.Query(q)
			if err != nil {
				errs[i] = err
				return
			}
			gmu.Lock()
			defer gmu.Unlock()
			for _, r := range rows {
				k := grpKey{series: cellInt(r[0]), bucket: cellInt(r[1])}
				g := groups[k]
				if g == nil {
					g = &grpVal{out: make([]any, len(p.cols)*2)}
					groups[k] = g
				}
				idx := 2
				for j, c := range p.cols {
					switch c.fn {
					case "COUNT":
						if g.out[j*2] == nil {
							g.out[j*2] = int64(0)
						}
						g.out[j*2] = g.out[j*2].(int64) + cellInt(r[idx])
						idx++
					case "SUM":
						if r[idx] != nil {
							if g.out[j*2] == nil {
								g.out[j*2] = float64(0)
							}
							g.out[j*2] = g.out[j*2].(float64) + cellFloat(r[idx])
						}
						idx++
					case "MIN":
						if r[idx] != nil {
							f := cellFloat(r[idx])
							if g.out[j*2] == nil || f < g.out[j*2].(float64) {
								g.out[j*2] = f
							}
						}
						idx++
					case "MAX":
						if r[idx] != nil {
							f := cellFloat(r[idx])
							if g.out[j*2] == nil || f > g.out[j*2].(float64) {
								g.out[j*2] = f
							}
						}
						idx++
					case "AVG":
						if r[idx] != nil {
							if g.out[j*2] == nil {
								g.out[j*2] = float64(0)
							}
							g.out[j*2] = g.out[j*2].(float64) + cellFloat(r[idx])
						}
						if r[idx+1] != nil {
							if g.out[j*2+1] == nil {
								g.out[j*2+1] = int64(0)
							}
							g.out[j*2+1] = g.out[j*2+1].(int64) + cellInt(r[idx+1])
						}
						idx += 2
					}
				}
			}
		}(i)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return nil, true, MapSQLiteErr(e)
		}
	}

	// 输出：按 (series, bucket) 升序
	keys := make([]grpKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sortGrpKeys(keys)
	var outRows [][]any
	var outCols []string
	if p.selSeries {
		outCols = append(outCols, p.seriesCol)
	}
	if p.selBucket {
		name := fmt.Sprintf("(%s/%d*%d)", p.bucketCol, p.bucketN, p.bucketN)
		if p.aliasName != "" {
			name = p.aliasName
		}
		outCols = append(outCols, name)
	}
	for _, c := range p.cols {
		outCols = append(outCols, c.raw)
	}
	for _, k := range keys {
		g := groups[k]
		var row []any
		if p.selSeries {
			row = append(row, k.series)
		}
		if p.selBucket {
			row = append(row, k.bucket)
		}
		for j, c := range p.cols {
			switch c.fn {
			case "COUNT":
				v := g.out[j*2]
				if v == nil {
					v = int64(0)
				}
				row = append(row, v)
			case "SUM":
				row = append(row, g.out[j*2])
			case "MIN", "MAX":
				row = append(row, g.out[j*2])
			case "AVG":
				if s, ok1 := g.out[j*2].(float64); ok1 {
					if n, ok2 := g.out[j*2+1].(int64); ok2 && n > 0 {
						row = append(row, s/float64(n))
					} else {
						row = append(row, nil)
					}
				} else {
					row = append(row, nil)
				}
			}
		}
		outRows = append(outRows, row)
	}
	logx.Infof("group-shard parallel: table=%s series=%s degree=%d groups=%d hint=%v cost=%dms",
		p.table, p.seriesCol, deg, len(outRows), p.hint, time.Since(started).Milliseconds())
	rs, err := meta.BuildResultset(outCols, outRows)
	if err != nil {
		return nil, true, err
	}
	return mysql.NewResult(rs), true, nil
}

// sortGrpKeys 按 (series, bucket) 升序。
func sortGrpKeys(keys []grpKey) {
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0; j-- {
			a, b := keys[j-1], keys[j]
			if a.series < b.series || (a.series == b.series && a.bucket <= b.bucket) {
				break
			}
			keys[j-1], keys[j] = keys[j], keys[j-1]
		}
	}
}

// buildDetailPlan 明细导出并行（方案 B）形态分析。
func (h *Handler) buildDetailPlan(sq string, deg int) *detailPlan {
	if deg <= 0 || h.cfg.ParallelQuery.Disable || deg > 32 {
		return nil
	}
	limit := int64(0)
	if loc := reTailLimitN.FindStringSubmatchIndex(sq); loc != nil {
		fmt.Sscanf(sq[loc[2]:loc[3]], "%d", &limit)
		sq = strings.TrimSpace(sq[:loc[0]])
	}
	// ORDER BY 单列（须与分片键一致）/ 无 ORDER BY
	orderBy := ""
	if i := strings.LastIndex(strings.ToUpper(sq), " ORDER BY "); i >= 0 {
		orderBy = strings.TrimSpace(sq[i+len(" ORDER BY "):])
		sq = strings.TrimSpace(sq[:i])
	}
	m := reDetailStmt.FindStringSubmatch(sq) // 禁止 GROUP BY（有分组的走 group 计划）
	if m == nil {
		return nil
	}
	selList, table, where := strings.TrimSpace(m[1]), strings.Trim(m[2], "`\""), strings.TrimSpace(m[3])
	if strings.Contains(strings.ToUpper(selList), "(") || strings.Contains(strings.ToUpper(selList), "DISTINCT") {
		return nil // 聚合/函数调用/去重不走明细并行
	}
	for _, part := range splitTopLevelLocal(selList) {
		if !reDetailCol.MatchString(strings.TrimSpace(part)) {
			return nil
		}
	}
	// 分片键：rowid 或单列主键
	shardCol, lo, hi, ok := h.detailShardKey(table)
	if !ok {
		return nil
	}
	if orderBy != "" {
		om := reOrderOne.FindStringSubmatch(orderBy)
		if om == nil || !strings.EqualFold(om[1], shardCol) {
			return nil
		}
	}
	return &detailPlan{table: table, shardCol: shardCol, selCols: selList, where: where,
		limit: limit, lo: lo, hi: hi, estRows: int(hi - lo + 1), deg: deg}
}

// detailShardKey 选明细并行分片键（rowid 优先，其次单列主键）。
func (h *Handler) detailShardKey(table string) (string, int64, int64, bool) {
	if _, rows, err := h.session.Query(fmt.Sprintf("SELECT min(rowid), max(rowid) FROM %s", quoteIdentLocal(table))); err == nil &&
		len(rows) > 0 && rows[0][0] != nil && rows[0][1] != nil {
		return "rowid", cellInt(rows[0][0]), cellInt(rows[0][1]), true
	}
	_, ti, err := h.session.Query(fmt.Sprintf("PRAGMA main.table_info(%s)", quoteIdentLocal(table)))
	if err != nil {
		return "", 0, 0, false
	}
	pkCol, pkCount, pkMaxOrd := "", 0, 0
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
	if pkCount != 1 || pkCol == "" {
		return "", 0, 0, false
	}
	_, rows, err := h.session.Query(fmt.Sprintf("SELECT min(%s), max(%s) FROM %s",
		quoteIdentLocal(pkCol), quoteIdentLocal(pkCol), quoteIdentLocal(table)))
	if err != nil || len(rows) == 0 || rows[0][0] == nil || rows[0][1] == nil {
		return "", 0, 0, false
	}
	return pkCol, cellInt(rows[0][0]), cellInt(rows[0][1]), true
}

// runDetailParallel 方案 B：rowid 区间并行 scan，行包并行读、按分片顺序顺序发送。
func (h *Handler) runDetailParallel(p *detailPlan) (*mysql.Result, bool, error) {
	deg := p.deg
	cond := ""
	if p.where != "" {
		cond = " AND (" + p.where + ")"
	}
	step := (p.hi - p.lo + 1) / int64(deg)
	results := make([][][]any, deg)
	errs := make([]error, deg)
	var wg sync.WaitGroup
	started := time.Now()
	for i := 0; i < deg; i++ {
		a := p.lo + step*int64(i)
		b := p.lo + step*int64(i)+step - 1
		if i == deg-1 {
			b = p.hi
		}
		wg.Add(1)
		go func(i int, a, b int64) {
			defer wg.Done()
			s, err := h.backend.OpenSession("detail-shard", h.remote, h.session.CurrentDB())
			if err != nil {
				errs[i] = err
				return
			}
			defer s.Close()
			q := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= %d AND %s <= %d%s",
				p.selCols, quoteIdentLocal(p.table),
				quoteIdentLocal(p.shardCol), a, quoteIdentLocal(p.shardCol), b, cond)
			_, rows, err := s.Query(q)
			if err != nil {
				errs[i] = err
				return
			}
			results[i] = rows
		}(i, a, b)
	}
	wg.Wait()
	for _, e := range errs {
		if e != nil {
			return nil, true, MapSQLiteErr(e)
		}
	}
	// 顺序发送：按分片顺序拼接（分片键升序 = 发送顺序）
	var out [][]any
	for i := 0; i < deg; i++ {
		out = append(out, results[i]...)
		if p.limit > 0 && int64(len(out)) >= p.limit {
			out = out[:p.limit]
			break
		}
	}
	logx.Infof("detail parallel: table=%s shard=%s degree=%d rows=%d hint=%v cost=%dms",
		p.table, p.shardCol, deg, len(out), p.hint, time.Since(started).Milliseconds())
	// 列名：与串行一致（* 展开为真实列名）
	cols := make([]string, 0)
	if strings.TrimSpace(p.selCols) == "*" {
		_, ti, err := h.session.Query(fmt.Sprintf("PRAGMA main.table_info(%s)", quoteIdentLocal(p.table)))
		if err != nil {
			return nil, true, MapSQLiteErr(err)
		}
		for _, r := range ti {
			if len(r) >= 2 {
				cols = append(cols, cellStr(r[1]))
			}
		}
	} else {
		for _, part := range splitTopLevelLocal(p.selCols) {
			cols = append(cols, strings.TrimSpace(part))
		}
	}
	rs, err := meta.BuildResultset(cols, out)
	if err != nil {
		return nil, true, err
	}
	return mysql.NewResult(rs), true, nil
}

func atoiSafeLocal(s string) int64 {
	var n int64
	fmt.Sscanf(strings.TrimSpace(s), "%d", &n)
	return n
}
