// Package handler — 列类型映射接线（v1.3.3）+ MySQL 8 ALTER/RENAME/用户变量拦截。
//
// 类型映射（colmeta 系统库）：
//   CREATE/ALTER/DROP/RENAME  → 解析原始 MySQL DDL 登记映射；
//   INSERT/UPDATE             → 浮点/DECIMAL 列的裸数字字面量自动加引号
//                               （TEXT 落盘防 SQLite 15 位格式化截断）；
//   SELECT                    → 结果集按映射还原 float64 并回放 MySQL 类型码；
//   WHERE 比较 / ORDER BY     → 浮点列引用包 CAST(col AS REAL)（TEXT 存储下
//                               保证数值比较与排序语义）。
package handler

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/colmeta"
	"glaciertdb/internal/logx"
)

var (
	reCreateTbl   = regexp.MustCompile(`(?is)^CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`)
	reDropTbl     = regexp.MustCompile(`(?is)^DROP\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+EXISTS\s+)?`)
	reRenameTable = regexp.MustCompile(`(?is)^RENAME\s+TABLE\s+(.+)$`)
	reAlterTable  = regexp.MustCompile(`(?is)^ALTER\s+TABLE\s+`)
	reSetUsrVar   = regexp.MustCompile("(?is)^SET\\s+(?:SESSION\\s+|LOCAL\\s+)?@([A-Za-z0-9_$.]+|`[^`]+`)\\s*(:?=)\\s*(.+)$")

	reInsertInto  = regexp.MustCompile(`(?is)^(\s*INSERT\s+(?:OR\s+[A-Z]+\s+)?INTO\s+)`)
	reUpdateHead  = regexp.MustCompile(`(?is)^(\s*UPDATE\s+)`)
	reValuesKw    = regexp.MustCompile(`(?i)\bVALUES\s*`)
	reNumericLit  = regexp.MustCompile(`^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$`)
	reFromTbl     = regexp.MustCompile("(?i)\\b(?:FROM|JOIN)\\s+(`[^`]+`|\"[^\"]+\"|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)")
	reUsrVarTok   = regexp.MustCompile("(?i)@(`[^`]+`|[A-Za-z0-9_$.]+)")
	reAndBoundary = regexp.MustCompile(`(?i)\sWHERE\s`)
)

// ---------------------------------------------------------------------------
// 用户变量（@var）
// ---------------------------------------------------------------------------

// setUserVar SET @a = expr（字符串剥引号存原文；数值/表达式文本原样）。
func (h *Handler) setUserVar(name, value string) {
	name = strings.ToLower(strings.Trim(name, "`"))
	v := strings.TrimSpace(value)
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		v = strings.ReplaceAll(v[1:len(v)-1], "''", "'")
	}
	h.mu.Lock()
	if h.usrVars == nil {
		h.usrVars = map[string]string{}
	}
	h.usrVars[name] = v
	h.mu.Unlock()
}

// substituteUsrVars 语句中的 @var 替换为字面量（@@系统变量与引号段不动）。
func (h *Handler) substituteUsrVars(q string) string {
	h.mu.Lock()
	if len(h.usrVars) == 0 {
		h.mu.Unlock()
		return q
	}
	vars := make(map[string]string, len(h.usrVars))
	for k, v := range h.usrVars {
		vars[k] = v
	}
	h.mu.Unlock()

	var sb strings.Builder
	i, n := 0, len(q)
	for i < n {
		c := q[i]
		if c == '\'' || c == '"' || c == '`' {
			j := i + 1
			closed := false
			for j < n {
				if q[j] == c {
					if j+1 < n && q[j+1] == c {
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				j++
			}
			if !closed {
				sb.WriteString(q[i:])
				return sb.String()
			}
			sb.WriteString(q[i:j])
			i = j
			continue
		}
		j := i
		for j < n && q[j] != '\'' && q[j] != '"' && q[j] != '`' {
			j++
		}
		seg := q[i:j]
		if strings.Contains(seg, "@") {
			seg = replaceUsrVarTokens(seg, vars)
		}
		sb.WriteString(seg)
		i = j
	}
	return sb.String()
}

// replaceUsrVarTokens 普通区段内替换 @var（@ 后紧跟 @ 的系统变量引用跳过）。
func replaceUsrVarTokens(seg string, vars map[string]string) string {
	var sb strings.Builder
	last := 0
	for _, loc := range reUsrVarTok.FindAllStringIndex(seg, -1) {
		if loc[0] > 0 && seg[loc[0]-1] == '@' {
			continue // @@xxx
		}
		m := reUsrVarTok.FindStringSubmatch(seg[loc[0]:loc[1]])
		name := strings.ToLower(strings.Trim(m[1], "`"))
		v, ok := vars[name]
		if !ok {
			continue
		}
		sb.WriteString(seg[last:loc[0]])
		if reNumericLit.MatchString(v) {
			sb.WriteString(v)
		} else {
			sb.WriteString("'" + strings.ReplaceAll(v, "'", "''") + "'")
		}
		last = loc[1]
	}
	sb.WriteString(seg[last:])
	return sb.String()
}

// ---------------------------------------------------------------------------
// DDL 变更登记
// ---------------------------------------------------------------------------

// afterWriteSuccess 写语句成功后登记/维护列类型映射（原语句文本解析）。
func (h *Handler) afterWriteSuccess(orig string) {
	if h.colmeta == nil {
		return
	}
	db := h.session.CurrentDB()

	if reCreateTbl.MatchString(orig) {
		tbl, cols, likeSrc, ok := colmeta.ParseCreateTable(orig)
		if !ok {
			return
		}
		if likeSrc != "" {
			if err := h.colmeta.CopyTableLike(db, likeSrc, tbl); err != nil {
				logx.Warnf("colmeta copy %s.%s<- %s: %v", db, tbl, likeSrc, err)
			}
			return
		}
		if err := h.colmeta.RecordCreateTable(db, tbl, cols); err != nil {
			logx.Warnf("colmeta record %s.%s: %v", db, tbl, err)
		}
		return
	}
	if reDropTbl.MatchString(orig) {
		rest := orig[len(reDropTbl.FindString(orig)):]
		var tbls []string
		for _, part := range strings.Split(rest, ",") {
			t, _ := colmeta.Ident(strings.TrimSpace(part))
			if t != "" {
				tbls = append(tbls, t)
			}
		}
		if len(tbls) > 0 {
			_ = h.colmeta.RecordDropTable(db, tbls...)
		}
		return
	}
	if reAlterTable.MatchString(orig) {
		h.recordAlter(orig)
	}
}

// recordAlter ALTER 语句的映射维护（ADD/DROP COLUMN、RENAME、CHANGE）。
func (h *Handler) recordAlter(orig string) {
	db := h.session.CurrentDB()
	rest := orig[len(reAlterTable.FindString(orig)):]
	tbl, after := colmeta.Ident(rest)
	if tbl == "" {
		return
	}
	a := strings.TrimSpace(after)
	low := strings.ToLower(a)

	switch {
	case strings.HasPrefix(low, "add "):
		body := strings.TrimSpace(a[4:])
		lowBody := strings.ToLower(body)
		if strings.HasPrefix(lowBody, "column ") {
			body = strings.TrimSpace(body[7:])
		} else if isIndexLikeClause(lowBody) {
			return // ADD KEY/INDEX/CONSTRAINT/PRIMARY/FOREIGN：非列
		}
		if cd, ok := parseOneColumnDef(body); ok {
			_ = h.colmeta.RecordAddColumn(db, tbl, cd)
		}
	case strings.HasPrefix(low, "drop "):
		body := strings.TrimSpace(a[5:])
		lowBody := strings.ToLower(body)
		if strings.HasPrefix(lowBody, "column ") {
			body = strings.TrimSpace(body[7:])
		} else if !reIdentLike.MatchString(body) {
			return // DROP KEY/INDEX/PRIMARY/FOREIGN：非列
		}
		if col, _ := colmeta.Ident(body); col != "" {
			_ = h.colmeta.RecordDropColumn(db, tbl, col)
		}
	case strings.HasPrefix(low, "rename"):
		body := strings.TrimSpace(a[len("rename"):])
		lowBody := strings.ToLower(body)
		if strings.HasPrefix(lowBody, "to ") || strings.HasPrefix(lowBody, "as ") {
			nt, _ := colmeta.Ident(strings.TrimSpace(body[3:]))
			if nt != "" {
				_ = h.colmeta.RecordRenameTable(db, tbl, nt)
			}
			return
		}
		if m := reRenameCol.FindStringSubmatch(body); m != nil {
			_ = h.colmeta.RecordRenameColumn(db, tbl, m[1], m[2])
		}
	case strings.HasPrefix(low, "change "):
		body := strings.TrimSpace(a[7:])
		if strings.HasPrefix(strings.ToLower(body), "column ") {
			body = strings.TrimSpace(body[7:])
		}
		old, r1 := colmeta.Ident(body)
		if old == "" {
			return
		}
		if cd, ok := parseOneColumnDef(strings.TrimSpace(r1)); ok {
			_ = h.colmeta.RecordChangeColumn(db, tbl, old, cd)
		}
	}
}

// isIndexLikeClause ADD 后的非列子句判定。
func isIndexLikeClause(lowBody string) bool {
	for _, p := range []string{"primary key", "unique", "key ", "index ", "fulltext", "spatial", "constraint", "foreign", "check"} {
		if strings.HasPrefix(lowBody, p) {
			return true
		}
	}
	return false
}

var (
	reIdentLike = regexp.MustCompile("^[A-Za-z_\\x{4e00}-\\x{9fa5}][A-Za-z0-9_$\\x{4e00}-\\x{9fa5}]*$")
	reRenameCol = regexp.MustCompile("(?i)^(?:COLUMN\\s+)?(`[^`]+`|[A-Za-z0-9_$\\x{4e00}-\\x{9fa5}]+)\\s+TO\\s+(`[^`]+`|[A-Za-z0-9_$\\x{4e00}-\\x{9fa5}]+)")
)

// parseOneColumnDef 单列定义（名字 + 类型段）解析。
func parseOneColumnDef(s string) (colmeta.ColDef, bool) {
	name, rest := colmeta.Ident(s)
	if name == "" {
		return colmeta.ColDef{}, false
	}
	return buildColDefLocal(name, strings.TrimSpace(rest))
}

// joinTypeParts 类型词拼接（括号段吸附前词，同 colmeta）。
func joinTypeParts(parts []string) string {
	var sb strings.Builder
	for i, p := range parts {
		if i > 0 && strings.HasPrefix(p, "(") {
			sb.WriteString(p)
			continue
		}
		if i > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(p)
	}
	return sb.String()
}

var reColStopKw = regexp.MustCompile(`(?i)^(NOT|NULL|DEFAULT|AUTO_INCREMENT|UNIQUE|PRIMARY|KEY|COMMENT|COLLATE|CHARACTER|REFERENCES|ON|CHECK|GENERATED|VIRTUAL|STORED|ENFORCED|INVISIBLE|SRID|WITH)`)

// buildColDefLocal 名字后的类型段收集（词 + 括号整体），遇约束关键字停。
func buildColDefLocal(name, rest string) (colmeta.ColDef, bool) {
	if rest == "" {
		return colmeta.ColDef{}, false
	}
	var typParts []string
	rem := rest
	for {
		rem = strings.TrimLeft(rem, " \t\r\n")
		if rem == "" {
			break
		}
		k := 0
		for k < len(rem) {
			ch := rem[k]
			if ch == '_' || ch == '$' || ch == '.' ||
				(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch >= 0x80 {
				k++
				continue
			}
			break
		}
		if k == 0 {
			break
		}
		tok := rem[:k]
		if reColStopKw.MatchString(strings.ToUpper(tok)) {
			break
		}
		typParts = append(typParts, tok)
		rem = strings.TrimLeft(rem[k:], " \t\r\n")
		if strings.HasPrefix(rem, "(") {
			body, after, ok := scanParen(rem)
			if !ok {
				return colmeta.ColDef{}, false
			}
			typParts = append(typParts, "("+body+")")
			rem = after
		}
	}
	typ := joinTypeParts(typParts)
	if typ == "" {
		return colmeta.ColDef{}, false
	}
	cd := colmeta.ColDef{Name: name, Type: typ, Code: colmeta.ParseTypeCode(typ), Nullable: true}
	if regexp.MustCompile(`(?i)\bNOT\s+NULL\b|\bPRIMARY\s+KEY\b`).MatchString(rest) {
		cd.Nullable = false
	}
	return cd, true
}

// ---------------------------------------------------------------------------
// 读写路径的类型转换
// ---------------------------------------------------------------------------

// quoteTextColumnLiterals INSERT/UPDATE 中浮点/DECIMAL 列的裸数字字面量加引号。
func (h *Handler) quoteTextColumnLiterals(sq string) string {
	if h.colmeta == nil {
		return sq
	}
	db := h.session.CurrentDB()
	if m := reInsertInto.FindString(sq); m != "" {
		rest := sq[len(m):]
		tbl, after := colmeta.Ident(rest)
		if tbl == "" {
			return sq
		}
		cols := h.textColumns(db, tbl)
		if len(cols) == 0 {
			return sq
		}
		after = strings.TrimSpace(after)
		var order []string
		if strings.HasPrefix(after, "(") {
			body, afterBody, ok := scanParen(after)
			if !ok {
				return sq
			}
			for _, c := range splitTopLevelLocal(body) {
				if n, _ := colmeta.Ident(strings.TrimSpace(c)); n != "" {
					order = append(order, strings.ToLower(n))
				}
			}
			after = strings.TrimSpace(afterBody)
		} else {
			order = h.tableColumnOrder(db, tbl)
			if len(order) == 0 {
				return sq
			}
		}
		vloc := reValuesKw.FindStringIndex(after)
		if vloc == nil {
			return sq
		}
		rowsTxt := strings.TrimLeft(after[vloc[1]:], " \t")
		if rowsTxt == "" || rowsTxt[0] != '(' {
			return sq
		}
		newRows, ok := quoteRowsLiterals(rowsTxt, order, cols)
		if !ok {
			return sq
		}
		return sq[:len(sq)-len(rowsTxt)] + newRows
	}
	if m := reUpdateHead.FindString(sq); m != "" {
		rest := sq[len(m):]
		tbl, after := colmeta.Ident(rest)
		if tbl == "" {
			return sq
		}
		cols := h.textColumns(db, tbl)
		if len(cols) == 0 {
			return sq
		}
		prefix := sq[:len(sq)-len(after)] // m + tbl
		sloc := regexp.MustCompile(`(?i)\bSET\s+`).FindStringIndex(after)
		if sloc == nil {
			return sq
		}
		setPart := after[sloc[1]:]
		tail := ""
		if wloc := reAndBoundary.FindStringIndex(setPart); wloc != nil {
			tail = setPart[wloc[0]:]
			setPart = setPart[:wloc[0]]
		}
		var parts []string
		for _, p := range splitTopLevelLocal(setPart) {
			p = strings.TrimSpace(p)
			eq := topLevelEqIndex(p)
			if eq < 0 {
				parts = append(parts, p)
				continue
			}
			col := strings.ToLower(strings.Trim(strings.TrimSpace(p[:eq]), "`\""))
			val := strings.TrimSpace(p[eq+1:])
			if cols[col] != "" && reNumericLit.MatchString(val) {
				p = p[:eq+1] + " '" + val + "'"
			}
			parts = append(parts, p)
		}
		return prefix + after[:sloc[1]] + strings.Join(parts, ", ") + tail
	}
	return sq
}

// textColumns 表中 TEXT 落盘列（col name → mysql type）。
func (h *Handler) textColumns(db, tbl string) map[string]string {
	m := h.colmeta.TableMap(db, tbl)
	if m == nil {
		return nil
	}
	out := map[string]string{}
	for k, c := range m {
		if colmeta.StoreText(c.Type) {
			out[k] = c.Type
		}
	}
	return out
}

// tableColumnOrder 表列序（colmeta 序号）。
func (h *Handler) tableColumnOrder(db, tbl string) []string {
	m := h.colmeta.TableMap(db, tbl)
	if m == nil {
		return nil
	}
	max := 0
	for _, c := range m {
		if c.Ordinal > max {
			max = c.Ordinal
		}
	}
	out := make([]string, max)
	for _, c := range m {
		if c.Ordinal >= 1 && c.Ordinal <= max {
			out[c.Ordinal-1] = strings.ToLower(c.Name)
		}
	}
	return out
}

// quoteRowsLiterals VALUES (..)[,(..)] 行组逐组解析；组内对应列的裸数字加引号。
// 最后一组之后的尾巴（ON CONFLICT …）原样保留。
func quoteRowsLiterals(rowsTxt string, order []string, cols map[string]string) (string, bool) {
	var sb strings.Builder
	pos := 0
	for pos < len(rowsTxt) {
		// 跳过空白与逗号
		for pos < len(rowsTxt) && (rowsTxt[pos] == ' ' || rowsTxt[pos] == '\t' || rowsTxt[pos] == '\n' || rowsTxt[pos] == '\r' || rowsTxt[pos] == ',') {
			sb.WriteByte(rowsTxt[pos])
			pos++
		}
		if pos >= len(rowsTxt) || rowsTxt[pos] != '(' {
			break // 尾巴（或语法形态不符）：原样保留
		}
		body, after, ok := scanParen(rowsTxt[pos:])
		if !ok {
			return "", false
		}
		items := splitTopLevelLocal(body)
		changed := false
		for i := range items {
			t := strings.TrimSpace(items[i])
			if i < len(order) && cols[order[i]] != "" && reNumericLit.MatchString(t) {
				items[i] = " " + t + " "
				changed = true
			}
		}
		if changed {
			sb.WriteString("(" + strings.Join(items, ",") + ")")
		} else {
			sb.WriteString(rowsTxt[pos : len(rowsTxt)-len(after)])
		}
		pos = len(rowsTxt) - len(after)
	}
	if pos < len(rowsTxt) {
		sb.WriteString(rowsTxt[pos:])
	}
	return sb.String(), true
}

// scanParen 从 s[0]=='(' 起配对括号（引号内不计），返回（内部, 右括号后, 是否闭合）。
func scanParen(s string) (string, string, bool) {
	if s == "" || s[0] != '(' {
		return "", "", false
	}
	depth := 0
	var quote byte
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
			if depth == 0 {
				return s[1:i], s[i+1:], true
			}
		}
	}
	return "", "", false
}

// topLevelEqIndex 顶层 '=' 下标。
func topLevelEqIndex(s string) int {
	depth := 0
	var quote byte
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
		case '=':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// rewriteFloatPredicates 浮点列的 WHERE 比较与 ORDER BY 引用包 CAST(col AS REAL)。
func (h *Handler) rewriteFloatPredicates(sq string) string {
	if h.colmeta == nil {
		return sq
	}
	db := h.session.CurrentDB()
	floatCols := map[string]bool{}
	for _, t := range detectFromTables(sq) {
		for k, c := range h.colmeta.TableMap(db, t) {
			if colmeta.RestoreFloat(c.Type) {
				floatCols[k] = true
			}
		}
	}
	if len(floatCols) == 0 {
		return sq
	}
	return rewriteOutsideQuotesLocal(sq, func(seg string) string {
		for col := range floatCols {
			re := regexp.MustCompile("(?i)(^|[^A-Za-z0-9_$])(`?" + regexp.QuoteMeta(col) + "`?)\\s*(=|!=|<>|>=|<=|>|<)")
			seg = re.ReplaceAllString(seg, "${1}CAST(${2} AS REAL) ${3}")
			if m := reOrderBy.FindStringIndex(seg); m != nil {
				seg = seg[:m[0]] + wrapOrderByCols(seg[m[0]:], col)
			}
		}
		return seg
	})
}

var reOrderBy = regexp.MustCompile(`(?i)\bORDER\s+BY\b`)

// rewriteOutsideQuotesLocal 引号外区段改写（handler 包内工具，语义同 meta 版）。
func rewriteOutsideQuotesLocal(q string, fn func(string) string) string {
	var sb strings.Builder
	i, n := 0, len(q)
	for i < n {
		c := q[i]
		if c == '\'' || c == '"' || c == '`' {
			j := i + 1
			closed := false
			for j < n {
				if q[j] == c {
					if j+1 < n && q[j+1] == c {
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				j++
			}
			if !closed {
				sb.WriteString(q[i:])
				return sb.String()
			}
			sb.WriteString(q[i:j])
			i = j
			continue
		}
		j := i
		for j < n && q[j] != '\'' && q[j] != '"' && q[j] != '`' {
			j++
		}
		sb.WriteString(fn(q[i:j]))
		i = j
	}
	return sb.String()
}

// wrapOrderByCols ORDER BY 子句里该列的裸引用包 CAST。
func wrapOrderByCols(orderBy string, col string) string {
	reItem := regexp.MustCompile("(?i)(^|,\\s*|\\s+)(`?" + regexp.QuoteMeta(col) + "`?)(\\s+(?:ASC|DESC))?(\\s*,|\\s*$)")
	return reItem.ReplaceAllString(orderBy, "${1}CAST(${2} AS REAL)${3}${4}")
}

// detectFromTables 语句中的 FROM/JOIN 表（小写）。
func detectFromTables(sq string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range reFromTbl.FindAllStringSubmatch(sq, -1) {
		t := strings.ToLower(strings.Trim(m[1], "`\""))
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

// typeRestorePlan 计算结果集各列的类型还原方案：返回 列下标→MySQL 类型码，
// 并把 rows 中浮点列的 TEXT 值就地转为 float64（须在 BuildResultset 前调用）。
func (h *Handler) typeRestorePlan(sq string, cols []string, rows [][]any) map[int]byte {
	if h.colmeta == nil || len(rows) == 0 || len(cols) == 0 {
		return nil
	}
	db := h.session.CurrentDB()
	cmap := map[string]colmeta.ColDef{}
	for _, t := range detectFromTables(sq) {
		for k, c := range h.colmeta.TableMap(db, t) {
			if _, exists := cmap[k]; !exists {
				cmap[k] = c
			}
		}
	}
	if len(cmap) == 0 {
		return nil
	}
	plan := map[int]byte{}
	for i, name := range cols {
		cd, ok := cmap[strings.ToLower(name)]
		if !ok {
			continue
		}
		if colmeta.RestoreFloat(cd.Type) {
			plan[i] = cd.Code
			for _, row := range rows {
				if i >= len(row) {
					break
				}
				switch v := row[i].(type) {
				case string:
					if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
						row[i] = f
					}
				case []byte:
					if f, err := strconv.ParseFloat(strings.TrimSpace(string(v)), 64); err == nil {
						row[i] = f
					}
				}
			}
		} else if cd.Code == colmeta.CodeNewDecimal {
			plan[i] = colmeta.CodeNewDecimal
		}
	}
	return plan
}

// LookupColumnTypeHook meta.ColumnTypeLookuper 回放钩子（proxy 注入）。
func LookupColumnTypeHook(cm *colmeta.Store) func(db, tbl, col, sqliteType string) string {
	return lookupColumnType(cm)
}

// lookupColumnType meta.ColumnTypeLookuper 回放钩子实现。
func lookupColumnType(cm *colmeta.Store) func(db, tbl, col, sqliteType string) string {
	return func(db, tbl, col, sqliteType string) string {
		if cm == nil {
			return ""
		}
		m := cm.TableMap(db, tbl)
		if m == nil {
			return ""
		}
		if c, ok := m[strings.ToLower(col)]; ok {
			return c.Type
		}
		return ""
	}
}

// ---------------------------------------------------------------------------
// RENAME TABLE / ALTER TABLE 兼容拦截
// ---------------------------------------------------------------------------

// handleRenameTable RENAME TABLE a TO b [, c TO d]（多对顺序执行）。
func (h *Handler) handleRenameTable(q string) (*mysql.Result, bool, error) {
	m := reRenameTable.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	if h.readonly {
		return nil, true, roErr()
	}
	db := h.session.CurrentDB()
	for _, pair := range splitTopLevelLocal(m[1]) {
		to := regexp.MustCompile(`(?i)^(.+?)\s+TO\s+(.+)$`).FindStringSubmatch(strings.TrimSpace(pair))
		if to == nil {
			return nil, true, mysql.NewError(1064, "syntax error near "+pair)
		}
		old, _ := colmeta.Ident(strings.TrimSpace(to[1]))
		new, _ := colmeta.Ident(strings.TrimSpace(to[2]))
		if old == "" || new == "" {
			return nil, true, mysql.NewError(1064, "syntax error near "+pair)
		}
		res, err := h.execWriteOne("ALTER TABLE " + quoteIdent(old) + " RENAME TO " + quoteIdent(new))
		if err != nil {
			return res, true, err
		}
		if h.colmeta != nil {
			_ = h.colmeta.RecordRenameTable(db, old, new)
		}
	}
	return metaOK(), true, nil
}

func metaOK() *mysql.Result { return &mysql.Result{} }

var alterNopPatterns = []string{
	`^auto_increment\s*=`, `^enable\s+keys`, `^disable\s+keys`,
	`^convert\s+to\s+(character\s+set|charset)`, `^collate`, `^default\s+charset`, `^charset`,
	`^drop\s+primary\s+key`, `^drop\s+foreign\s+key`, `^add\s+primary\s+key`,
	`^alter\s+(column\s+)?\S+\s+(set|drop)\s+default`, `^force`, `^validate`, `^discard\s+tablespace`,
}

// handleAlterTableIntercept ALTER TABLE 兼容拦截。
// 返回 handled=true 且 stmt==""：no-op 应答 OK；handled=true 且 stmt!="": 执行 stmt；
// handled=false：走原路径（含登记钩子）。
func (h *Handler) handleAlterTableIntercept(q string) (handled bool, stmt string, err error) {
	if !reAlterTable.MatchString(q) {
		return false, "", nil
	}
	rest := q[len(reAlterTable.FindString(q)):]
	tbl, after := colmeta.Ident(rest)
	if tbl == "" {
		return false, "", nil
	}
	a := strings.TrimSpace(after)
	low := strings.ToLower(a)

	for _, nop := range alterNopPatterns {
		if regexp.MustCompile(nop).MatchString(low) {
			return true, "", nil
		}
	}
	if strings.HasPrefix(low, "add ") {
		body := strings.TrimSpace(a[4:])
		lowBody := strings.ToLower(body)
		if strings.HasPrefix(lowBody, "column ") {
			return false, "", nil // 原路径（RewriteDDLTypes 处理类型）
		}
		if isIndexLikeClause(lowBody) {
			if m := reAlterAddIndex.FindStringSubmatch(body); m != nil {
				unique := ""
				if strings.EqualFold(strings.TrimSpace(m[1]), "UNIQUE") {
					unique = "UNIQUE "
				}
				name := m[2]
				if name == "" {
					name = tbl + "_ix_" + sanitizeIndexSuffix(m[3])
				}
				return true, "CREATE " + unique + "INDEX IF NOT EXISTS " + quoteIdent(name) +
					" ON " + quoteIdent(tbl) + m[3], nil
			}
			return true, "", nil // FULLTEXT/SPATIAL/CONSTRAINT 索引近似 no-op
		}
		return false, "", nil
	}
	if strings.HasPrefix(low, "drop ") {
		body := strings.TrimSpace(a[5:])
		lowBody := strings.ToLower(body)
		if strings.HasPrefix(lowBody, "column ") {
			return false, "", nil
		}
		if m := reAlterDropIndex.FindStringSubmatch(body); m != nil {
			return true, "DROP INDEX IF EXISTS " + quoteIdent(strings.Trim(m[1], "`")), nil
		}
		return true, "", nil // DROP PRIMARY/FOREIGN KEY no-op（上面已覆盖）
	}
	if m := reAlterModify.FindStringSubmatch(a); m != nil {
		if h.colmeta != nil {
			if cd, ok := parseOneColumnDef(m[1] + " " + m[2]); ok {
				_ = h.colmeta.RecordChangeColumn(h.session.CurrentDB(), tbl, m[1], cd)
			}
		}
		return true, "", nil // SQLite 动态类型：类型变更近似 no-op
	}
	if m := reAlterChange.FindStringSubmatch(a); m != nil {
		if m[1] != m[2] {
			return true, "ALTER TABLE " + quoteIdent(tbl) + " RENAME COLUMN " + quoteIdent(m[1]) +
				" TO " + quoteIdent(m[2]), nil
		}
		if h.colmeta != nil {
			if cd, ok := parseOneColumnDef(m[2] + " " + m[3]); ok {
				_ = h.colmeta.RecordChangeColumn(h.session.CurrentDB(), tbl, m[1], cd)
			}
		}
		return true, "", nil
	}
	return false, "", nil // ADD COLUMN/DROP COLUMN/RENAME/ADD CONSTRAINT 原路径
}

func sanitizeIndexSuffix(cols string) string {
	r := strings.NewReplacer("(", "_", ")", "", " ", "_", ",", "_", "`", "", "\"", "")
	return strings.ToLower(r.Replace(cols))
}

var (
	reAlterAddIndex  = regexp.MustCompile(`(?is)^((?:UNIQUE\s+)?)(?:(?:FULLTEXT|SPATIAL)\s+)?(?:KEY|INDEX)\s+(?:` + "(`[^`]+`|[A-Za-z0-9_$\\x{4e00}-\\x{9fa5}]+)" + `)?\s*(\([^)]*\))(?:\s+USING\s+BTREE)?`)
	reAlterDropIndex = regexp.MustCompile(`(?is)^(?:KEY|INDEX)\s+(` + "`[^`]+`" + `|[A-Za-z0-9_$\x{4e00}-\x{9fa5}]+)`)
	reAlterModify    = regexp.MustCompile(`(?is)^MODIFY\s+(?:COLUMN\s+)?(` + "`[^`]+`" + `|[A-Za-z0-9_$\x{4e00}-\x{9fa5}]+)` + `\s+(.+)$`)
	reAlterChange    = regexp.MustCompile(`(?is)^CHANGE\s+(?:COLUMN\s+)?(` + "`[^`]+`" + `|[A-Za-z0-9_$\x{4e00}-\x{9fa5}]+)` + `\s+(` + "`[^`]+`" + `|[A-Za-z0-9_$\x{4e00}-\x{9fa5}]+)` + `\s+(.+)$`)
)
