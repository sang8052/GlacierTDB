// Package meta — MySQL 8 兼容 M4 批（v1.3.3）：
// 运算符（&& || ! XOR DIV RLIKE BINARY、十六进制字面量、字符集引导符）、
// 函数（GROUP_CONCAT SEPARATOR / CAST / CONVERT / EXTRACT / DATE_ADD 族 /
// POSITION / TRIM 方向 / SUBSTRING FROM-FOR / UNIX_TIMESTAMP(x) / ISNULL / TIMESTAMPADD）、
// INSERT ... SET、UPDATE/DELETE ... ORDER BY ... LIMIT。字符串扫描安全（引号内不动）。
package meta

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	reDblAmp   = regexp.MustCompile(`&&`)
	reDblPipe  = regexp.MustCompile(`\|\|`)
	reXor      = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_$])XOR([^A-Za-z0-9_$]|$)`)
	reRlike    = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_$])RLIKE([^A-Za-z0-9_$]|$)`)
	reHexLit   = regexp.MustCompile(`(^|[^A-Za-z0-9_$."'])0[xX]([0-9A-Fa-f]+)([^A-Za-z0-9_$]|$)`)
	reBinIdent = regexp.MustCompile(`(?i)\bBINARY\s+([A-Za-z_][A-Za-z0-9_]*)`)
	reDiv      = regexp.MustCompile(`(?i)(\b[A-Za-z_][A-Za-z0-9_.]*|\d+(?:\.\d+)?)\s+DIV\s+(\b[A-Za-z_][A-Za-z0-9_.]*|\d+(?:\.\d+)?)`)

	reGroupConcat = regexp.MustCompile(`(?i)\bGROUP_CONCAT\s*\(`)
	reCastCall    = regexp.MustCompile(`(?i)\bCAST\s*\(`)
	reConvertCall = regexp.MustCompile(`(?i)\bCONVERT\s*\(`)
	reExtractCall = regexp.MustCompile(`(?i)\bEXTRACT\s*\(`)
	reDateSubCall = regexp.MustCompile(`(?i)\b(DATE_SUB|SUBDATE)\s*\(`)
	reDateAddCall = regexp.MustCompile(`(?i)\b(DATE_ADD|ADDDATE)\s*\(`)
	reTsAddCall   = regexp.MustCompile(`(?i)\bTIMESTAMPADD\s*\(`)
	reTrimDir     = regexp.MustCompile(`(?i)\bTRIM\s*\(`)
	rePosition    = regexp.MustCompile(`(?i)\bPOSITION\s*\(`)
	reSubstrFrom  = regexp.MustCompile(`(?i)\b(?:SUBSTRING|SUBSTR|MID)\s*\(\s*([^,()]+?)\s+FROM\s+(\d+)(?:\s+FOR\s+(\d+))?\s*\)`)
	reIsnullCall  = regexp.MustCompile(`(?i)\bISNULL\s*\(\s*([^()]+?)\s*\)`)
	reUnixTsArg   = regexp.MustCompile(`(?i)\bUNIX_TIMESTAMP\s*\(\s*([^()]+?)\s*\)`)
	reSysdate     = regexp.MustCompile(`(?i)\bSYSDATE\s*\(\s*\)`)
	reMidCall     = regexp.MustCompile(`(?i)\bMID\s*\(`)
	reSeparator   = regexp.MustCompile(`(?i)^SEPARATOR\s+(.+)$`)
	reSeparatorInline = regexp.MustCompile(`(?i)^(.+?)\s+SEPARATOR\s+(.+)$`)
	reInSplit     = regexp.MustCompile(`(?i)^(.*?)\s+IN\s+(.*)$`)
	reAsSplit     = regexp.MustCompile(`(?i)^(.*?)\s+AS\s+([A-Za-z]+[A-Za-z0-9_ (),'\"]*)$`)
	reUsing       = regexp.MustCompile(`(?i)^USING\b`)
	reUsingSingle = regexp.MustCompile(`(?i)^([^()]+?)\s+USING\s+[A-Za-z0-9_]+\s*$`)

	// INTERVAL n UNIT 参数（DATE_ADD 族第二参）
	reIntervalArg = regexp.MustCompile(`(?i)^\s*INTERVAL\s+(.+?)\s+(MICROSECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|QUARTER|YEAR)\s*$`)
	rePlainUnit   = regexp.MustCompile(`(?i)^\s*(.+?)\s+(MICROSECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|QUARTER|YEAR)\s*$`)
	reTopFrom     = regexp.MustCompile(`(?i)^(.*?)\s+FROM\s+(.*)$`)

	reInsertSet = regexp.MustCompile(`(?is)^(\s*INSERT\s+(?:OR\s+[A-Z]+\s+)?INTO\s+)` + "(`[^`]+`|\"[^\"]+\"|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" + `(\s+SET\s+)(.+)$`)

	reUpdLimit = regexp.MustCompile(`(?is)^UPDATE\s+(` + "(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" + `)\s+SET\s+(.+?)\s*(?:WHERE\s+(.+?))?\s*ORDER\s+BY\s+(.+?)\s+LIMIT\s+(\d+)\s*$`)
	reDelLimit = regexp.MustCompile(`(?is)^DELETE\s+FROM\s+(` + "(`[^`]+`|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" + `)\s*(?:WHERE\s+(.+?))?\s*ORDER\s+BY\s+(.+?)\s+LIMIT\s+(\d+)\s*$`)
)

// RewriteOperators MySQL→SQLite 运算符级改写（引号/反引号段内不动）。
// 必须在 CONCAT→|| 展开（RewriteMySQLCompat）之前调用：MySQL 默认 || 是 OR。
func RewriteOperators(q string) string {
	// 跨引号边界的字符串前缀改写（BINARY 'x' / N'x' / _utf8'x'）先行
	q = rewriteStringPrefixes(q)
	return rewriteOutsideQuotes(q, func(seg string) string {
		seg = reDblAmp.ReplaceAllString(seg, " AND ")
		seg = reDblPipe.ReplaceAllString(seg, " OR ")
		seg = replaceBang(seg)
		seg = reXor.ReplaceAllString(seg, "${1}<>${2}")
		seg = reRlike.ReplaceAllString(seg, "${1}REGEXP${2}")
		seg = reBinIdent.ReplaceAllString(seg, "CAST($1 AS BLOB)")
		seg = reDiv.ReplaceAllString(seg, "CAST($1 / $2 AS INTEGER)")
		seg = reHexLit.ReplaceAllStringFunc(seg, func(m string) string {
			g := reHexLit.FindStringSubmatch(m)
			hex := g[2]
			if len(hex)%2 == 1 {
				hex = "0" + hex
			}
			return g[1] + "x'" + strings.ToUpper(hex) + "'" + g[3]
		})
		return seg
	})
}

var rePrefixQuote = regexp.MustCompile(`(?i)^(BINARY\s+|N|_?[A-Za-z0-9_]*utf8[A-Za-z0-9_]*)'`)

// rewriteStringPrefixes 跨引号边界的前缀改写：
//
//	BINARY 'x'    → CAST('x' AS BLOB)
//	N'x'          → 'x'
//	_utf8mb4'x'   → 'x'
func rewriteStringPrefixes(q string) string {
	var sb strings.Builder
	sb.Grow(len(q) + 16)
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
		if i == 0 || !isWordByte(q[i-1]) {
			if m := rePrefixQuote.FindString(q[i:]); m != "" {
				head := strings.TrimSpace(m[:len(m)-1])
				if strings.EqualFold(head, "BINARY") {
					start := i + len(m) - 1 // 开引号
					j := start + 1
					closed := false
					for j < n {
						if q[j] == '\'' {
							if j+1 < n && q[j+1] == '\'' {
								j += 2
								continue
							}
							closed = true
							j++
							break
						}
						j++
					}
					if closed {
						sb.WriteString("CAST(" + q[start:j] + " AS BLOB)")
						i = j
						continue
					}
					// 字面量未闭合：保守原样
				} else {
					sb.WriteString("'")
					i += len(m)
					continue
				}
			}
		}
		sb.WriteByte(c)
		i++
	}
	return sb.String()
}

func isWordByte(b byte) bool {
	return b == '_' || b == '$' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b >= 0x80
}

// rewriteOutsideQuotes 对引号（'..' ".." `..`）之外的区段应用 fn。
// 引号段内双写引号为转义；未闭合引号原样返回（交由后端报错）。
func rewriteOutsideQuotes(q string, fn func(string) string) string {
	var sb strings.Builder
	sb.Grow(len(q) + 32)
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

// RewriteMySQLCompat2 M4 函数级改写（在 RewriteMySQLCompat 之后、DDL 映射之前）。
func RewriteMySQLCompat2(q string) string {
	// GROUP_CONCAT(x [SEPARATOR s])：SEPARATOR 语法 → group_concat(x, s)
	//（DISTINCT 与 SEPARATOR 同现时 SQLite 单实参限制 → 退默认逗号；
	//  已是 group_concat(x[, 'sep']) 形态时原样返回，防定位器二次命中死循环）
	q = rewriteCall(q, reGroupConcat, func(args []string) string {
		if len(args) == 0 {
			return "GROUP_CONCAT()"
		}
		hasDistinct := regexp.MustCompile(`(?i)^\s*DISTINCT\b`).MatchString(args[0])
		if hasDistinct {
			return "group_concat(" + strings.Join(args, ", ") + ")"
		}
		last := strings.TrimSpace(args[len(args)-1])
		if m := reSeparator.FindStringSubmatch(last); m != nil {
			// 独立尾参形态：GROUP_CONCAT(a, b SEPARATOR s)
			head := args[:len(args)-1]
			if len(head) == 0 {
				return "group_concat(" + args[0] + ")"
			}
			return "group_concat(" + strings.Join(head, ", ") + ", " + m[1] + ")"
		}
		if len(args) == 1 {
			// 内联形态：GROUP_CONCAT(x SEPARATOR s)（无逗号，单参数段）
			if m := reSeparatorInline.FindStringSubmatch(last); m != nil {
				return "group_concat(" + strings.TrimSpace(m[1]) + ", " + m[2] + ")"
			}
			return "group_concat(" + args[0] + ")"
		}
		if strings.HasPrefix(last, "'") {
			return "group_concat(" + strings.Join(args, ", ") + ")" // 已是 (x, 'sep')
		}
		return "group_concat(" + strings.Join(args, ", ") + ", ',')"
	})
	// CAST(expr AS T)：MySQL 类型词根 → SQLite 目标类型
	q = rewriteCall(q, reCastCall, func(args []string) string {
		if len(args) != 1 {
			return "CAST(" + strings.Join(args, ", ") + ")"
		}
		m := reAsSplit.FindStringSubmatch(strings.TrimSpace(args[0]))
		if m == nil {
			return "CAST(" + args[0] + ")"
		}
		return "CAST(" + m[1] + " AS " + sqliteCastType(m[2]) + ")"
	})
	// CONVERT(expr, T) / CONVERT(expr USING charset)
	q = rewriteCall(q, reConvertCall, func(args []string) string {
		if len(args) == 1 {
			// CONVERT(x USING cs)：无逗号形态
			if m := reUsingSingle.FindStringSubmatch(strings.TrimSpace(args[0])); m != nil {
				return "(" + m[1] + ")"
			}
			return "CONVERT(" + args[0] + ")"
		}
		if len(args) != 2 {
			return "CONVERT(" + strings.Join(args, ", ") + ")"
		}
		expr := strings.TrimSpace(args[0])
		second := strings.TrimSpace(args[1])
		if reUsing.MatchString(second) {
			return "(" + expr + ")"
		}
		return "CAST(" + expr + " AS " + sqliteCastType(second) + ")"
	})
	// EXTRACT(unit FROM expr) → extract_unit(expr, 'unit')
	q = rewriteCall(q, reExtractCall, func(args []string) string {
		if len(args) != 1 {
			return "EXTRACT(" + strings.Join(args, ", ") + ")"
		}
		m := reTopFrom.FindStringSubmatch(strings.TrimSpace(args[0]))
		if m == nil {
			return "EXTRACT(" + args[0] + ")"
		}
		unit := strings.ToUpper(strings.TrimSpace(m[1]))
		if !extractUnits[unit] {
			return "EXTRACT(" + args[0] + ")"
		}
		return "extract_unit(" + m[2] + ", '" + unit + "')"
	})
	// DATE_SUB/SUBDATE(date, INTERVAL n unit) → mysql_date_add(date, -n, 'unit')
	q = rewriteCall(q, reDateSubCall, func(args []string) string {
		return buildDateAdd(args, true)
	})
	// DATE_ADD/ADDDATE → mysql_date_add(date, +n, 'unit')
	q = rewriteCall(q, reDateAddCall, func(args []string) string {
		return buildDateAdd(args, false)
	})
	// TIMESTAMPADD(unit, n, date)
	q = rewriteCall(q, reTsAddCall, func(args []string) string {
		if len(args) != 3 {
			return "TIMESTAMPADD(" + strings.Join(args, ", ") + ")"
		}
		unit := strings.ToUpper(strings.Trim(strings.TrimSpace(args[0]), "'\""))
		if !extractUnits[unit] {
			return "TIMESTAMPADD(" + strings.Join(args, ", ") + ")"
		}
		return "mysql_date_add(" + args[2] + ", " + strings.TrimSpace(args[1]) + ", '" + unit + "')"
	})
	// TRIM([BOTH|LEADING|TRAILING] [rem] FROM str) → mysql_trim(kind, rem, str)
	q = rewriteCall(q, reTrimDir, func(args []string) string {
		if len(args) != 1 || !strings.Contains(strings.ToUpper(args[0]), " FROM ") {
			return "TRIM(" + strings.Join(args, ", ") + ")"
		}
		m := reTopFrom.FindStringSubmatch(strings.TrimSpace(args[0]))
		if m == nil {
			return "TRIM(" + strings.Join(args, ", ") + ")"
		}
		head := strings.TrimSpace(m[1])
		kind, rem := "both", "' '"
		tk := strings.Fields(head)
		i := 0
		if i < len(tk) {
			switch strings.ToUpper(tk[i]) {
			case "BOTH", "LEADING", "TRAILING":
				kind = strings.ToLower(tk[i])
				i++
			}
		}
		if i < len(tk) {
			rem = strings.Join(tk[i:], " ")
		}
		return "mysql_trim('" + kind + "', " + rem + ", " + m[2] + ")"
	})
	// POSITION(needle IN hay) → instr(hay, needle)
	q = rewriteCall(q, rePosition, func(args []string) string {
		if len(args) != 1 {
			return "POSITION(" + strings.Join(args, ", ") + ")"
		}
		m := reInSplit.FindStringSubmatch(strings.TrimSpace(args[0]))
		if m == nil {
			return "POSITION(" + strings.Join(args, ", ") + ")"
		}
		return "instr(" + m[2] + ", " + m[1] + ")"
	})
	// SUBSTRING(x FROM a [FOR b]) → substr(x, a[, b])
	if reSubstrFrom.MatchString(q) {
		q = reSubstrFrom.ReplaceAllStringFunc(q, func(m string) string {
			g := reSubstrFrom.FindStringSubmatch(m)
			if g[3] != "" {
				return "substr(" + g[1] + ", " + g[2] + ", " + g[3] + ")"
			}
			return "substr(" + g[1] + ", " + g[2] + ")"
		})
	}
	// ISNULL(x) → (x IS NULL)
	if reIsnullCall.MatchString(q) {
		q = reIsnullCall.ReplaceAllString(q, "($1 IS NULL)")
	}
	// UNIX_TIMESTAMP(expr) → CAST(strftime('%s', expr) AS INTEGER)（strftime 回文本）
	if reUnixTsArg.MatchString(q) {
		q = reUnixTsArg.ReplaceAllString(q, "CAST(strftime('%s', $1) AS INTEGER)")
	}
	// SYSDATE() → datetime('now')
	if reSysdate.MatchString(q) {
		q = reSysdate.ReplaceAllString(q, "datetime('now')")
	}
	// MID( → substr(
	if reMidCall.MatchString(q) {
		q = reMidCall.ReplaceAllString(q, "substr(")
	}
	// INSERT INTO t SET a=1, b=expr → INSERT INTO t(a,b) VALUES(1, expr)
	if m := reInsertSet.FindStringSubmatch(q); m != nil {
		var cols, vals []string
		bad := false
		for _, pair := range splitTopLevel(m[4]) {
			pair = strings.TrimSpace(pair)
			if pair == "" {
				bad = true
				break
			}
			eq := topLevelIndex(pair, '=')
			if eq < 0 {
				bad = true
				break
			}
			cols = append(cols, strings.TrimSpace(pair[:eq]))
			vals = append(vals, strings.TrimSpace(pair[eq+1:]))
		}
		if !bad && len(cols) > 0 {
			q = m[1] + m[2] + "(" + strings.Join(cols, ", ") + ") VALUES (" + strings.Join(vals, ", ") + ")"
		}
	}
	// UPDATE/DELETE ... ORDER BY ... LIMIT n → rowid 子查询形态
	q = rewriteUpdateDeleteLimit(q)
	// CREATE TABLE t LIKE src → 空拷贝建表
	q = rewriteCreateLike(q)
	return q
}

var extractUnits = map[string]bool{
	"MICROSECOND": true, "SECOND": true, "MINUTE": true, "HOUR": true,
	"DAY": true, "WEEK": true, "MONTH": true, "QUARTER": true, "YEAR": true,
}

// buildDateAdd DATE_ADD/ADDDATE/SUBDATE/DATE_SUB 参数组装：
// (date, INTERVAL n unit | n) → mysql_date_add(date, ±n, 'unit')；解析不了保守原样。
func buildDateAdd(args []string, neg bool) string {
	orig := "mysql_date_add(" + strings.Join(args, ", ") + ")"
	if len(args) != 2 {
		return orig
	}
	base := strings.TrimSpace(args[0])
	numTxt := strings.TrimSpace(args[1])
	unit := "DAY"
	if m := reIntervalArg.FindStringSubmatch(numTxt); m != nil {
		numTxt, unit = m[1], strings.ToUpper(m[2])
	} else if m := rePlainUnit.FindStringSubmatch(numTxt); m != nil {
		numTxt, unit = m[1], strings.ToUpper(m[2])
	} else if !extractUnits[unit] {
		// 纯数字（ADDDATE(date, n) = n 天）
		unit = "DAY"
	}
	n, err := strconv.ParseInt(strings.TrimSpace(numTxt), 10, 64)
	if err != nil {
		// 表达式 n：交给 Go 函数在运行期求值（sqlite 支持函数参数为表达式文本？否——
		// 保守：数字解析失败时传原表达式，mysql_date_add 容错解析）
		signTxt := numTxt
		if neg {
			signTxt = "-(" + numTxt + ")"
		}
		return "mysql_date_add(" + base + ", " + signTxt + ", '" + unit + "')"
	}
	if neg {
		n = -n
	}
	return "mysql_date_add(" + base + ", " + strconv.FormatInt(n, 10) + ", '" + unit + "')"
}

// sqliteCastType MySQL CAST 目标类型 → SQLite 等价。
func sqliteCastType(t string) string {
	w := strings.ToUpper(strings.TrimSpace(t))
	w = regexp.MustCompile(`\s+INTEGER$|\s+INT$`).ReplaceAllString(w, "")
	w = strings.TrimSpace(w)
	if i := strings.IndexByte(w, '('); i >= 0 {
		w = w[:i]
	}
	w = strings.TrimSpace(w)
	switch w {
	case "SIGNED", "UNSIGNED":
		return "INTEGER"
	case "CHAR", "NCHAR", "VARCHAR", "BINARY":
		return "TEXT"
	case "DECIMAL", "NUMERIC", "DEC", "FIXED":
		return "NUMERIC"
	case "DATE", "DATETIME", "TIME", "TIMESTAMP", "YEAR":
		return "TEXT"
	default:
		return w // REAL/DOUBLE/FLOAT/BLOB 原样（SQLite 认）
	}
}

// topLevelIndex 在顶层（括号/引号内不计）查找字节下标。
func topLevelIndex(s string, target byte) int {
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
		case target:
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// rewriteUpdateDeleteLimit MySQL UPDATE/DELETE ... ORDER BY ... LIMIT n 改写为
// rowid IN (SELECT rowid ... ORDER BY ... LIMIT n)（SQLite 不支持 UPDATE/DELETE LIMIT）。
// 仅单表无别名形态改写，其余保守原样。
func rewriteUpdateDeleteLimit(q string) string {
	if m := reUpdLimit.FindStringSubmatch(q); m != nil {
		sub := "SELECT rowid FROM " + m[1]
		if m[4] != "" {
			sub += " WHERE " + m[4]
		}
		sub += " ORDER BY " + m[5] + " LIMIT " + m[6]
		return "UPDATE " + m[1] + " SET " + m[3] + " WHERE rowid IN (" + sub + ")"
	}
	if m := reDelLimit.FindStringSubmatch(q); m != nil {
		sub := "SELECT rowid FROM " + m[1]
		if m[3] != "" {
			sub += " WHERE " + m[3]
		}
		sub += " ORDER BY " + m[4] + " LIMIT " + m[5]
		return "DELETE FROM " + m[1] + " WHERE rowid IN (" + sub + ")"
	}
	return q
}

var reCreateLike = regexp.MustCompile(`(?is)^(CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)` +
	"(`[^`]+`|\"[^\"]+\"|[A-Za-z0-9_\\x{4e00}-\\x{9fa5}]+)" + `\s+LIKE\s+(` + "`[^`]+`" + `|[A-Za-z0-9_\x{4e00}-\x{9fa5}]+)\s*$`)

// rewriteCreateLike CREATE TABLE t LIKE src → 空拷贝建表（SQLite 无 LIKE 语法；
// 结构近似，列类型映射仍按原文由 colmeta 登记回放）。
func rewriteCreateLike(q string) string {
	if m := reCreateLike.FindStringSubmatch(q); m != nil {
		return m[1] + m[2] + " AS SELECT * FROM " + m[3] + " WHERE 0"
	}
	return q
}

// replaceBang 「!」（非「!=」）→ NOT（RE2 无负向断言，手动扫描）。
func replaceBang(s string) string {
	var sb strings.Builder
	sb.Grow(len(s) + 8)
	for i := 0; i < len(s); i++ {
		if s[i] == '!' && (i+1 >= len(s) || s[i+1] != '=') {
			sb.WriteString(" NOT ")
			continue
		}
		sb.WriteByte(s[i])
	}
	return sb.String()
}
