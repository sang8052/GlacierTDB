// Package meta — MySQL 兼容增强 M1 高频批：
// LIMIT offset,count / CONCAT / IF / RAND / UUID / DDL 类型映射 / DEFAULT CURRENT_TIMESTAMP。
// 改写保守：匹配不上原样透传，宁可报 SQLite 错也不改错语义。
package meta

import (
	"regexp"
	"strings"
)

var (
	reMySQLLimit  = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)\s*,\s*(\d+)\s*$`)
	reRand       = regexp.MustCompile(`(?i)\bRAND\s*\(\s*\)`)
	reUUID       = regexp.MustCompile(`(?i)\bUUID\s*\(\s*\)`)
	reCallConcat = regexp.MustCompile(`(?i)\bCONCAT\s*\(`)
	reCallIF     = regexp.MustCompile(`(?i)\bIF\s*\(`)
	reInsertIgn  = regexp.MustCompile(`(?i)^(\s*INSERT\s+)IGNORE(\s+INTO\b)`)
	// TIMESTAMPDIFF(MONTH, a, b)：MySQL 单位是裸关键字，SQLite 会解析为列名 → 改写为字符串字面量
	reTsDiffUnit = regexp.MustCompile(`(?i)\bTIMESTAMPDIFF\s*\(\s*(MICROSECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|QUARTER|YEAR)\s*,`)
	reIsDDL      = regexp.MustCompile(`(?i)^\s*(CREATE\s+(?:TEMP\s+|UNIQUE\s+)?(?:TABLE|INDEX|VIEW)|ALTER\s+TABLE)\b`)

	// DDL 词法映射（MySQL → SQLite）；带括号形式在前、裸词在后（词边界防 INTEGER 误伤）
	ddlTypeRules = []struct {
		re   *regexp.Regexp
		repl string
	}{
		{regexp.MustCompile(`(?i)\b(TINYINT|MEDIUMINT|SMALLINT|BIGINT|BOOLEAN|BOOL|INT)\s*\(\d+\)`), "INTEGER"},
		{regexp.MustCompile(`(?i)\b(TINYINT|MEDIUMINT|SMALLINT|BIGINT|BOOLEAN|BOOL|INT)\b`), "INTEGER"},
		{regexp.MustCompile(`(?i)\b(VARCHAR|CHAR|TINYTEXT|MEDIUMTEXT|LONGTEXT)\s*\(\d+\)`), "TEXT"},
		{regexp.MustCompile(`(?i)\b(VARCHAR|CHAR|TINYTEXT|MEDIUMTEXT|LONGTEXT)\b`), "TEXT"},
		{regexp.MustCompile(`(?i)\b(DATETIME|TIMESTAMP|DATE|TIME|YEAR)\s*\(\d+\)`), "TEXT"},
		{regexp.MustCompile(`(?i)\b(DATETIME|TIMESTAMP|DATE|TIME|YEAR)\b`), "TEXT"},
		{regexp.MustCompile(`(?i)\bDOUBLE(\s+PRECISION)?\s*\(\d+,\d+\)`), "REAL"},
		{regexp.MustCompile(`(?i)\bDOUBLE(\s+PRECISION)?\b`), "REAL"},
		{regexp.MustCompile(`(?i)\bFLOAT\s*\(\d+(,\d+)?\)`), "REAL"},
		{regexp.MustCompile(`(?i)\bFLOAT\b`), "REAL"},
		{regexp.MustCompile(`(?i)\b(DECIMAL|NUMERIC)\s*\(\d+,\d+\)`), "TEXT"},
		{regexp.MustCompile(`(?i)\b(DECIMAL|NUMERIC)\b`), "TEXT"},
		{regexp.MustCompile(`(?i)\b(VARBINARY|BINARY)\s*\(\d+\)`), "BLOB"},
		{regexp.MustCompile(`(?i)\b(VARBINARY|BINARY)\b`), "BLOB"},
		{regexp.MustCompile(`(?i)\s*\bUNSIGNED\b`), ""},
		{regexp.MustCompile(`(?i)\s*\bZEROFILL\b`), ""},
		{regexp.MustCompile(`(?i)\s*\bAUTO_INCREMENT\b`), ""}, // INTEGER PRIMARY KEY 天然自增
		{regexp.MustCompile(`(?i)\s+ON\s+UPDATE\s+CURRENT_TIMESTAMP(\s*\(\d*\))?`), ""},
		{regexp.MustCompile(`(?i)\s+COMMENT\s+'(?:[^']|'')*'`), ""}, // 列/索引注释
		// MySQL 建表内联 KEY/INDEX 段（SQLite 用独立 CREATE INDEX）
		{regexp.MustCompile(`(?i),\s*(?:UNIQUE\s+)?(?:FULLTEXT\s+|SPATIAL\s+)?(?:KEY|INDEX)\s+` + "`?" + `\w+` + "`?" + `\s*\([^)]*\)`), ""},
		// 表尾属性：) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ...
		{regexp.MustCompile(`(?is)\)\s*ENGINE\s*=\s*\w+.*$`), ")"},
	}
	reDefaultNow = regexp.MustCompile(`(?i)\bDEFAULT\s+CURRENT_TIMESTAMP(\s*\(\d*\))?`)
)

// uuidExpr 生成 UUID v4 样式字符串的 SQLite 表达式。
const uuidExpr = `lower(hex(randomblob(4)))||'-'||lower(hex(randomblob(2)))||'-4'||substr(lower(hex(randomblob(2))),2)||'-'||substr('89ab',abs(random())%4+1,1)||substr(lower(hex(randomblob(2))),2)||'-'||lower(hex(randomblob(6)))`

// RewriteMySQLCompat M1 兼容改写（在时序/通用改写之后、DDL 映射之前调用 DDL 部分独立）。
func RewriteMySQLCompat(q string) string {
	// LIMIT offset, count → LIMIT count OFFSET offset（仅语句尾部，一次）
	if reMySQLLimit.MatchString(q) {
		q = reMySQLLimit.ReplaceAllString(q, "LIMIT $2 OFFSET $1")
	}
	// INSERT IGNORE INTO → INSERT OR IGNORE INTO（M2）
	if reInsertIgn.MatchString(q) {
		q = reInsertIgn.ReplaceAllString(q, "${1}OR IGNORE${2}")
	}
	// TIMESTAMPDIFF 单位关键字字符串化（M2；Go 函数实现见 internal/sqlite/funcs.go）
	if reTsDiffUnit.MatchString(q) {
		q = reTsDiffUnit.ReplaceAllString(q, "TIMESTAMPDIFF('$1',")
	}
	if reRand.MatchString(q) {
		q = reRand.ReplaceAllString(q, "(abs(random())/9223372036854775808.0)")
	}
	if reUUID.MatchString(q) {
		q = reUUID.ReplaceAllString(q, "("+uuidExpr+")")
	}
	q = rewriteCall(q, reCallConcat, func(args []string) string {
		return "(" + strings.Join(args, " || ") + ")"
	})
	q = rewriteCall(q, reCallIF, func(args []string) string {
		if len(args) != 3 {
			return "IF(" + strings.Join(args, ", ") + ")" // 参数数不符：保守原样
		}
		return "CASE WHEN " + args[0] + " THEN " + args[1] + " ELSE " + args[2] + " END"
	})
	return q
}

// RewriteDDLTypes DDL 语句的 MySQL→SQLite 词法映射（非 DDL 原样返回）。
func RewriteDDLTypes(q string) string {
	if !reIsDDL.MatchString(q) {
		return q
	}
	out := q
	for _, r := range ddlTypeRules {
		out = r.re.ReplaceAllString(out, r.repl)
	}
	out = reDefaultNow.ReplaceAllString(out, "DEFAULT (datetime('now'))")
	return out
}

// rewriteCall 对函数调用做括号配对参数分割后重组（支持嵌套：外层先展开，内层随后续轮次处理）。
func rewriteCall(q string, locator *regexp.Regexp, build func(args []string) string) string {
	for {
		m := locator.FindStringIndex(q)
		if m == nil {
			return q
		}
		open := m[1] - 1 // '('
		depth := 0
		var quote byte
		end := -1
		for i := open; i < len(q); i++ {
			c := q[i]
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
					end = i
				}
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			return q // 括号不配对：保守原样
		}
		args := splitTopLevel(q[open+1 : end])
		trimmed := make([]string, len(args))
		for i, a := range args {
			trimmed[i] = strings.TrimSpace(a)
		}
		rebuilt := build(trimmed)
		if rebuilt == q[m[0]:end+1] {
			return q // 未变化防死循环
		}
		q = q[:m[0]] + rebuilt + q[end+1:]
	}
}

