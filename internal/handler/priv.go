// Package handler — 语句权限校验（v1.0.10）。
// 普通用户（account_type=user）按系统库 user_grants 授权：
// 从语句解析目标表（FROM/INTO/JOIN/UPDATE/TABLE …），逐表比对所需权限；
// 不触表语句（SELECT 1 等）放行；无法分类的语句按 ALL 权限保守拒绝。
package handler

import (
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/auth"
)

// 名字引用：[db.]table，标识符可为反引号/双引号/方括号包裹或裸名。
const (
	privIdent  = "(?:`[^`]+`|\"[^\"]+\"|\\[[^\\]]+\\]|[A-Za-z0-9_$-]+)"
	privRef    = privIdent + `(?:\s*\.\s*` + privIdent + `)?`
	privRefLst = `(` + privRef + `(?:\s*,\s*` + privRef + `)*)`
)

var (
	rePrivFrom  = regexp.MustCompile(`(?i)\bFROM\s+(?:ONLY\s+)?` + privRefLst)
	rePrivJoin  = regexp.MustCompile(`(?i)\bJOIN\s+(` + privRef + `)`)
	rePrivInto  = regexp.MustCompile(`(?i)\bINTO\s+(` + privRef + `)`)
	rePrivTable = regexp.MustCompile(`(?i)\bTABLE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?(` + privRef + `)`)
	rePrivUpd   = regexp.MustCompile(`(?i)^UPDATE\s+(?:ONLY\s+)?(` + privRef + `)`)
	rePrivIdxOn = regexp.MustCompile(`(?i)\bON\s+(` + privRef + `)`)
	rePrivName  = regexp.MustCompile(privIdent)

	reKindSelect = regexp.MustCompile(`(?i)^(SELECT|WITH|EXPLAIN|VALUES|TABLE)\b`)
	reKindInsert = regexp.MustCompile(`(?i)^(INSERT|REPLACE)\b`)
	reKindUpdate = regexp.MustCompile(`(?i)^UPDATE\b`)
	reKindDelete = regexp.MustCompile(`(?i)^(DELETE|TRUNCATE)\b`)
	reKindCreate = regexp.MustCompile(`(?i)^CREATE\s+(?:TEMP\s+|UNIQUE\s+)?(TABLE|VIEW|INDEX)\b`)
	reKindDrop   = regexp.MustCompile(`(?i)^DROP\s+(TABLE|VIEW|INDEX|DATABASE|SCHEMA)\b`)
	reKindAlter  = regexp.MustCompile(`(?i)^(ALTER|RENAME)\b`)
	reKindIdxDDL = regexp.MustCompile(`(?i)^(CREATE\s+(?:UNIQUE\s+)?INDEX|DROP\s+INDEX)\b`)
)

// checkPrivilege 校验语句权限（仅普通用户；super 恒放行、readonly 由写拦截层管）。
func (h *Handler) checkPrivilege(q string) error {
	if h.auths == nil || h.userType != auth.TypeUser {
		return nil
	}
	priv, kindKnown := stmtPriv(q)
	if !kindKnown {
		return mysql.NewError(1142, "command denied to user '"+h.user+"' for this query (unrecognized statement, requires ALL)")
	}
	targets := stmtTargets(q, h.session.CurrentDB())
	if len(targets) == 0 {
		return nil // 不触表（SELECT 1 / 时间函数等）
	}
	for _, t := range targets {
		if !h.auths.CheckPrivilege(h.user, t.db, t.table, priv) {
			return mysql.NewError(1142, "command denied to user '"+h.user+"' for table '"+t.table+"' (need "+priv+")")
		}
	}
	return nil
}

// withMainVerbs WITH 前缀语句的主动词集合（CTE 之后的核心语句）。
var withMainVerbs = map[string]bool{
	"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	"REPLACE": true, "VALUES": true, "TABLE": true,
}

// StripWithPrefix 归一化 WITH 前缀语句（v1.3.11 安全修复：此前 WITH ... DELETE/INSERT
// 被读路径正则 ^SELECT|WITH 误判为读，SELECT 权限即可写、只读账号可写）。
// 返回主动词开头的形式，仅用于语句分类；非 WITH 语句原样返回。
// "WITH x AS (…) DELETE FROM t" → "DELETE FROM t"。
func StripWithPrefix(q string) string {
	t := strings.TrimSpace(q)
	if len(t) < 6 || !strings.EqualFold(t[:4], "WITH") {
		return t
	}
	c4 := t[4]
	if c4 != ' ' && c4 != '\t' && c4 != '\n' && c4 != '\r' && c4 != '(' {
		return t
	}
	depth, i, n := 0, 0, len(t)
	var quote byte
	for i < n {
		c := t[i]
		if quote != 0 {
			if c == '\\' && (quote == '\'' || quote == '"') {
				i += 2
				continue
			}
			if c == quote {
				quote = 0
			}
			i++
			continue
		}
		switch c {
		case '\'', '"', '`':
			quote = c
			i++
			continue
		case '(':
			depth++
			i++
			continue
		case ')':
			depth--
			i++
			continue
		}
		if depth == 0 && isIdentByteCls(c) {
			j := i
			for j < n && isIdentByteCls(t[j]) {
				j++
			}
			if withMainVerbs[strings.ToUpper(t[i:j])] {
				return t[i:]
			}
			i = j
			continue
		}
		i++
	}
	return t
}

func isIdentByteCls(c byte) bool {
	return c == '_' || (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

// stmtPriv 判定语句所需权限（priv, 是否已识别）。
func stmtPriv(q string) (string, bool) {
	q = StripWithPrefix(strings.TrimSpace(q))
	switch {
	case reKindSelect.MatchString(q):
		return "SELECT", true
	case reKindInsert.MatchString(q):
		return "INSERT", true
	case reKindUpdate.MatchString(q):
		return "UPDATE", true
	case reKindDelete.MatchString(q):
		return "DELETE", true
	case reKindCreate.MatchString(q):
		if m := reKindCreate.FindStringSubmatch(q); m != nil && strings.EqualFold(m[1], "INDEX") {
			return "INDEX", true
		}
		return "CREATE", true
	case reKindDrop.MatchString(q):
		if m := reKindDrop.FindStringSubmatch(q); m != nil && strings.EqualFold(m[1], "INDEX") {
			return "INDEX", true
		}
		return "DROP", true
	case reKindAlter.MatchString(q):
		return "ALTER", true
	}
	return "", false
}

type privTarget struct{ db, table string }

// stmtTargets 提取语句目标表（db 为空时用 currentDB 补齐）。
func stmtTargets(q, currentDB string) []privTarget {
	var raws []string
	addRef := func(s string) {
		for _, one := range strings.Split(s, ",") {
			one = strings.TrimSpace(one)
			if one != "" {
				raws = append(raws, one)
			}
		}
	}
	if m := rePrivUpd.FindStringSubmatch(q); m != nil {
		addRef(m[1])
	}
	for _, re := range []*regexp.Regexp{rePrivFrom, rePrivJoin, rePrivInto, rePrivTable} {
		for _, m := range re.FindAllStringSubmatch(q, -1) {
			addRef(m[1])
		}
	}
	// CREATE/DROP INDEX 的 ON 目标（仅这两类语句，避免 JOIN … ON 谓词误伤）
	if reKindIdxDDL.MatchString(q) {
		for _, m := range rePrivIdxOn.FindAllStringSubmatch(q, -1) {
			addRef(m[1])
		}
	}

	seen := map[string]bool{}
	var out []privTarget
	for _, r := range raws {
		names := rePrivName.FindAllString(r, -1)
		if len(names) == 0 {
			continue
		}
		db, table := "", unquoteIdentLocal(names[0])
		if len(names) >= 2 {
			db, table = unquoteIdentLocal(names[0]), unquoteIdentLocal(names[1])
		}
		if table == "" || strings.ContainsAny(table, "()") {
			continue
		}
		if db == "" {
			db = currentDB
		}
		key := strings.ToLower(db + "." + table)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, privTarget{db: db, table: table})
	}
	return out
}

func unquoteIdentLocal(s string) string {
	s = strings.TrimSpace(s)
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
