// Package meta — MySQL JSON 运算符改写（v1.0.10）。
//
// col->>'$.a'  →  json_extract(col,'$.a')                    （取文本值）
// col->'$.a'   →  CASE 包裹的 json_quote(json_extract(...))   （取 JSON 值：字符串加引号）
// 扫描引号感知：字符串 / 注释内的 -> 不动；操作数支持标识符、db.表、字面量、括号表达式；
// 嵌套链式（a->'$.x'->>'$.y'）递归处理。
package meta

import "strings"

// jsonOpSite 一次合法运算符出现位置。
type jsonOpSite struct {
	operandStart int    // 操作数起始
	opStart      int    // 运算符 "->" 起始
	litStart     int    // 路径字面量起始
	litEnd       int    // 路径字面量之后
	arrow        string // "->" 或 "->>"
}

// RewriteJSONOps 把 MySQL JSON 运算符（-> / ->>）改写为 SQLite JSON1 等价形式。
// 取「最右」合法运算符就地替换，操作数与头部递归处理——天然覆盖链式嵌套。
func RewriteJSONOps(q string) string {
	if !strings.Contains(q, "->") {
		return q
	}
	site, found := findLastJSONOp(q)
	if !found {
		return q
	}
	operandRaw := strings.TrimRight(q[site.operandStart:site.opStart], " \t\n\r")
	operand := RewriteJSONOps(operandRaw) // 嵌套运算符（操作数内部）
	lit := q[site.litStart:site.litEnd]   // 原始 '...' 路径字面量
	var mid string
	if site.arrow == "->>" {
		mid = "json_extract(" + operand + "," + lit + ")"
	} else {
		// -> 语义：字符串值加引号返回（MySQL JSON 值形态）
		ex := "json_extract(" + operand + "," + lit + ")"
		mid = "(CASE WHEN json_type(" + operand + "," + lit + ")='text' THEN json_quote(" + ex + ") ELSE " + ex + " END)"
	}
	return RewriteJSONOps(q[:site.operandStart]) + mid + q[site.litEnd:]
}

// findLastJSONOp 扫描全串（跳过引号/注释），返回最右侧的合法运算符位置。
func findLastJSONOp(q string) (jsonOpSite, bool) {
	var site jsonOpSite
	found := false
	i, n := 0, len(q)
	for i < n {
		c := q[i]
		switch c {
		case '\'', '"', '`':
			j, ok := skipQuotedSeg(q, i)
			if !ok {
				return site, false // 引号不闭合：放弃改写
			}
			i = j
		case '#':
			j := strings.IndexByte(q[i:], '\n')
			if j < 0 {
				i = n
			} else {
				i += j + 1
			}
		case '-':
			if i+1 < n && q[i+1] == '>' {
				ls := jsonLeftOperandStart(q, i)
				opEnd := i + 2
				arrow := "->"
				if opEnd < n && q[opEnd] == '>' {
					arrow = "->>"
					opEnd++
				}
				k := opEnd
				for k < n && isASCIISpace(q[k]) {
					k++
				}
				if ls >= 0 && k < n && q[k] == '\'' {
					litEnd, pathRaw, closed := scanMySQLLiteral(q, k)
					operandRaw := strings.TrimRight(q[ls:i], " \t\n\r")
					if closed && pathRaw != "" && operandRaw != "" {
						site = jsonOpSite{operandStart: ls, opStart: i, litStart: k, litEnd: litEnd, arrow: arrow}
						found = true
					}
				}
				i = opEnd
				continue
			}
			i++
		default:
			i++
		}
	}
	return site, found
}

// skipQuotedSeg 跳过引号段（双写引号 / 反斜杠转义），返回（段后位置, 是否闭合）。
func skipQuotedSeg(q string, i int) (int, bool) {
	c := q[i]
	j := i + 1
	for j < len(q) {
		if q[j] == '\\' && c != '`' && j+1 < len(q) {
			j += 2
			continue
		}
		if q[j] == c {
			if j+1 < len(q) && q[j+1] == c {
				j += 2
				continue
			}
			return j + 1, true
		}
		j++
	}
	return len(q), false
}

// jsonLeftOperandStart 回溯 `->` 左操作数的起始下标（-1 = 找不到合法操作数）。
func jsonLeftOperandStart(q string, opStart int) int {
	p := opStart - 1
	for p >= 0 && isASCIISpace(q[p]) {
		p--
	}
	if p < 0 {
		return -1
	}
	if q[p] == ')' { // 函数调用 / 括号表达式：回到配对 (
		depth := 0
		for p >= 0 {
			if q[p] == ')' {
				depth++
			} else if q[p] == '(' {
				depth--
				if depth == 0 {
					return p
				}
			}
			p--
		}
		return -1
	}
	for p >= 0 {
		c := q[p]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$' || c == '.':
			p--
		case c == '`' || c == '"': // 引号段标识符：跳到配对开引号后继续（db.`tbl` 链）
			p--
			for p >= 0 && q[p] != c {
				p--
			}
			p--
		case c == '\'': // 单引号字面量操作数（'' 连写为字面量内引号）
			p--
			for p >= 0 {
				if q[p] == '\'' {
					if p >= 1 && q[p-1] == '\'' {
						p -= 2
						continue
					}
					break // p 停在开引号
				}
				p--
			}
			p--
		case c == ']':
			p--
			for p >= 0 && q[p] != '[' {
				p--
			}
			p--
		case c == '>': // 操作数内 -> / ->> 的组成部分（链式嵌套）
			j := p
			for j >= 0 && q[j] == '>' {
				j--
			}
			if j >= 0 && q[j] == '-' {
				p = j - 1
				continue
			}
			return p + 1
		case c == '-': // 操作数内二元减号（a-1 等）
			p--
		default:
			return p + 1
		}
	}
	return p + 1
}
