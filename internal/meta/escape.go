// Package meta — MySQL 字符串字面量反转义（v1.0.9）。
//
// MySQL 协议客户端（pymysql / mysql CLI / SQLAlchemy 等）把参数值按 MySQL 规则
// 转义后拼进 SQL 文本（" → \"、' → \'、\ → \\、换行 → \n）；SQLite 的 '...'
// 字面量不处理反斜杠转义，原样透传会把转义序列当作数据落盘——JSON 配置存入后
// 读回带 \ 污染、标准 JSON 库无法解析、WHERE 等值比较永远无法命中（BUG.txt）。
//
// UnescapeLiterals 在语句进入 SQLite 前对单引号字面量做一次 MySQL→SQLite 还原：
// 先按 MySQL 规则解码，再按 SQLite 规则重编码（' → ''）。
// `标识符`、"标识符/字符串"、注释与字面量外的内容一律不动。
package meta

import "strings"

// mysqlUnescapeChar 解码反斜杠后的单个字符（MySQL 转义表）。
// 未知转义 \x → x（MySQL 语义为丢掉反斜杠）；\0 无法进入 SQLite SQL 文本
// （NUL 截断语句），保守保留两字符形式；\% 与 \_ 是 LIKE 通配符字面量，保留。
func mysqlUnescapeChar(b byte) string {
	switch b {
	case '0':
		return `\0`
	case '\'':
		return "'"
	case '"':
		return `"`
	case 'b':
		return "\b"
	case 'n':
		return "\n"
	case 'r':
		return "\r"
	case 't':
		return "\t"
	case 'Z':
		return "\x1a"
	case '\\':
		return `\`
	case '%':
		return `\%`
	case '_':
		return `\_`
	default:
		return string(b)
	}
}

// UnescapeLiterals 对 SQL 文本中的单引号字符串字面量做 MySQL→SQLite 转义还原。
// 无反斜杠时原样返回（零开销）；字面量未闭合同样原样返回，交由后端报语法错。
func UnescapeLiterals(q string) string {
	if !strings.Contains(q, `\`) || !strings.Contains(q, "'") {
		return q
	}
	var sb strings.Builder
	sb.Grow(len(q) + 16)
	i, n := 0, len(q)
	for i < n {
		c := q[i]
		switch c {
		case '\'', '"', '`':
			// 引号段（单引号做转义还原；双引号/反引号内容不动）
			if c == '\'' {
				j, content, closed := scanMySQLLiteral(q, i)
				if !closed {
					sb.WriteString(q[i:])
					return sb.String()
				}
				sb.WriteByte('\'')
				if strings.Contains(content, "'") {
					sb.WriteString(strings.ReplaceAll(content, "'", "''"))
				} else {
					sb.WriteString(content)
				}
				sb.WriteByte('\'')
				i = j
				continue
			}
			// "..." / `...`：找到配对结束引号（双写引号视为转义），内容原样
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
		case '#':
			// MySQL 行注释：引号不该被扫描
			j := strings.IndexByte(q[i:], '\n')
			if j < 0 {
				sb.WriteString(q[i:])
				return sb.String()
			}
			sb.WriteString(q[i : i+j+1])
			i += j + 1
		case '-':
			// MySQL `-- ` 行注释（要求前后空白，`x--1` 仍是表达式）
			if i+1 < n && q[i+1] == '-' &&
				(i == 0 || isASCIISpace(q[i-1])) &&
				(i+2 >= n || isASCIISpace(q[i+2])) {
				j := strings.IndexByte(q[i:], '\n')
				if j < 0 {
					sb.WriteString(q[i:])
					return sb.String()
				}
				sb.WriteString(q[i : i+j+1])
				i += j + 1
				continue
			}
			sb.WriteByte(c)
			i++
		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}

// scanMySQLLiteral 从 q[i]（开引号）起按 MySQL 规则扫描 '...' 字面量：
// \x 消费两字符、'' 为转义引号；返回（闭引号后位置, 解码后内容, 是否闭合）。
func scanMySQLLiteral(q string, i int) (int, string, bool) {
	var content strings.Builder
	j, n := i+1, len(q)
	for j < n {
		switch q[j] {
		case '\\':
			if j+1 < n {
				content.WriteString(mysqlUnescapeChar(q[j+1]))
				j += 2
			} else {
				content.WriteByte('\\')
				j++
			}
		case '\'':
			if j+1 < n && q[j+1] == '\'' {
				content.WriteByte('\'')
				j += 2
			} else {
				return j + 1, content.String(), true
			}
		default:
			content.WriteByte(q[j])
			j++
		}
	}
	return n, "", false
}

func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
