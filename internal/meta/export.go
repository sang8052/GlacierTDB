package meta

import (
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// LikeMatchPub 导出 likeMatch 供 handler 复用。
func LikeMatchPub(pattern, s string) bool { return likeMatch(pattern, s) }

func atoiSafe(s string) int64 {
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

// applyLimit 对已构建结果集做 MySQL 语义的 LIMIT [offset,]count。
func applyLimit(rs *mysql.Resultset, offset, limit int64) {
	if rs == nil {
		return
	}
	start := offset
	if start < 0 {
		start = 0
	}
	if start >= int64(len(rs.RowDatas)) {
		rs.RowDatas = nil
		return
	}
	end := int64(len(rs.RowDatas))
	if limit >= 0 && start+limit < end {
		end = start + limit
	}
	rs.RowDatas = rs.RowDatas[start:end]
}

// HasPlaceholders 检测文本协议中不应出现的参数占位符（裸 ? 与 $name，含 $$）。
// 与真 MySQL 行为对齐：真 MySQL 对 `select $$` / `select ?` 报 1064；
// 而 SQLite 会当未绑定参数执行返回 NULL，导致客户端状态机失步。
// 只扫描引号外内容（'...'、"..."、`...` 与注释内忽略）。
func HasPlaceholders(q string) bool {
	var quote byte
	i := 0
	for i < len(q) {
		c := q[i]
		if quote != 0 {
			if c == '\\' && (quote == '\'' || quote == '"') && i+1 < len(q) {
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
		case '-':
			if i+1 < len(q) && q[i+1] == '-' { // 行注释
				j := strings.IndexByte(q[i:], '\n')
				if j < 0 {
					return false
				}
				i += j + 1
			} else {
				i++
			}
		case '/':
			if i+1 < len(q) && q[i+1] == '*' { // 块注释
				j := strings.Index(q[i+2:], "*/")
				if j < 0 {
					return false
				}
				i += j + 4
			} else {
				i++
			}
		case '?', '$':
			return true
		default:
			i++
		}
	}
	return false
}

var _ = regexp.MustCompile // 保留 regexp 引用（若未来扩展）
