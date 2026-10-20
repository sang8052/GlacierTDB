package meta

import (
	"github.com/go-mysql-org/go-mysql/mysql"
)

// Result 是 mysql.Result 的别名，避免 handler 与 meta 双向依赖。
type Result = mysql.Result

// OKResult 空结果（OK 包）。
func OKResult() *Result { return &mysql.Result{} }

// NewMySQLError 构造 MySQL 协议错误。
func NewMySQLError(code uint16, msg string) error { return mysql.NewError(code, msg) }

// simpleRS 构造文本结果集。
func simpleRS(cols []string, rows [][]any) (*Result, error) {
	rs, err := BuildResultset(cols, rows)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

// SimpleRS 导出版本：供 handler 管理面构造结果集。
func SimpleRS(cols []string, rows [][]any) (*Result, error) { return simpleRS(cols, rows) }

// BuildResultset 构造原始 Resultset：先做空值规范化再编码。
// go-mysql v1.16 的 BuildSimpleResultset 会把 Go 空字符串 "" 编码成
// MySQL 协议 NULL（0xfb），导致 SQLite 里的 '' 回读变成 NULL；
// 这里把空串与空 []byte 统一为「长度 0 的字符串」，真正的 NULL（untyped nil）保持不变。
func BuildResultset(cols []string, rows [][]any) (*mysql.Resultset, error) {
	if rows != nil {
		norm := make([][]any, len(rows))
		for i, r := range rows {
			nr := make([]any, len(r))
			for j, v := range r {
				switch x := v.(type) {
				case string:
					if x == "" {
						nr[j] = []byte{}
						continue
					}
				case []byte:
					if x == nil {
						nr[j] = []byte{}
						continue
					}
				}
				nr[j] = v
			}
			norm[i] = nr
		}
		rows = norm
	}
	return mysql.BuildSimpleResultset(cols, rows, false)
}

// OKResultPub 导出版本：空 OK 应答。
func OKResultPub() *Result { return &mysql.Result{} }
