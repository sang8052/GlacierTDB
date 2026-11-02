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
// v1.4.0：改为就地规范化（消除逐行深拷贝的第二趟整体分配）——调用方传入的 rows
// 各自有独立底层数组（Query.Scan 逐行分配 / 管理面逐行构造），就地改写不影响调用方
// 后续只读用法（binarizeBlobFields 等对空值的判定不受影响）。
func BuildResultset(cols []string, rows [][]any) (*mysql.Resultset, error) {
	if rows != nil {
		for i := range rows {
			r := rows[i]
			for j, v := range r {
				switch x := v.(type) {
				case string:
					if x == "" {
						r[j] = []byte{}
					}
				case []byte:
					if x == nil {
						r[j] = []byte{}
					}
				}
			}
		}
	}
	return mysql.BuildSimpleResultset(cols, rows, false)
}

// OKResultPub 导出版本：空 OK 应答。
func OKResultPub() *Result { return &mysql.Result{} }
