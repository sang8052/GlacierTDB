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
	rs, err := mysql.BuildSimpleResultset(cols, rows, false)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

// SimpleRS 导出版本：供 handler 管理面构造结果集。
func SimpleRS(cols []string, rows [][]any) (*Result, error) { return simpleRS(cols, rows) }

// OKResultPub 导出版本：空 OK 应答。
func OKResultPub() *Result { return &mysql.Result{} }
