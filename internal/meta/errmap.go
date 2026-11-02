// Package meta — SQLite 错误 → MySQL 协议错误细化映射（v1.4.0）。
//
// 此前 handler.MapSQLiteErr 对 SQLITE_ERROR 一律报 1064、CONSTRAINT 一律报 1062，
// 客户端（pymysql / SQLAlchemy 的 IntegrityError / OperationalError / ProgrammingError
// 异常分类）无法据错误码区分「表不存在 / 列不存在 / 唯一键冲突 / 非空约束」等常见形态。
// 这里按 SQLite 错误消息文本前缀细分为 MySQL 标准错误码（数字字面量 + 注释标注名称，
// 与仓库既有 mysql.NewError(1290, …) 风格一致）：
//
//	no such table            → 1146 ER_NO_SUCH_TABLE
//	no such column           → 1054 ER_BAD_FIELD_ERROR
//	no such function         → 1305 ER_SP_DOES_NOT_EXIST
//	UNIQUE constraint failed → 1062 ER_DUP_ENTRY
//	NOT NULL constraint      → 1048 ER_BAD_NULL_ERROR
//	CHECK constraint failed  → 3819 ER_CHECK_CONSTRAINT_VIOLATED
//	FOREIGN KEY constraint   → 1452 ER_NO_REFERENCED_ROW_2
//	table X already exists   → 1050 ER_TABLE_EXISTS_ERROR
//	near "...": syntax error → 1064 ER_PARSE_ERROR（标准文案）
//	database is locked       → 1205 ER_LOCK_WAIT_TIMEOUT
//	readonly database / PERM → 1290 ER_OPTION_PREVENTS_STATEMENT
//
// 原则：只产出 MySQL 风格文案（SQLite 原文不透传给客户端，细节进服务端日志）。
package meta

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	sqlite3 "github.com/ncruces/go-sqlite3"

	"glaciertdb/internal/logx"
)

// MapSQLiteToMySQLError 把 SQLite 驱动错误细化为 MySQL 协议错误。
// currentDB 为当前库名（可为空——空时 1146/1305 文案退化为不带库前缀）。
// 已是 mysql.MyError 的错误原样返回；非 SQLite 错误按 1105 透传消息。
func MapSQLiteToMySQLError(err error, currentDB string) error {
	if err == nil {
		return nil
	}
	var me *mysql.MyError
	if errors.As(err, &me) {
		return err
	}
	var se *sqlite3.Error
	if !errors.As(err, &se) {
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, err.Error())
	}
	// ncruces 驱动的 Error() 带「sqlite3: <错误描述>: 」装饰前缀
	//（实测 v0.35.6：`sqlite3: SQL logic error: no such table: x`），
	// 匹配一律用全文检索而非前缀缀合；tail 从模式命中处取到行尾
	//（目标模式均为 SQLite 消息的末段，命中即取尾）。
	msg := se.Error()
	switch {
	case findIdx(msg, "no such table: ") >= 0:
		name := errTail(msg, findIdx(msg, "no such table: ")+len("no such table: "))
		name = qualifyName(name, currentDB)
		return mysql.NewError(1146, "Table '"+name+"' doesn't exist") // ER_NO_SUCH_TABLE
	case findIdx(msg, "no such column: ") >= 0:
		col := errTail(msg, findIdx(msg, "no such column: ")+len("no such column: "))
		return mysql.NewError(1054, "Unknown column '"+col+"' in 'field list'") // ER_BAD_FIELD_ERROR
	case findIdx(msg, "no such function: ") >= 0:
		fn := errTail(msg, findIdx(msg, "no such function: ")+len("no such function: "))
		if currentDB != "" {
			fn = currentDB + "." + fn
		}
		return mysql.NewError(1305, "FUNCTION "+fn+" does not exist") // ER_SP_DOES_NOT_EXIST
	case findIdx(msg, "UNIQUE constraint failed: ") >= 0:
		// SQLite 不回传冲突值，值域以 '?' 占位（客户端工具断言通常只看错误码）
		key := qualifyName(errTail(msg, findIdx(msg, "UNIQUE constraint failed: ")+len("UNIQUE constraint failed: ")), currentDB)
		return mysql.NewError(mysql.ER_DUP_ENTRY, "Duplicate entry '?' for key '"+key+"'")
	case findIdx(msg, "NOT NULL constraint failed: ") >= 0:
		col := qualifyName(errTail(msg, findIdx(msg, "NOT NULL constraint failed: ")+len("NOT NULL constraint failed: ")), currentDB)
		return mysql.NewError(1048, "Column '"+col+"' cannot be null") // ER_BAD_NULL_ERROR
	case findIdx(msg, "CHECK constraint failed: ") >= 0:
		name := errTail(msg, findIdx(msg, "CHECK constraint failed: ")+len("CHECK constraint failed: "))
		return mysql.NewError(3819, "Check constraint '"+name+"' is violated.")
	case findIdx(msg, "FOREIGN KEY constraint failed") >= 0:
		return mysql.NewError(1452, // ER_NO_REFERENCED_ROW_2
			"Cannot add or update a child row: a foreign key constraint fails")
	case reTableExists.MatchString(msg):
		name := reTableExists.FindStringSubmatch(msg)[1]
		return mysql.NewError(1050, "Table '"+name+"' already exists") // ER_TABLE_EXISTS_ERROR
	case reNearSyntax.MatchString(msg):
		// 形如 near "FROM": syntax error → 标准 1064 文案（细节进服务端日志）
		near := strings.Trim(reNearSyntax.FindStringSubmatch(msg)[1], `"`)
		logParseDetail(msg)
		return mysql.NewError(mysql.ER_PARSE_ERROR, parseErrMsg(near))
	case findIdx(msg, "database is locked") >= 0 || findIdx(msg, "database table is locked") >= 0:
		return mysql.NewError(mysql.ER_LOCK_WAIT_TIMEOUT,
			"Lock wait timeout exceeded; try restarting transaction (database is locked)")
	case findIdx(msg, "readonly") >= 0:
		logParseDetail(msg)
		return mysql.NewError(1290, // ER_OPTION_PREVENTS_STATEMENT
			"The MySQL server is running with the --read-only option so this statement cannot be executed")
	case se.Code() == sqlite3.ERROR:
		// 其余 SQLITE_ERROR：统一 1064 标准文案，SQLite 原文只进服务端日志
		logParseDetail(msg)
		return mysql.NewError(mysql.ER_PARSE_ERROR, parseErrMsg(""))
	case se.Code() == sqlite3.BUSY || se.Code() == sqlite3.LOCKED:
		return mysql.NewError(mysql.ER_LOCK_WAIT_TIMEOUT,
			"Lock wait timeout exceeded; try restarting transaction ("+msg+")")
	case se.Code() == sqlite3.READONLY || se.Code() == sqlite3.PERM:
		return mysql.NewError(1290, // ER_OPTION_PREVENTS_STATEMENT
			"The MySQL server is running with the --read-only option so this statement cannot be executed")
	case se.Code() == sqlite3.CANTOPEN:
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR,
			"Cannot open database file (is the directory writable for WAL?)")
	default:
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}
}

// errTail 取消息中 from 起的剩余正文（去空白）——模式命中点到行尾。
func errTail(msg string, from int) string {
	return strings.TrimSpace(msg[from:])
}

// findIdx 全文检索（ncruces 装饰前缀见上）。
func findIdx(msg, pattern string) int {
	return strings.Index(msg, pattern)
}

// reTableExists `table <name> already exists`（装饰前缀下同样命中）。
var reTableExists = regexp.MustCompile(`table ([^\s:]+) already exists`)

// reNearSyntax `near "...": syntax error`。
var reNearSyntax = regexp.MustCompile(`near (.+): syntax error`)

// qualifyName 错误对象名规范化：剥掉 SQLite schema 前缀 main./temp.；
// 无任何 schema 前缀且 currentDB 非空时补当前库名（对齐 MySQL 'db.table' 文案）。
func qualifyName(name, currentDB string) string {
	for _, p := range []string{"main.", "temp."} {
		if strings.HasPrefix(name, p) {
			name = name[len(p):]
			break
		}
	}
	if currentDB != "" && !strings.Contains(name, ".") {
		name = currentDB + "." + name
	}
	return name
}

// parseErrMsg 标准 1064 文案（near 段可为空）。
func parseErrMsg(near string) string {
	return fmt.Sprintf("You have an error in your SQL syntax; check the manual that "+
		"corresponds to your MySQL server version for the right syntax to use near '%s' at line 1", near)
}

// logParseDetail SQLite 原始错误细节进服务端日志（客户端只看 MySQL 文案）。
func logParseDetail(msg string) {
	logx.Debugf("sqlite error detail: %s", msg)
}
