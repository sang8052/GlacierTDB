package meta

import (
	"fmt"
	"regexp"
	"strings"
)

// information_schema 轻量兼容层：客户端工具（Navicat/DBeaver 等）会直查
// SELECT ... FROM information_schema.<T> [WHERE ...]，这里直接构造结果集应答，
// 不透传 SQLite（SQLite 无此库）。支持列投影与常用 WHERE（= / LIKE / IN / AND，
// DATABASE()/SCHEMA() 求值为当前库）。

var (
	reISQuery = regexp.MustCompile(`(?is)^\s*SELECT\s+(.+?)\s+FROM\s+(?:` + "`" + `|")?(information_schema|mysql|performance_schema|sys)(?:` + "`" + `|")?\s*\.\s*(?:` + "`" + `|")?([A-Za-z_][A-Za-z0-9_]*)(?:` + "`" + `|")?\s*(?:\s+WHERE\s+(.+?))?(?:\s+(?:ORDER\s+BY|GROUP\s+BY|LIMIT)\b.+)?\s*$`)
	reISFnDB  = regexp.MustCompile(`(?i)\b(DATABASE|SCHEMA)\s*\(\s*\)`)
	reISIn    = regexp.MustCompile(`(?i)(\w+)\s+IN\s*(\((?:\s*'[^']*'\s*,?\s*)+\))`)
)

// HandleInfoSchemaQuery 处理 information_schema 表查询；handled=false 表示不是此类查询。
func HandleInfoSchemaQuery(ctx *Ctx, q string) (*Result, bool, error) {
	m := reISQuery.FindStringSubmatch(q)
	if m == nil {
		return nil, false, nil
	}
	colsExpr := strings.TrimSpace(m[1])
	schema := strings.ToLower(m[2])
	table := strings.ToLower(m[3])
	where := strings.TrimSpace(m[4])
	if where != "" {
		where = reISFnDB.ReplaceAllStringFunc(where, func(string) string { return quoteStr(ctx.Sess.CurrentDB()) })
	}

	lookup := table
	if schema != "information_schema" {
		lookup = schema + "." + table // mysql.user 等系统库表
	}

	builder, ok := isTables[lookup]
	if !ok {
		// 未覆盖的表：返回带常用列的空结果，避免客户端报 no such table
		return emptyISResult(table, colsExpr)
	}
	cols, rows, err := builder(ctx)
	if err != nil {
		return nil, true, MapSQLError(err)
	}
	rows = filterISRows(cols, rows, where)
	return projectISResult(cols, rows, colsExpr)
}

type isBuilder func(ctx *Ctx) ([]string, [][]any, error)

var isTables map[string]isBuilder

func init() {
	isTables = map[string]isBuilder{
		"engines":       isEngines,
		"mysql.user":    isMysqlUser,
		"schemata":      isSchemata,
		"tables":        isTablesQ,
		"columns":       isColumns,
		"views":         isViews,
		"statistics":    isStatistics,
		"table_constraints": isTableConstraints,
		"key_column_usage":  isKeyColumnUsage,
		"triggers":      isTriggersQ,
		"collations":    isCollations,
		"character_sets": isCharacterSets,
		"routines":            isEmptyRoutine,
		"parameters":          isEmptyParams,
		"referential_constraints": isEmptyRefCons,
		"partitions":          isEmptyPartitions,
		"events":              isEmptyEvents,
		"processlist":         isProcesslistQ,
		"user_privileges":     isEmptyPrivileges,
		"schema_privileges":   isEmptyPrivileges,
		"table_privileges":    isEmptyPrivileges,
		"column_privileges":   isEmptyPrivileges,
	}
}

// ---------------------------------------------------------------------------
// 数据构造器
// ---------------------------------------------------------------------------

func isEngines(*Ctx) ([]string, [][]any, error) {
	return []string{"ENGINE", "SUPPORT", "COMMENT", "TRANSACTIONS", "XA", "SAVEPOINTS"}, [][]any{
		{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
		{"MEMORY", "YES", "Hash based, stored in memory, useful for temporary tables", "NO", "NO", "NO"},
		{"MyISAM", "YES", "MyISAM storage engine", "NO", "NO", "NO"},
		{"CSV", "YES", "CSV storage engine", "NO", "NO", "NO"},
		{"ARCHIVE", "YES", "Archive storage engine", "NO", "NO", "NO"},
		{"BLACKHOLE", "YES", "/dev/null storage engine (anything you write to it disappears)", "NO", "NO", "NO"},
	}, nil
}

func isSchemata(ctx *Ctx) ([]string, [][]any, error) {
	var rows [][]any
	for _, d := range ctx.Sess.ListDatabases() {
		if ctx.DBVisible != nil && !ctx.DBVisible(d) {
			continue // 多租户：USER 类型账号仅见授权库（v1.3.6）
		}
		rows = append(rows, []any{"def", d, "utf8mb4", "utf8mb4_0900_ai_ci", nil})
	}
	return []string{"CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME", "SQL_PATH"}, rows, nil
}

func isTablesQ(ctx *Ctx) ([]string, [][]any, error) {
	_, rows, err := ctx.Sess.Query("SELECT name, type FROM main.sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	db := ctx.Sess.CurrentDB()
	var out [][]any
	for _, r := range rows {
		name, typ := rowGet(r, 0), rowGet(r, 1)
		tt := "BASE TABLE"
		if typ == "view" {
			tt = "VIEW"
		}
		out = append(out, []any{"def", db, name, tt, "SQLite", nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, "utf8mb4_0900_ai_ci", nil, nil, ""})
	}
	return []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "ENGINE", "VERSION", "ROW_FORMAT", "TABLE_ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "TABLE_COLLATION", "CHECKSUM", "CREATE_OPTIONS", "TABLE_COMMENT"}, out, nil
}

func isColumns(ctx *Ctx) ([]string, [][]any, error) {
	_, tables, err := ctx.Sess.Query("SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	db := ctx.Sess.CurrentDB()
	var out [][]any
	ord := 0
	for _, tr := range tables {
		tname := rowGet(tr, 0)
		_, ti, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.table_info(%s)", quoteIdent(tname)))
		if err != nil {
			continue
		}
		for _, c := range ti {
			cname, ctype := rowGet(c, 1), rowGet(c, 2)
			nullable := "YES"
			if rowGet(c, 3) == "1" {
				nullable = "NO"
			}
			var dflt any
			if len(c) > 4 && c[4] != nil {
				dflt = toStr(c[4])
			}
			ctype = OverrideColumnType(db, tname, cname, ctype)
			dataType := strings.ToUpper(strings.TrimSpace(strings.SplitN(ctype, "(", 2)[0]))
			colKey := ""
			extra := ""
			if toI64(rowGet(c, 5)) > 0 {
				colKey = "PRI"
			}
			ord++
			out = append(out, []any{"def", db, tname, cname, int64(ord), dflt, nullable, dataType, nil, nil,
				ctype, colKey, extra, nil, nil, nil, nil, nil, "", "", "utf8mb4", "utf8mb4_0900_ai_ci"})
		}
	}
	return []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "COLUMN_DEFAULT", "IS_NULLABLE", "DATA_TYPE", "CHARACTER_MAXIMUM_LENGTH", "NUMERIC_PRECISION",
		"COLUMN_TYPE", "COLUMN_KEY", "EXTRA", "PRIVILEGES", "COLUMN_COMMENT", "GENERATION_EXPRESSION", "SRS_ID", "UDT_NAME", "EXTRA2", "DEFCOLL", "CHARACTER_SET_NAME", "COLLATION_NAME"}, out, nil
}

func isViews(ctx *Ctx) ([]string, [][]any, error) {
	_, rows, err := ctx.Sess.Query("SELECT name, sql FROM main.sqlite_master WHERE type='view' AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	db := ctx.Sess.CurrentDB()
	var out [][]any
	for _, r := range rows {
		out = append(out, []any{"def", db, rowGet(r, 0), rowGet(r, 1), "NONE", "NO", "glacier@%", "DEFINER", "utf8mb4", "utf8mb4_0900_ai_ci"})
	}
	return []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "VIEW_DEFINITION", "CHECK_OPTION", "IS_UPDATABLE", "DEFINER", "SECURITY_TYPE", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION"}, out, nil
}

func isStatistics(ctx *Ctx) ([]string, [][]any, error) {
	_, tables, err := ctx.Sess.Query("SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	db := ctx.Sess.CurrentDB()
	var out [][]any
	for _, tr := range tables {
		tname := rowGet(tr, 0)
		_, il, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_list(%s)", quoteIdent(tname)))
		if err != nil {
			continue
		}
		for _, ir := range il {
			idxName, unique := rowGet(ir, 1), rowGet(ir, 2) == "1"
			_, ii, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_info(%s)", quoteIdent(idxName)))
			if err != nil {
				continue
			}
			for seq, cr := range ii {
				cn := rowGet(cr, 2)
				var colVal any
				if cn != "" {
					colVal = cn
				}
				out = append(out, []any{"def", db, tname, b2i(!unique), db, idxName, int64(seq + 1), colVal, "A", int64(0), nil, nil, "", "BTREE", "", "", "YES", nil})
			}
		}
	}
	return []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "NON_UNIQUE", "INDEX_SCHEMA", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME", "COLLATION", "CARDINALITY", "SUB_PART", "PACKED", "NULLABLE", "INDEX_TYPE", "COMMENT", "INDEX_COMMENT", "IS_VISIBLE", "EXPRESSION"}, out, nil
}

func isTableConstraints(ctx *Ctx) ([]string, [][]any, error) {
	_, tables, err := ctx.Sess.Query("SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	db := ctx.Sess.CurrentDB()
	var out [][]any
	for _, tr := range tables {
		tname := rowGet(tr, 0)
		_, il, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_list(%s)", quoteIdent(tname)))
		if err != nil {
			continue
		}
		for _, ir := range il {
			idxName, origin := rowGet(ir, 1), rowGet(ir, 3)
			cType := ""
			switch origin {
			case "pk":
				cType = "PRIMARY KEY"
			case "u":
				cType = "UNIQUE"
			}
			if cType == "" {
				continue
			}
			out = append(out, []any{"def", db, idxName, db, tname, cType})
		}
	}
	return []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_TYPE"}, out, nil
}

func isKeyColumnUsage(ctx *Ctx) ([]string, [][]any, error) {
	_, tables, err := ctx.Sess.Query("SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite$_%' ESCAPE '$' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	db := ctx.Sess.CurrentDB()
	var out [][]any
	for _, tr := range tables {
		tname := rowGet(tr, 0)
		_, il, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_list(%s)", quoteIdent(tname)))
		if err != nil {
			continue
		}
		for _, ir := range il {
			idxName, origin := rowGet(ir, 1), rowGet(ir, 3)
			if origin != "pk" && origin != "u" {
				continue
			}
			_, ii, err := ctx.Sess.Query(fmt.Sprintf("PRAGMA main.index_info(%s)", quoteIdent(idxName)))
			if err != nil {
				continue
			}
			for seq, cr := range ii {
				out = append(out, []any{"def", db, idxName, "def", db, tname, rowGet(cr, 2), int64(seq + 1), nil, nil, nil})
			}
		}
	}
	return []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "POSITION_IN_UNIQUE_CONSTRAINT", "REFERENCED_TABLE_SCHEMA", "REFERENCED_COLUMN_NAME"}, out, nil
}

func isTriggersQ(ctx *Ctx) ([]string, [][]any, error) {
	_, rows, err := ctx.Sess.Query("SELECT name, sql, tbl_name FROM main.sqlite_master WHERE type='trigger' ORDER BY name")
	if err != nil {
		return nil, nil, err
	}
	db := ctx.Sess.CurrentDB()
	var out [][]any
	for _, r := range rows {
		out = append(out, []any{"def", db, rowGet(r, 0), "INSERT", rowGet(r, 2), rowGet(r, 1), "AFTER"})
	}
	return []string{"TRIGGER_CATALOG", "TRIGGER_SCHEMA", "TRIGGER_NAME", "EVENT_MANIPULATION", "EVENT_OBJECT_TABLE", "ACTION_STATEMENT", "ACTION_TIMING"}, out, nil
}

func isCollations(*Ctx) ([]string, [][]any, error) {
	return []string{"COLLATION_NAME", "CHARACTER_SET_NAME", "ID", "IS_DEFAULT", "IS_COMPILED", "SORTLEN", "PAD_ATTRIBUTE"}, [][]any{
		{"utf8mb4_0900_ai_ci", "utf8mb4", int64(255), "Yes", "Yes", int64(0), "NO PAD"},
		{"utf8mb4_general_ci", "utf8mb4", int64(45), "", "Yes", int64(1), "PAD SPACE"},
		{"utf8mb4_bin", "utf8mb4", int64(46), "", "Yes", int64(1), "PAD SPACE"},
		{"utf8mb3_general_ci", "utf8mb3", int64(33), "", "Yes", int64(1), "PAD SPACE"},
		{"latin1_swedish_ci", "latin1", int64(8), "Yes", "Yes", int64(1), "PAD SPACE"},
		{"binary", "binary", int64(63), "Yes", "Yes", int64(1), "NO PAD"},
	}, nil
}

func isCharacterSets(*Ctx) ([]string, [][]any, error) {
	return []string{"CHARACTER_SET_NAME", "DEFAULT_COLLATE_NAME", "DESCRIPTION", "MAXLEN"}, [][]any{
		{"utf8mb4", "utf8mb4_0900_ai_ci", "UTF-8 Unicode", int64(4)},
		{"utf8mb3", "utf8mb3_general_ci", "UTF-8 Unicode", int64(3)},
		{"latin1", "latin1_swedish_ci", "cp1252 West European", int64(1)},
		{"binary", "binary", "Binary pseudo charset", int64(1)},
		{"ascii", "ascii_general_ci", "US ASCII", int64(1)},
	}, nil
}

func isProcesslistQ(ctx *Ctx) ([]string, [][]any, error) {
	return []string{"ID", "USER", "HOST", "DB", "COMMAND", "TIME", "STATE", "INFO"},
		[][]any{{int64(ctx.ConnID), ctx.User, ctx.Remote, ctx.Sess.CurrentDB(), "Query", int64(0), "executing", nil}}, nil
}

func isEmptyRoutine(*Ctx) ([]string, [][]any, error) {
	return []string{"SPECIFIC_NAME", "ROUTINE_CATALOG", "ROUTINE_SCHEMA", "ROUTINE_NAME", "ROUTINE_TYPE", "DATA_TYPE"}, nil, nil
}

func isEmptyParams(*Ctx) ([]string, [][]any, error) {
	return []string{"SPECIFIC_CATALOG", "SPECIFIC_SCHEMA", "SPECIFIC_NAME", "ORDINAL_POSITION", "PARAMETER_MODE"}, nil, nil
}

func isEmptyRefCons(*Ctx) ([]string, [][]any, error) {
	return []string{"CONSTRAINT_CATALOG", "CONSTRAINT_SCHEMA", "CONSTRAINT_NAME", "UNIQUE_CONSTRAINT_CATALOG"}, nil, nil
}

func isEmptyPartitions(*Ctx) ([]string, [][]any, error) {
	return []string{"TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "PARTITION_NAME"}, nil, nil
}

func isEmptyEvents(*Ctx) ([]string, [][]any, error) {
	return []string{"EVENT_CATALOG", "EVENT_SCHEMA", "EVENT_NAME", "DEFINER"}, nil, nil
}

func isEmptyPrivileges(*Ctx) ([]string, [][]any, error) {
	return []string{"GRANTEE", "TABLE_CATALOG", "PRIVILEGE_TYPE", "IS_GRANTABLE"}, nil, nil
}

func emptyISResult(table, colsExpr string) (*Result, bool, error) {
	names := isProjectNames(colsExpr)
	if len(names) == 0 {
		names = []string{"NAME"}
	}
	res, err := simpleRS(names, nil)
	return res, true, err
}

// ---------------------------------------------------------------------------
// WHERE 过滤与投影
// ---------------------------------------------------------------------------

// filterISRows 支持 col = 'v' / col LIKE 'p' / col IN ('a','b') 的 AND 组合过滤。
func filterISRows(cols []string, rows [][]any, where string) [][]any {
	if strings.TrimSpace(where) == "" {
		return rows
	}
	type cond struct {
		col  string
		op   string // = / LIKE / IN
		vals []string
	}
	var conds []cond
	reC := regexp.MustCompile(`(?i)(\w+)\s*(=|LIKE|IN)\s*('(?:[^']|'')*'(?:\s*,\s*'(?:[^']|'')*')*)`)
	for _, m := range reC.FindAllStringSubmatch(where, -1) {
		vals := []string{}
		for _, v := range regexp.MustCompile(`'((?:[^']|'')*)'`).FindAllStringSubmatch(m[3], -1) {
			vals = append(vals, strings.ReplaceAll(v[1], "''", "'"))
		}
		conds = append(conds, cond{strings.ToLower(m[1]), strings.ToUpper(m[2]), vals})
	}
	if len(conds) == 0 {
		return rows // 无法解析的 WHERE：返回全量（客户端自行处理）
	}
	idx := func(name string) int {
		for i, c := range cols {
			if strings.EqualFold(c, name) {
				return i
			}
		}
		return -1
	}
	var out [][]any
	for _, r := range rows {
		ok := true
		for _, c := range conds {
			i := idx(c.col)
			if i < 0 || i >= len(r) || len(c.vals) == 0 {
				continue
			}
			cell := toStr(r[i])
			switch c.op {
			case "=":
				if cell != c.vals[0] {
					ok = false
				}
			case "LIKE":
				if !likeMatch(c.vals[0], cell) {
					ok = false
				}
			case "IN":
				hit := false
				for _, v := range c.vals {
					if cell == v {
						hit = true
						break
					}
				}
				if !hit {
					ok = false
				}
			}
			if !ok {
				break
			}
		}
		if ok {
			out = append(out, r)
		}
	}
	return out
}

// isProjectNames 从 SELECT 列表达式提取列名（忽略聚合/别名）。
func isProjectNames(colsExpr string) []string {
	var names []string
	for _, part := range splitTopLevel(colsExpr) {
		p := strings.TrimSpace(part)
		p = strings.ReplaceAll(p, "`", "")
		// 去掉 AS alias / 表别名前缀 / 函数包裹（保守：取末尾标识符）
		if i := regexp.MustCompile(`(?i)\s+AS\s+`).FindStringIndex(p); i != nil {
			p = strings.TrimSpace(p[i[1]:])
		}
		if i := strings.LastIndex(p, "."); i >= 0 {
			p = p[i+1:]
		}
		p = strings.Trim(p, `"`)
		if p == "" {
			p = "?"
		}
		names = append(names, p)
	}
	return names
}

// projectISResult 按客户端 SELECT 的列做投影；未知列补 NULL。
func projectISResult(cols []string, rows [][]any, colsExpr string) (*Result, bool, error) {
	names := isProjectNames(colsExpr)
	if strings.TrimSpace(colsExpr) == "*" || len(names) == 0 {
		res, err := simpleRS(cols, rows)
		return res, true, err
	}
	idx := func(name string) int {
		for i, c := range cols {
			if strings.EqualFold(c, name) {
				return i
			}
		}
		return -1
	}
	var out [][]any
	for _, r := range rows {
		row := make([]any, len(names))
		for j, n := range names {
			if i := idx(n); i >= 0 && i < len(r) {
				row[j] = r[i]
			} else {
				row[j] = nil
			}
		}
		out = append(out, row)
	}
	res, err := simpleRS(names, out)
	return res, true, err
}

// isMysqlUser 兼容客户端用户管理界面对 mysql.user 的查询（单账号 glacier）。
func isMysqlUser(ctx *Ctx) ([]string, [][]any, error) {
	privs := []string{"Select", "Insert", "Update", "Delete", "Create", "Drop", "Reload", "Shutdown",
		"Process", "File", "Grant", "References", "Index", "Alter", "Show_db", "Super",
		"Create_tmp_table", "Lock_tables", "Execute", "Create_view", "Show_view",
		"Create_routine", "Alter_routine", "Event", "Trigger", "Create_tablespace"}
	cols := []string{"Host", "User"}
	row := []any{"%", ctx.Sess.CurrentUser()}
	for _, p := range privs {
		cols = append(cols, p+"_priv")
		row = append(row, "Y")
	}
	cols = append(cols, "ssl_type", "ssl_cipher", "x509_issuer", "x509_subject",
		"max_questions", "max_updates", "max_connections", "max_user_connections",
		"plugin", "authentication_string", "password_expired", "password_last_changed",
		"password_lifetime", "account_locked", "Create_role_priv", "Drop_role_priv",
		"Password_reuse_history", "Password_reuse_time", "Password_require_current", "User_attributes")
	row = append(row, "", "", "", "", int64(0), int64(0), int64(0), int64(0),
		"mysql_native_password", "", "N", nil, nil, "N", "Y", "Y", nil, nil, nil, nil)
	return cols, [][]any{row}, nil
}
