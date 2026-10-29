package sqlite

import (
	"database/sql"
	"fmt"
	"sync"
)

// 只读直查（v1.3.13）：KV SQL 读穿透回源等内部组件按库执行 SELECT 用。
// 连接按库缓存（MaxOpenConns=1，与业务库同一 WAL——只读查询不阻塞写入）。

var (
	roMu    sync.Mutex
	roConns = map[string]*sql.DB{}
)

// QueryDBReadOnly 对指定库执行只读查询，返回 (列名, 行集)。
// 行数硬上限 1000（回源结果面向缓存回填，单行单列为主；不承担批量导出）。
// dbName 校验走建库同名规则（防注入）；库不存在返回错误。
func (b *Backend) QueryDBReadOnly(dbName, query string) ([]string, [][]any, error) {
	if !reDBName.MatchString(dbName) {
		return nil, nil, fmt.Errorf("invalid database name %q", dbName)
	}
	roMu.Lock()
	db, ok := roConns[dbName]
	if !ok {
		var err error
		db, err = b.openFileRetry(b.fileFor(dbName), 2)
		if err != nil {
			roMu.Unlock()
			return nil, nil, err
		}
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		roConns[dbName] = db
	}
	roMu.Unlock()
	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return cols, out, err
		}
		out = append(out, vals)
		if len(out) >= 1000 {
			break
		}
	}
	return cols, out, rows.Err()
}
