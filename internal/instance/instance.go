// Package instance 实例唯一 ID（v1.3.13）：UUID，首启生成后写入系统库
//（glacier_auth.db 的 instance_meta 表，随备份与加密体系走），重启不变。
//
// 暴露面（§6.2 需求）：MySQL SELECT @@glaciertdb_instance_id、Redis INFO server
// 字段、S3 全部响应头 x-glacier-instance-id（§1.4 防自旋依赖）、AMQP
// server-properties、启动日志 / 审计 / 异步任务回调 payload。
//
// 进程内单例：Load 在 main 启动时注入，UUID() 供全组件读取（未加载返回空串）。
package instance

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"
)

var id atomic.Value // string

// Load 打开（并初始化）系统库中的实例 ID 行；幂等。db 应为 glacier_auth 系统库。
func Load(db *sql.DB) (string, error) {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS instance_meta (
	  key TEXT PRIMARY KEY, value TEXT NOT NULL, created_ts TEXT NOT NULL DEFAULT '')`); err != nil {
		return "", fmt.Errorf("instance: init schema: %w", err)
	}
	var cur string
	err := db.QueryRow(`SELECT value FROM instance_meta WHERE key='instance_id'`).Scan(&cur)
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("instance: read: %w", err)
	}
	if cur == "" {
		cur = genUUID()
		if _, err := db.Exec(`INSERT INTO instance_meta(key,value,created_ts) VALUES('instance_id',?,?)
			ON CONFLICT(key) DO NOTHING`, cur, time.Now().Format("2006-01-02 15:04:05")); err != nil {
			return "", fmt.Errorf("instance: persist: %w", err)
		}
	}
	id.Store(cur)
	return cur, nil
}

// UUID 当前实例 ID（未加载 = 空串）。
func UUID() string {
	v, _ := id.Load().(string)
	return v
}

// genUUID UUID v4（36 字符连字格式）。
func genUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
