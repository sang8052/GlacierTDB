// Package config 加载与默认化 config.yaml。
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// GlacierVersion 是 GlacierTDB 自身版本（区别于对外模拟的 MySQL 协议版本）。
const GlacierVersion = "1.0.3.alpha"

// ConcurrencyOpts 并发模型：读并发多线程（可按库限流），写单库单线程（多库并行）。
type ConcurrencyOpts struct {
	GlobalThreads            int            `yaml:"global_threads"`                // 全局并发执行上限（读写共享，0=不限）
	PerDBReadThreads         int            `yaml:"per_db_read_threads"`           // 全局默认：单库读并发上限（0=不限）
	PerDBReadThreadsOverride map[string]int `yaml:"per_db_read_threads_override"`  // 按库覆盖（如 backup: 8）
}

type Auth struct {
	User          string   `yaml:"user"`
	Password      string   `yaml:"password"`
	ReadOnlyUsers []string `yaml:"readonly_users"`
}

type SQLiteOpts struct {
	Synchronous   string `yaml:"synchronous"` // OFF|NORMAL|FULL|EXTRA
	BusyTimeoutMs int    `yaml:"busy_timeout_ms"`
	CacheSizeKB   int    `yaml:"cache_size_kb"`
	MaxResultRows int64  `yaml:"max_result_rows"` // 单条查询最大返回行数保护
}

type LogOpts struct {
	Level       string `yaml:"level"`
	SlowQueryMs int64  `yaml:"slow_query_ms"`
	SQLLogFile  string `yaml:"sql_log_file"` // SQL 审计日志路径（空=关闭），如 /data/sql.log
}

type Config struct {
	Listen   string `yaml:"listen"`
	DBDir    string `yaml:"db_dir"`     // 库目录：一个库 = 目录下一个 <db>.db 文件
	DefaultDB string `yaml:"default_db"` // 未指定库时连接的默认库
	// DBPath 兼容旧配置：.db 文件 → dir=所在目录, 默认库=文件名去扩展名；目录 → 默认库取 DefaultDB
	DBPath        string `yaml:"db_path"`
	ServerVersion string `yaml:"server_version"`

	MemoryLimitMB  int `yaml:"memory_limit_mb"`  // Go 运行时软内存上限（0 = 不限制）
	MaxConnections int `yaml:"max_connections"` // 最大并发客户端连接数

	Concurrency ConcurrencyOpts `yaml:"concurrency"`

	Auth   Auth       `yaml:"auth"`
	SQLite SQLiteOpts `yaml:"sqlite"`
	Log    LogOpts    `yaml:"log"`
}

func Default() *Config {
	return &Config{
		Listen:        "0.0.0.0:3309",
		DBDir:         "/data",
		DefaultDB:     "backup",
		ServerVersion: "8.99.999",
		Auth: Auth{
			User:          "glacier",
			Password:      "iloveyou~",
			ReadOnlyUsers: []string{},
		},
		MemoryLimitMB:  0,
		MaxConnections: 64,
		Concurrency: ConcurrencyOpts{
			GlobalThreads:            0,
			PerDBReadThreads:         0,
			PerDBReadThreadsOverride: map[string]int{},
		},
		SQLite: SQLiteOpts{
			Synchronous:   "NORMAL",
			BusyTimeoutMs: 5000,
			CacheSizeKB:   256,
			MaxResultRows: 1000000,
		},
		Log: LogOpts{
			Level:       "info",
			SlowQueryMs: 200,
		},
	}
}

// Load 读取 yaml 配置；文件不存在时使用默认值（warn），存在但解析失败时致命。
func Load(path string) (*Config, error) {
	cfg := Default()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	cfg.Fill()
	return cfg, nil
}

// Fill 补齐派生字段与非法默认。
func (c *Config) Fill() {
	if c.Listen == "" {
		c.Listen = "0.0.0.0:3309"
	}
	if c.DBPath != "" {
		// 兼容旧配置：db_path 指向 .db 文件
		if strings.HasSuffix(c.DBPath, ".db") {
			c.DBDir = filepath.Dir(c.DBPath)
			c.DefaultDB = strings.TrimSuffix(filepath.Base(c.DBPath), ".db")
		} else {
			c.DBDir = c.DBPath
		}
	}
	if c.DBDir == "" {
		c.DBDir = "/data"
	}
	if c.DefaultDB == "" {
		c.DefaultDB = "backup"
	}
	if c.ServerVersion == "" {
		c.ServerVersion = "8.99.999"
	}
	// 用户名固定为 glacier（管理面约定），配置不一致时以固定值为准
	c.Auth.User = "glacier"
	if c.MaxConnections <= 0 {
		c.MaxConnections = 64
	}
	if c.MemoryLimitMB < 0 {
		c.MemoryLimitMB = 0
	}
	switch c.SQLite.Synchronous {
	case "OFF", "NORMAL", "FULL", "EXTRA":
	default:
		c.SQLite.Synchronous = "NORMAL"
	}
	if c.SQLite.BusyTimeoutMs <= 0 {
		c.SQLite.BusyTimeoutMs = 5000
	}
	if c.SQLite.CacheSizeKB <= 0 {
		c.SQLite.CacheSizeKB = 256
	}
	if c.SQLite.MaxResultRows <= 0 {
		c.SQLite.MaxResultRows = 1000000
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	if c.Log.SlowQueryMs <= 0 {
		c.Log.SlowQueryMs = 200
	}
}

// SynchronousLevel 返回 PRAGMA synchronous 数值。
func (o SQLiteOpts) SynchronousLevel() int {
	switch o.Synchronous {
	case "OFF":
		return 0
	case "FULL":
		return 2
	case "EXTRA":
		return 3
	default:
		return 1 // NORMAL
	}
}

// IsReadOnlyUser 判断用户是否只读账号。
func (c *Config) IsReadOnlyUser(user string) bool {
	for _, u := range c.Auth.ReadOnlyUsers {
		if u == user {
			return true
		}
	}
	return false
}
