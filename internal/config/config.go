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
const GlacierVersion = "1.0.9.alpha"

// ConcurrencyOpts 并发模型：读并发多线程（可按库限流），写单库单线程（多库并行）。
type ConcurrencyOpts struct {
	GlobalThreads            int            `yaml:"global_threads"`                // 全局并发执行上限（读写共享，0=不限）
	PerDBReadThreads         int            `yaml:"per_db_read_threads"`           // 全局默认：单库读并发上限（0=不限）
	PerDBReadThreadsOverride map[string]int `yaml:"per_db_read_threads_override"`  // 按库覆盖（如 backup: 8）
}

// ParallelOpts 多核并行查询：
//   1) hint /*+ PARALLEL(n) */ 显式触发；
//   2) 自动触发——单表可合并聚合语句，行数估算 ≥ scan_rows（0=关闭自动触发，默认 100 万）。
// 用 Disable 而非 Enable 以便省略时默认开启（yaml bool 零值）。
type ParallelOpts struct {
	Disable  bool  `yaml:"disable"`  // true = 关闭并行查询
	Workers  int   `yaml:"workers"`  // 自动触发的并行度（0 = CPU 核数，上限 32）
	ScanRows int64 `yaml:"scan_rows"` // 自动触发阈值：估算行数 ≥ 此值才并行（0=关闭）
}

// AsyncOpts 异步查询引擎。
type AsyncOpts struct {
	Enable            bool   `yaml:"enable"`
	Workers           int    `yaml:"workers"`
	MetricsIntervalSec int   `yaml:"metrics_interval_sec"` // WHEN 采样周期，最低 10
	ResultTTLHours    int    `yaml:"result_ttl_hours"`   // 兼容字段（小时）
	ResultRetainSec   int    `yaml:"result_retain_sec"`   // 结果保留秒数，默认 60，到期自动删除
	JobsDir           string `yaml:"jobs_dir"`
	JobsDirMaxGB      int    `yaml:"jobs_dir_max_gb"`
	DefaultTimeoutSec int    `yaml:"default_timeout_sec"`
	CallbackMaxInlineKB int  `yaml:"callback_max_inline_kb"`
	NodejsPath        string `yaml:"nodejs_path"`  // 后处理 runtime（空=禁用 nodejs）
	PythonPath        string `yaml:"python_path"`  // 后处理 runtime（空=禁用 python）
	ScriptDir         string `yaml:"script_dir"`   // SCRIPT 白名单目录（必须位于其下）
}

// MaintOpts 维护策略：VACUUM（执行期间锁该库全部写入）。
type MaintOpts struct {
	NoHotVacuum bool `yaml:"no_hot_vacuum"` // true = 禁止运行中 VACUUM（仅 CLI，服务停止时）
}

// SecOpts 静态加密（adiantum VFS，4KiB 块加密，防外部程序直接读底层文件）。
type SecOpts struct {
	Encrypt    bool   `yaml:"encrypt"`    // 首次启用要求空目录或已有加密库
	Passphrase string `yaml:"passphrase"` // 启用时必填
}

type Auth struct {
	User          string `yaml:"user"`
	Password      string `yaml:"password"`
	ReadOnlyUser     string `yaml:"readonly_user"`     // 只读账号名（v1.0.9，与管理账号同等认证；空 = glacier_ro）
	ReadOnlyPassword string `yaml:"readonly_password"` // 只读账号密码（可管理面热改；空 = 禁用只读登录）
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
	ParallelQuery ParallelOpts `yaml:"parallel_query"`
	AsyncQuery    AsyncOpts    `yaml:"async_query"`
	Maintenance   MaintOpts    `yaml:"maintenance"`
	Security      SecOpts      `yaml:"security"`

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
			User:             "glacier",
			Password:         "iloveyou~",
			ReadOnlyUser:     "glacier_ro",
			ReadOnlyPassword: "iloveyou~",
		},
		MemoryLimitMB:  0,
		MaxConnections: 64,
		ParallelQuery: ParallelOpts{ScanRows: 1000000},
		Maintenance:   MaintOpts{},
		AsyncQuery: AsyncOpts{
			Workers:            2,
			MetricsIntervalSec: 30,
			ResultTTLHours:     24,
			DefaultTimeoutSec:  1800,
			CallbackMaxInlineKB: 64,
		},
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
	if c.Auth.ReadOnlyUser == "" {
		c.Auth.ReadOnlyUser = "glacier_ro"
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = 64
	}
	if c.AsyncQuery.MetricsIntervalSec > 0 && c.AsyncQuery.MetricsIntervalSec < 10 {
		c.AsyncQuery.MetricsIntervalSec = 10
	}
	if c.AsyncQuery.MetricsIntervalSec == 0 {
		c.AsyncQuery.MetricsIntervalSec = 30
	}
	if c.AsyncQuery.ResultTTLHours <= 0 {
		c.AsyncQuery.ResultTTLHours = 24
	}
	if c.AsyncQuery.ResultRetainSec <= 0 {
		c.AsyncQuery.ResultRetainSec = 60
	}
	if c.AsyncQuery.DefaultTimeoutSec <= 0 {
		c.AsyncQuery.DefaultTimeoutSec = 1800
	}
	if c.AsyncQuery.Workers <= 0 {
		c.AsyncQuery.Workers = 2
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

// IsReadOnlyUser 判断用户是否只读账号（v1.0.9：只读账号同等认证，
// 仅允许查询与查看；数据面写语句、事务开启与管理面变更命令一律 1290 拒绝）。
func (c *Config) IsReadOnlyUser(user string) bool {
	return c.Auth.ReadOnlyUser != "" && strings.EqualFold(user, c.Auth.ReadOnlyUser)
}
