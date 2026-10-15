// mysql-tcp-proxy-sqlite 入口：配置加载、后端初始化、启动监听。
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/proxy"
	"glaciertdb/internal/sqlite"
)

func main() {
	var (
		showVer  = flag.Bool("v", false, "print GlacierTDB version and exit")
		showVer2 = flag.Bool("version", false, "print GlacierTDB version and exit")
		configPath = flag.String("config", "/app/config.yaml", "path to config.yaml")
		dbDir      = flag.String("db-dir", "", "override sqlite db directory (one db = one .db file)")
		dbPath     = flag.String("db", "", "override sqlite db file path (legacy; dir=dirname, default db=basename)")
		listen     = flag.String("listen", "", "override listen address")
	)
	flag.Parse()
	if *showVer || *showVer2 {
		fmt.Printf("GlacierTDB %s (mysql protocol %s, %s/%s)\n",
			config.GlacierVersion, "8.99.999", runtime.GOOS, runtime.GOARCH)
		return
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		logx.Errorf("load config: %v", err)
		os.Exit(1)
	}
	if *dbDir != "" {
		cfg.DBPath = ""
		cfg.DBDir = *dbDir
		cfg.Fill()
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
		cfg.Fill()
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	logx.SetLevel(logx.ParseLevel(cfg.Log.Level))
	if cfg.Log.SQLLogFile != "" {
		if err := logx.InitSQLLog(cfg.Log.SQLLogFile); err != nil {
			logx.Errorf("init sql log %s: %v", cfg.Log.SQLLogFile, err)
		} else {
			logx.Infof("sql audit log: %s", cfg.Log.SQLLogFile)
		}
	}

	// 可配置内存限制：Go 运行时软上限（GOMEMLIMIT 同效），GC 更积极回收
	if cfg.MemoryLimitMB > 0 {
		debug.SetMemoryLimit(int64(cfg.MemoryLimitMB) << 20)
		logx.Infof("memory limit set to %d MB (soft, GOMEMLIMIT)", cfg.MemoryLimitMB)
	}

	logx.Infof("GlacierTDB %s starting: version=%s db_dir=%s default_db=%s sync=%s busy_timeout=%dms cache=%dKB",
		config.GlacierVersion, cfg.ServerVersion, cfg.DBDir, cfg.DefaultDB, cfg.SQLite.Synchronous,
		cfg.SQLite.BusyTimeoutMs, cfg.SQLite.CacheSizeKB)

	backend, err := sqlite.NewBackend(cfg)
	if err != nil {
		logx.Errorf("init sqlite backend: %v\nhint: db 所在目录必须可写（WAL 需创建 -wal/-shm 文件），且应挂载目录而非单个 db 文件", err)
		os.Exit(1)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logx.Infof("shutting down")
		os.Exit(0)
	}()

	if err := proxy.Run(cfg, backend); err != nil {
		logx.Errorf("listen %s: %v", cfg.Listen, err)
		os.Exit(1)
	}
}
