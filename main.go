// mysql-tcp-proxy-sqlite 入口：配置加载、后端初始化、启动监听。
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"
	"path/filepath"
	"strings"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/dirlock"
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
		forceUnlock = flag.Bool("force-unlock", false, "db 目录锁被异常占用时强制解锁启动（拒绝抢活进程的锁）")
		vacuumMode  = flag.Bool("vacuum", false, "CLI 真空模式：VACUUM 库目录（服务必须停止；与目录锁互斥）")
		vacuumDB    = flag.String("vacuum-db", "", "仅 VACUUM 指定库（默认目录内全部库）")
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

	// db 目录唯一锁：服务与 CLI（-vacuum）互斥，同一目录仅一个进程
	lock, lerr := dirlock.Acquire(cfg.DBDir, *forceUnlock)
	if lerr != nil {
		logx.Errorf("%v", lerr)
		os.Exit(1)
	}
	defer lock.Release()
	logx.Infof("db dir lock acquired: %s", cfg.DBDir)

	if *vacuumMode {
		// CLI 真空模式：VACUUM 释放空间；执行时服务必然未运行（目录锁保证）
		var dbNames []string
		if *vacuumDB != "" {
			dbNames = []string{*vacuumDB}
		} else if gs, _ := filepath.Glob(filepath.Join(cfg.DBDir, "*.db")); len(gs) > 0 {
			for _, g := range gs {
				dbNames = append(dbNames, strings.TrimSuffix(filepath.Base(g), ".db"))
			}
		}
		for _, db := range dbNames {
			cost, sizeBefore, sizeAfter, err := backendVacuum(cfg, db)
			if err != nil {
				logx.Errorf("VACUUM %s 失败: %v", db, err)
				os.Exit(1)
			}
			logx.Infof("VACUUM %s 完成：耗时 %s（%d → %d 字节，执行期间该库写入被阻塞）", db, cost, sizeBefore, sizeAfter)
		}
		logx.Infof("vacuum 完成，退出")
		return
	}

	backend, err := sqlite.NewBackend(cfg)
	if err != nil {
		logx.Errorf("init sqlite backend: %v\nhint: db 所在目录必须可写（WAL 需创建 -wal/-shm 文件），且应挂载目录而非单个 db 文件", err)
		os.Exit(1)
	}

	// 用户权限系统（v1.0.10）：系统库表存用户，首次启动自动迁移旧版配置/glacier.json 策略
	authDB, err := backend.OpenSystemDB()
	if err != nil {
		logx.Errorf("open auth system db: %v", err)
		os.Exit(1)
	}
	authStore, err := auth.Open(authDB, auth.Bootstrap{
		AdminUser:          cfg.Auth.User,
		AdminPassword:      backend.GetAuthPassword(),
		ReadonlyUser:       cfg.Auth.ReadOnlyUser,
		ReadonlyPassword:   backend.GetReadonlyPassword(),
	}, cfg.Lock)
	if err != nil {
		logx.Errorf("init auth store: %v", err)
		os.Exit(1)
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logx.Infof("shutting down")
		os.Exit(0)
	}()

	if err := proxy.Run(cfg, backend, authStore); err != nil {
		logx.Errorf("listen %s: %v", cfg.Listen, err)
		os.Exit(1)
	}
}

// backendVacuum CLI 真空（独立于服务进程的轻量后端：仅做 VACUUM）。
func backendVacuum(cfg *config.Config, db string) (time.Duration, int64, int64, error) {
	b, err := sqlite.NewBackend(cfg)
	if err != nil {
		return 0, 0, 0, err
	}
	defer b.CloseAll()
	return b.VacuumDB(db)
}
