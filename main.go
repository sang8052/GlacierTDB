// mysql-tcp-proxy-sqlite 入口：配置加载、后端初始化、启动监听。
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"
	"path/filepath"
	"strings"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/colmeta"
	"glaciertdb/internal/dirlock"
	"glaciertdb/internal/kv"
	"glaciertdb/internal/mq"
	"glaciertdb/internal/s3"
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
		backupDir   = flag.String("backup", "", "CLI 在线备份：全部库 VACUUM INTO 目标目录（服务运行中可用；v1.3.12）")
		backupChk   = flag.Bool("backup-verify", true, "备份产物 integrity_check + 对象数校验（默认开）")
		unlockUser  = flag.String("unlock-user", "", "CLI 解锁账号（清除自动锁/手动锁/失败计数；服务运行中可用）")
		unlockIP    = flag.String("unlock-ip", "", "CLI 解锁 IP（清除自动锁/失败计数/防火墙封锁）")
		showLocks   = flag.Bool("show-locks", false, "CLI 查看当前防爆破锁定列表（账号 + IP）")
		configPath = flag.String("config", "config.yaml", "path to config.yaml（默认依次查找 ./config.yaml、exe 同目录、/app/config.yaml）")
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

	// 默认配置路径解析链：显式 -config 优先原样使用；未显式传参时按
	// ./config.yaml → exe 同目录/config.yaml → /app/config.yaml（Docker 惯例）回退
	cfgFile := resolveConfigPath(*configPath)
	cfg, err := config.Load(cfgFile)
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

	// CLI 在线备份模式（v1.3.12 P0-1）：不抢目录锁（WAL 多进程读安全），
	// 全部库 VACUUM INTO 快照单文件；全部成功后原子落位目标目录
	if *backupDir != "" {
		cliBackup(cfg, *backupDir, *backupChk)
		return
	}

	// CLI 解锁 / 查锁模式：旁路 dirlock（服务运行中可用），直连 glacier_auth 系统库
	// 做小事务（WAL 多进程并发安全），完成后退出、不启动任何监听。
	if *unlockUser != "" || *unlockIP != "" || *showLocks {
		cliAuthAdmin(cfg, *unlockUser, *unlockIP, *showLocks)
		return
	}

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

	logx.Infof("GlacierTDB %s starting: version=%s db_dir=%s default_db=%s sync=%s busy_timeout=%dms cache=%dKB wal_autocheckpoint=%d journal_size_limit=%dKB tls=%v",
		config.GlacierVersion, cfg.ServerVersion, cfg.DBDir, cfg.DefaultDB, cfg.SQLite.Synchronous,
		cfg.SQLite.BusyTimeoutMs, cfg.SQLite.CacheSizeKB,
		walCheckpointDisplay(cfg.SQLite.WalAutocheckpoint), cfg.SQLite.JournalSizeLimitKB, cfg.TLS.Enable)

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
		} else if gs, _ := filepath.Glob(filepath.Join(cfg.DBDir, "user", "*.db")); len(gs) > 0 {
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

	// 对象存储（v1.1.0，可选模块）：s3.enable 开启时启动独立 HTTP(S) 监听器
	//（默认 0.0.0.0:9000，AWS Signature V4；管理面 SQL 同步可用）
	var s3store *s3.Store
	var s3srv *s3.Server
	if cfg.S3.Enable {
		s3store, err = s3.Open(backend, cfg)
		if err != nil {
			logx.Errorf("init s3 store: %v", err)
			os.Exit(1)
		}
		s3srv = s3.NewServer(cfg, s3store, authStore)
		if err := s3srv.Start(); err != nil {
			logx.Errorf("start s3 server: %v", err)
			os.Exit(1)
		}
		logx.Infof("s3 object storage enabled: region=%q endpoints=%d", cfg.S3.Region, len(cfg.S3.Endpoints))
	}

	// 键值/队列（v1.2.0，mode:redis 同端口 RESP 服务）
	var kvstore *kv.Store
	if cfg.KV.Enable {
		kvstore, err = kv.Open(backend, cfg)
		if err != nil {
			logx.Errorf("init kv store: %v", err)
			os.Exit(1)
		}
		logx.Infof("kv: redis protocol enabled on %s (mode:redis / RESP sniff), max_memory_mb=%d eviction=%s",
			cfg.Listen, cfg.KV.MaxMemoryMB, cfg.KV.Eviction)
	}

	// 消息队列（v1.3.0，mode:rabbitmq 同端口 AMQP 0-9-1；MQTT 桥接 v1.3.3）
	var mqstore *mq.Store
	if cfg.MQ.Enable {
		mqstore, err = mq.Open(backend, cfg.MQ.MaxMessageMB)
		if err != nil {
			logx.Errorf("init mq store: %v", err)
			os.Exit(1)
		}
		logx.Infof("mq: amqp protocol enabled on %s (mode:rabbitmq / AMQP sniff, server_version=%s)",
			cfg.Listen, cfg.MQ.ServerVersion)
		if cfg.MQ.MQTT.Enable {
			logx.Infof("mqtt: enabled on %s (tcp sniff 0x1x / ws %s / mode:mqtt)%s",
				cfg.Listen, cfg.MQ.MQTT.WsPath, map[bool]string{true: " + standalone " + cfg.MQ.MQTT.Listen, false: ""}[cfg.MQ.MQTT.Listen != ""])
		}
	}

	// 列类型映射系统库（v1.3.3：每库每表「列 → MySQL 原始类型」，存取自动还原）
	metaDB, err := backend.OpenMetaDB()
	if err != nil {
		logx.Errorf("open colmeta system db: %v", err)
		os.Exit(1)
	}
	cmeta, err := colmeta.Open(metaDB)
	if err != nil {
		logx.Errorf("init colmeta store: %v", err)
		os.Exit(1)
	}
	if cfg.Timeseries.ColdAfterDays > 0 {
		logx.Infof("timeseries: cold policy enabled (after %d days, scan every %ds)",
			cfg.Timeseries.ColdAfterDays, cfg.Timeseries.ColdScanMinSec)
	}

	// 看门狗（v1.3.12 P0-4）：systemd Type=notify + WatchdogSec 部署下，
	// 启动就绪发 READY=1，之后周期心跳 WATCHDOG=1——心跳 = 一次 SQLite
	// SELECT 1（探真依赖，探活要探存储层）；NOTIFY_SOCKET 未设置时全部空转
	sdNotify("READY=1")
	go watchdogLoop(backend)

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logx.Infof("shutting down")
		sdNotify("STOPPING=1")
		os.Exit(0)
	}()

	if err := proxy.Run(cfg, backend, authStore, s3store, s3srv, kvstore, mqstore, cmeta); err != nil {
		logx.Errorf("listen %s: %v", cfg.Listen, err)
		sdNotify("STOPPING=1")
		os.Exit(1)
	}
}

// walCheckpointDisplay 调优项展示值（0 = SQLite 默认 1000）。
func walCheckpointDisplay(v int) int {
	if v == 0 {
		return 1000
	}
	return v
}

// ---------------------------------------------------------------------------
// 在线备份 CLI（v1.3.12 P0-1）
// ---------------------------------------------------------------------------

func cliBackup(cfg *config.Config, target string, verify bool) {
	start := time.Now()
	res, err := sqlite.BackupAll(cfg, target, verify)
	if err != nil {
		logx.Errorf("backup failed: %v（半成品临时目录已被清理，不影响现有备份）", err)
		os.Exit(1)
	}
	var total int64
	for _, r := range res {
		total += r.Size
		fmt.Printf("%-24s %10d bytes  %s\n", r.File, r.Size, r.Cost.Round(time.Millisecond))
	}
	fmt.Printf("backup done: %d dbs, %d bytes total, cost %s -> %s\n",
		len(res), total, time.Since(start).Round(time.Millisecond), target)
}

// ---------------------------------------------------------------------------
// systemd 看门狗（sd_notify 最小实现，v1.3.12 P0-4）
// ---------------------------------------------------------------------------

// sdNotify 向 NOTIFY_SOCKET 写一条状态（未设置/发送失败静默——裸跑无 systemd）。
func sdNotify(state string) {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return
	}
	if !strings.HasPrefix(sock, "@") {
		c, err := net.DialTimeout("unixgram", sock, time.Second)
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write([]byte(state))
		return
	}
	// @ 开头的抽象命名空间 socket（Linux）：以 NUL 打头地址
	c, err := net.DialTimeout("unixgram", "\x00"+sock[1:], time.Second)
	if err != nil {
		return
	}
	defer c.Close()
	_, _ = c.Write([]byte(state))
}

// watchdogLoop 心跳：每 10 秒对系统库做一次 SELECT 1，成功即 WATCHDOG=1。
// WatchdogSec 建议 30（心跳间隔的一半）——探活覆盖存储层真实可用性。
func watchdogLoop(backend *sqlite.Backend) {
	sock := os.Getenv("NOTIFY_SOCKET")
	watchdogUsec := os.Getenv("WATCHDOG_USEC")
	if sock == "" || watchdogUsec == "" {
		return // 非 systemd notify 部署：空转
	}
	iv := 10 * time.Second
	if usec, err := strconv.ParseInt(watchdogUsec, 10, 64); err == nil && usec > 0 {
		// 心跳间隔 = WatchdogSec / 2，下限 2s、上限 30s
		half := time.Duration(usec/2) * time.Microsecond
		if half < 2*time.Second {
			half = 2 * time.Second
		}
		if half > 30*time.Second {
			half = 30 * time.Second
		}
		iv = half
	}
	db, err := backend.OpenSystemDB()
	if err != nil {
		return
	}
	defer db.Close()
	t := time.NewTicker(iv)
	defer t.Stop()
	for range t.C {
		if _, err := db.Exec("SELECT 1"); err != nil {
			continue // 存储层异常：停发心跳，等 systemd WatchdogSec 到期重启
		}
		sdNotify("WATCHDOG=1")
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

// resolveConfigPath 默认配置路径解析链。显式传参（flag.Visit 可辨）原样使用；
// 默认值时若 ./config.yaml 不存在，依次回退 exe 同目录、/app/config.yaml。
func resolveConfigPath(given string) string {
	explicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "config" {
			explicit = true
		}
	})
	if explicit {
		return given
	}
	if _, err := os.Stat(given); err == nil {
		return given
	}
	if exe, err := os.Executable(); err == nil {
		cand := filepath.Join(filepath.Dir(exe), "config.yaml")
		if _, err := os.Stat(cand); err == nil {
			logx.Infof("config: %s 不存在，使用可执行文件同目录 %s", given, cand)
			return cand
		}
	}
	if _, err := os.Stat("/app/config.yaml"); err == nil {
		logx.Infof("config: %s 不存在，回退 /app/config.yaml", given)
		return "/app/config.yaml"
	}
	return given
}

// cliAuthAdmin CLI 解锁 / 查锁：直连 glacier_auth 系统库（不建默认库、不自检、
// 不抢目录锁），与运行中的服务进程通过 WAL 并发安全地做几行小事务。
func cliAuthAdmin(cfg *config.Config, unlockUser, unlockIP string, showLocks bool) {
	db, err := sqlite.OpenAuthDBDirect(cfg)
	if err != nil {
		logx.Errorf("open auth db: %v", err)
		os.Exit(1)
	}
	defer db.Close()
	store, err := auth.Open(db, auth.Bootstrap{
		AdminUser:    cfg.Auth.User,
		AdminPassword: cfg.Auth.Password,
	}, cfg.Lock)
	if err != nil {
		logx.Errorf("init auth store: %v", err)
		os.Exit(1)
	}
	if unlockUser != "" {
		if err := store.UnlockAccount(unlockUser); err != nil {
			logx.Errorf("unlock account %s 失败: %v", unlockUser, err)
			os.Exit(1)
		}
		fmt.Printf("账号 %s 已解锁（自动锁/手动锁/失败计数已清除）\n", unlockUser)
	}
	if unlockIP != "" {
		if err := store.UnlockIP(unlockIP); err != nil {
			logx.Errorf("unlock ip %s 失败: %v", unlockIP, err)
			os.Exit(1)
		}
		fmt.Printf("IP %s 已解锁（自动锁/失败计数/防火墙封锁已清除）\n", unlockIP)
	}
	locks := store.Locks()
	if len(locks) == 0 {
		fmt.Println("当前无防爆破锁定")
	} else {
		fmt.Printf("当前锁定 %d 项：\n", len(locks))
		for _, r := range locks {
			fmt.Printf("  %-8s %-24s 锁定于 %s  到期 %s（第 %d 级，失败 %d 次）\n",
				r.LockType, r.LockKey, r.LockedAt, r.UntilTS, r.Level, r.FailCount)
		}
	}
	if !showLocks {
		return
	}
}
