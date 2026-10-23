// Package proxy 负责 TCP 监听与连接生命周期。
package proxy

import (
	"net"
	"runtime/debug"
	"sync"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"glaciertdb/internal/admin"
	"glaciertdb/internal/auth"
	"glaciertdb/internal/asyncq"
	"glaciertdb/internal/config"
	"glaciertdb/internal/handler"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/s3"
	"glaciertdb/internal/sqlite"
)

// localAddrSeen 已观察到的服务端本地地址（caching_sha2 缓存 key 的 host 部分）。
var localAddrSeen sync.Map

// newMySQLServer 构造 go-mysql 服务端：握手默认登录算法取配置（v1.1.1 支持
// caching_sha2_password / sha256_password），并加载 RSA 密钥供非 TLS 密码交换。
func newMySQLServer(cfg *config.Config) *server.Server {
	plugin := cfg.Auth.AuthPlugin
	if plugin == "" {
		plugin = mysql.AUTH_NATIVE_PASSWORD
	}
	rsaKey, err := auth.LoadOrGenerateRSAKey(cfg.DBDir)
	if err != nil {
		logx.Warnf("load rsa key failed: %v（caching_sha2/sha256 非 TLS 登录将不可用，已回退 native）", err)
		plugin = mysql.AUTH_NATIVE_PASSWORD
	}
	if rsaKey == nil && plugin != mysql.AUTH_NATIVE_PASSWORD {
		logx.Warnf("no rsa key available: fallback to %s", mysql.AUTH_NATIVE_PASSWORD)
		plugin = mysql.AUTH_NATIVE_PASSWORD
	}
	srv := server.NewServer(cfg.ServerVersion, mysql.DEFAULT_COLLATION_ID, plugin, rsaKey, nil)
	logx.Infof("mysql auth plugin (handshake default): %s", plugin)
	return srv
}

// Run 阻塞监听并服务连接（s3store / s3srv 可为 nil：对象存储模块关闭）。
func Run(cfg *config.Config, backend *sqlite.Backend, store *auth.Store, s3store *s3.Store, s3srv *s3.Server) error {
	meta.MapSQLError = handler.MapSQLiteErr

	reg := admin.NewRegistry()
	var aq *asyncq.Engine
	if cfg.AsyncQuery.Enable {
		aq = asyncq.NewEngine(backend, cfg)
		logx.Infof("async query engine: jobs_dir=%s metrics_interval=%ds ttl=%dh", cfg.DBDir+"/jobs",
			cfg.AsyncQuery.MetricsIntervalSec, cfg.AsyncQuery.ResultTTLHours)
	}

	srv := newMySQLServer(cfg)

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	logx.Infof("listening on %s (mysql protocol -> sqlite dir %s, default db=%s, user=%s)",
		cfg.Listen, cfg.DBDir, cfg.DefaultDB, cfg.Auth.User)

	for {
		c, err := ln.Accept()
		if err != nil {
			logx.Errorf("accept: %v", err)
			continue
		}
		go handleConn(srv, c, cfg, backend, reg, aq, store, s3store, s3srv)
	}
}

func handleConn(srv *server.Server, raw net.Conn, cfg *config.Config, backend *sqlite.Backend,
	reg *admin.Registry, aq *asyncq.Engine, store *auth.Store, s3store *s3.Store, s3srv *s3.Server) {
	remote := raw.RemoteAddr().String()
	logx.Debugf("new connection from %s", remote)
	// 初始握手包的 status 位默认 0（无 AUTOCOMMIT），会导致 pymysql/SQLAlchemy
	// 误判 autocommit=0 而不发 SET AUTOCOMMIT=0，事务语义错乱；此处改写首包补上该位
	c := &handshakeStatusConn{Conn: raw}

	// 连接数上限保护（超限直接断开，防内存失控）
	if reg.Count() >= cfg.MaxConnections {
		logx.Warnf("reject connection from %s: max_connections=%d reached", remote, cfg.MaxConnections)
		raw.Close()
		return
	}

	h, err := handler.New(backend, cfg, remote, cfg.DefaultDB, reg, aq, store, s3store, s3srv)
	if err != nil {
		logx.Errorf("open sqlite session for %s: %v", remote, err)
		raw.Close()
		return
	}
	defer h.Close()

	// caching_sha2_password 缓存失效钩子：改密后按（user@LocalAddr）精确失效
	//（go-mysql 缓存 key 含连接本地地址，需遍历已观察到的地址）
	localAddr := raw.LocalAddr().String()
	localAddrSeen.Store(localAddr, struct{}{})
	h.SetAuthCacheInvalidator(func(u string) {
		localAddrSeen.Range(func(k, _ any) bool {
			srv.InvalidateCache(u, k.(string))
			return true
		})
	})

	// 认证走用户权限系统（v1.0.10）：每连接一个 Handler（捕获来源 IP），
	// GetCredential 阶段完成 IP/账号锁定与 host 授权检查，失败/成功落审计并防爆破计数
	authHandler := auth.NewHandler(store, remote)
	mc, err := srv.NewCustomizedConn(c, authHandler, h)
	if err != nil {
		logx.Debugf("handshake with %s failed: %v", remote, err)
		return
	}
	h.BindConn(mc)
	defer func() {
		// HandleCommand 出错路径会置 c.Conn=nil 并自行 Close，二次 Close 会 panic，需防护
		if r := recover(); r != nil {
			logx.Debugf("connection %s panic recovered: %v\n%s", remote, r, debug.Stack())
		}
		if !mc.Closed() {
			mc.Close()
		}
	}()

	for {
		if err := mc.HandleCommand(); err != nil {
			logx.Debugf("connection %s closed: %v", remote, err)
			return
		}
	}
}

// handshakeStatusConn 在首个 server->client 写包（初始握手）的 status 字段上
// 补 SERVER_STATUS_AUTOCOMMIT（0x0002），与真 MySQL 默认 autocommit=1 一致；
// pymysql 据此决定是否需要下发 SET AUTOCOMMIT=0（影响 SQLAlchemy 事务语义）。
type handshakeStatusConn struct {
	net.Conn
	done bool
}

func (w *handshakeStatusConn) Write(b []byte) (int, error) {
	if !w.done && len(b) > 32 {
		w.done = true
		// 包布局（含 4 字节头）：len(3) seq(1) proto(1) version(nul) threadid(4) salt(8) nul(1) cap_low(2) charset(1) status(2)
		vEnd := -1
		for i := 5; i < len(b); i++ {
			if b[i] == 0 {
				vEnd = i
				break
			}
		}
		if vEnd > 0 {
			statusPos := vEnd + 1 + 4 + 8 + 1 + 2 + 1 // status 低字节绝对偏移
			if statusPos+1 < len(b) {
				b[statusPos] |= 0x02 // SERVER_STATUS_AUTOCOMMIT
			}
		}
	}
	return w.Conn.Write(b)
}
