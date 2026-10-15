// Package proxy 负责 TCP 监听与连接生命周期。
package proxy

import (
	"net"
	"runtime/debug"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"glaciertdb/internal/admin"
	"glaciertdb/internal/asyncq"
	"glaciertdb/internal/config"
	"glaciertdb/internal/handler"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/sqlite"
)

// Run 阻塞监听并服务连接。
func Run(cfg *config.Config, backend *sqlite.Backend) error {
	meta.MapSQLError = handler.MapSQLiteErr

	reg := admin.NewRegistry()
	var aq *asyncq.Engine
	if cfg.AsyncQuery.Enable {
		aq = asyncq.NewEngine(backend, cfg)
		logx.Infof("async query engine: jobs_dir=%s metrics_interval=%ds ttl=%dh", cfg.DBDir+"/jobs",
			cfg.AsyncQuery.MetricsIntervalSec, cfg.AsyncQuery.ResultTTLHours)
	}

	srv := server.NewServer(cfg.ServerVersion, mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)

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
		go handleConn(srv, c, cfg, backend, reg, aq)
	}
}

func handleConn(srv *server.Server, raw net.Conn, cfg *config.Config, backend *sqlite.Backend, reg *admin.Registry, aq *asyncq.Engine) {
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

	h, err := handler.New(backend, cfg, remote, cfg.DefaultDB, reg, aq)
	if err != nil {
		logx.Errorf("open sqlite session for %s: %v", remote, err)
		raw.Close()
		return
	}
	defer h.Close()

	// 显式使用 mysql_native_password（NewConn 的默认 handler 会强制 caching_sha2）
	authHandler := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if err := authHandler.AddUser(cfg.Auth.User, backend.GetAuthPassword()); err != nil {
		logx.Errorf("add auth user: %v", err)
		raw.Close()
		return
	}
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
