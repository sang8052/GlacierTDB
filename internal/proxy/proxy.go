// Package proxy 负责 TCP 监听与连接生命周期。
package proxy

import (
	"bufio"
	"net"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"

	"glaciertdb/internal/admin"
	"glaciertdb/internal/auth"
	"glaciertdb/internal/asyncq"
	"glaciertdb/internal/colmeta"
	"glaciertdb/internal/config"
	"glaciertdb/internal/handler"
	"glaciertdb/internal/kv"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/mq"
	"glaciertdb/internal/mqtt"
	"glaciertdb/internal/meta"
	"glaciertdb/internal/s3"
	"glaciertdb/internal/sqlite"
)

// ---------------------------------------------------------------------------
// 3309 端口复用（v1.2.0，FEATURE §0）：
//   声明行 `mode:<service>`（连接级一次性）优先；否则协议嗅探——
//   RESP 首字节（* + - : $）→ Redis；AMQP 魔数 → RabbitMQ；
//   MQTT CONNECT（0x1x）/ HTTP Upgrade → MQTT（v1.3.3）；
//   客户端静默等握手（嗅探窗口内无数据）→ MySQL（默认）。
// ---------------------------------------------------------------------------

var reModeLine = regexp.MustCompile(`(?i)^mode:(mysql|redis|rabbitmq|mqtt)\s*$`)

// bufferedConn Read 优先取嗅探缓冲（go-mysql/kv 复用同一条连接）。
type bufferedConn struct {
	net.Conn
	br *bufio.Reader
}

func (b *bufferedConn) Read(p []byte) (int, error) { return b.br.Read(p) }

// sniffConn 嗅探首包并分发；返回后 MySQL 路径继续用带缓冲的连接。
func sniffConn(msrv *server.Server, raw net.Conn, cfg *config.Config, backend *sqlite.Backend,
	reg *admin.Registry, aq *asyncq.Engine, store *auth.Store,
	s3store *s3.Store, s3srv *s3.Server, kvs *kv.Store, mqs *mq.Store, cm *colmeta.Store) {
	remote := raw.RemoteAddr().String()
	bc := &bufferedConn{Conn: raw, br: bufio.NewReader(raw)}

	mqttOn := cfg.MQ.Enable && cfg.MQ.MQTT.Enable && mqs != nil

	sniff := time.Duration(cfg.MuxSniffMs) * time.Millisecond
	if sniff <= 0 || (!cfg.KV.Enable) {
		// 纯 MySQL（嗅探关闭）
		serveMySQL(msrv, bc, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm, remote)
		return
	}
	_ = raw.SetReadDeadline(time.Now().Add(sniff))
	first, ferr := bc.br.Peek(1)
	_ = raw.SetReadDeadline(time.Time{})
	if ferr != nil || len(first) == 0 {
		// 窗口内无数据：客户端静默等握手 → MySQL
		serveMySQL(msrv, bc, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm, remote)
		return
	}
	// HTTP（WebSocket 升级）→ MQTT over WS
	if strings.ContainsRune("GgPpOoHhDd", rune(first[0])) {
		if line := peekLine(bc.br); mqtt.IsHTTPRequest(line) {
			if mqttOn {
				logx.Debugf("mqtt: serve %s (http upgrade)", remote)
				mqtt.ServeWSConn(bc, bc.br, cfg.MQ.MQTT.WsPath, mqs, store, cfg)
			} else {
				bc.Close()
			}
			return
		}
	}
	switch first[0] {
	case 'm', 'M':
		// 可能是 mode:<service> 声明行；读到行尾判断
		line := peekLine(bc.br)
		if m := reModeLine.FindStringSubmatch(line); m != nil {
			consumeModeLine(bc.br) // 声明行消费掉（连接级一次性）
			switch strings.ToLower(m[1]) {
			case "mysql":
				serveMySQL(msrv, bc, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm, remote)
			case "redis":
				serveRedis(bc, cfg, store, kvs, remote)
			case "rabbitmq":
				serveRabbit(bc, cfg, store, mqs, remote)
			case "mqtt":
				serveMQTT(bc, cfg, store, mqs, remote)
			}
			return
		}
		// 非声明行：原样交 MySQL
		serveMySQL(msrv, bc, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm, remote)
	case '*', '+', '-', ':', '$':
		serveRedis(bc, cfg, store, kvs, remote)
	case 'A', 'a':
		if head, err := bc.br.Peek(4); err == nil && string(head) == "AMQP" {
			serveRabbit(bc, cfg, store, mqs, remote)
			return
		}
		serveMySQL(msrv, bc, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm, remote)
	default:
		// MQTT CONNECT：byte1 高半字节 = 1（0x10..0x1F）
		if mqttOn && first[0] >= 0x10 && first[0] <= 0x1F {
			serveMQTT(bc, cfg, store, mqs, remote)
			return
		}
		serveMySQL(msrv, bc, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm, remote)
	}
}

// peekLine 读取一行但保留缓冲（不消费语义：mode 声明行由调用方确认后丢弃）。
func peekLine(br *bufio.Reader) string {
	// 用 Peek 累计到行尾（单行上限 256 字节）
	for n := 1; n <= 256; n++ {
		buf, err := br.Peek(n)
		if len(buf) > 0 && buf[len(buf)-1] == '\n' {
			return strings.TrimSpace(string(buf))
		}
		if err != nil {
			return strings.TrimSpace(string(buf))
		}
	}
	buf, _ := br.Peek(256)
	return strings.TrimSpace(string(buf))
}

// consumeModeLine 丢弃已消费的声明行（mode:* 分发后调用）。
func consumeModeLine(br *bufio.Reader) {
	_, _ = br.ReadString('\n')
}

// serveRedis 进入 RESP 协议栈（v1.2.0）。
func serveRedis(bc *bufferedConn, cfg *config.Config, store *auth.Store, kvs *kv.Store, remote string) {
	if kvs == nil {
		bc.Close()
		return
	}
	consumeModeIfDeclared(bc.br)
	logx.Debugf("kv: serve %s (resp)", remote)
	kv.ServeConn(bc, kvs, store, cfg)
}

// consumeModeIfDeclared 若首行是 mode:redis 声明则消费掉。
func consumeModeIfDeclared(br *bufio.Reader) {
	if line := peekLine(br); reModeLine.MatchString(line) {
		_, _ = br.ReadString('\n')
	}
}

// serveRabbit 进入 AMQP 0-9-1 协议栈（v1.3.0）。
func serveRabbit(bc *bufferedConn, cfg *config.Config, store *auth.Store, mqs *mq.Store, remote string) {
	if mqs == nil {
		bc.Close()
		return
	}
	logx.Debugf("mq: serve %s (amqp)", remote)
	mq.ServeConn(bc, mqs, store, cfg)
}

// serveMQTT 进入 MQTT 协议栈（v1.3.3，tcp 直连；ws 在嗅探层已握手）。
func serveMQTT(bc *bufferedConn, cfg *config.Config, store *auth.Store, mqs *mq.Store, remote string) {
	if mqs == nil {
		bc.Close()
		return
	}
	logx.Debugf("mqtt: serve %s (tcp)", remote)
	mqtt.ServeConn(bc, mqs, store, cfg)
}

// serveMySQL MySQL 协议路径（原有逻辑）。
func serveMySQL(srv *server.Server, raw net.Conn, cfg *config.Config, backend *sqlite.Backend,
	reg *admin.Registry, aq *asyncq.Engine, store *auth.Store,
	s3store *s3.Store, s3srv *s3.Server, kvs *kv.Store, mqs *mq.Store, cm *colmeta.Store, remote string) {
	h, err := handler.New(backend, cfg, remote, cfg.DefaultDB, reg, aq, store, s3store, s3srv, kvs, mqs, cm)
	if err != nil {
		logx.Errorf("open sqlite session for %s: %v", remote, err)
		raw.Close()
		return
	}
	defer h.Close()
	localAddr := raw.LocalAddr().String()
	localAddrSeen.Store(localAddr, struct{}{})
	h.SetAuthCacheInvalidator(func(u string) {
		localAddrSeen.Range(func(k, _ any) bool {
			srv.InvalidateCache(u, k.(string))
			return true
		})
	})
	authHandler := auth.NewHandler(store, remote)
	mc, err := srv.NewCustomizedConn(&handshakeStatusConn{Conn: raw}, authHandler, h)
	if err != nil {
		logx.Debugf("handshake with %s failed: %v", remote, err)
		return
	}
	h.BindConn(mc)
	defer func() {
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

// Run 阻塞监听并服务连接（s3store / s3srv / kvs / mqs / cm 可为 nil：对应模块关闭）。
func Run(cfg *config.Config, backend *sqlite.Backend, store *auth.Store, s3store *s3.Store, s3srv *s3.Server, kvs *kv.Store, mqs *mq.Store, cm *colmeta.Store) error {
	meta.MapSQLError = handler.MapSQLiteErr
	if cm != nil {
		meta.ColumnTypeLookuper = handler.LookupColumnTypeHook(cm)
	}

	reg := admin.NewRegistry()
	var aq *asyncq.Engine
	if cfg.AsyncQuery.Enable {
		aq = asyncq.NewEngine(backend, cfg)
		logx.Infof("async query engine: jobs_dir=%s metrics_interval=%ds ttl=%dh", cfg.DBDir+"/jobs",
			cfg.AsyncQuery.MetricsIntervalSec, cfg.AsyncQuery.ResultTTLHours)
	}

	msrv := newMySQLServer(cfg)

	// 时序冷压缩后台策略（v1.3.3）
	handler.RunColdPolicy(backend, cfg)

	// MQTT 独立监听（可选；3309 复用始终可用）
	if cfg.MQ.Enable && cfg.MQ.MQTT.Enable && mqs != nil && cfg.MQ.MQTT.Listen != "" {
		go mqtt.ListenAndServe(cfg.MQ.MQTT.Listen, cfg.MQ.MQTT.WsPath, mqs, store, cfg)
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	logx.Infof("listening on %s (multiplex mysql%s%s -> sqlite dir %s, default db=%s, user=%s)",
		cfg.Listen, map[bool]string{true: "+redis", false: ""}[cfg.KV.Enable],
		map[bool]string{true: "+mqtt", false: ""}[cfg.MQ.Enable && cfg.MQ.MQTT.Enable],
		cfg.DBDir, cfg.DefaultDB, cfg.Auth.User)

	for {
		c, err := ln.Accept()
		if err != nil {
			logx.Errorf("accept: %v", err)
			continue
		}
		go sniffConn(msrv, c, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm)
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
