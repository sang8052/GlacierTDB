// Package proxy 负责 TCP 监听与连接生命周期。
package proxy

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"net"
	"regexp"
	"runtime/debug"
	"strconv"
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

	// mysql.enable=false：MySQL 面拒绝服务（RESP/AMQP/MQTT 不受影响），直接断开
	toMySQL := func() {
		if msrv == nil || !cfg.MySQL.Enable {
			logx.Debugf("mysql: disabled, close %s", remote)
			bc.Close()
			return
		}
		serveMySQL(msrv, bc, cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm, remote)
	}

	sniff := time.Duration(cfg.MuxSniffMs) * time.Millisecond
	if sniff <= 0 || (!cfg.KV.Enable) {
		// 纯 MySQL（嗅探关闭）
		toMySQL()
		return
	}
	_ = raw.SetReadDeadline(time.Now().Add(sniff))
	first, ferr := bc.br.Peek(1)
	_ = raw.SetReadDeadline(time.Time{})
	if ferr != nil || len(first) == 0 {
		// 窗口内无数据：客户端静默等握手 → MySQL
		toMySQL()
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
				toMySQL()
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
		toMySQL()
	case '*', '+', '-', ':', '$':
		serveRedis(bc, cfg, store, kvs, remote)
	case 'A', 'a':
		if head, err := bc.br.Peek(4); err == nil && string(head) == "AMQP" {
			serveRabbit(bc, cfg, store, mqs, remote)
			return
		}
		toMySQL()
	default:
		// MQTT CONNECT：byte1 高半字节 = 1（0x10..0x1F）
		if mqttOn && first[0] >= 0x10 && first[0] <= 0x1F {
			serveMQTT(bc, cfg, store, mqs, remote)
			return
		}
		toMySQL()
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

// unixAddrConn UDS 连接地址包装（v1.3.13 §5.1）：unix socket 的 RemoteAddr 不是
// IP（"@" / 空路径），统一映射为 127.0.0.1——主机授权（HostOK）、防爆破回环豁免、
// 审计展示与 TCP 本机连接语义完全一致。
type unixAddrConn struct {
	net.Conn
}

func (c unixAddrConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }
func (c unixAddrConn) LocalAddr() net.Addr  { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }

// anyToString KV 回源结果字符串化（nil → 空串；[]byte → 原文；数值 → 十进制）。
func anyToString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case []byte:
		return string(t)
	case string:
		return t
	case time.Time:
		return t.Format("2006-01-02 15:04:05")
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	default:
		return fmt.Sprint(t)
	}
}

// truncateForLog 日志截断。
func truncateForLog(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// newMySQLServer 构造 go-mysql 服务端：握手默认登录算法取配置（v1.1.1 支持
// caching_sha2_password / sha256_password），并加载 RSA 密钥供非 TLS 密码交换。
// tlsCfg 非空时握手报 CLIENT_SSL 能力位（标准 --ssl-mode 客户端协议内升级，v1.3.12）。
func newMySQLServer(cfg *config.Config, tlsCfg *tls.Config) *server.Server {
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
	srv := server.NewServer(cfg.ServerVersion, mysql.DEFAULT_COLLATION_ID, plugin, rsaKey, tlsCfg)
	logx.Infof("mysql auth plugin (handshake default): %s", plugin)
	if tlsCfg != nil {
		logx.Infof("mysql: native TLS upgrade advertised (CLIENT_SSL capability)")
	}
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
		asyncq.SetPrimary(aq) // 心跳上报读取异步积压（v1.3.13）
		if s3store != nil { // S3 管理面异步任务执行器（SUBMIT S3 ...，v1.3.11）
			aq.RegisterRunner("s3", func(j *asyncq.Job) ([]string, [][]any, error) {
				return s3store.RunSearchTask(j.Payload)
			})
		}
		logx.Infof("async query engine: jobs_dir=%s metrics_interval=%ds ttl=%dh", cfg.DBDir+"/jobs",
			cfg.AsyncQuery.MetricsIntervalSec, cfg.AsyncQuery.ResultTTLHours)
	}

	// KV SQL 读穿透执行器（v1.3.13 §3.2）：权限边界在此强制——透传查询只能触达
	// 同用户名下的 db（super 放行）+ 仅 SELECT；结果字符串化后交 kv 回填
	if kvs != nil {
		kvs.SetSourceExecutor(func(user, dbName, sql string) ([]string, [][]string, error) {
			up := strings.ToUpper(strings.TrimSpace(strings.TrimLeft(strings.TrimSpace(sql), "( \t\n")))
			if !strings.HasPrefix(up, "SELECT") {
				return nil, nil, fmt.Errorf("kv source: only SELECT statements are allowed (got %q)", truncateForLog(sql, 80))
			}
			if store != nil && store.UserType(user) != auth.TypeSuper && !store.HasDBGrant(user, dbName) {
				return nil, nil, fmt.Errorf("kv source: no privilege on database %s for user %s", dbName, user)
			}
			cols, rawRows, err := backend.QueryDBReadOnly(dbName, sql)
			if err != nil {
				return nil, nil, err
			}
			rows := make([][]string, 0, len(rawRows))
			for _, rr := range rawRows {
				out := make([]string, len(rr))
				for i, v := range rr {
					out[i] = anyToString(v)
				}
				rows = append(rows, out)
			}
			return cols, rows, nil
		})
	}

	var msrv *server.Server
	// TLS（v1.3.12）：3309 双模式接入 + MySQL 协议内升级 + MQTT 独立监听共用
	// 同一证书；证书缺失直接启动失败（fail-closed，不静默降级明文）
	tlsCfg, terr := LoadTLSConfig(cfg)
	if terr != nil {
		return fmt.Errorf("load tls config: %w", terr)
	}
	if cfg.MySQL.Enable {
		msrv = newMySQLServer(cfg, tlsCfg)
	} else {
		logx.Infof("mysql: protocol service disabled (mysql.enable=false)")
	}

	// 时序冷压缩后台策略（v1.3.3）
	handler.RunColdPolicy(backend, cfg)

	// MQTT 独立监听（可选；3309 复用始终可用）
	if cfg.MQ.Enable && cfg.MQ.MQTT.Enable && mqs != nil && cfg.MQ.MQTT.Listen != "" {
		go mqtt.ListenAndServe(cfg.MQ.MQTT.Listen, cfg.MQ.MQTT.WsPath, mqs, store, cfg, tlsCfg)
	}

	// mysql.enable=false 且 kv/mq 也关闭：3309 无任何复用服务，不占用端口
	if !cfg.MySQL.Enable && !cfg.KV.Enable && !cfg.MQ.Enable {
		logx.Warnf("3309 复用服务全部禁用（mysql/kv/mq），跳过监听；进程保持运行（S3 等独立端口服务不受影响）")
		select {} // 阻塞等待进程信号退出（main 的 signal handler 负责 os.Exit）
	}

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	logx.Infof("listening on %s (multiplex %s%s%s%s -> sqlite dir %s, default db=%s, user=%s)",
		cfg.Listen, map[bool]string{true: "mysql", false: ""}[cfg.MySQL.Enable],
		map[bool]string{true: "+redis", false: ""}[cfg.KV.Enable],
		map[bool]string{true: "+mqtt", false: ""}[cfg.MQ.Enable && cfg.MQ.MQTT.Enable],
		map[bool]string{true: "+tls", false: ""}[tlsCfg != nil],
		cfg.DBDir, cfg.DefaultDB, cfg.Auth.User)

	sniff := time.Duration(cfg.MuxSniffMs) * time.Millisecond
	if sniff <= 0 {
		sniff = time.Millisecond
	}
	// UDS 本地连接加速（v1.3.13 §5.1，仅 Linux）：同一协议多路复用器整体架在
	// Unix domain socket 上，鉴权照旧（各协议认证不变，纯传输加速）
	if cfg.ListenUnix != "" {
		if uln, uerr := listenUnix(cfg.ListenUnix); uerr != nil {
			return fmt.Errorf("listen_unix %s: %w", cfg.ListenUnix, uerr)
		} else {
			logx.Infof("listening on unix://%s (multiplex, auth required)", cfg.ListenUnix)
			go func() {
				for {
					c, err := uln.Accept()
					if err != nil {
						logx.Errorf("unix accept: %v", err)
						continue
					}
					go sniffConn(msrv, maybeTLSConn(unixAddrConn{Conn: c}, tlsCfg, sniff), cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm)
				}
			}()
		}
	}

	for {
		c, err := ln.Accept()
		if err != nil {
			logx.Errorf("accept: %v", err)
			continue
		}
		go sniffConn(msrv, maybeTLSConn(c, tlsCfg, sniff), cfg, backend, reg, aq, store, s3store, s3srv, kvs, mqs, cm)
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
