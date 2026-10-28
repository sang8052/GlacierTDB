// 3309 复用端口与 MQTT 独立监听的 TLS（v1.3.12）：
//   - 双模式同端口：首字节 0x16（TLS ClientHello）→ TLS 优先客户端
//     （redis-cli --tls / amqps:// / mqtts / wss），其余按明文协议嗅探——
//     明文与 TLS 客户端可共存一个端口；
//   - MySQL 另有协议内升级路径（server.NewServer 携带 tls.Config 后握手报
//     CLIENT_SSL 能力位，--ssl-mode=REQUIRED 等标准客户端直连可用）。
package proxy

import (
	"bufio"
	"crypto/tls"
	"net"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// LoadTLSConfig 读取配置构建服务端 TLS（tls.enable=false 返回 nil）。
// 证书缺失/不可用直接报错——不静默降级为明文（fail-closed）。
func LoadTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if !cfg.TLS.Enable {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, err
	}
	min := uint16(tls.VersionTLS12)
	if cfg.TLS.MinVersion == "1.3" {
		min = tls.VersionTLS13
	}
	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   min,
	}
	mv := cfg.TLS.MinVersion
	if mv == "" {
		mv = "1.2"
	}
	logx.Infof("tls: enabled on 3309 multiplex + mqtt (cert=%s, min=%s)",
		cfg.TLS.CertFile, mv)
	return tc, nil
}

// peekReplayConn 已预读首字节的连接（Read 优先取嗅探缓冲，字节不丢失）。
type peekReplayConn struct {
	net.Conn
	br *bufio.Reader
}

func (c *peekReplayConn) Read(p []byte) (int, error) { return c.br.Read(p) }

// maybeTLSConn 双模式 TLS 接入（tc=nil 原样返回）：peek 首字节，
// 0x16 = TLS ClientHello → 包装 tls.Server；其余（含超时静默的 MySQL 客户端）
// 原样交回（已 peek 的字节经缓冲回放，不丢失）。
func maybeTLSConn(c net.Conn, tc *tls.Config, sniff time.Duration) net.Conn {
	if tc == nil {
		return c
	}
	br := bufio.NewReader(c)
	_ = c.SetReadDeadline(time.Now().Add(sniff))
	first, err := br.Peek(1)
	_ = c.SetReadDeadline(time.Time{})
	if err == nil && len(first) == 1 && first[0] == 0x16 {
		return tls.Server(&peekReplayConn{Conn: c, br: br}, tc)
	}
	return &peekReplayConn{Conn: c, br: br}
}
