// Package mqtt — WebSocket 传输（v1.3.3）：RFC 6455 服务端握手 + 帧编解码，
// MQTT 包以二进制帧载荷承载（paho/mqtt.js 等 ws 客户端的标准形态）。
// 同端口 3309 上 HTTP Upgrade 与独立 mqtt.listen 监听均走本路径。
package mqtt

import (
	"bufio"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/mq"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsConn 把 WebSocket 帧流封装成 net.Conn（读：帧 → 连续字节流；写：二进制帧）。
type wsConn struct {
	net.Conn            // 底层传输（写与 deadline 委托）
	br    *bufio.Reader // 帧读取缓冲（复用嗅探阶段的缓冲）
	rmu   sync.Mutex
	wmu   sync.Mutex
	rbuf  []byte // 已解包未消费的字节
	closed bool
}

// HandshakeWS 读 HTTP 升级请求并回 101；path 为空表示不校验路径。
// 返回封装好的 net.Conn 供 MQTT 会话使用。
func HandshakeWS(c net.Conn, br *bufio.Reader, path string) (net.Conn, error) {
	// 读到 \r\n\r\n（上限 16KB）
	var head []byte
	for len(head) < 16<<10 {
		line, err := br.ReadString('\n')
		head = append(head, line...)
		if err != nil {
			return nil, fmt.Errorf("ws: read request head: %w", err)
		}
		if len(line) >= 2 && line[len(line)-2] == '\r' && line[len(line)-1] == '\n' {
			if len(head) >= 4 && head[len(head)-4] == '\r' && head[len(head)-3] == '\n' {
				break
			}
		}
	}
	reqLine := ""
	headers := map[string]string{}
	lines := strings.Split(string(head), "\r\n")
	for i, l := range lines {
		if i == 0 {
			reqLine = strings.TrimSpace(l)
			continue
		}
		if k := strings.IndexByte(l, ':'); k > 0 {
			headers[strings.ToLower(strings.TrimSpace(l[:k]))] = strings.TrimSpace(l[k+1:])
		}
	}
	if !strings.EqualFold(headers["upgrade"], "websocket") {
		_ = writeHTTP(c, "400 Bad Request", "expected websocket upgrade\n")
		return nil, fmt.Errorf("ws: not a websocket request")
	}
	key := headers["sec-websocket-key"]
	if key == "" {
		_ = writeHTTP(c, "400 Bad Request", "missing Sec-WebSocket-Key\n")
		return nil, fmt.Errorf("ws: missing key")
	}
	// 路径校验（缺省 /mqtt；path 为 * 表示任意）
	parts := strings.Fields(reqLine)
	if len(parts) >= 2 && path != "" && path != "*" {
		if !strings.EqualFold(strings.SplitN(parts[1], "?", 2)[0], path) {
			_ = writeHTTP(c, "404 Not Found", "unknown websocket path\n")
			return nil, fmt.Errorf("ws: path %s not allowed", parts[1])
		}
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n" +
		"Sec-WebSocket-Protocol: mqtt\r\n\r\n"
	if _, err := c.Write([]byte(resp)); err != nil {
		return nil, err
	}
	return &wsConn{Conn: c, br: br}, nil
}

func writeHTTP(c net.Conn, status, body string) error {
	resp := "HTTP/1.1 " + status + "\r\nContent-Type: text/plain\r\nConnection: close\r\nContent-Length: " +
		fmt.Sprintf("%d", len(body)) + "\r\n\r\n" + body
	_, err := c.Write([]byte(resp))
	return err
}

// Read 帧解包为字节流（控制帧就地处理：ping→pong、close→EOF）。
func (w *wsConn) Read(p []byte) (int, error) {
	w.rmu.Lock()
	defer w.rmu.Unlock()
	for len(w.rbuf) == 0 {
		if w.closed {
			return 0, io.EOF
		}
		fin, opcode, payload, err := w.readFrame()
		if err != nil {
			w.closed = true
			return 0, err
		}
		switch opcode {
		case 0x8: // close
			_ = w.writeFrame(0x8, nil)
			w.closed = true
			return 0, io.EOF
		case 0x9: // ping → pong
			_ = w.writeFrame(0xA, payload)
		case 0xA: // pong：忽略
		default: // 1 文本 / 2 二进制 / 0 续帧
			_ = fin
			w.rbuf = append(w.rbuf, payload...)
		}
	}
	n := copy(p, w.rbuf)
	w.rbuf = w.rbuf[n:]
	return n, nil
}

// Write 以二进制帧发送。
func (w *wsConn) Write(p []byte) (int, error) {
	if err := w.writeFrame(0x2, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *wsConn) readFrame() (fin bool, opcode byte, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(w.br, hdr[:]); err != nil {
		return
	}
	fin = hdr[0]&0x80 != 0
	opcode = hdr[0] & 0x0F
	masked := hdr[1]&0x80 != 0
	ln := uint64(hdr[1] & 0x7F)
	switch ln {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(w.br, ext[:]); err != nil {
			return
		}
		ln = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(w.br, ext[:]); err != nil {
			return
		}
		ln = binary.BigEndian.Uint64(ext[:])
	}
	if ln > 64<<20 {
		err = fmt.Errorf("ws: frame too large")
		return
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(w.br, mask[:]); err != nil {
			return
		}
	}
	payload = make([]byte, ln)
	if ln > 0 {
		if _, err = io.ReadFull(w.br, payload); err != nil {
			return
		}
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return
}

func (w *wsConn) writeFrame(opcode byte, payload []byte) error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	hdr := []byte{0x80 | opcode}
	n := len(payload)
	switch {
	case n < 126:
		hdr = append(hdr, byte(n))
	case n < 65536:
		hdr = append(hdr, 126, byte(n>>8), byte(n))
	default:
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		hdr = append(hdr, 127)
		hdr = append(hdr, ext[:]...)
	}
	if _, err := w.Conn.Write(hdr); err != nil {
		return err
	}
	if n > 0 {
		_, err := w.Conn.Write(payload)
		return err
	}
	return nil
}

// ServeWSConn 完整服务一条 WS 上的 MQTT 连接（握手 + 会话）。
func ServeWSConn(c net.Conn, br *bufio.Reader, wsPath string, store *mq.Store, auths *auth.Store, cfg *config.Config) {
	ws, err := HandshakeWS(c, br, wsPath)
	if err != nil {
		return
	}
	ServeConn(ws, store, auths, cfg)
}

// IsHTTPRequest 判定首行是否 HTTP 请求行（嗅探用）。
func IsHTTPRequest(line string) bool {
	for _, m := range []string{"GET ", "POST ", "PUT ", "DELETE ", "OPTIONS ", "HEAD ", "PATCH "} {
		if strings.HasPrefix(line, m) {
			return true
		}
	}
	return false
}

// ListenAndServe 独立 MQTT 监听（tcp 与 ws 同端口自动识别；tlsCfg 非空时
// 同端口支持 mqtts / wss 双模式接入——首字节 0x16 = TLS ClientHello，v1.3.12）。
func ListenAndServe(addr, wsPath string, store *mq.Store, auths *auth.Store, cfg *config.Config, tlsCfg *tls.Config) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	proto := "tcp + ws " + wsPath
	if tlsCfg != nil {
		proto = "tcp/ws/mqtts/wss dual-mode tls"
	}
	logx.Infof("mqtt: standalone listener on %s (%s)", addr, proto)
	for {
		c, err := ln.Accept()
		if err != nil {
			continue
		}
		go func(c net.Conn) {
			defer c.Close()
			br := bufio.NewReader(c)
			_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
			first, err := br.Peek(1)
			_ = c.SetReadDeadline(time.Time{})
			if err == nil && len(first) == 1 && first[0] == 0x16 && tlsCfg != nil {
				// TLS 优先客户端（mqtts / wss）：终结 TLS 后内部按 tcp/ws 继续
				tc := tls.Server(&bufConn{Conn: c, br: br}, tlsCfg)
				br2 := bufio.NewReader(tc)
				_ = tc.SetReadDeadline(time.Now().Add(10 * time.Second))
				f2, e2 := br2.Peek(1)
				_ = tc.SetReadDeadline(time.Time{})
				if e2 == nil && len(f2) == 1 && (f2[0] == 'G' || f2[0] == 'g' || f2[0] == 'P' || f2[0] == 'p' ||
					f2[0] == 'D' || f2[0] == 'O' || f2[0] == 'H' || f2[0] == 'o') {
					if line, _ := br2.Peek(64); IsHTTPRequest(string(line)) {
						ServeWSConn(tc, br2, wsPath, store, auths, cfg)
						return
					}
				}
				ServeConn(&bufConn{Conn: tc, br: br2}, store, auths, cfg)
				return
			}
			if err == nil && len(first) > 0 && (first[0] == 'G' || first[0] == 'g' ||
				first[0] == 'P' || first[0] == 'p' || first[0] == 'D' || first[0] == 'O' ||
				first[0] == 'H' || first[0] == 'o') {
				// 可能是 HTTP（WebSocket 升级）
				line, _ := br.Peek(64)
				if IsHTTPRequest(string(line)) {
					ServeWSConn(c, br, wsPath, store, auths, cfg)
					return
				}
			}
			// MQTT TCP 直连：已 Peek 的字节仍在缓冲
			ServeConn(&bufConn{Conn: c, br: br}, store, auths, cfg)
		}(c)
	}
}

// bufConn 让 mqtt.ServeConn 能读到嗅探缓冲里的首包。
type bufConn struct {
	net.Conn
	br *bufio.Reader
}

func (b *bufConn) Read(p []byte) (int, error) { return b.br.Read(p) }
