package mq

import (
	"net"
	"testing"
	"time"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/config"
	"glaciertdb/internal/sqlite"
)

// v1.4.0 协议级冒烟：真实 ServeConn + 包内 codec 充当最小 AMQP 客户端，覆盖
// bufio 写帧路径（连接首推 Start 落网 / 帧循环应答节奏 / 派发 goroutine Deliver
// 落网 / flusher confirm 回调落网）、mandatory basic.return 与 Get-Empty 应答。

type testServer struct {
	store *Store
	ln    net.Listener
}

func startTestServer(t *testing.T, asyncMs int) *testServer {
	t.Helper()
	cfg := &config.Config{DBDir: t.TempDir()}
	cfg.Fill()
	b, err := sqlite.NewBackend(cfg)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	authDB, err := b.OpenSystemDB()
	if err != nil {
		t.Fatalf("authdb: %v", err)
	}
	auths, err := auth.Open(authDB, auth.Bootstrap{AdminUser: "glacier", AdminPassword: "p"}, config.LockOpts{})
	if err != nil {
		t.Fatalf("auth: %v", err)
	}
	store, err := Open(b, config.MQOpts{Enable: true, AsyncFlushMs: asyncMs})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go ServeConn(c, store, auths, cfg)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return &testServer{store: store, ln: ln}
}

// dialClient 完成协议头 + Start/Start-Ok/Tune/Tune-Ok/Open/Open-Ok 握手。
func dialClient(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write([]byte("AMQP\x00\x00\x09\x01")); err != nil {
		t.Fatalf("proto header: %v", err)
	}
	// Connection.Start
	f, err := readFrame(c)
	if err != nil {
		t.Fatalf("read start: %v", err)
	}
	if m, _ := decodeMethod(f.payload); m == nil || m.class != 10 || m.method != 10 {
		t.Fatalf("expected Connection.Start, got %+v", f)
	}
	// Connection.Start-Ok（PLAIN：\0user\0pass）
	if err := writeMethodFrame(c, 0, 10, 11, []any{map[string]any{}, "PLAIN", []byte("\x00glacier\x00p"), "en_US"}); err != nil {
		t.Fatalf("start-ok: %v", err)
	}
	// Connection.Tune
	if f, err = readFrame(c); err != nil {
		t.Fatalf("read tune: %v", err)
	}
	if m, _ := decodeMethod(f.payload); m == nil || m.method != 30 {
		t.Fatalf("expected Connection.Tune, got %+v", f)
	}
	// Tune-Ok + Open
	if err := writeMethodFrame(c, 0, 10, 31, []any{uint16(2047), uint32(131072), uint16(0)}); err != nil {
		t.Fatalf("tune-ok: %v", err)
	}
	if err := writeMethodFrame(c, 0, 10, 40, []any{"127.0.0.1", byte(0), false}); err != nil {
		t.Fatalf("open: %v", err)
	}
	if f, err = readFrame(c); err != nil {
		t.Fatalf("read open-ok: %v", err)
	}
	if m, _ := decodeMethod(f.payload); m == nil || m.method != 41 {
		t.Fatalf("expected Connection.Open-Ok, got %+v", f)
	}
	return c
}

// writeMethodFrame 客户端编码并写一个方法帧。
func writeMethodFrame(c net.Conn, chNum uint16, class, method uint16, args []any) error {
	payload, err := (&methodMsg{class: class, method: method, args: args}).encode()
	if err != nil {
		return err
	}
	return writeFrame(c, frame{typ: frameMethod, channel: chNum, payload: payload})
}

// writeContentFrames 写内容头帧 + 内容体帧。
func writeContentFrames(t *testing.T, c net.Conn, chNum uint16, body []byte) error {
	t.Helper()
	hp := contentHeader{bodySize: uint64(len(body))}.encode()
	if err := writeFrame(c, frame{typ: frameHeader, channel: chNum, payload: hp}); err != nil {
		return err
	}
	return writeFrame(c, frame{typ: frameBody, channel: chNum, payload: body})
}

// readMethod 读一帧并要求为指定方法。
func readMethod(t *testing.T, c net.Conn, wantClass, wantMethod uint16) *methodMsg {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := readFrame(c)
	if err != nil {
		t.Fatalf("read method %d.%d: %v", wantClass, wantMethod, err)
	}
	m, err := decodeMethod(f.payload)
	if err != nil {
		t.Fatalf("decode method: %v", err)
	}
	if m.class != wantClass || m.method != wantMethod {
		t.Fatalf("expected method %d.%d, got %d.%d", wantClass, wantMethod, m.class, m.method)
	}
	return m
}

// readContent 读内容头帧 + 内容体帧（单帧体内联）。
func readContent(t *testing.T, c net.Conn) (contentHeader, []byte) {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	hf, err := readFrame(c)
	if err != nil || hf.typ != frameHeader {
		t.Fatalf("read content header: %v typ=%d", err, hf.typ)
	}
	h, err := decodeContentHeader(hf.payload)
	if err != nil {
		t.Fatalf("decode header: %v", err)
	}
	bf, err := readFrame(c)
	if err != nil || bf.typ != frameBody {
		t.Fatalf("read content body: %v typ=%d", err, bf.typ)
	}
	return h, bf.payload
}

func TestServeConnProtocol(t *testing.T) {
	srv := startTestServer(t, 20)
	c := dialClient(t, srv.ln.Addr().String())
	defer c.Close()
	const ch = 1

	// Channel.Open
	if err := writeMethodFrame(c, ch, 20, 10, []any{byte(0)}); err != nil {
		t.Fatal(err)
	}
	readMethod(t, c, 20, 11)

	// Queue.Declare（durable）
	if err := writeMethodFrame(c, ch, 50, 10, []any{uint16(0), "pq", false, true, false, false, false, map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	readMethod(t, c, 50, 11)

	// Confirm.Select → Select-Ok
	if err := writeMethodFrame(c, ch, 85, 10, []any{false}); err != nil {
		t.Fatal(err)
	}
	readMethod(t, c, 85, 11)

	// 发布 durable 消息（默认交换机直达队列）→ 等待异步 flush 后的 confirm basic.ack
	if err := writeMethodFrame(c, ch, 60, 40, []any{uint16(0), "", "pq", false, false}); err != nil {
		t.Fatal(err)
	}
	if err := writeContentFrames(t, c, ch, []byte("wire-hello")); err != nil {
		t.Fatal(err)
	}
	cm := readMethod(t, c, 60, 80) // basic.ack（confirm，flusher 回调落网）
	if len(cm.args) < 1 || cm.args[0] != int64(1) && cm.args[0] != uint64(1) {
		t.Fatalf("confirm delivery-tag = %v, want 1", cm.args)
	}

	// mandatory 不可路由：Basic.Return + 原内容回退 + confirm ack（return 在前）
	if err := writeMethodFrame(c, ch, 60, 40, []any{uint16(0), "", "ghost", true, false}); err != nil {
		t.Fatal(err)
	}
	if err := writeContentFrames(t, c, ch, []byte("returned")); err != nil {
		t.Fatal(err)
	}
	rm := readMethod(t, c, 60, 50) // basic.return
	if len(rm.args) < 1 || rm.args[0] != uint16(312) {
		t.Fatalf("return code = %v, want 312", rm.args)
	}
	if h, body := readContent(t, c); string(body) != "returned" || h.bodySize != 8 {
		t.Fatalf("returned content body=%q size=%d", body, h.bodySize)
	}
	readMethod(t, c, 60, 80) // confirm（routed=0 仍 ack，v1.3.2 语义）

	// Basic.Get 拉取存量消息 → Get-Ok + 内容 → Ack（dtag=1）
	if err := writeMethodFrame(c, ch, 60, 70, []any{uint16(0), "pq", false}); err != nil {
		t.Fatal(err)
	}
	gm := readMethod(t, c, 60, 71) // Basic.Get-Ok
	if len(gm.args) < 1 || gm.args[0] != int64(1) && gm.args[0] != uint64(1) {
		t.Fatalf("get delivery-tag = %v, want 1", gm.args)
	}
	if h, body := readContent(t, c); string(body) != "wire-hello" || h.bodySize != 10 {
		t.Fatalf("get content body=%q size=%d", body, h.bodySize)
	}
	if err := writeMethodFrame(c, ch, 60, 80, []any{uint64(1), false}); err != nil {
		t.Fatal(err)
	}
	// Get 空队列 → Get-Empty（请求-应答节奏：应答须及时落网）
	if err := writeMethodFrame(c, ch, 60, 70, []any{uint16(0), "pq", false}); err != nil {
		t.Fatal(err)
	}
	readMethod(t, c, 60, 72)

	// 消费：Consume → Consume-Ok（此时队列空）
	if err := writeMethodFrame(c, ch, 60, 20, []any{uint16(0), "pq", "c1", false, false, false, false, false, map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	readMethod(t, c, 60, 21)

	// 再发布一条：confirm / Deliver / content 三者到达，但交错次序不定
	//（Deliver+content 同写者连续，confirm 来自 flusher 稍后；均须到达）
	if err := writeMethodFrame(c, ch, 60, 40, []any{uint16(0), "", "pq", false, false}); err != nil {
		t.Fatal(err)
	}
	if err := writeContentFrames(t, c, ch, []byte("for-consumer")); err != nil {
		t.Fatal(err)
	}
	var gotConfirm, gotDeliver bool
	var dm *methodMsg
	var dh contentHeader
	var dbody []byte
	deadline := time.Now().Add(5 * time.Second)
	for !gotConfirm || !gotDeliver || dbody == nil {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting confirm/deliver/content (confirm=%v deliver=%v content=%v)", gotConfirm, gotDeliver, dbody != nil)
		}
		c.SetReadDeadline(time.Now().Add(time.Second))
		f, err := readFrame(c)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			t.Fatalf("read confirm/deliver/content: %v", err)
		}
		switch f.typ {
		case frameMethod:
			m, err := decodeMethod(f.payload)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if m.class == 60 && m.method == 80 {
				gotConfirm = true
			} else if m.class == 60 && m.method == 60 {
				gotDeliver = true
				dm = m
			}
		case frameHeader:
			if h, err := decodeContentHeader(f.payload); err == nil {
				dh = h
			}
		case frameBody:
			dbody = f.payload
		}
	}
	if len(dm.args) < 5 || dm.args[0] != "c1" {
		t.Fatalf("deliver args = %v", dm.args)
	}
	if string(dbody) != "for-consumer" || dh.bodySize != 12 {
		t.Fatalf("deliver content body=%q size=%d", dbody, dh.bodySize)
	}

	// Ack 派发消息（dtag 取 Deliver 帧携带值——通道计数器在 Get 时已步进过）
	if err := writeMethodFrame(c, ch, 60, 80, []any{toU64(dm.args[1]), false}); err != nil {
		t.Fatal(err)
	}
	// Ack 帧经服务端帧循环异步处理，轮询等待内存计数归零
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if srv.store.MessageCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := srv.store.FlushSync(); err != nil {
		t.Fatal(err)
	}
	if s := srv.store; s.MessageCount() != 0 {
		t.Fatalf("MessageCount = %d, want 0（deliver 后已 Ack）", s.MessageCount())
	}
}
