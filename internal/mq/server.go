package mq

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/config"
	"glaciertdb/internal/instance"
	"glaciertdb/internal/logx"
)

// connIDSeq 连接序号。
var connIDSeq uint64

type channelState struct {
	num      uint16
	nextTag  uint64
	unacked  map[uint64]int64 // delivery-tag → message id
	prefetch int
	closed   bool
	confirm  bool   // Confirm.Select 已开启 publisher confirm（v1.3.2）
	pubTag   uint64 // 发布序号（confirm 的 delivery-tag，v1.3.2）
}

// contentCollector Basic.Publish 内容收集（header + body 帧 + 路由目标）。
type contentCollector struct {
	header    contentHeader
	buf       []byte
	exchange  string
	rk        string
	mandatory bool
	pubTag    uint64
}

// connState 单条 AMQP 连接状态。
type connState struct {
	conn   net.Conn
	bw     *bufio.Writer // 帧写缓冲（v1.4.0：4KB 攒批；所有写入均持 wmu，Flush 同样纳入 wmu）
	wmu    sync.Mutex
	store  *Store
	auths  *auth.Store
	cfg    *config.Config
	ns     string // 登录账号命名空间
	connID uint64

	channels   map[uint16]*channelState
	collectors map[uint16]*contentCollector
	frameMax   int
	closed     bool
}

// ServeConn 服务一条 AMQP 0-9-1 连接（mode:rabbitmq 声明或 AMQP 魔数嗅探进入）。
func ServeConn(c net.Conn, store *Store, auths *auth.Store, cfg *config.Config) {
	defer c.Close()
	remote := c.RemoteAddr().String()
	connID := atomicAdd(&connIDSeq, 1)

	// 协议头：AMQP\x00\x00\x09\x01
	hdr := make([]byte, 8)
	if _, err := io.ReadFull(c, hdr); err != nil || string(hdr[:4]) != "AMQP" {
		logx.Debugf("mq: bad protocol header from %s", remote)
		return
	}
	cs := &connState{
		conn: c, store: store, auths: auths, cfg: cfg,
		connID: connID, channels: map[uint16]*channelState{},
		collectors: map[uint16]*contentCollector{},
		frameMax:   131072,
	}
	cs.bw = bufio.NewWriterSize(c, 4096) // v1.4.0：帧写缓冲
	defer cs.flush()                     // v1.4.0：连接结束落网残余帧（defer LIFO：在 RequeueConn 之后、c.Close 之前执行）
	defer func() {
		store.RequeueConn(connID)
		logx.Debugf("mq: connection %s closed", remote)
	}()
	logx.Debugf("mq: amqp connection from %s", remote)
	cs.serve(hdr)
}

func (cs *connState) serve(_ []byte) {
	// Connection.Start（0-9-1）；server-properties 的 version 可配置（mq.server_version，v1.3.3）
	srvVer := "3.13.0"
	if cs.cfg != nil && cs.cfg.MQ.ServerVersion != "" {
		srvVer = cs.cfg.MQ.ServerVersion
	}
	props := map[string]any{
		"product":     "GlacierTDB",
		"version":     srvVer,
		"platform":    "Go",
		"copyright":   "",
		"information": "GlacierTDB AMQP 0-9-1 basic queue (instance " + instance.UUID() + ")", // 实例 ID 能力串带出（v1.3.13 §6.2）
		"capabilities": map[string]any{
			"basic.nack":             true,
			"consumer_cancel_notify": true,
			"publisher_confirms":     true,
			"connection.blocked":     false,
		},
	}
	if err := cs.writeMethod(0, &methodMsg{class: 10, method: 10,
		args: []any{byte(0), byte(9), props, []byte("PLAIN"), []byte("en_US")}}); err != nil {
		return
	}
	cs.flush() // v1.4.0：Connection.Start 是服务端首推帧，必须落网否则客户端等不到（bufio 死锁）

	authed := false
	for {
		f, err := readFrame(cs.conn)
		if err != nil {
			return
		}
		switch f.typ {
		case frameHeartbeat:
			// 心跳：无应答（落到循环尾统一落网）
		case frameHeader:
			ch := cs.collectors[f.channel]
			if ch == nil {
				continue
			}
			h, err := decodeContentHeader(f.payload)
			if err != nil {
				return
			}
			ch.header = h
			if h.bodySize == 0 {
				delete(cs.collectors, f.channel)
				cs.finishPublish(f.channel, ch)
			}
		case frameBody:
			ch := cs.collectors[f.channel]
			if ch == nil {
				continue
			}
			ch.buf = append(ch.buf, f.payload...)
			if uint64(len(ch.buf)) >= ch.header.bodySize {
				delete(cs.collectors, f.channel)
				cs.finishPublish(f.channel, ch)
			}
		case frameMethod:
			m, err := decodeMethod(f.payload)
			if err != nil {
				return
			}
			if !authed && !(m.class == 10 && m.method == 11) {
				return // 未认证不允许其它方法
			}
			done := cs.handleMethod(f.channel, m)
			if m.class == 10 && m.method == 11 {
				authed = true
			}
			if done {
				return
			}
		}
		cs.flush() // v1.4.0：每处理完一个 inbound 帧后落网本帧应答（请求-应答节奏天然攒批；阻塞读前必落网，无死锁）
	}
}

// handleMethod 方法分发；返回 true 表示连接应结束。
func (cs *connState) handleMethod(chNum uint16, m *methodMsg) bool {
	k := key(m.class, m.method)

	switch k {
	// ---- Connection ----
	case key(10, 11): // Start-Ok
		mech := toStr(m.args[1])
		resp := ""
		if b, ok := m.args[2].([]byte); ok {
			resp = string(b)
		}
		user, pass := parsePlain(resp)
		if !strings.EqualFold(mech, "PLAIN") || !cs.authenticate(user, pass) {
			cs.writeMethod(0, &methodMsg{class: 10, method: 50,
				args: []any{uint16(403), "access refused", uint16(10), uint16(11)}})
			return true
		}
		cs.ns = user
		// Tune：channel-max 2047 / frame-max 128K / heartbeat 0（关闭心跳）
		cs.writeMethod(0, &methodMsg{class: 10, method: 30,
			args: []any{uint16(2047), uint32(131072), uint16(0)}})

	case key(10, 31): // Tune-Ok
		if fm := toU32(m.args[1]); fm >= 4096 {
			cs.frameMax = int(fm)
		}

	case key(10, 40): // Open
		cs.writeMethod(0, &methodMsg{class: 10, method: 41, args: []any{""}})

	case key(10, 50): // Close
		cs.writeMethod(0, &methodMsg{class: 10, method: 51, args: nil})
		return true

	case key(10, 51): // Close-Ok
		return true

	// ---- Channel ----
	case key(20, 10): // Open
		if cs.channels[chNum] == nil {
			cs.channels[chNum] = &channelState{num: chNum, unacked: map[uint64]int64{}, nextTag: 1}
		}
		cs.writeMethod(chNum, &methodMsg{class: 20, method: 11, args: []any{[]byte("")}})

	case key(20, 20): // Flow
		cs.writeMethod(chNum, &methodMsg{class: 20, method: 21, args: []any{toBool(m.args[0])}})

	case key(20, 40): // Close
		cs.closeChannel(chNum)
		cs.writeMethod(chNum, &methodMsg{class: 20, method: 41, args: nil}) // CloseOk=41

	case key(20, 41): // Close-Ok
		cs.closeChannel(chNum)

	// ---- Exchange ----
	case key(40, 10): // Declare
		name, typ := toStr(m.args[1]), toStr(m.args[2])
		passive, durable, autoDel := toBool(m.args[3]), toBool(m.args[4]), toBool(m.args[5])
		noWait := toBool(m.args[7])
		if err := cs.store.DeclareExchange(cs.ns, name, typ, durable, autoDel, passive); err != nil {
			cs.channelError(chNum, 406, err.Error(), 40, 10)
			return false
		}
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 40, method: 11, args: nil})
		}

	case key(40, 20): // Delete
		name := toStr(m.args[1])
		noWait := toBool(m.args[3])
		_ = cs.store.DeleteExchange(cs.ns, name)
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 40, method: 21, args: nil})
		}

	// ---- Queue ----
	case key(50, 10): // Declare
		name := toStr(m.args[1])
		passive, durable, exclusive, autoDel := toBool(m.args[2]), toBool(m.args[3]), toBool(m.args[4]), toBool(m.args[5])
		noWait := toBool(m.args[6])
		ready, cons, err := cs.store.DeclareQueue(cs.ns, name, durable, exclusive, autoDel, passive)
		if err != nil {
			cs.channelError(chNum, 406, err.Error(), 50, 10)
			return false
		}
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 50, method: 11,
				args: []any{name, uint32(ready), uint32(cons)}})
		}

	case key(50, 20): // Bind
		queue, exchange, rk := toStr(m.args[1]), toStr(m.args[2]), toStr(m.args[3])
		noWait := toBool(m.args[4])
		_ = cs.store.Bind(cs.ns, exchange, queue, rk)
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 50, method: 21, args: nil})
		}

	case key(50, 30): // Purge
		name := toStr(m.args[1])
		noWait := toBool(m.args[2])
		n, _ := cs.store.PurgeQueue(cs.ns, name)
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 50, method: 31, args: []any{uint32(n)}})
		}

	case key(50, 40): // Delete
		name := toStr(m.args[1])
		noWait := toBool(m.args[4])
		n, _ := cs.store.DeleteQueue(cs.ns, name)
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 50, method: 41, args: []any{uint32(n)}})
		}

	case key(50, 50): // Unbind
		queue, exchange, rk := toStr(m.args[1]), toStr(m.args[2]), toStr(m.args[3])
		_ = cs.store.Unbind(cs.ns, exchange, queue, rk)
		cs.writeMethod(chNum, &methodMsg{class: 50, method: 51, args: nil})

	// ---- Basic ----
	case key(60, 10): // Qos
		prefetch := int(toU16(m.args[1]))
		if ch := cs.channels[chNum]; ch != nil {
			ch.prefetch = prefetch
		}
		cs.writeMethod(chNum, &methodMsg{class: 60, method: 11, args: nil})

	case key(60, 20): // Consume
		queue, tag := toStr(m.args[1]), toStr(m.args[2])
		noAck, exclusive := toBool(m.args[4]), toBool(m.args[5])
		noWait := toBool(m.args[6])
		ch := cs.channels[chNum]
		if ch == nil {
			ch = &channelState{num: chNum, unacked: map[uint64]int64{}, nextTag: 1}
			cs.channels[chNum] = ch
		}
		if tag == "" {
			tag = fmt.Sprintf("amq.ctag-%d-%d", cs.connID, time.Now().UnixNano()%1e6)
		}
		cons := &Consumer{
			Tag: tag, NS: cs.ns, Queue: queue, NoAck: noAck, Exclusive: exclusive,
			ConnID: cs.connID, Prefetch: ch.prefetch,
			Deliver: cs.deliverFn(ch, tag, noAck),
		}
		cs.store.RegisterConsumer(cons)
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 60, method: 21, args: []any{tag}})
		}

	case key(60, 30): // Cancel
		tag := toStr(m.args[0])
		noWait := toBool(m.args[1])
		cs.cancelConsumer(chNum, tag)
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 60, method: 31, args: []any{tag}})
		}

	case key(60, 40): // Publish（mandatory 位保存到收集器，供 basic.return 回退，v1.3.2）
		exchange, rk := toStr(m.args[1]), toStr(m.args[2])
		mandatory := toBool(m.args[3])
		ch := cs.channels[chNum]
		if ch == nil {
			ch = &channelState{num: chNum, unacked: map[uint64]int64{}, nextTag: 1}
			cs.channels[chNum] = ch
		}
		ch.pubTag++
		cs.collectors[chNum] = &contentCollector{
			exchange: exchange, rk: rk, mandatory: mandatory, pubTag: ch.pubTag,
		}

	case key(60, 70): // Get
		queue := toStr(m.args[1])
		noAck := toBool(m.args[2])
		msg := cs.store.Get(cs.ns, queue, noAck)
		ch := cs.channels[chNum]
		if ch == nil {
			ch = &channelState{num: chNum, unacked: map[uint64]int64{}, nextTag: 1}
			cs.channels[chNum] = ch
		}
		if msg == nil {
			cs.writeMethod(chNum, &methodMsg{class: 60, method: 72, args: []any{""}})
			return false
		}
		tag := ch.nextTag
		ch.nextTag++
		if !noAck {
			ch.unacked[tag] = msg.ID
		}
		cs.writeMethod(chNum, &methodMsg{class: 60, method: 71,
			args: []any{uint64(tag), msg.Redelivered, msg.Exchange, msg.RoutingKey, uint32(0)}})
		cs.writeContent(chNum, contentHeader{bodySize: uint64(len(msg.Payload)), props: msg.Props}, msg.Payload)

	case key(60, 80): // Ack
		tag, multiple := toU64(m.args[0]), toBool(m.args[1])
		cs.ackTags(chNum, tag, multiple, false, false)

	case key(60, 90): // Reject
		tag, requeue := toU64(m.args[0]), toBool(m.args[1])
		cs.ackTags(chNum, tag, false, true, requeue)

	case key(60, 120): // Nack
		tag, multiple, requeue := toU64(m.args[0]), toBool(m.args[1]), toBool(m.args[2])
		cs.ackTags(chNum, tag, multiple, true, requeue)

	case key(60, 110): // Recover（全部重投）
		cs.recoverChannel(chNum)
		cs.writeMethod(chNum, &methodMsg{class: 60, method: 111, args: nil})

	// ---- Confirm / Tx ----
	case key(85, 10): // Confirm.Select（v1.3.2：开启 publisher confirm，逐条 basic.ack）
		noWait := toBool(m.args[0])
		ch := cs.channels[chNum]
		if ch == nil {
			ch = &channelState{num: chNum, unacked: map[uint64]int64{}, nextTag: 1}
			cs.channels[chNum] = ch
		}
		ch.confirm = true
		if !noWait {
			cs.writeMethod(chNum, &methodMsg{class: 85, method: 11, args: nil})
		}
	case key(90, 10):
		cs.writeMethod(chNum, &methodMsg{class: 90, method: 11, args: nil})
	case key(90, 20):
		cs.writeMethod(chNum, &methodMsg{class: 90, method: 21, args: nil})
	case key(90, 30):
		cs.writeMethod(chNum, &methodMsg{class: 90, method: 31, args: nil})

	default:
		name := "unknown"
		if sp, ok := methodSpecs[k]; ok {
			name = sp.name
		}
		logx.Debugf("mq: unhandled method %s (%d.%d)", name, m.class, m.method)
		cs.channelError(chNum, 540, "method "+name+" not implemented", m.class, m.method)
	}
	return false
}

// finishPublish 内容收集完成 → 发布；mandatory 不可路由回退（basic.return）+
// publisher confirm 确认帧（basic.ack / basic.nack，v1.3.2）。
// v1.4.0：async_flush 模式下 confirm 改由 flusher 在 journal 批量落库后回调写出
// （等价原"落库成功后 confirm"时序；basic.return 仍在发布时同步写——return→confirm
// 次序不变；入口即失败（如尺寸超限）未进 journal，同步 nack）。
func (cs *connState) finishPublish(chNum uint16, c *contentCollector) {
	props := c.header.props
	if len(props) == 0 {
		props = nil
	}
	ch := cs.channels[chNum]
	wantConfirm := ch != nil && ch.confirm
	asyncConfirm := wantConfirm && cs.store.async()
	var (
		routed int
		err    error
	)
	if asyncConfirm {
		routed, err = cs.store.PublishConfirm(cs.ns, c.exchange, c.rk, props, c.buf, deliveryMode(props),
			func(perr error) {
				cs.writeConfirm(chNum, c.pubTag, perr) // flusher goroutine 回调；wmu 串行化线程安全
			})
	} else {
		routed, err = cs.store.Publish(cs.ns, c.exchange, c.rk, props, c.buf, deliveryMode(props))
	}
	if err != nil {
		logx.Debugf("mq: publish failed: %v", err)
	}
	// mandatory 回退（312 NO_ROUTE）：一条也没路由到队列 → 方法帧 + 原内容发回发布方
	if c.mandatory && err == nil && routed == 0 {
		cs.writeMethod(chNum, &methodMsg{class: 60, method: 50,
			args: []any{uint16(312), "NO_ROUTE", c.exchange, c.rk}})
		cs.writeContent(chNum, c.header, c.buf)
	}
	if !wantConfirm {
		return
	}
	if asyncConfirm {
		if err != nil {
			cs.writeConfirm(chNum, c.pubTag, err) // 入口即失败：同步 nack（成功路径由 flusher 回调）
		}
		return
	}
	// 同步模式（async_flush_ms=0）：保持 v1.3.2 时序——Publish 返回（落库完成）后立刻 confirm
	//（confirm 模式逐条确认：回退后仍 ack = 已处理；失败 nack）
	cs.writeConfirm(chNum, c.pubTag, err)
}

// writeConfirm 写 publisher confirm 帧（perr=nil→basic.ack，否则→basic.nack）；
// v1.4.0 抽出供帧循环与 flusher 回调复用，尾部显式落网（bufio 攒批下 confirm 帧常
// 来自非帧循环 goroutine，必须自行 Flush）。
func (cs *connState) writeConfirm(chNum uint16, pubTag uint64, perr error) {
	if perr != nil {
		_ = cs.writeMethod(chNum, &methodMsg{class: 60, method: 120,
			args: []any{pubTag, false, false}})
	} else {
		_ = cs.writeMethod(chNum, &methodMsg{class: 60, method: 80,
			args: []any{pubTag, false}})
	}
	cs.flush()
}

// deliveryMode 解析 Basic.Properties 的 delivery-mode（属性位 0x1000）是否为 2（persistent）。
func deliveryMode(props []byte) bool {
	if len(props) < 3 {
		return false
	}
	flags := uint16(props[0])<<8 | uint16(props[1])
	d := &decBuf{b: props[2:]}
	skipShortstr := func() {
		if l, err := d.octet(); err == nil {
			d.off += int(l)
		}
	}
	skipTable := func() {
		if l, err := d.long(); err == nil {
			d.off += int(l)
		}
	}
	if flags&0x8000 != 0 {
		skipShortstr() // content-type
	}
	if flags&0x4000 != 0 {
		skipShortstr() // content-encoding
	}
	if flags&0x2000 != 0 {
		skipTable() // headers
	}
	if flags&0x1000 != 0 {
		if v, err := d.octet(); err == nil {
			return v == 2
		}
	}
	return false
}

// deliverFn 消费者推送回调（Basic.Deliver + 内容）。
func (cs *connState) deliverFn(ch *channelState, tag string, noAck bool) func(*Message) {
	return func(m *Message) {
		var dtag uint64
		if ch != nil {
			dtag = ch.nextTag
			ch.nextTag++
			if !noAck {
				ch.unacked[dtag] = m.ID
			}
		}
		cs.writeMethod(ch.num, &methodMsg{class: 60, method: 60,
			args: []any{tag, uint64(dtag), m.Redelivered, m.Exchange, m.RoutingKey}})
		cs.writeContent(ch.num, contentHeader{bodySize: uint64(len(m.Payload)), props: m.Props}, m.Payload)
		cs.flush() // v1.4.0：Deliver 来自派发 goroutine，显式落网（与帧循环应答经 wmu 串行）
	}
}

// ackTags 确认/否定确认。
func (cs *connState) ackTags(chNum uint16, tag uint64, multiple, nack, requeue bool) {
	ch := cs.channels[chNum]
	if ch == nil {
		return
	}
	var ids []int64
	if multiple {
		for t, id := range ch.unacked {
			if t <= tag {
				ids = append(ids, id)
				delete(ch.unacked, t)
			}
		}
	} else if id, ok := ch.unacked[tag]; ok {
		ids = append(ids, id)
		delete(ch.unacked, tag)
	}
	if len(ids) == 0 {
		return
	}
	if nack {
		cs.store.Nack(cs.ns, ids, requeue)
	} else {
		cs.store.Ack(cs.ns, ids)
	}
}

// recoverChannel Basic.Recover：未确认全部重入队。
func (cs *connState) recoverChannel(chNum uint16) {
	ch := cs.channels[chNum]
	if ch == nil {
		return
	}
	var ids []int64
	for t, id := range ch.unacked {
		ids = append(ids, id)
		delete(ch.unacked, t)
	}
	if len(ids) > 0 {
		cs.store.Nack(cs.ns, ids, true)
	}
}

// closeChannel 关通道：消费者注销 + 未确认重入队。
func (cs *connState) closeChannel(chNum uint16) {
	ch := cs.channels[chNum]
	if ch == nil {
		return
	}
	cs.cancelConsumer(chNum, "")
	var ids []int64
	for t, id := range ch.unacked {
		ids = append(ids, id)
		delete(ch.unacked, t)
	}
	if len(ids) > 0 {
		cs.store.Nack(cs.ns, ids, true)
	}
	ch.closed = true
	delete(cs.channels, chNum)
	delete(cs.collectors, chNum)
}

// cancelConsumer 注销通道上的消费者（tag 空 = 全部）。
func (cs *connState) cancelConsumer(chNum uint16, tag string) {
	for _, c := range cs.store.ConsumersByConn(cs.connID) {
		if tag == "" || c.Tag == tag {
			cs.store.UnregisterConsumer(c, true)
		}
	}
}

// channelError 通道级错误（Channel.Close 携带错误码）。
func (cs *connState) channelError(chNum uint16, code uint16, msg string, class, method uint16) {
	if len(msg) > 200 {
		msg = msg[:200]
	}
	cs.writeMethod(chNum, &methodMsg{class: 20, method: 40,
		args: []any{code, msg, class, method}})
}

// authenticate 统一账号体系认证。
func (cs *connState) authenticate(user, pass string) bool {
	if cs.auths == nil || user == "" {
		return false
	}
	u, ok, err := cs.auths.Lookup(user)
	if err != nil || !ok || u.Password != pass {
		remote := cs.conn.RemoteAddr().String()
		ip := remote
		if h, _, e := net.SplitHostPort(remote); e == nil {
			ip = h
		}
		cs.auths.RecordLogin(ip, remote, user, false, "mq: wrong password")
		return false
	}
	remote := cs.conn.RemoteAddr().String()
	ip := remote
	if h, _, e := net.SplitHostPort(remote); e == nil {
		ip = h
	}
	if locked, _ := cs.auths.IPLocked(ip); locked {
		return false
	}
	if locked, _ := cs.auths.AccountLocked(user); locked {
		return false
	}
	if ok, _ := cs.auths.HostOK(user, ip); !ok {
		return false
	}
	cs.auths.RecordLogin(ip, remote, user, true, "mq")
	return true
}

// writeMethod 写方法帧（连接写互斥；v1.4.0 写入 bufio，由调用路径显式 Flush）。
func (cs *connState) writeMethod(chNum uint16, m *methodMsg) error {
	payload, err := m.encode()
	if err != nil {
		return err
	}
	cs.wmu.Lock()
	defer cs.wmu.Unlock()
	return writeFrame(cs.bw, frame{typ: frameMethod, channel: chNum, payload: payload})
}

// writeContent 写内容（header + body 分帧；v1.4.0 写入 bufio——方法帧+内容帧序列
// 由调用方在同序列后 Flush 落网）。
func (cs *connState) writeContent(chNum uint16, h contentHeader, body []byte) error {
	cs.wmu.Lock()
	defer cs.wmu.Unlock()
	if err := writeFrame(cs.bw, frame{typ: frameHeader, channel: chNum, payload: h.encode()}); err != nil {
		return err
	}
	max := cs.frameMax
	if max <= 0 {
		max = 131072
	}
	for off := 0; off < len(body); off += max {
		end := off + max
		if end > len(body) {
			end = len(body)
		}
		if err := writeFrame(cs.bw, frame{typ: frameBody, channel: chNum, payload: body[off:end]}); err != nil {
			return err
		}
	}
	if len(body) == 0 {
		return writeFrame(cs.bw, frame{typ: frameBody, channel: chNum, payload: nil})
	}
	return nil
}

// flush 落网写缓冲（v1.4.0）：帧循环每处理完一个 inbound 帧后调用；Deliver /
// confirm 回调等非帧循环 goroutine 写完同样调用——bufio 写入与 Flush 全部经 wmu 串行。
func (cs *connState) flush() {
	cs.wmu.Lock()
	_ = cs.bw.Flush()
	cs.wmu.Unlock()
}

// parsePlain 解析 SASL PLAIN 响应（\0user\0pass）。
func parsePlain(resp string) (string, string) {
	parts := strings.Split(resp, "\x00")
	if len(parts) < 3 {
		return "", ""
	}
	return parts[1], parts[2]
}

func atomicAdd(p *uint64, d uint64) uint64 {
	return atomic.AddUint64(p, d)
}
