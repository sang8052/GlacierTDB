// Package mqtt — MQTT 3.1/3.1.1 接入（v1.3.3）：TCP 与 WebSocket 两种传输，
// 桥接进 mq 的 topic 交换机（exchange "mqtt"）：
//
//	topic  a/b/c   ↔  routing key a.b.c（'/' 与 '.' 互换）
//	过滤器 a/+/c   ↔  binding a.*.c；a/# ↔ a.#
//	QoS0 = noAck 推送；QoS1 走未确认/重入队语义（clean_session=0 时队列持久，
//	离线消息补投）；保留消息落 mqtt_retains，订阅命中即时补发。
//	认证沿用统一账号体系（CONNECT username/password）；命名空间 = 账号名。
package mqtt

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/mq"
)

// MQTT 包类型。
const (
	ptCONNECT     = 1
	ptCONNACK     = 2
	ptPUBLISH     = 3
	ptPUBACK      = 4
	ptPUBREC      = 5
	ptPUBREL      = 6
	ptPUBCOMP     = 7
	ptSUBSCRIBE   = 8
	ptSUBACK      = 9
	ptUNSUBSCRIBE = 10
	ptUNSUBACK    = 11
	ptPINGREQ     = 12
	ptPINGRESP    = 13
	ptDISCONNECT  = 14
)

// BridgeExchange MQTT 桥接交换机名。
const BridgeExchange = "mqtt"

var connIDSeq uint64

// ---------------------------------------------------------------------------
// 包编解码
// ---------------------------------------------------------------------------

type packet struct {
	typ   byte
	flags byte // byte1 低 4 位
	body  []byte
}

// readPacket 读一个 MQTT 包（Remaining Length varint + body）。
func readPacket(r io.Reader, max int) (packet, error) {
	var hdr [1]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return packet{}, err
	}
	var p packet
	p.typ = hdr[0] >> 4
	p.flags = hdr[0] & 0x0F
	// Remaining Length（1-4 字节 base-128 varint）
	mult := 1
	rl := 0
	for i := 0; i < 4; i++ {
		var b [1]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return packet{}, err
		}
		rl += int(b[0]&0x7F) * mult
		mult *= 128
		if b[0]&0x80 == 0 {
			break
		}
		if i == 3 {
			return packet{}, fmt.Errorf("mqtt: malformed remaining length")
		}
	}
	if rl > max {
		return packet{}, fmt.Errorf("mqtt: packet too large (%d)", rl)
	}
	p.body = make([]byte, rl)
	if rl > 0 {
		if _, err := io.ReadFull(r, p.body); err != nil {
			return packet{}, err
		}
	}
	return p, nil
}

type dec struct {
	b   []byte
	off int
}

func (d *dec) u8() (byte, bool) {
	if d.off+1 > len(d.b) {
		return 0, false
	}
	v := d.b[d.off]
	d.off++
	return v, true
}
func (d *dec) u16() (uint16, bool) {
	if d.off+2 > len(d.b) {
		return 0, false
	}
	v := uint16(d.b[d.off])<<8 | uint16(d.b[d.off+1])
	d.off += 2
	return v, true
}
func (d *dec) str() (string, bool) {
	l, ok := d.u16()
	if !ok || d.off+int(l) > len(d.b) {
		return "", false
	}
	s := string(d.b[d.off : d.off+int(l)])
	d.off += int(l)
	return s, true
}
func (d *dec) rest() []byte {
	if d.off >= len(d.b) {
		return nil
	}
	r := d.b[d.off:]
	d.off = len(d.b)
	return r
}

// ---------------------------------------------------------------------------
// 连接服务
// ---------------------------------------------------------------------------

type willMsg struct {
	topic   string
	payload []byte
	qos     int
	retain  bool
}

type conn struct {
	raw       net.Conn
	store     *mq.Store
	auths     *auth.Store
	cfg       *config.Config
	ns        string
	clientID  string
	clean     bool
	keepalive uint16
	will      *willMsg
	connBase  uint64 // mq 消费者归属的连接号（RequeueConn 用）

	wmu       sync.Mutex
	pid       uint16 // 出站 PUBLISH 包 id
	subMtx    sync.Mutex
	subFilter map[string]int // filter → qos（单队列单消费者，按过滤器分发）
	consumer  *mq.Consumer  // 队列消费者（首订阅时创建）
	closed    bool
}

// ServeConn 服务一条 MQTT TCP 连接。
func ServeConn(c net.Conn, store *mq.Store, auths *auth.Store, cfg *config.Config) {
	defer c.Close()
	remote := c.RemoteAddr().String()
	logx.Debugf("mqtt: connection from %s", remote)
	cc := &conn{raw: c, store: store, auths: auths, cfg: cfg, connBase: atomic.AddUint64(&connIDSeq, 1)}
	defer func() {
		if r := recover(); r != nil {
			logx.Debugf("mqtt: panic %s: %v", remote, r)
		}
		cc.cleanup()
	}()
	cc.serve()
}

func (cc *conn) serve() {
	// 首包 CONNECT（10s 握手窗口）
	_ = cc.raw.SetReadDeadline(time.Now().Add(10 * time.Second))
	p, err := readPacket(cc.raw, 4<<20)
	if err != nil {
		return
	}
	if p.typ != ptCONNECT {
		return
	}
	if !cc.handleConnect(p) {
		return
	}
	cc.loop()
}

// handleConnect 解析 CONNECT 并认证；失败写 CONNACK 错误码返回 false。
func (cc *conn) handleConnect(p packet) bool {
	d := &dec{b: p.body}
	proto, ok := d.str()
	if !ok {
		return false
	}
	level, ok := d.u8()
	if !ok {
		return false
	}
	flags, ok := d.u8()
	if !ok {
		return false
	}
	keepalive, _ := d.u16()
	if proto != "MQTT" && proto != "MQIsdp" {
		cc.writeConnack(1) // 不支持的协议名
		return false
	}
	if level != 3 && level != 4 {
		cc.writeConnack(1) // 3.1/3.1.1 之外（含 5.0）不支持
		return false
	}
	clientID, ok := d.str()
	if !ok {
		cc.writeConnack(2)
		return false
	}
	var will *willMsg
	if flags&0x04 != 0 { // will flag
		wt, ok1 := d.str()
		wm, ok2 := d.str()
		if !ok1 || !ok2 {
			return false
		}
		will = &willMsg{
			topic:   wt,
			payload: []byte(wm),
			qos:     int((flags >> 3) & 0x03),
			retain:  flags&0x20 != 0,
		}
	}
	user := ""
	if flags&0x80 != 0 {
		user, _ = d.str()
	}
	pass := ""
	if flags&0x40 != 0 {
		pass, _ = d.str()
	}
	remote := cc.raw.RemoteAddr().String()
	ip := remote
	if h, _, e := net.SplitHostPort(remote); e == nil {
		ip = h
	}
	if cc.auths == nil || user == "" || !mqttAuth(cc.auths, ip, remote, user, pass) {
		cc.writeConnack(4) // bad user/pass
		return false
	}
	cc.ns = user
	cc.clientID = sanitizeClientID(clientID)
	cc.clean = flags&0x02 != 0
	cc.keepalive = keepalive
	cc.will = will
	// 桥接交换机就位
	if err := cc.store.DeclareExchange(cc.ns, BridgeExchange, "topic", true, false, false); err != nil {
		logx.Debugf("mqtt: declare exchange: %v", err)
	}
	cc.writeConnack(0)
	logx.Debugf("mqtt: %s connected (client=%s ns=%s clean=%v keepalive=%ds)", remote, cc.clientID, cc.ns, cc.clean, keepalive)
	return true
}

func (cc *conn) loop() {
	for {
		if cc.keepalive > 0 {
			_ = cc.raw.SetReadDeadline(time.Now().Add(time.Duration(float64(cc.keepalive)*1.5) * time.Second))
		} else {
			_ = cc.raw.SetReadDeadline(time.Time{})
		}
		p, err := readPacket(cc.raw, 8<<20)
		if err != nil {
			// 异常断开：发遗嘱（DISCONNECT 之外的退出路径）
			cc.fireWill()
			return
		}
		switch p.typ {
		case ptPINGREQ:
			cc.wmu.Lock()
			_, _ = cc.raw.Write([]byte{0xD0, 0x00})
			cc.wmu.Unlock()
		case ptPUBLISH:
			cc.handlePublish(p)
		case ptPUBACK, ptPUBREC: // 客户端确认我方 QoS1 投递
		case ptPUBREL: // 入站 QoS2 近似：确认完成
			pid, _ := (&dec{b: p.body}).u16()
			cc.writeSimple([]byte{0x70, 0x02, byte(pid >> 8), byte(pid)})
		case ptSUBSCRIBE:
			cc.handleSubscribe(p)
		case ptUNSUBSCRIBE:
			cc.handleUnsubscribe(p)
		case ptDISCONNECT:
			cc.will = nil // 正常断开不发遗嘱
			return
		default:
			return // 未知包：断开
		}
	}
}

// handlePublish 入站发布：桥接 AMQP + 保留消息。
func (cc *conn) handlePublish(p packet) {
	qos := int((p.flags >> 1) & 0x03)
	retain := p.flags&0x01 != 0
	d := &dec{b: p.body}
	topic, ok := d.str()
	if !ok || topic == "" || strings.ContainsAny(topic, "+#") || strings.Contains(topic, "\x00") {
		return
	}
	var pid uint16
	if qos > 0 {
		pid, _ = d.u16()
	}
	payload := d.rest()
	rk := topicToRK(topic)
	_, err := cc.store.Publish(cc.ns, BridgeExchange, rk, nil, payload, qos > 0)
	if err != nil {
		logx.Debugf("mqtt: publish %q: %v", topic, err)
	}
	if retain {
		_ = cc.store.RetainPut(cc.ns, topic, rk, payload, qos)
	}
	switch qos {
	case 1:
		cc.writeSimple([]byte{0x40, 0x02, byte(pid >> 8), byte(pid)})
	case 2: // 近似：直接 PUBCOMP
		cc.writeSimple([]byte{0x70, 0x02, byte(pid >> 8), byte(pid)})
	}
}

// handleSubscribe 订阅：队列绑定 + 消费者推送 + 保留消息补发。
func (cc *conn) handleSubscribe(p packet) {
	d := &dec{b: p.body}
	pid, ok := d.u16()
	if !ok {
		return
	}
	type sub struct {
		filter string
		qos    int
	}
	var subs []sub
	for d.off < len(d.b) {
		f, ok1 := d.str()
		q, ok2 := d.u8()
		if !ok1 || !ok2 {
			return
		}
		subs = append(subs, sub{filter: f, qos: int(q & 0x03)})
	}
	if len(subs) == 0 {
		return
	}
	qname := "mqtt." + cc.clientID
	if _, _, err := cc.store.DeclareQueue(cc.ns, qname, !cc.clean, false, cc.clean, false); err != nil {
		logx.Debugf("mqtt: declare queue %s: %v", qname, err)
	}
	granted := make([]byte, 0, len(subs))
	for _, s := range subs {
		pattern := filterToPattern(s.filter)
		_ = cc.store.Bind(cc.ns, BridgeExchange, qname, pattern)
		granted = append(granted, byte(minI(s.qos, 1)))
	}
	// SUBACK 先行，再补发保留消息（MQTT 顺序语义）
	cc.writePacket(ptSUBACK, 0, append([]byte{byte(pid >> 8), byte(pid)}, granted...))
	for _, s := range subs {
		cc.ensureQueueConsumer(qname)
		cc.subMtx.Lock()
		if cc.subFilter == nil {
			cc.subFilter = map[string]int{}
		}
		cc.subFilter[s.filter] = minI(s.qos, 1)
		cc.subMtx.Unlock()
		for _, r := range cc.store.RetainAll(cc.ns) {
			if mqttTopicMatch(s.filter, r.Topic) {
				cc.deliverMQTT(r.Topic, r.Payload, minI(s.qos, 1), minI(s.qos, 1) == 1)
			}
		}
	}
}

// ensureQueueConsumer 每连接一个队列消费者：Deliver 时按全部过滤器分发
//（多过滤器共享队列，避免无匹配消费者吞消息，v1.3.3）。
func (cc *conn) ensureQueueConsumer(qname string) {
	cc.subMtx.Lock()
	if cc.consumer != nil {
		cc.subMtx.Unlock()
		return
	}
	cons := &mq.Consumer{
		Tag: "mqtt-sub-" + cc.clientID, NS: cc.ns, Queue: qname,
		NoAck:   true, // MQTT QoS1 出站重投由队列持久化承担（clean=0）
		ConnID:  cc.connBase,
		Deliver: cc.deliverRouted,
	}
	cc.consumer = cons
	cc.subMtx.Unlock()
	cc.store.RegisterConsumer(cons)
}

// deliverRouted 队列消息 → 命中过滤器逐一推送。
func (cc *conn) deliverRouted(m *mq.Message) {
	topic := rkToTopic(m.RoutingKey)
	cc.subMtx.Lock()
	subs := make(map[string]int, len(cc.subFilter))
	for f, q := range cc.subFilter {
		subs[f] = q
	}
	cc.subMtx.Unlock()
	for f, qos := range subs {
		if mqttTopicMatch(f, topic) {
			cc.deliverMQTT(topic, m.Payload, qos, qos == 1)
		}
	}
}

// deliverMQTT 出站 PUBLISH（连接写锁内组帧）。
func (cc *conn) deliverMQTT(topic string, payload []byte, qos int, needPID bool) {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	if cc.closed {
		return
	}
	flags := byte(qos << 1)
	if needPID && qos > 0 {
		cc.pid++
	}
	hdr := []byte{0x30 | flags}
	hdr = appendRemaining(hdr, 2+len(topic)+len(payload)+2*boolInt(needPID && qos > 0))
	w := make([]byte, 0, len(hdr)+64)
	w = append(w, hdr...)
	w = append(w, byte(len(topic)>>8), byte(len(topic)))
	w = append(w, topic...)
	if needPID && qos > 0 {
		w = append(w, byte(cc.pid>>8), byte(cc.pid))
	}
	w = append(w, payload...)
	_, _ = cc.raw.Write(w)
}

// handleUnsubscribe 解绑订阅。
func (cc *conn) handleUnsubscribe(p packet) {
	d := &dec{b: p.body}
	pid, ok := d.u16()
	if !ok {
		return
	}
	qname := "mqtt." + cc.clientID
	for d.off < len(d.b) {
		f, ok := d.str()
		if !ok {
			break
		}
		_ = cc.store.Unbind(cc.ns, BridgeExchange, qname, filterToPattern(f))
		cc.subMtx.Lock()
		delete(cc.subFilter, f)
		cc.subMtx.Unlock()
	}
	cc.writePacket(ptUNSUBACK, 0, []byte{byte(pid >> 8), byte(pid)})
}

// fireWill 异常断开 → 发布遗嘱。
func (cc *conn) fireWill() {
	if cc.will == nil {
		return
	}
	w := cc.will
	cc.will = nil
	_, _ = cc.store.Publish(cc.ns, BridgeExchange, topicToRK(w.topic), nil, w.payload, w.qos > 0)
	if w.retain {
		_ = cc.store.RetainPut(cc.ns, w.topic, topicToRK(w.topic), w.payload, w.qos)
	}
}

// cleanup 连接退出：注销消费者；clean session 删队列。
func (cc *conn) cleanup() {
	cc.wmu.Lock()
	cc.closed = true
	cc.wmu.Unlock()
	if cc.store == nil || cc.ns == "" {
		return
	}
	cc.subMtx.Lock()
	cons := cc.consumer
	cc.consumer = nil
	cc.subFilter = nil
	cc.subMtx.Unlock()
	if cons != nil {
		cc.store.UnregisterConsumer(cons, true)
	}
	if cc.clean && cc.clientID != "" {
		_, _ = cc.store.DeleteQueue(cc.ns, "mqtt."+cc.clientID)
	}
	cc.store.RequeueConn(cc.connBase)
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func (cc *conn) writeConnack(code byte) {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	_, _ = cc.raw.Write([]byte{0x20, 0x02, 0x00, code})
}

func (cc *conn) writeSimple(b []byte) {
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	_, _ = cc.raw.Write(b)
}

func (cc *conn) writePacket(typ, flags byte, body []byte) {
	hdr := appendRemaining([]byte{typ<<4 | flags}, len(body))
	cc.wmu.Lock()
	defer cc.wmu.Unlock()
	_, _ = cc.raw.Write(append(hdr, body...))
}

// appendRemaining 追加 Remaining Length varint。
func appendRemaining(b []byte, n int) []byte {
	for {
		d := byte(n % 128)
		n /= 128
		if n > 0 {
			d |= 0x80
		}
		b = append(b, d)
		if n == 0 {
			break
		}
	}
	return b
}

// topicToRK MQTT topic → AMQP routing key（'/' → '.'）。
func topicToRK(t string) string {
	return strings.ReplaceAll(t, "/", ".")
}

// rkToTopic routing key → MQTT topic（'.' → '/'）。
func rkToTopic(rk string) string {
	return strings.ReplaceAll(rk, ".", "/")
}

// filterToPattern MQTT 过滤器 → AMQP binding pattern（+ → *，# → #）。
func filterToPattern(f string) string {
	segs := strings.Split(f, "/")
	for i, s := range segs {
		if s == "+" {
			segs[i] = "*"
		}
	}
	return strings.Join(segs, ".")
}

// mqttTopicMatch MQTT 通配匹配（+ 单层，# 尾部多层）。
func mqttTopicMatch(filter, topic string) bool {
	f := strings.Split(filter, "/")
	t := strings.Split(topic, "/")
	for i := 0; i < len(f); i++ {
		if f[i] == "#" {
			return i == len(f)-1
		}
		if i >= len(t) {
			return false
		}
		if f[i] == "+" || f[i] == t[i] {
			continue
		}
		return false
	}
	return len(f) == len(t)
}

func sanitizeClientID(id string) string {
	var sb strings.Builder
	for _, r := range id {
		if r == 0 || r == '/' || r == '\n' {
			continue
		}
		sb.WriteRune(r)
	}
	if sb.Len() == 0 {
		sb.WriteString("anon-" + strconv.FormatInt(time.Now().UnixNano()%1e6, 10))
	}
	if sb.Len() > 180 {
		return sb.String()[:180]
	}
	return sb.String()
}

func minI(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// mqttAuth 统一账号认证（同 kv/mq：密码 + 锁定 + host + 审计）。
func mqttAuth(auths *auth.Store, ip, remote, user, pass string) bool {
	u, ok, err := auths.Lookup(user)
	if err != nil || !ok || u.Password != pass {
		auths.RecordLogin(ip, remote, user, false, "mqtt: wrong password")
		return false
	}
	if locked, _ := auths.IPLocked(ip); locked {
		return false
	}
	if locked, _ := auths.AccountLocked(user); locked {
		return false
	}
	if ok, _ := auths.HostOK(user, ip); !ok {
		auths.RecordLogin(ip, remote, user, false, "mqtt: host not allowed")
		return false
	}
	auths.RecordLogin(ip, remote, user, true, "mqtt")
	return true
}
