// Package mq — RabbitMQ 能力（v1.3.0，FEATURE §4）：3309 同端口 AMQP 0-9-1
// 基本队列——exchange（direct/fanout/topic）+ queue + binding 声明绑定、
// basic.publish / consume / ack / nack / reject、prefetch、持久化消息，
// 消息与消费游标落 SQLite（glacier_mq.db）重启不丢；管理面 SHOW QUEUES / SHOW EXCHANGES。
// 定位：基本队列通知 / 分发，不做镜像集群、不做事务性队列。
package mq

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// ---------------------------------------------------------------------------
// 帧层：type(1) + channel(2) + size(3) + payload + 0xCE
// ---------------------------------------------------------------------------

const (
	frameMethod    = 1
	frameHeader    = 2
	frameBody      = 3
	frameHeartbeat = 8
	frameEnd       = 0xCE
)

type frame struct {
	typ     byte
	channel uint16
	payload []byte
}

func readFrame(r io.Reader) (frame, error) {
	var f frame
	hdr := make([]byte, 7)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return f, err
	}
	f.typ = hdr[0]
	f.channel = binary.BigEndian.Uint16(hdr[1:3])
	size := int(uint32(hdr[4])<<16 | uint32(hdr[5])<<8 | uint32(hdr[6]))
	if size > 16<<20 {
		return f, fmt.Errorf("amqp: frame too large (%d)", size)
	}
	body := make([]byte, size+1)
	if _, err := io.ReadFull(r, body); err != nil {
		return f, err
	}
	if body[size] != frameEnd {
		return f, fmt.Errorf("amqp: bad frame end 0x%02x", body[size])
	}
	f.payload = body[:size]
	return f, nil
}

func writeFrame(w io.Writer, f frame) error {
	hdr := make([]byte, 7)
	hdr[0] = f.typ
	binary.BigEndian.PutUint16(hdr[1:3], f.channel)
	n := len(f.payload)
	hdr[4] = byte(n >> 16)
	hdr[5] = byte(n >> 8)
	hdr[6] = byte(n)
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	if n > 0 {
		if _, err := w.Write(f.payload); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte{frameEnd})
	return err
}

// ---------------------------------------------------------------------------
// 字段编码：octet/short/long/longlong/shortstr/longstr/table/bit
// ---------------------------------------------------------------------------

type encBuf struct{ b []byte }

func (e *encBuf) octet(v byte)   { e.b = append(e.b, v) }
func (e *encBuf) short(v uint16) { e.b = append(e.b, byte(v>>8), byte(v)) }
func (e *encBuf) long(v uint32)  { e.b = append(e.b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v)) }
func (e *encBuf) longlong(v uint64) {
	e.b = append(e.b, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func (e *encBuf) shortstr(s string) {
	if len(s) > 255 {
		s = s[:255]
	}
	e.b = append(e.b, byte(len(s)))
	e.b = append(e.b, s...)
}
func (e *encBuf) longstr(s []byte) {
	e.long(uint32(len(s)))
	e.b = append(e.b, s...)
}

// table 字段表编码（key → type + value）。
func (e *encBuf) table(t map[string]any) {
	sub := &encBuf{}
	for k, v := range t {
		sub.shortstr(k)
		switch x := v.(type) {
		case bool:
			sub.octet('t')
			if x {
				sub.octet(1)
			} else {
				sub.octet(0)
			}
		case string:
			sub.octet('S')
			sub.longstr([]byte(x))
		case []byte:
			sub.octet('x')
			sub.longstr(x)
		case int:
			sub.octet('I')
			sub.long(uint32(int32(x)))
		case int32:
			sub.octet('I')
			sub.long(uint32(x))
		case int64:
			sub.octet('l')
			sub.longlong(uint64(x))
		case float64:
			sub.octet('d')
			e.f64(sub, x)
		case map[string]any:
			sub.octet('F')
			sub.table(x)
		case []any:
			sub.octet('A')
			inner := &encBuf{}
			for _, it := range x {
				inner.tableVal(it)
			}
			sub.long(uint32(len(inner.b)))
			sub.b = append(sub.b, inner.b...)
		default:
			sub.octet('V') // void / 未知 → 空值
		}
	}
	e.long(uint32(len(sub.b)))
	e.b = append(e.b, sub.b...)
}

func (e *encBuf) f64(sub *encBuf, x float64) {
	bits := math.Float64bits(x)
	sub.longlong(bits)
}

func (e *encBuf) tableVal(v any) {
	switch x := v.(type) {
	case string:
		e.octet('S')
		e.longstr([]byte(x))
	case bool:
		e.octet('t')
		if x {
			e.octet(1)
		} else {
			e.octet(0)
		}
	case int:
		e.octet('I')
		e.long(uint32(int32(x)))
	default:
		e.octet('V')
	}
}

type decBuf struct{ b []byte; off int }

func (d *decBuf) need(n int) error {
	if d.off+n > len(d.b) {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (d *decBuf) octet() (byte, error) {
	if err := d.need(1); err != nil {
		return 0, err
	}
	v := d.b[d.off]
	d.off++
	return v, nil
}
func (d *decBuf) short() (uint16, error) {
	if err := d.need(2); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint16(d.b[d.off:])
	d.off += 2
	return v, nil
}
func (d *decBuf) long() (uint32, error) {
	if err := d.need(4); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint32(d.b[d.off:])
	d.off += 4
	return v, nil
}
func (d *decBuf) longlong() (uint64, error) {
	if err := d.need(8); err != nil {
		return 0, err
	}
	v := binary.BigEndian.Uint64(d.b[d.off:])
	d.off += 8
	return v, nil
}
func (d *decBuf) shortstr() (string, error) {
	l, err := d.octet()
	if err != nil {
		return "", err
	}
	if err := d.need(int(l)); err != nil {
		return "", err
	}
	s := string(d.b[d.off : d.off+int(l)])
	d.off += int(l)
	return s, nil
}
func (d *decBuf) longstr() ([]byte, error) {
	l, err := d.long()
	if err != nil {
		return nil, err
	}
	if err := d.need(int(l)); err != nil {
		return nil, err
	}
	s := d.b[d.off : d.off+int(l)]
	d.off += int(l)
	return s, nil
}

// table 解码字段表（未知类型优雅跳过）。
func (d *decBuf) table() (map[string]any, error) {
	l, err := d.long()
	if err != nil {
		return nil, err
	}
	end := d.off + int(l)
	if end > len(d.b) {
		return nil, io.ErrUnexpectedEOF
	}
	out := map[string]any{}
	for d.off < end {
		k, err := d.shortstr()
		if err != nil {
			return out, nil
		}
		v, err := d.tableValue()
		if err != nil {
			return out, nil
		}
		out[k] = v
	}
	return out, nil
}

func (d *decBuf) tableValue() (any, error) {
	t, err := d.octet()
	if err != nil {
		return nil, err
	}
	switch t {
	case 't':
		b, err := d.octet()
		return b != 0, err
	case 'b':
		b, err := d.octet()
		return int8(b), err
	case 'B':
		return d.octet()
	case 's':
		v, err := d.short()
		return int16(v), err
	case 'U':
		return d.short()
	case 'u':
		return d.long()
	case 'i', 'I':
		v, err := d.long()
		return int32(v), err
	case 'l':
		v, err := d.longlong()
		return int64(v), err
	case 'L':
		return d.longlong()
	case 'f':
		v, err := d.long()
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(v), nil
	case 'd':
		v, err := d.longlong()
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(v), nil
	case 'D':
		// decimal：scale(1) + value(4)
		if _, err := d.octet(); err != nil {
			return nil, err
		}
		v, err := d.long()
		return v, err
	case 'S':
		s, err := d.longstr()
		return string(s), err
	case 'x':
		return d.longstr()
	case 'T':
		return d.longlong()
	case 'F':
		return d.table()
	case 'A':
		l, err := d.long()
		if err != nil {
			return nil, err
		}
		end := d.off + int(l)
		var arr []any
		for d.off < end {
			v, err := d.tableValue()
			if err != nil {
				break
			}
			arr = append(arr, v)
		}
		return arr, nil
	case 'V':
		return nil, nil
	default:
		return nil, fmt.Errorf("amqp: unknown field type %q", t)
	}
}

// ---------------------------------------------------------------------------
// 方法定义（class:method → 参数规格）
// ---------------------------------------------------------------------------

// 参数类型码：o octet / h short / i long / L longlong / s shortstr / S longstr / t table / b bit
type methodSpec struct {
	name string
	args string // 如 "hsttbbbSt"：逐字符参数类型（bit 连续打包）
}

var methodSpecs = map[uint32]methodSpec{
	key(10, 10): {"Connection.Start", "ootSS"},
	key(10, 11): {"Connection.Start-Ok", "tsSs"},
	key(10, 30): {"Connection.Tune", "hih"},
	key(10, 31): {"Connection.Tune-Ok", "hih"},
	key(10, 40): {"Connection.Open", "sob"},
	key(10, 41): {"Connection.Open-Ok", "s"},
	key(10, 50): {"Connection.Close", "hshh"},
	key(10, 51): {"Connection.Close-Ok", ""},
	key(20, 10): {"Channel.Open", "o"},
	key(20, 11): {"Channel.Open-Ok", "S"},
	key(20, 20): {"Channel.Flow", "b"},
	key(20, 21): {"Channel.Flow-Ok", "b"},
	key(20, 40): {"Channel.Close", "hshh"},
	key(20, 41): {"Channel.Close-Ok", ""}, // 0-9-1 规范：CloseOk=41（51 是 Connection.Close-Ok）
	key(40, 10): {"Exchange.Declare", "hssbbbbbt"},
	key(40, 11): {"Exchange.Declare-Ok", ""},
	key(40, 20): {"Exchange.Delete", "hsbb"},
	key(40, 21): {"Exchange.Delete-Ok", ""},
	key(50, 10): {"Queue.Declare", "hsbbbbbt"},
	key(50, 11): {"Queue.Declare-Ok", "sii"}, // queue shortstr, message-count long, consumer-count long
	key(50, 20): {"Queue.Bind", "hsssbt"},
	key(50, 21): {"Queue.Bind-Ok", ""},
	key(50, 30): {"Queue.Purge", "hsb"},
	key(50, 31): {"Queue.Purge-Ok", "i"},
	key(50, 40): {"Queue.Delete", "hsbbb"},
	key(50, 41): {"Queue.Delete-Ok", "i"},
	key(50, 50): {"Queue.Unbind", "hssst"},
	key(50, 51): {"Queue.Unbind-Ok", ""},
	key(60, 10): {"Basic.Qos", "ihb"},
	key(60, 11): {"Basic.Qos-Ok", ""},
	key(60, 20): {"Basic.Consume", "hssbbbbbt"},
	key(60, 21): {"Basic.Consume-Ok", "s"},
	key(60, 30): {"Basic.Cancel", "sb"},
	key(60, 31): {"Basic.Cancel-Ok", "s"},
	key(60, 40): {"Basic.Publish", "hssbb"},
	key(60, 50): {"Basic.Return", "hsss"},
	key(60, 60): {"Basic.Deliver", "sLbss"},
	key(60, 70): {"Basic.Get", "hsb"},
	key(60, 71): {"Basic.Get-Ok", "Lbssi"},
	key(60, 72): {"Basic.Get-Empty", "s"},
	key(60, 80): {"Basic.Ack", "Lb"},
	key(60, 90): {"Basic.Reject", "Lb"},
	key(60, 110): {"Basic.Recover", "b"},
	key(60, 111): {"Basic.Recover-Ok", ""},
	key(60, 120): {"Basic.Nack", "Lbb"},
	key(85, 10): {"Confirm.Select", "b"},
	key(85, 11): {"Confirm.Select-Ok", ""},
	key(90, 10): {"Tx.Select", ""},
	key(90, 11): {"Tx.Select-Ok", ""},
	key(90, 20): {"Tx.Commit", ""},
	key(90, 21): {"Tx.Commit-Ok", ""},
	key(90, 30): {"Tx.Rollback", ""},
	key(90, 31): {"Tx.Rollback-Ok", ""},
}

func key(class, method uint16) uint32 { return uint32(class)<<16 | uint32(method) }

// methodMsg 方法载荷。
type methodMsg struct {
	class  uint16
	method uint16
	args   []any
}

// encodeMethod 编码方法帧载荷。
func (m *methodMsg) encode() ([]byte, error) {
	spec, ok := methodSpecs[key(m.class, m.method)]
	if !ok {
		return nil, fmt.Errorf("amqp: unsupported method %d.%d", m.class, m.method)
	}
	e := &encBuf{}
	e.short(m.class)
	e.short(m.method)
	ai := 0
	bitVal := byte(0)
	bitPos := 0
	flushBits := func() {
		if bitPos > 0 {
			e.b = append(e.b, bitVal)
			bitVal, bitPos = 0, 0
		}
	}
	for i := 0; i < len(spec.args); i++ {
		t := spec.args[i]
		if ai >= len(m.args) {
			return nil, fmt.Errorf("amqp: too few args for %s", spec.name)
		}
		v := m.args[ai]
		ai++
		switch t {
		case 'o':
			flushBits()
			switch x := v.(type) {
			case byte:
				e.octet(x)
			case int:
				e.octet(byte(x))
			default:
				return nil, fmt.Errorf("amqp: arg type")
			}
		case 'h':
			flushBits()
			e.short(toU16(v))
		case 'i':
			flushBits()
			e.long(toU32(v))
		case 'L':
			flushBits()
			e.longlong(toU64(v))
		case 's':
			flushBits()
			e.shortstr(toStr(v))
		case 'S':
			flushBits()
			if b, ok := v.([]byte); ok {
				e.longstr(b)
			} else {
				e.longstr([]byte(toStr(v)))
			}
		case 't':
			flushBits()
			if tb, ok := v.(map[string]any); ok {
				e.table(tb)
			} else {
				e.table(map[string]any{})
			}
		case 'b':
			if toBool(v) {
				bitVal |= 1 << bitPos
			}
			bitPos++
			if bitPos == 8 {
				flushBits()
			}
		}
	}
	flushBits()
	return e.b, nil
}

// decodeMethod 解码方法帧载荷。
func decodeMethod(payload []byte) (*methodMsg, error) {
	d := &decBuf{b: payload}
	cid, err := d.short()
	if err != nil {
		return nil, err
	}
	mid, err := d.short()
	if err != nil {
		return nil, err
	}
	spec, ok := methodSpecs[key(cid, mid)]
	if !ok {
		return &methodMsg{class: cid, method: mid}, nil // 未实现方法：交给上层报错
	}
	m := &methodMsg{class: cid, method: mid}
	bitPos := 0
	bitVal := byte(0)
	for i := 0; i < len(spec.args); i++ {
		switch spec.args[i] {
		case 'o':
			bitPos = 0
			v, err := d.octet()
			if err != nil {
				return nil, err
			}
			m.args = append(m.args, v)
		case 'h':
			bitPos = 0
			v, err := d.short()
			if err != nil {
				return nil, err
			}
			m.args = append(m.args, v)
		case 'i':
			bitPos = 0
			v, err := d.long()
			if err != nil {
				return nil, err
			}
			m.args = append(m.args, v)
		case 'L':
			bitPos = 0
			v, err := d.longlong()
			if err != nil {
				return nil, err
			}
			m.args = append(m.args, v)
		case 's':
			bitPos = 0
			v, err := d.shortstr()
			if err != nil {
				return nil, err
			}
			m.args = append(m.args, v)
		case 'S':
			bitPos = 0
			v, err := d.longstr()
			if err != nil {
				return nil, err
			}
			m.args = append(m.args, v)
		case 't':
			bitPos = 0
			v, err := d.table()
			if err != nil {
				return nil, err
			}
			m.args = append(m.args, v)
		case 'b':
			if bitPos == 0 {
				v, err := d.octet()
				if err != nil {
					return nil, err
				}
				bitVal = v
			}
			m.args = append(m.args, bitVal&(1<<bitPos) != 0)
			bitPos = (bitPos + 1) % 8
		}
	}
	return m, nil
}

// ---------------------------------------------------------------------------
// 内容头（class-id + weight + body-size + 属性原样透传）
// ---------------------------------------------------------------------------

type contentHeader struct {
	bodySize uint64
	props    []byte // property-flags + property-fields（原样存储与透传）
}

func decodeContentHeader(payload []byte) (contentHeader, error) {
	d := &decBuf{b: payload}
	if _, err := d.short(); err != nil { // class-id
		return contentHeader{}, err
	}
	if _, err := d.short(); err != nil { // weight
		return contentHeader{}, err
	}
	size, err := d.longlong()
	if err != nil {
		return contentHeader{}, err
	}
	return contentHeader{bodySize: size, props: d.b[d.off:]}, nil
}

func (h contentHeader) encode() []byte {
	e := &encBuf{}
	e.short(60) // Basic
	e.short(0)
	e.longlong(h.bodySize)
	if len(h.props) >= 2 {
		e.b = append(e.b, h.props...)
	} else {
		e.short(0) // property-flags 至少 2 字节（MQTT 桥接消息无属性时的兜底，v1.3.3）
	}
	return e.b
}

// ---------------------------------------------------------------------------
// 辅助转换
// ---------------------------------------------------------------------------

func toU16(v any) uint16 {
	switch x := v.(type) {
	case uint16:
		return x
	case int:
		return uint16(x)
	case uint32:
		return uint16(x)
	case byte:
		return uint16(x)
	}
	return 0
}

func toU32(v any) uint32 {
	switch x := v.(type) {
	case uint32:
		return x
	case int:
		return uint32(x)
	case uint16:
		return uint32(x)
	case int64:
		return uint32(x)
	}
	return 0
}

func toU64(v any) uint64 {
	switch x := v.(type) {
	case uint64:
		return x
	case int64:
		return uint64(x)
	case int:
		return uint64(x)
	}
	return 0
}

func toStr(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	}
	return ""
}

func toBool(v any) bool {
	b, _ := v.(bool)
	return b
}
