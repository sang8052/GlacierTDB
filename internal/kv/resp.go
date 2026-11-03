// Package kv — Redis 键值/队列基础能力（v1.2.0，FEATURE §3）。
//
// 通过 3309 端口复用的 `mode:redis` 声明行或 RESP 命令嗅探进入本协议栈：
//   - key 类型：string / list 队列；
//   - TTL：惰性过期 + 定期扫描；
//   - 自动落盘：变更写 SQLite（glacier_kv.db，WAL），内存为缓存层，重启无损恢复；
//     v1.3.15 起落盘模式可配（kv.persist_mode）：sync = 每写命令单事务实时落盘（默认）；
//     async = 内存先行 + AsyncFlushMs 周期批量落盘，崩溃丢失 ≤ flush 窗口（见 store.go 包注释）；
//   - 命名空间：每用户独立 namespace（key 按登录账号隔离，跨用户不可见）；
//   - 内存治理：kv.max_memory_mb 上限 + allkeys-lru / volatile-lru / noeviction 淘汰。
package kv

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// maxLineLen 命令行长度防护；maxBulkLen 单个批量参数上限。
const (
	maxLineLen = 512 << 10
	maxBulkLen = 64 << 20
)

// respRWBuf 初始读写缓冲（v1.3.15：32KB → 4KB，万级连接下每连接固定内存减半；
// RespWriter 的累积缓冲按需增长，大值写出后收缩回收）。
const respRWBuf = 4096

// RespReader RESP2 命令读取器（数组形式 + 内联形式）。
type RespReader struct {
	br   *bufio.Reader
	bulk []byte // 复用的 bulk 参数读取缓冲（v1.3.15：避免每参数 make+双重拷贝）
}

// NewRespReader 构造读取器。
func NewRespReader(r io.Reader) *RespReader {
	return &RespReader{br: bufio.NewReaderSize(r, respRWBuf)}
}

// growBulk 确保 bulk 缓冲 ≥ n 字节（2 的幂增长；v1.3.15）。
func (r *RespReader) growBulk(n int) {
	if cap(r.bulk) >= n {
		return
	}
	c := respRWBuf
	for c < n {
		c *= 2
	}
	r.bulk = make([]byte, c)
}

// readLine 读一行并以 string 返回（CRLF/LF 尾去除）。
// v1.3.15：改 ReadSlice 零拷贝快路径（超缓冲的罕见长行走拼接慢路径）。
func (r *RespReader) readLine() (string, error) {
	line, err := r.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		// 慢路径：单缓冲装不下（长内联命令），拼接读取
		big := append([]byte{}, line...)
		for err == bufio.ErrBufferFull {
			line, err = r.br.ReadSlice('\n')
			big = append(big, line...)
		}
		if err != nil {
			return "", err
		}
		if len(big) > maxLineLen+2 {
			return "", fmt.Errorf("protocol error: line too long")
		}
		return string(bytes.TrimRight(big, "\r\n")), nil
	}
	if err != nil {
		return "", err
	}
	if len(line) > maxLineLen+2 {
		return "", fmt.Errorf("protocol error: line too long")
	}
	return string(bytes.TrimRight(line, "\r\n")), nil
}

// ReadCommand 读取一条命令（参数为字符串切片）；连接关闭返回 io.EOF。
// v1.3.15：bulk 参数读进复用缓冲后 string(b) 一次成形（原实现 make([]byte,l+2)
// + string 双重分配/拷贝）；返回的 []string 与内部缓冲无别名（string 不可变），
// 可安全跨 goroutine 传递（独立运算核投递，v1.3.15）。
func (r *RespReader) ReadCommand() ([]string, error) {
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if line == "" {
			continue // 心跳/空行跳过
		}
		if line[0] != '*' {
			// 内联命令：PING / GET key ...（空白切分）
			return strings.Fields(line), nil
		}
		n, err := strconv.Atoi(line[1:])
		if err != nil || n < 0 || n > 1024*1024 {
			return nil, fmt.Errorf("protocol error: invalid multibulk length %q", line[1:])
		}
		args := make([]string, 0, n)
		for i := 0; i < n; i++ {
			hdr, err := r.readLine()
			if err != nil {
				return nil, err
			}
			if len(hdr) == 0 || hdr[0] != '$' {
				return nil, fmt.Errorf("protocol error: expected $, got %q", hdr)
			}
			l, err := strconv.Atoi(hdr[1:])
			if err != nil || l < 0 || l > maxBulkLen {
				return nil, fmt.Errorf("protocol error: invalid bulk length %q", hdr[1:])
			}
			total := l + 2 // 参数 + CRLF
			r.growBulk(total)
			buf := r.bulk[:total]
			if _, err := io.ReadFull(r.br, buf); err != nil {
				return nil, err
			}
			args = append(args, string(buf[:l])) // 唯一一次拷贝（entry.str 存 string，不可避免）
		}
		return args, nil
	}
}

// RespWriter RESP2 应答写入器。
// v1.3.15：bufio.Writer + fmt.Fprintf 换为内部字节切片直接 append（整型走
// strconv.AppendInt），Flush 一次 Write 系统调用写出；公开方法签名不变。
type RespWriter struct {
	dst io.Writer
	buf []byte
	// Resp3：对端曾以 HELLO 3 握手（redis-py ≥8 默认 RESP3 解析器）——该类解析器
	// 读 `$-1` 会按“读 -1 字节”挂死，故空值应答改发 RESP3 统一 null 帧 `_`
	// （v1.3.4，反馈①-补）；未 HELLO/HELLO 2 的 RESP2 连接维持 $-1/*-1。
	Resp3 bool
}

// respBufShrink 写出后缓冲收缩阈值（超过则重建小缓冲，防大值长驻连接内存）。
const respBufShrink = 64 << 10

// NewRespWriter 构造写入器。
func NewRespWriter(w io.Writer) *RespWriter {
	return &RespWriter{dst: w, buf: make([]byte, 0, respRWBuf)}
}

// Simple +OK 形式。
func (w *RespWriter) Simple(s string) {
	w.buf = append(w.buf, '+')
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, '\r', '\n')
}

// Error -ERR 形式（msg 自带前缀如 "ERR xxx" 时原样）。
func (w *RespWriter) Error(msg string) {
	if strings.HasPrefix(msg, "ERR ") || strings.HasPrefix(msg, "NOAUTH") ||
		strings.HasPrefix(msg, "WRONGTYPE") || strings.HasPrefix(msg, "OOM") ||
		strings.HasPrefix(msg, "NOSCRIPT") {
		w.buf = append(w.buf, '-')
		w.buf = append(w.buf, msg...)
		w.buf = append(w.buf, '\r', '\n')
		return
	}
	w.buf = append(w.buf, "-ERR "...)
	w.buf = append(w.buf, msg...)
	w.buf = append(w.buf, '\r', '\n')
}

// Int :n 形式。
func (w *RespWriter) Int(n int64) {
	w.buf = append(w.buf, ':')
	w.buf = strconv.AppendInt(w.buf, n, 10)
	w.buf = append(w.buf, '\r', '\n')
}

// Bulk $.. 形式（nil → $-1；Resp3 连接 → `_` 统一 null，防 RESP3 解析器挂死）。
func (w *RespWriter) Bulk(b []byte) {
	if b == nil {
		w.Null()
		return
	}
	w.buf = append(w.buf, '$')
	w.buf = strconv.AppendInt(w.buf, int64(len(b)), 10)
	w.buf = append(w.buf, '\r', '\n')
	w.buf = append(w.buf, b...)
	w.buf = append(w.buf, '\r', '\n')
}

// BulkStr 字符串便捷（直取 string 字节，无 []byte 转换分配；v1.3.15）。
func (w *RespWriter) BulkStr(s string) {
	w.buf = append(w.buf, '$')
	w.buf = strconv.AppendInt(w.buf, int64(len(s)), 10)
	w.buf = append(w.buf, '\r', '\n')
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, '\r', '\n')
}

// BulkArray 多 bulk 数组（元素 nil → 数组内 nil）。
func (w *RespWriter) BulkArray(items [][]byte) {
	w.buf = append(w.buf, '*')
	w.buf = strconv.AppendInt(w.buf, int64(len(items)), 10)
	w.buf = append(w.buf, '\r', '\n')
	for _, it := range items {
		w.Bulk(it)
	}
}

// NullArray 空数组（*-1，BLPOP/BRPOP 超时应答；Resp3 连接 → `_` 统一 null）。
func (w *RespWriter) NullArray() {
	if w.Resp3 {
		w.buf = append(w.buf, "_\r\n"...)
		return
	}
	w.buf = append(w.buf, "*-1\r\n"...)
}

// Null 空值应答：RESP2 `$-1` / RESP3 `_`（按连接协商形态，v1.3.4）。
func (w *RespWriter) Null() {
	if w.Resp3 {
		w.buf = append(w.buf, "_\r\n"...)
		return
	}
	w.buf = append(w.buf, "$-1\r\n"...)
}

// ScanReply SCAN 应答：[cursor, [keys...]]（cursor 为 bulk、keys 为数组，真实 Redis 形态，v1.3.3）。
func (w *RespWriter) ScanReply(cursor string, keys [][]byte) {
	w.buf = append(w.buf, "*2\r\n"...)
	w.BulkStr(cursor)
	w.buf = append(w.buf, '*')
	w.buf = strconv.AppendInt(w.buf, int64(len(keys)), 10)
	w.buf = append(w.buf, '\r', '\n')
	for _, k := range keys {
		w.Bulk(k)
	}
}

// Flush 提交缓冲（一次 Write；大值写出后收缩缓冲，v1.3.15）。
func (w *RespWriter) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	_, err := w.dst.Write(w.buf)
	if cap(w.buf) > respBufShrink {
		w.buf = make([]byte, 0, respRWBuf) // 防 64MB 大值把连接缓冲长驻放大
	} else {
		w.buf = w.buf[:0]
	}
	return err
}
