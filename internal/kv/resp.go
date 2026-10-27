// Package kv — Redis 键值/队列基础能力（v1.2.0，FEATURE §3）。
//
// 通过 3309 端口复用的 `mode:redis` 声明行或 RESP 命令嗅探进入本协议栈：
//   - key 类型：string / list 队列；
//   - TTL：惰性过期 + 定期扫描；
//   - 自动落盘：变更实时写 SQLite（glacier_kv.db，WAL），内存为缓存层，重启无损恢复；
//   - 命名空间：每用户独立 namespace（key 按登录账号隔离，跨用户不可见）；
//   - 内存治理：kv.max_memory_mb 上限 + allkeys-lru / volatile-lru / noeviction 淘汰。
package kv

import (
	"bufio"
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

// RespReader RESP2 命令读取器（数组形式 + 内联形式）。
type RespReader struct {
	br * bufio.Reader
}

// NewRespReader 构造读取器。
func NewRespReader(r io.Reader) *RespReader { return &RespReader{br: bufio.NewReaderSize(r, 32<<10)} }

func (r *RespReader) readLine() (string, error) {
	line, err := r.br.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) > maxLineLen {
		return "", fmt.Errorf("protocol error: line too long")
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// ReadCommand 读取一条命令（参数为字符串切片）；连接关闭返回 io.EOF。
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
			buf := make([]byte, l+2)
			if _, err := io.ReadFull(r.br, buf); err != nil {
				return nil, err
			}
			args = append(args, string(buf[:l]))
		}
		return args, nil
	}
}

// RespWriter RESP2 应答写入器（每条命令一次 Flush）。
type RespWriter struct {
	bw *bufio.Writer
	// Resp3：对端曾以 HELLO 3 握手（redis-py ≥8 默认 RESP3 解析器）——该类解析器
	// 读 `$-1` 会按“读 -1 字节”挂死，故空值应答改发 RESP3 统一 null 帧 `_`
	// （v1.3.4，反馈①-补）；未 HELLO/HELLO 2 的 RESP2 连接维持 $-1/*-1。
	Resp3 bool
}

// NewRespWriter 构造写入器。
func NewRespWriter(w io.Writer) *RespWriter { return &RespWriter{bw: bufio.NewWriterSize(w, 32<<10)} }

// Simple +OK 形式。
func (w *RespWriter) Simple(s string) { fmt.Fprintf(w.bw, "+%s\r\n", s) }

// Error -ERR 形式（msg 自带前缀如 "ERR xxx" 时原样）。
func (w *RespWriter) Error(msg string) {
	if strings.HasPrefix(msg, "ERR ") || strings.HasPrefix(msg, "NOAUTH") ||
		strings.HasPrefix(msg, "WRONGTYPE") || strings.HasPrefix(msg, "OOM") ||
		strings.HasPrefix(msg, "NOSCRIPT") {
		fmt.Fprintf(w.bw, "-%s\r\n", msg)
		return
	}
	fmt.Fprintf(w.bw, "-ERR %s\r\n", msg)
}

// Int :n 形式。
func (w *RespWriter) Int(n int64) { fmt.Fprintf(w.bw, ":%d\r\n", n) }

// Bulk $.. 形式（nil → $-1；Resp3 连接 → `_` 统一 null，防 RESP3 解析器挂死）。
func (w *RespWriter) Bulk(b []byte) {
	if b == nil {
		w.Null()
		return
	}
	fmt.Fprintf(w.bw, "$%d\r\n", len(b))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

// BulkStr 字符串便捷。
func (w *RespWriter) BulkStr(s string) { w.Bulk([]byte(s)) }

// BulkArray 多 bulk 数组（元素 nil → 数组内 nil）。
func (w *RespWriter) BulkArray(items [][]byte) {
	fmt.Fprintf(w.bw, "*%d\r\n", len(items))
	for _, it := range items {
		w.Bulk(it)
	}
}

// NullArray 空数组（*-1，BLPOP/BRPOP 超时应答；Resp3 连接 → `_` 统一 null）。
func (w *RespWriter) NullArray() {
	if w.Resp3 {
		w.bw.WriteString("_\r\n")
		return
	}
	w.bw.WriteString("*-1\r\n")
}

// Null 空值应答：RESP2 `$-1` / RESP3 `_`（按连接协商形态，v1.3.4）。
func (w *RespWriter) Null() {
	if w.Resp3 {
		w.bw.WriteString("_\r\n")
		return
	}
	w.bw.WriteString("$-1\r\n")
}

// ScanReply SCAN 应答：[cursor, [keys...]]（cursor 为 bulk、keys 为数组，真实 Redis 形态，v1.3.3）。
func (w *RespWriter) ScanReply(cursor string, keys [][]byte) {
	fmt.Fprintf(w.bw, "*2\r\n")
	w.BulkStr(cursor)
	fmt.Fprintf(w.bw, "*%d\r\n", len(keys))
	for _, k := range keys {
		w.Bulk(k)
	}
}

// Flush 提交缓冲。
func (w *RespWriter) Flush() error { return w.bw.Flush() }
