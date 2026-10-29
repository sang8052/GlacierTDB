package s3

import (
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// gzip 传输压缩（v1.3.13 §1.3，非存储侧：存储与计费 storage/bytes_in 口径不变，
// bytes_out 按实际线上字节 = 压缩后计）
//
// 头语义（专为反代场景设计）：请求头 x-glaciertdb-gzip: 0|1 —— nginx / CDN 等
// 代理层已开启 gzip 时，由代理配置注入 0 告知本端「不要重复压缩，省性能」；
// 缺省 / 异常值一律按 1（开启压缩）。该头不参与 SigV4 签名（代理层可自由注入
// 而不破坏鉴权——这正是做成非签名的原因）。压缩等级走系统配置 s3.gzip_level。
//
// 生效条件（全部满足才压）：
//   1. Accept-Encoding 允许 gzip；
//   2. x-glaciertdb-gzip != 0；
//   3. content-type 可压（text/json/xml/csv 等白名单）+ 字节魔数嗅探兜底
//      （图片 / 已压缩内容 magic 跳过）；
//   4. 非 Range 请求（gzip 与 206 不混用，Range 一律 identity）；
//   5. 非 HEAD / 对象自身无 Content-Encoding（已编码不再二次压）；
//   6. x-gimg 图片处理结果跳过（本身即图片）。
//
// 注意与时序库 COMPRESS COLD 区分（那是 TSDB 热表压缩，互不相关）。
// ---------------------------------------------------------------------------

// gzCompressibleTypes content-type 压缩白名单（前缀匹配）。
var gzCompressibleTypes = []string{
	"text/", "application/json", "application/xml", "application/javascript",
	"application/x-yaml", "application/yaml", "application/toml",
	"application/x-www-form-urlencoded", "application/sql", "application/csv",
	"image/svg+xml", "application/rss+xml", "application/atom+xml",
}

// compressedMagics 已压缩内容的字节魔数（嗅探兜底；命中即跳过压缩）。
var compressedMagics = [][]byte{
	{0x1f, 0x8b},                       // gzip
	{'P', 'K', 0x03, 0x04},             // zip
	{'P', 'K', 0x05, 0x06},             // zip（空包）
	{0x78, 0x01}, {0x78, 0x9c}, {0x78, 0xda}, // zlib
	{0x28, 0xb5, 0x2f, 0xfd},           // zstd
	{'B', 'Z', 'h'},                    // bzip2
	{0xfd, '7', 'z', 'X', 'Z', 0x00},   // xz
	{'7', 'z', 0xbc, 0xaf, 0x27, 0x1c}, // 7z
	{0x04, 0x22, 0x4d, 0x18},           // lz4
}

// gzWanted 判定该 GET 请求 + 对象是否走 gzip 输出。
func gzWanted(r *http.Request, contentType, contentEncoding string, isRange, headOnly, hasImg bool) bool {
	if isRange || headOnly || hasImg {
		return false
	}
	if contentEncoding != "" {
		return false // 对象自身已编码（gzip/br/aws-chunked 残留），不再二次压
	}
	if r.Header.Get("x-glaciertdb-gzip") == "0" { // 代理层显式关闭；缺省/异常值一律按开
		return false
	}
	ae := r.Header.Get("Accept-Encoding")
	if !strings.Contains(strings.ToLower(ae), "gzip") {
		return false
	}
	ct := strings.ToLower(strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0]))
	if ct == "" {
		return false
	}
	for _, p := range gzCompressibleTypes {
		if strings.HasPrefix(ct, p) {
			return true
		}
	}
	return false
}

// gzSkipByMagic 嗅探首字节：命中已压缩内容魔数 → 跳过压缩（调用方把嗅探字节
// 前置回读流）。
func gzSkipByMagic(head []byte) bool {
	for _, m := range compressedMagics {
		if len(head) >= len(m) && string(head[:len(m)]) == string(m) {
			return true
		}
	}
	return false
}

// sniffReader 预读嗅探包装：Read 先吐出嗅探缓冲再透传底层流。
type sniffReader struct {
	r    io.Reader
	head []byte
}

func (s *sniffReader) Read(p []byte) (int, error) {
	if len(s.head) > 0 {
		n := copy(p, s.head)
		s.head = s.head[n:]
		return n, nil
	}
	return s.r.Read(p)
}

// gzipWriterChain 组装 gzip 输出链：gzip.Writer(level) 包在限速层之上
//（限速计压缩后字节 = 实际线上流量）。调用方负责 Close() 冲刷尾部。
func newGzipWriter(w io.Writer, level int) *gzip.Writer {
	if level < 1 || level > 9 {
		level = 5
	}
	zw, err := gzip.NewWriterLevel(w, level)
	if err != nil {
		zw = gzip.NewWriter(w)
	}
	return zw
}

// setGzipHeaders 压缩路径响应头（Content-Length 不可预知 → 走 chunked）。
func setGzipHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Add("Vary", "Accept-Encoding")
	w.Header().Del("Content-Length")
}

// parseGzipLevel 配置值防御（1-9，默认 5）。
func parseGzipLevel(v int) int {
	if v < 1 || v > 9 {
		return 5
	}
	return v
}

// itoaLen Content-Length 快捷。
func itoaLen(n int) string { return strconv.Itoa(n) }
