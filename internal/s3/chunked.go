package s3

import (
	"bufio"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"hash"
	"io"
	"strconv"
	"strings"
)

// awsChunkedReader 解码 aws-chunked 传输编码（v1.3.15 S11 重写）。
// 帧格式：<hex-size>;chunk-signature=<sig>\r\n<data>\r\n ... 0;chunk-signature=<sig>\r\n[trailers]\r\n
//
// v1.3.15 S11：签名流式变体（x-amz-content-sha256 = STREAMING-AWS4-HMAC-SHA256-
// PAYLOAD[-TRAILER]）的逐块签名**真实校验**——此前注释自认「仅消费不校验」，
// 攻击者可在种子签名合法后篡改任意块内容/尺寸。现在：
//   - 每块读毕即按 SigV4 流式规范复算签名（chunkSigner 签名链：首块种子 =
//     请求整体签名，其后逐块推进），与帧内声明不符 → 403 XAmzContentSHA256Mismatch；
//   - 块帧缺 chunk-signature 参数同样拒绝（签名变体必须逐块签名）；
//   - x-amz-trailer（STREAMING-UNSIGNED-PAYLOAD-TRAILER）变体的块帧本就无签名，
//     不返回校验器（sigv4.go），trailer 段照旧消费跳过；
//   - 终块（size=0）以空负载哈希参与签名链校验。
//
// 校验时机：块尾（增量 sha256 流过 Read 零额外拷贝）；错误在 PutObject 消费
// reader 时向上传播，写事务不会提交——已流出字节不会成为对象内容。
type awsChunkedReader struct {
	br     *bufio.Reader
	remain int64
	done   bool

	// v1.3.15 S11：逐块签名校验（nil = 未签名变体，不校验）
	signer   *chunkSigner
	chunkSum hash.Hash // 当前块增量 sha256
	chunkSig string    // 当前块帧内声明签名
}

// newAWSChunkedReader 构造（csig 非 nil 时启用逐块签名校验，v1.3.15 S11）。
func newAWSChunkedReader(r io.Reader, csig *chunkSigner) *awsChunkedReader {
	return &awsChunkedReader{br: bufio.NewReaderSize(r, 32<<10), signer: csig}
}

// errChunkSig 块签名校验失败（SigV4 流式规范等价错误）。
func errChunkSig(reason string) error {
	return &s3Error{Code: "XAmzContentSHA256Mismatch", Status: 403,
		Msg: "The provided chunk signature does not match the calculated signature (" + reason + ")"}
}

// verifyChunk 校验刚读完的块签名并推进签名链（final=true = 终块空负载）。
func (c *awsChunkedReader) verifyChunk(declared string, final bool) error {
	if strings.TrimSpace(declared) == "" {
		return errChunkSig("chunk-signature missing")
	}
	sum := payloadEmpty
	if !final && c.chunkSum != nil {
		sum = hex.EncodeToString(c.chunkSum.Sum(nil))
	}
	want := c.signer.sign(sum)
	// 常量时间比较（签名链推进前先比较——失败即断流，prev 保持种子语义无碍）
	if subtle.ConstantTimeCompare([]byte(want), []byte(strings.ToLower(declared))) != 1 {
		return errChunkSig("mismatch")
	}
	return nil
}

func (c *awsChunkedReader) Read(p []byte) (int, error) {
	for c.remain == 0 && !c.done {
		line, err := c.br.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" { // 尾部多余空行
			continue
		}
		// "size;chunk-signature=..."（可能还有其他 ; 参数，忽略）
		sizePart := line
		sig := ""
		if i := strings.IndexByte(line, ';'); i >= 0 {
			sizePart = line[:i]
			for _, kv := range strings.Split(line[i+1:], ";") {
				if v, ok := strings.CutPrefix(kv, "chunk-signature="); ok {
					sig = v
				}
			}
		}
		size, err := strconv.ParseInt(strings.TrimSpace(sizePart), 16, 64)
		if err != nil {
			// (v1.3.15) 帧损坏：显式 400（此前为 500 透内部错误）
			return 0, &s3Error{Code: "InvalidRequest", Status: 400,
				Msg: "aws-chunked: bad chunk size"}
		}
		if size == 0 {
			// 终止块：先校验终块签名（空负载），再消费 trailer 行直到空行
			if c.signer != nil {
				if verr := c.verifyChunk(sig, true); verr != nil {
					c.done = true
					return 0, verr
				}
			}
			for {
				t, terr := c.br.ReadString('\n')
				if terr != nil {
					break
				}
				if strings.TrimRight(t, "\r\n") == "" {
					break
				}
			}
			c.done = true
			break
		}
		if c.signer != nil { // (v1.3.15 S11) 签名变体：块必须带签名
			c.chunkSum = sha256.New()
			c.chunkSig = sig
			if strings.TrimSpace(sig) == "" {
				return 0, errChunkSig("chunk-signature missing")
			}
		}
		c.remain = size
	}
	if c.remain == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > c.remain {
		n = int(c.remain)
	}
	m, err := c.br.Read(p[:n])
	c.remain -= int64(m)
	if c.signer != nil && m > 0 {
		c.chunkSum.Write(p[:m])
	}
	if err == io.EOF && c.remain == 0 {
		// 数据块后的 \r\n 分隔
		_, _ = c.br.Discard(2)
		if c.signer != nil {
			if verr := c.verifyChunk(c.chunkSig, false); verr != nil {
				return m, verr
			}
		}
		return m, nil
	}
	if c.remain == 0 && err == nil {
		_, _ = c.br.Discard(2)
		if c.signer != nil {
			if verr := c.verifyChunk(c.chunkSig, false); verr != nil {
				return m, verr
			}
		}
	}
	return m, err
}
