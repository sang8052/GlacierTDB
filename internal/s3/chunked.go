package s3

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// awsChunkedReader 解码 aws-chunked 传输编码（STREAMING-AWS4-HMAC-SHA256-PAYLOAD）。
// 帧格式：<hex-size>;chunk-signature=<sig>\r\n<data>\r\n ... 0;chunk-signature=<sig>\r\n[trailers]\r\n
// 种子签名已在 Authorization 校验中覆盖（payload hash 为字面量 STREAMING-...），
// 逐块签名此处仅消费不校验（与 MinIO 默认行为一致，HTTPS 下可接受）。
type awsChunkedReader struct {
	br     *bufio.Reader
	remain int64
	done   bool
}

func newAWSChunkedReader(r io.Reader) *awsChunkedReader {
	return &awsChunkedReader{br: bufio.NewReaderSize(r, 32<<10)}
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
		// "size;chunk-signature=..."
		sizePart := line
		if i := strings.IndexByte(line, ';'); i >= 0 {
			sizePart = line[:i]
		}
		size, err := strconv.ParseInt(strings.TrimSpace(sizePart), 16, 64)
		if err != nil {
			return 0, fmt.Errorf("aws-chunked: bad chunk size %q", sizePart)
		}
		if size == 0 {
			// 终止块：消费 trailer 行直到空行
			for {
				t, err := c.br.ReadString('\n')
				if err != nil {
					break
				}
				if strings.TrimRight(t, "\r\n") == "" {
					break
				}
			}
			c.done = true
			break
		}
		c.remain = size
	}
	if c.remain == 0 {
		if c.done {
			return 0, io.EOF
		}
		return 0, io.EOF
	}
	n := len(p)
	if int64(n) > c.remain {
		n = int(c.remain)
	}
	m, err := c.br.Read(p[:n])
	c.remain -= int64(m)
	if err == io.EOF && c.remain == 0 {
		// 数据块后的 \r\n 分隔
		_, _ = c.br.Discard(2)
		return m, nil
	}
	if c.remain == 0 && err == nil {
		_, _ = c.br.Discard(2)
	}
	return m, err
}
