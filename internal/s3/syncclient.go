package s3

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// 出站 SigV4 请求签名（v1.3.13：数据同步引擎推送远端 S3 端点用）。
// 与 verifySigV4（入站校验）对应：签名 host + 所有 x-amz-* 头 + content-type
//（存在时）；payload hash 传入（未知长度流式用 UNSIGNED-PAYLOAD）。
// ---------------------------------------------------------------------------

// signV4Request 给出站请求签名（原地写 Authorization / x-amz-date /
// x-amz-content-sha256 头）。payloadHash 空 = UNSIGNED-PAYLOAD。
func signV4Request(req *http.Request, ak, sk, region, payloadHash string) {
	if payloadHash == "" {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	if region == "" {
		region = "us-east-1"
	}
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateShort := now.Format("20060102")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)

	host := req.URL.Host
	if host == "" {
		host = req.Host
	}
	if req.URL.Port() == "80" || req.URL.Port() == "443" {
		// 标准端口不进签名 host（与 AWS SDK 语义一致；verifySigV4 同样剥离）
		if h, _, err := splitHostPort(host); err == nil {
			host = h
		}
	}

	// 签名头集合：host + content-type（存在时）+ 全部 x-amz-*（含 meta / date / sha256）
	headers := map[string]string{"host": host}
	signedList := []string{"host"}
	if ct := req.Header.Get("Content-Type"); ct != "" {
		headers["content-type"] = ct
		signedList = append(signedList, "content-type")
	}
	for k, vs := range req.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-") && len(vs) > 0 {
			headers[lk] = strings.TrimSpace(vs[0])
			signedList = append(signedList, lk)
		}
	}
	sort.Strings(signedList)

	var canonHeaders strings.Builder
	for _, h := range signedList {
		canonHeaders.WriteString(h)
		canonHeaders.WriteByte(':')
		canonHeaders.WriteString(headers[h])
		canonHeaders.WriteByte('\n')
	}

	// canonical query
	q := req.URL.Query()
	var keys []string
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var canonQuery strings.Builder
	for i, k := range keys {
		if i > 0 {
			canonQuery.WriteByte('&')
		}
		canonQuery.WriteString(uriEncode(k, true))
		canonQuery.WriteByte('=')
		vals := q[k]
		sort.Strings(vals)
		canonQuery.WriteString(uriEncode(vals[0], true))
	}

	canonicalRequest := strings.Join([]string{
		req.Method,
		uriEncodePath(req.URL.EscapedPath()),
		canonQuery.String(),
		canonHeaders.String(),
		strings.Join(signedList, ";"),
		payloadHash,
	}, "\n")

	scope := dateShort + "/" + region + "/s3/aws4_request"
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+sk), []byte(dateShort))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		ak, scope, strings.Join(signedList, ";"), signature))
}

// uriEncode S3 SigV4 URI 编码（encodeSlash=true 用于 path 之外；path 用 false
// 保留 '/'）。
func uriEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// uriEncodePath 规范化 path 编码（'/' 保留）。
func uriEncodePath(p string) string {
	if p == "" {
		return "/"
	}
	return uriEncode(p, false)
}

// splitHostPort 兼容包装（避免 net.SplitHostPort 对无端口 host 报错）。
func splitHostPort(hostPort string) (string, string, error) {
	i := strings.LastIndexByte(hostPort, ':')
	if i < 0 {
		return hostPort, "", nil
	}
	if strings.Contains(hostPort[i:], "]") { // IPv6 无端口
		return hostPort, "", nil
	}
	if strings.Count(hostPort, ":") > 1 { // IPv6 字面量（带端口形如 [::1]:9000 已由方括号包裹判断）
		if strings.HasSuffix(hostPort, "]") {
			return hostPort, "", nil
		}
		if j := strings.LastIndexByte(hostPort, ']'); j > 0 {
			return hostPort[1:j], hostPort[j+2:], nil
		}
		return hostPort, "", nil
	}
	return hostPort[:i], hostPort[i+1:], nil
}
