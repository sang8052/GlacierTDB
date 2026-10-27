// Package s3 — 兼容 AWS S3 的对象存储（v1.1.0，可选模块）。
// AWS Signature V4 标准签名鉴权；存储模型为「一个 bucket 一张表」（系统库
// glacier_s3.db，超阈值内容走分块表）；AK/SK 密钥、下载限速、请求策略条件、
// 上传回调、图片处理、多版本 / 对象锁 / 生命周期、virtual-host + SNI 证书、
// 计费记账（s3_usage）。
package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// SigV4 常量。
const (
	sigV4Algorithm    = "AWS4-HMAC-SHA256"
	payloadEmpty      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // sha256("")
	payloadUnsigned   = "UNSIGNED-PAYLOAD"
	payloadStreaming  = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	signTimeSkew      = 15 * time.Minute // 允许时钟偏差
	serviceName       = "s3"
)

// isStreamingPayload 判断 x-amz-content-sha256 声明是否为 aws-chunked 流式编码。
// 覆盖新版 botocore / AWS CLI 的 CRC32 默认校验和编码：
//   STREAMING-UNSIGNED-PAYLOAD-TRAILER
//   STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER
//   STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD(-TRAILER)
// 此类请求体是分帧格式（trailer 携带 crc32），必须解码后才是净荷，
// 且不做整负载 sha256 比对（声明值是字面量而非哈希）。
func isStreamingPayload(h string) bool {
	return h != "" && strings.HasPrefix(h, "STREAMING-")
}

// credScope 一次签名请求的凭据范围（从 Authorization / 预签名查询串解析）。
type credScope struct {
	AK            string
	Date          string // yyyymmdd
	Region        string
	Service       string
	SignedHeaders []string
	Signature     string
	Presigned     bool
	Expires       int    // 预签名有效秒数
	ReqDateTime   string // yyyymmddThhmmssZ
}

// awsURIEncode AWS 规则百分号编码（unreserved: A-Za-z0-9-_.~；encodeSlash=false 时保留 /）。
func awsURIEncode(s string, encodeSlash bool) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		case c == '/' && !encodeSlash:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// percentDecode 百分号解码（查询串里的 + 按字面加号处理，与 S3 客户端一致）。
func percentDecode(s string) string {
	if !strings.Contains(s, "%") {
		return s
	}
	if out, err := url.PathUnescape(s); err == nil {
		return out
	}
	return s
}

// canonicalQueryString 规范化查询串（按键值排序后 URI 编码，空格 → %20）。
func canonicalQueryString(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	type kv struct{ k, v string }
	var pairs []kv
	for _, part := range strings.Split(rawQuery, "&") {
		if part == "" {
			continue
		}
		k, v := part, ""
		if i := strings.IndexByte(part, '='); i >= 0 {
			k, v = part[:i], part[i+1:]
		}
		pairs = append(pairs, kv{awsURIEncode(percentDecode(k), true), awsURIEncode(percentDecode(v), true)})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].k != pairs[j].k {
			return pairs[i].k < pairs[j].k
		}
		return pairs[i].v < pairs[j].v
	})
	parts := make([]string, 0, len(pairs))
	for _, p := range pairs {
		parts = append(parts, p.k+"="+p.v)
	}
	return strings.Join(parts, "&")
}

// canonicalHeaders 规范化签名头（小写头名、去空白、多值逗号连接）。
func canonicalHeaders(r *http.Request, signedHeaders []string) string {
	var b strings.Builder
	for _, h := range signedHeaders {
		h = strings.ToLower(strings.TrimSpace(h))
		b.WriteString(h)
		b.WriteByte(':')
		switch h {
		case "host":
			b.WriteString(strings.TrimSpace(r.Host))
		case "(request-target)":
			b.WriteString(strings.ToLower(r.Method) + " " + r.URL.RequestURI())
		default:
			vals := r.Header.Values(h)
			for i, v := range vals {
				if i > 0 {
					b.WriteByte(',')
				}
				b.WriteString(strings.TrimSpace(v))
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// buildCanonicalRequest 组装规范化请求（canonicalURI 为已选定的规范化路径）。
func buildCanonicalRequest(r *http.Request, canonicalURI, rawQuery, payloadHash string, signedHeaders []string) string {
	return strings.Join([]string{
		r.Method,
		canonicalURI,
		canonicalQueryString(rawQuery),
		canonicalHeaders(r, signedHeaders),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
}

func hexSHA256(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key, data []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return m.Sum(nil)
}

// signingKey 派生签名密钥：HMAC(HMAC(HMAC(HMAC("AWS4"+SK, date), region), service), "aws4_request")。
func signingKey(sk, date, region, service string) []byte {
	k := hmacSHA256([]byte("AWS4"+sk), []byte(date))
	k = hmacSHA256(k, []byte(region))
	k = hmacSHA256(k, []byte(service))
	return hmacSHA256(k, []byte("aws4_request"))
}

func stringToSign(dateTime, scope, canonicalReq string) string {
	return strings.Join([]string{sigV4Algorithm, dateTime, scope, hexSHA256([]byte(canonicalReq))}, "\n")
}

// parseAuthHeader 解析 Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...
func parseAuthHeader(v string) (credScope, error) {
	var cs credScope
	v = strings.TrimSpace(v)
	if !strings.HasPrefix(v, sigV4Algorithm+" ") {
		return cs, fmt.Errorf("unsupported authorization algorithm")
	}
	rest := strings.TrimSpace(v[len(sigV4Algorithm):])
	for _, seg := range splitAuthParts(rest) {
		eq := strings.IndexByte(seg, '=')
		if eq <= 0 {
			continue
		}
		k, val := strings.TrimSpace(seg[:eq]), strings.TrimSpace(seg[eq+1:])
		val = strings.Trim(val, `"`)
		switch strings.ToLower(k) {
		case "credential":
			parts := strings.Split(val, "/")
			if len(parts) != 5 {
				return cs, fmt.Errorf("malformed credential scope %q", val)
			}
			cs.AK, cs.Date, cs.Region, cs.Service = parts[0], parts[1], parts[2], parts[3]
		case "signedheaders":
			cs.SignedHeaders = strings.Split(strings.ToLower(val), ";")
		case "signature":
			cs.Signature = val
		}
	}
	if cs.AK == "" || cs.Signature == "" || len(cs.SignedHeaders) == 0 {
		return cs, fmt.Errorf("incomplete authorization header")
	}
	return cs, nil
}

// splitAuthParts 按逗号切分（Credential 值内无逗号，SignedHeaders 分号列表亦无逗号）。
func splitAuthParts(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// claimedPayloadHash 取请求负载哈希声明（x-amz-content-sha256；预签名按 UNSIGNED-PAYLOAD）。
func claimedPayloadHash(r *http.Request, presigned bool) string {
	if presigned {
		return payloadUnsigned
	}
	if v := strings.TrimSpace(r.Header.Get("X-Amz-Content-Sha256")); v != "" {
		return v
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodDelete:
		return payloadEmpty
	}
	return ""
}

// verifySignature 用给定 canonicalURI 候选验证签名，返回是否命中。
func verifySignature(r *http.Request, cs credScope, sk, payloadHash, rawQuery string, canonicalURIs []string) bool {
	dateTime := cs.ReqDateTime
	scope := cs.Date + "/" + cs.Region + "/" + cs.Service + "/aws4_request"
	key := signingKey(sk, cs.Date, cs.Region, cs.Service)
	for _, cu := range canonicalURIs {
		cReq := buildCanonicalRequest(r, cu, rawQuery, payloadHash, cs.SignedHeaders)
		sts := stringToSign(dateTime, scope, cReq)
		mac := hmacSHA256(key, []byte(sts))
		if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(mac)), []byte(strings.ToLower(cs.Signature))) == 1 {
			return true
		}
	}
	return false
}

// requestRawQuery 取原始查询串（不做二次转义）。
func requestRawQuery(r *http.Request) string {
	if r.URL == nil {
		return ""
	}
	return r.URL.RawQuery
}

// requestPathCandidates 规范化路径候选：
//  1. 解码路径按 AWS 规则编码（标准客户端）
//  2. 原样发送的转义路径（编码已规范的客户端）
//  3. 转义路径再次编码（Java SDK 双重编码风格）
func requestPathCandidates(r *http.Request) []string {
	escaped := ""
	if r.URL != nil {
		escaped = r.URL.EscapedPath()
	}
	decoded := r.URL.Path
	var out []string
	out = append(out, awsURIEncode(decoded, false))
	if escaped != "" {
		out = append(out, escaped)
		out = append(out, awsURIEncode(escaped, true))
	}
	// 去重
	seen := map[string]bool{}
	var uniq []string
	for _, c := range out {
		if c != "" && !seen[c] {
			seen[c] = true
			uniq = append(uniq, c)
		}
	}
	return uniq
}

// verifySigV4 校验请求签名（表头鉴权或预签名 URL），返回签名身份与负载哈希声明。
// secretFor(ak) 返回 (owner, sk, status, policyJSON)。
func verifySigV4(r *http.Request, region string,
	secretFor func(ak string) (owner, sk, status, policy string, ok bool)) (credScope, string, error) {
	var cs credScope
	rawQuery := requestRawQuery(r)

	// 预签名 URL：X-Amz-* 查询参数
	q := r.URL.Query()
	if q.Get("X-Amz-Algorithm") == sigV4Algorithm {
		cs.Presigned = true
		cs.AK = strings.SplitN(q.Get("X-Amz-Credential"), "/", 2)[0]
		credParts := strings.Split(q.Get("X-Amz-Credential"), "/")
		if len(credParts) != 5 {
			return cs, "", fmt.Errorf("malformed X-Amz-Credential")
		}
		cs.AK, cs.Date, cs.Region, cs.Service = credParts[0], credParts[1], credParts[2], credParts[3]
		cs.ReqDateTime = q.Get("X-Amz-Date")
		cs.SignedHeaders = strings.Split(strings.ToLower(q.Get("X-Amz-SignedHeaders")), ";")
		cs.Signature = q.Get("X-Amz-Signature")
		cs.Expires, _ = strconv.Atoi(q.Get("X-Amz-Expires"))
		if cs.Expires <= 0 {
			cs.Expires = 604800 // S3 默认 7 天
		}
		if cs.Expires > 604800 { // v1.3.11：对齐 AWS 上限（7 天），防止万年签名
			return cs, "", fmt.Errorf("X-Amz-Expires must be <= 604800")
		}
		if len(cs.SignedHeaders) == 0 {
			return cs, "", fmt.Errorf("missing X-Amz-SignedHeaders")
		}
		// 有效期（v1.3.11：日期不可解析时 fail-closed，不再静默跳过时效校验）
		t, err := time.Parse("20060102T150405Z", cs.ReqDateTime)
		if err != nil {
			return cs, "", fmt.Errorf("malformed X-Amz-Date")
		}
		if now := time.Now().UTC(); now.After(t.Add(time.Duration(cs.Expires)*time.Second)) ||
			now.Before(t.Add(-signTimeSkew)) {
			return cs, "", fmt.Errorf("presigned URL expired")
		}
	} else {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			return cs, "", fmt.Errorf("missing Authorization header")
		}
		var err error
		if cs, err = parseAuthHeader(auth); err != nil {
			return cs, "", err
		}
		cs.ReqDateTime = r.Header.Get("X-Amz-Date")
		if cs.ReqDateTime == "" {
			if d := r.Header.Get("Date"); d != "" {
				if t, err := http.ParseTime(d); err == nil {
					cs.ReqDateTime = t.UTC().Format("20060102T150405Z")
				}
			}
		}
		if cs.ReqDateTime == "" {
			return cs, "", fmt.Errorf("missing X-Amz-Date")
		}
		// v1.3.11：日期不可解析时 fail-closed
		t, terr := time.Parse("20060102T150405Z", cs.ReqDateTime)
		if terr != nil {
			return cs, "", fmt.Errorf("malformed X-Amz-Date")
		}
		if dt := time.Now().UTC().Sub(t); dt > signTimeSkew || dt < -signTimeSkew {
			return cs, "", fmt.Errorf("request time too skewed")
		}
	}

	// region 范围校验：'*' / 空 = 任意；否则必须与配置一致
	if region != "" && region != "*" && !strings.EqualFold(region, cs.Region) {
		return cs, "", fmt.Errorf("region mismatch: signature %q, server %q", cs.Region, region)
	}

	owner, sk, status, policy, ok := secretFor(cs.AK)
	if !ok {
		return cs, "", &s3Error{Code: "InvalidAccessKeyId", Status: 403,
			Msg: "The AWS Access Key Id you provided does not exist in our records: " + cs.AK}
	}
	_ = owner
	_ = policy
	if status != "active" {
		return cs, "", &s3Error{Code: "InvalidAccessKeyId", Status: 403,
			Msg: "The AWS Access Key Id is " + status}
	}

	payloadHash := claimedPayloadHash(r, cs.Presigned)
	if payloadHash == "" {
		return cs, "", &s3Error{Code: "InvalidRequest", Status: 400,
			Msg: "Missing x-amz-content-sha256 header"}
	}
	signQuery := rawQuery
	if cs.Presigned {
		signQuery = stripQueryParam(rawQuery, "X-Amz-Signature")
	}
	if verifySignature(r, cs, sk, payloadHash, signQuery, requestPathCandidates(r)) {
		return cs, payloadHash, nil
	}
	return cs, "", &s3Error{Code: "SignatureDoesNotMatch", Status: 403,
		Msg: "The request signature we calculated does not match the signature you provided. " +
			"Check your key and signing method."}
}

// stripQueryParam 从原始查询串剔除指定参数（预签名 X-Amz-Signature 不参与规范化）。
func stripQueryParam(rawQuery, name string) string {
	if rawQuery == "" {
		return ""
	}
	var keep []string
	for _, part := range strings.Split(rawQuery, "&") {
		k := part
		if i := strings.IndexByte(part, '='); i >= 0 {
			k = part[:i]
		}
		if strings.EqualFold(percentDecode(k), name) {
			continue
		}
		keep = append(keep, part)
	}
	return strings.Join(keep, "&")
}
