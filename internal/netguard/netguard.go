// Package netguard 回调/出站 URL 安全校验（v1.4.0，防 SSRF）。
//
// 适用于「租户可控」的回调 URL（S3 PUT 的 x-glacier-callback-url 请求头、
// SUBMIT QUERY ... CALLBACK）：这类 URL 允许普通业务账号指定，服务端会向其
// POST 数据，若不设防即构成 SSRF——攻击者可探测内网、打云元数据端点
//（169.254.169.254）窃取凭据。
//
// 策略（security.callback_net_policy）：
//   - standard（默认）：仅允许 http/https；封禁回环（127/8、::1）与链路本地
//     （169.254/16、fe80::/10，含云元数据端点）；不封 RFC1918（内网部署兼容）。
//   - strict：在 standard 基础上加封全部私网段（10/8、172.16/12、192.168/16、
//     fc00::/7）与 0.0.0.0/8、组播段。
//   - open：不封 IP，仅强制 scheme 白名单（不推荐）。
//
// 所有策略下回调请求一律不跟随重定向（重定向可把安全目标跳到内网地址）。
package netguard

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// Policy 校验策略。
type Policy string

const (
	Standard Policy = "standard"
	Strict   Policy = "strict"
	Open     Policy = "open"
)

// ParsePolicy 解析配置串（非法值回退 standard）。
func ParsePolicy(s string) Policy {
	switch Policy(s) {
	case Strict, Open:
		return Policy(s)
	default:
		return Standard
	}
}

// Check 校验 http(s) 回调 URL；违反策略时返回带原因的错误（不会向上抛内部细节）。
// 仅做静态校验（scheme + 字面 host 解析），域名解析后的 IP 不在此处复核——
// DNS rebinding 防护由 NewClient 的连接期 Control 钩子拦截（两道防线合用）。
func Check(rawURL string, p Policy) error {
	return check(rawURL, p, false)
}

// CheckHost 校验任意 scheme 的回调目标（tcp:/amqp:/kafka: 等）——
// 只做 host 维度策略校验，不限制协议本身。
func CheckHost(rawURL string, p Policy) error {
	return check(rawURL, p, true)
}

func check(rawURL string, p Policy, anyScheme bool) error {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("callback url invalid")
	}
	if !anyScheme && u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("callback url scheme must be http/https")
	}
	host := u.Hostname()
	if host == "" && u.Scheme == "" {
		// 形如 "host:port"（无 scheme）：url.Parse 会把 host 当 scheme，
		// 手工剥端口取 host
		rest := strings.TrimSuffix(strings.TrimSpace(rawURL), "/")
		if i := strings.LastIndex(rest, ":"); i > 0 {
			host = rest[:i]
		}
	}
	if host == "" {
		return fmt.Errorf("callback url host empty")
	}
	if p == Open {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return nil // 域名：静态校验放行，HTTP 场景由 NewClient 连接期复核解析 IP
	}
	if blockedIP(ip, p) {
		return fmt.Errorf("callback url host not allowed by policy")
	}
	return nil
}

func blockedIP(ip net.IP, p Policy) bool {
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	if p == Strict && (ip.IsPrivate() || ip.IsInterfaceLocalMulticast()) {
		return true
	}
	return false
}

// BlockedResolvedIP 连接期复核：域名解析出的 IP 违反策略时拒绝连接。
// 供 NewClient 的 Control 钩子调用。
func BlockedResolvedIP(ip net.IP, p Policy) bool {
	if p == Open {
		return false
	}
	return blockedIP(ip, p)
}

// NewClient 构造带策略防护的 http.Client（v1.4.0）：
//   - 拒绝跟随重定向（重定向可把已校验目标跳转到内网地址，绕过静态校验）；
//   - 连接期 Control 钩子复核实际连上的 IP（防 DNS rebinding：静态校验时域名
//     解析到公网，真正拨号时再解析到内网）。
func NewClient(p Policy, timeout time.Duration) *http.Client {
	d := &net.Dialer{
		Timeout: timeout,
		// Control 钩子在真实拨号前复核解析 IP（防 DNS rebinding）
		Control: func(network, address string, _ syscall.RawConn) error {
			if p == Open {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil
			}
			ip := net.ParseIP(host)
			if ip != nil && BlockedResolvedIP(ip, p) {
				return fmt.Errorf("dial %s blocked by callback net policy", host)
			}
			return nil
		},
	}
	tr := &http.Transport{DialContext: d.DialContext}
	return &http.Client{
		Transport: tr,
		Timeout:   timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return fmt.Errorf("callback redirect not allowed")
		},
	}
}
