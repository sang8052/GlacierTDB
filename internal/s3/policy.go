package s3

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// policy 请求策略条件（参考阿里云 OSS policy 条件匹配）：
// 条件绑定 UA / 来源 IP / Referer / 自定义 header / 方法，全部满足才放行（AND）。
// 空字段不参与匹配；IP 列表任一命中即过；headers 每个键都必须匹配。
type policy struct {
	UA      string            `json:"ua,omitempty"`
	Referer string            `json:"referer,omitempty"`
	IP      []string          `json:"ip,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Methods []string          `json:"methods,omitempty"`
}

// parsePolicy 解析策略 JSON（空串 = 无条件）。
func parsePolicy(s string) (*policy, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var p policy
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		return nil, fmt.Errorf("invalid policy json: %w", err)
	}
	return &p, nil
}

// globMatch 通配匹配（* 任意串，? 单字符；大小写敏感）。
func globMatch(pattern, s string) bool {
	// 迭代式通配（防递归爆炸）
	pi, si, star, match := 0, 0, -1, 0
	for si < len(s) {
		switch {
		case pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]):
			pi++
			si++
		case pi < len(pattern) && pattern[pi] == '*':
			star = pi
			match = si
			pi++
		case star >= 0:
			pi = star + 1
			match++
			si = match
		default:
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}

// matchAny 列表任一匹配。
func matchAny(patterns []string, s string) bool {
	for _, p := range patterns {
		if globMatch(p, s) {
			return true
		}
	}
	return false
}

// check 校验请求是否满足策略条件（不满足返回 s3Error 403）。
func (p *policy) check(r *http.Request, ip string) error {
	if p == nil {
		return nil
	}
	if p.UA != "" && !globMatch(p.UA, r.UserAgent()) {
		return &s3Error{Code: "AccessDenied", Status: 403,
			Msg: "Request policy condition failed: User-Agent does not match " + p.UA}
	}
	if p.Referer != "" && !globMatch(p.Referer, r.Referer()) {
		return &s3Error{Code: "AccessDenied", Status: 403,
			Msg: "Request policy condition failed: Referer does not match " + p.Referer}
	}
	if len(p.Methods) > 0 && !matchAnyFold(p.Methods, r.Method) {
		return &s3Error{Code: "AccessDenied", Status: 403,
			Msg: "Request policy condition failed: method " + r.Method + " not allowed"}
	}
	if len(p.IP) > 0 {
		ok := false
		for _, spec := range p.IP {
			if ipHit(spec, ip) {
				ok = true
				break
			}
		}
		if !ok {
			return &s3Error{Code: "AccessDenied", Status: 403,
				Msg: "Request policy condition failed: source IP " + ip + " not allowed"}
		}
	}
	for k, want := range p.Headers {
		got := r.Header.Get(k)
		if got == "" || !globMatch(want, got) {
			return &s3Error{Code: "AccessDenied", Status: 403,
				Msg: "Request policy condition failed: header " + k + " mismatch"}
		}
	}
	return nil
}

func matchAnyFold(patterns []string, s string) bool {
	for _, p := range patterns {
		if strings.EqualFold(p, s) {
			return true
		}
	}
	return false
}

// ipHit 判断 IP 是否命中（精确 / CIDR / 前缀通配 10.1.%）。
func ipHit(spec, ip string) bool {
	spec = strings.TrimSpace(spec)
	switch {
	case spec == "" || spec == "*":
		return true
	case strings.Contains(spec, "/"):
		_, cidr, err := net.ParseCIDR(spec)
		if err != nil {
			return false
		}
		pip := net.ParseIP(ip)
		return pip != nil && cidr.Contains(pip)
	case strings.HasSuffix(spec, "%"):
		return strings.HasPrefix(ip, strings.TrimSuffix(spec, "%"))
	default:
		return spec == ip
	}
}
