package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"glaciertdb/internal/instance"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/netguard"
)

// notifyPut 对象写入完成回调：POST JSON（HMAC 签名、退避重试，fire-and-forget）。
// 通道与用户系统告警一致：X-Glacier-Signature: sha256=<hmac_hex>。
// URL / 密钥三级优先（v1.3.12）：请求级头 x-glacier-callback-url[-secret] >
// 桶级（SET S3 CALLBACK）> 全局（s3.callback_url / s3.callback_secret）。
// (v1.4.0 S2)：出站 client 换 netguard 防护（策略 security.callback_net_policy）——
// 不跟随重定向 + 连接期实际 IP 复核（防 DNS rebinding）；写路径另有静态预检
//（Server.checkCallbackURL），此处 client 侧 Control 是第二道防线（重试全程生效）。
func (s *Server) notifyPut(b Bucket, m ObjectMeta, replaced bool, ov *callbackOverride) {
	url, secret := "", ""
	if ov != nil && ov.url != "" {
		url, secret = ov.url, ov.secret
	}
	if url == "" {
		url = b.CallbackURL
	}
	if url == "" {
		url = s.cfg.S3.CallbackURL
	}
	if secret == "" {
		secret = b.CallbackSecret
	}
	if secret == "" {
		secret = s.cfg.S3.CallbackSecret
	}
	if url == "" {
		return
	}
	// (v1.4.0 S2) 每次回调独立构造 netguard 防护 client（回调 fire-and-forget，
	// 无连接复用诉求；换取不跟随重定向 + 连接期 IP 复核）
	client := netguard.NewClient(netguard.ParsePolicy(s.cfg.Security.CallbackNetPolicy), 10*time.Second)
	payload := map[string]any{
		"event":        "object_created",
		"instance_id":  instance.UUID(), // 中心侧区分来源盒子（v1.3.13 §7.3）
		"bucket":       b.Name,
		"key":          m.Key,
		"version_id":   m.VersionID,
		"size":         m.Size,
		"etag":         m.ETag,
		"content_type": m.ContentType,
		"replaced":     replaced,
		"ts":           time.Now().Format("2006-01-02 15:04:05"),
	}
	go func() {
		body, _ := json.Marshal(payload)
		for attempt, backoff := range []time.Duration{0, time.Second, 5 * time.Second, 15 * time.Second} {
			time.Sleep(backoff)
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Glacier-Event", "object_created")
			if secret != "" {
				mac := hmac.New(sha256.New, []byte(secret))
				mac.Write(body)
				req.Header.Set("X-Glacier-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
			}
			resp, err := client.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return
				}
			}
			logx.Warnf("s3 upload callback %s attempt %d failed (bucket=%s key=%s)", url, attempt+1, b.Name, m.Key)
		}
	}()
}
