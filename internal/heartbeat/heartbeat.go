// Package heartbeat 主动心跳上报（v1.3.13 §6.5）。
//
// 语义：中心端 = 用户自建的 HTTP 服务器（中心监控所有盒子的大屏）；
// GlacierTDB 只负责按配置定时 POST JSON（body 可选 HMAC-SHA256 签名，
// X-Glacier-Signature: sha256=<hex>，与告警 / 上传回调同签名头惯例）。
// 失败仅记日志（避免失败风暴）；默认关闭（heartbeat.url 为空）。
package heartbeat

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"runtime"
	"sync/atomic"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/instance"
	"glaciertdb/internal/logx"
)

// Collector 各组件规模采集回调（返回 payload 的一部分；返回 nil = 该组件关闭）。
type Collector func() map[string]any

var (
	s3Collector    atomic.Value // Collector
	kvCollector    atomic.Value
	mqCollector    atomic.Value
	asyncCollector atomic.Value
	errCounter     atomic.Int64 // 组件上报的最近一轮错误计数（可扩展；当前记 S3 错误日志行数）
)

// Register 注册组件采集器（模块关闭时不注册）。
func RegisterS3(c Collector)    { s3Collector.Store(c) }
func RegisterKV(c Collector)   { kvCollector.Store(c) }
func RegisterMQ(c Collector)   { mqCollector.Store(c) }
func RegisterAsync(c Collector) { asyncCollector.Store(c) }

// AddErrors 组件错误计数累加（心跳 payload errors 字段来源之一）。
func AddErrors(n int64) { errCounter.Add(n) }

// Errors 当前错误计数。
func Errors() int64 { return errCounter.Load() }

// Run 阻塞式心跳循环（调用方 go Run(...)）。启动先立即上报一次，之后按周期。
func Run(cfg config.HeartbeatOpts, start time.Time) {
	iv := time.Duration(cfg.IntervalSec) * time.Second
	if iv <= 0 {
		iv = 60 * time.Second
	}
	client := &http.Client{Timeout: 10 * time.Second}
	send := func() {
		payload := snapshot(start)
		if len(cfg.Fields) > 0 { // 字段白名单（顶层键过滤）
			allow := map[string]bool{}
			for _, f := range cfg.Fields {
				allow[f] = true
			}
			filtered := map[string]any{}
			for k, v := range payload {
				if allow[k] {
					filtered[k] = v
				}
			}
			payload = filtered
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return
		}
		req, err := http.NewRequest(http.MethodPost, cfg.URL, bytes.NewReader(body))
		if err != nil {
			logx.Warnf("heartbeat: bad url %s: %v", cfg.URL, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Glacier-Event", "heartbeat")
		req.Header.Set("X-Glacier-Instance-Id", instance.UUID())
		if cfg.Secret != "" {
			mac := hmac.New(sha256.New, []byte(cfg.Secret))
			mac.Write(body)
			req.Header.Set("X-Glacier-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
		}
		resp, err := client.Do(req)
		if err != nil {
			logx.Warnf("heartbeat: post failed: %v", err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			logx.Warnf("heartbeat: center responded %d", resp.StatusCode)
		}
	}
	send()
	t := time.NewTicker(iv)
	defer t.Stop()
	for range t.C {
		send()
	}
}

// snapshot 汇总当前 payload：实例 ID + 版本 + uptime + 各组件规模 + 错误计数。
func snapshot(start time.Time) map[string]any {
	p := map[string]any{
		"instance_id": instance.UUID(),
		"version":     config.DisplayVersion(),
		"uptime_sec":  int64(time.Since(start).Seconds()),
		"goos":        runtime.GOOS,
		"goarch":      runtime.GOARCH,
		"ts":          time.Now().Format("2006-01-02 15:04:05"),
	}
	if v, ok := s3Collector.Load().(Collector); ok && v != nil {
		p["s3"] = v()
	}
	if v, ok := kvCollector.Load().(Collector); ok && v != nil {
		p["kv"] = v()
	}
	if v, ok := mqCollector.Load().(Collector); ok && v != nil {
		p["mq"] = v()
	}
	if v, ok := asyncCollector.Load().(Collector); ok && v != nil {
		p["async"] = v()
	}
	p["errors"] = errCounter.Load()
	return p
}
