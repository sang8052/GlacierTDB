// Package handler — 账号级配额（v1.3.6 多租户）：每账号 QPS（最小间隔节流）与
// 并发执行上限（信号量），配额来自 auth 系统库 user_quotas（SET USER QOS 热改，
// 内存缓存）。仅约束 USER 类型账号；super/readonly 不受限。
package handler

import (
	"sync"
	"time"

	"glaciertdb/internal/auth"
)

type userLimiter struct {
	mu     sync.Mutex
	lastAt time.Time // 上次放行时刻（QPS 最小间隔）
	qps    int64
	sem    chan struct{}
	semCap int64
}

var (
	userLimitMu    sync.Mutex
	userLimiters   = map[string]*userLimiter{}
)

// limiterFor 取（或按当前配额重建）账号限流器。
func (h *Handler) limiterFor(user string) *userLimiter {
	if h.auths == nil {
		return nil
	}
	qps, conc := h.auths.QuotaFor(user)
	if qps <= 0 && conc <= 0 {
		userLimitMu.Lock()
		delete(userLimiters, user) // 配额清零即回收
		userLimitMu.Unlock()
		return nil
	}
	userLimitMu.Lock()
	defer userLimitMu.Unlock()
	l := userLimiters[user]
	if l == nil || l.semCap != conc {
		l = &userLimiter{}
		if conc > 0 {
			l.sem = make(chan struct{}, conc)
			l.semCap = conc
		}
		userLimiters[user] = l
	}
	l.qps = qps // 见下方字段（为简化并发写在锁内更新）
	return l
}

// acquire 账号配额准入：QPS 最小间隔 + 并发信号量；返回释放函数。
// 并发满/速率到点时等待（与全局 QoS wait 语义一致）。
func (h *Handler) acquireUserQuota() (release func()) {
	if h.auths == nil || h.userType != auth.TypeUser || h.user == "" {
		return func() {}
	}
	l := h.limiterFor(h.user)
	if l == nil {
		return func() {}
	}
	// QPS：串行化最小间隔（简单令牌节流，等待而非拒绝）
	if l.qps > 0 {
		for {
			l.mu.Lock()
			now := time.Now()
			wait := l.lastAt.Add(time.Second / time.Duration(l.qps)).Sub(now)
			if wait <= 0 {
				l.lastAt = now
				l.mu.Unlock()
				break
			}
			l.mu.Unlock()
			time.Sleep(wait)
		}
	}
	// 并发信号量
	if l.sem != nil {
		l.sem <- struct{}{}
		return func() { <-l.sem }
	}
	return func() {}
}
