// Package admin 提供连接注册表：支撑管理面（SHOW CLIENTS / SHOW TRANSACTIONS / KILL）。
package admin

import (
	"sync"
)

// ConnInfo 是一个客户端连接的瞬时快照（只读）。
type ConnInfo struct {
	ID               uint64
	User             string
	Remote           string
	DB               string
	AdminMode        bool
	ConnectedSec     int64
	InTx             bool
	TxSec            int64 // 事务已持续秒数（InTx 时有效）
	HoldingWriteLock bool
	LastQuery        string
}

// Target 由 handler 实现：提供快照与主动断开能力。
type Target interface {
	Snapshot() ConnInfo
	Kill()
}

// Registry 全局连接注册表。
type Registry struct {
	mu    sync.RWMutex
	conns map[uint64]Target
}

func NewRegistry() *Registry {
	return &Registry{conns: make(map[uint64]Target)}
}

func (r *Registry) Add(id uint64, t Target) {
	r.mu.Lock()
	r.conns[id] = t
	r.mu.Unlock()
}

func (r *Registry) Remove(id uint64) {
	r.mu.Lock()
	delete(r.conns, id)
	r.mu.Unlock()
}

func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.conns)
}

// List 返回全部连接快照（按 ID 升序）。
func (r *Registry) List() []ConnInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ConnInfo, 0, len(r.conns))
	for _, t := range r.conns {
		out = append(out, t.Snapshot())
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].ID < out[j-1].ID; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// Kill 断开指定连接；返回是否找到。
func (r *Registry) Kill(id uint64) bool {
	r.mu.RLock()
	t, ok := r.conns[id]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	go t.Kill() // 异步：允许 KILL 自己时不阻塞当前命令的应答
	return true
}
