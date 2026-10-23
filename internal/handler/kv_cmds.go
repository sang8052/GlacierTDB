package handler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
)

// handleKVAdmin KV 键值/队列管理命令（v1.2.0，USE GlacierTDB 管理面）：
//
//	SHOW KV STATS                          —— 占用/命中率（kv_stats 表同源）
//	SET KV MAX_MEMORY <mb>                 —— 热改内存上限（0 = 不限，重启失效）
//	SET KV EVICTION <policy>               —— allkeys-lru | volatile-lru | noeviction
func (h *Handler) handleKVAdmin(q string) (*mysql.Result, bool, error) {
	toks := lexSQL(q)
	if len(toks) == 0 {
		return nil, false, nil
	}
	up := func(i int) string {
		if i < len(toks) {
			return strings.ToUpper(toks[i])
		}
		return ""
	}
	isKV := (up(0) == "SHOW" && up(1) == "KV") || (up(0) == "SET" && up(1) == "KV")
	if !isKV {
		return nil, false, nil
	}
	if h.kvstore == nil {
		return nil, true, mysql.NewError(1105, "KV module is disabled (set kv.enable: true)")
	}

	switch up(0) + " " + up(1) {
	case "SHOW KV":
		res, err := meta.SimpleRS(kvStatCols, h.kvStatRows())
		return res, true, err

	case "SET KV":
		if err := h.adminMutateGate(); err != nil {
			return nil, true, err
		}
		switch up(2) {
		case "MAX_MEMORY":
			mb, err := strconv.Atoi(argVal(toks, 3))
			if err != nil || mb < 0 {
				return nil, true, mysql.NewError(1064, "SET KV MAX_MEMORY <mb> (0 = unlimited)")
			}
			h.kvstore.SetMaxMemory(mb)
			logx.Infof("kv: max_memory set to %d MB by conn %d (runtime only)", mb, h.connID)
			return meta.OKResultPub(), true, nil
		case "EVICTION":
			policy := strings.ToLower(argVal(toks, 3))
			if err := h.kvstore.SetEviction(policy); err != nil {
				return nil, true, mysql.NewError(1064, err.Error())
			}
			logx.Infof("kv: eviction set to %s by conn %d (runtime only)", policy, h.connID)
			return meta.OKResultPub(), true, nil
		}
		return nil, true, mysql.NewError(1064, "SET KV (MAX_MEMORY <mb> | EVICTION <policy>)")
	}
	return nil, false, nil
}

var kvStatCols = []string{"namespace", "keys", "memory_bytes", "max_memory_mb", "eviction",
	"hits", "misses", "hit_rate", "expired", "evicted", "ops", "uptime_sec"}

// kvStatRows kv_stats 行集（SHOW KV STATS / SELECT * FROM kv_stats）。
func (h *Handler) kvStatRows() [][]any {
	var rows [][]any
	for _, r := range h.kvstore.Stats() {
		hitRate := 0.0
		if r.Hits+r.Misses > 0 {
			hitRate = float64(r.Hits) / float64(r.Hits+r.Misses)
		}
		rows = append(rows, []any{r.NS, r.Keys, r.Bytes,
			int64(h.kvstore.MaxMemoryMB()), h.kvstore.Eviction(),
			r.Hits, r.Misses, fmt.Sprintf("%.4f", hitRate), r.Expired, r.Evicted,
			h.kvstore.OpsTotal(), h.kvstore.UptimeSec()})
	}
	return rows
}
