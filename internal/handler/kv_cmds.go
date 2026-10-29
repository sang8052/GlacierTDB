package handler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"glaciertdb/internal/kv"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/meta"
)

// handleKVAdmin KV 键值/队列管理命令（v1.2.0，USE GlacierTDB 管理面）：
//
//	SHOW KV STATS                          —— 占用/命中率（kv_stats 表同源）
//	SET KV MAX_MEMORY <mb>                 —— 热改内存上限（0 = 不限，重启失效）
//	SET KV EVICTION <policy>               —— allkeys-lru | volatile-lru | noeviction
//	SHOW KV SOURCE                         —— SQL 读穿透规则 + 计数（v1.3.13）
//	KV SOURCE ADD <key前缀> <db> '<sql 模板>' [TTL <ms>] —— 注册回源规则（仅 super）
//	KV SOURCE DEL <key前缀>                —— 删除规则（DEL '' / ALL = 全清）
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
	isKV := (up(0) == "SHOW" && up(1) == "KV") || (up(0) == "SET" && up(1) == "KV") ||
		(up(0) == "KV" && up(1) == "SOURCE")
	if !isKV {
		return nil, false, nil
	}
	if h.kvstore == nil {
		return nil, true, mysql.NewError(1105, "KV module is disabled (set kv.enable: true)")
	}

	switch up(0) + " " + up(1) {
	case "SHOW KV":
		if up(2) == "SOURCE" { // SHOW KV SOURCE：回源规则 + 计数（先于统计表路由）
			rules := h.kvstore.SourceRules()
			hits, misses, errs, execUs := h.kvstore.SourceCounters()
			rows := [][]any{{"*", "", "", int64(0), hits, misses, errs, execUs}}
			for _, r := range rules {
				rows = append(rows, []any{r.Prefix, r.DB, r.SQLTpl, r.TTLms, nil, nil, nil, nil})
			}
			res, err := meta.SimpleRS([]string{"prefix", "db", "sql_tpl", "ttl_ms", "hits", "misses", "errors", "exec_us_total"}, rows)
			return res, true, err
		}
		res, err := meta.SimpleRS(kvStatCols, h.kvStatRows())
		return res, true, err

	case "KV SOURCE":
		switch up(2) {
		case "ADD":
			if err := h.adminMutateGate(); err != nil {
				return nil, true, err
			}
			prefix, db := argVal(toks, 3), argVal(toks, 4)
			tpl, ttl := "", int64(0)
			for i := 5; i < len(toks); i++ {
				switch strings.ToUpper(toks[i]) {
				case "TTL":
					i++
					if n, err := strconv.ParseInt(argVal(toks, i), 10, 64); err == nil {
						ttl = n
					}
				default:
					if tpl == "" {
						tpl = strings.Trim(toks[i], "'\"")
					}
				}
			}
			if prefix == "" || db == "" || tpl == "" {
				return nil, true, mysql.NewError(1064,
					"KV SOURCE ADD <key-prefix> <db> '<sql template with {key}/{N} placeholders>' [TTL <ms>]")
			}
			if err := h.kvstore.AddSourceRule(kv.SourceRule{Prefix: prefix, DB: db, SQLTpl: tpl, TTLms: ttl}); err != nil {
				return nil, true, mysql.NewError(1064, err.Error())
			}
			logx.Infof("kv: source rule added prefix=%q db=%s by conn %d", prefix, db, h.connID)
			return meta.OKResultPub(), true, nil
		case "DEL", "DROP":
			if err := h.adminMutateGate(); err != nil {
				return nil, true, err
			}
			prefix := argVal(toks, 3)
			n := h.kvstore.DelSourceRule(prefix)
			logx.Infof("kv: source rules removed (%d) by conn %d", n, h.connID)
			res, err := meta.SimpleRS([]string{"removed"}, [][]any{{n}})
			return res, true, err
		default:
			res, err := meta.SimpleRS([]string{"command"},
				[][]any{{"KV SOURCE ADD <prefix> <db> '<sql>' [TTL <ms>]", "KV SOURCE DEL <prefix>", "SHOW KV SOURCE"}})
			return res, true, err
		}

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
	"hits", "misses", "hit_rate", "expired", "evicted", "evicted_prio", "evicted_lru",
	"source_hits", "source_misses", "source_errors", "ops", "uptime_sec"}

// kvStatRows kv_stats 行集（SHOW KV STATS / SELECT * FROM kv_stats）。
// v1.3.13：新增驱逐优先级分层计数与 SQL 回源计数（回源为全局值，'*' 行聚合）。
func (h *Handler) kvStatRows() [][]any {
	var rows [][]any
	for _, r := range h.kvstore.Stats() {
		hitRate := 0.0
		if r.Hits+r.Misses > 0 {
			hitRate = float64(r.Hits) / float64(r.Hits+r.Misses)
		}
		srcH, srcM, srcE := any(nil), any(nil), any(nil)
		if r.NS == "*" {
			srcH, srcM, srcE = r.SrcHits, r.SrcMisses, r.SrcErrors
		}
		rows = append(rows, []any{r.NS, r.Keys, r.Bytes,
			int64(h.kvstore.MaxMemoryMB()), h.kvstore.Eviction(),
			r.Hits, r.Misses, fmt.Sprintf("%.4f", hitRate), r.Expired, r.Evicted,
			r.EvPrio, r.EvLRU, srcH, srcM, srcE,
			h.kvstore.OpsTotal(), h.kvstore.UptimeSec()})
	}
	return rows
}
