// Package kv — Redis 键值/队列基础能力（v1.2.0，FEATURE §3）。
//
// 通过 3309 端口复用的 `mode:redis` 声明行或 RESP 命令嗅探进入本协议栈：
//   - key 类型：string / list 队列；
//   - TTL：惰性过期 + 定期扫描；
//   - 自动落盘：变更写 SQLite（glacier_kv.db，WAL），内存为缓存层，重启无损恢复；
//     落盘模式（v1.3.15，kv.persist_mode）：
//     sync（默认）= 每写命令单显式事务实时落盘，崩溃一致性与 v1.3.x 等价；
//     async = 内存先行 + AsyncFlushMs 周期批量落盘——崩溃丢失 ≤ flush 窗口
//     （默认 200ms；main.go 停机钩子调 FlushSync 可把优雅停机的丢失窗口收敛为 0）；
//   - 命名空间：每用户独立 namespace（key 按登录账号隔离，跨用户不可见）；
//   - 内存治理：kv.max_memory_mb 上限 + allkeys-lru / volatile-lru / noeviction 淘汰；
//     v1.3.15 起为 16 分片存储：分片键 = FNV-1a(ns+"/"+key)&15，每分片独立互斥锁
//     与 LRU 双向链表，键操作只落对应分片锁；跨分片操作（FlushDB/淘汰全扫/
//     过期扫描/Stats 汇总）逐分片加锁，任何路径不同时持有两把分片锁。
//
// v1.3.15 崩溃语义补充（惰性过期 / 淘汰的 SQL 补删）：
//   - 读路径惰性过期与内存淘汰只摘内存并投递待删标记（pendingDelete，经脏表
//     通道），SQL 行删除由后台 flusher 补做——锁内零 IO；
//   - 崩溃时的残留行：启动 load 已按 expiresAt 过滤清理过期行（v1.3.x 即有）；
//     async 模式下「已淘汰但未及补删」的键可能在重启后复活一次（其值要么从未
//     落盘、要么补删标记丢失），属 async 模式既定语义；sync 模式淘汰/删除仍为
//     同步删行，无此窗口。
//
// 锁序约定：shard.mu → dirtyMu（markDirty 允许在持分片锁时调用）；
// 禁止反向（flusher 换出脏集后先释放 dirtyMu 再取分片锁）。
package kv

import (
	"container/list"
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/sqlite"
)

// shardCount 分片数（v1.3.15：全局单锁 → 16 分片，降低多连接写争用）；
// evictScanWindow 淘汰第一轮在 LRU 尾部方向的最大扫描步数（超出退化第二轮纯 LRU）；
// lruItemOverhead 每个 list 元素的内存开销估值。
const (
	shardCount      = 16
	evictScanWindow = 512
	lruItemOverhead = 24
)

// entry 内存缓存项（string 或 list）。
type entry struct {
	typ       byte // 's' string | 'l' list
	str       []byte
	items     []listItem
	expiresAt int64 // epoch ms，0 = 永不过期
	lastUsed  uint64
	prio      int // 驱逐优先级（v1.3.13）：-1 = 未设置（纯 LRU），0-255 显式（0 最先驱逐，255 最受保护）
	// cachedSize 可变部分大小缓存（v1.3.15）：'s' = len(str)；'l' = Σ(len(val)+24)。
	// Set/Push/Pop/Append 增量维护，estEntry O(1)（原实现每次 O(list 长) 全量估算）。
	cachedSize int64
	// el 所在分片 LRU 链表元素（v1.3.15）；O(1) MoveToFront / Remove。
	el *list.Element
}

type listItem struct {
	seq int64
	val []byte
}

// lruNode 分片 LRU 链表元素负载（v1.3.15）：ns+key 反查内存 entry。
type lruNode struct {
	ns  string
	key string
}

// namespace 单用户命名空间（在单个分片内的切片；同一 ns 的 key 按分片键散布
// 于全部分片，Stats/FlushDB/KEYS 跨分片汇聚）。
type namespace struct {
	entries map[string]*entry
	bytes   int64
	hits    int64
	misses  int64
	expired int64
	evicted int64
	evPrio  int64 // 显式优先级（≠-1）键的淘汰计数（v1.3.13）
	evLRU   int64 // 未设优先级键的纯 LRU 淘汰计数（v1.3.13）
}

// shard 存储分片（v1.3.15）：独立互斥锁 + 命名空间表 + LRU 双向链表。
type shard struct {
	mu    sync.Mutex
	ns    map[string]*namespace
	lru   *list.List // Front = 最近使用，Back = 最久未用；元素 Value = *lruNode
	clock uint64     // 分片本地 LRU 时钟（lastUsed 仅作观测，链表序即 LRU 序）
}

// Store 键值存储：内存缓存 + SQLite 落盘（glacier_kv.db）。
// v1.3.15：16 分片存储 + 可配落盘模式（sync/async）+ 独立运算核执行通道。
type Store struct {
	shards [shardCount]*shard
	db     *sql.DB
	dir    string

	maxB     atomic.Int64 // 内存上限字节（0 = 不限）
	evic     atomic.Value // allkeys-lru | volatile-lru | noeviction（string）
	memTotal atomic.Int64 // 全局估算字节（各分片计账的原子汇总）

	blockMu sync.Mutex    // blockCh 轮换保护（原全局锁职责拆出）
	blockCh chan struct{} // 阻塞弹出广播唤醒（Push 后轮换，v1.3.2）

	// 落盘模式（v1.3.15）
	persistMode string // sync | async
	flushMs     int    // 异步刷盘周期毫秒

	// 独立运算核（v1.3.15）
	dedicated  bool
	coreCPU    int
	corePinned atomic.Int64 // 绑定的核号（-1 = 未绑/未启用）
	execCh     chan func()  // nil = 非专用模式（原地执行）

	// 脏标记（v1.3.15；详见 store_flush.go）
	dirtyMu   sync.Mutex
	dirtyKeys map[string]byte // ns\x00key → kind('w' 重写 / 'd' 删行)
	dirtyNS   map[string]struct{}

	flushTotal atomic.Int64
	flushFail  atomic.Int64

	expiredTotal atomic.Int64
	evictedTotal atomic.Int64
	evictedPrio  atomic.Int64 // 显式优先级键淘汰计数（v1.3.13）
	evictedLRU   atomic.Int64 // 未设优先级键 LRU 淘汰计数（v1.3.13）
	ops          atomic.Int64
	startTS      time.Time

	// SQL 读穿透（v1.3.13 §3.2）：规则表 + 执行器钩子（proxy 注入，权限边界在执行侧）
	srcMu     sync.Mutex
	srcRules  []SourceRule
	srcFlight map[string]*srcCall
	srcExec   SourceExecutor

	srcHits   atomic.Int64
	srcMisses atomic.Int64
	srcErrors atomic.Int64
	srcExecUs atomic.Int64
}

// SourceRule KV SQL 读穿透规则（kv_sources 表；管理面 KV SOURCE ADD 注册，仅 super）。
type SourceRule struct {
	Prefix    string `json:"prefix"`  // key 前缀（最长前缀优先命中）
	DB        string `json:"db"`      // 回源业务库
	SQLTpl    string `json:"sql_tpl"` // SQL 模板：{key} 整键代入；{N} 按 ':' 分段取第 N 段
	TTLms     int64  `json:"ttl_ms"`  // 回填缓存 TTL（默认 300000；≤0 = 不回填，仅直返）
	CreatedTS string `json:"created_ts"`
}

// SourceExecutor 回源 SQL 执行器：以发起请求的用户身份做库授权校验（仅同用户
// 名下 db）后执行只读 SELECT。返回 (列名, 行集字符串化, 错误)。由 proxy 注入。
type SourceExecutor func(user, dbName, sql string) ([]string, [][]string, error)

// StatRow kv_stats 行。
type StatRow struct {
	NS        string
	Keys      int64
	Bytes     int64
	Hits      int64
	Misses    int64
	Expired   int64
	Evicted   int64
	EvPrio    int64 // 显式优先级键淘汰（v1.3.13）
	EvLRU     int64 // 未设优先级键 LRU 淘汰（v1.3.13）
	SrcHits   int64 // SQL 回源命中（v1.3.13）
	SrcMisses int64 // SQL 回源未命中（v1.3.13）
	SrcErrors int64 // SQL 回源执行错误（v1.3.13）
}

// Open 打开系统库并恢复内存状态，启动 TTL 扫描 / flusher / 独立运算核。
func Open(backend *sqlite.Backend, cfg *config.Config) (*Store, error) {
	db, err := backend.OpenKVDB()
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS kv_meta (
  ns TEXT NOT NULL, key TEXT NOT NULL, type TEXT NOT NULL,
  expires_at INTEGER NOT NULL DEFAULT 0,
  minseq INTEGER NOT NULL DEFAULT 0, maxseq INTEGER NOT NULL DEFAULT -1,
  created_ts INTEGER NOT NULL DEFAULT 0, updated_ts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, key)
);
CREATE TABLE IF NOT EXISTS kv_sources (
  prefix TEXT PRIMARY KEY, db TEXT NOT NULL, sql_tpl TEXT NOT NULL,
  ttl_ms INTEGER NOT NULL DEFAULT 300000, created_ts TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS kv_str (
  ns TEXT NOT NULL, key TEXT NOT NULL, value BLOB,
  PRIMARY KEY (ns, key)
);
CREATE TABLE IF NOT EXISTS kv_list (
  ns TEXT NOT NULL, key TEXT NOT NULL, seq INTEGER NOT NULL, value BLOB,
  PRIMARY KEY (ns, key, seq)
);`); err != nil {
		return nil, fmt.Errorf("init kv schema: %w", err)
	}
	s := &Store{
		dir:         cfg.DBDir,
		db:          db, // v1.3.15 修复：漏挂导致 loadSources/load 解引用 nil panic
		blockCh:     make(chan struct{}),
		startTS:     time.Now(),
		srcFlight:   map[string]*srcCall{},
		persistMode: cfg.KV.PersistMode,
		flushMs:     cfg.KV.AsyncFlushMs,
		dedicated:   cfg.KV.DedicatedCore,
		coreCPU:     cfg.KV.CoreCPU,
	}
	if s.persistMode == "" {
		s.persistMode = "sync"
	}
	if s.flushMs <= 0 {
		s.flushMs = 200
	}
	if s.flushMs < 20 {
		s.flushMs = 20
	}
	s.corePinned.Store(-1)
	s.evic.Store("allkeys-lru")
	if cfg.KV.Eviction != "" {
		s.evic.Store(cfg.KV.Eviction)
	}
	s.maxB.Store(int64(cfg.KV.MaxMemoryMB) << 20)
	for i := range s.shards {
		s.shards[i] = &shard{ns: map[string]*namespace{}, lru: list.New()}
	}
	// 存量库补列（幂等）：驱动逐驱逐优先级（v1.3.13）
	_, _ = db.Exec(`ALTER TABLE kv_meta ADD COLUMN prio INTEGER NOT NULL DEFAULT -1`)
	s.loadSources()
	if err := s.load(); err != nil {
		return nil, err
	}
	// flusher：两种模式都启动——async 模式批量落盘；sync 模式承接读路径惰性
	// 过期 / 后台扫描产生的补删标记（锁内零 IO，行删除 ≤ flush 周期内完成）
	s.startFlusher()
	// 独立运算核（v1.3.15）：dedicated_core = 全部非阻塞 RESP 命令投递到单
	// goroutine 串行执行（Redis 单线程模型），Linux 下绑核隔离调度噪声
	if s.dedicated {
		s.startExecCore()
	}
	go s.expireLoop(time.Duration(cfg.KV.ScanIntervalSec) * time.Second)
	logx.Infof("kv: store ready (keys=%d, max_memory_mb=%d, eviction=%s, persist=%s, flush=%dms, core=%s)",
		s.totalKeys(), cfg.KV.MaxMemoryMB, s.policy(), s.persistMode, s.flushMs, s.CoreMode())
	return s, nil
}

// load 启动恢复（SQLite → 内存，跳过并清理已过期键——按 expiresAt 过滤，
// 兜底 async 补删 / 惰性过期的崩溃残留行，v1.3.x 即有）。
func (s *Store) load() error {
	rows, err := s.db.Query(`SELECT ns,key,type,expires_at FROM kv_meta`)
	if err != nil {
		return err
	}
	type metaRow struct {
		ns, key, typ string
		exp          int64
	}
	var metas []metaRow
	for rows.Next() {
		var m metaRow
		if err := rows.Scan(&m.ns, &m.key, &m.typ, &m.exp); err == nil {
			metas = append(metas, m)
		}
	}
	rows.Close()
	// 优先级列（v1.3.13；存量库无列时静默跳过）
	prios := map[string]int{}
	if prows, err := s.db.Query(`SELECT ns,key,prio FROM kv_meta`); err == nil {
		for prows.Next() {
			var ns, key string
			var prio int
			if prows.Scan(&ns, &key, &prio) == nil {
				prios[ns+"\x00"+key] = prio
			}
		}
		prows.Close()
	}
	now := time.Now().UnixMilli()
	for _, m := range metas {
		if m.exp > 0 && m.exp <= now {
			_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, m.ns, m.key)
			_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, m.ns, m.key)
			_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, m.ns, m.key)
			continue
		}
		e := &entry{expiresAt: m.exp, prio: prios[m.ns+"\x00"+m.key]}
		if m.typ == "l" || m.typ == "list" {
			e.typ = 'l'
			lr, err := s.db.Query(`SELECT seq,value FROM kv_list WHERE ns=? AND key=? ORDER BY seq`, m.ns, m.key)
			if err != nil {
				continue
			}
			for lr.Next() {
				var it listItem
				if lr.Scan(&it.seq, &it.val) == nil {
					e.items = append(e.items, it)
					e.cachedSize += int64(len(it.val)) + lruItemOverhead // v1.3.15：cachedSize 一次算清
				}
			}
			lr.Close()
		} else {
			e.typ = 's'
			_ = s.db.QueryRow(`SELECT value FROM kv_str WHERE ns=? AND key=?`, m.ns, m.key).Scan(&e.str)
			e.cachedSize = int64(len(e.str))
		}
		s.putMem(s.shardOf(m.ns, m.key), m.ns, m.key, e)
	}
	return nil
}

// ---------------------------------------------------------------------------
// 分片基础设施（v1.3.15）
// ---------------------------------------------------------------------------

// shardOf 分片键 = FNV-1a(ns + "/" + key) & 15（手写避免 hash.Hash 分配）。
func (s *Store) shardOf(ns, key string) *shard {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(ns); i++ {
		h ^= uint32(ns[i])
		h *= prime32
	}
	h ^= uint32('/')
	h *= prime32
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= prime32
	}
	return s.shards[h&uint32(shardCount-1)]
}

// getNS 取/建分片内命名空间（调用方持 sh.mu）。
func getNS(sh *shard, ns string) *namespace {
	n := sh.ns[ns]
	if n == nil {
		n = &namespace{entries: map[string]*entry{}}
		sh.ns[ns] = n
	}
	return n
}

// putMem 挂入内存并计账（调用方持 sh.mu；同键旧值若在则先行替换）。
// 驱逐优先级是 key 的属性：值被覆盖时保持原优先级（KEYPRIO 独立于 SET，v1.3.13）。
func (s *Store) putMem(sh *shard, ns, key string, e *entry) {
	n := getNS(sh, ns)
	if old := n.entries[key]; old != nil {
		e.prio = old.prio
		sz := estEntry(key, old)
		n.bytes -= sz
		s.memTotal.Add(-sz)
		if old.el != nil {
			sh.lru.Remove(old.el)
			old.el = nil
		}
	}
	n.entries[key] = e
	e.el = sh.lru.PushFront(&lruNode{ns: ns, key: key})
	sz := estEntry(key, e)
	n.bytes += sz
	s.memTotal.Add(sz)
	sh.clock++
	e.lastUsed = sh.clock
}

// unMem 摘除内存并计账（调用方持 sh.mu）。
func (s *Store) unMem(sh *shard, ns, key string) {
	n := sh.ns[ns]
	if n == nil {
		return
	}
	e, ok := n.entries[key]
	if !ok {
		return
	}
	sz := estEntry(key, e)
	n.bytes -= sz
	s.memTotal.Add(-sz)
	delete(n.entries, key)
	if e.el != nil {
		sh.lru.Remove(e.el)
		e.el = nil
	}
}

// adjMemShard 按估算差值调账（list 增删后；oldEst 为变更前估值，调用方持 sh.mu）。
func (s *Store) adjMemShard(sh *shard, ns, key string, e *entry, oldEst int64) {
	n := sh.ns[ns]
	if n == nil {
		return
	}
	delta := estEntry(key, e) - oldEst
	n.bytes += delta
	s.memTotal.Add(delta)
}

// estEntry 估算内存占用（v1.3.15：可变部分缓存于 e.cachedSize，O(1)；
// 原实现 list 类每次遍历全表）。
func estEntry(key string, e *entry) int64 {
	return int64(len(key)) + 96 + e.cachedSize
}

// expireLocked 惰性过期检查（调用方持 sh.mu）；返回键是否仍有效。
// v1.3.15：锁内只摘内存 + 投递待删标记（'d'），SQL 行删除由 flusher 补做
// （两种模式一致，锁内零 IO；崩溃残留行由启动 load 按 TTL 过滤兜底）。
func (s *Store) expireLocked(sh *shard, ns, key string, e *entry) bool {
	if e.expiresAt > 0 && e.expiresAt <= time.Now().UnixMilli() {
		s.unMem(sh, ns, key)
		s.markDirty(ns, key, dirtyKindDelete)
		s.expiredTotal.Add(1)
		if n := sh.ns[ns]; n != nil {
			n.expired++
		}
		return false
	}
	return true
}

// touch 更新 LRU：链表 MoveToFront + lastUsed（调用方持 sh.mu）。
func (s *Store) touch(sh *shard, e *entry) {
	sh.clock++
	e.lastUsed = sh.clock
	if e.el != nil {
		sh.lru.MoveToFront(e.el)
	}
}

// policy 当前淘汰策略（atomic.Value 读）。
func (s *Store) policy() string {
	if v := s.evic.Load(); v != nil {
		if p, ok := v.(string); ok {
			return p
		}
	}
	return "allkeys-lru"
}

// ---------------------------------------------------------------------------
// 命令实现（导出签名与 v1.3.x 完全一致）
// ---------------------------------------------------------------------------

// Get GET key。
func (s *Store) Get(ns, key string) ([]byte, bool, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	n := getNS(sh, ns)
	e := n.entries[key]
	if e == nil || !s.expireLocked(sh, ns, key, e) {
		n.misses++
		sh.mu.Unlock()
		return nil, false, nil
	}
	if e.typ != 's' {
		sh.mu.Unlock()
		return nil, false, errWrongType
	}
	s.touch(sh, e)
	n.hits++
	val := e.str
	sh.mu.Unlock()
	return val, true, nil
}

var errWrongType = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")

// Set SET key value [ttlMs] [nx|xx]（nx/xx: ' ' 不限、n 仅不存在、x 仅存在）。
// 返回 (applied, err)。
// v1.3.15：sync 模式 3 条隐式事务合并为 1 条显式事务（syncWriteStr）；
// async 模式仅写内存 + 标脏（flusher 批量落盘）。
func (s *Store) Set(ns, key string, val []byte, ttlMs int64, mode byte) (bool, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	n := getNS(sh, ns)
	e := n.entries[key]
	live := false
	if e != nil && s.expireLocked(sh, ns, key, e) {
		live = true
		if mode == 'n' {
			sh.mu.Unlock()
			return false, nil
		}
	} else if mode == 'x' {
		sh.mu.Unlock()
		return false, nil
	}
	if s.policy() == "noeviction" && s.maxB.Load() > 0 {
		nsz := int64(len(key)) + 96 + int64(len(val))
		if live {
			if s.memTotal.Load()-estEntry(key, e)+nsz > s.maxB.Load() {
				sh.mu.Unlock()
				return false, errOOM
			}
		} else if s.memTotal.Load()+nsz > s.maxB.Load() {
			sh.mu.Unlock()
			return false, errOOM
		}
	}
	ttlAt := int64(0)
	if ttlMs > 0 {
		ttlAt = time.Now().UnixMilli() + ttlMs // 相对 TTL → 绝对时刻
	}
	s.writeStrMem(sh, ns, key, val, ttlAt)
	if s.persistMode == "sync" {
		// sync 模式：SQL 在分片锁内执行——与 v1.3.x 全局锁内落盘的写序语义等价
		//（两把并发写同键时 DB 序 = 内存序，不发散）；只阻塞本分片 1/16 键空间
		s.syncWriteStr(ns, key, val, ttlAt)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		s.markDirty(ns, key, dirtyKindWrite) // async：锁外标脏（分片锁外、命令返回前）
	}
	s.evictAfterWrite(sh, ns, key)
	return true, nil
}

var errOOM = fmt.Errorf("OOM command not allowed when used memory > 'maxmemory'")

// Del DEL key... → 删除数。
func (s *Store) Del(ns string, keys ...string) int64 {
	s.ops.Add(1)
	var n int64
	for _, key := range keys {
		sh := s.shardOf(ns, key)
		sh.mu.Lock()
		var e *entry
		if nd := sh.ns[ns]; nd != nil {
			e = nd.entries[key]
		}
		if e == nil {
			sh.mu.Unlock()
			continue
		}
		if !s.expireLocked(sh, ns, key, e) {
			sh.mu.Unlock()
			continue // 刚被惰性过期清掉（补删已投递）
		}
		s.unMem(sh, ns, key)
		if s.persistMode == "sync" {
			s.syncDeleteRows(ns, key) // sync：锁内删行（写序与内存一致）
		}
		sh.mu.Unlock()
		n++
		if s.persistMode != "sync" {
			s.markDirty(ns, key, dirtyKindDelete)
		}
	}
	return n
}

// Exists EXISTS key... → 命中数。
func (s *Store) Exists(ns string, keys ...string) int64 {
	s.ops.Add(1)
	var n int64
	for _, key := range keys {
		sh := s.shardOf(ns, key)
		sh.mu.Lock()
		if nd := sh.ns[ns]; nd != nil {
			if e := nd.entries[key]; e != nil && s.expireLocked(sh, ns, key, e) {
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}

// SetTTL 设置/清除过期（ttlMs<=0 立即删除；EXPIRE key 0 语义）。
func (s *Store) SetTTL(ns, key string, ttlMs int64) (bool, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	nd := sh.ns[ns]
	var e *entry
	if nd != nil {
		e = nd.entries[key]
	}
	if e == nil || !s.expireLocked(sh, ns, key, e) {
		sh.mu.Unlock()
		return false, nil
	}
	if ttlMs <= 0 {
		s.unMem(sh, ns, key)
		if s.persistMode == "sync" {
			s.syncDeleteRows(ns, key)
		}
		sh.mu.Unlock()
		if s.persistMode != "sync" {
			s.markDirty(ns, key, dirtyKindDelete)
		}
		return true, nil
	}
	e.expiresAt = time.Now().UnixMilli() + ttlMs
	if s.persistMode == "sync" {
		_, _ = s.db.Exec(`UPDATE kv_meta SET expires_at=?, updated_ts=? WHERE ns=? AND key=?`,
			e.expiresAt, time.Now().UnixMilli(), ns, key)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		s.markDirty(ns, key, dirtyKindWrite)
	}
	return true, nil
}

// Persist 清除 TTL。
func (s *Store) Persist(ns, key string) bool {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	nd := sh.ns[ns]
	var e *entry
	if nd != nil {
		e = nd.entries[key]
	}
	if e == nil || !s.expireLocked(sh, ns, key, e) {
		sh.mu.Unlock()
		return false
	}
	if e.expiresAt == 0 {
		sh.mu.Unlock()
		return false
	}
	e.expiresAt = 0
	if s.persistMode == "sync" {
		_, _ = s.db.Exec(`UPDATE kv_meta SET expires_at=0 WHERE ns=? AND key=?`, ns, key)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		s.markDirty(ns, key, dirtyKindWrite)
	}
	return true
}

// TTL 查询剩余毫秒：-2 无键、-1 无 TTL。
func (s *Store) TTL(ns, key string) int64 {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	nd := sh.ns[ns]
	var e *entry
	if nd != nil {
		e = nd.entries[key]
	}
	if e == nil || !s.expireLocked(sh, ns, key, e) {
		return -2
	}
	if e.expiresAt == 0 {
		return -1
	}
	ms := e.expiresAt - time.Now().UnixMilli()
	if ms < 0 {
		return -2
	}
	return ms
}

// Type 类型（string/list/none）。
func (s *Store) Type(ns, key string) string {
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	return s.typeLocked(sh, ns, key)
}

// typeLocked 内部（调用方持 sh.mu）。
func (s *Store) typeLocked(sh *shard, ns, key string) string {
	nd := sh.ns[ns]
	var e *entry
	if nd != nil {
		e = nd.entries[key]
	}
	if e == nil || !s.expireLocked(sh, ns, key, e) {
		return "none"
	}
	if e.typ == 'l' {
		return "list"
	}
	return "string"
}

// IncrBy INCR/DECR/INCRBY/DECRBY 族。
func (s *Store) IncrBy(ns, key string, delta int64) (int64, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	nd := getNS(sh, ns)
	e := nd.entries[key]
	var cur int64
	var ttlAt int64
	if e != nil && s.expireLocked(sh, ns, key, e) {
		if e.typ != 's' {
			sh.mu.Unlock()
			return 0, errWrongType
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(e.str)), 10, 64)
		if err != nil {
			sh.mu.Unlock()
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		cur = v
	}
	if e != nil {
		ttlAt = e.expiresAt // 保持原 TTL 语义（含已过期键的历史行为，与 v1.3.x 一致）
	}
	nv := cur + delta
	val := []byte(strconv.FormatInt(nv, 10))
	s.writeStrMem(sh, ns, key, val, ttlAt)
	if s.persistMode == "sync" {
		s.syncWriteStr(ns, key, val, ttlAt)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		s.markDirty(ns, key, dirtyKindWrite)
	}
	s.evictAfterWrite(sh, ns, key)
	return nv, nil
}

// writeStrMem 写 string 类型 entry 的内存部分（调用方持 sh.mu；v1.3.15）。
func (s *Store) writeStrMem(sh *shard, ns, key string, val []byte, ttlAt int64) {
	e := &entry{typ: 's', str: val, expiresAt: ttlAt, cachedSize: int64(len(val))}
	s.putMem(sh, ns, key, e)
}

// Append APPEND key val → 新长度。
func (s *Store) Append(ns, key string, val []byte) (int64, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	nd := getNS(sh, ns)
	e := nd.entries[key]
	ttlAt := int64(0)
	var cur []byte
	if e != nil && s.expireLocked(sh, ns, key, e) {
		if e.typ != 's' {
			sh.mu.Unlock()
			return 0, errWrongType
		}
		cur = e.str
		ttlAt = e.expiresAt
	}
	nv := append(append([]byte{}, cur...), val...)
	s.writeStrMem(sh, ns, key, nv, ttlAt)
	if s.persistMode == "sync" {
		s.syncWriteStr(ns, key, nv, ttlAt)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		s.markDirty(ns, key, dirtyKindWrite)
	}
	s.evictAfterWrite(sh, ns, key)
	return int64(len(nv)), nil
}

// MGet MGET key...（缺失 → nil 元素）。
func (s *Store) MGet(ns string, keys []string) [][]byte {
	s.ops.Add(1)
	out := make([][]byte, len(keys))
	for i, key := range keys {
		sh := s.shardOf(ns, key)
		sh.mu.Lock()
		n := getNS(sh, ns)
		if e := n.entries[key]; e != nil && s.expireLocked(sh, ns, key, e) && e.typ == 's' {
			s.touch(sh, e)
			n.hits++
			out[i] = e.str
		} else {
			n.misses++
		}
		sh.mu.Unlock()
	}
	return out
}

// MSet MSET k v ...（v1.3.15：分片下逐键原子——跨键整体原子性经 MULTI/EXEC 表达）。
func (s *Store) MSet(ns string, kvs [][2][]byte) {
	s.ops.Add(1)
	for _, kv := range kvs {
		key := string(kv[0])
		sh := s.shardOf(ns, key)
		sh.mu.Lock()
		s.writeStrMem(sh, ns, key, kv[1], 0)
		if s.persistMode == "sync" {
			s.syncWriteStr(ns, key, kv[1], 0)
		}
		sh.mu.Unlock()
		if s.persistMode != "sync" {
			s.markDirty(ns, key, dirtyKindWrite)
		}
		s.evictAfterWrite(sh, ns, key)
	}
}

// Keys KEYS pattern（glob：* ?；v1.3.15：跨分片汇聚）。
func (s *Store) Keys(ns, pattern string) []string {
	s.ops.Add(1)
	var out []string
	for i := range s.shards {
		sh := s.shards[i]
		sh.mu.Lock()
		if n := sh.ns[ns]; n != nil {
			for key, e := range n.entries {
				if !s.expireLocked(sh, ns, key, e) {
					continue
				}
				if globMatch(pattern, key) {
					out = append(out, key)
				}
			}
		}
		sh.mu.Unlock()
	}
	return out
}

// ---------------------------------------------------------------------------
// list 队列
// ---------------------------------------------------------------------------

// listMem 取 list 项（调用方持 sh.mu；v1.3.15：纯内存部分）。
func (s *Store) listMem(sh *shard, ns, key string) *entry {
	nd := sh.ns[ns]
	var e *entry
	if nd != nil {
		e = nd.entries[key]
	}
	if e != nil && s.expireLocked(sh, ns, key, e) {
		if e.typ != 'l' {
			return nil
		}
		return e
	}
	return nil
}

// listIndexMem 取/建 list 项（调用方持 sh.mu；纯内存建项，SQL 由命令层锁外做）。
// 返回 (entry, 是否新建)。
func (s *Store) listIndexMem(sh *shard, ns, key string) (*entry, bool) {
	nd := sh.ns[ns]
	var e *entry
	if nd != nil {
		e = nd.entries[key]
	}
	if e != nil && s.expireLocked(sh, ns, key, e) {
		if e.typ != 'l' {
			return nil, false
		}
		return e, false
	}
	e = &entry{typ: 'l'}
	s.putMem(sh, ns, key, e)
	return e, true
}

func (s *Store) listSeq(e *entry) (int64, int64) {
	if len(e.items) == 0 {
		return 0, -1
	}
	return e.items[0].seq, e.items[len(e.items)-1].seq
}

// Push LPUSH/RPUSH（left=true 左插）→ 新长度。
// v1.3.15：sync 模式每元素 INSERT + meta UPDATE 合并为单显式事务；async 模式标脏。
func (s *Store) Push(ns, key string, vals [][]byte, left bool) (int64, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	e, created := s.listIndexMem(sh, ns, key)
	if e == nil {
		sh.mu.Unlock()
		return 0, errWrongType
	}
	oldEst := estEntry(key, e)
	minS, maxS := s.listSeq(e)
	insSeq := make([]int64, 0, len(vals))
	insVal := make([][]byte, 0, len(vals))
	for _, v := range vals {
		var seq int64
		if left {
			minS--
			seq = minS
			e.items = append([]listItem{{seq: seq, val: v}}, e.items...)
		} else {
			maxS++
			seq = maxS
			e.items = append(e.items, listItem{seq: seq, val: v})
		}
		e.cachedSize += int64(len(v)) + lruItemOverhead
		insSeq = append(insSeq, seq)
		insVal = append(insVal, v)
	}
	s.adjMemShard(sh, ns, key, e, oldEst)
	s.touch(sh, e)
	newLen := int64(len(e.items)) // 锁内快照（Pop 可能并发截短 items）
	ttlAt := e.expiresAt
	if s.persistMode == "sync" {
		s.syncPush(ns, key, insSeq, insVal, minS, maxS, ttlAt, created)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		s.markDirty(ns, key, dirtyKindWrite)
	}
	s.evictAfterWrite(sh, ns, key)
	s.notifyPush() // 唤醒阻塞弹出（BLPOP/BRPOP，v1.3.2）
	return newLen, nil
}

// Pop LPOP/RPOP（count<=0 单个；返回元素列表；键不存在返回空）。
func (s *Store) Pop(ns, key string, left bool, count int) ([][]byte, error) {
	s.ops.Add(1)
	return s.popOnce(ns, key, left, count)
}

// popOnce 弹出实现（v1.3.15：分片锁内做内存摘除，锁外补 SQL/标脏；
// Pop 与 BlockPop 共用）。
func (s *Store) popOnce(ns, key string, left bool, count int) ([][]byte, error) {
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	e := s.listMem(sh, ns, key)
	if e == nil {
		if nd := sh.ns[ns]; nd != nil && nd.entries[key] != nil {
			sh.mu.Unlock()
			return nil, errWrongType
		}
		sh.mu.Unlock()
		return nil, nil
	}
	if len(e.items) == 0 {
		sh.mu.Unlock()
		return nil, nil
	}
	oldEst := estEntry(key, e)
	if count <= 0 {
		count = 1
	}
	if count > len(e.items) {
		count = len(e.items)
	}
	out := make([][]byte, 0, count)
	seqs := make([]int64, 0, count)
	for i := 0; i < count; i++ {
		var it listItem
		if left {
			it = e.items[0]
			e.items = e.items[1:]
		} else {
			it = e.items[len(e.items)-1]
			e.items = e.items[:len(e.items)-1]
		}
		e.cachedSize -= int64(len(it.val)) + lruItemOverhead
		out = append(out, it.val)
		seqs = append(seqs, it.seq)
	}
	emptied := len(e.items) == 0
	var newMinS, newMaxS int64
	if emptied {
		s.unMem(sh, ns, key)
	} else {
		newMinS, newMaxS = s.listSeq(e) // 锁内快照（meta 回写基线）
		s.adjMemShard(sh, ns, key, e, oldEst)
		s.touch(sh, e)
	}
	if s.persistMode == "sync" {
		s.syncPop(ns, key, seqs, emptied, newMinS, newMaxS)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		// 空/非空统一标 'w'：键已空时 flusher 按缺失键删行（幂等）
		s.markDirty(ns, key, dirtyKindWrite)
	}
	return out, nil
}

// BlockPop BLPOP/BRPOP（v1.3.2）：按 key 顺序尝试弹出一个元素（left=true 左弹），
// 全部为空则阻塞等待 Push 唤醒或超时；timeout<=0 表示无限等待。
// 返回 (key, value, ok)；ok=false 为超时。键持有非 list 值 → errWrongType。
func (s *Store) BlockPop(ns string, keys []string, left bool, timeout time.Duration) (string, []byte, bool, error) {
	deadline := time.Time{}
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		s.blockMu.Lock()
		ch := s.blockCh // 先取唤醒通道再扫数据，避免丢失唤醒
		s.blockMu.Unlock()
		for _, key := range keys {
			items, err := s.popOnce(ns, key, left, 1)
			if err != nil {
				return "", nil, false, err
			}
			if len(items) > 0 {
				s.ops.Add(1)
				return key, items[0], true, nil
			}
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			s.ops.Add(1)
			return "", nil, false, nil
		}
		if !deadline.IsZero() {
			t := time.NewTimer(time.Until(deadline))
			select {
			case <-ch:
				t.Stop()
			case <-t.C:
				s.ops.Add(1)
				return "", nil, false, nil
			}
			continue
		}
		<-ch // 无限等待：仅 Push 唤醒
	}
}

// notifyPush Push 后唤醒全部阻塞弹出者（轮换广播通道）。
func (s *Store) notifyPush() {
	s.blockMu.Lock()
	close(s.blockCh)
	s.blockCh = make(chan struct{})
	s.blockMu.Unlock()
}

// LLen LLEN（键不存在 → 0；类型不符 → errWrongType）。
func (s *Store) LLen(ns, key string) (int64, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e := s.listMem(sh, ns, key)
	if e == nil {
		nd := sh.ns[ns]
		var other *entry
		if nd != nil {
			other = nd.entries[key]
		}
		if other != nil && s.typeLocked(sh, ns, key) == "list" {
			return 0, nil
		}
		if s.typeLocked(sh, ns, key) == "string" {
			return 0, errWrongType
		}
		return 0, nil
	}
	return int64(len(e.items)), nil
}

// LRange LRANGE key start stop（含负下标；键不存在返回空）。
func (s *Store) LRange(ns, key string, start, stop int64) ([][]byte, error) {
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	e := s.listMem(sh, ns, key)
	if e == nil {
		if s.typeLocked(sh, ns, key) == "string" {
			return nil, errWrongType
		}
		return [][]byte{}, nil
	}
	l := int64(len(e.items))
	if start < 0 {
		start += l
	}
	if stop < 0 {
		stop += l
	}
	if start < 0 {
		start = 0
	}
	if stop >= l {
		stop = l - 1
	}
	if start > stop || start >= l {
		return [][]byte{}, nil
	}
	out := make([][]byte, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, e.items[i].val)
	}
	s.touch(sh, e)
	return out, nil
}

// Flush FLUSHDB：清空本命名空间（v1.3.15 分片版：按 0→15 顺序获取全部分片锁
// 后整体清空——唯一同时持有多把分片锁的路径，其余路径均单锁，无死锁；
// 恢复 v1.3.x 全局锁的 FLUSHDB 原子性，杜绝与并发 SET 的删行/写行交叠）。
func (s *Store) Flush(ns string) int64 {
	s.ops.Add(1)
	for i := range s.shards {
		s.shards[i].mu.Lock()
	}
	var cnt int64
	for i := range s.shards {
		sh := s.shards[i]
		if n := sh.ns[ns]; n != nil {
			cnt += int64(len(n.entries))
			s.memTotal.Add(-n.bytes)
			// 摘除本 ns 在本分片 LRU 链表中的全部元素
			var els []*list.Element
			for el := sh.lru.Front(); el != nil; el = el.Next() {
				if nd := el.Value.(*lruNode); nd.ns == ns {
					els = append(els, el)
				}
			}
			for _, el := range els {
				sh.lru.Remove(el)
			}
			delete(sh.ns, ns)
		}
	}
	if s.persistMode == "sync" {
		tx, err := s.db.Begin()
		if err == nil {
			_, _ = tx.Exec(`DELETE FROM kv_meta WHERE ns=?`, ns)
			_, _ = tx.Exec(`DELETE FROM kv_str WHERE ns=?`, ns)
			_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=?`, ns)
			if cerr := tx.Commit(); cerr != nil {
				logx.Debugf("kv flushdb commit: %v", cerr)
			}
		} else {
			logx.Debugf("kv flushdb: %v", err)
		}
	}
	for i := shardCount - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
	if s.persistMode != "sync" {
		s.markDirtyNS(ns)
	}
	return cnt
}

// ---------------------------------------------------------------------------
// 内存治理（v1.3.15 分片版）
// ---------------------------------------------------------------------------

// evictAfterWrite 写后淘汰检查：先在本分片逐到水位线下，仍超限则全分片轮扫。
// 调用方不得持有任何分片锁（v1.3.15）。
func (s *Store) evictAfterWrite(sh *shard, protectNs, protectKey string) {
	if s.maxB.Load() <= 0 || s.policy() == "noeviction" {
		return
	}
	if s.memTotal.Load() <= s.maxB.Load() {
		return
	}
	sh.mu.Lock()
	for s.memTotal.Load() > s.maxB.Load() {
		if !s.evictOneLocked(sh, protectNs, protectKey) {
			break
		}
	}
	sh.mu.Unlock()
	s.evictGlobal(protectNs, protectKey)
}

// evictGlobal 全分片轮扫淘汰（SetMaxMemory/SetEviction 热改后、写入分片逐完
// 仍超限时；每轮每分片至多逐出一键，直到达标或整轮无进展）。
func (s *Store) evictGlobal(protectNs, protectKey string) {
	if s.maxB.Load() <= 0 || s.policy() == "noeviction" {
		return
	}
	for s.memTotal.Load() > s.maxB.Load() {
		progress := false
		for i := 0; i < shardCount; i++ {
			if s.memTotal.Load() <= s.maxB.Load() {
				break
			}
			sh := s.shards[i]
			sh.mu.Lock()
			if s.evictOneLocked(sh, protectNs, protectKey) {
				progress = true
			}
			sh.mu.Unlock()
		}
		if !progress {
			return
		}
	}
}

// evictOneLocked 从本分片淘汰一键（调用方持 sh.mu）；返回是否淘汰。
// 驱逐优先级语义（v1.3.13 保持）：prio 值越小越先被逐出——第一轮在 LRU 尾部
// 方向 evictScanWindow 步窗口内的显式 prio（≥0）键中选 prio 最小者（同级取更
// 久未用 = 离 Back 更近者）；窗口内无显式 prio 键 → 第二轮纯 LRU 摘尾。
// volatile-lru 策略两轮均跳过无 TTL 键。对 v1.3.13 全库扫语义的取舍：显式
// prio 键只在 LRU 尾部窗口内参选（近期活跃的显式 prio 键暂不逐出），已在
// CHANGELOG 标注（v1.3.15）。
func (s *Store) evictOneLocked(sh *shard, protectNs, protectKey string) bool {
	volatileOnly := s.policy() == "volatile-lru"
	eligible := func(e *entry) bool { return !volatileOnly || e.expiresAt != 0 }
	var pickEl *list.Element
	var pickE *entry
	pickPrio := 1 << 30
	steps := 0
	// 第一轮：尾部窗口内的显式 prio 最小者
	for el := sh.lru.Back(); el != nil && steps < evictScanWindow; el = el.Prev() {
		steps++
		nd := el.Value.(*lruNode)
		if nd.ns == protectNs && nd.key == protectKey {
			continue
		}
		n := sh.ns[nd.ns]
		if n == nil {
			continue
		}
		e := n.entries[nd.key]
		if e == nil || !eligible(e) {
			continue
		}
		if e.prio >= 0 && e.prio < pickPrio {
			pickPrio = e.prio
			pickEl, pickE = el, e
		}
	}
	if pickEl == nil {
		// 第二轮：纯 LRU 摘尾
		for el := sh.lru.Back(); el != nil; el = el.Prev() {
			nd := el.Value.(*lruNode)
			if nd.ns == protectNs && nd.key == protectKey {
				continue
			}
			n := sh.ns[nd.ns]
			if n == nil {
				continue
			}
			e := n.entries[nd.key]
			if e == nil || !eligible(e) {
				continue
			}
			pickEl, pickE = el, e
			break
		}
	}
	if pickEl == nil {
		return false
	}
	nd := pickEl.Value.(*lruNode)
	prioEvict := pickE.prio >= 0
	s.unMem(sh, nd.ns, nd.key)
	s.evictedTotal.Add(1)
	if prioEvict {
		s.evictedPrio.Add(1)
	} else {
		s.evictedLRU.Add(1)
	}
	if n := sh.ns[nd.ns]; n != nil {
		n.evicted++
		if prioEvict {
			n.evPrio++
		} else {
			n.evLRU++
		}
	}
	if s.persistMode == "sync" {
		s.syncDeleteRows(nd.ns, nd.key)
	} else {
		s.markDirty(nd.ns, nd.key, dirtyKindDelete)
	}
	return true
}

// ---------------------------------------------------------------------------
// 驱逐优先级命令（v1.3.13 §3.1）：KEYPRIO key <n>（n = -1 清除）/ GETPRIO key
// ---------------------------------------------------------------------------

// SetPrio 设置键驱逐优先级（-1 清除 = 回归纯 LRU；0-255 显式，0 最先驱逐 255
// 最受保护）。键不存在返回 false。
func (s *Store) SetPrio(ns, key string, prio int) (bool, error) {
	if prio < -1 || prio > 255 {
		return false, fmt.Errorf("ERR priority out of range (expect -1..255)")
	}
	s.ops.Add(1)
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	nd := getNS(sh, ns)
	e := nd.entries[key]
	if e == nil || !s.expireLocked(sh, ns, key, e) {
		sh.mu.Unlock()
		return false, nil
	}
	e.prio = prio
	if s.persistMode == "sync" {
		_, _ = s.db.Exec(`UPDATE kv_meta SET prio=?, updated_ts=? WHERE ns=? AND key=?`,
			prio, time.Now().UnixMilli(), ns, key)
	}
	sh.mu.Unlock()
	if s.persistMode != "sync" {
		s.markDirty(ns, key, dirtyKindWrite)
	}
	s.evictAfterWrite(sh, "", "") // 降保护级（调大 prio）可能立刻需要腾地方
	return true, nil
}

// GetPrio 查询键驱逐优先级（-2 = 键不存在；-1 = 未设置）。
func (s *Store) GetPrio(ns, key string) int {
	sh := s.shardOf(ns, key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	nd := getNS(sh, ns)
	e := nd.entries[key]
	if e == nil || !s.expireLocked(sh, ns, key, e) {
		return -2
	}
	return e.prio
}

// SetMaxMemory 热改内存上限（MB，0=不限）。
func (s *Store) SetMaxMemory(mb int) {
	if mb < 0 {
		mb = 0
	}
	s.maxB.Store(int64(mb) << 20)
	s.evictGlobal("", "")
}

// SetEviction 热改淘汰策略。
func (s *Store) SetEviction(policy string) error {
	switch policy {
	case "allkeys-lru", "volatile-lru", "noeviction":
	default:
		return fmt.Errorf("invalid eviction %q (allkeys-lru|volatile-lru|noeviction)", policy)
	}
	s.evic.Store(policy)
	s.evictGlobal("", "")
	return nil
}

// MaxMemoryMB 当前上限。
func (s *Store) MaxMemoryMB() int { return int(s.maxB.Load() >> 20) }

// Eviction 当前策略。
func (s *Store) Eviction() string { return s.policy() }

// Stats kv_stats 行（每命名空间 + '*' 合计；回源计数为全局值，v1.3.13）。
// v1.3.15：跨分片逐分片快照汇聚；按 ns 名排序输出（分片化前行序为 map 随机序）。
func (s *Store) Stats() []StatRow {
	srcH, srcM, srcE := s.srcHits.Load(), s.srcMisses.Load(), s.srcErrors.Load()
	acc := map[string]*StatRow{}
	for i := range s.shards {
		sh := s.shards[i]
		sh.mu.Lock()
		for ns, n := range sh.ns {
			r := acc[ns]
			if r == nil {
				r = &StatRow{NS: ns}
				acc[ns] = r
			}
			r.Keys += int64(len(n.entries))
			r.Bytes += n.bytes
			r.Hits += n.hits
			r.Misses += n.misses
			r.Expired += n.expired
			r.Evicted += n.evicted
			r.EvPrio += n.evPrio
			r.EvLRU += n.evLRU
		}
		sh.mu.Unlock()
	}
	var out []StatRow
	total := StatRow{NS: "*", SrcHits: srcH, SrcMisses: srcM, SrcErrors: srcE}
	for _, r := range acc {
		total.Keys += r.Keys
		total.Bytes += r.Bytes
		total.Hits += r.Hits
		total.Misses += r.Misses
		total.Expired += r.Expired
		total.Evicted += r.Evicted
		total.EvPrio += r.EvPrio
		total.EvLRU += r.EvLRU
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NS < out[j].NS })
	out = append(out, total)
	return out
}

// MemBytes 当前估算内存占用。
func (s *Store) MemBytes() int64 { return s.memTotal.Load() }

// OpsTotal 累计命令数。
func (s *Store) OpsTotal() int64 { return s.ops.Load() }

// UptimeSec 运行秒数。
func (s *Store) UptimeSec() int64 { return int64(time.Since(s.startTS).Seconds()) }

// totalKeys 全分片键数（Open 启动日志用）。
func (s *Store) totalKeys() int64 {
	var n int64
	for i := range s.shards {
		sh := s.shards[i]
		sh.mu.Lock()
		for _, ns := range sh.ns {
			n += int64(len(ns.entries))
		}
		sh.mu.Unlock()
	}
	return n
}

// expireLoop 定期扫描过期键（主动过期；v1.3.15：逐分片锁内摘内存，锁外投递
// 待删标记，SQL 补删由 flusher 完成——锁内零 IO）。
func (s *Store) expireLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		now := time.Now().UnixMilli()
		type dead struct{ ns, key string }
		for i := range s.shards {
			sh := s.shards[i]
			var list []dead
			sh.mu.Lock()
			for ns, n := range sh.ns {
				for k, e := range n.entries {
					if e.expiresAt > 0 && e.expiresAt <= now {
						list = append(list, dead{ns, k})
					}
				}
			}
			for _, d := range list {
				s.unMem(sh, d.ns, d.key)
				s.expiredTotal.Add(1)
				if n := sh.ns[d.ns]; n != nil {
					n.expired++
				}
			}
			sh.mu.Unlock()
			for _, d := range list {
				s.markDirty(d.ns, d.key, dirtyKindDelete)
			}
		}
	}
}

// globMatch Redis 风格 glob（* 任意、? 单字符、[...] 字符类简配为不支持即字面）。
func globMatch(pattern, s string) bool {
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

// KvDBPath 系统库路径（运维可见）。
func KvDBPath(dir string) string { return filepath.Join(dir, sqlite.SystemKVDB+".db") }

// EvictedPrioCount 显式优先级键累计淘汰数（INFO 观测，v1.3.13）。
func (s *Store) EvictedPrioCount() int64 { return s.evictedPrio.Load() }

// EvictedLRUCount 未设优先级键累计 LRU 淘汰数（INFO 观测，v1.3.13）。
func (s *Store) EvictedLRUCount() int64 { return s.evictedLRU.Load() }

// ---------------------------------------------------------------------------
// sync 模式落盘（v1.3.15：每写命令单显式事务，替代原多条隐式事务）
// ---------------------------------------------------------------------------

// syncWriteStr string 键整键落盘（DELETE kv_list + UPSERT kv_meta + UPSERT kv_str，
// 单显式事务；调用方为 sync 模式写路径，锁外执行）。
func (s *Store) syncWriteStr(ns, key string, val []byte, ttlAt int64) {
	now := time.Now().UnixMilli()
	tx, err := s.db.Begin()
	if err != nil {
		logx.Debugf("kv sync write %s/%s: %v", ns, key, err)
		return
	}
	_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
	_, _ = tx.Exec(`INSERT INTO kv_meta(ns,key,type,expires_at,minseq,maxseq,created_ts,updated_ts)
		VALUES(?,?,'s',?,0,-1,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET type='s', expires_at=excluded.expires_at,
		minseq=0, maxseq=-1, updated_ts=excluded.updated_ts`, ns, key, ttlAt, now, now)
	_, _ = tx.Exec(`INSERT INTO kv_str(ns,key,value) VALUES(?,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET value=excluded.value`, ns, key, val)
	if cerr := tx.Commit(); cerr != nil {
		logx.Debugf("kv sync write commit %s/%s: %v", ns, key, cerr)
	}
}

// syncDeleteRows 三表行删除（单显式事务；sync 模式 Del/淘汰/显式过期用）。
func (s *Store) syncDeleteRows(ns, key string) {
	tx, err := s.db.Begin()
	if err != nil {
		logx.Debugf("kv sync delete %s/%s: %v", ns, key, err)
		return
	}
	_, _ = tx.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, ns, key)
	_, _ = tx.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
	_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
	if cerr := tx.Commit(); cerr != nil {
		logx.Debugf("kv sync delete commit %s/%s: %v", ns, key, cerr)
	}
}

// syncPush list 推送落盘（每元素 INSERT + meta UPSERT，单显式事务；
// created = 本次命令新建了 list 键（需清 kv_str 残行并写入 meta 行））。
func (s *Store) syncPush(ns, key string, seqs []int64, vals [][]byte, minS, maxS, ttlAt int64, created bool) {
	now := time.Now().UnixMilli()
	tx, err := s.db.Begin()
	if err != nil {
		logx.Debugf("kv sync push %s/%s: %v", ns, key, err)
		return
	}
	if created {
		_, _ = tx.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
		_, _ = tx.Exec(`INSERT INTO kv_meta(ns,key,type,expires_at,minseq,maxseq,created_ts,updated_ts)
			VALUES(?,?,'l',?,?,?,?,?)
			ON CONFLICT(ns,key) DO UPDATE SET type='l', expires_at=excluded.expires_at,
			minseq=excluded.minseq, maxseq=excluded.maxseq, updated_ts=excluded.updated_ts`,
			ns, key, ttlAt, minS, maxS, now, now)
	} else {
		_, _ = tx.Exec(`UPDATE kv_meta SET minseq=?, maxseq=?, updated_ts=? WHERE ns=? AND key=?`,
			minS, maxS, now, ns, key)
	}
	for i, seq := range seqs {
		_, _ = tx.Exec(`INSERT INTO kv_list(ns,key,seq,value) VALUES(?,?,?,?)
			ON CONFLICT(ns,key,seq) DO UPDATE SET value=excluded.value`, ns, key, seq, vals[i])
	}
	if cerr := tx.Commit(); cerr != nil {
		logx.Debugf("kv sync push commit %s/%s: %v", ns, key, cerr)
	}
}

// syncPop list 弹出落盘（每元素 DELETE + meta 收尾，单显式事务；
// emptied = 弹空后键已删除；newMinS/newMaxS = 弹出后剩余首尾 seq 快照）。
func (s *Store) syncPop(ns, key string, seqs []int64, emptied bool, newMinS, newMaxS int64) {
	now := time.Now().UnixMilli()
	tx, err := s.db.Begin()
	if err != nil {
		logx.Debugf("kv sync pop %s/%s: %v", ns, key, err)
		return
	}
	for _, seq := range seqs {
		_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=? AND key=? AND seq=?`, ns, key, seq)
	}
	if emptied {
		_, _ = tx.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, ns, key)
	} else {
		_, _ = tx.Exec(`UPDATE kv_meta SET minseq=?, maxseq=?, updated_ts=? WHERE ns=? AND key=?`,
			newMinS, newMaxS, now, ns, key)
	}
	if cerr := tx.Commit(); cerr != nil {
		logx.Debugf("kv sync pop commit %s/%s: %v", ns, key, cerr)
	}
}
