package kv

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/sqlite"
)

// entry 内存缓存项（string 或 list）。
type entry struct {
	typ       byte // 's' string | 'l' list
	str       []byte
	items     []listItem
	expiresAt int64 // epoch ms，0 = 永不过期
	lastUsed  uint64
}

type listItem struct {
	seq int64
	val []byte
}

// namespace 单用户命名空间。
type namespace struct {
	entries map[string]*entry
	bytes   int64
	hits    int64
	misses  int64
	expired int64
	evicted int64
}

// Store 键值存储：内存缓存 + SQLite 实时落盘（glacier_kv.db）。
type Store struct {
	mu   sync.Mutex
	db   *sql.DB
	dir  string
	ns   map[string]*namespace
	mem  int64  // 全局估算字节
	maxB int64  // 内存上限字节（0 = 不限）
	evic string // allkeys-lru | volatile-lru | noeviction
	clock uint64

	blockCh chan struct{} // 阻塞弹出广播唤醒（Push 后轮换，v1.3.2）

	expiredTotal atomic.Int64
	evictedTotal atomic.Int64
	ops          atomic.Int64
	startTS      time.Time
}

// StatRow kv_stats 行。
type StatRow struct {
	NS      string
	Keys    int64
	Bytes   int64
	Hits    int64
	Misses  int64
	Expired int64
	Evicted int64
}

// Open 打开系统库并恢复内存状态，启动 TTL 扫描。
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
		db:       db,
		dir:      cfg.DBDir,
		ns:       map[string]*namespace{},
		maxB:     int64(cfg.KV.MaxMemoryMB) << 20,
		evic:     cfg.KV.Eviction,
		blockCh:  make(chan struct{}),
		startTS:  time.Now(),
	}
	if s.evic == "" {
		s.evic = "allkeys-lru"
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	go s.expireLoop(time.Duration(cfg.KV.ScanIntervalSec) * time.Second)
	logx.Infof("kv: store ready (keys=%d, max_memory_mb=%d, eviction=%s)", s.totalKeys(), cfg.KV.MaxMemoryMB, s.evic)
	return s, nil
}

// load 启动恢复（SQLite → 内存，跳过并清理已过期键）。
func (s *Store) load() error {
	rows, err := s.db.Query(`SELECT ns,key,type,expires_at FROM kv_meta`)
	if err != nil {
		return err
	}
	type metaRow struct{ ns, key, typ string; exp int64 }
	var metas []metaRow
	for rows.Next() {
		var m metaRow
		if err := rows.Scan(&m.ns, &m.key, &m.typ, &m.exp); err == nil {
			metas = append(metas, m)
		}
	}
	rows.Close()
	now := time.Now().UnixMilli()
	for _, m := range metas {
		if m.exp > 0 && m.exp <= now {
			_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, m.ns, m.key)
			_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, m.ns, m.key)
			_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, m.ns, m.key)
			continue
		}
		e := &entry{expiresAt: m.exp}
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
				}
			}
			lr.Close()
		} else {
			e.typ = 's'
			_ = s.db.QueryRow(`SELECT value FROM kv_str WHERE ns=? AND key=?`, m.ns, m.key).Scan(&e.str)
		}
		s.putMem(m.ns, m.key, e)
	}
	return nil
}

func (s *Store) nsGet(ns string) *namespace {
	n := s.ns[ns]
	if n == nil {
		n = &namespace{entries: map[string]*entry{}}
		s.ns[ns] = n
	}
	return n
}

// putMem 挂入内存并计账（调用方持锁；旧值需先 unMem）。
func (s *Store) putMem(ns, key string, e *entry) {
	n := s.nsGet(ns)
	n.entries[key] = e
	sz := estEntry(key, e)
	n.bytes += sz
	s.mem += sz
	s.clock++
	e.lastUsed = s.clock
}

// unMem 摘除内存并计账。
func (s *Store) unMem(ns, key string) {
	n := s.ns[ns]
	if n == nil {
		return
	}
	e, ok := n.entries[key]
	if !ok {
		return
	}
	sz := estEntry(key, e)
	n.bytes -= sz
	s.mem -= sz
	delete(n.entries, key)
}

// estEntry 估算内存占用（粗略但单调）。
func estEntry(key string, e *entry) int64 {
	base := int64(len(key)) + 96
	if e.typ == 's' {
		return base + int64(len(e.str))
	}
	total := base
	for _, it := range e.items {
		total += int64(len(it.val)) + 24
	}
	return total
}

// expireLocked 惰性过期检查；返回键是否仍有效。
func (s *Store) expireLocked(ns, key string, e *entry) bool {
	if e.expiresAt > 0 && e.expiresAt <= time.Now().UnixMilli() {
		s.unMem(ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
		s.expiredTotal.Add(1)
		if n := s.ns[ns]; n != nil {
			n.expired++
		}
		return false
	}
	return true
}

// touch 更新 LRU 时钟。
func (s *Store) touch(e *entry) {
	s.clock++
	e.lastUsed = s.clock
}

// ---------------------------------------------------------------------------
// 命令实现
// ---------------------------------------------------------------------------

// Get GET key。
func (s *Store) Get(ns, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	n := s.nsGet(ns)
	e := n.entries[key]
	if e == nil || !s.expireLocked(ns, key, e) {
		n.misses++
		return nil, false, nil
	}
	if e.typ != 's' {
		return nil, false, errWrongType
	}
	s.touch(e)
	n.hits++
	return e.str, true, nil
}

var errWrongType = fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")

// Set SET key value [ttlMs] [nx|xx]（nx/xx: ' ' 不限、n 仅不存在、x 仅存在）。
// 返回 (applied, err)。
func (s *Store) Set(ns, key string, val []byte, ttlMs int64, mode byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	n := s.nsGet(ns)
	if e := n.entries[key]; e != nil && s.expireLocked(ns, key, e) {
		if mode == 'n' {
			return false, nil
		}
	} else if mode == 'x' {
		return false, nil
	}
	if s.evic == "noeviction" && s.maxB > 0 {
		if e := n.entries[key]; e != nil {
			old := estEntry(key, e)
			if s.mem- old + int64(len(key))+96+int64(len(val)) > s.maxB {
				return false, errOOM
			}
		} else if s.mem+int64(len(key))+96+int64(len(val)) > s.maxB {
			return false, errOOM
		}
	}
	// 清旧值（类型切换时连 list 行）
	s.unMem(ns, key)
	_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
	e := &entry{typ: 's', str: val}
	if ttlMs > 0 {
		e.expiresAt = time.Now().UnixMilli() + ttlMs // 相对 TTL → 绝对时刻
	}
	s.putMem(ns, key, e)
	now := time.Now().UnixMilli()
	_, _ = s.db.Exec(`INSERT INTO kv_meta(ns,key,type,expires_at,minseq,maxseq,created_ts,updated_ts)
		VALUES(?,?,'s',?,0,-1,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET type='s', expires_at=excluded.expires_at,
		minseq=0, maxseq=-1, updated_ts=excluded.updated_ts`, ns, key, e.expiresAt, now, now)
	_, _ = s.db.Exec(`INSERT INTO kv_str(ns,key,value) VALUES(?,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET value=excluded.value`, ns, key, val)
	s.evictLocked(ns, key)
	return true, nil
}

var errOOM = fmt.Errorf("OOM command not allowed when used memory > 'maxmemory'")

// Del DEL key... → 删除数。
func (s *Store) Del(ns string, keys ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	var n int64
	for _, key := range keys {
		e := s.nsGet(ns).entries[key]
		if e == nil {
			continue
		}
		if s.expireLocked(ns, key, e) {
			n++
		} else {
			continue // 刚被惰性过期清掉（行已删）
		}
		s.unMem(ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
	}
	return n
}

// Exists EXISTS key... → 命中数。
func (s *Store) Exists(ns string, keys ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	var n int64
	nd := s.nsGet(ns)
	for _, key := range keys {
		if e := nd.entries[key]; e != nil && s.expireLocked(ns, key, e) {
			n++
		}
	}
	return n
}

// SetTTL 设置/清除过期（ttlMs<=0 立即删除；EXPIRE key 0 语义）。
func (s *Store) SetTTL(ns, key string, ttlMs int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	nd := s.nsGet(ns)
	e := nd.entries[key]
	if e == nil || !s.expireLocked(ns, key, e) {
		return false, nil
	}
	if ttlMs <= 0 {
		s.unMem(ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
		return true, nil
	}
	e.expiresAt = time.Now().UnixMilli() + ttlMs
	_, _ = s.db.Exec(`UPDATE kv_meta SET expires_at=?, updated_ts=? WHERE ns=? AND key=?`,
		e.expiresAt, time.Now().UnixMilli(), ns, key)
	return true, nil
}

// Persist 清除 TTL。
func (s *Store) Persist(ns, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	nd := s.nsGet(ns)
	e := nd.entries[key]
	if e == nil || !s.expireLocked(ns, key, e) {
		return false
	}
	if e.expiresAt == 0 {
		return false
	}
	e.expiresAt = 0
	_, _ = s.db.Exec(`UPDATE kv_meta SET expires_at=0 WHERE ns=? AND key=?`, ns, key)
	return true
}

// TTL 查询剩余毫秒：-2 无键、-1 无 TTL。
func (s *Store) TTL(ns, key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	nd := s.nsGet(ns)
	e := nd.entries[key]
	if e == nil || !s.expireLocked(ns, key, e) {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	nd := s.nsGet(ns)
	e := nd.entries[key]
	if e == nil || !s.expireLocked(ns, key, e) {
		return "none"
	}
	if e.typ == 'l' {
		return "list"
	}
	return "string"
}

// IncrBy INCR/DECR/INCRBY/DECRBY 族。
func (s *Store) IncrBy(ns, key string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	nd := s.nsGet(ns)
	e := nd.entries[key]
	var cur int64
	if e != nil && s.expireLocked(ns, key, e) {
		if e.typ != 's' {
			return 0, errWrongType
		}
		v, err := strconv.ParseInt(strings.TrimSpace(string(e.str)), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("ERR value is not an integer or out of range")
		}
		cur = v
	}
	nv := cur + delta
	s.storeStrLocked(ns, key, []byte(strconv.FormatInt(nv, 10)), ttlKeep(e))
	return nv, nil
}

func ttlKeep(e *entry) int64 {
	if e == nil {
		return 0
	}
	return e.expiresAt
}

// storeStrLocked 写 string 值（保留 TTL 语义：ttlAt 原样恢复）。
func (s *Store) storeStrLocked(ns, key string, val []byte, ttlAt int64) {
	s.unMem(ns, key)
	_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
	e := &entry{typ: 's', str: val, expiresAt: ttlAt}
	s.putMem(ns, key, e)
	now := time.Now().UnixMilli()
	_, _ = s.db.Exec(`INSERT INTO kv_meta(ns,key,type,expires_at,minseq,maxseq,created_ts,updated_ts)
		VALUES(?,?,'s',?,0,-1,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET type='s', expires_at=excluded.expires_at, updated_ts=excluded.updated_ts`,
		ns, key, ttlAt, now, now)
	_, _ = s.db.Exec(`INSERT INTO kv_str(ns,key,value) VALUES(?,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET value=excluded.value`, ns, key, val)
	s.evictLocked(ns, key)
}

// Append APPEND key val → 新长度。
func (s *Store) Append(ns, key string, val []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	nd := s.nsGet(ns)
	e := nd.entries[key]
	ttlAt := int64(0)
	var cur []byte
	if e != nil && s.expireLocked(ns, key, e) {
		if e.typ != 's' {
			return 0, errWrongType
		}
		cur = e.str
		ttlAt = e.expiresAt
	}
	nv := append(append([]byte{}, cur...), val...)
	s.storeStrLocked(ns, key, nv, ttlAt)
	return int64(len(nv)), nil
}

// MGet MGET key...（缺失 → nil 元素）。
func (s *Store) MGet(ns string, keys []string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	nd := s.nsGet(ns)
	out := make([][]byte, len(keys))
	for i, key := range keys {
		if e := nd.entries[key]; e != nil && s.expireLocked(ns, key, e) && e.typ == 's' {
			s.touch(e)
			nd.hits++
			out[i] = e.str
		} else {
			nd.misses++
		}
	}
	return out
}

// MSet MSET k v ...。
func (s *Store) MSet(ns string, kvs [][2][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	for _, kv := range kvs {
		s.storeStrLocked(ns, string(kv[0]), kv[1], 0)
	}
}

// Keys KEYS pattern（glob：* ?）。
func (s *Store) Keys(ns, pattern string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	nd := s.nsGet(ns)
	var out []string
	for key, e := range nd.entries {
		if !s.expireLocked(ns, key, e) {
			continue
		}
		if globMatch(pattern, key) {
			out = append(out, key)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// list 队列
// ---------------------------------------------------------------------------

func (s *Store) listLocked(ns, key string) *entry {
	nd := s.nsGet(ns)
	e := nd.entries[key]
	if e != nil && s.expireLocked(ns, key, e) {
		if e.typ != 'l' {
			return nil
		}
		return e
	}
	return nil
}

// listIndexLocked 取/建 list 项。
func (s *Store) listIndexLocked(ns, key string, create bool) *entry {
	nd := s.nsGet(ns)
	e := nd.entries[key]
	if e != nil && s.expireLocked(ns, key, e) {
		if e.typ != 'l' {
			return nil
		}
		return e
	}
	if !create {
		return nil
	}
	e = &entry{typ: 'l'}
	s.putMem(ns, key, e)
	now := time.Now().UnixMilli()
	_, _ = s.db.Exec(`INSERT INTO kv_meta(ns,key,type,expires_at,minseq,maxseq,created_ts,updated_ts)
		VALUES(?,?,'l',0,0,-1,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET type='l', minseq=0, maxseq=-1, updated_ts=excluded.updated_ts`,
		ns, key, now, now)
	_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
	return e
}

func (s *Store) listSeq(e *entry) (int64, int64) {
	if len(e.items) == 0 {
		return 0, -1
	}
	return e.items[0].seq, e.items[len(e.items)-1].seq
}

// Push LPUSH/RPUSH（left=true 左插）→ 新长度。
func (s *Store) Push(ns, key string, vals [][]byte, left bool) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	e := s.listIndexLocked(ns, key, true)
	if e == nil {
		return 0, errWrongType
	}
	oldEst := estEntry(key, e)
	minS, maxS := s.listSeq(e)
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
		_, _ = s.db.Exec(`INSERT INTO kv_list(ns,key,seq,value) VALUES(?,?,?,?)
			ON CONFLICT(ns,key,seq) DO UPDATE SET value=excluded.value`, ns, key, seq, v)
	}
	s.adjMem(ns, key, e, oldEst)
	_, _ = s.db.Exec(`UPDATE kv_meta SET minseq=?, maxseq=?, updated_ts=? WHERE ns=? AND key=?`,
		minS, maxS, time.Now().UnixMilli(), ns, key)
	s.evictLocked(ns, key)
	s.notifyPushLocked() // 唤醒阻塞弹出（BLPOP/BRPOP，v1.3.2）
	return int64(len(e.items)), nil
}

// adjMem 按估算差值调账（list 增删后；oldEst 为变更前估值）。
func (s *Store) adjMem(ns, key string, e *entry, oldEst int64) {
	n := s.ns[ns]
	if n == nil {
		return
	}
	delta := estEntry(key, e) - oldEst
	n.bytes += delta
	s.mem += delta
}

// Pop LPOP/RPOP（count<=0 单个；返回元素列表；键不存在返回空）。
func (s *Store) Pop(ns, key string, left bool, count int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	return s.popLocked(ns, key, left, count)
}

// popLocked 弹出实现（调用方持 s.mu）。
func (s *Store) popLocked(ns, key string, left bool, count int) ([][]byte, error) {
	e := s.listLocked(ns, key)
	if e == nil {
		if s.nsGet(ns).entries[key] != nil {
			return nil, errWrongType
		}
		return nil, nil
	}
	if len(e.items) == 0 {
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
	for i := 0; i < count; i++ {
		var it listItem
		if left {
			it = e.items[0]
			e.items = e.items[1:]
		} else {
			it = e.items[len(e.items)-1]
			e.items = e.items[:len(e.items)-1]
		}
		out = append(out, it.val)
		_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=? AND seq=?`, ns, key, it.seq)
	}
	if len(e.items) == 0 {
		s.unMem(ns, key)
		_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, ns, key)
	} else {
		minS, maxS := s.listSeq(e)
		_, _ = s.db.Exec(`UPDATE kv_meta SET minseq=?, maxseq=?, updated_ts=? WHERE ns=? AND key=?`,
			minS, maxS, time.Now().UnixMilli(), ns, key)
		s.adjMem(ns, key, e, oldEst)
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
		s.mu.Lock()
		ch := s.blockCh // 先取唤醒通道再扫数据（持锁内），避免丢失唤醒
		for _, key := range keys {
			items, err := s.popLocked(ns, key, left, 1)
			if err != nil {
				s.mu.Unlock()
				return "", nil, false, err
			}
			if len(items) > 0 {
				s.mu.Unlock()
				s.ops.Add(1)
				return key, items[0], true, nil
			}
		}
		s.mu.Unlock()
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			s.ops.Add(1)
			return "", nil, false, nil
		}
		var wait <-chan time.Time
		if !deadline.IsZero() {
			t := time.NewTimer(time.Until(deadline))
			wait = t.C
			select {
			case <-ch:
				t.Stop()
			case <-wait:
				s.ops.Add(1)
				return "", nil, false, nil
			}
			continue
		}
		<-ch // 无限等待：仅 Push 唤醒
	}
}

// notifyPushLocked Push 后唤醒全部阻塞弹出者（轮换广播通道；调用方持 s.mu）。
func (s *Store) notifyPushLocked() {
	close(s.blockCh)
	s.blockCh = make(chan struct{})
}

// LLen LLEN（键不存在 → 0；类型不符 → errWrongType）。
func (s *Store) LLen(ns, key string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	e := s.listLocked(ns, key)
	if e == nil {
		if s.nsGet(ns).entries[key] != nil && s.TypeLocked(ns, key) == "list" {
			return 0, nil
		}
		if s.TypeLocked(ns, key) == "string" {
			return 0, errWrongType
		}
		return 0, nil
	}
	return int64(len(e.items)), nil
}

// TypeLocked 内部（调用方持锁）。
func (s *Store) TypeLocked(ns, key string) string {
	nd := s.ns[ns]
	if nd == nil {
		return "none"
	}
	e := nd.entries[key]
	if e == nil || !s.expireLocked(ns, key, e) {
		return "none"
	}
	if e.typ == 'l' {
		return "list"
	}
	return "string"
}

// LRange LRANGE key start stop（含负下标；键不存在返回空）。
func (s *Store) LRange(ns, key string, start, stop int64) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	e := s.listLocked(ns, key)
	if e == nil {
		if s.TypeLocked(ns, key) == "string" {
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
	s.touch(e)
	return out, nil
}

// Flush FLUSHDB：清空本命名空间。
func (s *Store) Flush(ns string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ops.Add(1)
	n := s.ns[ns]
	var cnt int64
	if n != nil {
		cnt = int64(len(n.entries))
	}
	delete(s.ns, ns)
	_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=?`, ns)
	_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=?`, ns)
	_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=?`, ns)
	s.recalcMem()
	return cnt
}

// recalcMem 全局内存重算。
func (s *Store) recalcMem() {
	var total int64
	for _, n := range s.ns {
		var nb int64
		for k, e := range n.entries {
			nb += estEntry(k, e)
		}
		n.bytes = nb
		total += nb
	}
	s.mem = total
}

// ---------------------------------------------------------------------------
// 内存治理
// ---------------------------------------------------------------------------

// evictLocked 写后按策略淘汰（noeviction 由调用方前置检查）。
func (s *Store) evictLocked(protectNs, protectKey string) {
	if s.maxB <= 0 || s.evic == "noeviction" {
		return
	}
	for s.mem > s.maxB {
		var pickNs, pickKey string
		var best uint64
		found := false
		for ns, n := range s.ns {
			for k, e := range n.entries {
				if ns == protectNs && k == protectKey {
					continue
				}
				if s.evic == "volatile-lru" && e.expiresAt == 0 {
					continue
				}
				if !found || e.lastUsed < best {
					pickNs, pickKey, best, found = ns, k, e.lastUsed, true
				}
			}
		}
		if !found {
			return
		}
		s.unMem(pickNs, pickKey)
		_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, pickNs, pickKey)
		_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, pickNs, pickKey)
		_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, pickNs, pickKey)
		s.evictedTotal.Add(1)
		if n := s.ns[pickNs]; n != nil {
			n.evicted++
		}
	}
}

// SetMaxMemory 热改内存上限（MB，0=不限）。
func (s *Store) SetMaxMemory(mb int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if mb < 0 {
		mb = 0
	}
	s.maxB = int64(mb) << 20
	s.evictLocked("", "")
}

// SetEviction 热改淘汰策略。
func (s *Store) SetEviction(policy string) error {
	switch policy {
	case "allkeys-lru", "volatile-lru", "noeviction":
	default:
		return fmt.Errorf("invalid eviction %q (allkeys-lru|volatile-lru|noeviction)", policy)
	}
	s.mu.Lock()
	s.evic = policy
	s.evictLocked("", "")
	s.mu.Unlock()
	return nil
}

// MaxMemoryMB 当前上限。
func (s *Store) MaxMemoryMB() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.maxB >> 20)
}

// Eviction 当前策略。
func (s *Store) Eviction() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.evic
}

// Stats kv_stats 行（每命名空间 + '*' 合计）。
func (s *Store) Stats() []StatRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []StatRow
	total := StatRow{NS: "*"}
	for ns, n := range s.ns {
		r := StatRow{NS: ns, Keys: int64(len(n.entries)), Bytes: n.bytes,
			Hits: n.hits, Misses: n.misses, Expired: n.expired, Evicted: n.evicted}
		total.Keys += r.Keys
		total.Bytes += r.Bytes
		total.Hits += r.Hits
		total.Misses += r.Misses
		total.Expired += r.Expired
		total.Evicted += r.Evicted
		out = append(out, r)
	}
	out = append(out, total)
	return out
}

// MemBytes 当前估算内存占用。
func (s *Store) MemBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mem
}

// OpsTotal 累计命令数。
func (s *Store) OpsTotal() int64 { return s.ops.Load() }

// UptimeSec 运行秒数。
func (s *Store) UptimeSec() int64 { return int64(time.Since(s.startTS).Seconds()) }

func (s *Store) totalKeys() int64 {
	var n int64
	for _, ns := range s.ns {
		n += int64(len(ns.entries))
	}
	return n
}

// expireLoop 定期扫描过期键（主动过期）。
func (s *Store) expireLoop(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for range t.C {
		now := time.Now().UnixMilli()
		type dead struct{ ns, key string }
		var list []dead
		s.mu.Lock()
		for ns, n := range s.ns {
			for k, e := range n.entries {
				if e.expiresAt > 0 && e.expiresAt <= now {
					list = append(list, dead{ns, k})
				}
			}
		}
		for _, d := range list {
			s.unMem(d.ns, d.key)
			_, _ = s.db.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, d.ns, d.key)
			_, _ = s.db.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, d.ns, d.key)
			_, _ = s.db.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, d.ns, d.key)
			s.expiredTotal.Add(1)
			if n := s.ns[d.ns]; n != nil {
				n.expired++
			}
		}
		s.mu.Unlock()
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
