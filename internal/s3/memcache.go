package s3

// ---------------------------------------------------------------------------
// 统一内存缓存 + 纯内存对象（v1.4.0）
//
// memCache 是包内所有「可驱逐」内存字节的单一预算出口（s3.mem_cache /
// mem_cache_mb / mem_cache_item_mb）。命名空间约定：
//
//	"o:<bucket>\x00<key>\x00<version>\x00<etag>"  内联对象 content 读缓存
//	"c:<poolKey>"                                 chunk 池 BLOB 读缓存（内容寻址，
//	                                              数据不可变，永不需要失效）
//	"g:<sha256>"                                  图片派生缓存 memory 后端（imgcache.go）
//
// 保守性设计：所有读缓存键都带内容身份（version / etag / 内容哈希）——对象被
// 覆盖后元数据行给出新身份，旧键天然 miss，写路径失效只是空间卫生。因此任何
// 失效覆盖不到的场景最坏也只是「少缓存」，绝不会把旧字节当新对象返回。
//
// 纯内存对象（PUT 带 x-glacier-memory: true）不在本 LRU 内：它是该对象的唯一
// 数据副本，被 LRU 驱逐即数据丢失——由 Store.memObjs 权威持有（不驱逐），
// 写入时做显式预算准入（单条上限 / 总预算超限直接 400，见 PutMemoryObject）。
//
// s.mem == nil 表示整体禁用（s3.mem_cache: false），所有调用点判 nil。
// ---------------------------------------------------------------------------

import (
	"container/list"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
)

// memCache 字节预算 LRU（container/list + map；front = 最近使用）。
type memCache struct {
	mu       sync.Mutex
	ll       *list.List
	items    map[string]*list.Element
	bytes    int64
	maxBytes int64 // -1 = 不限；>0 = 预算字节
	maxItem  int64 // 单条目上限字节（<=0 = 不限）

	hits    int64
	misses  int64
	evicted int64
}

// memCacheEntry 缓存条目（b 为调用方字节，缓存层只读不改）。
type memCacheEntry struct {
	key  string
	b    []byte
	cost int64
}

// newMemCache 构造（maxBytes<0 = 不限；maxItem<=0 = 单条不限）。
func newMemCache(maxBytes, maxItem int64) *memCache {
	return &memCache{
		ll:       list.New(),
		items:    make(map[string]*list.Element),
		maxBytes: maxBytes,
		maxItem:  maxItem,
	}
}

// get 命中返回缓存字节（零拷贝；调用方只读，不得修改/追加/长期持有语义外引用）。
func (m *memCache) get(key string) ([]byte, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	el, ok := m.items[key]
	if !ok {
		m.misses++
		m.mu.Unlock()
		return nil, false
	}
	m.ll.MoveToFront(el)
	m.hits++
	e := el.Value.(*memCacheEntry)
	m.mu.Unlock()
	return e.b, true
}

// put 写入（cost = 计费字节，<=0 按 len(b)）。超单条上限静默拒绝（缓存语义：
// 少缓存一条不影响正确性）。同 key 重复写入按新值覆盖并重算预算。
func (m *memCache) put(key string, b []byte, cost int64) {
	if m == nil || len(b) == 0 {
		return
	}
	if cost <= 0 {
		cost = int64(len(b))
	}
	if m.maxItem > 0 && cost > m.maxItem {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		m.ll.MoveToFront(el)
		e := el.Value.(*memCacheEntry)
		m.bytes -= e.cost
		e.b, e.cost = b, cost
		m.bytes += cost
		m.trimLocked()
		return
	}
	el := m.ll.PushFront(&memCacheEntry{key: key, b: b, cost: cost})
	m.items[key] = el
	m.bytes += cost
	m.trimLocked()
}

// trimLocked 预算回收：从 LRU 尾部（最旧）逐出，直至预算内。
func (m *memCache) trimLocked() {
	if m.maxBytes < 0 {
		return
	}
	for m.bytes > m.maxBytes {
		back := m.ll.Back()
		if back == nil {
			return
		}
		e := back.Value.(*memCacheEntry)
		m.ll.Remove(back)
		delete(m.items, e.key)
		m.bytes -= e.cost
		m.evicted++
	}
}

// invalidatePrefix 前缀失效（O(n) 扫描；调用频率 = 写路径，量级可接受）。
func (m *memCache) invalidatePrefix(prefix string) {
	if m == nil || prefix == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var doomed []*list.Element
	for k, el := range m.items {
		if strings.HasPrefix(k, prefix) {
			doomed = append(doomed, el)
		}
	}
	for _, el := range doomed {
		e := el.Value.(*memCacheEntry)
		m.ll.Remove(el)
		delete(m.items, e.key)
		m.bytes -= e.cost
		m.evicted++
	}
}

// invalidate 单键失效。
func (m *memCache) invalidate(key string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if el, ok := m.items[key]; ok {
		e := el.Value.(*memCacheEntry)
		m.ll.Remove(el)
		delete(m.items, key)
		m.bytes -= e.cost
		m.evicted++
	}
}

// stats 观测计数（hits / misses / bytes / evicted / items）。
func (m *memCache) stats() (hits, misses, bytes, evicted, items int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.hits, m.misses, m.bytes, m.evicted, int64(len(m.items))
}

// ---------------------------------------------------------------------------
// Store 侧接线（nil 安全的薄包装 + 缓存键构造）
// ---------------------------------------------------------------------------

// ocacheKey 内联对象读缓存键（version + etag 入键：错位即 miss，见文件头注释）。
func ocacheKey(bucket, key, version, etag string) string {
	return "o:" + bucket + "\x00" + key + "\x00" + version + "\x00" + etag
}

// oBucketPrefix 对象 key 全版本读缓存的前缀（写路径失效用）。
func oBucketPrefix(bucket, key string) string {
	return "o:" + bucket + "\x00" + key + "\x00"
}

func (s *Store) memCacheGet(key string) ([]byte, bool) {
	if s.mem == nil {
		return nil, false
	}
	return s.mem.get(key)
}

func (s *Store) memCachePut(key string, b []byte) {
	if s.mem == nil {
		return
	}
	s.mem.put(key, b, int64(len(b)))
}

func (s *Store) memCacheInvalidatePrefix(prefix string) {
	if s.mem == nil {
		return
	}
	s.mem.invalidatePrefix(prefix)
}

// MemCacheStats 内存缓存观测（多行 "key: value"；SHOW S3 STATS 接线用，v1.4.0）。
func (s *Store) MemCacheStats() string {
	if s.mem == nil {
		return "mem_cache: disabled\n"
	}
	hits, misses, bytes, evicted, items := s.mem.stats()
	s.memObjMu.RLock()
	moBytes, moCount := s.memObjBytes, 0
	for _, m := range s.memObjs {
		moCount += len(m)
	}
	s.memObjMu.RUnlock()
	var b strings.Builder
	fmt.Fprintf(&b, "mem_cache: enabled\n")
	fmt.Fprintf(&b, "mem_cache_hits: %d\n", hits)
	fmt.Fprintf(&b, "mem_cache_misses: %d\n", misses)
	fmt.Fprintf(&b, "mem_cache_items: %d\n", items)
	fmt.Fprintf(&b, "mem_cache_bytes: %d\n", bytes)
	fmt.Fprintf(&b, "mem_cache_evicted: %d\n", evicted)
	if s.mem.maxBytes > 0 {
		fmt.Fprintf(&b, "mem_cache_budget_bytes: %d\n", s.mem.maxBytes)
	} else {
		fmt.Fprintf(&b, "mem_cache_budget_bytes: unlimited\n")
	}
	fmt.Fprintf(&b, "mem_objects: %d\n", moCount)
	fmt.Fprintf(&b, "mem_objects_bytes: %d\n", moBytes)
	return b.String()
}

// ---------------------------------------------------------------------------
// 纯内存对象（x-glacier-memory: true；权威数据，不落 SQLite，重启即失）
// ---------------------------------------------------------------------------

// memObj 纯内存对象条目（body 为权威数据，读方只读）。
type memObj struct {
	meta ObjectMeta
	body []byte
}

// PutMemoryObject 写纯内存对象（v1.4.0）。语义定稿（beta）：
//   - 元数据 + 内容仅存进程内存（memObjs 权威持有，不参与 LRU 驱逐），重启即失；
//   - 单版本覆盖：versioning 桶亦不产生版本链 / 删除标记（VersionID 恒为空）；
//   - 同 key 的既有 SQLite 副本（含全部版本）随本次写入清除——每个 key 的权威
//     副本唯一化，避免「内存对象被 DELETE 后旧行复活」的影子态；
//   - 预算准入：mem_cache 关闭 → 400；单对象超 mem_cache_item_mb → 400；
//     内存对象总量超 mem_cache_mb（非 -1 时）→ 400。显式失败优先于静默驱逐丢数据；
//   - 对象锁 / legal-hold 不适用（无 SQLite 行可挂；SetRetain / SetLegalHold 对
//     内存对象返回 NoSuchKey）。
func (s *Store) PutMemoryObject(bucket, key string, r io.Reader, attrs ObjectAttrs) (ObjectMeta, error) {
	var out ObjectMeta
	if key == "" || len(key) > 1024 {
		return out, &s3Error{Code: "InvalidArgument", Status: 400, Msg: "Invalid object key"}
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return out, err
	}
	// 读取全部内容（受 max_object_mb 约束；纯内存对象本就驻留内存，无溢出文件路径）
	var body []byte
	if s.maxObject > 0 {
		body, err = io.ReadAll(io.LimitReader(r, s.maxObject+1))
		if err == nil && int64(len(body)) > s.maxObject {
			return out, &s3Error{Code: "EntityTooLarge", Status: 400,
				Msg: fmt.Sprintf("Object size %d exceeds max_object_mb limit %d", int64(len(body)), s.maxObject>>20)}
		}
	} else {
		body, err = io.ReadAll(r)
	}
	if err != nil {
		return out, err
	}
	if err := s.memObjAdmit(int64(len(body))); err != nil {
		return out, err
	}
	sum := md5.Sum(body)
	meta := ObjectMeta{
		Key: key, IsLatest: true, Size: int64(len(body)), ETag: hex.EncodeToString(sum[:]),
		ContentType: attrs.ContentType, ContentEncoding: attrs.ContentEncoding,
		CacheControl: attrs.CacheControl, ContentDisposition: attrs.ContentDisposition,
		Metadata: attrs.Metadata, Storage: "memory", CreatedTS: nowTS(),
	}
	// 写锁内清除同 key 的 SQLite 副本（deleteAllVersionsLocked 内部自调聚合扣减）
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.deleteAllVersionsLocked(b, key); err != nil {
		return out, err
	}
	s.memObjStore(bucket, key, meta, body)
	// 读缓存空间卫生（o: 键带 version/etag 本就错位即 miss，此处顺带清理）
	s.memCacheInvalidatePrefix(oBucketPrefix(bucket, key))
	s.AddUsage(b.Owner, bucket, "PUT", int64(len(body)), 0)
	return meta, nil
}

// memObjAdmit 内存对象预算准入（v1.4.0；显式失败优先于静默驱逐丢数据）。
func (s *Store) memObjAdmit(size int64) error {
	if s.mem == nil {
		return &s3Error{Code: "InvalidRequest", Status: 400,
			Msg: "x-glacier-memory requires s3.mem_cache: true in config"}
	}
	if s.mem.maxItem > 0 && size > s.mem.maxItem {
		return &s3Error{Code: "EntityTooLarge", Status: 400,
			Msg: fmt.Sprintf("memory object size %d exceeds mem_cache_item_mb %d", size, s.mem.maxItem>>20)}
	}
	if s.mem.maxBytes > 0 {
		s.memObjMu.RLock()
		total := s.memObjBytes
		s.memObjMu.RUnlock()
		if total+size > s.mem.maxBytes {
			return &s3Error{Code: "EntityTooLarge", Status: 400,
				Msg: fmt.Sprintf("memory object store full: mem_cache_mb budget %d exceeded", s.mem.maxBytes>>20)}
		}
	}
	return nil
}

func (s *Store) memObjStore(bucket, key string, meta ObjectMeta, body []byte) {
	s.memObjMu.Lock()
	if s.memObjs == nil {
		s.memObjs = map[string]map[string]*memObj{}
	}
	m := s.memObjs[bucket]
	if m == nil {
		m = map[string]*memObj{}
		s.memObjs[bucket] = m
	}
	if old, ok := m[key]; ok {
		s.memObjBytes -= int64(len(old.body))
	}
	m[key] = &memObj{meta: meta, body: body}
	s.memObjBytes += int64(len(body))
	s.memObjMu.Unlock()
}

// memObjGet 取内存对象（meta + body 只读引用；不存在 = false）。
func (s *Store) memObjGet(bucket, key string) (ObjectMeta, []byte, bool) {
	s.memObjMu.RLock()
	defer s.memObjMu.RUnlock()
	m := s.memObjs[bucket]
	if m == nil {
		return ObjectMeta{}, nil, false
	}
	mo, ok := m[key]
	if !ok {
		return ObjectMeta{}, nil, false
	}
	return mo.meta, mo.body, true
}

// memObjMeta 取内存对象元数据。
func (s *Store) memObjMeta(bucket, key string) (ObjectMeta, bool) {
	s.memObjMu.RLock()
	defer s.memObjMu.RUnlock()
	m := s.memObjs[bucket]
	if m == nil {
		return ObjectMeta{}, false
	}
	mo, ok := m[key]
	if !ok {
		return ObjectMeta{}, false
	}
	return mo.meta, true
}

// memObjDel 删除内存对象，返回被删对象的元数据（无 → false）。
func (s *Store) memObjDel(bucket, key string) (ObjectMeta, bool) {
	s.memObjMu.Lock()
	defer s.memObjMu.Unlock()
	m := s.memObjs[bucket]
	if m == nil {
		return ObjectMeta{}, false
	}
	mo, ok := m[key]
	if !ok {
		return ObjectMeta{}, false
	}
	delete(m, key)
	s.memObjBytes -= int64(len(mo.body))
	if len(m) == 0 {
		delete(s.memObjs, bucket)
	}
	return mo.meta, true
}

// memObjMetas 桶内全部内存对象元数据快照（LIST 合并用；key 升序由调用方排）。
func (s *Store) memObjMetas(bucket string) map[string]ObjectMeta {
	s.memObjMu.RLock()
	defer s.memObjMu.RUnlock()
	m := s.memObjs[bucket]
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]ObjectMeta, len(m))
	for k, mo := range m {
		out[k] = mo.meta
	}
	return out
}

// memObjDropBucket 删桶级联清理内存对象。
func (s *Store) memObjDropBucket(bucket string) {
	s.memObjMu.Lock()
	defer s.memObjMu.Unlock()
	m := s.memObjs[bucket]
	if m == nil {
		return
	}
	for _, mo := range m {
		s.memObjBytes -= int64(len(mo.body))
	}
	delete(s.memObjs, bucket)
}
