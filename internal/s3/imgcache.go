// Package s3 — 图片预处理结果磁盘缓存（GET 加工链路，v1.3.10）。
//
// 缓存键 = sha256(bucket + key + versionId + etag + 参数规范化串)：对象被覆盖/
// 删除后 etag/version 变化，旧缓存条目自然失效（残留由 LRU 容量回收清理）。
// 存储位置 <db_dir>/sys/gimg_cache/（系统数据区，明文派生图，与底层加密库分离）；
// 条目为 <hash>.bin（结果字节）+ <hash>.meta（content-type 的 JSON）。
// 容量上限 s3.image_cache_mb（默认 1024，-1 不限）：写满后按 mtime 从旧到新淘汰。
// 单飞（single-flight）：并发同参数请求只处理一次，其余等结果落缓存。
package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"glaciertdb/internal/logx"
)

// imgCache 磁盘缓存器（dir 空 = 功能关闭，所有操作变 no-op）。
type imgCache struct {
	dir      string
	maxBytes int64 // -1 = 不限

	mu       sync.Mutex
	flights  map[string]*imgCacheFlight
	writes   atomic.Int64 // 自上次清理以来的写入字节数（触发阈值后异步 enforce）
	enforcing atomic.Bool
}

type imgCacheFlight struct {
	done chan struct{} // close 后可读结果
	data []byte
	ct   string
	ok   bool
}

// imgCacheMeta 条目元数据（<hash>.meta）。
type imgCacheMeta struct {
	ContentType string `json:"ct"`
	CreatedTS   string `json:"ts"`
}

// newImgCache 创建缓存器（dir 空 或 maxMB==0 关闭；maxMB<0 = 不限容量）。
func newImgCache(dir string, maxMB int) *imgCache {
	c := &imgCache{flights: map[string]*imgCacheFlight{}}
	if dir == "" || maxMB == 0 {
		return c
	}
	c.dir = filepath.Join(dir, "gimg_cache")
	c.maxBytes = int64(maxMB) << 20
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		logx.Warnf("s3: image cache dir init failed (%v) — 缓存停用", err)
		c.dir = ""
	}
	return c
}

// enabled 缓存是否启用。
func (c *imgCache) enabled() bool { return c != nil && c.dir != "" }

// key 计算缓存键（对象身份 + 参数规范化串）。
func (c *imgCache) key(bucket, key, versionID, etag string, p imgParams) string {
	h := sha256.New()
	h.Write([]byte(bucket))
	h.Write([]byte{0})
	h.Write([]byte(key))
	h.Write([]byte{0})
	h.Write([]byte(versionID))
	h.Write([]byte{0})
	h.Write([]byte(etag))
	h.Write([]byte{0})
	h.Write([]byte(p.canonical()))
	return hex.EncodeToString(h.Sum(nil))
}

func (c *imgCache) binPath(hash string) string  { return filepath.Join(c.dir, hash+".bin") }
func (c *imgCache) metaPath(hash string) string { return filepath.Join(c.dir, hash+".meta") }

// get 命中则返回结果与 content-type（并 touch mtime 维持 LRU 语义）。
func (c *imgCache) get(hash string) ([]byte, string, bool) {
	if !c.enabled() {
		return nil, "", false
	}
	data, err := os.ReadFile(c.binPath(hash))
	if err != nil {
		return nil, "", false
	}
	ct := ""
	if raw, err := os.ReadFile(c.metaPath(hash)); err == nil {
		var m imgCacheMeta
		if json.Unmarshal(raw, &m) == nil {
			ct = m.ContentType
		}
	}
	now := time.Now()
	_ = os.Chtimes(c.binPath(hash), now, now)
	return data, ct, true
}

// put 写入条目（临时文件 + 原子改名），超阈值后异步触发容量回收。
func (c *imgCache) put(hash string, data []byte, ct string) {
	if !c.enabled() || len(data) == 0 {
		return
	}
	meta, _ := json.Marshal(imgCacheMeta{ContentType: ct, CreatedTS: nowTS()})
	tmp, err := os.CreateTemp(c.dir, "tmp-*")
	if err != nil {
		return
	}
	name := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(name)
		return
	}
	tmp.Close()
	if err := os.Rename(name, c.binPath(hash)); err != nil {
		os.Remove(name)
		return
	}
	_ = os.WriteFile(c.metaPath(hash), meta, 0o644)
	c.writes.Add(int64(len(data)))
	if c.maxBytes > 0 && c.writes.Load() > c.maxBytes/4 && c.enforcing.CompareAndSwap(false, true) {
		go func() {
			defer c.enforcing.Store(false)
			c.enforce()
		}()
	}
}

// do 单飞执行 fn：同 key 并发请求只跑一次 fn，其余复用其结果。
func (c *imgCache) do(hash string, fn func() ([]byte, string, error)) ([]byte, string, error) {
	if !c.enabled() {
		return fn()
	}
	c.mu.Lock()
	if fl, ok := c.flights[hash]; ok {
		c.mu.Unlock()
		<-fl.done
		if fl.ok {
			return fl.data, fl.ct, nil
		}
		// 先行请求失败：本请求自行重算（失败通常是参数/数据问题，重算会得到同错）
		return fn()
	}
	fl := &imgCacheFlight{done: make(chan struct{})}
	c.flights[hash] = fl
	c.mu.Unlock()

	data, ct, err := fn()
	fl.data, fl.ct, fl.ok = data, ct, err == nil
	close(fl.done)

	c.mu.Lock()
	delete(c.flights, hash)
	c.mu.Unlock()
	return data, ct, err
}

// enforce 容量回收：目录内 .bin 按 mtime 从旧到新删除，直至总量 ≤ maxBytes。
func (c *imgCache) enforce() {
	if !c.enabled() || c.maxBytes <= 0 {
		return
	}
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	type item struct {
		path string
		size int64
		mod  time.Time
		hash string
	}
	var total int64
	var items []item
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".bin") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		total += info.Size()
		items = append(items, item{path: filepath.Join(c.dir, name), size: info.Size(), mod: info.ModTime()})
	}
	if total <= c.maxBytes {
		c.writes.Store(0)
		return
	}
	sort.Slice(items, func(i, j int) bool { return items[i].mod.Before(items[j].mod) })
	removed := 0
	for _, it := range items {
		if total <= c.maxBytes {
			break
		}
		if os.Remove(it.path) == nil {
			os.Remove(strings.TrimSuffix(it.path, ".bin") + ".meta")
			total -= it.size
			removed++
		}
	}
	c.writes.Store(0)
	if removed > 0 {
		logx.Infof("s3: image cache LRU evicted %d entries (cap %d MB)", removed, c.maxBytes>>20)
	}
}
