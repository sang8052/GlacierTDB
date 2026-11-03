package kv

// ---------------------------------------------------------------------------
// 异步落盘 / 补删引擎（v1.3.15 任务 2）
//
// 脏标记模型：写命令成功后（分片锁外、命令返回前）对受影响键标脏：
//   'w'（dirtyKindWrite）= 按当前内存状态整键重写三表；
//   'd'（dirtyKindDelete）= 删除三表行（惰性过期 / 淘汰 / 删除标记）。
// 同键后标覆盖前标（最终状态语义：删除后重建 → 只剩 'w'；写后删 → 只剩 'd'）。
//
// flusher 每 AsyncFlushMs 换出脏集，对每个脏键在分片锁内取内存快照、锁外合并为
// 批量事务写 SQLite；崩溃丢失 ≤ flush 窗口（async 模式），失败整轮回滚重试（幂等）。
//
// sync 模式：写命令内联单事务落盘、淘汰/删除同步删行，唯一经此通道的是读路径
// 惰性过期与 expireLoop 产生的 'd' 补删标记（锁内零 IO，≤ 一个 flush 周期内删净；
// 崩溃残留行由启动 load 按 expiresAt 过滤兜底）。
// ---------------------------------------------------------------------------

import (
	"database/sql"
	"strings"
	"time"

	"glaciertdb/internal/logx"
)

// 脏标记类型。
const (
	dirtyKindWrite  byte = 'w'
	dirtyKindDelete byte = 'd'
)

// flushStmtsPerTx 删除/字符串批事务的语句数上限（list 键整键独立事务，不入批）。
const flushStmtsPerTx = 256

// markDirty 标脏单键（允许持分片锁调用——锁序 shard.mu → dirtyMu，禁止反向）。
func (s *Store) markDirty(ns, key string, kind byte) {
	if s.persistMode != "async" && kind != dirtyKindDelete {
		return // sync 模式：写路径内联落盘，仅补删标记走本通道
	}
	s.dirtyMu.Lock()
	if s.dirtyKeys == nil {
		s.dirtyKeys = map[string]byte{}
	}
	s.dirtyKeys[ns+"\x00"+key] = kind
	s.dirtyMu.Unlock()
}

// markDirtyNS 标脏整个命名空间（FLUSHDB async 模式；flusher 按命名空间整删三表）。
func (s *Store) markDirtyNS(ns string) {
	s.dirtyMu.Lock()
	if s.dirtyNS == nil {
		s.dirtyNS = map[string]struct{}{}
	}
	s.dirtyNS[ns] = struct{}{}
	s.dirtyMu.Unlock()
}

// startFlusher 启动落盘 goroutine（Open 时调用；两种模式都启动——见文件头说明）。
func (s *Store) startFlusher() {
	iv := time.Duration(s.flushMs) * time.Millisecond
	if iv < 20*time.Millisecond {
		iv = 20 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(iv)
		defer t.Stop()
		for range t.C {
			if err := s.flushOnce(); err != nil {
				s.flushFail.Add(1)
				logx.Debugf("kv flush: %v", err)
			}
		}
	}()
}

// FlushSync 立即同步执行一轮脏数据落盘（main.go 停机钩子调用，把优雅停机的
// async 丢失窗口收敛为 0；管理面亦可调用）。v1.3.15 新增。
func (s *Store) FlushSync() error { return s.flushOnce() }

// PersistMode 当前落盘模式（INFO 观测，v1.3.15）。
func (s *Store) PersistMode() string { return s.persistMode }

// FlushCounters 落盘计数快照：(成功轮数, 失败轮数, 当前待刷键数)（INFO 观测，v1.3.15）。
func (s *Store) FlushCounters() (total, fail, dirty int64) {
	s.dirtyMu.Lock()
	d := int64(len(s.dirtyKeys))
	s.dirtyMu.Unlock()
	return s.flushTotal.Load(), s.flushFail.Load(), d
}

// flushOnce 执行一轮落盘：换出脏集 → 批量事务 → 失败整轮回滚脏集重试。
func (s *Store) flushOnce() error {
	s.dirtyMu.Lock()
	wipes := s.dirtyNS
	s.dirtyNS = nil
	keys := s.dirtyKeys
	s.dirtyKeys = nil
	s.dirtyMu.Unlock()
	if len(wipes) == 0 && len(keys) == 0 {
		return nil
	}
	s.flushTotal.Add(1)
	if err := s.flushBatches(wipes, keys); err != nil {
		s.flushFail.Add(1)
		// 失败回滚脏集（幂等合并：已有标记不覆盖），下轮重试
		s.dirtyMu.Lock()
		if s.dirtyKeys == nil {
			s.dirtyKeys = map[string]byte{}
		}
		for k, v := range keys {
			if _, ok := s.dirtyKeys[k]; !ok {
				s.dirtyKeys[k] = v
			}
		}
		if s.dirtyNS == nil {
			s.dirtyNS = map[string]struct{}{}
		}
		for ns := range wipes {
			s.dirtyNS[ns] = struct{}{}
		}
		s.dirtyMu.Unlock()
		return err
	}
	return nil
}

// splitDirtyKey 脏键拆分（ns 内不含 NUL——用户名即命名空间；key 可含 NUL，
// 取首个 NUL 为分隔符）。
func splitDirtyKey(k string) (ns, key string, ok bool) {
	i := strings.IndexByte(k, 0)
	if i < 0 {
		return "", "", false
	}
	return k[:i], k[i+1:], true
}

// flushBatches 脏集 → SQLite 批量事务（单 flusher goroutine 串行写，与连接池
// 其他使用者无锁冲突；事务内 stmts 攒批提交）。
// 对任务稿「长 list 分批 1000」的取舍：list 键整键单事务（DELETE + 全量 INSERT
// 原子提交）——事务内分批无意义，跨事务分批会在崩溃时留下半写 list（v1.3.15）。
func (s *Store) flushBatches(wipes map[string]struct{}, keys map[string]byte) error {
	var tx *sql.Tx
	stmts := 0
	commit := func() error {
		if tx == nil {
			return nil
		}
		err := tx.Commit()
		tx = nil
		stmts = 0
		return err
	}
	begin := func() error {
		if tx != nil {
			return nil
		}
		t, err := s.db.Begin()
		if err != nil {
			return err
		}
		tx = t
		return nil
	}
	delRows := func(ns, key string) {
		_, _ = tx.Exec(`DELETE FROM kv_meta WHERE ns=? AND key=?`, ns, key)
		_, _ = tx.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
		_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
		stmts += 3
	}
	// 命名空间整体清空
	for ns := range wipes {
		if err := begin(); err != nil {
			return err
		}
		_, _ = tx.Exec(`DELETE FROM kv_meta WHERE ns=?`, ns)
		_, _ = tx.Exec(`DELETE FROM kv_str WHERE ns=?`, ns)
		_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=?`, ns)
		stmts += 3
		if stmts >= flushStmtsPerTx {
			if err := commit(); err != nil {
				return err
			}
		}
	}
	now := time.Now().UnixMilli()
	for k, kind := range keys {
		ns, key, ok := splitDirtyKey(k)
		if !ok {
			continue
		}
		if kind == dirtyKindDelete {
			if err := begin(); err != nil {
				return err
			}
			delRows(ns, key)
			if stmts >= flushStmtsPerTx {
				if err := commit(); err != nil {
					return err
				}
			}
			continue
		}
		// 'w'：分片锁内取内存快照，锁外写库
		sh := s.shardOf(ns, key)
		sh.mu.Lock()
		var e *entry
		if n := sh.ns[ns]; n != nil {
			e = n.entries[key]
		}
		var snap *entry
		if e != nil && !(e.expiresAt > 0 && e.expiresAt <= now) {
			snap = &entry{typ: e.typ, str: e.str, expiresAt: e.expiresAt}
			if e.typ == 'l' {
				// list 必须深拷贝：并发 RPUSH 的 append 可能原地写同一底层数组，
				// 锁外遍历浅引用会构成数据竞争（v1.3.15）
				snap.items = make([]listItem, len(e.items))
				copy(snap.items, e.items)
			}
		}
		sh.mu.Unlock()
		if snap == nil {
			// 键已消失 / 已过期（标脏后被删）→ 按删行处理
			if err := begin(); err != nil {
				return err
			}
			delRows(ns, key)
			if stmts >= flushStmtsPerTx {
				if err := commit(); err != nil {
					return err
				}
			}
			continue
		}
		if snap.typ == 's' {
			if err := begin(); err != nil {
				return err
			}
			_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
			_, _ = tx.Exec(`INSERT INTO kv_meta(ns,key,type,expires_at,minseq,maxseq,created_ts,updated_ts)
				VALUES(?,?,'s',?,0,-1,?,?)
				ON CONFLICT(ns,key) DO UPDATE SET type='s', expires_at=excluded.expires_at,
				minseq=0, maxseq=-1, updated_ts=excluded.updated_ts`, ns, key, snap.expiresAt, now, now)
			_, _ = tx.Exec(`INSERT INTO kv_str(ns,key,value) VALUES(?,?,?)
				ON CONFLICT(ns,key) DO UPDATE SET value=excluded.value`, ns, key, snap.str)
			stmts += 3
			if stmts >= flushStmtsPerTx {
				if err := commit(); err != nil {
					return err
				}
			}
			continue
		}
		// list：先提交攒批，整键独立事务
		if err := commit(); err != nil {
			return err
		}
		if err := s.flushList(ns, key, snap, now); err != nil {
			return err
		}
	}
	return commit()
}

// flushList list 键整键重写（单事务：DELETE kv_list + 全量 INSERT + UPSERT meta
// + DELETE kv_str；长 list 单事务保证崩溃不半写）。
func (s *Store) flushList(ns, key string, snap *entry, now int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	_, _ = tx.Exec(`DELETE FROM kv_list WHERE ns=? AND key=?`, ns, key)
	minS, maxS := int64(0), int64(-1)
	if len(snap.items) > 0 {
		minS, maxS = snap.items[0].seq, snap.items[len(snap.items)-1].seq
	}
	for _, it := range snap.items {
		if _, err := tx.Exec(`INSERT INTO kv_list(ns,key,seq,value) VALUES(?,?,?,?)
			ON CONFLICT(ns,key,seq) DO UPDATE SET value=excluded.value`, ns, key, it.seq, it.val); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	_, _ = tx.Exec(`INSERT INTO kv_meta(ns,key,type,expires_at,minseq,maxseq,created_ts,updated_ts)
		VALUES(?,?,'l',?,?,?,?,?)
		ON CONFLICT(ns,key) DO UPDATE SET type='l', expires_at=excluded.expires_at,
		minseq=excluded.minseq, maxseq=excluded.maxseq, updated_ts=excluded.updated_ts`,
		ns, key, snap.expiresAt, minS, maxS, now, now)
	_, _ = tx.Exec(`DELETE FROM kv_str WHERE ns=? AND key=?`, ns, key)
	return tx.Commit()
}
