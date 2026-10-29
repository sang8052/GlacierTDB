package s3

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// chunk 池（v1.3.13 §1.1 存储底层核心改造）
//
// 此前分块数据按「对象+版本私有」存于各桶 chk_<bucket> 表，跨对象/版本不共享：
// CopyObject 整体复制数据、同名覆盖直接丢旧块、multipart 重传全部重写。
// 现引入全局分块池 s3_chunks（内容寻址 SHA-256 + 固定分块）：
//
//	CREATE TABLE s3_chunks (
//	  hash       TEXT PRIMARY KEY,   -- 池键 = <scopeID>_<sha256hex>（scope 前缀隔离去重范围）
//	  size       INTEGER NOT NULL,
//	  refcnt     INTEGER NOT NULL DEFAULT 0,   -- 引用计数（与对象写入/删除同事务增减）
//	  is_delete  INTEGER NOT NULL DEFAULT 0,   -- refcnt 归零置 1：墓碑 = 纯 GC 中间态（无回收站）
//	  deleted_ts TEXT NOT NULL DEFAULT '',
//	  created_ts TEXT NOT NULL DEFAULT '',
//	  data       BLOB NOT NULL
//	);
//
// 桶分块表 chk_<bucket> 退化为引用表：(key, version_id, seq, hash)，data 列对
// 新写入恒为空 blob（存量行迁移前仍持有数据，读路径自动回退）。
// 对象 manifest 升级 v2：直接记录池键序列（{"v":2,"cs":..,"hs":[..]}）。
//
// 引用计数规则（全部在持 writeMu 的单事务内完成，v1.3.12 写路径已单事务化）：
//   - 写入引用（PutObject / UploadPart / FlashCreate / CopyObject 零拷贝）：
//     INSERT OR IGNORE 池行（有数据时）+ refcnt+1（并复活 is_delete=0——覆盖
//     「refcnt 归零墓碑未及 GC 又被新写入引用」的窗口）；
//   - 释放引用（对象删除 / 覆盖旧版本 / Abort / 删桶）：refcnt-1，归零置
//     is_delete=1 + deleted_ts；
//   - GC：挂现有生命周期 / 空间回收框架（不新造调度器）——deleted_ts 超过
//     s3.chunk_gc_ttl_sec（默认 3600）的墓碑批量删数据行，空间由既有
//     AutoReclaim 增量回收（v1.3.11）拿回。
//
// 去重范围 s3.dedup_scope（默认 user）：池键以 scopeID 为前缀——
//   bucket = <bucket>_<sha>（桶内去重）/ user = <owner>_<sha>（同属主跨桶）/
//   system = _<sha>（全实例）。⚠ system 档存在侧信道风险：可通过闪传是否秒回
//   探测他人是否拥有某文件（文档必须标注）。范围切换不追溯既有数据（不同
//   前缀的池键互不命中），仅影响新写入。
// ---------------------------------------------------------------------------

// chunkManifestV2 清单 v2（v1.3.13）：池键序列（逻辑块序 i → hs[i]）+ 各块实际
// 字节数 ss[i]（与 hs 等长）。SS 必须携带——multipart 合并对象各 part 末块不满块，
// 均匀除法 seek 会错位（Range 读依赖按累计字节数定位）。
type chunkManifestV2 struct {
	V  int      `json:"v"`
	CS int64    `json:"cs"`
	HS []string `json:"hs"`
	SS []int64  `json:"ss"`
}

// parseChunkManifestV2 解析 v2 清单；返回 (池键序列, 各块字节数, 分块大小)。
// SS 缺失/长度不符（仅可能来自本版本开发期的单请求对象）按均匀 cs 合成。
func parseChunkManifestV2(s string) ([]string, []int64, int64) {
	if s == "" {
		return nil, nil, 0
	}
	var m chunkManifestV2
	if json.Unmarshal([]byte(s), &m) != nil || m.V != 2 || len(m.HS) == 0 || m.CS <= 0 {
		return nil, nil, 0
	}
	ss := m.SS
	if len(ss) != len(m.HS) {
		ss = make([]int64, len(m.HS))
		for i := range ss {
			ss[i] = m.CS
		}
	}
	return m.HS, ss, m.CS
}

// chunkSizeSeq 单次连续写入的分块尺寸序列（每块 chunkSize，末块为余数）。
func chunkSizeSeq(total, chunkSize int64, n int) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = chunkSize
	}
	if n > 0 && total > 0 {
		out[n-1] = total - chunkSize*int64(n-1)
	}
	return out
}

// scopeFor 对象写入的去重范围 ID（见包注释；范围切换不追溯既有数据）。
func (s *Store) scopeFor(b Bucket) string {
	switch s.dedupScope {
	case "bucket":
		return b.Name
	case "system":
		return ""
	default:
		return b.Owner
	}
}

// poolKey scopeID + sha256hex 组成池键。
func poolKey(scopeID, contentHash string) string { return scopeID + "_" + contentHash }

// poolUpsertRef 写路径引用：内容入池（缺失时插入）+ 计数 +1 并复活。
// 返回池键。调用方持 writeMu、tx 随主事务提交。
func (s *Store) poolUpsertRef(tx dbtx, scopeID string, data []byte) (string, error) {
	pk := poolKey(scopeID, hexSHA256(data))
	if _, err := tx.Exec(`INSERT INTO s3_chunks(hash,size,refcnt,is_delete,deleted_ts,created_ts,data)
		VALUES(?,?,0,0,'',?,?) ON CONFLICT(hash) DO NOTHING`, pk, len(data), nowTS(), data); err != nil {
		return "", err
	}
	if _, err := tx.Exec(`UPDATE s3_chunks SET refcnt=refcnt+1, is_delete=0, deleted_ts='' WHERE hash=?`, pk); err != nil {
		return "", err
	}
	return pk, nil
}

// poolRefExisting 闪传/零拷贝路径引用：池行必须已存在（无数据可补），计数 +1
// 并复活；行已被 GC 清掉返回 false（调用方整体回退为普通上传）。调用方持 writeMu。
func poolRefExisting(tx dbtx, pk string) (bool, error) {
	res, err := tx.Exec(`UPDATE s3_chunks SET refcnt=refcnt+1, is_delete=0, deleted_ts='' WHERE hash=?`, pk)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// releaseChunkRefs 释放一批引用（每 hash 按 ref 行数计减；归零置墓碑）。
// 调用方持 writeMu；q 传事务则随主事务提交。
func releaseChunkRefs(q dbtx, hashes []string) error {
	if len(hashes) == 0 {
		return nil
	}
	cnt := map[string]int64{}
	for _, h := range hashes {
		if h == "" {
			continue // 存量未迁移行无 hash
		}
		cnt[h]++
	}
	for h, n := range cnt {
		// SQLite UPDATE 全部引用旧行值：refcnt-? 即旧计数减 n，CASE 同步判定
		if _, err := q.Exec(`UPDATE s3_chunks SET
			refcnt=MAX(0,refcnt-?),
			is_delete=CASE WHEN refcnt-?<=0 THEN 1 ELSE is_delete END,
			deleted_ts=CASE WHEN refcnt-?<=0 THEN ? ELSE deleted_ts END
			WHERE hash=?`, n, n, n, nowTS(), h); err != nil {
			return err
		}
	}
	return nil
}

// collectChunkHashes 取引用行的池键（删除/复制/删桶前收集；key 为空 = 全表——
// 删桶用）。调用方持 writeMu 或纯读均可。
func (s *Store) collectChunkHashes(tbl, key, versionID string) ([]string, error) {
	q := fmt.Sprintf(`SELECT hash FROM %s`, tbl)
	var args []any
	if key != "" {
		q += ` WHERE key=?`
		args = append(args, key)
		if versionID != "" {
			q += ` AND version_id=?`
			args = append(args, versionID)
		}
	}
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err == nil {
			out = append(out, h)
		}
	}
	return out, rows.Err()
}

// writeChunkRefsFromReader 流式把 rc 按块写入池 + 引用表，返回 (池键序列, 字节数)。
// 调用方持 writeMu；tx 随主事务提交。seq 从 startSeq 起（multipart 暂存段用）。
func (s *Store) writeChunkRefsFromReader(tx *sql.Tx, tbl, key, ver, scopeID string, startSeq int64,
	rc io.Reader) ([]string, int64, error) {
	buf := make([]byte, s.chunkSize)
	var hashes []string
	var total int64
	var seq int64
	for {
		n, rerr := io.ReadFull(rc, buf)
		if n > 0 {
			pk, err := s.poolUpsertRef(tx, scopeID, buf[:n])
			if err != nil {
				return nil, total, err
			}
			if _, err := tx.Exec(fmt.Sprintf(`INSERT INTO %s(key,version_id,seq,hash,data) VALUES(?,?,?,?,x'')`, tbl),
				key, ver, startSeq+seq, pk); err != nil {
				return nil, total, err
			}
			hashes = append(hashes, pk)
			total += int64(n)
			seq++
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			return hashes, total, nil
		}
		if rerr != nil {
			return nil, total, rerr
		}
	}
}

// GCChunkTombstones 清理到期墓碑（is_delete=1 且 deleted_ts 超过 TTL）。
// 挂生命周期扫描（lifecycleLoop）周期调用——不新造调度器；持 writeMu 与写路径
// 互斥（复活窗口与删除互斥：引用与 GC 串行）。返回清理的池行数。
func (s *Store) GCChunkTombstones() int64 {
	cutoff := time.Now().Add(-time.Duration(s.chunkGCTTL) * time.Second).Format("2006-01-02 15:04:05")
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`DELETE FROM s3_chunks WHERE is_delete=1 AND deleted_ts<>'' AND deleted_ts<?`, cutoff)
	if err != nil {
		return 0
	}
	n, _ := res.RowsAffected()
	return n
}

// PoolStats 池规模统计（SHOW S3 SPACE 展示用）。
type PoolStats struct {
	Chunks     int64 // 池内 chunk 总数
	LiveBytes  int64 // 存活（refcnt>0）chunk 数据字节
	Tombstones int64 // 墓碑数（is_delete=1，待 GC）
	TombBytes  int64 // 墓碑数据字节（GC 后可回收）
	SharedRefs int64 // refcnt>1 的 chunk 数（发生了实际共享）
}

// PoolStats 读取当前池统计（纯读）。
func (s *Store) PoolStats() PoolStats {
	var st PoolStats
	_ = s.db.QueryRow(`SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN refcnt>0 THEN size ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN is_delete=1 THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN is_delete=1 THEN size ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN refcnt>1 THEN 1 ELSE 0 END),0)
		FROM s3_chunks`).Scan(&st.Chunks, &st.LiveBytes, &st.Tombstones, &st.TombBytes, &st.SharedRefs)
	return st
}

// migrateChunkPool 存量桶后台逐桶幂等迁移（v1.3.13）：把 chk_<bucket> 的私有
// 数据块搬入池（hash 列回填、data 清空、refcnt 建立）。不阻塞启动（后台
// goroutine，每批 64 行让出写锁）；重复启动安全（hash<>'' 的行天然跳过）。
func (s *Store) migrateChunkPool() {
	for _, b := range s.ListBuckets("") {
		if err := s.migrateBucketChunkPool(b); err != nil {
			logx.Warnf("s3: chunk pool migrate bucket %s: %v", b.Name, err)
		}
	}
	logx.Infof("s3: chunk pool migration done (scope=%s)", s.dedupScope)
}

func (s *Store) migrateBucketChunkPool(b Bucket) error {
	tbl := chunkTableFor(b.TableName)
	scopeID := s.scopeFor(b)
	const batch = 64
	for {
		type refRow struct {
			key, ver string
			seq      int64
			data     []byte
		}
		s.writeMu.Lock()
		rows, err := s.db.Query(fmt.Sprintf(
			`SELECT key,version_id,seq,data FROM %s WHERE hash='' AND length(data)>0 LIMIT ?`, tbl), batch)
		if err != nil {
			s.writeMu.Unlock()
			return err
		}
		var batchRows []refRow
		for rows.Next() {
			var r refRow
			if err := rows.Scan(&r.key, &r.ver, &r.seq, &r.data); err == nil {
				batchRows = append(batchRows, r)
			}
		}
		rows.Close()
		if len(batchRows) == 0 {
			s.writeMu.Unlock()
			return nil
		}
		tx, err := s.db.Begin()
		if err != nil {
			s.writeMu.Unlock()
			return err
		}
		failed := false
		for _, r := range batchRows {
			pk, err := s.poolUpsertRef(tx, scopeID, r.data)
			if err != nil {
				failed = true
				break
			}
			if _, err := tx.Exec(fmt.Sprintf(`UPDATE %s SET hash=?, data=x'' WHERE key=? AND version_id=? AND seq=?`, tbl),
				pk, r.key, r.ver, r.seq); err != nil {
				failed = true
				break
			}
		}
		if failed {
			tx.Rollback()
			s.writeMu.Unlock()
			return fmt.Errorf("batch migrate failed")
		}
		if err := tx.Commit(); err != nil {
			s.writeMu.Unlock()
			return err
		}
		s.writeMu.Unlock()
		if len(batchRows) < batch {
			return nil
		}
	}
}
