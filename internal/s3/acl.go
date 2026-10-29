// Package s3 — 桶共享授权 + AK scope + 操作者维度记账（v1.3.14 §1/§2）。
//
// 桶共享模型：桶仍是单属主（s3_buckets.owner 不变），叠加共享授权表
// s3_bucket_grants(bucket, username, perm, granted_by, created_ts)，
// perm ∈ {ro, rw}。判定链（server.go 单点接入）：
//
//	属主 / super → 全权；
//	grant 命中 ro → 读（GET/HEAD/LIST/预签名读）放行、写 403；
//	grant 命中 rw → 全部放行（DELETE 仍受 WORM/对象锁约束——权限模型不越过保留策略）；
//	无 grant → 403（v1.3.11 租户隔离基线不放宽）。
//
// 不支持转授（无 grant_option）：grantee 不能把共享桶再授给第三人；
// granted_by 字段留作将来扩展。rw grant 的删除受对象锁约束照旧。
//
// AK scope（§2）：s3_keys.scope JSON（空 = 现状全量）：
//
//	{"buckets":["imgs","logs*"], "prefixes":["report/"], "perm":"ro"}
//
// buckets 支持尾部 * 通配（空 = 全部桶）；prefixes 为 key 前缀收窄（LIST 请求的
// prefix/marker 必须落在 scope 前缀内，否则 403——比结果过滤简单且无泄漏面）；
// perm ro|rw（默认 rw）。存量 AK 不迁移：空 scope 行为不变。预签名 URL 天然
// 继承（服务端按 AK 行校验，ro AK 签不出可写 URL）。
package s3

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// 桶共享授权（§1）
// ---------------------------------------------------------------------------

// BucketGrant 共享授权行。
type BucketGrant struct {
	Bucket    string
	User      string
	Perm      string // ro | rw
	GrantedBy string
	CreatedTS string
}

// ValidGrantPerm 授权档位校验。
func ValidGrantPerm(p string) bool { return p == "ro" || p == "rw" }

// GrantBucket 授予共享（UPSERT：重复授予覆盖档位）。仅管理面（super）调用。
func (s *Store) GrantBucket(bucket, user, perm, grantedBy string) error {
	if !ValidGrantPerm(perm) {
		return fmt.Errorf("invalid grant perm %q (ro|rw)", perm)
	}
	b, err := s.Bucket(bucket)
	if err != nil {
		return err
	}
	if user == b.Owner {
		return fmt.Errorf("user '%s' is the owner of bucket '%s' (no grant needed)", user, bucket)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = s.db.Exec(`INSERT INTO s3_bucket_grants(bucket,username,perm,granted_by,created_ts)
		VALUES(?,?,?,?,?) ON CONFLICT(bucket,username) DO UPDATE SET perm=excluded.perm,
		granted_by=excluded.granted_by`, bucket, user, perm, grantedBy, nowTS())
	if err != nil {
		return err
	}
	s.invalidateGrants(bucket)
	return nil
}

// RevokeBucketGrant 撤销共享；级联失效该用户在该桶签发的魔法 token（§4 联动）。
func (s *Store) RevokeBucketGrant(bucket, user string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	res, err := s.db.Exec(`DELETE FROM s3_bucket_grants WHERE bucket=? AND username=?`, bucket, user)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("no grant of bucket '%s' to user '%s'", bucket, user)
	}
	_, _ = s.db.Exec(`DELETE FROM s3_magic_tokens WHERE bucket=? AND created_by=?`, bucket, user)
	s.invalidateGrants(bucket)
	return nil
}

// BucketGrants 授权列表（bucket / user 任一非空过滤；都空 = 全部，按桶+用户排序）。
func (s *Store) BucketGrants(bucket, user string) []BucketGrant {
	q := `SELECT bucket,username,perm,granted_by,created_ts FROM s3_bucket_grants`
	var conds []string
	var args []any
	if bucket != "" {
		conds = append(conds, "bucket=?")
		args = append(args, bucket)
	}
	if user != "" {
		conds = append(conds, "username=?")
		args = append(args, user)
	}
	if len(conds) > 0 {
		q += " WHERE " + strings.Join(conds, " AND ")
	}
	q += " ORDER BY bucket, username"
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []BucketGrant
	for rows.Next() {
		var g BucketGrant
		if err := rows.Scan(&g.Bucket, &g.User, &g.Perm, &g.GrantedBy, &g.CreatedTS); err == nil {
			out = append(out, g)
		}
	}
	return out
}

// grantsCacheEntry 桶授权缓存条目（挂既有 cacheMu / 2s TTL 体系；GRANT/REVOKE 主动失效）。
type grantsCacheEntry struct {
	perms    map[string]string // username → perm
	expires  time.Time
}

// grantsFor 桶授权快照（带缓存）。
func (s *Store) grantsFor(bucket string) map[string]string {
	now := time.Now()
	s.cacheMu.Lock()
	if e, ok := s.grantsCache[bucket]; ok && now.Before(e.expires) {
		s.cacheMu.Unlock()
		return e.perms
	}
	s.cacheMu.Unlock()
	perms := map[string]string{}
	rows, err := s.db.Query(`SELECT username,perm FROM s3_bucket_grants WHERE bucket=?`, bucket)
	if err == nil {
		for rows.Next() {
			var u, p string
			if rows.Scan(&u, &p) == nil {
				perms[u] = p
			}
		}
		rows.Close()
	}
	s.cacheMu.Lock()
	if s.grantsCache == nil {
		s.grantsCache = map[string]grantsCacheEntry{}
	}
	s.grantsCache[bucket] = grantsCacheEntry{perms: perms, expires: now.Add(bucketCacheTTL)}
	s.cacheMu.Unlock()
	return perms
}

// invalidateGrants 授权缓存失效（GRANT/REVOKE/删桶调用）。
func (s *Store) invalidateGrants(bucket string) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	delete(s.grantsCache, bucket)
}

// GrantPerm 命中查询：返回 (perm, true) 表示 user 对 bucket 有共享授权。
func (s *Store) GrantPerm(bucket, user string) (string, bool) {
	p, ok := s.grantsFor(bucket)[user]
	return p, ok
}

// dropBucketGrantsLocked 删桶级联（DropBucket 内调用；调用方持 writeMu）。
func (s *Store) dropBucketGrantsLocked(bucket string) {
	_, _ = s.db.Exec(`DELETE FROM s3_bucket_grants WHERE bucket=?`, bucket)
	_, _ = s.db.Exec(`DELETE FROM s3_magic_tokens WHERE bucket=?`, bucket)
	s.invalidateGrants(bucket)
}

// ---------------------------------------------------------------------------
// AK scope（§2）
// ---------------------------------------------------------------------------

// KeyScope AK 资源范围（s3_keys.scope JSON；零值 = 全量 rw，与存量行为一致）。
type KeyScope struct {
	Buckets []string `json:"buckets,omitempty"` // 允许桶（尾部 * 通配；空 = 全部）
	Prefixes []string `json:"prefixes,omitempty"` // key 前缀收窄（空 = 不限）
	Perm    string   `json:"perm,omitempty"`    // ro | rw（空 = rw）
}

// ParseKeyScope 解析 scope JSON（空串 = 全量）。
func ParseKeyScope(s string) (KeyScope, error) {
	var ks KeyScope
	s = strings.TrimSpace(s)
	if s == "" {
		return ks, nil
	}
	if err := json.Unmarshal([]byte(s), &ks); err != nil {
		return ks, fmt.Errorf("bad key scope json: %w", err)
	}
	switch ks.Perm {
	case "", "rw", "ro":
	default:
		return KeyScope{}, fmt.Errorf("bad key scope perm %q (ro|rw)", ks.Perm)
	}
	for _, p := range ks.Prefixes {
		if p == "" || strings.Contains(p, "*") {
			return KeyScope{}, fmt.Errorf("key scope prefix %q invalid (non-empty, no wildcard)", p)
		}
	}
	return ks, nil
}

// String 展示摘要（SHOW S3 KEYS 用）：buckets[..] prefixes[..] ro。
func (ks KeyScope) String() string {
	if len(ks.Buckets) == 0 && len(ks.Prefixes) == 0 && (ks.Perm == "" || ks.Perm == "rw") {
		return "full"
	}
	var parts []string
	if len(ks.Buckets) > 0 {
		parts = append(parts, "buckets["+strings.Join(ks.Buckets, ",")+"]")
	}
	if len(ks.Prefixes) > 0 {
		parts = append(parts, "prefixes["+strings.Join(ks.Prefixes, ",")+"]")
	}
	if ks.Perm == "ro" {
		parts = append(parts, "ro")
	}
	if len(parts) == 0 {
		return "full"
	}
	return strings.Join(parts, " ")
}

// MarshalScope 序列化落库（零值 → 空串，保持存量行兼容）。
func (ks KeyScope) MarshalScope() string {
	if len(ks.Buckets) == 0 && len(ks.Prefixes) == 0 && ks.Perm != "ro" {
		return ""
	}
	b, _ := json.Marshal(ks)
	return string(b)
}

// bucketMatch 桶名匹配（尾部 * 通配；空列表 = 全部）。
func (ks KeyScope) bucketMatch(bucket string) bool {
	if len(ks.Buckets) == 0 {
		return true
	}
	for _, pat := range ks.Buckets {
		if strings.HasSuffix(pat, "*") {
			if strings.HasPrefix(bucket, strings.TrimSuffix(pat, "*")) {
				return true
			}
			continue
		}
		if bucket == pat {
			return true
		}
	}
	return false
}

// prefixMatch key 前缀匹配（空列表 = 不限）。
func (ks KeyScope) prefixMatch(key string) bool {
	if len(ks.Prefixes) == 0 {
		return true
	}
	for _, p := range ks.Prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// prefixAllowed LIST 请求的 prefix/marker 必须落在 scope 前缀内（§6.2：比结果
// 过滤简单且无泄漏面——请求前缀越出 scope 直接 403）。
func (ks KeyScope) prefixAllowed(prefix, marker string) bool {
	if len(ks.Prefixes) == 0 {
		return true
	}
	// marker（继续遍历起点）也必须落在范围内，否则可逐段探测越界 key
	probe := prefix
	if marker > probe {
		probe = marker
	}
	if probe == "" {
		return false // 有前缀限制却不给 prefix 的全量 LIST：拒绝（无泄漏面）
	}
	for _, p := range ks.Prefixes {
		if strings.HasPrefix(probe, p) {
			return true
		}
	}
	return false
}

// checkKeyScope AK scope 二次校验（SigV4 验签通过后；nil = 通过）。
// 错误细分（§2 定稿）：桶越权 / 前缀越权 / 方法越权（ro 命中写动词）。
// write 由 isWriteRequest 预判；isList 标记 LIST 形态（bucket 级 GET）。
func checkKeyScope(ks KeyScope, bucket, key, prefix, marker string, write, isList bool) *s3Error {
	if len(ks.Buckets) == 0 && len(ks.Prefixes) == 0 && ks.Perm != "ro" {
		return nil // 空 scope = 现状全量
	}
	if bucket != "" && !ks.bucketMatch(bucket) {
		return &s3Error{Code: "AccessDenied", Status: 403,
			Msg: "Access Denied: key scope does not allow bucket '" + bucket + "'"}
	}
	if write && ks.Perm == "ro" {
		return &s3Error{Code: "AccessDenied", Status: 403,
			Msg: "Access Denied: key is read-only (scope perm=ro), write is not allowed"}
	}
	if isList {
		if !ks.prefixAllowed(prefix, marker) {
			return &s3Error{Code: "AccessDenied", Status: 403,
				Msg: "Access Denied: key scope requires list prefix within " + strings.Join(ks.Prefixes, ",")}
		}
		return nil
	}
	if key != "" && !ks.prefixMatch(key) {
		return &s3Error{Code: "AccessDenied", Status: 403,
			Msg: "Access Denied: key '" + key + "' outside key scope prefixes " + strings.Join(ks.Prefixes, ",")}
	}
	return nil
}

// ---------------------------------------------------------------------------
// 操作者维度记账（§1 定稿：存储字节仍记桶属主，操作计数增加操作者分摊）
// ---------------------------------------------------------------------------

// AddOpUsage 操作者维度操作计数（operator != 桶属主 时才有分摊意义；
// super 为管理动作不计）。与属主记账同款内存缓冲 + 批量落库。
func (s *Store) AddOpUsage(operator, bucket, class string) {
	if operator == "" || bucket == "" {
		return
	}
	s.usageMu.Lock()
	if s.opUsageBuf == nil {
		s.opUsageBuf = map[string]*usageDelta{}
	}
	k := operator + "\x00" + bucket
	d := s.opUsageBuf[k]
	if d == nil {
		d = &usageDelta{}
		s.opUsageBuf[k] = d
	}
	switch strings.ToUpper(class) {
	case "GET":
		d.get++
	case "PUT":
		d.put++
	case "LIST":
		d.list++
	case "DELETE":
		d.del++
	}
	s.usageMu.Unlock()
}

// flushOpUsage 操作者缓冲批量落库（flushUsage 顺带调用）。
func (s *Store) flushOpUsage() {
	s.usageMu.Lock()
	if len(s.opUsageBuf) == 0 {
		s.usageMu.Unlock()
		return
	}
	buf := s.opUsageBuf
	s.opUsageBuf = map[string]*usageDelta{}
	s.usageMu.Unlock()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	for k, d := range buf {
		op, bucket, _ := strings.Cut(k, "\x00")
		_, _ = s.db.Exec(`INSERT INTO s3_usage_op(operator,bucket,get_count,put_count,list_count,delete_count,updated_ts)
			VALUES(?,?,?,?,?,?,?) ON CONFLICT(operator,bucket) DO UPDATE SET
			get_count=get_count+excluded.get_count, put_count=put_count+excluded.put_count,
			list_count=list_count+excluded.list_count, delete_count=delete_count+excluded.delete_count,
			updated_ts=excluded.updated_ts`,
			op, bucket, d.get, d.put, d.list, d.del, nowTS())
	}
}

// OpUsageRow 操作者维度用量行。
type OpUsageRow struct {
	Operator string
	Bucket   string
	Get, Put, List, Del int64
	UpdatedTS string
}

// OpUsage 操作者维度用量列表（operator 非空过滤）。
func (s *Store) OpUsage(operator string) []OpUsageRow {
	q := `SELECT operator,bucket,get_count,put_count,list_count,delete_count,updated_ts FROM s3_usage_op`
	var args []any
	if operator != "" {
		q += ` WHERE operator=?`
		args = append(args, operator)
	}
	q += ` ORDER BY operator, bucket`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []OpUsageRow
	for rows.Next() {
		var r OpUsageRow
		if err := rows.Scan(&r.Operator, &r.Bucket, &r.Get, &r.Put, &r.List, &r.Del, &r.UpdatedTS); err == nil {
			out = append(out, r)
		}
	}
	// 缓冲增量合并（实时视图）
	s.usageMu.Lock()
	for k, d := range s.opUsageBuf {
		op, bucket, _ := strings.Cut(k, "\x00")
		if operator != "" && op != operator {
			continue
		}
		hit := false
		for i := range out {
			if out[i].Operator == op && out[i].Bucket == bucket {
				out[i].Get += int64(d.get)
				out[i].Put += int64(d.put)
				out[i].List += int64(d.list)
				out[i].Del += int64(d.del)
				hit = true
				break
			}
		}
		if !hit {
			out = append(out, OpUsageRow{Operator: op, Bucket: bucket,
				Get: int64(d.get), Put: int64(d.put), List: int64(d.list), Del: int64(d.del)})
		}
	}
	s.usageMu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Operator != out[j].Operator {
			return out[i].Operator < out[j].Operator
		}
		return out[i].Bucket < out[j].Bucket
	})
	return out
}

// sharedBucketsFor 用户被共享的桶列表（ListBuckets 增补；带 shared 标注由调用方处理）。
func (s *Store) sharedBucketsFor(user string) []Bucket {
	var out []Bucket
	for _, b := range s.ListBuckets("") {
		if _, ok := s.GrantPerm(b.Name, user); ok {
			out = append(out, b)
		}
	}
	return out
}
