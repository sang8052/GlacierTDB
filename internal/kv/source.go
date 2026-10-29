package kv

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// SQL 读穿透 read-through 回源（v1.3.13 §3.2）
//
// 语义：缓存键在 Redis 层查不到时（如 sys.cpu_name），按注册的规则透传 SQL 到
// 业务库回源查询（如 SELECT ... FROM xxx WHERE ...），结果回填缓存后返回。
//
// 规则注册（管理 SQL，仅 super）：KV SOURCE ADD <key前缀> <db> '<sql 模板>' [TTL <ms>]
//   - 模板占位：{key} = 整键代入；{N} 按 ':' 或 '.' 分段取第 N 段（{0} 起始；
//     段不足 → 回源报错按 miss 处理）；参数化绑定（防注入）；
//   - 权限边界：注册限 super；执行时以发起请求的用户身份做库授权校验
//     （透传查询只能触达同用户名下的 db——执行器由 proxy 注入并强制）；
//   - 结果形态：单行单列 → 字符串；单行多列 → JSON 对象；多行 → JSON 数组；
//   - 回填：TTL 规则级配置（默认 300000ms；≤0 = 不回填仅直返）；空结果不回填
//     （Redis GET miss 语义保持，防穿透靠 single-flight + 短 TTL 规则）；
//   - 触发面：仅 GET（MGET/HGET 不参与——批量语义下回源放大不可控）；
//   - 防击穿：同 key 并发 miss single-flight 合并为一次回源；
//   - 观测：回源命中 / 未命中 / 执行耗时计数进 INFO 与 SHOW KV STATS。
// ---------------------------------------------------------------------------

// SetSourceExecutor 注入回源执行器（proxy 启动时；nil = 关闭回源）。
func (s *Store) SetSourceExecutor(ex SourceExecutor) {
	s.srcMu.Lock()
	s.srcExec = ex
	s.srcMu.Unlock()
}

// loadSources 启动加载规则表（kv_sources；表缺失/查询失败静默降级为无规则）。
func (s *Store) loadSources() {
	rows, err := s.db.Query(`SELECT prefix,db,sql_tpl,ttl_ms,created_ts FROM kv_sources`)
	if err != nil {
		return
	}
	defer rows.Close()
	var rules []SourceRule
	for rows.Next() {
		var r SourceRule
		if rows.Scan(&r.Prefix, &r.DB, &r.SQLTpl, &r.TTLms, &r.CreatedTS) == nil {
			rules = append(rules, r)
		}
	}
	// 最长前缀优先排序
	sortByPrefixLen(rules)
	s.srcMu.Lock()
	s.srcRules = rules
	s.srcMu.Unlock()
}

// sortByPrefixLen 最长前缀优先（降序）。
func sortByPrefixLen(rs []SourceRule) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && len(rs[j].Prefix) > len(rs[j-1].Prefix); j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

// SourceRules 当前规则快照（SHOW KV SOURCE 用）。
func (s *Store) SourceRules() []SourceRule {
	s.srcMu.Lock()
	defer s.srcMu.Unlock()
	out := make([]SourceRule, len(s.srcRules))
	copy(out, s.srcRules)
	return out
}

// SourceCounters 回源计数快照（观测面）。
func (s *Store) SourceCounters() (hits, misses, errs, execUs int64) {
	return s.srcHits.Load(), s.srcMisses.Load(), s.srcErrors.Load(), s.srcExecUs.Load()
}

// AddSourceRule 注册/覆盖规则（内存 + 落库；调用方已做 super 门禁）。
func (s *Store) AddSourceRule(r SourceRule) error {
	if r.Prefix == "" || r.DB == "" || r.SQLTpl == "" {
		return fmt.Errorf("ERR KV SOURCE ADD requires <prefix> <db> '<sql template>'")
	}
	if !reDBIdent.MatchString(r.DB) {
		return fmt.Errorf("ERR invalid database name %q", r.DB)
	}
	ttl := r.TTLms
	if ttl == 0 {
		ttl = 300000
	}
	r.TTLms = ttl
	s.srcMu.Lock()
	defer s.srcMu.Unlock()
	replaced := false
	for i := range s.srcRules {
		if s.srcRules[i].Prefix == r.Prefix {
			s.srcRules[i] = r
			replaced = true
			break
		}
	}
	if !replaced {
		s.srcRules = append(s.srcRules, r)
	}
	sortByPrefixLen(s.srcRules)
	_, err := s.db.Exec(`INSERT INTO kv_sources(prefix,db,sql_tpl,ttl_ms,created_ts) VALUES(?,?,?,?,?)
		ON CONFLICT(prefix) DO UPDATE SET db=excluded.db, sql_tpl=excluded.sql_tpl,
		ttl_ms=excluded.ttl_ms`, r.Prefix, r.DB, r.SQLTpl, r.TTLms, time.Now().Format("2006-01-02 15:04:05"))
	return err
}

// DelSourceRule 删除规则（prefix 为空 = 全清）。返回删除条数。
func (s *Store) DelSourceRule(prefix string) int64 {
	s.srcMu.Lock()
	defer s.srcMu.Unlock()
	var removed int64
	var kept []SourceRule
	for _, r := range s.srcRules {
		if prefix == "" || r.Prefix == prefix {
			removed++
			continue
		}
		kept = append(kept, r)
	}
	s.srcRules = kept
	if prefix == "" {
		_, _ = s.db.Exec(`DELETE FROM kv_sources`)
	} else {
		_, _ = s.db.Exec(`DELETE FROM kv_sources WHERE prefix=?`, prefix)
	}
	return removed
}

// matchSourceRule 最长前缀命中。
func (s *Store) matchSourceRule(key string) (SourceRule, bool) {
	s.srcMu.Lock()
	defer s.srcMu.Unlock()
	for _, r := range s.srcRules {
		if strings.HasPrefix(key, r.Prefix) {
			return r, true
		}
	}
	return SourceRule{}, false
}

// renderSourceSQL 模板渲染：{key} 整键；{N} 按 ':' 或 '.' 分段。返回 (sql, 绑定值)。
func renderSourceSQL(tpl, key string) (string, []any, error) {
	var args []any
	var b strings.Builder
	rest := tpl
	for {
		i := strings.IndexByte(rest, '{')
		if i < 0 {
			b.WriteString(rest)
			break
		}
		j := strings.IndexByte(rest[i:], '}')
		if j < 0 {
			b.WriteString(rest)
			break
		}
		name := rest[i+1 : i+j]
		b.WriteString(rest[:i])
		switch {
		case name == "key":
			args = append(args, key)
			b.WriteString("?")
		case len(name) >= 1 && allDigits(name):
			// 段分隔符：':' 或 '.'（需求示例 sys.cpu_name 为点风格；两者混用均可）
			seg := strings.FieldsFunc(key, func(r rune) bool { return r == ':' || r == '.' })
			idx := int(name[0] - '0')
			for _, c := range name[1:] {
				idx = idx*10 + int(c-'0')
			}
			if idx >= len(seg) {
				return "", nil, fmt.Errorf("key segment %d out of range", idx)
			}
			args = append(args, seg[idx])
			b.WriteString("?")
		default:
			return "", nil, fmt.Errorf("unsupported placeholder {%s} (expect {key} / {N})", name)
		}
		rest = rest[i+j+1:]
	}
	return b.String(), args, nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// renderResult 回源结果 → 缓存值：单行单列 → 字符串；单行多列 → JSON 对象；
// 多行 → JSON 数组。空结果 → (nil, false)。
func renderResult(cols []string, rows [][]string) ([]byte, bool) {
	if len(rows) == 0 {
		return nil, false
	}
	keyFor := func(i int) string {
		if i < len(cols) && cols[i] != "" {
			return cols[i]
		}
		return fmt.Sprintf("c%d", i)
	}
	if len(rows) == 1 {
		r := rows[0]
		if len(r) == 1 {
			return []byte(r[0]), true
		}
		obj := map[string]string{}
		for i, v := range r {
			obj[keyFor(i)] = v
		}
		out, _ := json.Marshal(obj)
		return out, true
	}
	arr := make([]map[string]string, 0, len(rows))
	for _, r := range rows {
		obj := map[string]string{}
		for i, v := range r {
			obj[keyFor(i)] = v
		}
		arr = append(arr, obj)
	}
	out, _ := json.Marshal(arr)
	return out, true
}

// sourceLookup GET miss 时的读穿透入口：规则未命中 / 回源失败 → (nil, false)；
// 命中 → 值（并按规则 TTL NX 回填缓存）。同 key 并发 miss single-flight 合并。
func (s *Store) sourceLookup(ns, key string) ([]byte, bool) {
	rule, ok := s.matchSourceRule(key)
	if !ok {
		s.srcMisses.Add(1)
		return nil, false
	}
	s.srcMu.Lock()
	ex := s.srcExec
	// single-flight：同 (ns,key) 并发 miss 只回源一次，等待方共享结果
	fk := ns + "\x00" + key
	if c, inFlight := s.srcFlight[fk]; inFlight {
		s.srcMu.Unlock()
		<-c.done
		if c.err != nil || !c.hit {
			s.srcMisses.Add(1)
			return nil, false
		}
		s.srcHits.Add(1)
		if bv, hit, _ := s.Get(ns, key); hit { // 回填后从缓存取（与缓存一致）
			return bv, true
		}
		return c.val, true
	}
	call := &srcCall{done: make(chan struct{})}
	s.srcFlight[fk] = call
	s.srcMu.Unlock()

	defer func() {
		s.srcMu.Lock()
		delete(s.srcFlight, fk)
		s.srcMu.Unlock()
		close(call.done)
	}()

	var err error
	call.val, call.hit, err = func() ([]byte, bool, error) {
		if ex == nil {
			return nil, false, fmt.Errorf("source executor not wired")
		}
		sqlText, args, rerr := renderSourceSQL(rule.SQLTpl, key)
		if rerr != nil {
			return nil, false, rerr
		}
		start := time.Now()
		cols, rows, qerr := ex(ns, rule.DB, bindInline(sqlText, args))
		s.srcExecUs.Add(int64(time.Since(start).Microseconds()))
		if qerr != nil {
			return nil, false, qerr
		}
		val, ok := renderResult(cols, rows)
		if !ok {
			return nil, false, nil // 空结果：按 miss（不回填负缓存）
		}
		return val, true, nil
	}()
	if err != nil {
		logx.Warnf("kv source: %s.%s exec failed: %v", ns, key, err) // 观测：回源失败可见（v1.3.13）
		s.srcErrors.Add(1)
		call.err = err
		return nil, false
	}
	if !call.hit {
		s.srcMisses.Add(1)
		return nil, false
	}
	s.srcHits.Add(1)
	if rule.TTLms > 0 {
		_, _ = s.Set(ns, key, call.val, rule.TTLms, 'n') // NX 回填：不覆盖显式写入
	}
	return call.val, true
}

// srcCall single-flight 会话。
type srcCall struct {
	done chan struct{}
	val  []byte
	hit  bool
	err  error
}

// bindInline SQL 模板 + 绑定值 → 可执行 SQL（参数来源为 key 段 / 整键，可能含
// 引号——单引号加倍转义后内联；执行器侧另有库授权 + 只读 SELECT 校验）。
func bindInline(sqlText string, args []any) string {
	if len(args) == 0 {
		return sqlText
	}
	var b strings.Builder
	rest := sqlText
	for _, a := range args {
		i := strings.Index(rest, "?")
		if i < 0 {
			break
		}
		b.WriteString(rest[:i])
		b.WriteString("'" + strings.ReplaceAll(fmt.Sprint(a), "'", "''") + "'")
		rest = rest[i+1:]
	}
	b.WriteString(rest)
	return b.String()
}

// reDBIdent 库名合法性（与 sqlite 后端建库同名规则）。
var reDBIdent = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
