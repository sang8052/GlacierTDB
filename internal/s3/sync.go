package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/instance"
	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// 数据主动同步引擎（v1.3.13 §1.4「数据北上」）
//
// 两大用途：备份（桶数据推远端 S3 端点）/ 业务伴随同步（边缘 S3 的文件伴随业务
// 上云；每对象同步结果回调通知业务，由业务决定重传、改名等后续动作）。
//
// 触发面：PUT/Complete 事件驱动（notifyPut 钩子入队，不轮询扫描）+ 失败退避
// 重试队列（1s/5s/30s/2m/5m，5 次后放弃并回调 failed）。
//
// 防自旋（三层）：
//   1. 直环：出站请求带 x-glacier-instance-id；上游 GlacierTDB 的 S3 入口识别
//      该头并比对自己实例 ID——一致即 409 拒绝（server.go putObject 入口）；
//   2. 多跳环（A→B→A）：经同步写入的对象打 x-amz-meta-sync-origin 元数据标记，
//      同步规则默认跳过带标记的对象（skip_sync_origin 可关），机制上断环；
//   3. S3 所有响应头恒带 x-glacier-instance-id（双保险，§6.2）。
//
// 删除传播默认关闭（备份语义：远端保留已删对象）；规则级 sync_deletes 开启后
// 删除事件产生远端 DELETE。
// 首次全量：full_sync_on_start；定期对账：reconcile_sec 周期 List 远端比对
// （缺漏 / 大小不一致 → 补推）。
// ---------------------------------------------------------------------------

// syncJob 待同步任务。
type syncJob struct {
	rule    config.S3SyncRule
	bucket  string
	key     string
	isDel   bool
	tries   int
	cancel  chan struct{}
	stop    bool
}

// syncEngine 同步引擎（每 Server 一个；规则可空 = 完全空转）。
type syncEngine struct {
	srv   *Server
	cfg   *config.Config
	store *Store
	rules []config.S3SyncRule

	client *http.Client

	mu    sync.Mutex
	queue []*syncJob
	wake  chan struct{}
	stats syncStats

	nextReconcile map[string]int64 // 规则名 → 下次对账 unix 秒
}

type syncStats struct {
	queued      int64
	synced      int64
	failed      int64
	skipped     int64 // sync-origin 跳过 / 规则不匹配等
	lastErr     string
	lastSyncTS  string
}

// newSyncEngine 构造引擎（rules 为空返回 nil）。
func newSyncEngine(srv *Server, rules []config.S3SyncRule) *syncEngine {
	if len(rules) == 0 {
		return nil
	}
	e := &syncEngine{
		srv:           srv,
		cfg:           srv.cfg,
		store:         srv.store,
		rules:         rules,
		client:        &http.Client{Timeout: 120 * time.Second},
		wake:          make(chan struct{}, 1),
		nextReconcile: map[string]int64{},
	}
	for i := range rules {
		logx.Infof("s3 sync: rule %q bucket=%q prefix=%q -> %s/%s%s (deletes=%v full=%v reconcile=%ds)",
			rules[i].Name, rules[i].Bucket, rules[i].MatchPrefix, rules[i].RemoteURL,
			rules[i].RemoteBucket, rules[i].RemotePrefix, rules[i].SyncDeletes,
			rules[i].FullSyncOnStart, rules[i].ReconcileSec)
		if rules[i].FullSyncOnStart {
			go e.fullSync(rules[i])
		}
	}
	go e.loop()
	return e
}

// matchRules 返回命中 (bucket, key) 的规则（前缀匹配；多条规则可同时命中——
// 同一对象按规则序逐条推送）。
func (e *syncEngine) matchRules(bucket, key string) []config.S3SyncRule {
	var out []config.S3SyncRule
	for _, r := range e.rules {
		if r.Bucket != bucket {
			continue
		}
		if r.MatchPrefix != "" && !strings.HasPrefix(key, r.MatchPrefix) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// enqueuePut 对象写入事件（notifyPut 调用）。
func (e *syncEngine) objectPut(b Bucket, m ObjectMeta) {
	for _, r := range e.matchRules(b.Name, m.Key) {
		if r.SkipSyncOrigin && m.Metadata["sync-origin"] != "" {
			e.mu.Lock()
			e.stats.skipped++
			e.mu.Unlock()
			continue // 多跳断环：经同步写入的对象不再同步
		}
		e.push(&syncJob{rule: r, bucket: b.Name, key: m.Key, cancel: make(chan struct{})})
	}
}

// objectDeleted 删除事件（SyncDeletes 开启才传播）。
func (e *syncEngine) objectDeleted(bucket, key string) {
	for _, r := range e.matchRules(bucket, key) {
		if !r.SyncDeletes {
			continue
		}
		e.push(&syncJob{rule: r, bucket: bucket, key: key, isDel: true, cancel: make(chan struct{})})
	}
}

func (e *syncEngine) push(j *syncJob) {
	e.mu.Lock()
	e.queue = append(e.queue, j)
	e.stats.queued++
	e.mu.Unlock()
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// loop 单工作协程：事件驱动消费 + 退避重试（到期任务先出队）。
func (e *syncEngine) loop() {
	type retry struct {
		j     *syncJob
		next  time.Time
		tries int
	}
	var retries []retry
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		// 到期重试回填队列
		now := time.Now()
		var still []retry
		for _, rt := range retries {
			if !rt.next.After(now) {
				e.push(rt.j)
			} else {
				still = append(still, rt)
			}
		}
		retries = still

		var job *syncJob
		e.mu.Lock()
		if len(e.queue) > 0 {
			job = e.queue[0]
			e.queue = e.queue[1:]
		}
		e.mu.Unlock()
		if job == nil {
			select {
			case <-e.wake:
			case <-tick.C:
			}
			continue
		}
		if err := e.exec(job); err != nil {
			tries := job.tries + 1
			job.tries = tries
			backoff := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, 2 * time.Minute, 5 * time.Minute}
			b := backoff[len(backoff)-1]
			if tries-1 < len(backoff) {
				b = backoff[tries-1]
			}
			logx.Warnf("s3 sync: %s/%s -> %s attempt %d failed: %v", job.bucket, job.key, job.rule.Name, tries, err)
			if tries >= 5 {
				e.finish(job, false, err.Error())
				continue
			}
			retries = append(retries, retry{j: job, next: time.Now().Add(b)})
			continue
		}
		e.finish(job, true, "")
	}
}

// finish 收尾计数 + 每对象结果回调。
func (e *syncEngine) finish(j *syncJob, ok bool, errMsg string) {
	e.mu.Lock()
	if ok {
		e.stats.synced++
		e.stats.lastSyncTS = time.Now().Format("2006-01-02 15:04:05")
	} else {
		e.stats.failed++
		e.stats.lastErr = errMsg
	}
	e.stats.queued--
	e.mu.Unlock()
	if j.rule.CallbackURL != "" && !j.stop {
		e.notify(j, ok, errMsg)
	}
}

// exec 执行单个同步任务（PUT 或 DELETE）。
func (e *syncEngine) exec(j *syncJob) error {
	select {
	case <-j.cancel:
		return nil
	default:
	}
	r := j.rule
	if j.isDel {
		remoteKey := r.RemotePrefix + j.key
		req, err := http.NewRequest(http.MethodDelete, syncURL(r.RemoteURL, r.RemoteBucket, remoteKey), nil)
		if err != nil {
			return err
		}
		signV4Request(req, r.RemoteAK, r.RemoteSK, r.RemoteRegion, "")
		resp, err := e.client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		return fmt.Errorf("remote DELETE responded %d", resp.StatusCode)
	}

	m, rc, err := e.store.GetObject(j.bucket, j.key, "")
	if err != nil {
		return err // 本地已删：任务失败（记数），不重试价值有限——退避后自然放弃
	}
	defer rc.Close()
	if r.SkipSyncOrigin && m.Metadata["sync-origin"] != "" {
		e.mu.Lock()
		e.stats.skipped++
		e.mu.Unlock()
		return nil
	}
	remoteKey := r.RemotePrefix + j.key
	req, err := http.NewRequest(http.MethodPut, syncURL(r.RemoteURL, r.RemoteBucket, remoteKey), rc)
	if err != nil {
		return err
	}
	req.ContentLength = m.Size
	if m.ContentType != "" {
		req.Header.Set("Content-Type", m.ContentType)
	}
	// 多跳断环标记 + 防自旋头（上游 GlacierTDB 比对自己实例 ID，一致即拒绝）
	req.Header.Set("x-amz-meta-sync-origin", instance.UUID())
	req.Header.Set("x-glacier-instance-id", instance.UUID())
	signV4Request(req, r.RemoteAK, r.RemoteSK, r.RemoteRegion, "")
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("remote PUT responded %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// syncURL 拼接远端对象 URL（path-style：http(s)://host/bucket/key）。
func syncURL(base, bucket, key string) string {
	return strings.TrimRight(base, "/") + "/" + bucket + "/" + escapeKeyPath(key)
}

// escapeKeyPath key 逐段 URL 转义（保留 /）。
func escapeKeyPath(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = hexEscapePathSegment(p)
	}
	return strings.Join(parts, "/")
}

func hexEscapePathSegment(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// fullSync 启动全量：列源桶匹配前缀对象，全部入队。
func (e *syncEngine) fullSync(r config.S3SyncRule) {
	marker := ""
	n := 0
	for {
		res, err := e.store.ListObjects(r.Bucket, r.MatchPrefix, "", marker, "", 1000)
		if err != nil {
			logx.Warnf("s3 sync: full sync list %s: %v", r.Bucket, err)
			return
		}
		for _, en := range res.Entries {
			if r.SkipSyncOrigin {
				if m, err := e.store.HeadObject(r.Bucket, en.Key, ""); err == nil && m.Metadata["sync-origin"] != "" {
					continue
				}
			}
			e.push(&syncJob{rule: r, bucket: r.Bucket, key: en.Key, cancel: make(chan struct{})})
			n++
		}
		if !res.IsTruncated || len(res.Entries) == 0 {
			break
		}
		marker = res.Entries[len(res.Entries)-1].Key
	}
	logx.Infof("s3 sync: full sync queued %d objects for rule %q", n, r.Name)
}

// reconcileTick 定期对账（挂生命周期扫描周期调用）：List 远端，缺漏 / 大小
// 不一致的本地对象补推。周期粒度 = 规则 reconcile_sec 与生命周期扫描周期的
// 公倍数语义简化为：每 tick 检查是否到达规则自己的周期点。
func (e *syncEngine) reconcileTick() {
	now := time.Now().Unix()
	for _, r := range e.rules {
		if r.ReconcileSec <= 0 {
			continue
		}
		e.mu.Lock()
		due := e.nextReconcile[r.Name] == 0 || now >= e.nextReconcile[r.Name]
		if due {
			e.nextReconcile[r.Name] = now + int64(r.ReconcileSec)
		}
		e.mu.Unlock()
		if !due {
			continue
		}
		go e.reconcile(r)
	}
}

// reconcile 单规则对账：远端 ListObjectsV2 → 与本地比对（存在性 + 大小）。
func (e *syncEngine) reconcile(r config.S3SyncRule) {
	remote := map[string]int64{}
	prefix := r.RemotePrefix
	token := ""
	for {
		u := syncURL(r.RemoteURL, r.RemoteBucket, "") + "?list-type=2&prefix=" + hexEscapePathSegment(prefix)
		if token != "" {
			u += "&continuation-token=" + hexEscapePathSegment(token)
		}
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			return
		}
		signV4Request(req, r.RemoteAK, r.RemoteSK, r.RemoteRegion, "")
		resp, err := e.client.Do(req)
		if err != nil {
			logx.Warnf("s3 sync: reconcile list %q: %v", r.Name, err)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
		resp.Body.Close()
		if resp.StatusCode != 200 {
			logx.Warnf("s3 sync: reconcile list %q responded %d", r.Name, resp.StatusCode)
			return
		}
		keys, sizes, next := parseListV2(string(body))
		for i, k := range keys {
			remote[strings.TrimPrefix(k, prefix)] = sizes[i]
		}
		if next == "" {
			break
		}
		token = next
	}
	marker := ""
	n := 0
	for {
		res, err := e.store.ListObjects(r.Bucket, r.MatchPrefix, "", marker, "", 1000)
		if err != nil {
			return
		}
		for _, en := range res.Entries {
			if rs, ok := remote[en.Key]; !ok || rs != en.Size {
				if r.SkipSyncOrigin {
					if m, err := e.store.HeadObject(r.Bucket, en.Key, ""); err == nil && m.Metadata["sync-origin"] != "" {
						continue
					}
				}
				e.push(&syncJob{rule: r, bucket: r.Bucket, key: en.Key, cancel: make(chan struct{})})
				n++
			}
		}
		if !res.IsTruncated || len(res.Entries) == 0 {
			break
		}
		marker = res.Entries[len(res.Entries)-1].Key
	}
	if n > 0 {
		logx.Infof("s3 sync: reconcile %q queued %d missing/stale objects", r.Name, n)
	}
}

// parseListV2 极简 ListObjectsV2 XML 解析（Key/Size/NextContinuationToken）。
func parseListV2(xmlBody string) (keys []string, sizes []int64, nextToken string) {
	for _, seg := range strings.Split(xmlBody, "<Contents>") {
		if !strings.Contains(seg, "</Contents>") {
			continue
		}
		payload := seg[:strings.Index(seg, "</Contents>")]
		key := xmlTag(payload, "Key")
		size := int64(0)
		fmt.Sscanf(xmlTag(payload, "Size"), "%d", &size)
		if key != "" {
			keys = append(keys, key)
			sizes = append(sizes, size)
		}
	}
	if v := xmlTag(xmlBody, "NextContinuationToken"); v != "" {
		// token 可能带 XML 实体转义；还原常见两个
		v = strings.ReplaceAll(v, "&amp;", "&")
		v = strings.ReplaceAll(v, "&quot;", `"`)
		nextToken = v
	}
	return
}

// xmlTag 提取首个 <tag>value</tag> 值（不做完整 XML 解析——远端应答受信）。
func xmlTag(s, tag string) string {
	open := "<" + tag + ">"
	close := "</" + tag + ">"
	i := strings.Index(s, open)
	if i < 0 {
		return ""
	}
	rest := s[i+len(open):]
	j := strings.Index(rest, close)
	if j < 0 {
		return ""
	}
	return rest[:j]
}

// notify 每对象同步结果回调（POST JSON + HMAC；fire-and-forget）。
func (e *syncEngine) notify(j *syncJob, ok bool, errMsg string) {
	payload := map[string]any{
		"event":       "sync_result",
		"rule":        j.rule.Name,
		"bucket":      j.bucket,
		"key":         j.key,
		"remote":      j.rule.RemoteBucket + "/" + j.rule.RemotePrefix + j.key,
		"delete":      j.isDel,
		"status":      map[bool]string{true: "ok", false: "failed"}[ok],
		"error":       errMsg,
		"instance_id": instance.UUID(),
		"ts":          time.Now().Format("2006-01-02 15:04:05"),
	}
	body, _ := json.Marshal(payload)
	url := j.rule.CallbackURL
	secret := j.rule.CallbackSecret
	go func() {
		for attempt, backoff := range []time.Duration{0, time.Second, 5 * time.Second} {
			time.Sleep(backoff)
			req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-Glacier-Event", "sync_result")
			if secret != "" {
				mac := hmac.New(sha256.New, []byte(secret))
				mac.Write(body)
				req.Header.Set("X-Glacier-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
			}
			resp, err := e.client.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					return
				}
			}
			logx.Warnf("s3 sync callback %s attempt %d failed (rule=%s key=%s)", url, attempt+1, j.rule.Name, j.key)
		}
	}()
}

// SyncStatsView 管理面观测（SHOW S3 SYNC；引擎未启用返回 nil）。
func (s *Server) SyncStatsView() *SyncStats {
	if s.sync == nil {
		return nil
	}
	st := s.sync.statsSnapshot()
	return &st
}

// SyncStats 观测面（SHOW S3 SYNC）。
type SyncStats struct {
	Rules      int
	Queued     int64
	Synced     int64
	Failed     int64
	Skipped    int64
	LastErr    string
	LastSyncTS string
}

func (e *syncEngine) statsSnapshot() SyncStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return SyncStats{Rules: len(e.rules), Queued: e.stats.queued, Synced: e.stats.synced,
		Failed: e.stats.failed, Skipped: e.stats.skipped, LastErr: e.stats.lastErr, LastSyncTS: e.stats.lastSyncTS}
}
