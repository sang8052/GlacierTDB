// Package asyncq — 异步查询引擎（v1.0.5）。
//
// SUBMIT QUERY '...' [CALLBACK url [SIGN secret]] [TIMEOUT s]
//   [AT '2026-10-16 17:00:00'] [WINDOW '17:00-20:00'] [WHEN CPU<70 AND MEM<80 ...]
//   [PARTITION BY ROWS n] [SCRIPT path [OUTPUT xls|csv]]
//
// 语义：
//   AT     —— 指定时刻执行（"就 5 点"）；到点后仍受 WHEN 门槛约束，超时 EXPIRED；
//   WINDOW —— 每日双边窗口 [起,止)（支持跨午夜）；窗口结束仍未达标 → 当日 EXPIRED（不跨天滚动）；
//   WHEN   —— 资源门槛（CPU/MEM/IO，服务器级与应用级），采样周期可配（默认 30s，最低 10s）；
//   回调   —— DONE / ERROR / EXPIRED 一律 POST 回调（可选 HMAC 签名头）；2xx 视为成功，退避重试；
//   结果   —— db_dir/jobs/<id>.jsonl 流式落盘，FETCH 分页拉取。
package asyncq

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/sqlite"
)

// State 作业状态。
type State string

const (
	StatePending   State = "PENDING"   // 等待 AT/WINDOW/WHEN
	StateRunning   State = "RUNNING"
	StateScript    State = "SCRIPT"    // 后处理管道
	StateDone      State = "DONE"
	StateError     State = "ERROR"
	StateCancelled State = "CANCELLED"
	StateExpired   State = "EXPIRED"   // 窗口结束/超时未执行
)

// Job 异步作业。
type Job struct {
	ID          string     `json:"job_id"`
	SQL         string     `json:"sql"`
	State       State      `json:"state"`
	CallbackURL string     `json:"callback_url,omitempty"` // http(s) URL / tcp:host:port / amqp:amqp://… / kafka:broker
	CallbackProto string  `json:"callback_proto,omitempty"` // http|tcp|amqp|kafka
	CallbackOpt  string    `json:"callback_opt,omitempty"`  // kafka topic / amqp routing key
	SignSecret  string     `json:"-"`
	At          *time.Time `json:"at,omitempty"`     // AT 指定时刻
	Window      [2]string  `json:"window,omitempty"` // WINDOW 每日 ["17:00","20:00"]
	TimeoutSec  int        `json:"timeout_sec"`
	SubmitTS    time.Time  `json:"submit_ts"`
	StartTS     time.Time  `json:"-"`
	EndTS       time.Time  `json:"-"`
	ElapsedMs   int64      `json:"elapsed_ms"`
	Rows        int64      `json:"rows"`
	Bytes       int64      `json:"bytes"`
	Error       string     `json:"error,omitempty"`
	ExpiresTS   time.Time  `json:"expires_ts"`
	PartRows    int64      `json:"partition_rows,omitempty"` // 分片回调大小
	RetainSec   int        `json:"retain_sec,omitempty"`     // 结果保留秒数（覆盖全局默认 60s）
	Script      string     `json:"script,omitempty"`
	ScriptOut   string     `json:"script_output,omitempty"`
	WhenExpr    string     `json:"when,omitempty"` // WHEN 资源门槛表达式
	EverySec    int        `json:"every_sec,omitempty"`    // 周期秒（0=一次性）
	EveryAt     string     `json:"every_at,omitempty"`     // EVERY DAY AT 'HH:MM' 的每日时刻
	RecurGroup  string     `json:"recur_group,omitempty"`  // 周期链 ID；CANCEL 取消整链
	CancelCh    chan struct{} `json:"-"`
}

// Engine 异步查询引擎。
type Engine struct {
	mu      sync.Mutex
	jobs    map[string]*Job
	order   []string
	backend *sqlite.Backend
	cfg     *config.Config
	client  *http.Client
	jobsDir string
	samp    *Sampler // WHEN 资源采样器
	cgrpMu  sync.Mutex
	cgrp    map[string]bool // 已取消的周期链
}

// NewEngine 创建引擎并启动调度循环。
func NewEngine(b *sqlite.Backend, cfg *config.Config) *Engine {
	e := &Engine{
		jobs:    make(map[string]*Job),
		backend: b,
		cfg:     cfg,
		client:  &http.Client{Timeout: 10 * time.Second},
		jobsDir: cfg.AsyncQuery.JobsDir,
		samp:    NewSampler(cfg.AsyncQuery.MetricsIntervalSec),
		cgrp:    make(map[string]bool),
	}
	if e.jobsDir == "" {
		e.jobsDir = filepath.Join(cfg.DBDir, "jobs")
	}
	_ = os.MkdirAll(e.jobsDir, 0o755)
	go e.loop()
	return e
}

// Submit 注册作业，返回 job_id。
func (e *Engine) Submit(j *Job) string {
	if j.TimeoutSec <= 0 {
		j.TimeoutSec = e.cfg.AsyncQuery.DefaultTimeoutSec
	}
	if j.TimeoutSec <= 0 {
		j.TimeoutSec = 1800
	}
	j.ID = fmt.Sprintf("j_%s_%04d", time.Now().Format("20060102_150405"), time.Now().UnixNano()%10000)
	j.State = StatePending
	j.SubmitTS = time.Now()
	j.CancelCh = make(chan struct{})
	if j.EverySec > 0 || j.EveryAt != "" {
		if j.RecurGroup == "" {
			j.RecurGroup = fmt.Sprintf("g_%s_%04d", time.Now().Format("20060102_150405"), time.Now().UnixNano()%10000)
		}
	}
	ttl := time.Duration(e.cfg.AsyncQuery.ResultRetainSec) * time.Second
	if j.RetainSec > 0 {
		ttl = time.Duration(j.RetainSec) * time.Second
	}
	if ttl <= 0 {
		ttl = 60 * time.Second // 默认 60s，到期自动删除
	}
	j.ExpiresTS = time.Now().Add(ttl)

	e.mu.Lock()
	e.jobs[j.ID] = j
	e.order = append(e.order, j.ID)
	e.mu.Unlock()
	go e.runWhenDue(j) // AT/立即/WHEN 判定与执行
	return j.ID
}

// loop 每秒检查：WINDOW 过期、结果 TTL 清理。
func (e *Engine) loop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		e.mu.Lock()
		for id, j := range e.jobs {
			if (j.State == StateDone || j.State == StateError) && now.After(j.ExpiresTS) {
				j.State = StateExpired
				_ = os.Remove(e.resultPath(id))
			}
		}
		e.mu.Unlock()
	}
}

// runWhenDue 判定执行时机：AT 到点 / WINDOW 内 WHEN 满足 / 窗口结束 EXPIRED。
func (e *Engine) runWhenDue(j *Job) {
	deadline := j.SubmitTS.Add(time.Duration(j.TimeoutSec) * time.Second)
	for {
		e.mu.Lock()
		if j.State != StatePending {
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()

		now := time.Now()
		if now.After(deadline) {
			e.finish(j, StateExpired, "timeout before execution (window/resource not satisfied)")
			return
		}
		select {
		case <-j.CancelCh:
			e.finish(j, StateCancelled, "cancelled")
			return
		case <-time.After(time.Second):
		}

		// AT：未到点等待
		if j.At != nil && now.Before(*j.At) {
			continue
		}
		// WINDOW：窗外等待；窗口尾刻过 → EXPIRED（不跨天）
		if j.Window[0] != "" {
			inWin, winEnd := inDailyWindow(now, j.Window[0], j.Window[1])
			if !inWin {
				if now.After(winEnd) {
					e.finish(j, StateExpired, "daily window closed before execution")
					return
				}
				continue
			}
			wd := winEnd
			if now.After(deadline) || wd.After(deadline) {
				// 窗口或超时先到：以先到者为准判定
				if wd.After(deadline) {
					wd = deadline
				}
				_ = wd
			}
		}
		// WHEN：资源门槛
		if !e.samp.Satisfied(j.WhenExpr) {
			continue
		}
		e.execute(j)
		return
	}
}

// execute 执行查询并流式落盘 + 回调。
func (e *Engine) execute(j *Job) {
	e.mu.Lock()
	if j.State != StatePending {
		e.mu.Unlock()
		return
	}
	j.State = StateRunning
	j.StartTS = time.Now()
	e.mu.Unlock()

	s, err := e.backend.OpenSession("async", "job:"+j.ID, e.cfg.DefaultDB)
	if err != nil {
		e.finish(j, StateError, "open session: "+err.Error())
		return
	}
	defer s.Close()

	cols, rows, err := s.Query(j.SQL)
	if err != nil {
		e.finish(j, StateError, err.Error())
		return
	}

	// 流式写 JSONL
	f, err := os.Create(e.resultPath(j.ID))
	if err != nil {
		e.finish(j, StateError, "create result file: "+err.Error())
		return
	}
	var buf bytes.Buffer
	hdr, _ := json.Marshal(map[string]any{"columns": cols})
	buf.Write(hdr)
	buf.WriteByte('\n')
	for _, r := range rows {
		select {
		case <-j.CancelCh:
			f.Close()
			_ = os.Remove(e.resultPath(j.ID))
			e.finish(j, StateCancelled, "cancelled during run")
			return
		default:
		}
		line, _ := json.Marshal(r)
		buf.Write(line)
		buf.WriteByte('\n')
		if buf.Len() >= 1<<20 {
			f.Write(buf.Bytes())
			buf.Reset()
		}
		j.Rows++
	}
	f.Write(buf.Bytes())
	st, _ := f.Stat()
	if st != nil {
		j.Bytes = st.Size()
	}
	f.Close()

	e.finish(j, StateDone, "")
}

// finish 收尾 + 回调（DONE/ERROR/EXPIRED/CANCELLED 全部回调）。
func (e *Engine) finish(j *Job, st State, errMsg string) {
	e.mu.Lock()
	j.State = st
	j.EndTS = time.Now()
	j.ElapsedMs = j.EndTS.Sub(j.StartTS).Milliseconds()
	j.Error = errMsg
	e.mu.Unlock()
	// 周期任务：未取消 → 自动排下一次（定时任务升级版）
	if j.RecurGroup != "" && j.State != StateCancelled {
		e.cgrpMu.Lock()
		cancelled := e.cgrp[j.RecurGroup]
		e.cgrpMu.Unlock()
		if !cancelled {
			next := *j
			next.ID = ""
			next.State = StatePending
			next.Rows, next.Bytes, next.Error = 0, 0, ""
			next.SubmitTS = time.Now()
			next.CancelCh = make(chan struct{})
			if j.EveryAt != "" {
				var h, m int
				fmt.Sscanf(j.EveryAt, "%d:%d", &h, &m)
				now := time.Now()
				t := time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, now.Location())
				if !t.After(now) {
					t = t.Add(24 * time.Hour)
				}
				next.At = &t
				next.Window = [2]string{}
			} else {
				t := time.Now().Add(time.Duration(j.EverySec) * time.Second)
				next.At = &t
			}
			e.SubmitClone(&next)
		}
	}
	if j.CallbackURL != "" {
		payload := map[string]any{
			"job_id":     j.ID,
			"state":      string(j.State),
			"rows":       j.Rows,
			"bytes":      j.Bytes,
			"elapsed_ms": j.ElapsedMs,
			"expires_ts": j.ExpiresTS.Format("2006-01-02 15:04:05"),
		}
		if j.Error != "" {
			payload["error"] = j.Error
		}
		body, _ := json.Marshal(payload)
		_ = body
		go e.deliver(j, payload)
	}
}

// callbackHTTP POST 回调（带可选签名，退避重试）。
func (e *Engine) callbackHTTP(j *Job, body []byte) {
	for attempt, backoff := range []time.Duration{0, time.Second, 5 * time.Second, 15 * time.Second} {
		time.Sleep(backoff)
		req, err := http.NewRequest(http.MethodPost, j.CallbackURL, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Glacier-Job", j.ID)
		if j.SignSecret != "" {
			mac := hmac.New(sha256.New, []byte(j.SignSecret))
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
		logx.Warnf("async callback %s attempt %d failed (job %s)", j.CallbackURL, attempt+1, j.ID)
	}
}

func (e *Engine) resultPath(id string) string { return filepath.Join(e.jobsDir, id+".jsonl") }

// ---------------------------------------------------------------------------
// 查询接口（handler 调用）
// ---------------------------------------------------------------------------

// List 返回全部作业快照（新→旧）。
func (e *Engine) List() []Job {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Job, 0, len(e.order))
	for i := len(e.order) - 1; i >= 0; i-- {
		if j, ok := e.jobs[e.order[i]]; ok {
			out = append(out, *j)
		}
	}
	return out
}

// Get 取单个作业。
func (e *Engine) Get(id string) (Job, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	j, ok := e.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

// Fetch 分页读取结果：返回列头 + 行。
func (e *Engine) Fetch(id string, limit, offset int) ([]string, [][]any, error) {
	j, ok := e.Get(id)
	if !ok {
		return nil, nil, fmt.Errorf("unknown job '%s'", id)
	}
	if j.State != StateDone {
		return nil, nil, fmt.Errorf("job '%s' state is %s, not DONE", id, j.State)
	}
	f, err := os.Open(e.resultPath(id))
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var cols []string
	lineNo := 0
	var out [][]any
	for {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			break
		}
		if lineNo == 0 {
			var h struct {
				Columns []string `json:"columns"`
			}
			if json.Unmarshal(raw, &h) == nil && h.Columns != nil {
				cols = h.Columns
				lineNo++
				continue
			}
		}
		idx := lineNo - 1
		lineNo++
		if idx < offset {
			continue
		}
		if limit > 0 && len(out) >= limit {
			break
		}
		var row []any
		if err := json.Unmarshal(raw, &row); err == nil {
			out = append(out, row)
		}
	}
	return cols, out, nil
}

// Cancel 取消作业（执行中触发中断标记）。
func (e *Engine) Cancel(id string) error {
	e.mu.Lock()
	j, ok := e.jobs[id]
	e.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown job '%s'", id)
	}
	if j.RecurGroup != "" {
		// 周期链：即使链头已 DONE，CANCEL 也表示停止整条链
		e.cgrpMu.Lock()
		e.cgrp[j.RecurGroup] = true
		e.cgrpMu.Unlock()
		if j.State == StatePending || j.State == StateRunning {
			close(j.CancelCh)
		}
		return nil
	}
	if j.State == StateDone || j.State == StateError || j.State == StateExpired || j.State == StateCancelled {
		return fmt.Errorf("job '%s' already finished (%s)", id, j.State)
	}
	close(j.CancelCh) // runWhenDue/execute 感知
	if j.RecurGroup != "" {
		e.cgrpMu.Lock()
		e.cgrp[j.RecurGroup] = true
		e.cgrpMu.Unlock()
	}
	return nil
}

// Drop 提前删除结果文件。
func (e *Engine) Drop(id string) error {
	e.mu.Lock()
	j, ok := e.jobs[id]
	e.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown job '%s'", id)
	}
	_ = os.Remove(e.resultPath(id))
	j.State = StateExpired
	return nil
}

// inDailyWindow 判断 now 是否位于每日窗口 [start,end)（支持跨午夜），返回窗口结束时刻。
func inDailyWindow(now time.Time, startHM, endHM string) (bool, time.Time) {
	sh, sm, ok1 := parseHM(startHM)
	eh, em, ok2 := parseHM(endHM)
	if !ok1 || !ok2 {
		return true, now // 解析失败视为无窗口限制
	}
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	st := day.Add(time.Duration(sh)*time.Hour + time.Duration(sm)*time.Minute)
	en := day.Add(time.Duration(eh)*time.Hour + time.Duration(em)*time.Minute)
	if !en.After(st) { // 跨午夜：22:00-06:00
		en = en.Add(24 * time.Hour)
		if now.Before(st) {
			st = st.Add(-24 * time.Hour)
			en = en.Add(-24 * time.Hour)
		}
	}
	return !now.Before(st) && now.Before(en), en
}

func parseHM(s string) (int, int, bool) {
	s = strings.TrimSpace(s)
	var h, m int
	if n, _ := fmt.Sscanf(s, "%d:%d", &h, &m); n == 2 && h >= 0 && h < 24 && m >= 0 && m < 60 {
		return h, m, true
	}
	return 0, 0, false
}

// SubmitClone 内部克隆提交（周期链重排，保留 RecurGroup）。
func (e *Engine) SubmitClone(j *Job) string {
	id := e.Submit(j)
	_ = id
	return id
}
