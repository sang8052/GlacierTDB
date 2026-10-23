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
	"glaciertdb/internal/meta"
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
	WindowWd    int        `json:"window_weekday,omitempty"` // WINDOW 周几限定（0=每日，1..7=周一..周日）
	TimeoutSec  int        `json:"timeout_sec"`
	SubmitTS    time.Time  `json:"submit_ts"`
	StartTS     time.Time  `json:"-"`
	EndTS       time.Time  `json:"-"`
	ElapsedMs   int64      `json:"elapsed_ms"`
	Rows        int64      `json:"rows"`
	Bytes       int64      `json:"bytes"`
	Error       string     `json:"error,omitempty"`
	ExpiresTS   time.Time  `json:"expires_ts"`
	PartRows    int64      `json:"partition_rows,omitempty"` // 分片回调大小（行数）
	PartBytes   int64      `json:"partition_bytes,omitempty"` // 分片回调大小（行序列化字节数）
	RetainSec   int        `json:"retain_sec,omitempty"`     // 结果保留秒数（覆盖全局默认 60s）
	Script      string     `json:"script,omitempty"`
	ScriptOut   string     `json:"script_output,omitempty"`
	WhenExpr    string     `json:"when,omitempty"` // WHEN 资源门槛表达式
	EverySec    int        `json:"every_sec,omitempty"`    // 周期秒（0=一次性）
	EveryAt     string     `json:"every_at,omitempty"`     // EVERY DAY/WEEK AT 'HH:MM' 的时刻
	EveryWd     int        `json:"every_weekday,omitempty"` // 每周调度周几（0=非每周，1..7=周一..周日，v1.0.10）
	EveryWeeks  int        `json:"every_weeks,omitempty"`   // 每 N 周（v1.0.10，默认 1）
	EveryMonths   int      `json:"every_months,omitempty"`   // 每 N 月（0=非每月，v1.3.1）
	EveryMonthDay int      `json:"every_month_day,omitempty"` // 每月几号 1-31（月尾 clamp，v1.3.1）
	EveryYears    int      `json:"every_years,omitempty"`    // 每 N 年（0=非每年，v1.3.1）
	EveryYearDay  string   `json:"every_year_day,omitempty"` // 每年 'MM-DD'（v1.3.1）
	QosWaitMs   int64      `json:"qos_wait_ms,omitempty"`   // 本次执行的 QoS 排队毫秒（v1.0.10）
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
	pauseMu sync.RWMutex
	paused  map[string]bool // 已暂停的周期链/任务（PAUSE QUERY，v1.0.10）
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
		paused:  make(map[string]bool),
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
	if j.EverySec > 0 || j.EveryAt != "" || j.EveryWd > 0 || j.EveryMonths > 0 || j.EveryYears > 0 {
		if j.RecurGroup == "" {
			j.RecurGroup = fmt.Sprintf("g_%s_%04d", time.Now().Format("20060102_150405"), time.Now().UnixNano()%10000)
		}
	}
	// 每周调度：首次执行排到下一个周几时刻（不立即执行）
	if j.EveryWd > 0 && j.At == nil {
		t := nextWeekOccurrence(time.Now(), j.EveryWd, j.EveryAt)
		j.At = &t
	}
	// 每月 / 每年调度：首次执行排到下一个日历时刻（月尾日 clamp，v1.3.1）
	if j.EveryMonths > 0 && j.At == nil {
		if j.EveryMonthDay <= 0 {
			j.EveryMonthDay = time.Now().Day()
		}
		t := nextMonthOccurrence(time.Now(), j.EveryMonths, j.EveryMonthDay, j.EveryAt)
		j.At = &t
	}
	if j.EveryYears > 0 && j.At == nil {
		if j.EveryYearDay == "" {
			j.EveryYearDay = time.Now().Format("01-02")
		}
		t := nextYearOccurrence(time.Now(), j.EveryYears, j.EveryYearDay, j.EveryAt)
		j.At = &t
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
		// PAUSE：挂起排期（冻结超时倒计时，不丢链）
		if e.isPaused(j) {
			deadline = deadline.Add(time.Second)
			select {
			case <-j.CancelCh:
				e.finish(j, StateCancelled, "cancelled")
				return
			case <-time.After(time.Second):
			}
			continue
		}
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
		// WINDOW：窗外等待；窗口尾刻过 → EXPIRED（不跨天）；带周几限定时非当日等待
		if j.Window[0] != "" {
			inWin, winEnd := inWindow(now, j.WindowWd, j.Window[0], j.Window[1])
			if !inWin {
				if now.After(winEnd) {
					e.finish(j, StateExpired, "daily window closed before execution")
					return
				}
				continue
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

	cols, rows, err := s.Query(meta.UnescapeLiterals(j.SQL))
	if err != nil {
		e.finish(j, StateError, err.Error())
		return
	}
	j.QosWaitMs = s.QoSWaitMs() // QoS 排队时长（query_jobs.qos_wait_ms）

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

	// SCRIPT 后处理管道（v1.1.1）：结果 JSONL → 脚本 stdin → 产物落 jobs/<id>.<output>
	if j.Script != "" {
		e.mu.Lock()
		j.State = StateScript
		e.mu.Unlock()
		if err := e.runScript(j); err != nil {
			e.finish(j, StateError, err.Error())
			return
		}
	}

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
			next.QosWaitMs = 0
			next.SubmitTS = time.Now()
			next.CancelCh = make(chan struct{})
			if j.EveryMonths > 0 {
				// 每月（每 N 月）调度：下一个日历月几时刻（月尾日 clamp，v1.3.1）
				t := nextMonthOccurrence(time.Now(), j.EveryMonths, j.EveryMonthDay, j.EveryAt)
				next.At = &t
				next.Window = [2]string{}
				next.WindowWd = 0
			} else if j.EveryYears > 0 {
				// 每年（每 N 年）调度：下一个 MM-DD 时刻（2/29 平年 clamp 到 2/28，v1.3.1）
				t := nextYearOccurrence(time.Now(), j.EveryYears, j.EveryYearDay, j.EveryAt)
				next.At = &t
				next.Window = [2]string{}
				next.WindowWd = 0
			} else if j.EveryWd > 0 {
				// 每周（每 N 周）调度：下一周几时刻（N>1 再推 (N-1) 周）
				t := nextWeekOccurrence(time.Now(), j.EveryWd, j.EveryAt)
				if j.EveryWeeks > 1 {
					t = t.Add(time.Duration(j.EveryWeeks-1) * 7 * 24 * time.Hour)
				}
				next.At = &t
				next.Window = [2]string{}
				next.WindowWd = 0
			} else if j.EveryAt != "" {
				var h, m int
				fmt.Sscanf(j.EveryAt, "%d:%d", &h, &m)
				now := time.Now()
				t := time.Date(now.Year(), now.Month(), now.Day(), h, m, 0, 0, now.Location())
				if !t.After(now) {
					t = t.Add(24 * time.Hour)
				}
				next.At = &t
				next.Window = [2]string{}
				next.WindowWd = 0
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
		if j.PartRows > 0 {
			payload["parts"] = (j.Rows + j.PartRows - 1) / j.PartRows
			payload["partition_rows"] = j.PartRows
		}
		if j.PartBytes > 0 {
			payload["partition_bytes"] = j.PartBytes
		}
		body, _ := json.Marshal(payload)
		_ = body
		go e.deliver(j, payload)
		// PARTITION 分片回调：每片独立 POST（v1.0.8；仅 HTTP 通道）
		if (j.PartRows > 0 || j.PartBytes > 0) && j.State == StateDone &&
			j.CallbackProto != "tcp" && j.CallbackProto != "amqp" && j.CallbackProto != "kafka" {
			go e.deliverParts(j)
		}
	}
}

// deliverParts 分片回调：把结果按 PartRows（行数）或 PartBytes（行序列化字节数）
// 切片逐片 POST；单片负载超过 callback_max_inline_kb 时省略 data、附拉取凭证。
func (e *Engine) deliverParts(j *Job) {
	cols, rows, err := e.Fetch(j.ID, 0, 0)
	if err != nil || len(rows) == 0 {
		return
	}
	type chunk struct{ off, end int }
	var chunks []chunk
	if j.PartRows > 0 {
		for off := 0; off < len(rows); off += int(j.PartRows) {
			end := off + int(j.PartRows)
			if end > len(rows) {
				end = len(rows)
			}
			chunks = append(chunks, chunk{off, end})
		}
	} else if j.PartBytes > 0 {
		cur, start := 0, 0
		for k, r := range rows {
			b, _ := json.Marshal(r)
			cur += len(b)
			if int64(cur) >= j.PartBytes {
				chunks = append(chunks, chunk{start, k + 1})
				start, cur = k+1, 0
			}
		}
		if start < len(rows) {
			chunks = append(chunks, chunk{start, len(rows)})
		}
	}
	total := int64(len(chunks))
	limitKB := int64(e.cfg.AsyncQuery.CallbackMaxInlineKB)
	for pi, c := range chunks {
		payload := map[string]any{
			"job_id":        j.ID,
			"state":         string(j.State),
			"part":          int64(pi + 1),
			"parts":         total,
			"rows":          int64(c.end - c.off),
			"columns":       cols,
			"data_encoding": "json",
		}
		body, merr := json.Marshal(payload)
		if merr != nil {
			return
		}
		if limitKB <= 0 || int64(len(body)) < limitKB*1024 {
			withData := make(map[string]any, len(payload)+1)
			for k, v := range payload {
				withData[k] = v
			}
			withData["data"] = rows[c.off:c.end]
			body, _ = json.Marshal(withData)
		} else {
			payload["fetch"] = fmt.Sprintf("FETCH QUERY RESULT '%s' LIMIT %d OFFSET %d", j.ID, c.end-c.off, c.off)
			body, _ = json.Marshal(payload)
		}
		e.callbackHTTP(j, body) // 复用签名头 + 退避重试
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

// Vars 返回 WHEN 监测变量当前快照（SHOW QUERY VARS，v1.0.9）。
func (e *Engine) Vars() []VarInfo { return e.samp.Snapshot() }

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

// inWindow 判断 now 是否位于窗口 [start,end)（支持跨午夜），返回窗口结束时刻。
// wd：0=每日窗口；1..7=周一..周日限定（非当日不满足，返回 24h 后作伪窗口尾，由 TIMEOUT 兜底）。
func inWindow(now time.Time, wd int, startHM, endHM string) (bool, time.Time) {
	if wd != 0 {
		// Go: Sunday=0..Saturday=6；约定 wd: 1=周一..7=周日
		if int(now.Weekday()) != wd%7 {
			return false, now.Add(24 * time.Hour)
		}
	}
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

// ---------------------------------------------------------------------------
// 周期调度增强（v1.0.10）：PAUSE/RESUME / 周几调度 / 周期配置查询
// ---------------------------------------------------------------------------

// pauseKey 暂停键：周期链优先，单任务兜底。
func pauseKey(j *Job) string {
	if j.RecurGroup != "" {
		return j.RecurGroup
	}
	return j.ID
}

// Pause 暂停周期链/任务排期（不丢链；TIMEOUT 倒计时冻结）。
func (e *Engine) Pause(id string) error {
	j, ok := e.Get(id)
	if !ok {
		return fmt.Errorf("unknown job '%s'", id)
	}
	e.pauseMu.Lock()
	e.paused[pauseKey(&j)] = true
	e.pauseMu.Unlock()
	return nil
}

// Resume 恢复周期链/任务排期。
func (e *Engine) Resume(id string) error {
	j, ok := e.Get(id)
	if !ok {
		return fmt.Errorf("unknown job '%s'", id)
	}
	e.pauseMu.Lock()
	delete(e.paused, pauseKey(&j))
	e.pauseMu.Unlock()
	return nil
}

func (e *Engine) isPaused(j *Job) bool {
	e.pauseMu.RLock()
	defer e.pauseMu.RUnlock()
	return e.paused[pauseKey(j)]
}

// nextWeekOccurrence 计算下一个「周几 + 时刻」（wd: 1..7=周一..周日；at 为空用 now 的时刻）。
func nextWeekOccurrence(now time.Time, wd int, at string) time.Time {
	goWd := wd % 7 // 周一=1..周日=7 → Go Weekday: Mon=1..Sun=0
	h, m := now.Hour(), now.Minute()
	if hh, mm, ok := parseHM(at); ok {
		h, m = hh, mm
	}
	for i := 0; i < 8; i++ {
		day := now.AddDate(0, 0, i)
		if int(day.Weekday()) != goWd {
			continue
		}
		t := time.Date(day.Year(), day.Month(), day.Day(), h, m, 0, 0, now.Location())
		if t.After(now) {
			return t
		}
	}
	return now.Add(7 * 24 * time.Hour) // 兜底
}

// clampDay 月尾日 clamp：day 超过当月天数取当月最后一天（1/31 → 2/28）。
func clampDay(year int, month time.Month, day int) int {
	if day < 1 {
		day = 1
	}
	last := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if day > last {
		return last
	}
	return day
}

// nextMonthOccurrence 计算下一个「每月 day + 时刻」（每 months 月一跳；月尾日 clamp）。
// day<=0 取 after 当日；at 为空用 after 的时刻。
func nextMonthOccurrence(after time.Time, months, day int, at string) time.Time {
	if months <= 0 {
		months = 1
	}
	if day <= 0 {
		day = after.Day()
	}
	h, m := after.Hour(), after.Minute()
	if hh, mm, ok := parseHM(at); ok {
		h, m = hh, mm
	}
	for k := 0; k <= 12*months+12; k += months {
		base := time.Date(after.Year(), after.Month(), 1, 0, 0, 0, 0, after.Location()).AddDate(0, k, 0)
		t := time.Date(base.Year(), base.Month(), clampDay(base.Year(), base.Month(), day), h, m, 0, 0, after.Location())
		if t.After(after) {
			return t
		}
	}
	return after.AddDate(0, months, 0) // 兜底
}

// nextYearOccurrence 计算下一个「每年 MM-DD + 时刻」（每 years 年一跳；2/29 平年 clamp 到 2/28）。
// mmdd 解析失败取 after 当日；at 为空用 after 的时刻。
func nextYearOccurrence(after time.Time, years int, mmdd, at string) time.Time {
	if years <= 0 {
		years = 1
	}
	month, day := int(after.Month()), after.Day()
	if mm, dd, err := parseMMDD(mmdd); err == nil {
		month, day = mm, dd
	}
	h, m := after.Hour(), after.Minute()
	if hh, mm, ok := parseHM(at); ok {
		h, m = hh, mm
	}
	for k := 0; k <= years+2; k += years {
		y := after.Year() + k
		t := time.Date(y, time.Month(month), clampDay(y, time.Month(month), day), h, m, 0, 0, after.Location())
		if t.After(after) {
			return t
		}
	}
	return after.AddDate(years, 0, 0) // 兜底
}

// parseMMDD 解析 'MM-DD'（月 1-12、日 1-31）。
func parseMMDD(s string) (int, int, error) {
	var mm, dd int
	if n, _ := fmt.Sscanf(strings.TrimSpace(s), "%d-%d", &mm, &dd); n != 2 || mm < 1 || mm > 12 || dd < 1 || dd > 31 {
		return 0, 0, fmt.Errorf("invalid MM-DD %q", s)
	}
	return mm, dd, nil
}

// ScheduleRow 周期配置行（SHOW QUERY SCHEDULE / query_schedule）。
type ScheduleRow struct {
	RecurGroup string
	JobID      string
	SQL        string
	Schedule   string
	Window     string
	When       string
	NextRun    string
	LastRun    string
	LastState  string
	Paused     bool
}

// Schedule 返回全部周期链配置（每链一行，取最新任务为链头）。
func (e *Engine) Schedule() []ScheduleRow {
	jobs := e.List() // 新→旧
	seen := map[string]bool{}
	var out []ScheduleRow
	for i := range jobs {
		j := jobs[i]
		if j.RecurGroup == "" || seen[j.RecurGroup] {
			continue
		}
		seen[j.RecurGroup] = true
		row := ScheduleRow{
			RecurGroup: j.RecurGroup,
			JobID:      j.ID,
			SQL:        j.SQL,
			Schedule:   scheduleExpr(&j),
			When:       j.WhenExpr,
			Paused:     e.isPaused(&j),
		}
		if j.Window[0] != "" {
			row.Window = j.Window[0] + "-" + j.Window[1]
			if j.WindowWd > 0 {
				row.Window = wdCN(j.WindowWd) + " " + row.Window
			}
		}
		if j.At != nil {
			row.NextRun = j.At.Format("2006-01-02 15:04:05")
		}
		for k := i + 1; k < len(jobs); k++ {
			if jobs[k].RecurGroup == j.RecurGroup {
				if !jobs[k].EndTS.IsZero() {
					row.LastRun = jobs[k].EndTS.Format("2006-01-02 15:04:05")
				} else {
					row.LastRun = jobs[k].SubmitTS.Format("2006-01-02 15:04:05")
				}
				row.LastState = string(jobs[k].State)
				break
			}
		}
		out = append(out, row)
	}
	return out
}

// scheduleExpr 生成人类可读周期表达。
func scheduleExpr(j *Job) string {
	switch {
	case j.EveryMonths > 0:
		s := "EVERY "
		if j.EveryMonths > 1 {
			s += fmt.Sprintf("%d MONTHS", j.EveryMonths)
		} else {
			s += "MONTH"
		}
		if j.EveryMonthDay > 0 {
			s += fmt.Sprintf(" ON %d", j.EveryMonthDay)
		}
		if j.EveryAt != "" {
			s += " AT " + j.EveryAt
		}
		return s
	case j.EveryYears > 0:
		s := "EVERY "
		if j.EveryYears > 1 {
			s += fmt.Sprintf("%d YEARS", j.EveryYears)
		} else {
			s += "YEAR"
		}
		if j.EveryYearDay != "" {
			s += " ON " + j.EveryYearDay
		}
		if j.EveryAt != "" {
			s += " AT " + j.EveryAt
		}
		return s
	case j.EveryWd > 0:
		s := "EVERY "
		if j.EveryWeeks > 1 {
			s += fmt.Sprintf("%d WEEKS", j.EveryWeeks)
		} else {
			s += "WEEK"
		}
		s += " ON " + wdCN(j.EveryWd)
		if j.EveryAt != "" {
			s += " AT " + j.EveryAt
		}
		return s
	case j.EveryAt != "":
		return "EVERY DAY AT " + j.EveryAt
	case j.EverySec > 0:
		switch {
		case j.EverySec%86400 == 0:
			return fmt.Sprintf("EVERY %d DAYS", j.EverySec/86400)
		case j.EverySec%3600 == 0:
			return fmt.Sprintf("EVERY %d HOURS", j.EverySec/3600)
		case j.EverySec%60 == 0:
			return fmt.Sprintf("EVERY %d MINUTES", j.EverySec/60)
		default:
			return fmt.Sprintf("EVERY %d SECONDS", j.EverySec)
		}
	}
	return ""
}

// wdCN 1..7 → 周一..周日。
func wdCN(wd int) string {
	names := []string{"", "周一", "周二", "周三", "周四", "周五", "周六", "周日"}
	if wd >= 1 && wd <= 7 {
		return names[wd]
	}
	return fmt.Sprintf("周%d", wd)
}

// ScheduleExpr 人类可读周期表达（query_jobs.schedule 列）。
func (j *Job) ScheduleExpr() string { return scheduleExpr(j) }
