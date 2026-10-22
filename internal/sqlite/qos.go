// Package sqlite — 查询 QoS 限流（v1.0.10）。
// 全系统 + 单库两级配额：qps（令牌桶）/ concurrency（并发闸门）/ rows_per_sec（行吞吐节流）；
// 读 / 写 / 异步任务执行统一记账（都走 Session.Query / Exec）。
// on_exceed: wait = 排队等待（默认）| reject = 立即拒绝（QoSRejectError）。
package sqlite

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"glaciertdb/internal/config"
)

// QoSRejectError QoS 超限拒绝错误（handler 映射为 MySQL 错误）。
type QoSRejectError struct {
	Msg string
}

func (e *QoSRejectError) Error() string { return e.Msg }

// IsQoSReject 判断错误是否 QoS 拒绝。
func IsQoSReject(err error) bool {
	var q *QoSRejectError
	return errors.As(err, &q)
}

// tokenBucket 简易令牌桶（rate = 每秒令牌数，<=0 不限）。
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newBucket(rate float64) *tokenBucket {
	burst := rate
	if burst < 1 {
		burst = 1
	}
	return &tokenBucket{rate: rate, burst: burst, tokens: burst, last: time.Now()}
}

// take 取一个令牌；needWait 返回需等待时长（0 = 已取得）。
func (b *tokenBucket) take() (needWait time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rate <= 0 {
		return 0
	}
	now := time.Now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	b.last = now
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	if b.tokens >= 1 {
		b.tokens--
		return 0
	}
	return time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
}

func (b *tokenBucket) setRate(rate float64) {
	b.mu.Lock()
	b.rate = rate
	if rate < 1 {
		b.burst = 1
	} else {
		b.burst = rate
	}
	b.tokens = b.burst
	b.last = time.Now()
	b.mu.Unlock()
}

// concGate 并发闸门（支持阻塞等待与立即拒绝）。
type concGate struct {
	mu   sync.Mutex
	cond *sync.Cond
	cur  int
	max  int // <=0 不限
}

func newGate(max int) *concGate {
	g := &concGate{max: max}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *concGate) acquire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.max > 0 && g.cur >= g.max {
		g.cond.Wait()
	}
	g.cur++
}

func (g *concGate) tryAcquire() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.max > 0 && g.cur >= g.max {
		return false
	}
	g.cur++
	return true
}

func (g *concGate) release() {
	g.mu.Lock()
	g.cur--
	g.cond.Signal()
	g.mu.Unlock()
}

func (g *concGate) setMax(n int) {
	g.mu.Lock()
	g.max = n
	g.cond.Broadcast()
	g.mu.Unlock()
}

func (g *concGate) current() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cur
}

// qosScope 一级配额（全局或单库）的执行体与统计。
type qosScope struct {
	name string
	mu   sync.Mutex
	lim  config.QosLimit

	bucket *tokenBucket
	gate   *concGate

	// 统计
	passed, queued, rejected int64
	waitTotalMs              int64
	waitCount                int64

	// 行吞吐窗口
	rowsWindowStart time.Time
	rowsInWindow    int64
}

func newScope(name string, lim config.QosLimit) *qosScope {
	s := &qosScope{
		name:            name,
		lim:             lim,
		bucket:          newBucket(float64(lim.Qps)),
		gate:            newGate(lim.Concurrency),
		rowsWindowStart: time.Now(),
	}
	return s
}

// qosManager 全系统 + 单库 QoS。
type qosManager struct {
	mu   sync.RWMutex
	cfg  config.QosOpts
	gl   *qosScope
	dbs  map[string]*qosScope
}

func newQoSManager(cfg config.QosOpts) *qosManager {
	qm := &qosManager{
		cfg: cfg,
		gl:  newScope("GLOBAL", cfg.Global),
		dbs: make(map[string]*qosScope),
	}
	for db, lim := range cfg.Databases {
		qm.dbs[db] = newScope(db, lim)
	}
	return qm
}

func (q *qosManager) scopeFor(db string) *qosScope {
	q.mu.RLock()
	s, ok := q.dbs[db]
	q.mu.RUnlock()
	if ok {
		return s
	}
	return nil
}

// acquire 获取执行配额（全局 + 单库两级），返回等待毫秒数与 release。
func (q *qosManager) acquire(db string) (waitedMs int64, release func(), err error) {
	start := time.Now()
	if e := q.acquireScope(q.gl); e != nil {
		return 0, nil, e
	}
	if s := q.scopeFor(db); s != nil {
		if e := q.acquireScope(s); e != nil {
			q.gl.gate.release()
			return 0, nil, e
		}
	}
	waited := time.Since(start).Milliseconds()
	return waited, func() {
		if s := q.scopeFor(db); s != nil {
			s.gate.release()
		}
		q.gl.gate.release()
	}, nil
}

func (q *qosManager) acquireScope(s *qosScope) error {
	reject := q.cfg.OnExceed == "reject"
	waited := false
	// 1) 令牌桶（QPS）
	for {
		need := s.bucket.take()
		if need <= 0 {
			break
		}
		if reject {
			s.mu.Lock()
			s.rejected++
			s.mu.Unlock()
			return &QoSRejectError{Msg: fmt.Sprintf("query rate limit exceeded (QoS scope %s, on_exceed=reject)", s.name)}
		}
		waited = true
		time.Sleep(need)
	}
	// 2) 并发闸门
	if reject {
		if !s.gate.tryAcquire() {
			s.mu.Lock()
			s.rejected++
			s.mu.Unlock()
			return &QoSRejectError{Msg: fmt.Sprintf("query concurrency limit exceeded (QoS scope %s, on_exceed=reject)", s.name)}
		}
	} else {
		s.gate.acquire()
	}
	s.mu.Lock()
	s.passed++
	if waited {
		s.queued++
	}
	s.mu.Unlock()
	return nil
}

// paceRows 行吞吐节流：窗口内累计行数超过 rows_per_sec 时睡到窗口结束。
func (q *qosManager) paceRows(db string, n int) {
	scopes := []*qosScope{q.gl}
	if s := q.scopeFor(db); s != nil {
		scopes = append(scopes, s)
	}
	for _, s := range scopes {
		if s.lim.RowsPerSec <= 0 {
			continue
		}
		s.mu.Lock()
		now := time.Now()
		if now.Sub(s.rowsWindowStart) >= time.Second {
			s.rowsWindowStart = now
			s.rowsInWindow = 0
		}
		s.rowsInWindow += int64(n)
		over := s.rowsInWindow - s.lim.RowsPerSec
		if over > 0 {
			sleep := time.Duration(float64(over) / float64(s.lim.RowsPerSec) * float64(time.Second))
			s.rowsInWindow = 0
			s.rowsWindowStart = now.Add(sleep)
			s.mu.Unlock()
			time.Sleep(sleep)
		} else {
			s.mu.Unlock()
		}
	}
}

// SetGlobal 热改全局配额（n<0 不动该项）。
func (q *qosManager) SetGlobal(lim config.QosLimit) {
	q.mu.Lock()
	q.cfg.Global = lim
	q.mu.Unlock()
	q.gl.apply(lim)
}

// SetDB 热改/新建单库配额。
func (q *qosManager) SetDB(db string, lim config.QosLimit) {
	q.mu.Lock()
	q.cfg.Databases[db] = lim
	q.mu.Unlock()
	if s := q.scopeFor(db); s != nil {
		s.apply(lim)
		return
	}
	q.mu.Lock()
	if _, ok := q.dbs[db]; !ok {
		q.dbs[db] = newScope(db, lim)
	}
	q.mu.Unlock()
}

// ClearDB 删除单库配额覆盖（回到仅全局）。
func (q *qosManager) ClearDB(db string) {
	q.mu.Lock()
	delete(q.cfg.Databases, db)
	delete(q.dbs, db)
	q.mu.Unlock()
}

func (s *qosScope) apply(lim config.QosLimit) {
	s.mu.Lock()
	s.lim = lim
	s.mu.Unlock()
	s.bucket.setRate(float64(lim.Qps))
	s.gate.setMax(lim.Concurrency)
}

// QoSRow SHOW QOS / qos_status 输出行。
type QoSRow struct {
	Scope      string
	Lim        config.QosLimit
	Passed     int64
	Queued     int64
	Rejected   int64
	Waiting    int
	AvgWaitMs  int64
}

// Info 汇总 QoS 配置与实时统计。
func (q *qosManager) Info() []QoSRow {
	q.mu.RLock()
	scopes := []*qosScope{q.gl}
	names := make([]string, 0, len(q.dbs))
	for db := range q.dbs {
		names = append(names, db)
	}
	q.mu.RUnlock()
	for _, db := range names {
		scopes = append(scopes, q.scopeFor(db))
	}
	var out []QoSRow
	for _, s := range scopes {
		if s == nil {
			continue
		}
		s.mu.Lock()
		avg := int64(0)
		if s.waitCount > 0 {
			avg = s.waitTotalMs / s.waitCount
		}
		row := QoSRow{
			Scope: s.name, Lim: s.lim,
			Passed: s.passed, Queued: s.queued, Rejected: s.rejected,
			Waiting: s.gate.current(), AvgWaitMs: avg,
		}
		s.mu.Unlock()
		out = append(out, row)
	}
	return out
}
