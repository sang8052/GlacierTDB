package s3

import (
	"io"
	"sync"
	"time"

	"glaciertdb/internal/config"
)

// tokenBucket 令牌桶限速（bytes/s，0 = 不限速直通）。
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	tokens float64
	last   time.Time
}

func newTokenBucket(bytesPerSec int64) *tokenBucket {
	r := float64(bytesPerSec)
	return &tokenBucket{rate: r, tokens: r, last: time.Now()}
}

// wait 阻塞至 n 字节额度可用（rate<=0 直通）。
// 语义：先按墙钟补充令牌，不足则睡到足额再扣——保证吞吐恰为 rate。
func (t *tokenBucket) wait(n int) {
	if t == nil || t.rate <= 0 {
		return
	}
	need := float64(n)
	for {
		t.mu.Lock()
		now := time.Now()
		t.tokens += now.Sub(t.last).Seconds() * t.rate
		if t.tokens > t.rate {
			t.tokens = t.rate // 突发上限 = 1 秒额度
		}
		t.last = now
		if t.tokens >= need {
			t.tokens -= need
			t.mu.Unlock()
			return
		}
		sleep := time.Duration((need - t.tokens) / t.rate * float64(time.Second))
		t.mu.Unlock()
		time.Sleep(sleep)
	}
}

// userState 单用户聚合限速状态（多连接共享总带宽 + 并发计数）。
type userState struct {
	tb    *tokenBucket
	conns int
}

// limiter 限速管理：单连接限速 + 用户级总带宽合并限速 + 用户并发上限。
type limiter struct {
	mu      sync.Mutex
	users   map[string]*userState
	getLim  func(bucket string) config.S3Limit
}

func newLimiter(getLim func(bucket string) config.S3Limit) *limiter {
	return &limiter{users: map[string]*userState{}, getLim: getLim}
}

// acquire 占用一个用户并发槽；返回释放函数（nil, err = 超限 503 SlowDown）。
func (l *limiter) acquire(owner, bucket string) (release func(), userTB *tokenBucket, err error) {
	lim := l.getLim(bucket)
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.users[owner]
	if st == nil {
		st = &userState{tb: newTokenBucket(lim.PerUserBps)}
		l.users[owner] = st
	}
	if lim.PerUserConns > 0 && st.conns >= lim.PerUserConns {
		return nil, nil, &s3Error{Code: "SlowDown", Status: 503,
			Msg: "Too many concurrent connections for this user"}
	}
	st.conns++
	// 用户总带宽桶速率按最新配置刷新（配置非 0 时）
	if lim.PerUserBps > 0 {
		st.tb.mu.Lock()
		st.tb.rate = float64(lim.PerUserBps)
		st.tb.mu.Unlock()
	}
	release = func() {
		l.mu.Lock()
		if s := l.users[owner]; s != nil {
			s.conns--
		}
		l.mu.Unlock()
	}
	return release, st.tb, nil
}

// throttledWriter 限速写出：按块等待单连接桶 + 用户总带宽桶。
type throttledWriter struct {
	w      io.Writer
	connTB *tokenBucket
	userTB *tokenBucket
	written int64
}

const throttleBlock = 32 << 10 // 32KB 粒度

func (t *throttledWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > throttleBlock {
			n = throttleBlock
		}
		t.connTB.wait(n)
		if t.userTB != nil {
			t.userTB.wait(n)
		}
		m, err := t.w.Write(p[:n])
		total += m
		t.written += int64(m)
		if err != nil {
			return total, err
		}
		p = p[n:]
	}
	return total, nil
}

// closeNotifyWriter 传输计数回调（下载字节记账）。
type countingWriter struct {
	w      io.Writer
	count  int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.count += int64(n)
	return n, err
}

// limitWriter 组合：限速 → 计数 → 原始 ResponseWriter（flush 由 io.Copy 语义自然驱动）。
func newLimitedWriter(w io.Writer, connBps int64, userTB *tokenBucket) (*throttledWriter, *countingWriter) {
	tw := &throttledWriter{w: w, connTB: newTokenBucket(connBps), userTB: userTB}
	cw := &countingWriter{w: tw}
	return tw, cw
}
