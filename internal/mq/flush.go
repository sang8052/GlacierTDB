package mq

import (
	"strings"
	"sync/atomic"
	"time"

	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// 写后落盘（v1.4.0）：async_flush_ms > 0 时启用
//
// durable 消息发布时内存构建（含 body/props/persistent/入队时间），推入目标队列的
// 内存 ready 链 + journal（待落盘），并 nudge flusher；flusher 被唤醒后把 journal
// 全量单事务 INSERT mq_messages，成功后统一回调 publisher confirm（等价原实现
// "落库成功后才 confirm" 的时序——confirm 帧改由 flusher goroutine 经连接 wmu 写出，
// 线程安全）。transient（delivery_mode=1）消息纯内存：不进 journal 不落库，重启即失
// （与 RabbitMQ 非持久化语义一致，docs/消息队列.md 有注明）。
//
// 旁路批处理：派发产生的 durable unacked 状态转移（unackPend）与确认/丢弃产生的
// 删除（delPend）随每轮 flush 各以一条批量语句落库；行尚未落库时 UPDATE/DELETE
// 空作用——在途 journal 行由 state 快照 + 提交前原子复查消化（见 flushBatch），
// 提交窗口之后的删除必有对应 delPend 兜底，不产生复活行。
//
// async_flush_ms == 0：本文件的异步路径整体旁路（publish/派发/确认走 store.go
// 旧同步路径），仅统计内存聚合与 statLoop 照常工作（两种模式共用）。
// ---------------------------------------------------------------------------

const (
	mqJournalBacklog   = 4096                   // journal 背压阈值：超过则发布方内联同步刷一轮（ Publish 不持锁等待）
	mqStatFlush        = 5 * time.Second        // 统计内存聚合批量落库周期
	mqDispatchBatch    = 256                    // SQL 模式单消费者单轮派发上限
	mqDispatchIdleTick = 250 * time.Millisecond // 派发空闲退避 tick（原 50ms 全扫；kick 即时驱动）
	mqIDChunk          = 500                    // IN 子句参数分片（SQLite 变量数上限内）
)

// 消息内存态（Message.state；原子访问——flusher 在事务内免锁复查并发删除）。
const (
	msgStateReady = iota
	msgStateUnacked
	msgStateGone
)

// statDelta 单队列统计增量（仿 s3 usageBuf，v1.4.0）。
type statDelta struct {
	pub, del, ack int64
}

// msgList 内存 ready 链：append 入队尾（= id 递增 FIFO）、pop 自队首、
// requeue 回插队首（等价旧实现 ORDER BY id 的"旧消息优先"语义）。
type msgList struct {
	xs   []*Message
	head int
}

func (l *msgList) pop() *Message {
	if l.head >= len(l.xs) {
		return nil
	}
	m := l.xs[l.head]
	l.xs[l.head] = nil
	l.head++
	if l.head >= len(l.xs) {
		l.xs = l.xs[:0]
		l.head = 0
	}
	return m
}

func (l *msgList) pushFront(m *Message) {
	if l.head > 0 {
		l.head--
		l.xs[l.head] = m
		return
	}
	l.xs = append(l.xs, nil)
	copy(l.xs[1:], l.xs)
	l.xs[0] = m
}

// jrow journal 落库行快照：换出时在 s.mu 下复制标量（含 state），flush 事务期间
// 与派发/确认的并发修改不再构成数据竞争。
type jrow struct {
	m       *Message
	id      int64
	ns      string
	queue   string
	ex      string
	rk      string
	props   []byte
	payload []byte
	pers    int
	redel   int
	state   string
	created int64
}

// async 是否启用写后落盘（async_flush_ms > 0；0 = 旧同步路径，v1.4.0）。
func (s *Store) async() bool { return s.flushIv > 0 }

// nudgeFlush 唤醒 flusher。
func (s *Store) nudgeFlush() {
	select {
	case s.flushKick <- struct{}{}:
	default:
	}
}

// flushLoop 落盘循环（v1.4.0）：kick 或周期触发，journal 与旁路批量单事务落库。
func (s *Store) flushLoop() {
	t := time.NewTicker(s.flushIv)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-s.flushKick:
		}
		if err := s.flushOnce(); err != nil {
			logx.Debugf("mq: async flush error: %v", err)
		}
	}
}

// flushOnce 换出 journal / confirm 回调 / 旁路列表 → 单事务落库 → confirm 回调（锁外）。
// 可从 flushLoop 与发布背压路径并发调用（换出在 s.mu 下原子完成，天然幂等）。
func (s *Store) flushOnce() error {
	s.mu.Lock()
	if len(s.journal) == 0 && len(s.pendConfirms) == 0 && len(s.unackPend) == 0 && len(s.delPend) == 0 {
		s.mu.Unlock()
		return nil
	}
	j := s.journal
	pc := s.pendConfirms
	up := s.unackPend
	dp := s.delPend
	s.journal, s.pendConfirms, s.unackPend, s.delPend = nil, nil, nil, nil
	// 换出时复制落库快照（标量在 s.mu 下读取，事务期不再竞争）
	rows := make([]jrow, 0, len(j))
	for _, m := range j {
		st := "ready"
		if atomic.LoadUint32(&m.state) == msgStateUnacked {
			st = "unacked"
		}
		rows = append(rows, jrow{m: m, id: m.ID, ns: m.NS, queue: m.Queue, ex: m.Exchange,
			rk: m.RoutingKey, props: m.Props, payload: m.Payload, pers: b2i(m.Persistent),
			redel: b2i(m.Redelivered), state: st, created: m.created})
	}
	s.mu.Unlock()

	var ferr error
	if len(rows) > 0 || len(up) > 0 || len(dp) > 0 {
		ferr = s.flushBatch(rows, up, dp)
	}
	// publisher confirm：本批落库结果统一回调（err=nil→basic.ack，失败→basic.nack）。
	// 回调在 flusher goroutine 执行，不持 s.mu；连接写锁由回调实现方自管（wmu）。
	for _, cb := range pc {
		cb(ferr)
	}
	return ferr
}

// flushBatch 单事务批量落库：journal INSERT + 派发 unacked 批量 UPDATE + 删除批量 DELETE。
// 任一步失败整体回滚，confirm 以错误回调（basic.nack）感知。
func (s *Store) flushBatch(rows []jrow, unackIDs, delIDs []int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Commit 成功后为 no-op

	if len(rows) > 0 {
		stmt, err := tx.Prepare(`INSERT INTO mq_messages(id,ns,queue,exchange,routing_key,props,payload,persistent,redelivered,state,created)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		var inserted []*Message
		for _, r := range rows {
			if atomic.LoadUint32(&r.m.state) == msgStateGone {
				continue // 换出后被删除（purge/delete/ack）：跳过落库
			}
			if _, err := stmt.Exec(r.id, r.ns, r.queue, r.ex, r.rk, r.props, r.payload, r.pers, r.redel, r.state, r.created); err != nil {
				stmt.Close()
				return err
			}
			inserted = append(inserted, r.m)
		}
		stmt.Close()
		// 提交前按原子 state 复查并发删除，同事务内清除——杜绝"INSERT 晚于 DELETE 提交"的复活行
		for _, m := range inserted {
			if atomic.LoadUint32(&m.state) == msgStateGone {
				if _, err := tx.Exec(`DELETE FROM mq_messages WHERE id=?`, m.ID); err != nil {
					return err
				}
			}
		}
	}
	for _, chunk := range chunkIDs(unackIDs, mqIDChunk) {
		if _, err := tx.Exec(`UPDATE mq_messages SET state='unacked' WHERE id IN (`+idIN(len(chunk))+`)`, idsArgs(chunk)...); err != nil {
			return err
		}
	}
	for _, chunk := range chunkIDs(delIDs, mqIDChunk) {
		if _, err := tx.Exec(`DELETE FROM mq_messages WHERE id IN (`+idIN(len(chunk))+`)`, idsArgs(chunk)...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// loadMessages 重启恢复：mq_messages 全量载入内存 ready 链（unacked 已在 Open 的
// UPDATE 中重置 ready + redelivered=1，与断连重入队语义一致），并播种内存 ID 分配器
// （≥ max(sqlite_sequence, max_id)；此后显式 id 插入自动推进 sqlite_sequence）。
func (s *Store) loadMessages() error {
	var seq, maxID int64
	_ = s.db.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name='mq_messages'`).Scan(&seq)
	_ = s.db.QueryRow(`SELECT COALESCE(MAX(id),0) FROM mq_messages`).Scan(&maxID)
	if seq > maxID {
		maxID = seq
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.idSeq = maxID
	rows, err := s.db.Query(`SELECT id,ns,queue,exchange,routing_key,props,payload,persistent,redelivered
		FROM mq_messages ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var m Message
		var pers, redel int
		if err := rows.Scan(&m.ID, &m.NS, &m.Queue, &m.Exchange, &m.RoutingKey, &m.Props, &m.Payload, &pers, &redel); err != nil {
			return err
		}
		m.Persistent = pers != 0
		m.Redelivered = redel != 0
		m.state = msgStateReady
		s.pushReadyLocked(qkey(m.NS, m.Queue), &m)
		s.live[m.ID] = &m // v1.4.0：恢复消息同样入 live 索引——否则恢复后 Ack/Nack/Requeue 找不到消息（unacked 计数漂移、requeue 丢消息）
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	logx.Infof("mq: %d messages loaded into memory (async flush, interval=%dms)", n, s.flushIv.Milliseconds())
	return nil
}

// FlushSync 同步刷空 journal 与全部旁路列表 + 统计缓冲（main.go 停机钩子，v1.4.0）。
func (s *Store) FlushSync() error {
	var lastErr error
	if s.async() {
		s.nudgeFlush()
		for i := 0; i < 128; i++ { // 防呆上限；正常一至两轮即空
			if err := s.flushOnce(); err != nil {
				lastErr = err
			}
			s.mu.Lock()
			pending := len(s.journal) + len(s.pendConfirms) + len(s.unackPend) + len(s.delPend)
			s.mu.Unlock()
			if pending == 0 {
				break
			}
		}
	}
	s.flushStats()
	return lastErr
}

// ---------------------------------------------------------------------------
// 统计内存聚合（v1.4.0）：bumpStatLocked 改写 statBuf，statLoop 每 5s 批量 UPSERT
//（仿 s3 usageBuf 先例）；SHOW QUEUES 读取时合并 DB 行 + 未落库增量。
// ---------------------------------------------------------------------------

// statLoop 统计缓冲周期落库（两种模式共用）。
func (s *Store) statLoop() {
	t := time.NewTicker(mqStatFlush)
	defer t.Stop()
	for range t.C {
		s.flushStats()
	}
}

// flushStats 统计缓冲批量落库（换出缓冲 → 逐行 UPSERT；不持 s.mu，statMu 为叶锁）。
func (s *Store) flushStats() {
	s.statMu.Lock()
	if len(s.statBuf) == 0 {
		s.statMu.Unlock()
		return
	}
	buf := s.statBuf
	s.statBuf = map[string]*statDelta{}
	s.statMu.Unlock()
	now := time.Now().Unix()
	for k, d := range buf {
		i := strings.IndexByte(k, 0)
		if i < 0 {
			continue
		}
		_, _ = s.db.Exec(`INSERT INTO mq_stats(ns,name,published,delivered,acked,updated) VALUES(?,?,?,?,?,?)
			ON CONFLICT(ns,name) DO UPDATE SET published=published+excluded.published,
			delivered=delivered+excluded.delivered, acked=acked+excluded.acked, updated=excluded.updated`,
			k[:i], k[i+1:], d.pub, d.del, d.ack, now)
	}
}

// ---------------------------------------------------------------------------
// 批量语句辅助
// ---------------------------------------------------------------------------

// idIN 生成 n 个占位符 "?,?,..."。
func idIN(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

// idsArgs []int64 → []any。
func idsArgs(ids []int64) []any {
	out := make([]any, len(ids))
	for i, id := range ids {
		out[i] = id
	}
	return out
}

// chunkIDs 按 n 切片（IN 参数分片，防超 SQLite 变量数上限）。
func chunkIDs(ids []int64, n int) [][]int64 {
	if len(ids) == 0 {
		return nil
	}
	var out [][]int64
	for i := 0; i < len(ids); i += n {
		end := i + n
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[i:end])
	}
	return out
}
