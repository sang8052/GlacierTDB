package mq

import (
	"database/sql"
	"testing"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/sqlite"
)

// v1.4.0 冒烟回归：写后落盘（journal+confirm）、内存派发、ack 批量删除、
// transient 纯内存、重启恢复、async_flush_ms=0 旧同步路径兼容。
// 说明：运行需 sqlite.Backend（本机无工具链，在远端 docker 内 go test 执行）。

func newTestStore(t *testing.T, asyncMs int) (*Store, *sql.DB) {
	t.Helper()
	cfg := &config.Config{DBDir: t.TempDir()}
	cfg.Fill()
	b, err := sqlite.NewBackend(cfg)
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	db, err := b.OpenMQDB()
	if err != nil {
		t.Fatalf("mqdb: %v", err)
	}
	s, err := Open(b, config.MQOpts{Enable: true, AsyncFlushMs: asyncMs})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return s, db
}

func dbCount(t *testing.T, db *sql.DB, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// waitDBCount 轮询等待落库行数达标（异步刷盘最终一致）。
func waitDBCount(t *testing.T, db *sql.DB, want int64, query string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if dbCount(t, db, query, args...) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("db count wait timeout: %s want=%d got=%d", query, want, dbCount(t, db, query, args...))
}

func TestAsyncFlushConfirmDispatchAck(t *testing.T) {
	s, db := newTestStore(t, 20)
	if !s.async() {
		t.Fatal("async mode expected")
	}
	if err := s.DeclareExchange("u1", "ex", "direct", true, false, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.DeclareQueue("u1", "q", true, false, false, false); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind("u1", "ex", "q", "k1"); err != nil {
		t.Fatal(err)
	}

	// publisher confirm：journal 批量落库成功后回调
	conf := make(chan error, 1)
	n, err := s.PublishConfirm("u1", "ex", "k1", nil, []byte("hello"), true, func(e error) { conf <- e })
	if err != nil || n != 1 {
		t.Fatalf("publish routed=%d err=%v", n, err)
	}
	select {
	case e := <-conf:
		if e != nil {
			t.Fatalf("confirm err: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("confirm timeout")
	}
	waitDBCount(t, db, 1, `SELECT COUNT(*) FROM mq_messages WHERE ns='u1' AND state='ready'`)

	// 内存派发 + Ack（durable 批量 DELETE 随 flush 落库）
	got := make(chan *Message, 8)
	s.RegisterConsumer(&Consumer{Tag: "t1", NS: "u1", Queue: "q", Deliver: func(m *Message) { got <- m }})
	var msgID int64
	select {
	case m := <-got:
		msgID = m.ID
		if string(m.Payload) != "hello" {
			t.Fatalf("payload = %q", m.Payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver timeout")
	}
	s.Ack("u1", []int64{msgID})
	waitDBCount(t, db, 0, `SELECT COUNT(*) FROM mq_messages WHERE ns='u1'`)

	// transient（delivery_mode=1）：纯内存，不进 journal 不落库
	if _, err := s.Publish("u1", "ex", "k1", nil, []byte("tmp"), false); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m.Persistent {
			t.Fatal("transient message marked persistent")
		}
		s.Ack("u1", []int64{m.ID})
	case <-time.After(5 * time.Second):
		t.Fatal("transient deliver timeout")
	}
	time.Sleep(80 * time.Millisecond) // 数轮 flush
	if c := dbCount(t, db, `SELECT COUNT(*) FROM mq_messages WHERE ns='u1'`); c != 0 {
		t.Fatalf("transient rows in db = %d, want 0", c)
	}
	if s.MessageCount() != 0 {
		t.Fatalf("MessageCount = %d, want 0", s.MessageCount())
	}
	if c := s.QueueCount(); c != 1 {
		t.Fatalf("QueueCount = %d, want 1", c)
	}
	qs := s.Queues("u1")
	if len(qs) != 1 || qs[0].Published < 2 || qs[0].Delivered < 2 || qs[0].Acked < 2 {
		t.Fatalf("Queues stat = %+v", qs)
	}
}

func TestAsyncRecovery(t *testing.T) {
	cfg := &config.Config{DBDir: t.TempDir()}
	cfg.Fill()
	b, err := sqlite.NewBackend(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(b, config.MQOpts{Enable: true, AsyncFlushMs: 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.DeclareQueue("u1", "rq", true, false, false, false); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Publish("u1", "", "rq", nil, []byte("m"), true); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.FlushSync(); err != nil {
		t.Fatal(err)
	}
	// 3 条全部派发（prefetch 0 = 不限），ack 1 条留 2 条未确认
	got := make(chan *Message, 8)
	s.RegisterConsumer(&Consumer{Tag: "t", NS: "u1", Queue: "rq", Deliver: func(m *Message) { got <- m }})
	var ids []int64
	for i := 0; i < 3; i++ {
		select {
		case m := <-got:
			ids = append(ids, m.ID)
		case <-time.After(5 * time.Second):
			t.Fatal("deliver timeout")
		}
	}
	s.Ack("u1", ids[:1])
	// 等派发态落库（unackPend 批量 UPDATE 异步）：2 条均为 unacked 后再模拟重启，
	// 恢复的 redelivered 标志才是确定性的（异步窗口内该标志本就是尽力而为）
	waitDBCount(t, b2db(t, b), 2, `SELECT COUNT(*) FROM mq_messages WHERE ns='u1' AND state='unacked'`)

	// 模拟崩溃重启：内存态丢弃，DB 里的 unacked 恢复为 ready + redelivered
	s2, err := Open(b, config.MQOpts{Enable: true, AsyncFlushMs: 20})
	if err != nil {
		t.Fatal(err)
	}
	if c := s2.MessageCount(); c != 2 {
		t.Fatalf("recovered MessageCount = %d, want 2", c)
	}
	ready, _, err := s2.DeclareQueue("u1", "rq", true, false, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if ready != 2 {
		t.Fatalf("recovered ready = %d, want 2", ready)
	}
	got2 := make(chan *Message, 8)
	s2.RegisterConsumer(&Consumer{Tag: "t2", NS: "u1", Queue: "rq", Deliver: func(m *Message) { got2 <- m }})
	for i := 0; i < 2; i++ {
		select {
		case m := <-got2:
			if !m.Redelivered {
				t.Fatal("recovered message should be redelivered")
			}
			s2.Ack("u1", []int64{m.ID})
		case <-time.After(5 * time.Second):
			t.Fatal("recover deliver timeout")
		}
	}
	if err := s2.FlushSync(); err != nil {
		t.Fatal(err)
	}
	if c := s2.MessageCount(); c != 0 {
		t.Fatalf("MessageCount after ack = %d, want 0", c)
	}
}

// TestSyncModeRegression async_flush_ms=0：旧同步路径（逐条 INSERT/UPDATE/DELETE）回归。
func TestSyncModeRegression(t *testing.T) {
	s, db := newTestStore(t, 0)
	if s.async() {
		t.Fatal("sync mode expected")
	}
	if _, _, err := s.DeclareQueue("u1", "sq", true, false, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish("u1", "", "sq", nil, []byte("d1"), true); err != nil {
		t.Fatal(err)
	}
	waitDBCount(t, db, 1, `SELECT COUNT(*) FROM mq_messages WHERE ns='u1' AND state='ready'`)
	got := make(chan *Message, 4)
	s.RegisterConsumer(&Consumer{Tag: "t", NS: "u1", Queue: "sq", Prefetch: 10, Deliver: func(m *Message) { got <- m }})
	var id int64
	select {
	case m := <-got:
		id = m.ID
		if m.Redelivered {
			t.Fatal("fresh message marked redelivered")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver timeout")
	}
	s.Ack("u1", []int64{id})
	waitDBCount(t, db, 0, `SELECT COUNT(*) FROM mq_messages WHERE ns='u1'`)
	if s.MessageCount() != 0 {
		t.Fatalf("MessageCount = %d, want 0", s.MessageCount())
	}
}

// TestRouteCacheEquivalence 路由内存缓存与原 SQL 语义等价：direct/topic/fanout + 默认交换机。
func TestRouteCacheEquivalence(t *testing.T) {
	s, _ := newTestStore(t, 20)
	if err := s.DeclareExchange("u1", "direct-ex", "direct", true, false, false); err != nil {
		t.Fatal(err)
	}
	if err := s.DeclareExchange("u1", "topic-ex", "topic", true, false, false); err != nil {
		t.Fatal(err)
	}
	if err := s.DeclareExchange("u1", "fan-ex", "fanout", true, false, false); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"qa", "qb", "qc"} {
		if _, _, err := s.DeclareQueue("u1", q, true, false, false, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Bind("u1", "direct-ex", "qa", "k1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind("u1", "direct-ex", "qb", "k2"); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind("u1", "topic-ex", "qa", "a.*"); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind("u1", "topic-ex", "qb", "a.#"); err != nil {
		t.Fatal(err)
	}
	if err := s.Bind("u1", "fan-ex", "qc", ""); err != nil {
		t.Fatal(err)
	}

	s.mu.Lock()
	// direct：精确匹配
	if got := s.routeLocked("u1", "direct-ex", "k1"); len(got) != 1 || got[0] != "qa" {
		s.mu.Unlock()
		t.Fatalf("direct k1 = %v", got)
	}
	if got := s.routeLocked("u1", "direct-ex", "nope"); len(got) != 0 {
		s.mu.Unlock()
		t.Fatalf("direct nope = %v", got)
	}
	// topic：* 单词 / # 多词
	if got := s.routeLocked("u1", "topic-ex", "a.b"); len(got) != 2 || got[0] != "qa" || got[1] != "qb" {
		s.mu.Unlock()
		t.Fatalf("topic a.b = %v", got)
	}
	if got := s.routeLocked("u1", "topic-ex", "a"); len(got) != 1 || got[0] != "qb" {
		s.mu.Unlock()
		t.Fatalf("topic a = %v", got)
	}
	if got := s.routeLocked("u1", "topic-ex", "x.y"); len(got) != 0 {
		s.mu.Unlock()
		t.Fatalf("topic x.y = %v", got)
	}
	// fanout：全量且有序
	if got := s.routeLocked("u1", "fan-ex", "anything"); len(got) != 1 || got[0] != "qc" {
		s.mu.Unlock()
		t.Fatalf("fanout = %v", got)
	}
	// 默认交换机：routing key 即队列名
	if got := s.routeLocked("u1", "", "qa"); len(got) != 1 || got[0] != "qa" {
		s.mu.Unlock()
		t.Fatalf("default ex = %v", got)
	}
	if got := s.routeLocked("u1", "", "ghost"); len(got) != 0 {
		s.mu.Unlock()
		t.Fatalf("default ex ghost = %v", got)
	}
	// 未声明交换机不可路由
	if got := s.routeLocked("u1", "ghost-ex", "k1"); len(got) != 0 {
		s.mu.Unlock()
		t.Fatalf("ghost ex = %v", got)
	}
	s.mu.Unlock()

	// 解绑后不再路由（Unbind 内部取 s.mu，须在锁外调用）
	if err := s.Unbind("u1", "direct-ex", "qa", "k1"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	got := s.routeLocked("u1", "direct-ex", "k1")
	s.mu.Unlock()
	if len(got) != 0 {
		t.Fatalf("after unbind = %v", got)
	}
}

// b2db 测试辅助：再开一个只读连接池查库。
func b2db(t *testing.T, b *sqlite.Backend) *sql.DB {
	t.Helper()
	db, err := b.OpenMQDB()
	if err != nil {
		t.Fatal(err)
	}
	return db
}
