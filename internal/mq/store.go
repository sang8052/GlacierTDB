package mq

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
	"glaciertdb/internal/sqlite"
)

// Message 队列消息（属性原样透传）。
type Message struct {
	ID          int64
	NS          string
	Queue       string
	Exchange    string
	RoutingKey  string
	Props       []byte // content-header 属性（property-flags + fields）
	Payload     []byte
	Persistent  bool
	Redelivered bool

	// v1.4.0 内存态（仅 async 模式使用）：state 原子访问——flusher 事务内免锁复查
	// 并发删除；created 为入队时间（journal 落库 created 列）。
	state   uint32
	created int64
}

// Consumer 活动消费者（连接级，随连接销毁）。
type Consumer struct {
	Tag        string
	NS         string
	Queue      string
	NoAck      bool
	Prefetch   int // 0 = 不限
	Exclusive  bool
	ConnID     uint64
	Deliver    func(m *Message) // 发送 Deliver + 内容（连接写锁由实现方持有）
	unacked    int
	unackedSet map[int64]bool
}

// QueueInfo 队列信息（SHOW QUEUES）。
type QueueInfo struct {
	NS        string
	Name      string
	Ready     int64
	Unacked   int64
	Consumers int
	Published int64
	Delivered int64
	Acked     int64
	Durable   bool
}

// ExchangeInfo 交换机信息（SHOW EXCHANGES）。
type ExchangeInfo struct {
	NS       string
	Name     string
	Type     string
	Durable  bool
	Bindings int
}

// Store AMQP 存储：SQLite 落盘（glacier_mq.db）+ 内存消费者注册表。
// v1.4.0：路由内存缓存（route_cache.go）+ 写后落盘 journal/flusher（flush.go）+
// 统计内存聚合；async_flush_ms=0 时保留旧同步路径（publish/派发/确认逐条 SQL）。
type Store struct {
	mu          sync.Mutex
	db          *sql.DB
	consumers   map[string][]*Consumer // ns|queue → 消费者
	deliveredBy map[int64]*Consumer    // message id → 接收消费者（ack 回补 prefetch）
	kick        chan struct{}
	maxMsg      int64

	published atomic.Int64
	delivered atomic.Int64
	acked     atomic.Int64
	startTS   time.Time

	// ---- 路由内存缓存（v1.4.0，route_cache.go）：Publish 热路径免查库 ----
	routeMu sync.RWMutex
	rExch   map[string]*routeExchange // ns\x00name → 交换机
	rQueue  map[string]*routeQueue    // ns\x00name → 队列
	rBind   map[string][]routeBinding // ns\x00exchange → 绑定列表

	// ---- 写后落盘（v1.4.0，flush.go）：journal 与旁路批处理（s.mu 保护）----
	flushIv      time.Duration       // 落盘周期（opts.AsyncFlushMs；0 = 旧同步路径）
	flushKick    chan struct{}       // flusher 唤醒
	journal      []*Message          // 待落盘 durable 消息
	pendConfirms []func(error)       // 待 flush 成功后回调的 publisher confirm
	unackPend    []int64             // 待批量 UPDATE state='unacked' 的 durable id
	delPend      []int64             // 待批量 DELETE 的 durable id
	ready        map[string]*msgList // ns\x00queue → 内存 ready 链（async 模式）
	live         map[int64]*Message  // id → 活跃内存消息（ready+unacked，async 模式）
	unackedMsgs  map[string]int64    // ns\x00queue → unacked 计数（async 模式 SHOW 用）
	idSeq        int64               // 内存 ID 分配器（Open 以 max(seq,max_id) 播种）

	// ---- 统计内存聚合（v1.4.0）：statBuf 缓冲 + statLoop 每 5s 批量 UPSERT ----
	statMu  sync.Mutex
	statBuf map[string]*statDelta
}

// Open 打开系统库、恢复（未确认消息重入队）并启动分发循环。
// v1.4.0：签名改为接收 config.MQOpts（AsyncFlushMs 写后落盘周期、MaxMessageMB 单条上限；
// main.go 已配合改为 mq.Open(backend, cfg.MQ)）。
func Open(backend *sqlite.Backend, opts config.MQOpts) (*Store, error) {
	db, err := backend.OpenMQDB()
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS mq_exchanges (
  ns TEXT NOT NULL, name TEXT NOT NULL, type TEXT NOT NULL, durable INTEGER NOT NULL DEFAULT 1,
  auto_delete INTEGER NOT NULL DEFAULT 0, args TEXT NOT NULL DEFAULT '', created INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, name)
);
CREATE TABLE IF NOT EXISTS mq_queues (
  ns TEXT NOT NULL, name TEXT NOT NULL, durable INTEGER NOT NULL DEFAULT 1,
  exclusive INTEGER NOT NULL DEFAULT 0, auto_delete INTEGER NOT NULL DEFAULT 0,
  args TEXT NOT NULL DEFAULT '', created INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, name)
);
CREATE TABLE IF NOT EXISTS mq_bindings (
  ns TEXT NOT NULL, exchange TEXT NOT NULL, queue TEXT NOT NULL, routing_key TEXT NOT NULL,
  PRIMARY KEY (ns, exchange, queue, routing_key)
);
CREATE TABLE IF NOT EXISTS mq_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ns TEXT NOT NULL, queue TEXT NOT NULL, exchange TEXT NOT NULL DEFAULT '',
  routing_key TEXT NOT NULL DEFAULT '', props BLOB, payload BLOB,
  persistent INTEGER NOT NULL DEFAULT 1, redelivered INTEGER NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'ready', created INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS mq_msg_q ON mq_messages(ns, queue, state, id);
CREATE TABLE IF NOT EXISTS mq_stats (
  ns TEXT NOT NULL, name TEXT NOT NULL,
  published INTEGER NOT NULL DEFAULT 0, delivered INTEGER NOT NULL DEFAULT 0,
  acked INTEGER NOT NULL DEFAULT 0, updated INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, name)
);
CREATE TABLE IF NOT EXISTS mqtt_retains (
  ns TEXT NOT NULL, topic TEXT NOT NULL, rk TEXT NOT NULL DEFAULT '',
  payload BLOB, qos INTEGER NOT NULL DEFAULT 0, ts INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ns, topic)
);`); err != nil {
		return nil, fmt.Errorf("init mq schema: %w", err)
	}
	// 重启恢复：未确认消息重入队（消费游标语义随状态机落盘）
	_, _ = db.Exec(`UPDATE mq_messages SET state='ready', redelivered=1 WHERE state='unacked'`)

	s := &Store{
		db:          db,
		consumers:   map[string][]*Consumer{},
		deliveredBy: map[int64]*Consumer{},
		kick:        make(chan struct{}, 1),
		maxMsg:      int64(opts.MaxMessageMB) << 20,
		flushIv:     time.Duration(opts.AsyncFlushMs) * time.Millisecond,
		flushKick:   make(chan struct{}, 1),
		rExch:       map[string]*routeExchange{},
		rQueue:      map[string]*routeQueue{},
		rBind:       map[string][]routeBinding{},
		ready:       map[string]*msgList{},
		live:        map[int64]*Message{},
		unackedMsgs: map[string]int64{},
		statBuf:     map[string]*statDelta{},
		startTS:     time.Now(),
	}
	if err := s.loadRoutes(); err != nil { // v1.4.0：路由缓存全量加载
		return nil, fmt.Errorf("mq: load route cache: %w", err)
	}
	if s.async() { // v1.4.0：消息全量载入内存 + flusher 后台批量落盘
		if err := s.loadMessages(); err != nil {
			return nil, fmt.Errorf("mq: load messages: %w", err)
		}
		go s.flushLoop()
	}
	go s.dispatchLoop()
	go s.statLoop() // v1.4.0：统计内存聚合批量落库（两种模式共用）
	logx.Infof("mq: store ready (max_message_mb=%d, async_flush_ms=%d)", opts.MaxMessageMB, opts.AsyncFlushMs)
	return s, nil
}

func qkey(ns, queue string) string { return ns + "\x00" + queue }

// ---------------------------------------------------------------------------
// 声明 / 绑定 / 删除（写 SQL 保持不变，落库成功后同步维护路由内存缓存，v1.4.0）
// ---------------------------------------------------------------------------

// DeclareExchange 声明交换机；existing 不匹配返回 406 语义错误。
func (s *Store) DeclareExchange(ns, name, typ string, durable, autoDelete, passive bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var curType string
	var curDur, curAD int
	err := s.db.QueryRow(`SELECT type,durable,auto_delete FROM mq_exchanges WHERE ns=? AND name=?`, ns, name).
		Scan(&curType, &curDur, &curAD)
	if err == nil {
		// v1.4.0：已存在——以 DB 行值自愈缓存（防漂移），passive 语义不变
		s.setRouteExchangeLocked(ns, name, curType, curDur != 0, curAD != 0)
		if passive {
			return nil
		}
		if curType != typ || (curDur != 0) != durable {
			return fmt.Errorf("PRECONDITION_FAILED - inequivalent arg 'type|durable' for exchange '%s'", name)
		}
		return nil
	}
	if passive {
		return fmt.Errorf("NOT_FOUND - no exchange '%s'", name)
	}
	if typ == "" {
		typ = "direct"
	}
	_, err = s.db.Exec(`INSERT INTO mq_exchanges(ns,name,type,durable,auto_delete,created) VALUES(?,?,?,?,?,?)`,
		ns, name, typ, b2i(durable), b2i(autoDelete), time.Now().Unix())
	if err == nil { // v1.4.0：缓存同步维护
		s.setRouteExchangeLocked(ns, name, typ, durable, autoDelete)
	}
	return err
}

// DeclareQueue 声明队列，返回 (ready 数, 消费者数)。
func (s *Store) DeclareQueue(ns, name string, durable, exclusive, autoDelete, passive bool) (int64, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var curDur, curEx, curAD int
	err := s.db.QueryRow(`SELECT durable,exclusive,auto_delete FROM mq_queues WHERE ns=? AND name=?`, ns, name).
		Scan(&curDur, &curEx, &curAD)
	if err != nil {
		if passive {
			return 0, 0, fmt.Errorf("NOT_FOUND - no queue '%s'", name)
		}
		if _, err := s.db.Exec(`INSERT INTO mq_queues(ns,name,durable,exclusive,auto_delete,created) VALUES(?,?,?,?,?,?)`,
			ns, name, b2i(durable), b2i(exclusive), b2i(autoDelete), time.Now().Unix()); err != nil {
			return 0, 0, err
		}
		s.setRouteQueueLocked(ns, name, durable, exclusive, autoDelete) // v1.4.0：缓存维护
	} else {
		s.setRouteQueueLocked(ns, name, curDur != 0, curEx != 0, curAD != 0) // v1.4.0：缓存自愈
		if !passive && (curDur != 0) != durable {
			return 0, 0, fmt.Errorf("PRECONDITION_FAILED - inequivalent arg 'durable' for queue '%s'", name)
		}
	}
	var ready int64
	if s.async() {
		ready = s.readyCountLocked(qkey(ns, name)) // v1.4.0：async 模式读内存（journal 未落盘部分 DB 缺行）
	} else {
		ready, _ = s.countReady(ns, name)
	}
	return ready, len(s.consumers[qkey(ns, name)]), nil
}

// Bind 队列绑定交换机。
func (s *Store) Bind(ns, exchange, queue, routingKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.Exec(`INSERT OR IGNORE INTO mq_bindings(ns,exchange,queue,routing_key) VALUES(?,?,?,?)`,
		ns, exchange, queue, routingKey)
	if err == nil {
		if n, _ := res.RowsAffected(); n > 0 { // v1.4.0：实际新插入才记缓存（幂等去重）
			s.addRouteBindingLocked(ns, exchange, queue, routingKey)
		}
	}
	return err
}

// Unbind 解绑。
func (s *Store) Unbind(ns, exchange, queue, routingKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM mq_bindings WHERE ns=? AND exchange=? AND queue=? AND routing_key=?`,
		ns, exchange, queue, routingKey)
	if err == nil { // v1.4.0：缓存同步维护
		s.dropRouteBindingLocked(ns, exchange, queue, routingKey)
	}
	return err
}

// DeleteExchange 删除交换机（含绑定）。
func (s *Store) DeleteExchange(ns, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.db.Exec(`DELETE FROM mq_bindings WHERE ns=? AND exchange=?`, ns, name)
	_, err := s.db.Exec(`DELETE FROM mq_exchanges WHERE ns=? AND name=?`, ns, name)
	if err == nil { // v1.4.0：缓存同步维护
		s.dropRouteExchangeLocked(ns, name)
	}
	return err
}

// DeleteQueue 删除队列（含消息与绑定），返回删除前 ready 数。
func (s *Store) DeleteQueue(ns, name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := qkey(ns, name)
	var n int64
	if s.async() {
		n = s.readyCountLocked(k) // v1.4.0：async 模式 ready 数以内存为准
	} else {
		n, _ = s.countReady(ns, name)
	}
	if s.async() {
		// v1.4.0：内存全清（ready + unacked；原 SQL 同样删除该队列全部消息行）。
		// ready 链清空并标记 gone（journal 在途行由 flusher 复查跳过/回除）；
		// unacked 消息经 live 定位一并清理，deliveredBy 顺带回收并回补 prefetch 计数。
		if l := s.ready[k]; l != nil {
			for {
				m := l.pop()
				if m == nil {
					break
				}
				m.state = msgStateGone
				delete(s.live, m.ID)
			}
		}
		for id, m := range s.live {
			if m.NS == ns && m.Queue == name {
				m.state = msgStateGone
				delete(s.live, id)
				if c := s.deliveredBy[id]; c != nil {
					c.unacked--
					if c.unacked < 0 {
						c.unacked = 0
					}
					c.dropUnacked([]int64{id})
					delete(s.deliveredBy, id)
				}
			}
		}
		delete(s.unackedMsgs, k)
		// 统计缓冲丢弃（避免延迟落库复活已删队列的 stats 行）
		s.statMu.Lock()
		delete(s.statBuf, k)
		s.statMu.Unlock()
	}
	_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE ns=? AND queue=?`, ns, name)
	_, _ = s.db.Exec(`DELETE FROM mq_bindings WHERE ns=? AND queue=?`, ns, name)
	_, _ = s.db.Exec(`DELETE FROM mq_queues WHERE ns=? AND name=?`, ns, name)
	_, _ = s.db.Exec(`DELETE FROM mq_stats WHERE ns=? AND name=?`, ns, name)
	s.consumers[k] = nil
	s.dropRouteQueueLocked(ns, name) // v1.4.0：缓存级联清理（队列 + 各交换机指向它的绑定）
	return n, nil
}

// PurgeQueue 清空队列 ready 消息，返回清除数。
func (s *Store) PurgeQueue(ns, name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := qkey(ns, name)
	if s.async() {
		// v1.4.0：内存 ready 链清空并标记 gone（journal 在途行由 flusher 复查跳过）；
		// 已落库的 ready 行照旧 SQL 清除（unacked 行保留——原语义）。
		n := s.readyCountLocked(k)
		if l := s.ready[k]; l != nil {
			for {
				m := l.pop()
				if m == nil {
					break
				}
				m.state = msgStateGone
				delete(s.live, m.ID)
			}
		}
		_, err := s.db.Exec(`DELETE FROM mq_messages WHERE ns=? AND queue=? AND state='ready'`, ns, name)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	res, err := s.db.Exec(`DELETE FROM mq_messages WHERE ns=? AND queue=? AND state='ready'`, ns, name)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (s *Store) countReady(ns, queue string) (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM mq_messages WHERE ns=? AND queue=? AND state='ready'`, ns, queue).Scan(&n)
	return n, err
}

// ---------------------------------------------------------------------------
// 发布与路由
// ---------------------------------------------------------------------------

// Publish 发布消息（默认交换机 "" 直达队列名；direct/fanout/topic 路由）。
// 返回投递到的队列数（0 = 不可路由，mandatory 回退判定用，v1.3.2）。
// v1.4.0 保持签名不变（mqtt 桥接依赖）；confirm 场景用 PublishConfirm。
func (s *Store) Publish(ns, exchange, routingKey string, props, payload []byte, persistent bool) (int, error) {
	return s.publish(ns, exchange, routingKey, props, payload, persistent, nil)
}

// PublishConfirm 带 publisher confirm 回调的发布（v1.4.0）。
// async 模式下 cb 在本批 journal 落库事务结束后由 flusher 调用（err=nil→basic.ack，
// 失败→basic.nack），等价原"落库成功后 confirm"时序；回调在 flusher goroutine 执行、
// 不持 s.mu，连接写锁由回调实现方自管（connState.writeMethod/writeFlush 持 wmu）。
func (s *Store) PublishConfirm(ns, exchange, routingKey string, props, payload []byte, persistent bool, cb func(err error)) (int, error) {
	return s.publish(ns, exchange, routingKey, props, payload, persistent, cb)
}

// publish 发布核心（v1.4.0）：async 模式内存构建消息入 ready 链 + journal；
// sync 模式保持原逐条 INSERT 落库路径。
func (s *Store) publish(ns, exchange, routingKey string, props, payload []byte, persistent bool, cb func(error)) (int, error) {
	if s.maxMsg > 0 && int64(len(payload)) > s.maxMsg {
		return 0, fmt.Errorf("message size %d exceeds max_message_mb limit", len(payload))
	}
	s.mu.Lock()
	// v1.4.0 背压：journal 积压超阈值 → 本调用内联同步刷一轮（必须先放锁再执行事务）
	if s.async() && len(s.journal) > mqJournalBacklog {
		s.mu.Unlock()
		_ = s.flushOnce()
		s.mu.Lock()
	}
	queues := s.routeLocked(ns, exchange, routingKey)
	now := time.Now().Unix()
	for _, q := range queues {
		if s.async() {
			// v1.4.0：内存构建消息——durable 进 ready 链 + journal（flusher 批量落盘）；
			// transient 纯内存不落库（重启即失，与 RabbitMQ 非持久化语义一致）
			m := &Message{ID: s.nextIDLocked(), NS: ns, Queue: q, Exchange: exchange,
				RoutingKey: routingKey, Props: props, Payload: payload,
				Persistent: persistent, created: now}
			s.pushReadyLocked(qkey(ns, q), m)
			s.live[m.ID] = m
			if persistent {
				s.journal = append(s.journal, m)
			}
		} else {
			_, _ = s.db.Exec(`INSERT INTO mq_messages(ns,queue,exchange,routing_key,props,payload,persistent,redelivered,state,created)
				VALUES(?,?,?,?,?,?,?,0,'ready',?)`,
				ns, q, exchange, routingKey, props, payload, b2i(persistent), now)
		}
		s.bumpStatLocked(ns, q, "published", 1)
	}
	if cb != nil {
		if s.async() {
			s.pendConfirms = append(s.pendConfirms, cb) // flusher 批量落库后统一回调
		} else {
			defer cb(nil) // 同步模式：返回即落库完成，直接回调
		}
	}
	s.mu.Unlock()
	s.published.Add(1)
	if s.async() {
		s.nudgeFlush()
	}
	s.nudge()
	return len(queues), nil
}

// routeLocked 解析路由目标队列（内存路由缓存，调用方持 s.mu；v1.4.0 替代原逐条
// SELECT——等价性对照见 route_cache.go 头注释）。
func (s *Store) routeLocked(ns, exchange, routingKey string) []string {
	s.routeMu.RLock()
	defer s.routeMu.RUnlock()
	if exchange == "" {
		// 默认交换机：routing key 即队列名（原 SQL：mq_queues 存在性判断）
		if s.rQueue[qkey(ns, routingKey)] != nil {
			return []string{routingKey}
		}
		return nil
	}
	re := s.rExch[qkey(ns, exchange)] // 原 SQL：mq_exchanges 查 type，未声明不可路由
	if re == nil {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, b := range s.rBind[qkey(ns, exchange)] { // 原 SQL：mq_bindings 遍历
		var match bool
		switch re.typ {
		case "fanout":
			match = true
		case "topic":
			match = topicMatch(b.rk, routingKey)
		default: // direct（原实现对非 fanout/topic 一律按 direct 精确匹配）
			match = b.rk == routingKey
		}
		if match && !seen[b.queue] {
			seen[b.queue] = true
			out = append(out, b.queue)
		}
	}
	if len(out) > 1 {
		sort.Strings(out) // 原 SQL 走 (ns,exchange,queue,rk) 主键索引，天然按 queue 升序——保持同一确定性次序
	}
	return out
}

// topicMatch topic 路由匹配（* 单词、# 零或多词）。
func topicMatch(pattern, key string) bool {
	p := strings.Split(pattern, ".")
	k := strings.Split(key, ".")
	return topicMatchWords(p, k)
}

func topicMatchWords(p, k []string) bool {
	if len(p) == 0 {
		return len(k) == 0
	}
	if p[0] == "#" {
		for i := 0; i <= len(k); i++ {
			if topicMatchWords(p[1:], k[i:]) {
				return true
			}
		}
		return false
	}
	if len(k) == 0 {
		return false
	}
	if p[0] == "*" || p[0] == k[0] {
		return topicMatchWords(p[1:], k[1:])
	}
	return false
}

// ---------------------------------------------------------------------------
// 消费 / 确认
// ---------------------------------------------------------------------------

// RegisterConsumer 注册消费者并触发分发。
func (s *Store) RegisterConsumer(c *Consumer) {
	s.mu.Lock()
	s.consumers[qkey(c.NS, c.Queue)] = append(s.consumers[qkey(c.NS, c.Queue)], c)
	s.mu.Unlock()
	s.nudge()
}

// UnregisterConsumer 注销消费者；requeue=true 时其未确认消息重入队。
func (s *Store) UnregisterConsumer(c *Consumer, requeue bool) {
	s.mu.Lock()
	list := s.consumers[qkey(c.NS, c.Queue)]
	for i, x := range list {
		if x == c {
			s.consumers[qkey(c.NS, c.Queue)] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if requeue && len(c.unackedIDs()) > 0 {
		if s.async() {
			// v1.4.0：内存回插队首即可——重启恢复本就把 unacked 重置 ready+redelivered=1，
			// 落库态无需回写（等价原逐条 UPDATE 的最终效果）
			for _, id := range c.unackedIDs() {
				if m := s.live[id]; m != nil && m.state == msgStateUnacked {
					s.requeueLocked(m)
				}
				delete(s.deliveredBy, id)
			}
		} else {
			for _, id := range c.unackedIDs() {
				_, _ = s.db.Exec(`UPDATE mq_messages SET state='ready', redelivered=1 WHERE id=? AND state='unacked'`, id)
				delete(s.deliveredBy, id)
			}
		}
	}
	s.mu.Unlock()
	s.nudge()
}

// Ack 确认（删除消息；按 deliveredBy 回补消费者 prefetch 计数）。
// v1.4.0：async 模式去掉每 id SELECT queue（deliveredBy/内存消息自带队列），
// durable 批量 DELETE（delPend 随 flush 落库）；transient 纯内存。
func (s *Store) Ack(ns string, ids []int64) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
	if s.async() {
		s.ackAsyncLocked(ns, ids)
		s.mu.Unlock()
		s.acked.Add(int64(len(ids)))
		s.nudge()
		return
	}
	// 统计需在删除前取队列名
	queues := map[string]int64{}
	for _, id := range ids {
		var q string
		if err := s.db.QueryRow(`SELECT queue FROM mq_messages WHERE id=?`, id).Scan(&q); err == nil {
			queues[q]++
		}
	}
	ph := make([]string, len(ids))
	args := make([]any, 0, len(ids)+1)
	args = append(args, ns)
	for i, id := range ids {
		ph[i] = "?"
		args = append(args, id)
	}
	_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE ns=? AND id IN (`+strings.Join(ph, ",")+`)`, args...)
	for q, n := range queues {
		s.bumpStatLocked(ns, q, "acked", n)
	}
	for _, id := range ids {
		if c := s.deliveredBy[id]; c != nil {
			c.unacked--
			if c.unacked < 0 {
				c.unacked = 0
			}
			c.dropUnacked([]int64{id})
			delete(s.deliveredBy, id)
		}
	}
	s.mu.Unlock()
	s.acked.Add(int64(len(ids)))
	s.nudge()
}

// ackAsyncLocked async 模式确认（调用方持 s.mu；v1.4.0）。
func (s *Store) ackAsyncLocked(ns string, ids []int64) {
	queues := map[string]int64{}
	var delIDs []int64
	for _, id := range ids {
		c := s.deliveredBy[id]
		m := s.live[id]
		q := ""
		if c != nil {
			q = c.Queue
		}
		if q == "" && m != nil {
			q = m.Queue
		}
		if q != "" {
			queues[q]++
		}
		if m != nil {
			m.state = msgStateGone
			delete(s.live, id)
			if m.Persistent {
				delIDs = append(delIDs, id) // 批量删除随 flush 落库（行未落库时空作用，幂等）
			}
			k := qkey(m.NS, m.Queue)
			if s.unackedMsgs[k] > 0 {
				s.unackedMsgs[k]--
			}
		}
		if c != nil {
			c.unacked--
			if c.unacked < 0 {
				c.unacked = 0
			}
			c.dropUnacked([]int64{id})
			delete(s.deliveredBy, id)
		}
	}
	if len(delIDs) > 0 {
		for _, chunk := range chunkIDs(delIDs, mqIDChunk) {
			_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE ns=? AND id IN (`+idIN(len(chunk))+`)`, append([]any{ns}, idsArgs(chunk)...)...)
		}
	}
	for q, n := range queues {
		s.bumpStatLocked(ns, q, "acked", n)
	}
}

// Nack 否定确认：requeue=true 重入队（redelivered），否则丢弃。
func (s *Store) Nack(ns string, ids []int64, requeue bool) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
	if s.async() {
		s.nackAsyncLocked(ns, ids, requeue)
		s.mu.Unlock()
		s.nudge()
		return
	}
	for _, id := range ids {
		if requeue {
			_, _ = s.db.Exec(`UPDATE mq_messages SET state='ready', redelivered=1 WHERE id=?`, id)
		} else {
			var q string
			_ = s.db.QueryRow(`SELECT queue FROM mq_messages WHERE id=?`, id).Scan(&q)
			_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE id=?`, id)
			if q != "" {
				s.bumpStatLocked(ns, q, "acked", 1)
			}
		}
		if c := s.deliveredBy[id]; c != nil {
			c.unacked--
			if c.unacked < 0 {
				c.unacked = 0
			}
			c.dropUnacked([]int64{id})
		}
		delete(s.deliveredBy, id)
	}
	s.mu.Unlock()
	s.nudge()
}

// nackAsyncLocked async 模式否定确认（调用方持 s.mu；v1.4.0）。
func (s *Store) nackAsyncLocked(ns string, ids []int64, requeue bool) {
	for _, id := range ids {
		c := s.deliveredBy[id]
		m := s.live[id]
		if requeue {
			// 内存回插队首（旧 id 优先语义）；DB 行无需回写（重启恢复统一重置 ready）
			if m != nil && m.state == msgStateUnacked {
				s.requeueLocked(m)
			}
		} else {
			q := ""
			if m != nil {
				q = m.Queue
				m.state = msgStateGone
				delete(s.live, id)
				if m.Persistent {
					s.delPend = append(s.delPend, id)
				}
				k := qkey(m.NS, m.Queue)
				if s.unackedMsgs[k] > 0 {
					s.unackedMsgs[k]--
				}
			}
			if q == "" && c != nil {
				q = c.Queue
			}
			if q != "" {
				s.bumpStatLocked(ns, q, "acked", 1) // 丢弃按原实现计入 acked
			}
		}
		if c != nil {
			c.unacked--
			if c.unacked < 0 {
				c.unacked = 0
			}
			c.dropUnacked([]int64{id})
		}
		delete(s.deliveredBy, id)
	}
}

// ConsumersByConn 某连接的全部消费者。
func (s *Store) ConsumersByConn(connID uint64) []*Consumer {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Consumer
	for _, list := range s.consumers {
		for _, c := range list {
			if c.ConnID == connID {
				out = append(out, c)
			}
		}
	}
	return out
}

// Get 同步获取一条（Basic.Get）；返回 nil 表示队列空。
func (s *Store) Get(ns, queue string, noAck bool) *Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.async() {
		// v1.4.0：内存 ready 链队首直取（等价 SELECT ... ORDER BY id LIMIT 1）
		m := s.popReadyLocked(ns, queue)
		if m == nil {
			return nil
		}
		if noAck {
			m.state = msgStateGone
			delete(s.live, m.ID)
			if m.Persistent {
				s.delPend = append(s.delPend, m.ID)
			}
			s.bumpStatLocked(ns, queue, "acked", 1)
		} else {
			m.state = msgStateUnacked
			s.unackedMsgs[qkey(ns, queue)]++
			if m.Persistent {
				s.unackPend = append(s.unackPend, m.ID)
			}
		}
		s.bumpStatLocked(ns, queue, "delivered", 1)
		s.delivered.Add(1)
		return m
	}
	row := s.db.QueryRow(`SELECT id,exchange,routing_key,props,payload,persistent,redelivered
		FROM mq_messages WHERE ns=? AND queue=? AND state='ready' ORDER BY id LIMIT 1`, ns, queue)
	m, err := scanMsg(row, ns, queue)
	if err != nil {
		return nil
	}
	if noAck {
		_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE id=?`, m.ID)
		s.bumpStatLocked(ns, queue, "acked", 1)
	} else {
		_, _ = s.db.Exec(`UPDATE mq_messages SET state='unacked' WHERE id=?`, m.ID)
	}
	s.bumpStatLocked(ns, queue, "delivered", 1)
	s.delivered.Add(1)
	return m
}

// RequeueConn 连接断开：该连接消费者未确认消息全部重入队。
func (s *Store) RequeueConn(connID uint64) {
	s.mu.Lock()
	if s.async() {
		// v1.4.0：内存回插队首（旧 id 优先语义）；DB 行无需回写——重启恢复本就把
		// unacked 重置 ready+redelivered=1，落库态等价
		for k, list := range s.consumers {
			kept := list[:0]
			for _, c := range list {
				if c.ConnID == connID {
					for _, id := range c.unackedIDs() {
						if m := s.live[id]; m != nil && m.state == msgStateUnacked {
							s.requeueLocked(m)
						}
						delete(s.deliveredBy, id)
					}
				} else {
					kept = append(kept, c)
				}
			}
			s.consumers[k] = kept
		}
		s.mu.Unlock()
		s.nudge()
		return
	}
	for k, list := range s.consumers {
		kept := list[:0]
		for _, c := range list {
			if c.ConnID == connID {
				for _, id := range c.unackedIDs() {
					_, _ = s.db.Exec(`UPDATE mq_messages SET state='ready', redelivered=1 WHERE id=?`, id)
					delete(s.deliveredBy, id)
				}
			} else {
				kept = append(kept, c)
			}
		}
		s.consumers[k] = kept
	}
	s.mu.Unlock()
	s.nudge()
}

// ---------------------------------------------------------------------------
// 分发循环
// ---------------------------------------------------------------------------

func (s *Store) nudge() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *Store) dispatchLoop() {
	// v1.4.0：空闲退避 50ms → 250ms（kick 即时驱动活跃路径）
	t := time.NewTicker(mqDispatchIdleTick)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-s.kick:
		}
		s.dispatchOnce()
	}
}

func (s *Store) dispatchOnce() {
	type pair struct {
		c *Consumer
		m *Message
	}
	var todo []pair
	s.mu.Lock()
	for k, list := range s.consumers {
		if len(list) == 0 {
			continue
		}
		ns, queue := qkeySplit(k)
		for _, c := range list {
			if s.async() {
				// v1.4.0：内存 ready 链直取（跳过 SELECT）；durable 派发态走 unackPend 批量 UPDATE
				avail := -1
				if c.Prefetch > 0 {
					avail = c.Prefetch - c.unacked
					if avail <= 0 {
						continue
					}
				}
				taken := 0
				for {
					if avail >= 0 && taken >= avail {
						break
					}
					m := s.popReadyLocked(ns, queue)
					if m == nil {
						break
					}
					taken++
					if c.NoAck {
						m.state = msgStateGone
						delete(s.live, m.ID)
						if m.Persistent {
							s.delPend = append(s.delPend, m.ID)
						}
					} else {
						m.state = msgStateUnacked
						c.unacked++
						c.noteUnacked(m.ID)
						s.deliveredBy[m.ID] = c
						s.unackedMsgs[k]++
						if m.Persistent {
							s.unackPend = append(s.unackPend, m.ID)
						}
					}
					s.bumpStatLocked(ns, queue, "delivered", 1)
					s.delivered.Add(1)
					todo = append(todo, pair{c, m})
				}
				continue
			}
			// v1.4.0：SQL 模式批量化——单消费者单 SELECT（LIMIT=剩余 prefetch，上限 256）
			// + 单条批量 UPDATE/DELETE，替代原逐条 SELECT+UPDATE（仍全程持锁）
			for {
				limit := mqDispatchBatch
				if c.Prefetch > 0 {
					remain := c.Prefetch - c.unacked
					if remain <= 0 {
						break
					}
					if remain < limit {
						limit = remain
					}
				}
				rows, err := s.db.Query(`SELECT id,exchange,routing_key,props,payload,persistent,redelivered
					FROM mq_messages WHERE ns=? AND queue=? AND state='ready' ORDER BY id LIMIT ?`, ns, queue, limit)
				if err != nil {
					break
				}
				var batch []*Message
				for rows.Next() {
					if m, err := scanMsg(rows, ns, queue); err == nil {
						batch = append(batch, m)
					}
				}
				rows.Close()
				if len(batch) == 0 {
					break
				}
				var unackIDs, delIDs []int64
				for _, m := range batch {
					if c.NoAck {
						delIDs = append(delIDs, m.ID)
					} else {
						unackIDs = append(unackIDs, m.ID)
						c.unacked++
						c.noteUnacked(m.ID)
						s.deliveredBy[m.ID] = c
					}
					s.bumpStatLocked(ns, queue, "delivered", 1)
					s.delivered.Add(1)
					todo = append(todo, pair{c, m})
				}
				if len(unackIDs) > 0 {
					_, _ = s.db.Exec(`UPDATE mq_messages SET state='unacked' WHERE id IN (`+idIN(len(unackIDs))+`)`, idsArgs(unackIDs)...)
				}
				if len(delIDs) > 0 {
					_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE id IN (`+idIN(len(delIDs))+`)`, idsArgs(delIDs)...)
				}
				if c.Prefetch > 0 || len(batch) < mqDispatchBatch {
					break
				}
			}
		}
	}
	s.mu.Unlock()
	// Deliver 在锁外调用（连接写锁与 store 锁无锁序环；失败由断连重入队兜底）
	for _, p := range todo {
		p.c.Deliver(p.m)
	}
}

func qkeySplit(k string) (string, string) {
	if i := strings.IndexByte(k, 0); i >= 0 {
		return k[:i], k[i+1:]
	}
	return k, ""
}

// scanMsg 行扫描（v1.4.0：参数放宽为 Scan 接口，*sql.Row 与 *sql.Rows 通用）。
func scanMsg(row interface{ Scan(dest ...any) error }, ns, queue string) (*Message, error) {
	var m Message
	var pers, redel int
	err := row.Scan(&m.ID, &m.Exchange, &m.RoutingKey, &m.Props, &m.Payload, &pers, &redel)
	if err != nil {
		return nil, err
	}
	m.NS, m.Queue = ns, queue
	m.Persistent = pers != 0
	m.Redelivered = redel != 0
	return &m, nil
}

// ---------------------------------------------------------------------------
// 内存 ready 链 / ID 分配辅助（v1.4.0，async 模式）
// ---------------------------------------------------------------------------

// pushReadyLocked 消息入队尾（= id 递增 FIFO，等价 ORDER BY id；调用方持 s.mu）。
func (s *Store) pushReadyLocked(k string, m *Message) {
	l := s.ready[k]
	if l == nil {
		l = &msgList{}
		s.ready[k] = l
	}
	l.xs = append(l.xs, m)
}

// popReadyLocked 取队首（调用方持 s.mu）。
func (s *Store) popReadyLocked(ns, queue string) *Message {
	if l := s.ready[qkey(ns, queue)]; l != nil {
		return l.pop()
	}
	return nil
}

// readyCountLocked ready 链长度（调用方持 s.mu）。
func (s *Store) readyCountLocked(k string) int64 {
	if l := s.ready[k]; l != nil {
		return int64(len(l.xs) - l.head)
	}
	return 0
}

// requeueLocked 消息回插队首（旧 id 优先语义）+ unacked 计数回退（调用方持 s.mu）。
func (s *Store) requeueLocked(m *Message) {
	k := qkey(m.NS, m.Queue)
	l := s.ready[k]
	if l == nil {
		l = &msgList{}
		s.ready[k] = l
	}
	l.pushFront(m)
	m.state = msgStateReady
	m.Redelivered = true
	if s.unackedMsgs[k] > 0 {
		s.unackedMsgs[k]--
	}
}

// nextIDLocked 分配消息 ID（调用方持 s.mu；Open 播种 ≥ max(seq,max_id)，
// 显式 id 插入自动推进 sqlite_sequence）。
func (s *Store) nextIDLocked() int64 {
	s.idSeq++
	return s.idSeq
}

// ---------------------------------------------------------------------------
// 统计
// ---------------------------------------------------------------------------

// bumpStatLocked 队列统计记账（v1.4.0：内存缓冲 + statLoop 每 5s 批量 UPSERT，
// 替代原每消息一条 INSERT——发布/派发热路径零 SQL；调用方持 s.mu，statMu 为叶锁）。
func (s *Store) bumpStatLocked(ns, name, field string, n int64) {
	s.statMu.Lock()
	if s.statBuf == nil {
		s.statBuf = map[string]*statDelta{}
	}
	k := qkey(ns, name)
	d := s.statBuf[k]
	if d == nil {
		d = &statDelta{}
		s.statBuf[k] = d
	}
	switch field {
	case "published":
		d.pub += n
	case "delivered":
		d.del += n
	case "acked":
		d.ack += n
	}
	s.statMu.Unlock()
}

// Queues 队列列表（SHOW QUEUES / mq_queues）。
func (s *Store) Queues(ns string) []QueueInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := `SELECT ns,name,durable FROM mq_queues`
	var args []any
	if ns != "" {
		q += ` WHERE ns=?`
		args = append(args, ns)
	}
	q += ` ORDER BY ns,name`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []QueueInfo
	for rows.Next() {
		var info QueueInfo
		var dur int
		if rows.Scan(&info.NS, &info.Name, &dur) != nil {
			continue
		}
		info.Durable = dur != 0
		if s.async() {
			// v1.4.0：async 模式 ready/unacked 以内存为准（journal 未落盘部分 DB 缺行）
			k := qkey(info.NS, info.Name)
			info.Ready = s.readyCountLocked(k)
			info.Unacked = s.unackedMsgs[k]
		} else {
			_ = s.db.QueryRow(`SELECT COUNT(*) FROM mq_messages WHERE ns=? AND queue=? AND state='ready'`,
				info.NS, info.Name).Scan(&info.Ready)
			_ = s.db.QueryRow(`SELECT COUNT(*) FROM mq_messages WHERE ns=? AND queue=? AND state='unacked'`,
				info.NS, info.Name).Scan(&info.Unacked)
		}
		info.Consumers = len(s.consumers[qkey(info.NS, info.Name)])
		var pub, del, ack sql.NullInt64
		_ = s.db.QueryRow(`SELECT published,delivered,acked FROM mq_stats WHERE ns=? AND name=?`,
			info.NS, info.Name).Scan(&pub, &del, &ack)
		// v1.4.0：合并内存统计缓冲（statLoop 未落库的增量）
		s.statMu.Lock()
		if d := s.statBuf[qkey(info.NS, info.Name)]; d != nil {
			pub.Int64 += d.pub
			del.Int64 += d.del
			ack.Int64 += d.ack
		}
		s.statMu.Unlock()
		info.Published, info.Delivered, info.Acked = pub.Int64, del.Int64, ack.Int64
		out = append(out, info)
	}
	return out
}

// Exchanges 交换机列表（SHOW EXCHANGES / mq_exchanges）。
func (s *Store) Exchanges(ns string) []ExchangeInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := `SELECT ns,name,type,durable FROM mq_exchanges`
	var args []any
	if ns != "" {
		q += ` WHERE ns=?`
		args = append(args, ns)
	}
	q += ` ORDER BY ns,name`
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []ExchangeInfo
	for rows.Next() {
		var info ExchangeInfo
		var dur int
		if rows.Scan(&info.NS, &info.Name, &info.Type, &dur) != nil {
			continue
		}
		info.Durable = dur != 0
		_ = s.db.QueryRow(`SELECT COUNT(*) FROM mq_bindings WHERE ns=? AND exchange=?`,
			info.NS, info.Name).Scan(&info.Bindings)
		out = append(out, info)
	}
	return out
}

// Counters 总量统计。
func (s *Store) Counters() (pub, del, ack int64, uptime int64) {
	return s.published.Load(), s.delivered.Load(), s.acked.Load(), int64(time.Since(s.startTS).Seconds())
}

// ---------------------------------------------------------------------------
// 消费者未确认集
// ---------------------------------------------------------------------------

func (c *Consumer) noteUnacked(id int64) {
	if c.unackedSet == nil {
		c.unackedSet = map[int64]bool{}
	}
	c.unackedSet[id] = true
}

func (c *Consumer) unackedIDs() []int64 {
	out := make([]int64, 0, len(c.unackedSet))
	for id := range c.unackedSet {
		out = append(out, id)
	}
	return out
}

func (c *Consumer) dropUnacked(ids []int64) {
	for _, id := range ids {
		delete(c.unackedSet, id)
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// DefaultExchange 默认交换机名（routing key = 队列名）。
const DefaultExchange = ""

// ---------------------------------------------------------------------------
// MQTT 保留消息（v1.3.3）
// ---------------------------------------------------------------------------

// Retained 一条保留消息。
type Retained struct {
	Topic   string
	Payload []byte
	QoS     int
}

// RetainPut 更新/写入保留消息；payload 空 = 删除（MQTT 语义）。
func (s *Store) RetainPut(ns, topic, rk string, payload []byte, qos int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(payload) == 0 {
		_, err := s.db.Exec(`DELETE FROM mqtt_retains WHERE ns=? AND topic=?`, ns, topic)
		return err
	}
	_, err := s.db.Exec(`INSERT INTO mqtt_retains(ns,topic,rk,payload,qos,ts) VALUES(?,?,?,?,?,?)
		ON CONFLICT(ns,topic) DO UPDATE SET rk=excluded.rk, payload=excluded.payload, qos=excluded.qos, ts=excluded.ts`,
		ns, topic, rk, payload, qos, time.Now().Unix())
	return err
}

// RetainDrop 显式删除保留消息。
func (s *Store) RetainDrop(ns, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.db.Exec(`DELETE FROM mqtt_retains WHERE ns=? AND topic=?`, ns, topic)
}

// RetainAll 取全部保留消息（过滤交给调用方的 MQTT 通配匹配）。
func (s *Store) RetainAll(ns string) []Retained {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT topic,payload,qos FROM mqtt_retains WHERE ns=? ORDER BY topic`, ns)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []Retained
	for rows.Next() {
		var r Retained
		var q int
		if rows.Scan(&r.Topic, &r.Payload, &q) == nil {
			r.QoS = q
			out = append(out, r)
		}
	}
	return out
}

// RetainStats 保留消息统计（管理面 mqtt_retains 表）。
func (s *Store) RetainStats() [][]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`SELECT ns,topic,LENGTH(payload),qos,ts FROM mqtt_retains ORDER BY ns,topic`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		var ns, topic string
		var size, qos, ts int64
		if rows.Scan(&ns, &topic, &size, &qos, &ts) == nil {
			out = append(out, []any{ns, topic, size, int64(qos), int64(ts)})
		}
	}
	return out
}

// QueueCount 队列总数（心跳上报用，v1.3.13；v1.4.0 改读路由内存缓存，免 SQL）。
func (s *Store) QueueCount() int64 {
	s.routeMu.RLock()
	defer s.routeMu.RUnlock()
	return int64(len(s.rQueue))
}

// MessageCount 消息总数（ready + unacked，心跳上报用，v1.3.13）。
// v1.4.0：async 模式读内存（journal 未落盘部分 DB 缺行），同步模式照旧 COUNT。
func (s *Store) MessageCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.async() {
		var n int64
		for _, l := range s.ready {
			n += int64(len(l.xs) - l.head)
		}
		for _, c := range s.unackedMsgs {
			n += c
		}
		return n
	}
	var n int64
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM mq_messages`).Scan(&n)
	return n
}
