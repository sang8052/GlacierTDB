package mq

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
}

// Consumer 活动消费者（连接级，随连接销毁）。
type Consumer struct {
	Tag       string
	NS        string
	Queue     string
	NoAck     bool
	Prefetch  int // 0 = 不限
	Exclusive bool
	ConnID    uint64
	Deliver   func(m *Message) // 发送 Deliver + 内容（连接写锁由实现方持有）
	unacked   int
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
type Store struct {
	mu          sync.Mutex
	db          *sql.DB
	consumers   map[string][]*Consumer // ns|queue → 消费者
	deliveredBy map[int64]*Consumer    // message id → 接收消费者（ack 回补 prefetch）
	kick      chan struct{}
	maxMsg    int64

	published atomic.Int64
	delivered atomic.Int64
	acked     atomic.Int64
	startTS   time.Time
}

// Open 打开系统库、恢复（未确认消息重入队）并启动分发循环。
func Open(backend *sqlite.Backend, maxMsgMB int) (*Store, error) {
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
);`); err != nil {
		return nil, fmt.Errorf("init mq schema: %w", err)
	}
	// 重启恢复：未确认消息重入队（消费游标语义随状态机落盘）
	_, _ = db.Exec(`UPDATE mq_messages SET state='ready', redelivered=1 WHERE state='unacked'`)

	s := &Store{
		db:          db,
		consumers:   map[string][]*Consumer{},
		deliveredBy: map[int64]*Consumer{},
		kick:      make(chan struct{}, 1),
		maxMsg:    int64(maxMsgMB) << 20,
		startTS:   time.Now(),
	}
	go s.dispatchLoop()
	logx.Infof("mq: store ready (max_message_mb=%d)", maxMsgMB)
	return s, nil
}

func qkey(ns, queue string) string { return ns + "\x00" + queue }

// ---------------------------------------------------------------------------
// 声明 / 绑定 / 删除
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
	} else if !passive && (curDur != 0) != durable {
		return 0, 0, fmt.Errorf("PRECONDITION_FAILED - inequivalent arg 'durable' for queue '%s'", name)
	}
	ready, _ := s.countReady(ns, name)
	return ready, len(s.consumers[qkey(ns, name)]), nil
}

// Bind 队列绑定交换机。
func (s *Store) Bind(ns, exchange, queue, routingKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`INSERT OR IGNORE INTO mq_bindings(ns,exchange,queue,routing_key) VALUES(?,?,?,?)`,
		ns, exchange, queue, routingKey)
	return err
}

// Unbind 解绑。
func (s *Store) Unbind(ns, exchange, queue, routingKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(`DELETE FROM mq_bindings WHERE ns=? AND exchange=? AND queue=? AND routing_key=?`,
		ns, exchange, queue, routingKey)
	return err
}

// DeleteExchange 删除交换机（含绑定）。
func (s *Store) DeleteExchange(ns, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.db.Exec(`DELETE FROM mq_bindings WHERE ns=? AND exchange=?`, ns, name)
	_, err := s.db.Exec(`DELETE FROM mq_exchanges WHERE ns=? AND name=?`, ns, name)
	return err
}

// DeleteQueue 删除队列（含消息与绑定），返回删除前 ready 数。
func (s *Store) DeleteQueue(ns, name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, _ := s.countReady(ns, name)
	_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE ns=? AND queue=?`, ns, name)
	_, _ = s.db.Exec(`DELETE FROM mq_bindings WHERE ns=? AND queue=?`, ns, name)
	_, _ = s.db.Exec(`DELETE FROM mq_queues WHERE ns=? AND name=?`, ns, name)
	_, _ = s.db.Exec(`DELETE FROM mq_stats WHERE ns=? AND name=?`, ns, name)
	s.consumers[qkey(ns, name)] = nil
	return n, nil
}

// PurgeQueue 清空队列 ready 消息，返回清除数。
func (s *Store) PurgeQueue(ns, name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
func (s *Store) Publish(ns, exchange, routingKey string, props, payload []byte, persistent bool) error {
	if s.maxMsg > 0 && int64(len(payload)) > s.maxMsg {
		return fmt.Errorf("message size %d exceeds max_message_mb limit", len(payload))
	}
	s.mu.Lock()
	queues := s.routeLocked(ns, exchange, routingKey)
	for _, q := range queues {
		_, _ = s.db.Exec(`INSERT INTO mq_messages(ns,queue,exchange,routing_key,props,payload,persistent,redelivered,state,created)
			VALUES(?,?,?,?,?,?,?,0,'ready',?)`,
			ns, q, exchange, routingKey, props, payload, b2i(persistent), time.Now().Unix())
		s.bumpStatLocked(ns, q, "published", 1)
	}
	s.mu.Unlock()
	s.published.Add(1)
	s.nudge()
	return nil
}

// routeLocked 解析路由目标队列（调用方持锁）。
func (s *Store) routeLocked(ns, exchange, routingKey string) []string {
	if exchange == "" {
		// 默认交换机：routing key 即队列名
		var exists string
		if err := s.db.QueryRow(`SELECT name FROM mq_queues WHERE ns=? AND name=?`, ns, routingKey).Scan(&exists); err == nil {
			return []string{routingKey}
		}
		return nil
	}
	var typ string
	if err := s.db.QueryRow(`SELECT type FROM mq_exchanges WHERE ns=? AND name=?`, ns, exchange).Scan(&typ); err != nil {
		return nil
	}
	rows, err := s.db.Query(`SELECT queue,routing_key FROM mq_bindings WHERE ns=? AND exchange=?`, ns, exchange)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	seen := map[string]bool{}
	for rows.Next() {
		var q, rk string
		if rows.Scan(&q, &rk) != nil {
			continue
		}
		match := false
		switch typ {
		case "fanout":
			match = true
		case "topic":
			match = topicMatch(rk, routingKey)
		default: // direct
			match = rk == routingKey
		}
		if match && !seen[q] {
			seen[q] = true
			out = append(out, q)
		}
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
		for _, id := range c.unackedIDs() {
			_, _ = s.db.Exec(`UPDATE mq_messages SET state='ready', redelivered=1 WHERE id=? AND state='unacked'`, id)
			delete(s.deliveredBy, id)
		}
	}
	s.mu.Unlock()
	s.nudge()
}

// Ack 确认（删除消息；按 deliveredBy 回补消费者 prefetch 计数）。
func (s *Store) Ack(ns string, ids []int64) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
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

// Nack 否定确认：requeue=true 重入队（redelivered），否则丢弃。
func (s *Store) Nack(ns string, ids []int64, requeue bool) {
	if len(ids) == 0 {
		return
	}
	s.mu.Lock()
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
	t := time.NewTicker(50 * time.Millisecond)
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
			for c.Prefetch <= 0 || c.unacked < c.Prefetch {
				row := s.db.QueryRow(`SELECT id,exchange,routing_key,props,payload,persistent,redelivered
					FROM mq_messages WHERE ns=? AND queue=? AND state='ready' ORDER BY id LIMIT 1`, ns, queue)
				m, err := scanMsg(row, ns, queue)
				if err != nil {
					break
				}
				if c.NoAck {
					_, _ = s.db.Exec(`DELETE FROM mq_messages WHERE id=?`, m.ID)
				} else {
					_, _ = s.db.Exec(`UPDATE mq_messages SET state='unacked' WHERE id=?`, m.ID)
					c.unacked++
					c.noteUnacked(m.ID)
					s.deliveredBy[m.ID] = c
				}
				s.bumpStatLocked(ns, queue, "delivered", 1)
				s.delivered.Add(1)
				todo = append(todo, pair{c, m})
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

func scanMsg(row *sql.Row, ns, queue string) (*Message, error) {
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
// 统计
// ---------------------------------------------------------------------------

func (s *Store) bumpStatLocked(ns, name, field string, n int64) {
	col := map[string]string{"published": "published", "delivered": "delivered", "acked": "acked"}[field]
	if col == "" {
		return
	}
	_, _ = s.db.Exec(`INSERT INTO mq_stats(ns,name,published,delivered,acked,updated) VALUES(?,?,0,0,0,?)
		ON CONFLICT(ns,name) DO UPDATE SET `+col+`=`+col+`+?, updated=excluded.updated`,
		ns, name, time.Now().Unix(), n)
}

func (s *Store) bumpStatByIDLocked(ns string, id int64, field string, n int64) {
	var q string
	if err := s.db.QueryRow(`SELECT queue FROM mq_messages WHERE id=?`, id).Scan(&q); err == nil {
		s.bumpStatLocked(ns, q, field, n)
	}
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
		_ = s.db.QueryRow(`SELECT COUNT(*) FROM mq_messages WHERE ns=? AND queue=? AND state='ready'`,
			info.NS, info.Name).Scan(&info.Ready)
		_ = s.db.QueryRow(`SELECT COUNT(*) FROM mq_messages WHERE ns=? AND queue=? AND state='unacked'`,
			info.NS, info.Name).Scan(&info.Unacked)
		info.Consumers = len(s.consumers[qkey(info.NS, info.Name)])
		var pub, del, ack sql.NullInt64
		_ = s.db.QueryRow(`SELECT published,delivered,acked FROM mq_stats WHERE ns=? AND name=?`,
			info.NS, info.Name).Scan(&pub, &del, &ack)
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
