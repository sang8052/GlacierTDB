package mq

import (
	"strings"

	"glaciertdb/internal/logx"
)

// ---------------------------------------------------------------------------
// 路由内存缓存（v1.3.15）：Publish 热路径不再查库
//
// 原实现 routeLocked 每次发布都要执行 SELECT：
//   ① 默认交换机：SELECT name FROM mq_queues WHERE ns=? AND name=?（存在性判断）
//   ② 命名交换机：SELECT type FROM mq_exchanges WHERE ns=? AND name=?
//   ③ 绑定遍历：  SELECT queue,routing_key FROM mq_bindings WHERE ns=? AND exchange=?
// 本文件把 ①②③ 变成 map 查找；Open 时全量加载三表，声明/绑定/删除等写路径在
// 落库成功后同步维护缓存（写 SQL 保持不变，仅追加内存维护）。
//
// 等价性约定（内存实现 vs 原 SQL 逐条对应）：
//   ① → rQueue[qkey(ns,rk)] 命中即返回 [rk]（原 SQL 无 durable 过滤，缓存同样不过滤）；
//   ② → rExch[qkey(ns,exch)] 未命中返回 nil（含未声明交换机不可路由语义）；
//   ③ → rBind[qkey(ns,exch)] 遍历，fanout=恒真 / topic=topicMatch / 其余(direct)=rk 相等，
//       输出按队列名去重——原 SQL 走 (ns,exchange,queue,routing_key) 主键索引，
//       结果天然按 queue 升序，内存实现输出前 sort.Strings 保持同一确定性次序
//       （决定多目标队列消息落库/派发的先后，行为等价）。
//   注：现行 schema 的 mq_bindings 无 args 列、路由无 x-match/headers 分支，
//   故缓存不携带 args（与原实现严格等价，无能力增减）。
// 锁序：routeMu 为叶锁（s.mu → routeMu，绝不反向；routeMu 内不触碰 s.mu 域字段）。
// ---------------------------------------------------------------------------

// routeExchange 交换机缓存行（对应 mq_exchanges 的路由相关列）。
type routeExchange struct {
	typ        string
	durable    bool
	autoDelete bool
}

// routeQueue 队列缓存行（对应 mq_queues 的路由相关列；键含命名空间）。
type routeQueue struct {
	durable    bool
	exclusive  bool
	autoDelete bool
}

// routeBinding 绑定缓存行（对应 mq_bindings 一行：队列 + routing key）。
type routeBinding struct {
	queue string
	rk    string
}

// loadRoutes 全量加载路由缓存（Open 时调用，单线程；v1.3.15）。
func (s *Store) loadRoutes() error {
	s.routeMu.Lock()
	defer s.routeMu.Unlock()

	rows, err := s.db.Query(`SELECT ns,name,type,durable,auto_delete FROM mq_exchanges`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var ns, name, typ string
		var dur, ad int
		if err := rows.Scan(&ns, &name, &typ, &dur, &ad); err != nil {
			rows.Close()
			return err
		}
		s.rExch[qkey(ns, name)] = &routeExchange{typ: typ, durable: dur != 0, autoDelete: ad != 0}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = s.db.Query(`SELECT ns,name,durable,exclusive,auto_delete FROM mq_queues`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var ns, name string
		var dur, ex, ad int
		if err := rows.Scan(&ns, &name, &dur, &ex, &ad); err != nil {
			rows.Close()
			return err
		}
		s.rQueue[qkey(ns, name)] = &routeQueue{durable: dur != 0, exclusive: ex != 0, autoDelete: ad != 0}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	rows, err = s.db.Query(`SELECT ns,exchange,queue,routing_key FROM mq_bindings`)
	if err != nil {
		return err
	}
	nb := 0
	for rows.Next() {
		var ns, exch, queue, rk string
		if err := rows.Scan(&ns, &exch, &queue, &rk); err != nil {
			rows.Close()
			return err
		}
		k := qkey(ns, exch)
		s.rBind[k] = append(s.rBind[k], routeBinding{queue: queue, rk: rk})
		nb++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	logx.Infof("mq: route cache loaded (exchanges=%d queues=%d bindings=%d)", len(s.rExch), len(s.rQueue), nb)
	return nil
}

// setRouteExchangeLocked 缓存写入/自愈交换机（调用方持 s.mu；existed=true 时以 DB 行值自愈防漂移）。
func (s *Store) setRouteExchangeLocked(ns, name, typ string, durable, autoDelete bool) {
	s.routeMu.Lock()
	s.rExch[qkey(ns, name)] = &routeExchange{typ: typ, durable: durable, autoDelete: autoDelete}
	s.routeMu.Unlock()
}

// setRouteQueueLocked 缓存写入/自愈队列（调用方持 s.mu）。
func (s *Store) setRouteQueueLocked(ns, name string, durable, exclusive, autoDelete bool) {
	s.routeMu.Lock()
	s.rQueue[qkey(ns, name)] = &routeQueue{durable: durable, exclusive: exclusive, autoDelete: autoDelete}
	s.routeMu.Unlock()
}

// addRouteBindingLocked 追加绑定（调用方持 s.mu；幂等——同 queue+rk 不重复）。
func (s *Store) addRouteBindingLocked(ns, exchange, queue, rk string) {
	s.routeMu.Lock()
	k := qkey(ns, exchange)
	for _, b := range s.rBind[k] {
		if b.queue == queue && b.rk == rk {
			s.routeMu.Unlock()
			return
		}
	}
	s.rBind[k] = append(s.rBind[k], routeBinding{queue: queue, rk: rk})
	s.routeMu.Unlock()
}

// dropRouteBindingLocked 删除一条绑定（调用方持 s.mu）。
func (s *Store) dropRouteBindingLocked(ns, exchange, queue, rk string) {
	s.routeMu.Lock()
	k := qkey(ns, exchange)
	list := s.rBind[k]
	for i, b := range list {
		if b.queue == queue && b.rk == rk {
			s.rBind[k] = append(list[:i], list[i+1:]...)
			break
		}
	}
	s.routeMu.Unlock()
}

// dropRouteExchangeLocked 删除交换机及其全部绑定（调用方持 s.mu）。
func (s *Store) dropRouteExchangeLocked(ns, name string) {
	s.routeMu.Lock()
	delete(s.rExch, qkey(ns, name))
	delete(s.rBind, qkey(ns, name))
	s.routeMu.Unlock()
}

// dropRouteQueueLocked 删除队列并级联清理各交换机上指向它的绑定
// （等价原 SQL：DELETE FROM mq_bindings WHERE ns=? AND queue=?；调用方持 s.mu）。
func (s *Store) dropRouteQueueLocked(ns, name string) {
	s.routeMu.Lock()
	delete(s.rQueue, qkey(ns, name))
	prefix := ns + "\x00"
	for k, list := range s.rBind {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		kept := list[:0]
		for _, b := range list {
			if b.queue != name {
				kept = append(kept, b)
			}
		}
		if len(kept) == 0 {
			delete(s.rBind, k)
		} else {
			s.rBind[k] = kept
		}
	}
	s.routeMu.Unlock()
}
