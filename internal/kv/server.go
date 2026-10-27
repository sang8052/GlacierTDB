package kv

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// connCount 当前 RESP 连接数（对齐 MySQL max_connections 保护）。
var connCount atomic.Int64

// ---------------------------------------------------------------------------
// 发布订阅 hub（v1.3.5）：频道/模式订阅表，按命名空间隔离（与 key 一致）。
// ---------------------------------------------------------------------------

var (
	pubsubMu sync.Mutex
	chanSubs = map[string]map[*session]struct{}{} // ns\x00channel → 订阅连接
	patSubs  = map[string]map[*session]struct{}{} // ns\x00pattern → 订阅连接
)

func subKey(ns, name string) string { return ns + "\x00" + name }

func subAdd(tab map[string]map[*session]struct{}, key string, s *session) {
	pubsubMu.Lock()
	defer pubsubMu.Unlock()
	if tab[key] == nil {
		tab[key] = map[*session]struct{}{}
	}
	tab[key][s] = struct{}{}
}

func subRemove(tab map[string]map[*session]struct{}, key string, s *session) {
	pubsubMu.Lock()
	defer pubsubMu.Unlock()
	if m := tab[key]; m != nil {
		delete(m, s)
		if len(m) == 0 {
			delete(tab, key)
		}
	}
}

// session 一条 RESP 连接的执行状态（v1.3.5 重构：dispatch 可复用于 MULTI EXEC）。
type session struct {
	conn   net.Conn
	store  *Store
	auths  *auth.Store
	cfg    *config.Config
	ip     string
	remote string
	ns     string

	w   *RespWriter
	wmu sync.Mutex // 应答与订阅推送的写互斥（PUBLISH 来自其他连接 goroutine）

	subs int // 订阅数（channel + pattern，SUBSCRIBE 确认帧的计数）

	execBuf *bytes.Buffer // 非 nil = 正在 EXEC：推送帧改写入事务应答缓冲（保序）
}

// reply 执行一段应答写入并 flush（写互斥）。
func (s *session) reply(fn func()) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	fn()
	if err := s.w.Flush(); err != nil {
		logx.Debugf("kv: write to %s: %v", s.remote, err)
	}
}

// pushRaw 订阅推送：帧已组好，写互斥下原样发出（EXEC 期间写入事务缓冲保序）。
func (s *session) pushRaw(frame []byte) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.execBuf != nil {
		s.execBuf.Write(frame)
		return
	}
	if _, err := s.conn.Write(frame); err != nil {
		logx.Debugf("kv: push to %s: %v", s.remote, err)
	}
}

// pushFrame 组一个 bulk 数组帧（订阅确认/消息推送共用）并发出。
func (s *session) pushFrame(parts ...[]byte) {
	var buf bytes.Buffer
	bw := NewRespWriter(&buf)
	bw.BulkArray(parts)
	_ = bw.Flush()
	s.pushRaw(buf.Bytes())
}

// pushSubConfirm 订阅确认帧：*3 [kind, name, :count]（count 为 integer，Redis 形态）。
func (s *session) pushSubConfirm(kind, name string, count int) {
	var buf bytes.Buffer
	buf.WriteString("*3\r\n")
	bw := NewRespWriter(&buf)
	bw.BulkStr(kind)
	bw.BulkStr(name)
	bw.Int(int64(count))
	_ = bw.Flush()
	s.pushRaw(buf.Bytes())
}

// unsubscribeAll 连接断开时注销全部订阅。
func (s *session) unsubscribeAll() {
	pubsubMu.Lock()
	var keys []string
	for k := range chanSubs {
		keys = append(keys, k)
	}
	for _, k := range keys {
		if m := chanSubs[k]; m != nil {
			delete(m, s)
			if len(m) == 0 {
				delete(chanSubs, k)
			}
		}
	}
	keys = keys[:0]
	for k := range patSubs {
		keys = append(keys, k)
	}
	for _, k := range keys {
		if m := patSubs[k]; m != nil {
			delete(m, s)
			if len(m) == 0 {
				delete(patSubs, k)
			}
		}
	}
	pubsubMu.Unlock()
	s.subs = 0
}

// ServeConn 服务一个 RESP 连接（mode:redis 声明或嗅探进入）。
// 认证沿用统一账号体系（AUTH <password> / AUTH <user> <password>），
// 登录审计与防爆破计数与 MySQL 侧一致；命名空间 = 登录账号。
// v1.3.5：MULTI/EXEC 事务 + SUBSCRIBE/PSUBSCRIBE/PUBLISH 发布订阅。
func ServeConn(c net.Conn, store *Store, auths *auth.Store, cfg *config.Config) {
	defer c.Close()
	if cfg.MaxConnections > 0 {
		if connCount.Load() >= int64(cfg.MaxConnections) {
			return // 超限静默断开（与 MySQL 侧行为一致）
		}
	}
	connCount.Add(1)
	defer connCount.Add(-1)

	remote := c.RemoteAddr().String()
	ip := remote
	if h, _, err := net.SplitHostPort(remote); err == nil {
		ip = h
	}
	logx.Debugf("kv: connection from %s", remote)

	s := &session{
		conn: c, store: store, auths: auths, cfg: cfg,
		ip: ip, remote: remote, w: NewRespWriter(c),
	}
	defer s.unsubscribeAll()

	// MULTI 入队状态（v1.3.5）
	inMulti := false
	var queue [][]string

	r := NewRespReader(c)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			if err != io.EOF {
				logx.Debugf("kv: protocol from %s: %v", remote, err)
			}
			return
		}
		if len(args) == 0 {
			continue
		}
		cmd := strings.ToUpper(args[0])
		rest := args[1:]

		// 认证门（AUTH/PING/ECHO/QUIT/COMMAND/HELLO 除外）
		if s.ns == "" && !preAuthOK(cmd) {
			s.reply(func() { s.w.Error("NOAUTH Authentication required.") })
			continue
		}

		// MULTI 入队（v1.3.5）：除控制命令外一律入队应答 QUEUED
		if inMulti && cmd != "MULTI" && cmd != "EXEC" && cmd != "DISCARD" && cmd != "QUIT" {
			queue = append(queue, args)
			s.reply(func() { s.w.Simple("QUEUED") })
			continue
		}
		switch cmd {
		case "MULTI":
			if inMulti {
				s.reply(func() { s.w.Error("ERR MULTI calls can not be nested") })
				continue
			}
			inMulti = true
			queue = queue[:0]
			s.reply(func() { s.w.Simple("OK") })
		case "EXEC":
			if !inMulti {
				s.reply(func() { s.w.Error("ERR EXEC without MULTI") })
				continue
			}
			batch := queue
			queue = nil
			inMulti = false
			// 逐条执行：应答帧收集进缓冲，外包 *N 数组（数组元素可为任意完整帧）
			var out bytes.Buffer
			out.WriteString(fmt.Sprintf("*%d\r\n", len(batch)))
			savedW := s.w
			savedExec := s.execBuf
			s.execBuf = &out // 事务期间推送帧也进缓冲（数组元素可为任意帧）
			s.w = NewRespWriter(&out)
			quitAfter := false
			for _, qa := range batch {
				if len(qa) == 0 {
					continue
				}
				if s.dispatch(strings.ToUpper(qa[0]), qa[1:]) {
					quitAfter = true
					break
				}
			}
			s.w = savedW
			s.execBuf = savedExec
			s.wmu.Lock()
			_, _ = c.Write(out.Bytes())
			_ = s.w.Flush()
			s.wmu.Unlock()
			if quitAfter {
				return
			}
		case "DISCARD":
			if !inMulti {
				s.reply(func() { s.w.Error("ERR DISCARD without MULTI") })
				continue
			}
			inMulti = false
			queue = nil
			s.reply(func() { s.w.Simple("OK") })
		case "QUIT":
			s.reply(func() { s.w.Simple("OK") })
			return
		default:
			if s.dispatch(cmd, rest) {
				return
			}
		}
	}
}

// dispatch 单命令执行（EXEC 逐条复用）；返回 true 表示连接应关闭（QUIT）。
func (s *session) dispatch(cmd string, rest []string) bool {
	w := s.w
	ns := s.ns
	store := s.store
	ip, remote := s.ip, s.remote
	reply := s.reply

	switch cmd {
	case "AUTH":
		user, ok := doAuth(rest, ip, remote, s.auths)
		if ok {
			s.ns = user
			reply(func() { w.Simple("OK") })
		} else {
			reply(func() { w.Error("WRONGPASS invalid username-password pair") })
		}

	case "PING":
		if len(rest) > 0 {
			reply(func() { w.BulkStr(rest[0]) })
		} else {
			reply(func() { w.Simple("PONG") })
		}

	case "ECHO":
		if len(rest) != 1 {
			reply(func() { w.Error("ERR wrong number of arguments for 'echo' command") })
		} else {
			reply(func() { w.BulkStr(rest[0]) })
		}

	case "COMMAND":
		reply(func() { w.BulkArray(nil) })

	case "SELECT":
		// 命名空间即隔离（无 db 编号语义）；仅接受 0
		if len(rest) == 1 && rest[0] == "0" {
			reply(func() { w.Simple("OK") })
		} else {
			reply(func() { w.Error("ERR DB index is out of range") })
		}

	case "INFO":
		reply(func() {
			w.BulkStr(fmt.Sprintf("# Server\r\nredis_version:glaciertdb-kv\r\nuptime_in_seconds:%d\r\n"+
				"# Memory\r\nused_memory:%d\r\nmaxmemory:%d\r\nmaxmemory_policy:%s\r\n"+
				"# Stats\r\nops:%d\r\n", store.UptimeSec(), store.MemBytes(),
				int64(store.MaxMemoryMB())<<20, store.Eviction(), store.OpsTotal()))
		})

	case "HELLO":
		// HELLO [protover [AUTH user pass] [SETNAME name]]（BUG.txt ①，v1.3.3）：
		// 最小应答——server map + proto=2（RESP2 only，客户端据此自动降级）；
		// redis-py ≥8 默认发 HELLO 3 AUTH default <pwd>，收到 map 即继续（proto 校验已被
		// redis-py 注释，RESP3 解析器对 RESP2 基础类型帧兼容）。
		protover, hasAuth, auser, apass, badArg := parseHelloArgs(rest)
		if badArg {
			reply(func() { w.Error("ERR syntax error in HELLO") })
			return false
		}
		if protover != "" && protover != "2" && protover != "3" {
			reply(func() { w.Error("NOPROTO unsupported protocol version") })
			return false
		}
		// RESP3 协商（v1.3.4，反馈①-补）：HELLO 3 的客户端解析器为 RESP3——
		// 空值帧改发 `_`（RESP3 null），防 `$-1`/`*-1` 被按「读 -1 字节」挂死；
		// 协议本身仍 RESP2（HELLO 应答 proto 恒 2，非空值帧两种解析器兼容）。
		w.Resp3 = protover == "3"
		if hasAuth {
			// HELLO 内嵌认证：default 走单密码路径，其余按账号
			args := []string{auser, apass}
			if auser == "default" {
				args = []string{apass}
			}
			if u, ok := doAuth(args, ip, remote, s.auths); ok {
				s.ns = u
			} else {
				reply(func() { w.Error("WRONGPASS invalid username-password pair") })
				return false
			}
		}
		id := atomic.AddUint64(&helloConnSeq, 1)
		reply(func() {
			w.BulkArray([][]byte{
				[]byte("server"), []byte("redis"),
				[]byte("version"), []byte(config.GlacierVersion),
				[]byte("proto"), []byte("2"),
				[]byte("id"), []byte(fmt.Sprintf("%d", id)),
				[]byte("mode"), []byte("standalone"),
				[]byte("role"), []byte("master"),
				[]byte("modules"), {},
			})
		})

	case "SCAN":
		// SCAN cursor [MATCH pattern] [COUNT n] [TYPE t]（v1.3.5 真游标）：
		// 排序快照 + 索引游标，每批 COUNT（默认 10）个匹配 key，遍历完回 0；
		// 遍历期间的写入可能漏见/重见（Redis 游标语义允许）。
		if len(rest) < 1 {
			reply(func() { w.Error("ERR wrong number of arguments for 'scan' command") })
			return false
		}
		cursor, err := strconv.ParseUint(strings.TrimSpace(rest[0]), 10, 64)
		if err != nil {
			reply(func() { w.Error("ERR invalid cursor") })
			return false
		}
		pattern, typ := "*", ""
		count := int64(10)
		bad := false
		for i := 1; i < len(rest); i++ {
			switch strings.ToUpper(rest[i]) {
			case "MATCH":
				if i+1 >= len(rest) {
					bad = true
				} else {
					pattern = rest[i+1]
					i++
				}
			case "COUNT":
				if i+1 >= len(rest) {
					bad = true
				} else {
					n, perr := strconv.ParseInt(rest[i+1], 10, 64)
					if perr != nil || n <= 0 {
						bad = true
					} else {
						if n > 10000 {
							n = 10000
						}
						count = n
					}
					i++
				}
			case "TYPE":
				if i+1 >= len(rest) {
					bad = true
				} else {
					typ = strings.ToLower(rest[i+1])
					i++
				}
			default:
				bad = true
			}
			if bad {
				break
			}
		}
		if bad {
			reply(func() { w.Error("ERR syntax error") })
			return false
		}
		keys := store.Keys(ns, pattern)
		if typ != "" {
			var filtered []string
			for _, k := range keys {
				if store.Type(ns, k) == typ {
					filtered = append(filtered, k)
				}
			}
			keys = filtered
		}
		sort.Strings(keys)
		if cursor > uint64(len(keys)) {
			cursor = uint64(len(keys))
		}
		end := cursor + uint64(count)
		if end > uint64(len(keys)) {
			end = uint64(len(keys))
		}
		batch := keys[cursor:end]
		next := uint64(0)
		if end < uint64(len(keys)) {
			next = end
		}
		keyBytes := make([][]byte, 0, len(batch))
		for _, k := range batch {
			keyBytes = append(keyBytes, []byte(k))
		}
		reply(func() { w.ScanReply(strconv.FormatUint(next, 10), keyBytes) })

	case "GET":
		if len(rest) != 1 {
			reply(func() { w.Error("ERR wrong number of arguments for 'get' command") })
			return false
		}
		v, ok, gerr := store.Get(ns, rest[0])
		reply(func() {
			if gerr != nil {
				w.Error(gerr.Error())
			} else if !ok {
				w.Bulk(nil)
			} else {
				w.Bulk(v)
			}
		})

	case "SET":
		if len(rest) < 2 {
			reply(func() { w.Error("ERR wrong number of arguments for 'set' command") })
			return false
		}
		key, val := rest[0], rest[1]
		var ttlMs int64
		mode := byte(' ')
		bad := false
		for i := 2; i < len(rest); i++ {
			switch strings.ToUpper(rest[i]) {
			case "EX":
				if i+1 >= len(rest) {
					bad = true
					break
				}
				n, perr := strconvParseInt(rest[i+1])
				if perr != nil || n <= 0 {
					bad = true
					break
				}
				ttlMs = n * 1000
				i++
			case "PX":
				if i+1 >= len(rest) {
					bad = true
					break
				}
				n, perr := strconvParseInt(rest[i+1])
				if perr != nil || n <= 0 {
					bad = true
					break
				}
				ttlMs = n
				i++
			case "NX":
				mode = 'n'
			case "XX":
				mode = 'x'
			default:
				bad = true
			}
			if bad {
				break
			}
		}
		if bad {
			reply(func() { w.Error("ERR syntax error") })
			return false
		}
		applied, serr := store.Set(ns, key, []byte(val), ttlMs, mode)
		reply(func() {
			if serr != nil {
				w.Error(serr.Error())
			} else if !applied {
				w.Bulk(nil)
			} else {
				w.Simple("OK")
			}
		})

	case "DEL":
		reply(func() { w.Int(store.Del(ns, rest...)) })

	case "UNLINK":
		reply(func() { w.Int(store.Del(ns, rest...)) })

	case "EXISTS":
		reply(func() { w.Int(store.Exists(ns, rest...)) })

	case "EXPIRE", "PEXPIRE":
		if len(rest) != 2 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		n, perr := strconvParseInt(rest[1])
		if perr != nil {
			reply(func() { w.Error("ERR value is not an integer or out of range") })
			return false
		}
		if cmd == "EXPIRE" {
			n *= 1000
		}
		ok, _ := store.SetTTL(ns, rest[0], n)
		reply(func() {
			if ok {
				w.Int(1)
			} else {
				w.Int(0)
			}
		})

	case "PERSIST":
		reply(func() {
			if store.Persist(ns, rest[0]) {
				w.Int(1)
			} else {
				w.Int(0)
			}
		})

	case "TTL", "PTTL":
		if len(rest) != 1 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		ms := store.TTL(ns, rest[0])
		reply(func() {
			if cmd == "TTL" && ms > 0 {
				w.Int(ms / 1000)
			} else {
				w.Int(ms)
			}
		})

	case "TYPE":
		if len(rest) != 1 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		reply(func() { w.Simple(store.Type(ns, rest[0])) })

	case "INCR", "DECR", "INCRBY", "DECRBY":
		if len(rest) < 1 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		delta := int64(1)
		switch cmd {
		case "DECR":
			delta = -1
		case "INCRBY", "DECRBY":
			if len(rest) != 2 {
				reply(func() { w.Error("ERR wrong number of arguments") })
				return false
			}
			n, perr := strconvParseInt(rest[1])
			if perr != nil {
				reply(func() { w.Error("ERR value is not an integer or out of range") })
				return false
			}
			delta = n
			if cmd == "DECRBY" {
				delta = -delta
			}
		}
		nv, ierr := store.IncrBy(ns, rest[0], delta)
		reply(func() {
			if ierr != nil {
				w.Error(ierr.Error())
			} else {
				w.Int(nv)
			}
		})

	case "APPEND":
		if len(rest) != 2 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		n, aerr := store.Append(ns, rest[0], []byte(rest[1]))
		reply(func() {
			if aerr != nil {
				w.Error(aerr.Error())
			} else {
				w.Int(n)
			}
		})

	case "MGET":
		if len(rest) < 1 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		reply(func() { w.BulkArray(store.MGet(ns, rest)) })

	case "MSET":
		if len(rest) < 2 || len(rest)%2 != 0 {
			reply(func() { w.Error("ERR wrong number of arguments for 'mset' command") })
			return false
		}
		var kvs [][2][]byte
		for i := 0; i+1 < len(rest); i += 2 {
			kvs = append(kvs, [2][]byte{[]byte(rest[i]), []byte(rest[i+1])})
		}
		store.MSet(ns, kvs)
		reply(func() { w.Simple("OK") })

	case "KEYS":
		if len(rest) != 1 {
			reply(func() { w.Error("ERR wrong number of arguments for 'keys' command") })
			return false
		}
		keys := store.Keys(ns, rest[0])
		items := make([][]byte, 0, len(keys))
		for _, k := range keys {
			items = append(items, []byte(k))
		}
		reply(func() { w.BulkArray(items) })

	case "LPUSH", "RPUSH":
		if len(rest) < 2 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		vals := make([][]byte, 0, len(rest)-1)
		for _, v := range rest[1:] {
			vals = append(vals, []byte(v))
		}
		n, perr := store.Push(ns, rest[0], vals, cmd == "LPUSH")
		reply(func() {
			if perr != nil {
				w.Error(perr.Error())
			} else {
				w.Int(n)
			}
		})

	case "LPOP", "RPOP":
		if len(rest) < 1 || len(rest) > 2 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		count := 0
		if len(rest) == 2 {
			n, perr := strconvParseInt(rest[1])
			if perr != nil || n <= 0 {
				reply(func() { w.Error("ERR value is not an integer or out of range") })
				return false
			}
			count = int(n)
		}
		items, perr := store.Pop(ns, rest[0], cmd == "LPOP", count)
		reply(func() {
			if perr != nil {
				w.Error(perr.Error())
				return
			}
			if count > 0 {
				if items == nil {
					w.Bulk(nil)
				} else {
					w.BulkArray(items)
				}
				return
			}
			if len(items) == 0 {
				w.Bulk(nil)
			} else {
				w.Bulk(items[0])
			}
		})

	case "LLEN":
		if len(rest) != 1 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		n, lerr := store.LLen(ns, rest[0])
		reply(func() {
			if lerr != nil {
				w.Error(lerr.Error())
			} else {
				w.Int(n)
			}
		})

	case "LRANGE":
		if len(rest) != 3 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		start, err1 := strconvParseInt(rest[1])
		stop, err2 := strconvParseInt(rest[2])
		if err1 != nil || err2 != nil {
			reply(func() { w.Error("ERR value is not an integer or out of range") })
			return false
		}
		items, lerr := store.LRange(ns, rest[0], start, stop)
		reply(func() {
			if lerr != nil {
				w.Error(lerr.Error())
			} else {
				w.BulkArray(items)
			}
		})

	case "BLPOP", "BRPOP":
		// BLPOP key [key ...] timeout（v1.3.2 阻塞弹出：超时返回空数组，Push 唤醒）
		if len(rest) < 2 {
			reply(func() { w.Error("ERR wrong number of arguments") })
			return false
		}
		keys := rest[:len(rest)-1]
		secs, ferr := strconv.ParseFloat(strings.TrimSpace(rest[len(rest)-1]), 64)
		if ferr != nil {
			reply(func() { w.Error("ERR timeout is not a float or out of range") })
			return false
		}
		if secs < 0 {
			reply(func() { w.Error("ERR timeout is negative") })
			return false
		}
		var d time.Duration
		if secs > 0 {
			d = time.Duration(secs * float64(time.Second))
		}
		key, val, ok, perr := store.BlockPop(ns, keys, cmd == "BLPOP", d)
		reply(func() {
			if perr != nil {
				w.Error(perr.Error())
				return
			}
			if !ok {
				w.NullArray() // 超时（RESP2 *-1 / RESP3 _）
				return
			}
			w.BulkArray([][]byte{[]byte(key), val})
		})

	// ---- 发布订阅（v1.3.5）----

	case "SUBSCRIBE":
		if len(rest) < 1 {
			reply(func() { w.Error("ERR wrong number of arguments for 'subscribe' command") })
			return false
		}
		for _, ch := range rest {
			subAdd(chanSubs, subKey(ns, ch), s)
			s.subs++
			s.pushSubConfirm("subscribe", ch, s.subs)
		}

	case "UNSUBSCRIBE":
		if len(rest) == 0 {
			// 全部退订
			pubsubMu.Lock()
			var chs []string
			for k := range chanSubs {
				if strings.HasPrefix(k, ns+"\x00") {
					if _, ok := chanSubs[k][s]; ok {
						chs = append(chs, strings.TrimPrefix(k, ns+"\x00"))
					}
				}
			}
			pubsubMu.Unlock()
			for _, ch := range chs {
				subRemove(chanSubs, subKey(ns, ch), s)
				s.subs--
				s.pushSubConfirm("unsubscribe", ch, s.subs)
			}
			return false
		}
		for _, ch := range rest {
			subRemove(chanSubs, subKey(ns, ch), s)
			if s.subs > 0 {
				s.subs--
			}
			s.pushSubConfirm("unsubscribe", ch, s.subs)
		}

	case "PSUBSCRIBE":
		if len(rest) < 1 {
			reply(func() { w.Error("ERR wrong number of arguments for 'psubscribe' command") })
			return false
		}
		for _, pat := range rest {
			subAdd(patSubs, subKey(ns, pat), s)
			s.subs++
			s.pushSubConfirm("psubscribe", pat, s.subs)
		}

	case "PUNSUBSCRIBE":
		if len(rest) == 0 {
			pubsubMu.Lock()
			var pats []string
			for k := range patSubs {
				if strings.HasPrefix(k, ns+"\x00") {
					if _, ok := patSubs[k][s]; ok {
						pats = append(pats, strings.TrimPrefix(k, ns+"\x00"))
					}
				}
			}
			pubsubMu.Unlock()
			for _, pat := range pats {
				subRemove(patSubs, subKey(ns, pat), s)
				s.subs--
				s.pushSubConfirm("punsubscribe", pat, s.subs)
			}
			return false
		}
		for _, pat := range rest {
			subRemove(patSubs, subKey(ns, pat), s)
			if s.subs > 0 {
				s.subs--
			}
			s.pushSubConfirm("punsubscribe", pat, s.subs)
		}

	case "PUBLISH":
		if len(rest) != 2 {
			reply(func() { w.Error("ERR wrong number of arguments for 'publish' command") })
			return false
		}
		channel, payload := rest[0], []byte(rest[1])
		type patTarget struct {
			sub *session
			pat string
		}
		pubsubMu.Lock()
		var chanTargets []*session
		var patTargets []patTarget
		seen := map[*session]struct{}{}
		if m := chanSubs[subKey(ns, channel)]; m != nil {
			for sub := range m {
				chanTargets = append(chanTargets, sub)
				seen[sub] = struct{}{}
			}
		}
		for k, m := range patSubs {
			if !strings.HasPrefix(k, ns+"\x00") {
				continue
			}
			pat := strings.TrimPrefix(k, ns+"\x00")
			if !globMatch(pat, channel) {
				continue
			}
			for sub := range m {
				patTargets = append(patTargets, patTarget{sub, pat})
				seen[sub] = struct{}{}
			}
		}
		pubsubMu.Unlock()
		// channel 订阅者收 message；pattern 订阅者收 pmessage（同连接双收，Redis 语义）
		for _, sub := range chanTargets {
			sub.pushFrame([]byte("message"), []byte(channel), payload)
		}
		for _, t := range patTargets {
			t.sub.pushFrame([]byte("pmessage"), []byte(t.pat), []byte(channel), payload)
		}
		reply(func() { w.Int(int64(len(seen))) }) // 接收连接数（去重）

	case "WATCH":
		reply(func() { w.Error("ERR WATCH is not supported (transactions are queue-and-execute only)") })

	case "FLUSHDB", "FLUSHALL":
		store.Flush(ns)
		reply(func() { w.Simple("OK") })

	default:
		reply(func() { w.Error(fmt.Sprintf("ERR unknown command '%s'", strings.ToLower(cmd))) })
	}
	return false
}

// preAuthOK 认证前允许的命令。
func preAuthOK(cmd string) bool {
	switch cmd {
	case "AUTH", "PING", "ECHO", "QUIT", "COMMAND", "HELLO":
		return true
	}
	return false
}

// helloConnSeq HELLO 应答的连接序号（server map 的 id 字段）。
var helloConnSeq uint64

// doAuth 认证（统一账号体系：密码比对 + host 授权 + 锁定检查 + 登录审计/防爆破）。
// 返回命名空间（账号名）。
func doAuth(rest []string, ip, remote string, auths *auth.Store) (string, bool) {
	var user, pass string
	switch len(rest) {
	case 1:
		pass = rest[0]
		if auths == nil {
			return "", false
		}
		for _, u := range auths.Users() {
			if u.Password == pass && u.Password != "" {
				user = u.Username
				break
			}
		}
		if user == "" {
			auths.RecordLogin(ip, remote, "", false, "kv: wrong password")
			return "", false
		}
	case 2:
		user, pass = rest[0], rest[1]
	default:
		return "", false
	}
	if auths == nil {
		return "", false
	}
	u, ok, err := auths.Lookup(user)
	if err != nil || !ok || u.Password != pass {
		auths.RecordLogin(ip, remote, user, false, "kv: wrong password")
		return "", false
	}
	if locked, _ := auths.IPLocked(ip); locked {
		return "", false
	}
	if locked, _ := auths.AccountLocked(user); locked {
		return "", false
	}
	if ok, _ := auths.HostOK(user, ip); !ok {
		auths.RecordLogin(ip, remote, user, false, "kv: host not allowed")
		return "", false
	}
	auths.RecordLogin(ip, remote, user, true, "kv")
	return user, true
}

// parseHelloArgs 解析 HELLO 参数：[protover] [AUTH user pass] [SETNAME name]。
// 返回（协议版本（可空）, 是否携带 AUTH, user, pass, 语法是否错误）。
func parseHelloArgs(rest []string) (protover string, hasAuth bool, user, pass string, bad bool) {
	i := 0
	if i < len(rest) && !strings.EqualFold(rest[i], "AUTH") && !strings.EqualFold(rest[i], "SETNAME") {
		protover = rest[i]
		i++
	}
	for i < len(rest) {
		switch strings.ToUpper(rest[i]) {
		case "AUTH":
			if i+2 >= len(rest) {
				return "", false, "", "", true
			}
			hasAuth, user, pass = true, rest[i+1], rest[i+2]
			i += 3
		case "SETNAME":
			if i+1 >= len(rest) {
				return "", false, "", "", true
			}
			i += 2 // 连接名接受忽略
		default:
			return "", false, "", "", true
		}
	}
	return protover, hasAuth, user, pass, false
}

// strconvParseInt 整数解析（快捷）。
func strconvParseInt(s string) (int64, error) {
	var n int64
	var neg bool
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	}
	if s == "" {
		return 0, fmt.Errorf("not an integer")
	}
	for _, c := range []byte(s) {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not an integer")
		}
		n = n*10 + int64(c-'0')
		if n > 1<<62 {
			return 0, fmt.Errorf("out of range")
		}
	}
	if neg {
		n = -n
	}
	return n, nil
}
