package kv

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"

	"glaciertdb/internal/auth"
	"glaciertdb/internal/config"
	"glaciertdb/internal/logx"
)

// connCount 当前 RESP 连接数（对齐 MySQL max_connections 保护）。
var connCount atomic.Int64

// ServeConn 服务一个 RESP 连接（mode:redis 声明或嗅探进入）。
// 认证沿用统一账号体系（AUTH <password> / AUTH <user> <password>），
// 登录审计与防爆破计数与 MySQL 侧一致；命名空间 = 登录账号。
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

	r := NewRespReader(c)
	w := NewRespWriter(c)
	ns := "" // 认证后 = 账号命名空间

	reply := func(fn func()) {
		fn()
		if err := w.Flush(); err != nil {
			logx.Debugf("kv: write to %s: %v", remote, err)
		}
	}

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
		if ns == "" && !preAuthOK(cmd) {
			reply(func() { w.Error("NOAUTH Authentication required.") })
			continue
		}

		switch cmd {
		case "AUTH":
			user, ok := doAuth(rest, ip, remote, auths)
			if ok {
				ns = user
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

		case "QUIT":
			reply(func() { w.Simple("OK") })
			return

		case "COMMAND":
			reply(func() { w.BulkArray(nil) })

		case "SELECT":
			// 命名空间即隔离（无 db 编号语义）；仅接受 0
			if len(rest) == 1 && rest[0] == "0" {
				reply(func() { w.Simple("OK") })
			} else {
				reply(func() { w.Error("ERR DB index is out of range") })
			}

		case "HELLO":
			reply(func() { w.Error("ERR unknown command 'HELLO' (RESP2 only; disable HELLO/RESP3 on client)") })

		case "INFO":
			reply(func() {
				w.BulkStr(fmt.Sprintf("# Server\r\nredis_version:glaciertdb-kv\r\nuptime_in_seconds:%d\r\n"+
					"# Memory\r\nused_memory:%d\r\nmaxmemory:%d\r\nmaxmemory_policy:%s\r\n"+
					"# Stats\r\nops:%d\r\n", store.UptimeSec(), store.MemBytes(),
					int64(store.MaxMemoryMB())<<20, store.Eviction(), store.OpsTotal()))
			})

		case "GET":
			if len(rest) != 1 {
				reply(func() { w.Error("ERR wrong number of arguments for 'get' command") })
				continue
			}
			v, ok, err := store.Get(ns, rest[0])
			reply(func() {
				if err != nil {
					w.Error(err.Error())
				} else if !ok {
					w.Bulk(nil)
				} else {
					w.Bulk(v)
				}
			})

		case "SET":
			if len(rest) < 2 {
				reply(func() { w.Error("ERR wrong number of arguments for 'set' command") })
				continue
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
					n, err := strconvParseInt(rest[i+1])
					if err != nil || n <= 0 {
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
					n, err := strconvParseInt(rest[i+1])
					if err != nil || n <= 0 {
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
				continue
			}
			applied, err := store.Set(ns, key, []byte(val), ttlMs, mode)
			reply(func() {
				if err != nil {
					w.Error(err.Error())
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
				continue
			}
			n, err := strconvParseInt(rest[1])
			if err != nil {
				reply(func() { w.Error("ERR value is not an integer or out of range") })
				continue
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
				continue
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
				continue
			}
			reply(func() { w.Simple(store.Type(ns, rest[0])) })

		case "INCR", "DECR", "INCRBY", "DECRBY":
			if len(rest) < 1 {
				reply(func() { w.Error("ERR wrong number of arguments") })
				continue
			}
			delta := int64(1)
			switch cmd {
			case "DECR":
				delta = -1
			case "INCRBY", "DECRBY":
				if len(rest) != 2 {
					reply(func() { w.Error("ERR wrong number of arguments") })
					continue
				}
				n, err := strconvParseInt(rest[1])
				if err != nil {
					reply(func() { w.Error("ERR value is not an integer or out of range") })
					continue
				}
				delta = n
				if cmd == "DECRBY" {
					delta = -delta
				}
			}
			nv, err := store.IncrBy(ns, rest[0], delta)
			reply(func() {
				if err != nil {
					w.Error(err.Error())
				} else {
					w.Int(nv)
				}
			})

		case "APPEND":
			if len(rest) != 2 {
				reply(func() { w.Error("ERR wrong number of arguments") })
				continue
			}
			n, err := store.Append(ns, rest[0], []byte(rest[1]))
			reply(func() {
				if err != nil {
					w.Error(err.Error())
				} else {
					w.Int(n)
				}
			})

		case "MGET":
			if len(rest) < 1 {
				reply(func() { w.Error("ERR wrong number of arguments") })
				continue
			}
			reply(func() { w.BulkArray(store.MGet(ns, rest)) })

		case "MSET":
			if len(rest) < 2 || len(rest)%2 != 0 {
				reply(func() { w.Error("ERR wrong number of arguments for 'mset' command") })
				continue
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
				continue
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
				continue
			}
			vals := make([][]byte, 0, len(rest)-1)
			for _, v := range rest[1:] {
				vals = append(vals, []byte(v))
			}
			n, err := store.Push(ns, rest[0], vals, cmd == "LPUSH")
			reply(func() {
				if err != nil {
					w.Error(err.Error())
				} else {
					w.Int(n)
				}
			})

		case "LPOP", "RPOP":
			if len(rest) < 1 || len(rest) > 2 {
				reply(func() { w.Error("ERR wrong number of arguments") })
				continue
			}
			count := 0
			if len(rest) == 2 {
				n, err := strconvParseInt(rest[1])
				if err != nil || n <= 0 {
					reply(func() { w.Error("ERR value is not an integer or out of range") })
					continue
				}
				count = int(n)
			}
			items, err := store.Pop(ns, rest[0], cmd == "LPOP", count)
			reply(func() {
				if err != nil {
					w.Error(err.Error())
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
				continue
			}
			n, err := store.LLen(ns, rest[0])
			reply(func() {
				if err != nil {
					w.Error(err.Error())
				} else {
					w.Int(n)
				}
			})

		case "LRANGE":
			if len(rest) != 3 {
				reply(func() { w.Error("ERR wrong number of arguments") })
				continue
			}
			start, err1 := strconvParseInt(rest[1])
			stop, err2 := strconvParseInt(rest[2])
			if err1 != nil || err2 != nil {
				reply(func() { w.Error("ERR value is not an integer or out of range") })
				continue
			}
			items, err := store.LRange(ns, rest[0], start, stop)
			reply(func() {
				if err != nil {
					w.Error(err.Error())
				} else {
					w.BulkArray(items)
				}
			})

		case "FLUSHDB", "FLUSHALL":
			store.Flush(ns)
			reply(func() { w.Simple("OK") })

		default:
			reply(func() { w.Error(fmt.Sprintf("ERR unknown command '%s'", strings.ToLower(cmd))) })
		}
	}
}

// preAuthOK 认证前允许的命令。
func preAuthOK(cmd string) bool {
	switch cmd {
	case "AUTH", "PING", "ECHO", "QUIT", "COMMAND", "HELLO":
		return true
	}
	return false
}

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
