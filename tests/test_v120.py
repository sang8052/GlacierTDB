#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.2.0 黑盒测试（Redis 键值/队列 + 3309 端口复用）：
1) 端口复用：mode:redis 声明行 / RESP 直接嗅探 / 内联命令 / MySQL 默认不受影响
2) 基础能力：GET/SET/DEL/EXISTS/APPEND/INCR/DECR/INCRBY/DECRBY/MGET/MSET/KEYS/TYPE
3) TTL：EXPIRE/PEXPIRE/TTL/PTTL/PERSIST/惰性过期
4) 队列：LPUSH/RPUSH/LPOP/RPOP（含 count）/LLEN/LRANGE（含负下标）
5) 命名空间隔离：每用户独立（跨用户不可见）
6) 内存治理：max_memory + allkeys-lru 淘汰 / noeviction OOM / kv_stats 占用与命中率
7) 认证：AUTH（单/双参）、NOAUTH、错误密码
8) --check-restart：容器重启后数据无损恢复（落盘验证）
"""
import base64
import hashlib
import hmac
import socket
import sys
import time

HOST, PORT = "proxy", 3309
MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASS = "proxy", 3309, "glacier", "iloveyou~"
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


class RedisError(Exception):
    pass


class Redis:
    """极简 RESP2 客户端（stdlib 实现）。"""

    def __init__(self, declare_mode=None, timeout=30):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""
        if declare_mode:
            self.sock.sendall(("mode:%s\r\n" % declare_mode).encode())

    def _readline(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RedisError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _readexactly(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise RedisError("connection closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def read(self):
        line = self._readline()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            raise RedisError(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            return self._readexactly(n + 2)[:-2]
        if t == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise RedisError("bad reply: %r" % line)

    def cmd(self, *args):
        out = b"*%d\r\n" % len(args)
        for a in args:
            if isinstance(a, str):
                a = a.encode()
            out += b"$%d\r\n%s\r\n" % (len(a), a)
        self.sock.sendall(out)
        return self.read()

    def send_raw(self, data):
        self.sock.sendall(data)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def mysql_connect(user=MYSQL_USER, password=MYSQL_PASS):
    import pymysql
    return pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=user, password=password,
                           database="backup", charset="utf8mb4", autocommit=True,
                           read_timeout=120, write_timeout=120)


def phase_restart_check():
    """--check-restart：验证重启后 KV 数据从 SQLite 无损恢复。"""
    r = Redis(declare_mode="redis")
    try:
        check(r.cmd("AUTH", MYSQL_USER, MYSQL_PASS) == "OK", "重启后 AUTH 成功")
        v = r.cmd("GET", "kv:p120:marker")
        check(v == b"p120-marker-1", "字符串键重启后恢复（%r）" % v)
        n = r.cmd("LLEN", "kv:p120:list")
        check(n == 1, "队列键重启后恢复（LLEN=%s）" % n)
        item = r.cmd("LRANGE", "kv:p120:list", "0", "-1")
        check(item == [b"li-1"], "队列元素重启后恢复（%r）" % item)
        ttl = r.cmd("TTL", "kv:p120:marker")
        check(ttl == -1, "无 TTL 键恢复后 TTL=-1（%s）" % ttl)
    finally:
        r.close()
        print("V120-RESTART: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


def phase_main():
    # ---------- 1. 端口复用 ----------
    r = Redis(declare_mode="redis")
    check(r.cmd("PING") == "PONG", "mode:redis 声明 + PING")
    check(r.cmd("PING", "hello") == b"hello", "PING msg")

    # 未认证拒绝
    try:
        r.cmd("GET", "x")
        check(False, "未认证 GET 应报 NOAUTH")
    except RedisError as e:
        check("NOAUTH" in str(e), "未认证 → NOAUTH（%s）" % str(e)[:40])

    # 错误密码
    try:
        r.cmd("AUTH", MYSQL_USER, "wrongpass")
        check(False, "错误密码应拒绝")
    except RedisError as e:
        check("WRONGPASS" in str(e), "错误密码 → WRONGPASS")

    check(r.cmd("AUTH", MYSQL_USER, MYSQL_PASS) == "OK", "AUTH user password 双参")
    r.close()

    # 内联命令（非 RESP 数组）
    r = Redis(declare_mode="redis")
    r.send_raw(b"AUTH " + MYSQL_PASS.encode() + b"\r\n")
    check(r.read() == "OK", "内联命令 AUTH")
    r.send_raw(b"PING\r\n")
    check(r.read() == "PONG", "内联命令 PING")
    r.close()

    # RESP 直接嗅探（无声明行）
    r = Redis()
    check(r.cmd("AUTH", MYSQL_PASS) == "OK", "RESP 嗅探 + 单参 AUTH（密码匹配任意账号）")
    check(r.cmd("PING") == "PONG", "RESP 嗅探命令执行")
    r.close()

    # MySQL 默认路径不受影响（同端口、无声明行、静默等握手）
    conn = mysql_connect()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    check(cur.fetchone()[0] == 1, "同端口 MySQL 默认路径不受影响")
    conn.close()

    # ---------- 2. 基础能力 ----------
    r = Redis(declare_mode="redis")
    r.cmd("AUTH", MYSQL_USER, MYSQL_PASS)
    check(r.cmd("SET", "k1", "v1") == "OK", "SET")
    check(r.cmd("GET", "k1") == b"v1", "GET")
    check(r.cmd("EXISTS", "k1") == 1, "EXISTS")
    check(r.cmd("DEL", "k1", "nokey") == 1, "DEL 计数")
    check(r.cmd("GET", "k1") is None, "GET 缺失 → nil")

    check(r.cmd("APPEND", "k2", "ab") == 2, "APPEND 新键")
    check(r.cmd("APPEND", "k2", "cd") == 4, "APPEND 追加")
    check(r.cmd("GET", "k2") == b"abcd", "APPEND 结果")

    check(r.cmd("SET", "n", "10") == "OK", "SET 数字")
    check(r.cmd("INCR", "n") == 11, "INCR")
    check(r.cmd("DECR", "n") == 10, "DECR")
    check(r.cmd("INCRBY", "n", "5") == 15, "INCRBY")
    check(r.cmd("DECRBY", "n", "3") == 12, "DECRBY")

    r.cmd("MSET", "m1", "a", "m2", "b")
    got = r.cmd("MGET", "m1", "m2", "m3")
    check(got == [b"a", b"b", None], "MGET/MSET（含缺失 nil）")

    keys = r.cmd("KEYS", "m*")
    check(sorted(k.decode() for k in keys) == ["m1", "m2"], "KEYS 通配")
    check(r.cmd("TYPE", "m1") == "string", "TYPE string")
    check(r.cmd("TYPE", "nokey") == "none", "TYPE none")

    # NX / XX
    check(r.cmd("SET", "nxk", "v", "NX") == "OK", "SET NX 不存在 → OK")
    check(r.cmd("SET", "nxk", "v2", "NX") is None, "SET NX 已存在 → nil")
    check(r.cmd("SET", "nxk", "v3", "XX") == "OK", "SET XX 已存在 → OK")
    check(r.cmd("SET", "xxk", "v", "XX") is None, "SET XX 不存在 → nil")

    # ---------- 3. TTL ----------
    r.cmd("SET", "t1", "v", "EX", "2")
    ttl = r.cmd("TTL", "t1")
    check(0 < ttl <= 2, "SET EX + TTL（%s）" % ttl)
    check(r.cmd("PTTL", "t1") > 0, "PTTL")
    check(r.cmd("PERSIST", "t1") == 1, "PERSIST")
    check(r.cmd("TTL", "t1") == -1, "PERSIST 后 TTL=-1")

    r.cmd("SET", "t2", "v")
    check(r.cmd("EXPIRE", "t2", "1") == 1, "EXPIRE")
    check(r.cmd("TTL", "nokey") == -2, "TTL 无键 → -2")
    time.sleep(1.3)
    check(r.cmd("GET", "t2") is None, "惰性过期（到期后 GET nil）")
    r.cmd("SET", "t3", "v")
    r.cmd("PEXPIRE", "t3", "1000")
    time.sleep(1.3)
    check(r.cmd("EXISTS", "t3") == 0, "惰性过期（EXISTS 0）")

    # ---------- 4. 队列 ----------
    check(r.cmd("RPUSH", "q1", "a", "b") == 2, "RPUSH")
    check(r.cmd("LPUSH", "q1", "z") == 3, "LPUSH")
    check(r.cmd("LLEN", "q1") == 3, "LLEN")
    got = r.cmd("LRANGE", "q1", "0", "-1")
    check(got == [b"z", b"a", b"b"], "LRANGE 全量（%r）" % got)
    got = r.cmd("LRANGE", "q1", "-2", "-1")
    check(got == [b"a", b"b"], "LRANGE 负下标")
    check(r.cmd("LPOP", "q1") == b"z", "LPOP")
    check(r.cmd("RPOP", "q1") == b"b", "RPOP")
    r.cmd("RPUSH", "q2", "1", "2", "3")
    got = r.cmd("LPOP", "q2", "2")
    check(got == [b"1", b"2"], "LPOP count=2（%r）" % got)
    check(r.cmd("RPOP", "q2") == b"3", "RPOP 清空")
    check(r.cmd("RPOP", "q2") is None, "RPOP 空队列 → nil")
    check(r.cmd("LLEN", "nokey") == 0, "LLEN 无键 → 0")

    # 类型错误
    r.cmd("SET", "str1", "v")
    try:
        r.cmd("LPUSH", "str1", "x")
        check(False, "WRONGTYPE 期望")
    except RedisError as e:
        check("WRONGTYPE" in str(e), "WRONGTYPE（对 string 执行列表命令）")

    # ---------- 5. 命名空间隔离 ----------
    conn = mysql_connect()
    cur = conn.cursor()
    cur.execute("USE GlacierTDB")
    for u in ("kvuser",):
        try:
            cur.execute("DROP USER '%s'" % u)
        except Exception:
            pass
    cur.execute("CREATE USER 'kvuser'@'%' IDENTIFIED BY 'kvpass1'")
    check(True, "CREATE USER kvuser")
    conn.close()

    r.cmd("SET", "secret-of-glacier", "top")
    r2 = Redis(declare_mode="redis")
    check(r2.cmd("AUTH", "kvuser", "kvpass1") == "OK", "kvuser 登录")
    keys = r2.cmd("KEYS", "*")
    check(all(b"secret" not in k for k in keys), "命名空间隔离（kvuser 看不到 glacier 的键）")
    check(r2.cmd("GET", "secret-of-glacier") is None, "跨用户 GET → nil")
    r2.cmd("SET", "kvuser-key", "mine")
    check(r2.cmd("GET", "kvuser-key") == b"mine", "kvuser 自己的键可读")
    check(r.cmd("EXISTS", "kvuser-key") == 0, "glacier 看不到 kvuser 的键")
    r2.close()

    # ---------- 6. 内存治理 ----------
    conn = mysql_connect()
    cur = conn.cursor()
    cur.execute("USE GlacierTDB")
    cur.execute("FLUSHDB") if False else None
    r.cmd("FLUSHDB")
    cur.execute("SET KV MAX_MEMORY 1")          # 1 MB
    cur.execute("SET KV EVICTION allkeys-lru")
    check(True, "SET KV MAX_MEMORY/EVICTION 热改")
    val = "x" * 2048
    for i in range(2000):                        # ~4MB 注入 → 触发淘汰
        r.cmd("SET", "ev-%04d" % i, val)
    check(r.cmd("GET", "ev-0000") is None, "allkeys-lru 淘汰最早键")
    check(r.cmd("EXISTS", "ev-1999") == 1, "最新键保留")
    cur.execute("SHOW KV STATS")
    rows = cur.fetchall()
    tot = [x for x in rows if x[0] == "*"]
    check(tot and tot[0][9] > 0, "kv_stats evicted 计数（%s）" % (tot[0][9] if tot else "-"))
    check(tot and tot[0][2] <= 1 << 20, "内存占用不超过上限（%s 字节）" % (tot[0][2] if tot else "-"))

    cur.execute("SET KV EVICTION noeviction")
    try:
        r.cmd("SET", "huge", "y" * (2 << 20))
        check(False, "noeviction 超限应 OOM")
    except RedisError as e:
        check("OOM" in str(e), "noeviction → OOM 错误")

    cur.execute("SET KV MAX_MEMORY 0")           # 恢复不限
    cur.execute("SET KV EVICTION allkeys-lru")
    check(r.cmd("SET", "after-reset", "ok") == "OK", "恢复后可写")

    cur.execute("SELECT * FROM kv_stats")
    rows = cur.fetchall()
    check(len(rows) >= 2 and any(x[0] == "*" for x in rows), "SELECT * FROM kv_stats 可查")
    cur.execute("SELECT namespace, keys FROM kv_stats WHERE namespace = '*'")
    check(cur.fetchone()[0] == "*", "kv_stats WHERE 过滤")
    hitrow = [x for x in rows if x[0] == "*"][0]
    check(hitrow[5] > 0 and hitrow[6] > 0, "命中/未命中统计（hits=%s misses=%s）" % (hitrow[5], hitrow[6]))
    conn.close()

    # ---------- 7. 杂项 ----------
    try:
        r.cmd("NOSUCHCMD", "x")
        check(False, "未知命令应报错")
    except RedisError as e:
        check("unknown command" in str(e), "未知命令 → ERR")

    # ---------- 8. 持久化标记（供 --check-restart 验证）----------
    r.cmd("DEL", "kv:p120:marker", "kv:p120:list")
    r.cmd("SET", "kv:p120:marker", "p120-marker-1")
    r.cmd("RPUSH", "kv:p120:list", "li-1")
    check(r.cmd("GET", "kv:p120:marker") == b"p120-marker-1", "持久化标记写入")
    r.close()

    print("V120: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--check-restart":
        return phase_restart_check()
    return phase_main()


if __name__ == "__main__":
    raise SystemExit(main())
