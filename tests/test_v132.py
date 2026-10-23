#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.2 黑盒测试：
1) Redis BLPOP/BRPOP 阻塞弹出：非阻塞命中 / 方向（左·右）/ 多 key 顺序 /
   Push 唤醒阻塞客户端 / 超时返回空数组 / WRONGTYPE / 非法超时
2) AMQP publisher confirm：confirm.select + 逐条 basic.ack（pika confirm_delivery）
3) AMQP mandatory 回退：不可路由 basic.return（312 NO_ROUTE，内容原样）/
   可路由不回退 / confirm 与回退并存
"""
import socket
import threading
import time

import pika

HOST, PORT = "proxy", 3309
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
    """极简 RESP2 客户端（与 test_v120.py 同源）。"""

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

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def pika_connect():
    params = pika.ConnectionParameters(
        host=HOST, port=PORT, heartbeat=0, blocked_connection_timeout=30,
        credentials=pika.PlainCredentials("glacier", "iloveyou~"),
        socket_timeout=30)
    return pika.BlockingConnection(params)


def safe_close(conn):
    """pika 1.4.4 关闭握手有已知怪癖（CloseOk 分发抛 KeyError），关闭失败不影响会话内验证。"""
    try:
        conn.close()
    except Exception:
        pass


def phase_kv():
    r = Redis(declare_mode="redis")
    r.cmd("AUTH", "glacier", "iloveyou~")
    for k in ("kv:b132:q1", "kv:b132:q2", "kv:b132:blk", "kv:b132:str"):
        r.cmd("DEL", k)

    # 1. 非阻塞命中（已有数据立即返回 [key, value]）
    r.cmd("RPUSH", "kv:b132:q1", "a")
    got = r.cmd("BLPOP", "kv:b132:q1", "1")
    check(got == [b"kv:b132:q1", b"a"], "BLPOP 非阻塞命中（%r）" % got)

    # 2. BRPOP 方向：RPUSH a b c → 右端弹出 c
    r.cmd("RPUSH", "kv:b132:q1", "a", "b", "c")
    got = r.cmd("BRPOP", "kv:b132:q1", "1")
    check(got == [b"kv:b132:q1", b"c"], "BRPOP 从右端弹出（%r）" % got)
    r.cmd("DEL", "kv:b132:q1")

    # 3. 多 key 按参数顺序取首个非空
    r.cmd("RPUSH", "kv:b132:q2", "x")
    got = r.cmd("BLPOP", "kv:b132:q1", "kv:b132:q2", "1")
    check(got == [b"kv:b132:q2", b"x"], "BLPOP 多 key 按参数顺序（%r）" % got)

    # 4. Push 唤醒阻塞中的 BLPOP
    result = {}

    def waiter():
        rr = Redis(declare_mode="redis")
        rr.cmd("AUTH", "glacier", "iloveyou~")
        t0 = time.time()
        try:
            result["got"] = rr.cmd("BLPOP", "kv:b132:blk", "5")
        except RedisError as e:
            result["err"] = str(e)
        result["dt"] = time.time() - t0
        rr.close()

    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(0.5)
    r.cmd("RPUSH", "kv:b132:blk", "wake")
    th.join(timeout=10)
    check(result.get("got") == [b"kv:b132:blk", b"wake"] and result.get("dt", 99) < 3,
          "Push 唤醒阻塞中的 BLPOP（%.2fs，got=%r）" % (result.get("dt", -1), result.get("got")))

    # 5. 超时返回空数组
    t0 = time.time()
    got = r.cmd("BLPOP", "kv:b132:none", "1")
    dt = time.time() - t0
    check(got is None and 0.7 <= dt <= 4, "BLPOP 超时返回空数组（%.2fs）" % dt)

    # 6. WRONGTYPE
    r.cmd("SET", "kv:b132:str", "v")
    try:
        r.cmd("BLPOP", "kv:b132:str", "1")
        check(False, "BLPOP 非 list 键应报错")
    except RedisError as e:
        check("WRONGTYPE" in str(e), "BLPOP 非 list 键 → WRONGTYPE（%s）" % e)

    # 7. 非法超时
    try:
        r.cmd("BLPOP", "kv:b132:q1", "-1")
        check(False, "BLPOP 负超时应报错")
    except RedisError as e:
        check("negative" in str(e), "BLPOP 负超时 → 报错（%s）" % e)

    r.cmd("DEL", "kv:b132:q2", "kv:b132:str")
    r.close()
    print("V132-KV: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))


def phase_mq():
    conn = pika_connect()
    ch = conn.channel()

    # 1. publisher confirm：confirm.select 后逐条 basic.ack（publish 不抛异常）
    ch.confirm_delivery()
    ch.queue_declare(queue="q_c132", durable=False)
    ch.queue_purge(queue="q_c132")  # 清残留，保证取到本次发布的消息
    ok, err = True, ""
    try:
        ch.basic_publish(exchange="", routing_key="q_c132", body=b"c1")
        ch.basic_publish(exchange="", routing_key="q_c132", body=b"c2")
    except Exception as e:
        ok, err = False, str(e)
    check(ok, "confirm_delivery 逐条 basic.ack（publish 完成 %s）" % (err if err else "无异常"))
    m = ch.basic_get(queue="q_c132", auto_ack=True)
    check(m[0] is not None and m[2] == b"c1", "确认后消息正常入队（%r）" % (m[2] if m[0] else None))

    # 2/3. mandatory 回退（未开 confirm 的通道：回退走 on_return 回调，不抛异常）
    conn2 = pika_connect()
    ch2 = conn2.channel()
    ch2.exchange_declare(exchange="ex_nr132", exchange_type="direct")
    returns = []
    ch2.add_on_return_callback(lambda c, m, p, b: returns.append((m.reply_code, m.exchange, m.routing_key, b)))
    ch2.basic_publish(exchange="ex_nr132", routing_key="no.route", body=b"back", mandatory=True)
    conn2.process_data_events(time_limit=2)
    check(len(returns) == 1 and returns[0][0] == 312 and returns[0][3] == b"back"
          and returns[0][1] == "ex_nr132" and returns[0][2] == "no.route",
          "mandatory 不可路由 → basic.return 312 NO_ROUTE（%r）" % returns)

    ch2.queue_declare(queue="q_r132")
    ch2.queue_bind(queue="q_r132", exchange="ex_nr132", routing_key="hit")
    returns.clear()
    ch2.basic_publish(exchange="ex_nr132", routing_key="hit", body=b"ok", mandatory=True)
    conn2.process_data_events(time_limit=1)
    check(not returns, "mandatory 可路由 → 无回退")
    m = ch2.basic_get(queue="q_r132", auto_ack=True)
    check(m[0] is not None and m[2] == b"ok", "可路由消息正常入队")

    # 4. confirm + mandatory 回退并存：basic.return 先回、basic.ack 后到；
    #    pika confirm 模式将回退汇总为 UnroutableError（抛出前确认帧已收讫，
    #    回退内容经 on_return 回调送达）
    ret2 = []
    ch.add_on_return_callback(lambda c, m, p, b: ret2.append((m.reply_code, b)))
    try:
        ch.basic_publish(exchange="ex_nr132", routing_key="no.route2", body=b"both", mandatory=True)
        check(False, "confirm+mandatory 回退应抛 UnroutableError")
    except pika.exceptions.UnroutableError:
        conn.process_data_events(time_limit=2)  # 回退回调在事件循环中分发
        check(any(code == 312 and body == b"both" for code, body in ret2),
              "confirm + mandatory 回退并存（回退回调 %r，ack 已收讫）" % ret2)
    except Exception as e:
        check(False, "confirm+mandatory 回退异常类型不符：%r" % e)

    safe_close(conn)
    safe_close(conn2)
    print("V132-MQ: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))


def main() -> int:
    phase_kv()
    phase_mq()
    print("V132: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
