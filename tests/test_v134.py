#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.4 黑盒测试：空值应答挂死 RESP3 客户端（反馈①-补）。

v1.3.3 的 HELLO 修复让 redis-py ≥8（默认 RESP3 解析器）能直连，但协议仍回
RESP2 空值帧：RESP3 解析器对 `$-1` 按「读 -1 字节」处理——GET miss / MGET
部分 miss / BLPOP 超时等一切空值应答挂死到 socket 超时。

修复：HELLO 3 协商后，该连接的空值应答改发 RESP3 统一 null 帧 `_`；
RESP2 连接（未 HELLO / HELLO 2 / protocol=2）维持 `$-1` / `*-1` 原语义。
"""
import socket
import struct
import sys
import time

import redis

HOST, PORT = "proxy", 3309
USER, PW = "glacier", "iloveyou~"
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def r3(timeout=8):
    """RESP3 客户端（不传 protocol，redis-py ≥8 默认）。"""
    return redis.Redis(host=HOST, port=PORT, username="default", password=PW,
                       socket_timeout=timeout)


def r2(timeout=8):
    """显式 RESP2 客户端（回归保护）。"""
    return redis.Redis(host=HOST, port=PORT, password=PW, protocol=2,
                       socket_timeout=timeout)


class RawResp:
    """极简 RESP2/RESP3 裸客户端（帧级断言用）。"""

    def __init__(self, timeout=8):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def _readline(self):
        while b"\r\n" not in self.buf:
            d = self.sock.recv(65536)
            if not d:
                raise EOFError("closed")
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _readn(self, n):
        while len(self.buf) < n:
            d = self.sock.recv(65536)
            if not d:
                raise EOFError("closed")
            self.buf += d
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def read(self):
        line = self._readline()
        t, rest = line[:1], line[1:]
        if t in (b"+", b"-", b"_"):
            return None if t == b"_" else rest.decode()
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            v = self._readn(n)
            self._readline()
            return v
        if t == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise ValueError("bad frame %r" % line)

    def cmd(self, *args):
        parts = [b"*%d" % len(args)]
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            parts.append(b"$%d" % len(b))
            parts.append(b)
        self.sock.sendall(b"\r\n".join(parts) + b"\r\n")
        return self.read()

    def close(self):
        self.sock.close()


def part_resp3_nil():
    print("---- Part 1: RESP3 空值应答（redis-py ≥8 默认 protocol）----")
    r = r3()
    check(r.ping(), "RESP3 客户端连接 + PING")
    # GET miss（v1.3.3 此处挂死超时）
    t0 = time.time()
    v = r.get("no-such-key-v134")
    check(v is None, "GET miss 返回 None（%.0fms，v1.3.3 挂死）" % ((time.time() - t0) * 1000))
    # MGET 部分 miss（BulkArray 内嵌空值）
    r.set("v134:hit", "x")
    got = r.mget("v134:hit", "no-such-key-v134")
    check(got == [b"x", None], "MGET 部分miss 内嵌 null（%r）" % got)
    # BLPOP 超时（NullArray 空值）
    t0 = time.time()
    v = r.blpop("v134:empty:list", timeout=1)
    check(v is None, "BLPOP 超时返回 None（%.0fms）" % ((time.time() - t0) * 1000))
    # LPOP/RPOP 空列表（nil）
    check(r.lpop("v134:empty:list") is None, "LPOP 空列表返回 None")
    # TTL 不存在键（RESP2 中 -1/-2 为 int 帧，与空值无关，回归确认）
    check(r.ttl("no-such-key-v134") == -2, "TTL 不存在键 = -2")
    # GET/SET 正常值不受影响
    check(r.get("v134:hit") == b"x" and r.delete("v134:hit") == 1, "正常值 SET/GET/DEL 不受影响")
    r.close()

    # 裸帧断言：HELLO 3 后空值帧为 `_`；SCAN 空结果仍为 `*0`（合法空数组非 null）
    raw = RawResp()
    m = raw.cmd("HELLO", "3", "AUTH", "default", PW)
    asMap = dict(zip(m[::2], m[1::2]))
    check(asMap.get(b"proto") == b"2", "HELLO 3 应答 proto 仍为 2")
    v = raw.cmd("GET", "no-such-key-v134")
    check(v is None, "HELLO 3 后 GET miss → null")
    # 直接看原始字节确认帧形态
    raw.sock.sendall(b"*2\r\n$3\r\nGET\r\n$15\r\nno-such-key-v134\r\n")
    time.sleep(0.3)
    frame = b""
    try:
        raw.sock.settimeout(1)
        while True:
            d = raw.sock.recv(4096)
            if not d:
                break
            frame += d
    except socket.timeout:
        pass
    check(frame == b"_\r\n", "空值帧形态为 RESP3 `_`（got %r）" % frame)
    raw.close()


def part_resp2_regression():
    print("---- Part 2: RESP2 连接空值语义回归 ----")
    r = r2()
    check(r.ping(), "RESP2 连接 PING")
    check(r.get("no-such-key-v134") is None, "RESP2 GET miss = None")
    check(r.lpop("v134:empty:list") is None, "RESP2 LPOP 空列表 = None")
    check(r.blpop("v134:empty:list", timeout=1) is None, "RESP2 BLPOP 超时 = None")
    r.close()
    # 裸帧断言：未 HELLO 的连接空值帧维持 RESP2 `$-1`
    raw = RawResp()
    raw.cmd("AUTH", USER, PW)
    raw.sock.sendall(b"*2\r\n$3\r\nGET\r\n$15\r\nno-such-key-v134\r\n")
    time.sleep(0.3)
    frame = b""
    try:
        raw.sock.settimeout(1)
        while True:
            d = raw.sock.recv(4096)
            if not d:
                break
            frame += d
    except socket.timeout:
        pass
    check(frame == b"$-1\r\n", "RESP2 空值帧维持 $-1（got %r）" % frame)
    raw.close()

    # HELLO 2 显式协商后同样维持 RESP2 空值
    raw2 = RawResp()
    raw2.cmd("HELLO", "2", "AUTH", "default", PW)
    v = raw2.cmd("GET", "no-such-key-v134")
    check(v is None, "HELLO 2 后 GET miss = None（RESP2 帧）")
    raw2.close()


def main():
    part_resp3_nil()
    part_resp2_regression()
    print("V134: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
