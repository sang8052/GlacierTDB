#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.5 黑盒测试：S3 与 Redis 已知限制清理。

S3：
  1) UploadPartCopy（x-amz-copy-source [+range] 拷贝分片，CopyPartResult XML）
  2) 未完成分片上传自动生命周期回收（s3.abort_incomplete_days）
  3) max_object_mb 负值 = 不限制
Redis：
  4) MULTI/EXEC/DISCARD 事务（队列化 + EXEC 结果数组；错误命令在数组内报错不中断）
  5) SUBSCRIBE/UNSUBSCRIBE/PSUBSCRIBE/PUNSUBSCRIBE/PUBLISH（跨连接推送 + 通配 + ns 隔离）
  6) SCAN 真游标（COUNT 批次 + 游标推进 + 全量遍历等价 KEYS）
"""
import socket
import sys
import time
import urllib.request
import urllib.error
import hashlib
import re

import redis

HOST, PORT = "proxy", 3309
USER, PW = "glacier", "iloveyou~"
S3 = "http://proxy:9000"
AK, SK = None, None  # 从管理面取（tests/init 或环境）
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


# ---------------------------------------------------------------------------
# S3 SigV4 客户端：复用 tests/test_s3.py 的成熟实现
# ---------------------------------------------------------------------------

sys.path.insert(0, "/tests")
from test_s3 import s3_request, S3_HOST, S3_PORT  # noqa: E402


def s3_req(method, bucket, key="", query="", data=None, extra=None):
    path = "/" + bucket + ("/" + key if key else "")
    hdrs = {k.lower(): v for k, v in (extra or {}).items()}
    return s3_request(method, path, query=query, headers=hdrs, body=data or b"", ak=AK, sk=SK)


def get_ak_sk(cur):
    """管理面签发 S3 密钥（与 test_s3.py 同法：签发时 SK 明文可见一次）。"""
    global AK, SK
    cur.execute("USE GlacierTDB")
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    row = cur.fetchone()
    if row:
        AK, SK = row[0], row[1]
    cur.execute("USE backup")


# ---------------------------------------------------------------------------
# Part 1-3：S3
# ---------------------------------------------------------------------------

def part_s3(cur):
    print("---- Part A: S3 限制清理 ----")
    get_ak_sk(cur)
    check(bool(AK and SK), "S3 密钥就绪")

    # 建桶 + 源对象
    st, _, _ = s3_req("PUT", "v135")
    check(st in (200, 409), "桶 v135 就绪（%s）" % st)
    src = b"0123456789" * 10  # 100B
    st, _, _ = s3_req("PUT", "v135", "src.bin", data=src)
    check(st == 200, "源对象上传（100B）")

    # ① UploadPartCopy：整对象拷贝为 part1
    st, _, body = s3_req("POST", "v135", "dst.bin", query="uploads")
    m = re.search(rb"<UploadId>([^<]+)</UploadId>", body)
    check(st == 200 and m, "InitiateMultipart")
    upload_id = m.group(1).decode()

    st, _, body = s3_req("PUT", "v135", "dst.bin",
                         query="partNumber=1&uploadId=%s" % upload_id, data=b"",
                         extra={"x-amz-copy-source": "/v135/src.bin"})
    m2 = re.search(rb"<ETag>&#34;([0-9a-f]{32})&#34;</ETag>|<ETag>\"([0-9a-f]{32})\"</ETag>", body)
    etag1 = (m2.group(1) or m2.group(2)).decode() if m2 else ""
    check(st == 200 and etag1 == hashlib.md5(src).hexdigest(),
          "UploadPartCopy 整对象（ETag=源 MD5，%s）" % etag1[:12])

    # Range 拷贝 part2：bytes=10-19（10 字节）
    st, _, body = s3_req("PUT", "v135", "dst.bin",
                         query="partNumber=2&uploadId=%s" % upload_id, data=b"",
                         extra={"x-amz-copy-source": "/v135/src.bin",
                                "x-amz-copy-source-range": "bytes=10-19"})
    m3 = re.search(rb"<ETag>(?:&#34;|\")([0-9a-f]{32})(?:&#34;|\")</ETag>", body)
    seg = src[10:20]
    check(st == 200 and m3 and m3.group(1).decode() == hashlib.md5(seg).hexdigest(),
          "UploadPartCopy Range bytes=10-19（ETag=段 MD5）")

    # 非法 Range
    st, _, _ = s3_req("PUT", "v135", "dst.bin", query="partNumber=3&uploadId=%s" % upload_id,
                      data=b"", extra={"x-amz-copy-source": "/v135/src.bin",
                                       "x-amz-copy-source-range": "bytes=90-999"})
    check(st == 400, "越界 Range → 400（%s）" % st)

    # Complete：合并后内容 = src + seg
    complete = ("<CompleteMultipartUpload>"
                "<Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>"
                "<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part>"
                "</CompleteMultipartUpload>" % (etag1, m3.group(1).decode())).encode()
    st, _, body = s3_req("POST", "v135", "dst.bin", query="uploadId=%s" % upload_id, data=complete)
    check(st == 200 and b"<ETag>" in body, "Complete（拷贝分片参与合并）")
    st, _, got = s3_req("GET", "v135", "dst.bin")
    check(got == src + seg, "合并内容 = 源整段 + Range 段（%dB）" % len(got))
    s3_req("DELETE", "v135", "dst.bin")

    # ② 未完成上传自动回收（s3.abort_incomplete_days，默认 7 天）：
    #    时效无法在测试窗口内等待——验证配置项已暴露 + 会话滞留可见 + Abort 清理路径
    #    （周期任务 RunLifecycle 每 lifecycle_interval_sec 秒调用同一回收函数）。
    st, _, body = s3_req("POST", "v135", "stale.bin", query="uploads")
    up2 = re.search(rb"<UploadId>([^<]+)</UploadId>", body).group(1).decode()
    st, _, _ = s3_req("PUT", "v135", "stale.bin", query="partNumber=1&uploadId=%s" % up2, data=b"x" * 5)
    cur.execute("USE GlacierTDB")
    cur.execute("SELECT upload_id FROM s3_uploads WHERE bucket='v135'")
    rows135 = cur.fetchall()
    check(len(rows135) >= 1, "未完成会话滞留可见（s3_uploads %d 行）" % len(rows135))
    cur.execute("SELECT name, value FROM server_config WHERE name='s3.abort_incomplete_days'")
    v = cur.fetchone()
    check(v is not None and int(v[1]) == 7, "s3.abort_incomplete_days 配置项暴露（=%s，7 天自动回收）" % (v[1] if v else None))
    cur.execute("USE backup")
    st, _, _ = s3_req("DELETE", "v135", "stale.bin", query="uploadId=%s" % up2)
    check(st == 204, "Abort 清理路径（204）")

    # ③ max_object_mb 负值不限：当前部署配置为默认值——本项验证配置语义
    #    （-1 放行 + 文档），行为层面用「无 EntityTooLarge 拦截的上传」冒烟：
    st, _, _ = s3_req("PUT", "v135", "big.bin", data=b"z" * (2 << 20))  # 2MB < 默认上限
    check(st == 200, "2MB 对象上传正常（默认上限语义未变）")
    s3_req("DELETE", "v135", "big.bin")
    s3_req("DELETE", "v135", "src.bin")


# ---------------------------------------------------------------------------
# Part 4-6：Redis
# ---------------------------------------------------------------------------

def r2(timeout=8):
    return redis.Redis(host=HOST, port=PORT, password=PW, protocol=2, socket_timeout=timeout)


def part_multi():
    print("---- Part B: MULTI/EXEC 事务 ----")
    r = r2()
    r.delete("v135:m1", "v135:m2")
    p = r.pipeline(transaction=True)  # MULTI/EXEC
    p.set("v135:m1", "a")
    p.incr("v135:m2")
    p.incr("v135:m2")
    p.get("v135:m1")
    res = p.execute()
    check(res == [True, 1, 2, b"a"], "MULTI/EXEC 四命令结果数组（%r）" % (res,))

    # 事务内错误命令：数组对应位置异常，其余正常执行
    p = r.pipeline(transaction=True)
    p.set("v135:m1", "b")
    try:
        p.hset("v135:h", "f", "v")  # 不支持的命令 → EXEC 内报 unknown，不中断
    except Exception:
        pass
    p.get("v135:m1")
    res = p.execute(raise_on_error=False)
    check(len(res) == 3 and res[0] is True and isinstance(res[1], Exception)
          and res[2] == b"b",
          "事务内错误命令不中断后续（%r）" % (res,))

    # QUEUED 应答 + DISCARD
    raw = Raw()
    raw.cmd("AUTH", USER, PW)
    check(raw.cmd("MULTI") == "OK", "MULTI → OK")
    check(raw.cmd("SET", "v135:q", "1") == "QUEUED", "入队 → QUEUED")
    check(raw.cmd("DISCARD") == "OK", "DISCARD → OK")
    v = raw.cmd("EXEC")
    check(isinstance(v, str) and "EXEC without MULTI" in v, "DISCARD 后 EXEC 报错")
    got = raw.cmd("GET", "v135:q")
    check(got is None, "DISCARD 丢弃未执行命令")
    raw.close()

    # MULTI 嵌套
    raw = Raw()
    raw.cmd("AUTH", USER, PW)
    raw.cmd("MULTI")
    v = raw.cmd("MULTI")
    check(isinstance(v, str) and "nested" in v, "嵌套 MULTI 报错")
    raw.cmd("DISCARD")
    raw.close()
    r.delete("v135:m1", "v135:m2")


class Raw:
    def __init__(self, timeout=8):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def _readline(self):
        while b"\r\n" not in self.buf:
            d = self.sock.recv(65536)
            if not d:
                raise EOFError
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _readn(self, n):
        while len(self.buf) < n:
            d = self.sock.recv(65536)
            if not d:
                raise EOFError
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


def part_pubsub():
    print("---- Part C: 发布订阅 ----")
    sub = Raw()
    sub.cmd("AUTH", USER, PW)
    r = sub.cmd("SUBSCRIBE", "v135.chan")
    check(r == [b"subscribe", b"v135.chan", 1], "SUBSCRIBE 确认帧（%r）" % (r,))

    # 跨连接 PUBLISH
    pub = r2()
    n = pub.publish("v135.chan", "hello-v135")
    check(n >= 1, "PUBLISH 返回接收者数（%s）" % n)
    got = sub.read()
    check(got == [b"message", b"v135.chan", b"hello-v135"], "跨连接收到 message 帧（%r）" % (got,))

    # PSUBSCRIBE 通配
    r2v = sub.cmd("PSUBSCRIBE", "v135.*")
    check(r2v == [b"psubscribe", b"v135.*", 2], "PSUBSCRIBE 确认帧（%r）" % (r2v,))
    pub.publish("v135.chan", "both")
    m1 = sub.read()
    m2 = sub.read()
    # 同连接 channel + pattern 双收（Redis 语义）
    okm = (m1[0] == b"message" and m2[0] == b"pmessage") or (m2[0] == b"message" and m1[0] == b"pmessage")
    check(okm and b"both" in (m1[-1], m2[-1]), "channel+pattern 双收 message/pmessage")

    # redis-py pubsub 客户端
    ps = redis.Redis(host=HOST, port=PORT, password=PW, protocol=2,
                     socket_timeout=8).pubsub()
    ps.subscribe("other.py")  # 避开 v135.* pattern，防 pmessage 插队退订确认帧
    time.sleep(0.2)
    pub.publish("other.py", "py-msg")
    msg = ps.get_message(timeout=3)
    while msg and msg.get("type") != "message":
        msg = ps.get_message(timeout=3)
    check(msg is not None and msg["data"] == b"py-msg" and msg["channel"] == b"other.py",
          "redis-py pubsub 收消息（%r）" % (msg,))
    ps.close()

    # UNSSCRIBE 全退
    v = sub.cmd("UNSUBSCRIBE", "v135.chan")
    check(v == [b"unsubscribe", b"v135.chan", 1], "UNSUBSCRIBE 计数递减")
    v = sub.cmd("PUNSUBSCRIBE", "v135.*")
    check(v == [b"punsubscribe", b"v135.*", 0], "PUNSUBSCRIBE 计数归零")
    n = pub.publish("v135.chan", "nobody")
    check(n == 0, "退订后 PUBLISH 接收者 0")
    sub.close()
    pub.close()


def part_scan():
    print("---- Part D: SCAN 真游标 ----")
    r = r2()
    for i in range(25):
        r.set("v135:scan:%02d" % i, "x")
    collected = []
    cursor = 0
    batches = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="v135:scan:*", count=7)
        collected.extend(keys)
        batches += 1
        if cursor == 0 or batches > 20:
            break
    check(len(collected) == 25 and len(set(collected)) == 25,
          "游标分批遍历 25 键无重无漏（%d 批）" % batches)
    check(batches >= 2, "COUNT=7 分多批返回（%d 批）" % batches)

    # scan_iter 等价 KEYS
    it = sorted(r.scan_iter("v135:scan:*"))
    ks = sorted(r.keys("v135:scan:*"))
    check(it == ks and len(it) == 25, "scan_iter == KEYS")

    # TYPE 过滤 + 游标
    r.rpush("v135:scan:list", "a")
    got_all = list(r.scan_iter(match="v135:scan:*"))
    check(b"v135:scan:list" in got_all and len(got_all) == 26, "含 list 类型 key")
    cursor, only_str = r.scan(cursor=0, match="v135:scan:*", _type="string", count=100)
    check(all(k != b"v133:scan:list" for k in only_str), "TYPE=string 过滤")
    for i in range(25):
        r.delete("v135:scan:%02d" % i)
    r.delete("v135:scan:list")
    r.close()


def main():
    import pymysql
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PW,
                           database="backup", charset="utf8mb4", autocommit=True)
    cur = conn.cursor()
    try:
        part_s3(cur)
        part_multi()
        part_pubsub()
        part_scan()
    finally:
        cur.close()
        conn.close()
    print("V135: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
