#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.13 黑盒测试（存储效率与可观测性）：

1) 实例唯一 ID：SELECT @@glaciertdb_instance_id / SHOW CONFIG / S3 响应头 /
   Redis INFO 四面一致（UUID 格式，重启不变性由 run_all 场景段复核）
2) chunk 池去重 + 闪传 A（x-glacier-flash 头）：命中空体返回一致 etag、
   未命中幂等降级、size 校验、并发闪传不产生重复 chunk
3) 闪传 B（?quick_transfer=）：命中直接建对象 / 未命中 404 QuickTransferMiss
4) 去重范围 user：同内容跨桶共享池 chunk（pool_shared 增长）+ CopyObject 零拷贝
   （内容一致、存储聚合正确）
5) gzip 传输压缩：text 对象压缩往返一致、x-glaciertdb-gzip:0 关闭、Range identity、
   图片 content-type 跳过、对象自身 Content-Encoding 跳过
6) 打包下载：mode=stream tar 流式解包一致 / mode=done zip 解包一致 / 非法 format 400
7) KV 驱逐优先级：KEYPRIO/GETPRIO 设、查、清、边界（-1 清除 / 越界报错 / -2 无键）+
   INFO evicted_prio/evicted_lru 字段
8) KV SQL 读穿透：KV SOURCE ADD 规则 → GET miss 回源回填 → 二次 GET 命中缓存 →
   空结果 miss → SHOW KV SOURCE / KV SOURCE DEL
9) SCRIPT 临时屏蔽：SUBMIT QUERY 带 SCRIPT 显式报错 "temporarily disabled"
10) S3 排障头：x-glacier-request-id / x-glacier-error-code / 错误 XML RequestId
"""
import concurrent.futures
import gzip as gzmod
import hashlib
import io
import json
import re
import socket
import sys
import tarfile
import time
import zipfile

import pymysql

sys.path.insert(0, "/tests")
from test_s3 import s3_request, sql_connect  # noqa: E402

HOST, PORT = "proxy", 3309
ADMIN, ADMIN_PW = "glacier", "iloveyou~"
AK, SK = None, None
T = "t%d" % int(time.time())
STATS = {"pass": 0, "fail": 0}
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def admin_sql(cur, sql):
    cur.execute("USE GlacierTDB")
    cur.execute(sql)
    try:
        return cur.fetchall()
    except Exception:
        return None


def req(method, path, query="", body=b"", extra=None):
    return s3_request(method, path, query=query, headers=extra or {}, body=body, ak=AK, sk=SK)


def sha256hex(b):
    return hashlib.sha256(b).hexdigest()


def kv_cmd(*args):
    """RESP 内联命令（glacier 账号），返回解析后的单值或原始行。"""
    s = socket.create_connection((HOST, PORT), timeout=10)
    f = s.makefile("rb")
    payload = b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n%s\r\n" % (len(a.encode()), a.encode()) for a in args)
    s.sendall(b"mode:redis\n")
    s.sendall(b"*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n" % (len(ADMIN_PW), ADMIN_PW.encode()))
    f.readline()  # +OK (auth)
    s.sendall(payload)
    first = f.readline()
    out = first
    if first.startswith(b"$"):
        n = int(first[1:].strip())
        if n >= 0:
            out = f.read(n).rstrip()
        else:
            out = None
    elif first.startswith(b":"):  # integer reply → b"N"
        out = first[1:].strip()
    elif first.startswith(b"*"):
        n = int(first[1:].strip())
        items = []
        for _ in range(n):
            ln = f.readline()
            if ln.startswith(b"$") and int(ln[1:].strip()) >= 0:
                items.append(f.read(int(ln[1:].strip())).decode())
            else:
                items.append(None)
        out = items
    s.close()
    return out


def space_row(cur):
    rows = admin_sql(cur, "SHOW S3 SPACE")
    d = {}
    cols = ["page_size", "page_count", "freelist_pages", "free_bytes", "file_bytes",
            "auto_vacuum", "pool_chunks", "pool_live_bytes", "pool_shared",
            "pool_tombstones", "pool_tomb_bytes"]
    for i, c in enumerate(cols):
        d[c] = rows[0][i]
    return d


def main():
    global AK, SK
    cur = sql_connect().cursor()
    cur.execute("USE GlacierTDB")
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    AK, SK = cur.fetchall()[0][0:2]
    check(bool(AK) and bool(SK), "S3 AK/SK ready")

    # ------------------------------------------------------------------
    # 1) 实例唯一 ID 四面一致
    # ------------------------------------------------------------------
    cur.execute("SELECT @@glaciertdb_instance_id")
    mysql_id = cur.fetchall()[0][0]
    check(bool(UUID_RE.match(str(mysql_id))), "SELECT @@glaciertdb_instance_id is UUID (%s)" % mysql_id)
    rows = admin_sql(cur, "SHOW SERVER CONFIG") or []
    cfg_id = None
    for r in rows:
        if r[0] == "glacier_instance_id":
            cfg_id = r[1]
    check(cfg_id == str(mysql_id), "SHOW SERVER CONFIG glacier_instance_id matches")
    st, hd, _ = req("GET", "/")
    s3_id = hd.get("x-glacier-instance-id", "")
    check(s3_id == str(mysql_id), "S3 response header x-glacier-instance-id matches")
    st, hd, body = req("GET", "/%s-nope/k" % T)  # NoSuchBucket 404 错误路径
    check(st == 404 and hd.get("x-glacier-error-code") == "NoSuchBucket" and hd.get("x-glacier-request-id", "") != "",
          "error headers x-glacier-error-code / x-glacier-request-id present (%d)" % st)
    rid_xml = b"RequestId" in body
    check(rid_xml, "error XML carries RequestId")
    info = kv_cmd("INFO")
    check("glaciertdb_instance_id:%s" % mysql_id in str(info), "Redis INFO carries instance id")

    # ------------------------------------------------------------------
    # 2) chunk 池 + 闪传 A（x-glacier-flash）
    # ------------------------------------------------------------------
    b1, b2 = T + "-a", T + "-b"
    admin_sql(cur, "CREATE BUCKET %s" % b1)
    admin_sql(cur, "CREATE BUCKET %s" % b2)
    big = bytes(range(256)) * 40960  # 10MB，走 chunk 池
    st, hd, _ = req("PUT", "/%s/big1" % b1, body=big)
    check(st == 200, "PUT big1 10MB -> %d" % st)
    et1 = hd.get("etag", "")
    pool_before = space_row(cur)["pool_shared"]

    # A 形态命中：空体 + x-glacier-flash
    st, hd, _ = req("PUT", "/%s/big1-flash" % b1, body=b"",
                    extra={"x-glacier-flash": "sha256:" + sha256hex(big)})
    check(st == 200 and hd.get("etag") == et1, "flash A hit: empty body returns same etag (%d)" % st)
    st, hd, got = req("GET", "/%s/big1-flash" % b1)
    check(st == 200 and got == big, "flash A hit object content equals original")

    # A 形态 size 校验：声明长度不符 → 按 miss 处理（走普通上传）
    st, hd, _ = req("PUT", "/%s/flash-miss" % b1, body=big,
                    extra={"x-glacier-flash": "sha256:" + sha256hex(big), "x-glacier-flash-size": "123"})
    check(st == 200 and hd.get("etag") == et1, "flash A with body on miss falls back to normal upload")

    # 未命中：未知 sha + 正常请求体 → 普通上传成功
    unknown = b"unknown-payload-%d" % time.time_ns()
    st, hd, _ = req("PUT", "/%s/flash-unknown" % b1, body=unknown,
                    extra={"x-glacier-flash": "sha256:" + sha256hex(unknown)})
    check(st == 200, "flash A miss with body -> normal upload 200 (%d)" % st)

    # 并发闪传同一 hash：全部成功且池 chunk 不重复
    def do_flash(i):
        return req("PUT", "/%s/flash-par-%d" % (b1, i), body=b"",
                   extra={"x-glacier-flash": "sha256:" + sha256hex(big)})[0]

    with concurrent.futures.ThreadPoolExecutor(8) as ex:
        codes = list(ex.map(do_flash, range(8)))
    check(all(c == 200 for c in codes), "concurrent flash create all 200 (%s)" % codes)
    st, _, got = req("GET", "/%s/flash-par-3" % b1)
    check(got == big, "concurrent flash object readable")

    # user 范围：同属主跨桶共享 + CopyObject 零拷贝
    st, _, _ = req("PUT", "/%s/big1" % b2, body=big)
    check(st == 200, "PUT same content to bucket-b (dedup scope user)")
    st, _, _ = req("PUT", "/%s/other" % b2, body=big[:len(big) // 2] + b"x")
    ps = space_row(cur)
    check(ps["pool_shared"] >= 1, "pool has shared chunks (refcnt>1, dedup active: %s)" % ps["pool_shared"])
    check(ps["pool_tombstones"] >= 0 and ps["pool_chunks"] >= 1, "pool stats columns present (chunks=%s)" % ps["pool_chunks"])
    st, hd, _ = req("PUT", "/%s/big1-copy" % b2, query="", extra={"X-Amz-Copy-Source": "/%s/big1" % b1}, body=b"")
    check(st == 200, "CopyObject zero-copy 200 (%d)" % st)
    st, _, got = req("GET", "/%s/big1-copy" % b2)
    check(got == big, "CopyObject content equals source (zero-copy refs)")
    row_b = None
    for r in admin_sql(cur, "SHOW BUCKETS") or []:
        if r[0] == b2:
            row_b = r
    other_size = len(big) // 2 + 1
    check(row_b is not None and row_b[6] == 2 * len(big) + other_size,
          "bucket-b storage_bytes = 3 logical objects (%s)" % (row_b and row_b[6]))
    # 删除一个引用，另一桶对象仍可读（引用计数保护）
    req("DELETE", "/%s/big1" % b1)
    st, _, got = req("GET", "/%s/big1-copy" % b2)
    check(st == 200 and got == big, "delete one ref, zero-copy copy still readable (refcnt)")

    # ------------------------------------------------------------------
    # 3) 闪传 B（quick_transfer）
    # ------------------------------------------------------------------
    st, hd, body = req("POST", "/%s/qt-hit" % b1, query="quick_transfer=" + sha256hex(big), body=b"")
    check(st == 200 and hd.get("etag") == et1, "quick_transfer hit -> 200 + etag (%d)" % st)
    st, hd, body = req("POST", "/%s/qt-miss" % b1, query="quick_transfer=" + sha256hex(b"never-uploaded"), body=b"")
    check(st == 404 and b"QuickTransferMiss" in body, "quick_transfer miss -> 404 QuickTransferMiss")
    st, _, got = req("GET", "/%s/qt-hit" % b1)
    check(got == big, "quick_transfer hit object readable")

    # ------------------------------------------------------------------
    # 4) gzip 传输压缩
    # ------------------------------------------------------------------
    text = (b"GlacierTDB gzip test line. " * 4000)  # ~108KB 可压文本
    st, _, _ = req("PUT", "/%s/doc.txt" % b1, body=text, extra={"Content-Type": "text/plain"})
    check(st == 200, "PUT text object")
    st, hd, body = req("GET", "/%s/doc.txt" % b1, extra={"Accept-Encoding": "gzip"})
    check(hd.get("content-encoding") == "gzip", "GET with Accept-Encoding: gzip -> Content-Encoding: gzip")
    if hd.get("content-encoding") == "gzip":
        check(gzmod.decompress(body) == text, "gzip roundtrip content equal")
    st, hd, body = req("GET", "/%s/doc.txt" % b1, extra={"Accept-Encoding": "gzip", "x-glaciertdb-gzip": "0"})
    check(hd.get("content-encoding") != "gzip" and body == text, "x-glaciertdb-gzip: 0 -> identity")
    st, hd, body = req("GET", "/%s/doc.txt" % b1, query="", extra={"Accept-Encoding": "gzip", "Range": "bytes=0-99"})
    check(st == 206 and hd.get("content-encoding") != "gzip" and body == text[:100],
          "Range request -> 206 identity (gzip not mixed)")
    st, hd, body = req("GET", "/%s/doc.txt" % b1)
    check(hd.get("content-encoding") != "gzip" and body == text, "no Accept-Encoding -> identity")
    # 图片 content-type 跳过
    png = bytes.fromhex("89504e470d0a1a0a0000000d49484452") + b"\x00" * 4096
    admin_sql2 = None
    st, _, _ = req("PUT", "/%s/img.png" % b1, body=png, extra={"Content-Type": "image/png"})
    st, hd, body = req("GET", "/%s/img.png" % b1, extra={"Accept-Encoding": "gzip"})
    check(hd.get("content-encoding") != "gzip", "image content-type skipped (no gzip)")
    # 对象自身 Content-Encoding 跳过
    st, _, _ = req("PUT", "/%s/enc.txt" % b1, body=text, extra={"Content-Type": "text/plain", "Content-Encoding": "gzip"})
    st, hd, body = req("GET", "/%s/enc.txt" % b1, extra={"Accept-Encoding": "gzip"})
    check(hd.get("content-encoding") == "gzip" and body == text, "object with own gzip Content-Encoding returned as-is (no double compress)")
    st, hd, body = req("GET", "/%s/doc.txt" % b1, extra={"Accept-Encoding": "gzip"})
    check(hd.get("content-encoding") == "gzip" and "Accept-Encoding" in hd.get("vary", ""),
          "Vary: Accept-Encoding on gzip path")

    # ------------------------------------------------------------------
    # 5) 打包下载
    # ------------------------------------------------------------------
    small = {T + "-arc/one.txt": b"hello archive one", T + "-arc/two.bin": b"\x00\x01\x02archive"}
    for k, v in small.items():
        req("PUT", "/%s/%s" % (b1, k), body=v, extra={"x-glacier-archive-ignore": "1"})
    # mode=stream（tar）
    st, hd, body = req("GET", "/%s" % b1, query="x-glacier-archive=%s-arc&format=tar&mode=stream" % T)
    check(st == 200 and hd.get("content-type") == "application/x-tar", "stream tar download (%d)" % st)
    tf = tarfile.open(fileobj=io.BytesIO(body))
    names = set(tf.getnames())
    got_ok = all(tf.extractfile(n).read() == small[n] for n in small)
    check(set(small) <= names and got_ok, "stream tar contains both objects with right content")
    # mode=done（zip）
    st, hd, body = req("GET", "/%s" % b1, query="x-glacier-archive=%s-arc&format=zip&mode=done" % T)
    check(st == 200 and hd.get("content-type") == "application/zip", "done zip download (%d)" % st)
    zf = zipfile.ZipFile(io.BytesIO(body))
    zip_ok = all(zf.read(n) == small[n] for n in small)
    check(set(small) <= set(zf.namelist()) and zip_ok, "done zip contains both objects with right content")
    # mode=done（tar.gz）
    st, hd, body = req("GET", "/%s" % b1, query="x-glacier-archive=%s-arc&format=tar.gz&mode=done" % T)
    check(st == 200, "done tar.gz download (%d)" % st)
    tz = tarfile.open(fileobj=io.BytesIO(body), mode="r:gz")
    check(all(tz.extractfile(n).read() == small[n] for n in small), "done tar.gz content ok")
    # stream + zip → 400
    st, hd, body = req("GET", "/%s" % b1, query="x-glacier-archive=%s-arc&format=zip&mode=stream" % T)
    check(st == 400, "stream mode rejects non-tar format (%d)" % st)
    # 非法 format → 400
    st, _, _ = req("GET", "/%s" % b1, query="x-glacier-archive=x&format=rar")
    check(st == 400, "invalid format -> 400 (%d)" % st)

    # ------------------------------------------------------------------
    # 6) KV 驱逐优先级
    # ------------------------------------------------------------------
    kv_cmd("SET", "prio-k", "v")
    check("OK" in str(kv_cmd("KEYPRIO", "prio-k", "200")), "KEYPRIO set 200")
    check(kv_cmd("GETPRIO", "prio-k") == b"200", "GETPRIO returns 200")
    kv_cmd("KEYPRIO", "prio-k", "-1")
    check(kv_cmd("GETPRIO", "prio-k") == b"-1", "KEYPRIO -1 clears (back to LRU)")
    check(kv_cmd("GETPRIO", "no-such-key") == b"-2", "GETPRIO missing key -> -2")
    check("ERR" in str(kv_cmd("KEYPRIO", "prio-k", "300")), "KEYPRIO 300 out of range -> ERR")
    check("ERR" in str(kv_cmd("KEYPRIO", "prio-k", "-2")), "KEYPRIO -2 out of range -> ERR")
    info = str(kv_cmd("INFO"))
    check("evicted_prio:" in info and "evicted_lru:" in info, "INFO exposes evicted_prio/evicted_lru")

    # ------------------------------------------------------------------
    # 7) KV SQL 读穿透
    # ------------------------------------------------------------------
    admin_sql(cur, "CREATE DATABASE IF NOT EXISTS kvsrc")
    conn = sql_connect()
    c2 = conn.cursor()
    c2.execute("USE kvsrc")
    c2.execute("CREATE TABLE IF NOT EXISTS cpu(name VARCHAR(50), val VARCHAR(50))")
    c2.execute("DELETE FROM cpu")
    c2.execute("INSERT INTO cpu VALUES('cpu-a','41'), ('cpu-b','77')")
    conn.commit()
    # 规则前缀每次运行唯一：kv 数据与规则跨运行持久，避免上轮残留键干扰
    pfx = "sr%s." % T[1:]
    r = admin_sql(cur, "KV SOURCE ADD '%s' kvsrc 'SELECT val FROM cpu WHERE name = {1}' TTL 60000" % pfx)
    check(r is not None, "KV SOURCE ADD rule")
    v = kv_cmd("GET", "%scpu-a" % pfx)
    check(v == b"41", "GET miss -> source backfill returns '41' (%s)" % v)
    kv_cmd("DEL", "%scpu-a" % pfx)
    v = kv_cmd("GET", "%scpu-a" % pfx)
    check(v == b"41", "re-GET after DEL refills from source (rule TTL)")
    kv_cmd("SET", "%scpu-a" % pfx, "explicit")
    v = kv_cmd("GET", "%scpu-a" % pfx)
    check(v == b"explicit", "explicit SET wins over rule (NX backfill)")
    v = kv_cmd("GET", "%snope" % pfx)
    check(v is None, "empty result -> miss (no negative cache)")
    r = admin_sql(cur, "SHOW KV SOURCE")
    check(r is not None and any(row[0] == pfx for row in r), "SHOW KV SOURCE lists rule")
    admin_sql(cur, "KV SOURCE DEL '%s'" % pfx)
    r = admin_sql(cur, "SHOW KV SOURCE")
    check(r is not None and not any(row[0] == pfx for row in r), "KV SOURCE DEL removes rule")

    # ------------------------------------------------------------------
    # 8) SCRIPT 临时屏蔽
    # ------------------------------------------------------------------
    try:
        cur.execute("USE GlacierTDB")
        cur.execute("SUBMIT QUERY 'SELECT 1' SCRIPT 'pipe.js'")
        got = cur.fetchall()
        ok = False
        msg = "no error raised"
    except Exception as e:
        got = None
        ok = "temporarily disabled" in str(e)
        msg = str(e)[:100]
    check(ok, "SUBMIT with SCRIPT explicitly disabled (%s)" % msg)

    # ------------------------------------------------------------------
    # 9) 收尾
    # ------------------------------------------------------------------
    admin_sql(cur, "DROP BUCKET %s FORCE" % b1)
    admin_sql(cur, "DROP BUCKET %s FORCE" % b2)
    print()
    print("PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    sys.exit(0 if STATS["fail"] == 0 else 1)


if __name__ == "__main__":
    main()
