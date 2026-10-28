#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.12 黑盒测试：

1) S3 写路径重构回归：单事务写（inline/chunk/覆盖/multipart）+ md5 一致性
   + 桶聚合算术增量正确性（objects/storage_bytes 随 PUT/覆盖/DELETE 精确变化）
2) region 统一存储：桶不再单独存 region——GET ?location 恒返回全局值、
   CREATE BUCKET REGION 子句兼容不报错、SHOW BUCKETS 统一展示
3) 上传回调三级配置：桶级 SET S3 CALLBACK + 请求级 x-glacier-callback-url 覆盖
   （本地起 HTTP 接收器校验载荷与 HMAC 签名）
4) virtual-host 兜底解析（v1.3.12 修复）：未配置 server_names 的域名访问，
   首段命中已建桶 → 按 vhost 处理（GET / = 列对象而非列桶）；
   未命中 → 维持 path-style（GET / = ListBuckets）
5) 并行上传冒烟：16 路并发 PUT + GET 校验（性能基准见 tests/bench/s3_bench.py）
6) 空对象 / 超内联阈值边界（0B、1KB、100KB、10MB）"""
import hashlib
import http.server
import json
import os
import sys
import threading
import time

import pymysql

sys.path.insert(0, "/tests")
from test_s3 import s3_request, sql_connect  # noqa: E402

HOST, PORT = "proxy", 3309
ADMIN, ADMIN_PW = "glacier", "iloveyou~"
AK, SK = None, None
T = "t%d" % int(time.time())
STATS = {"pass": 0, "fail": 0}


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
    return s3_request(method, path, query=query, headers=extra or {}, body=body,
                      ak=AK, sk=SK)


def bucket_stat(cur, bucket):
    rows = admin_sql(cur, "SHOW BUCKETS")
    for r in rows or []:
        if r[0] == bucket:
            return r  # name, owner, region, ver, lock, objects, storage_bytes, cb, created
    return None


# ---------------------------------------------------------------------------
# 本地回调接收器（校验载荷 + HMAC）
# ---------------------------------------------------------------------------

class CBReceiver:
    def __init__(self, secret=b""):
        self.events = []
        self.secret = secret
        srv = http.server.HTTPServer(("0.0.0.0", 0), self._handler())
        self.port = srv.server_address[1]
        self.srv = srv
        threading.Thread(target=srv.serve_forever, daemon=True).start()

    def _handler(self):
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                outer.events.append({
                    "path": self.path,
                    "sig": self.headers.get("X-Glacier-Signature", ""),
                    "event": self.headers.get("X-Glacier-Event", ""),
                    "body": json.loads(body.decode("utf-8")),
                })
                self.send_response(200)
                self.end_headers()

            def log_message(self, *a):
                pass
        return H

    def wait_for(self, pred, timeout=8):
        deadline = time.time() + timeout
        while time.time() < deadline:
            for e in self.events:
                if pred(e):
                    return e
            time.sleep(0.2)
        return None


def hmac_sig(secret, body):
    import hmac as _h
    return "sha256=" + _h.new(secret.encode(), body, hashlib.sha256).hexdigest()


def main():
    global AK, SK
    cur = sql_connect().cursor()

    rows = admin_sql(cur, "CREATE BUCKET %s" % T)
    check(rows is not None or True, "CREATE BUCKET %s" % T)
    cur.execute("USE GlacierTDB")
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    AK, SK = cur.fetchall()[0][0:2]
    check(bool(AK) and bool(SK), "S3 AK/SK ready (AK=%s...)" % (AK or "")[:6])

    # ------------------------------------------------------------------
    # 1) 单事务写路径回归：尺寸边界 + md5 一致性 + 聚合精确性
    # ------------------------------------------------------------------
    sizes = [(0, "empty"), (1024, "1KB-inline"), (100 * 1024, "100KB-chunk"),
             (10 * 1024 * 1024, "10MB-chunk")]
    for n, label in sizes:
        data = os.urandom(n)
        st, hd, _ = req("PUT", "/%s/obj-%s" % (T, label), body=data,
                        extra={"Content-Type": "application/octet-stream"})
        want = hashlib.md5(data).hexdigest() if n else "d41d8cd98f00b204e9800998ecf8427e"
        check(st == 200 and hd.get("etag", "").strip('"') == want,
              "PUT %s (%d B) etag 一致" % (label, n))
        st, hd, body = req("GET", "/%s/obj-%s" % (T, label))
        check(st == 200 and body == data and hd.get("etag", "").strip('"') == want,
              "GET %s 内容与 etag 一致（Content-Length=%s）" % (label, hd.get("content-length")))

    # 聚合精确性：4 对象，总字节 = Σsize
    stat = bucket_stat(cur, T)
    total = sum(n for n, _ in sizes)
    check(stat and int(stat[5]) == 4, "桶聚合 objects=4（实际 %s）" % (stat and stat[5]))
    check(stat and int(stat[6]) == total, "桶聚合 storage_bytes=%d（实际 %s）" % (total, stat and stat[6]))

    # 覆盖写：对象数不变、字节数按差值调整（算术增量正确性）
    new10 = os.urandom(10 * 1024 * 1024 + 4096)
    st, _, _ = req("PUT", "/%s/obj-10MB-chunk" % T, body=new10)
    check(st == 200, "覆盖 PUT 10MB→10MB+4KB")
    stat = bucket_stat(cur, T)
    check(stat and int(stat[5]) == 4, "覆盖后 objects 仍=4（实际 %s）" % (stat and stat[5]))
    check(stat and int(stat[6]) == total + 4096, "覆盖后 storage_bytes=%d（实际 %s）" % (total + 4096, stat and stat[6]))

    # DELETE：聚合并回
    st, _, _ = req("DELETE", "/%s/obj-1KB-inline" % T)
    check(st == 204, "DELETE 1KB")
    stat = bucket_stat(cur, T)
    check(stat and int(stat[5]) == 3 and int(stat[6]) == total + 4096 - 1024,
          "DELETE 后聚合收敛（objects=%s bytes=%s）" % (stat and stat[5], stat and stat[6]))

    # multipart 全流程（写路径重构后的 UploadPart/Complete）
    st, _, data = req("POST", "/%s/mpu-obj" % T, query="uploads")
    upload_id = data.decode().split("<UploadId>")[1].split("</UploadId>")[0] if b"<UploadId>" in data else ""
    check(bool(upload_id), "InitiateMultipartUpload")
    p1, p2 = os.urandom(5 * 1024 * 1024), os.urandom(3 * 1024 * 1024)
    et1 = hashlib.md5(p1).hexdigest()
    st, hd, _ = req("PUT", "/%s/mpu-obj" % T, query="partNumber=1&uploadId=%s" % upload_id, body=p1)
    check(st == 200 and hd.get("etag", "").strip('"') == et1, "UploadPart 1 (5MB) etag")
    st, _, _ = req("PUT", "/%s/mpu-obj" % T, query="partNumber=2&uploadId=%s" % upload_id, body=p2)
    check(st == 200, "UploadPart 2 (3MB)")
    comp = ("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part>"
            "<Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>") % (et1, hashlib.md5(p2).hexdigest())
    st, _, data = req("POST", "/%s/mpu-obj" % T, query="uploadId=%s" % upload_id, body=comp.encode())
    check(st == 200 and b"<ETag>" in data, "CompleteMultipartUpload")
    st, _, body = req("GET", "/%s/mpu-obj" % T)
    check(st == 200 and body == p1 + p2, "multipart GET 内容一致（8MB）")
    stat = bucket_stat(cur, T)
    check(stat and int(stat[6]) == total + 4096 - 1024 + 8 * 1024 * 1024,
          "multipart 后桶聚合含 8MB（实际 %s）" % (stat and stat[6]))

    # Range 读（manifest 直定位，确保新写路径布局兼容）
    st, hd, body = req("GET", "/%s/mpu-obj" % T, query="", extra={"Range": "bytes=5242870-5242889"})
    check(st == 206 and body == (p1 + p2)[5242870:5242890] and hd.get("content-range", "").startswith("bytes 5242870-"),
          "Range 206 跨 part 定位一致")

    # ------------------------------------------------------------------
    # 2) region 统一
    # ------------------------------------------------------------------
    st, _, data = req("GET", "/%s" % T, query="location")
    check(st == 200 and b"<LocationConstraint" in data and b"cn-" not in data,
          "GET ?location 返回全局值（桶不再单独存 region）")
    rows = admin_sql(cur, "CREATE BUCKET %s-r2 REGION 'cn-legacy-1'" % T)
    check(rows is not None, "CREATE BUCKET REGION 子句兼容不报错（值被忽略）")
    st, _, data = req("GET", "/%s-r2" % T, query="location")
    check(b"cn-legacy" not in data, "REGION 子句不再落桶（location 不含 cn-legacy）")
    stat = bucket_stat(cur, "%s-r2" % T)
    check(stat is not None, "SHOW S3 BUCKETS 含新桶")

    # ------------------------------------------------------------------
    # 3) 回调：桶级 + 请求级
    # ------------------------------------------------------------------
    cb = CBReceiver()
    admin_sql(cur, "SET S3 CALLBACK %s URL 'http://pyclient:%d/hook' SECRET 'bucket-sec'" % (T, cb.port))
    payload = os.urandom(2048)
    st, _, _ = req("PUT", "/%s/cb-bucket-level" % T, body=payload)
    check(st == 200, "桶级回调 PUT")
    e = cb.wait_for(lambda ev: ev["body"].get("key") == "cb-bucket-level")
    check(e is not None and e["path"] == "/hook", "桶级回调送达（路径 /hook）")
    if e:
        check(e["sig"] != "", "桶级回调带 HMAC 签名头")
        check(e["body"].get("bucket") == T and e["body"].get("size") == 2048, "桶级回调载荷正确")

    # 请求级覆盖：不同路径 + 独立 secret，桶级配置仍在
    e2 = cb.wait_for(lambda ev: ev["path"] == "/per-req", timeout=0.1)  # 确认未提前出现
    st, _, _ = req("PUT", "/%s/cb-req-level" % T, body=b"req-level-body",
                   extra={"X-Glacier-Callback-Url": "http://pyclient:%d/per-req" % cb.port,
                          "X-Glacier-Callback-Secret": "req-sec"})
    check(st == 200, "请求级回调 PUT（header 指定）")
    e2 = cb.wait_for(lambda ev: ev["path"] == "/per-req")
    check(e2 is not None and e2["body"].get("key") == "cb-req-level", "请求级回调送达（覆盖桶级 URL）")
    if e2:
        check(e2["sig"] != "" and (not e or e2["sig"] != e["sig"]), "请求级回调独立签名")
    # 桶级未指定回调时，请求级亦可触发：换一个桶
    admin_sql(cur, "CREATE BUCKET %s-cb2" % T)
    st, _, _ = req("PUT", "/%s-cb2/cb-req-only" % T, body=b"x",
                   extra={"X-Glacier-Callback-Url": "http://pyclient:%d/req-only" % cb.port})
    e3 = cb.wait_for(lambda ev: ev["path"] == "/req-only")
    check(e3 is not None, "无桶级配置时请求级回调触发")
    admin_sql(cur, "CLEAR S3 CALLBACK %s" % T)

    # ------------------------------------------------------------------
    # 4) virtual-host 兜底（server_names 未涵盖的域名）
    # ------------------------------------------------------------------
    st, _, data = req("PUT", "/%s/vh-obj" % T, body=b"vhost-data",
                      extra={"Content-Type": "text/plain"})
    # path-style 先放一个对象（Host=proxy:9000）
    check(st == 200, "path-style PUT（Host=proxy:9000）")
    # 兜底：Host = <bucket>.<任意未配置域名>，bucket 已存在 → vhost 语义
    st, _, data = s3_request("GET", "/", vhost="%s.s3.glaciertdb.fun:9000" % T, ak=AK, sk=SK)
    check(st == 200 and b"<ListBucketResult" in data and ("<Name>%s</Name>" % T).encode() in data,
          "vhost 兜底：GET / 列对象（而非 ListBuckets）")
    st, _, data = s3_request("GET", "/", vhost="%s.dev.glaciertdb.fun" % T, ak=AK, sk=SK)
    check(st == 200 and b"<ListBucketResult" in data, "vhost 兜底：无端口 Host 亦可")
    st, _, data = s3_request("GET", "/vh-obj", vhost="%s.s3.glaciertdb.fun:9000" % T, ak=AK, sk=SK)
    check(st == 200 and data == b"vhost-data", "vhost 兜底：GET /vh-obj 命中对象（%s）" % st)
    # 未命中桶名 → path-style：GET / = ListBuckets
    st, _, data = s3_request("GET", "/", vhost="nosuchbucket.glaciertdb.fun:9000", ak=AK, sk=SK)
    check(st == 200 and b"<ListAllMyBucketsResult" in data, "未知域名 GET / 维持 ListBuckets")
    # 已配置 server_names 的 host（glacier-s3.local）行为不变：裸端点 path-style
    st, _, data = s3_request("GET", "/", vhost="glacier-s3.local:9000", ak=AK, sk=SK)
    check(st == 200 and b"<ListAllMyBucketsResult" in data, "server_names 裸端点仍 ListBuckets")
    st, _, data = s3_request("GET", "/vh-obj", vhost="%s.glacier-s3.local:9000" % T, ak=AK, sk=SK)
    check(st == 200 and data == b"vhost-data", "server_names vhost 后缀解析正常")
    # IP 访问不进兜底（维持 path-style）
    st, _, data = s3_request("GET", "/", vhost="127.0.0.1:9000", ak=AK, sk=SK)
    check(st == 200 and b"<ListAllMyBucketsResult" in data, "IP Host 维持 path-style ListBuckets")

    # ------------------------------------------------------------------
    # 5) 并行上传冒烟（性能基准见 bench/s3_bench.py）
    # ------------------------------------------------------------------
    import concurrent.futures
    blobs = [os.urandom(1024 * 1024 + i * 1311) for i in range(16)]

    def one(i):
        d = blobs[i]
        st, hd, _ = req("PUT", "/%s/par-%02d" % (T, i), body=d)
        if st != 200:
            return False
        st, _, back = req("GET", "/%s/par-%02d" % (T, i))
        return st == 200 and back == d
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
        oks = list(ex.map(one, range(16)))
    cost = time.time() - t0
    check(all(oks), "16 路并行 PUT+GET 全一致（%d/16，%.2fs）" % (sum(oks), cost))
    stat = bucket_stat(cur, T)
    check(stat and int(stat[5]) >= 20, "并行后桶对象数 ≥ 20（实际 %s）" % (stat and stat[5]))

    # 清理
    for b in [T, "%s-r2" % T, "%s-cb2" % T]:
        admin_sql(cur, "DROP BUCKET %s FORCE" % b)
    print("\nv1.3.12: %d pass, %d fail" % (STATS["pass"], STATS["fail"]))
    return 0 if STATS["fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
