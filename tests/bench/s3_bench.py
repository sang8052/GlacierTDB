#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""S3 上传/下载性能基准（v1.3.12；素材 tests/imgs 或随机数据）。

用法（在被测容器网络内）：
    python s3_bench.py <s3_host> <s3_port> <ak> <sk> [--imgs DIR] [--n N] [--par P]

输出：串行 / 并行(P 路) PUT 与 GET 的吞吐（MB/s）与平均延迟，
用于验证 v1.3.12 写路径优化（单事务化 / 元数据缓存 / TouchKey 缓冲）。
对比基线：v1.3.11 全局写锁 6 次提交版本。
"""
import concurrent.futures
import hashlib
import hmac
import datetime
import http.client
import os
import sys
import time

def _h(key, msg):
    if isinstance(key, str):
        key = key.encode()
    return hmac.new(key, msg.encode() if isinstance(msg, str) else msg, hashlib.sha256).digest()

class S3:
    def __init__(self, host, port, ak, sk, region="us-east-1"):
        self.host, self.port, self.ak, self.sk, self.region = host, port, ak, sk, region

    def req(self, method, path, query="", headers=None, body=b""):
        host = "%s:%d" % (self.host, self.port)
        t = datetime.datetime.now(datetime.timezone.utc)
        amzdate, datestamp = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
        h = dict(headers or {})
        h["Host"] = host
        h["x-amz-date"] = amzdate
        payload_hash = hashlib.sha256(body).hexdigest()
        h["x-amz-content-sha256"] = payload_hash
        signed = sorted(k.lower() for k in h)
        canon_headers = "".join("%s:%s\n" % (k, h[next(x for x in h if x.lower() == k)]) for k in signed)
        pairs = []
        for part in query.split("&"):
            if part:
                k, _, v = part.partition("=")
                pairs.append((k, v))
        canon_qs = "&".join("%s=%s" % kv for kv in sorted(pairs))
        canon = "\n".join([method, path, canon_qs, canon_headers, ";".join(signed), payload_hash])
        scope = "%s/%s/s3/aws4_request" % (datestamp, self.region)
        sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canon.encode()).hexdigest()])
        sig = hmac.new(_h(_h(_h(_h(("AWS4" + self.sk).encode(), datestamp), self.region), "s3"), "aws4_request"),
                      sts.encode(), hashlib.sha256).hexdigest()
        h["Authorization"] = ("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
                              % (self.ak, scope, ";".join(signed), sig))
        c = http.client.HTTPConnection(self.host, self.port, timeout=300)
        c.request(method, path + (("?" + query) if query else ""), body=body, headers=h)
        r = c.getresponse()
        data = r.read()
        c.close()
        if r.status not in (200, 204):
            raise RuntimeError("s3 %s %s -> %d %s" % (method, path, r.status, data[:120]))
        return data


def bench(s3, bucket, blobs, par):
    def put(i):
        return s3.req("PUT", "/%s/bench-%04d" % (bucket, i), body=blobs[i],
                      headers={"Content-Type": "application/octet-stream"})
    def get(i):
        return len(s3.req("GET", "/%s/bench-%04d" % (bucket, i)))
    total = sum(len(b) for b in blobs)
    out = {}
    for name, fn, workers in [("serial-put", put, 1), ("par-put", put, par),
                              ("serial-get", get, 1), ("par-get", get, par)]:
        idx = list(range(len(blobs)))
        t0 = time.time()
        if workers == 1:
            for i in idx:
                fn(i)
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
                list(ex.map(fn, idx))
        cost = time.time() - t0
        out[name] = (total / cost, cost, len(blobs) / cost)
        print("%-11s %8.1f MB/s  %6.1f ops/s  (%d objs, %.2fs)" % (name, total / cost / 1e6, len(blobs) / cost, len(blobs), cost))
    return out


def main():
    host, port, ak, sk = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    args = sys.argv[5:]
    imgs = args[args.index("--imgs") + 1] if "--imgs" in args else ""
    n = int(args[args.index("--n") + 1]) if "--n" in args else 32
    par = int(args[args.index("--par") + 1]) if "--par" in args else 16
    size_mb = float(args[args.index("--size") + 1]) if "--size" in args else 5.0
    bucket = args[args.index("--bucket") + 1] if "--bucket" in args else "bench-v1312"

    if imgs and os.path.isdir(imgs):
        files = sorted(os.listdir(imgs))[:n]
        blobs = [open(os.path.join(imgs, f), "rb").read() for f in files]
        print("dataset: %d files from %s (%d bytes total)" % (len(blobs), imgs, sum(len(b) for b in blobs)))
    else:
        sz = int(size_mb * 1024 * 1024)
        blobs = [os.urandom(sz) for _ in range(n)]
        print("dataset: %d random blobs x %.2fMB" % (n, size_mb))

    s3 = S3(host, int(port), ak, sk)
    try:
        s3.req("PUT", "/%s" % bucket)
    except Exception:
        pass
    res = bench(s3, bucket, blobs, par)
    for i in range(len(blobs)):
        try:
            s3.req("DELETE", "/%s/bench-%04d" % (bucket, i))
        except Exception:
            pass
    print("SUMMARY put-par=%.1fMB/s get-par=%.1fMB/s" % (res["par-put"][0] / 1e6, res["par-get"][0] / 1e6))
    return 0


if __name__ == "__main__":
    sys.exit(main())
