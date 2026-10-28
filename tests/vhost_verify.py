#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""virtual-host 域名部署语义验证（v1.3.12 需求⑤，独立于 run_all 手动运行）：

    python vhost_verify.py <endpoint_host> [port] [--ak AK --sk SK --https]

验证点（bucket + endpoint 解析）：
  1. endpoint 域名（如 s3.dev.glaciertdb.fun）GET / → ListAllMyBucketsResult（列桶）
  2. bucket 域名（bucket.s3.dev.glaciertdb.fun）GET / → ListBucketResult（列对象，
     而非误判 ListBuckets —— v1.3.12 修复点）
  3. bucket 域名 PUT/GET /key 正常（virtual-host 寻址写读）
  4. path-style 与 virtual-host 读到同一对象
  5. boto3（若安装）addressing_style='virtual' 全链路复验
"""
import datetime
import hashlib
import hmac
import http.client
import ssl
import sys

STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def _h(key, msg):
    if isinstance(key, str):
        key = key.encode()
    return hmac.new(key, msg.encode() if isinstance(msg, str) else msg, hashlib.sha256).digest()


def sign(method, path, query="", headers=None, body=b"", ak="", sk="", region="us-east-1", host=""):
    t = datetime.datetime.now(datetime.timezone.utc)
    amzdate, datestamp = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
    h = dict(headers or {})
    h["Host"] = host
    h["x-amz-date"] = amzdate
    ph = hashlib.sha256(body).hexdigest()
    h["x-amz-content-sha256"] = ph
    signed = sorted(k.lower() for k in h)
    canon_h = "".join("%s:%s\n" % (k, h[next(x for x in h if x.lower() == k)]) for k in signed)
    pairs = []
    for part in query.split("&"):
        if part:
            k, _, v = part.partition("=")
            pairs.append((k, v))
    canon_qs = "&".join("%s=%s" % kv for kv in sorted(pairs))
    canon = "\n".join([method, path, canon_qs, canon_h, ";".join(signed), ph])
    scope = "%s/%s/s3/aws4_request" % (datestamp, region)
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canon.encode()).hexdigest()])
    sig = hmac.new(_h(_h(_h(_h(("AWS4" + sk).encode(), datestamp), region), "s3"), "aws4_request"),
                  sts.encode(), hashlib.sha256).hexdigest()
    h["Authorization"] = ("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
                          % (ak, scope, ";".join(signed), sig))
    return h


def req(host, port, method, path, query="", headers=None, body=b"", ak="", sk="",
        region="us-east-1", https=False, insecure=False):
    host_hdr = "%s:%d" % (host, port)
    hdrs = sign(method, path, query, headers, body, ak, sk, region, host_hdr)
    if https:
        if insecure:
            ctx = ssl._create_unverified_context()
        else:
            ctx = ssl.create_default_context()
        c = http.client.HTTPSConnection(host, port, timeout=30, context=ctx)
    else:
        c = http.client.HTTPConnection(host, port, timeout=30)
    c.request(method, path + (("?" + query) if query else ""), body=body, headers=hdrs)
    r = c.getresponse()
    data = r.read()
    c.close()
    return r.status, dict((k.lower(), v) for k, v in r.getheaders()), data


def main():
    ep = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 443
    a = sys.argv[3:]
    ak = a[a.index("--ak") + 1] if "--ak" in a else ""
    sk = a[a.index("--sk") + 1] if "--sk" in a else ""
    bucket = a[a.index("--bucket") + 1] if "--bucket" in a else "vhost-verify"
    https = "--https" in a
    insecure = "--insecure" in a
    region = a[a.index("--region") + 1] if "--region" in a else "us-east-1"
    if not ak or not sk:
        print("需要 --ak/--sk")
        return 2
    vh = "%s.%s" % (bucket, ep)

    # ① endpoint 域名：列桶
    st, _, data = req(ep, port, "GET", "/", ak=ak, sk=sk, region=region, https=https, insecure=insecure)
    check(st == 200 and b"ListAllMyBucketsResult" in data,
          "endpoint 域名 GET / → ListBuckets（%d）" % st)

    # ② bucket 域名：列对象（修复点：此前被误判 ListBuckets）
    st, _, data = req(vh, port, "GET", "/", ak=ak, sk=sk, region=region, https=https, insecure=insecure)
    check(st == 200 and b"ListBucketResult" in data and b"ListAllMyBucketsResult" not in data,
          "bucket 域名 GET / → ListObjects（vhost 修复点，%d）" % st)

    # ③ virtual-host 写读
    payload = b"vhost-verify-%s" % bucket.encode()
    st, _, _ = req(vh, port, "PUT", "/probe.txt", headers={"Content-Type": "text/plain"},
                   body=payload, ak=ak, sk=sk, region=region, https=https, insecure=insecure)
    check(st == 200, "bucket 域名 PUT /probe.txt（%d）" % st)
    st, _, data = req(vh, port, "GET", "/probe.txt", ak=ak, sk=sk, region=region, https=https, insecure=insecure)
    check(st == 200 and data == payload, "bucket 域名 GET /probe.txt 内容一致")

    # ④ path-style 读同一对象
    st, _, data = req(ep, port, "GET", "/%s/probe.txt" % bucket, ak=ak, sk=sk, region=region, https=https, insecure=insecure)
    check(st == 200 and data == payload, "path-style GET 读到同一对象（跨寻址一致）")

    # ⑤ boto3 复验（可选）
    try:
        import boto3
        from botocore.config import Config
        import urllib.parse
        url = "%s://%s" % ("https" if https else "http", ("%s:%d" % (ep, port)) if port not in (80, 443) else ep)
        s3 = boto3.client("s3", endpoint_url=url, aws_access_key_id=ak, aws_secret_access_key=sk,
                          region_name=region, verify=(not insecure),
                          config=Config(s3={"addressing_style": "virtual"}, retries={"max_attempts": 1}))
        r = s3.list_objects_v2(Bucket=bucket, Prefix="probe")
        keys = [o["Key"] for o in r.get("Contents", [])]
        check("probe.txt" in keys, "boto3 virtual addressing list_objects_v2 命中")
        body = s3.get_object(Bucket=bucket, Key="probe.txt")["Body"].read()
        check(body == payload, "boto3 virtual addressing get_object 一致")
    except ImportError:
        print("[SKIP] boto3 未安装，跳过 SDK 复验")

    print("\nvhost verify: %d pass, %d fail" % (STATS["pass"], STATS["fail"]))
    return 0 if STATS["fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
