#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.1.0 S3 对象存储黑盒测试：
1) 管理面 SQL：CREATE BUCKET / CREATE S3 KEY（AK 32 位 AS 前缀 / SK 48 位 SK 前缀）/
   SHOW S3 KEYS / DISABLE·ENABLE·RENEW·DROP S3 KEY
2) SigV4 鉴权：PUT/GET/HEAD/DELETE 对象、ListObjectsV2、签名错误 403、预签名 URL
3) 多版本：版本链 / ListObjectVersions / ?versionId= / 删除标记
4) 对象锁：S3 RETAIN（GOVERNANCE 旁路）/ LEGAL_HOLD
5) 请求策略：UA 条件不满足 403，满足放行；IP / 自定义 header 条件
6) 图片：base64 上传 / 缩放 / 格式转换 / base64 输出
7) 大对象分块存储（> inline 阈值）与 md5 一致性
8) 限速：单连接下载限速生效（桶级覆盖）
9) virtual-host / 自定义域名（自签发证书接入）
10) 计费：s3_usage 记账 / EXPORT S3 USAGE / HTTP 导出接口
11) 上传回调（HMAC 签名校验）
12) 生命周期配置
"""
import base64
import datetime
import hashlib
import hmac
import http.client
import http.server
import json
import os
import threading
import time

import pymysql

HOST, PORT, USER, PASSWORD = "proxy", 3309, "glacier", "iloveyou~"
S3_HOST, S3_PORT = "proxy", 9000
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def sql_connect():
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database="backup", charset="utf8mb4", autocommit=True,
                           read_timeout=120, write_timeout=120)


# ---------------------------------------------------------------------------
# SigV4 签名（AWS 标准算法）
# ---------------------------------------------------------------------------

def _hmac(key, msg):
    if isinstance(key, str):
        key = key.encode("utf-8")
    return hmac.new(key, msg.encode("utf-8") if isinstance(msg, str) else msg, hashlib.sha256).digest()


def sign_request(method, path, query="", headers=None, body=b"", ak="", sk="",
                 region="us-east-1", host=None, sign_time=None):
    host = host or ("%s:%d" % (S3_HOST, S3_PORT))
    headers = dict(headers or {})
    t = sign_time or datetime.datetime.now(datetime.timezone.utc)
    amzdate = t.strftime("%Y%m%dT%H%M%SZ")
    datestamp = t.strftime("%Y%m%d")

    canon_uri = "/".join(seg if seg == "" else
                         "".join("%%%02X" % b if not (chr(b) in
                                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")
                                else chr(b) for b in seg.encode("utf-8"))
                         for seg in path.split("/"))
    pairs = []
    for part in query.split("&"):
        if not part:
            continue
        k, _, v = part.partition("=")
        pairs.append((k, v))
    def enc(s):
        return "".join("%%%02X" % b if not (chr(b) in
                       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")
                       else chr(b) for b in s.encode("utf-8"))
    pairs = sorted((enc(k), enc(v)) for k, v in pairs)
    canon_qs = "&".join("%s=%s" % (k, v) for k, v in pairs)

    payload_hash = hashlib.sha256(body).hexdigest()

    hdrs = {k.lower(): str(v).strip() for k, v in headers.items()}
    hdrs["host"] = host
    hdrs["x-amz-date"] = amzdate
    hdrs["x-amz-content-sha256"] = payload_hash
    signed = sorted(hdrs.keys())
    canon_headers = "".join("%s:%s\n" % (h, hdrs[h]) for h in signed)
    canon_req = "\n".join([method, canon_uri, canon_qs, canon_headers,
                           ";".join(signed), payload_hash])
    scope = "%s/%s/s3/aws4_request" % (datestamp, region)
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canon_req.encode()).hexdigest()])
    k = _hmac(_hmac(_hmac(_hmac("AWS4" + sk, datestamp), region), "s3"), "aws4_request")
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()

    out = dict(hdrs)
    out["Authorization"] = ("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
                            % (ak, scope, ";".join(signed), sig))
    return out


def s3_request(method, path, query="", headers=None, body=b"", ak="", sk="",
               region="us-east-1", vhost=None, timeout=60):
    """vhost = 自定义 Host 头（virtual-host 模式）；连接始终指向 S3_HOST:S3_PORT。"""
    host_hdr = vhost or ("%s:%d" % (S3_HOST, S3_PORT))
    hdrs = sign_request(method, path, query, headers, body, ak, sk, region, host_hdr)
    conn = http.client.HTTPConnection(S3_HOST, S3_PORT, timeout=timeout)
    url = path + (("?" + query) if query else "")
    conn.request(method, url, body=body, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    out = (resp.status, dict((k.lower(), v) for k, v in resp.getheaders()), data)
    conn.close()
    return out


def s3_presigned_get(path, query_params, ak, sk, region="us-east-1", expires=600):
    """生成预签名 GET URL 并请求（无 Authorization 头）。"""
    t = datetime.datetime.now(datetime.timezone.utc)
    amzdate = t.strftime("%Y%m%dT%H%M%SZ")
    datestamp = t.strftime("%Y%m%d")
    scope = "%s/%s/s3/aws4_request" % (datestamp, region)
    def enc(s):
        return "".join("%%%02X" % b if not (chr(b) in
                       "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~")
                       else chr(b) for b in s.encode("utf-8"))
    params = dict(query_params)
    params["X-Amz-Algorithm"] = "AWS4-HMAC-SHA256"
    params["X-Amz-Credential"] = "%s/%s" % (ak, scope)
    params["X-Amz-Date"] = amzdate
    params["X-Amz-Expires"] = str(expires)
    params["X-Amz-SignedHeaders"] = "host"
    pairs = sorted((enc(k), enc(v)) for k, v in params.items())
    canon_qs = "&".join("%s=%s" % (k, v) for k, v in pairs)
    host = "%s:%d" % (S3_HOST, S3_PORT)
    canon_req = "\n".join(["GET", path, canon_qs, "host:%s\n" % host, "host", "UNSIGNED-PAYLOAD"])
    sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canon_req.encode()).hexdigest()])
    k = _hmac(_hmac(_hmac(_hmac("AWS4" + sk, datestamp), region), "s3"), "aws4_request")
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    url = path + "?" + canon_qs + "&X-Amz-Signature=" + sig
    conn = http.client.HTTPConnection(S3_HOST, S3_PORT, timeout=60)
    conn.request("GET", url, headers={"Host": host})
    resp = conn.getresponse()
    data = resp.read()
    out = (resp.status, dict(resp.getheaders()), data)
    conn.close()
    return out


# ---------------------------------------------------------------------------
# 测试素材
# ---------------------------------------------------------------------------

# 生成合法的 RGB PNG（stdlib 构造，保证可被 image.Decode 解码）
def make_png(w, h, rgb=(255, 0, 0)):
    import struct
    import zlib

    def chunk(typ, data):
        return (struct.pack(">I", len(data)) + typ + data +
                struct.pack(">I", zlib.crc32(typ + data) & 0xFFFFFFFF))

    raw = b"".join(b"\x00" + bytes(rgb) * w for _ in range(h))
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", w, h, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw))
            + chunk(b"IEND", b""))


PNG_2X2 = make_png(2, 2)


def main() -> int:
    conn = sql_connect()
    cur = conn.cursor()
    cur.execute("USE GlacierTDB")

    # ---------- 1. 管理面：桶与密钥 ----------
    cur.execute("SHOW BUCKETS")
    for leftover in ("s3demo", "s3ver", "s3demo2"):
        try:
            cur.execute("DROP BUCKET %s FORCE" % leftover)  # 清理上次残留
        except pymysql.MySQLError:
            pass
    cur.execute("CREATE BUCKET s3demo OWNER 'glacier'")
    check(True, "CREATE BUCKET s3demo")
    cur.execute("CREATE BUCKET s3ver OWNER 'glacier' VERSIONING ON OBJECT_LOCK ON")
    check(True, "CREATE BUCKET s3ver（VERSIONING + OBJECT_LOCK）")

    cur.execute("SELECT * FROM s3_buckets")
    rows = cur.fetchall()
    check(any(r[0] == "s3demo" and r[1] == "glacier" for r in rows), "SELECT * FROM s3_buckets 可查（%d 行）" % len(rows))

    cur.execute("CREATE S3 KEY FOR 'glacier'")
    row = cur.fetchone()
    ak, sk = row[0], row[1]
    check(len(ak) == 32 and ak.startswith("AS"), "AK 32 位（前缀 AS）：%s..." % ak[:6])
    check(len(sk) == 48 and sk.startswith("SK"), "SK 48 位（前缀 SK），签发时明文可见一次")

    cur.execute("SHOW S3 KEYS")
    keys = cur.fetchall()
    check(any(k[0] == ak for k in keys), "SHOW S3 KEYS 含新密钥")
    check(all(len(k) < 3 or not str(k[3]).startswith("SK") for k in keys), "SHOW S3 KEYS 不泄露 SK 明文")

    # ---------- 2. SigV4 对象读写 ----------
    body = b"hello glacier s3"
    st, hd, data = s3_request("PUT", "/s3demo/hello.txt", headers={"Content-Type": "text/plain"},
                              body=body, ak=ak, sk=sk)
    check(st == 200 and "etag" in hd, "PUT 对象 200 + ETag（%s）" % st)

    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk=sk)
    check(st == 200 and data == body, "GET 对象内容一致")
    check(hd.get("content-type", "").startswith("text/plain"), "Content-Type 透传")

    st, hd, data = s3_request("HEAD", "/s3demo/hello.txt", ak=ak, sk=sk)
    check(st == 200 and hd.get("content-length") == str(len(body)), "HEAD 对象 Content-Length")

    st, hd, data = s3_request("GET", "/s3demo/", query="list-type=2", ak=ak, sk=sk)
    check(st == 200 and b"hello.txt" in data, "ListObjectsV2 列出对象")

    st, hd, data = s3_request("GET", "/s3demo/", query="list-type=2&prefix=hello", ak=ak, sk=sk)
    check(st == 200 and b"hello.txt" in data, "ListObjectsV2 prefix 过滤")

    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk="SK" + "x" * 46)
    check(st == 403 and b"SignatureDoesNotMatch" in data, "错误 SK → 403 SignatureDoesNotMatch")

    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak="AS" + "X" * 30, sk=sk)
    check(st == 403 and b"InvalidAccessKeyId" in data, "未知 AK → 403 InvalidAccessKeyId")

    st, hd, data = s3_request("GET", "/s3demo/hello.txt", region="cn-north-99", ak=ak, sk=sk)
    check(st == 200, "region '*' 配置下任意 region 签名可过")

    # 预签名 URL
    st, hd, data = s3_presigned_get("/s3demo/hello.txt", {}, ak, sk)
    check(st == 200 and data == body, "预签名 URL GET 成功")

    # 大对象（> 64KB inline 阈值 → 分块表）
    big = os.urandom(300 * 1024)
    st, hd, data = s3_request("PUT", "/s3demo/big.bin", body=big, ak=ak, sk=sk, timeout=120)
    check(st == 200, "PUT 300KB 大对象（分块存储）")
    st, hd, data = s3_request("GET", "/s3demo/big.bin", ak=ak, sk=sk, timeout=120)
    check(st == 200 and hashlib.md5(data).hexdigest() == hashlib.md5(big).hexdigest(),
          "GET 大对象 md5 一致（%d 字节）" % len(data))

    # COPY
    st, hd, data = s3_request("PUT", "/s3demo/hello2.txt",
                              headers={"x-amz-copy-source": "/s3demo/hello.txt"}, body=b"", ak=ak, sk=sk)
    check(st == 200 and b"CopyObjectResult" in data, "x-amz-copy-source 复制对象")

    # ---------- 3. 多版本 ----------
    st, hd, data = s3_request("PUT", "/s3ver/v.txt", body=b"v1", ak=ak, sk=sk)
    v1 = hd.get("x-amz-version-id", "")
    st, hd, data = s3_request("PUT", "/s3ver/v.txt", body=b"v2", ak=ak, sk=sk)
    v2 = hd.get("x-amz-version-id", "")
    check(v1 != "" and v2 != "" and v1 != v2, "版本链：两次 PUT 不同 versionId")

    st, hd, data = s3_request("GET", "/s3ver/v.txt", ak=ak, sk=sk)
    check(st == 200 and data == b"v2", "GET 返回最新版本")
    st, hd, data = s3_request("GET", "/s3ver/v.txt", query="versionId=" + v1, ak=ak, sk=sk)
    check(st == 200 and data == b"v1", "GET ?versionId= 返回历史版本")

    st, hd, data = s3_request("GET", "/s3ver/", query="versions", ak=ak, sk=sk)
    check(st == 200 and b"<Version>" in data and b"v.txt" in data, "ListObjectVersions 列出版本链")

    # ---------- 4. 对象锁 ----------
    cur.execute("S3 RETAIN s3ver/v.txt UNTIL '2099-01-01' GOVERNANCE")
    st, hd, data = s3_request("DELETE", "/s3ver/v.txt", ak=ak, sk=sk)
    check(st == 403 and b"AccessDenied" in data, "GOVERNANCE 保留期内 DELETE 403")

    st, hd, data = s3_request("PUT", "/s3ver/x.txt", body=b"x1", ak=ak, sk=sk)
    cur.execute("S3 RETAIN s3ver/x.txt UNTIL '2099-01-01' GOVERNANCE")
    st, hd, data = s3_request("DELETE", "/s3ver/x.txt",
                              headers={"x-amz-bypass-governance-retention": "true"}, ak=ak, sk=sk)
    check(st in (200, 204), "GOVERNANCE 可旁路删除（x-amz-bypass-governance-retention）")

    cur.execute("S3 RETAIN s3ver/v.txt UNTIL '2099-01-01' COMPLIANCE")
    st, hd, data = s3_request("DELETE", "/s3ver/v.txt",
                              headers={"x-amz-bypass-governance-retention": "true"}, ak=ak, sk=sk)
    check(st == 403, "COMPLIANCE 保留期内不可删除")

    # LEGAL_HOLD：非版本桶覆盖写被锁拒绝
    cur.execute("S3 LEGAL_HOLD s3demo/hello.txt ON")
    st, hd, data = s3_request("PUT", "/s3demo/hello.txt", body=b"overwrite", ak=ak, sk=sk)
    check(st == 403, "LEGAL_HOLD ON 时覆盖写 403")
    cur.execute("S3 LEGAL_HOLD s3demo/hello.txt OFF")

    # 版本开启：DELETE 产生删除标记
    st, hd, data = s3_request("PUT", "/s3ver/w.txt", body=b"w1", ak=ak, sk=sk)
    st, hd, data = s3_request("DELETE", "/s3ver/w.txt", ak=ak, sk=sk)
    check(st in (200, 204) and hd.get("x-amz-delete-marker", "") == "true", "版本开启：DELETE 产生删除标记")
    st, hd, data = s3_request("GET", "/s3ver/w.txt", ak=ak, sk=sk)
    check(st == 404, "删除标记后 GET 404")
    st, hd, data = s3_request("GET", "/s3ver/", query="versions", ak=ak, sk=sk)
    check(b"DeleteMarker" in data, "ListObjectVersions 可见删除标记")

    # ---------- 5. 请求策略条件 ----------
    pol = json.dumps({"ua": "glacier-test/*", "headers": {"x-app": "iot"}})
    cur.execute("SET S3 POLICY KEY '%s' '%s'" % (ak, pol))
    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk=sk)
    check(st == 403 and b"AccessDenied" in data, "策略条件不满足（UA 不匹配）→ 403")
    st, hd, data = s3_request("GET", "/s3demo/hello.txt",
                              headers={"User-Agent": "glacier-test/1.0", "x-app": "iot"}, ak=ak, sk=sk)
    check(st == 200, "UA + 自定义 header 条件满足 → 放行")
    st, hd, data = s3_request("GET", "/s3demo/hello.txt",
                              headers={"User-Agent": "glacier-test/1.0", "x-app": "other"}, ak=ak, sk=sk)
    check(st == 403, "自定义 header 条件不满足 → 403")
    cur.execute("CLEAR S3 POLICY KEY '%s'" % ak)
    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk=sk)
    check(st == 200, "清除策略后恢复")

    # ---------- 6. 图片处理 ----------
    st, hd, data = s3_request("PUT", "/s3demo/pic.png", query="x-gimg=base64",
                              headers={"Content-Type": "text/plain"},
                              body=base64.b64encode(PNG_2X2), ak=ak, sk=sk)
    check(st == 200, "base64 图片直传（解码后二进制落盘）")
    st, hd, data = s3_request("GET", "/s3demo/pic.png", ak=ak, sk=sk)
    check(st == 200 and data == PNG_2X2, "上传解码正确（二进制与原图一致）")

    st, hd, data = s3_request("GET", "/s3demo/pic.png",
                              query="x-gimg-resize=8,8&x-gimg-format=png", ak=ak, sk=sk)
    check(st == 200 and hd.get("content-type", "").startswith("image/png") and data[:8] == b"\x89PNG\r\n\x1a\n",
          "图片缩放 + 格式转换输出 PNG")

    st, hd, data = s3_request("GET", "/s3demo/pic.png",
                              query="x-gimg-resize=8,8&x-gimg-format=jpeg", ak=ak, sk=sk)
    check(st == 200 and data[:2] == b"\xff\xd8", "图片格式转换输出 JPEG")

    st, hd, data = s3_request("GET", "/s3demo/pic.png", query="x-gimg=base64", ak=ak, sk=sk)
    check(st == 200 and base64.b64decode(data) == PNG_2X2, "图片转 base64 输出")

    st, hd, data = s3_request("GET", "/s3demo/pic.png", query="x-gimg-crop=0,0,1,1&x-gimg-format=png", ak=ak, sk=sk)
    check(st == 200 and data[:8] == b"\x89PNG\r\n\x1a\n", "图片裁剪")

    # ---------- 7. 下载限速（桶级覆盖） ----------
    cur.execute("ALTER BUCKET s3demo LIMIT CONN_BPS = 102400")  # 100KB/s
    big2 = os.urandom(250 * 1024)
    s3_request("PUT", "/s3demo/lim.bin", body=big2, ak=ak, sk=sk, timeout=120)
    t0 = time.time()
    st, hd, data = s3_request("GET", "/s3demo/lim.bin", ak=ak, sk=sk, timeout=120)
    elapsed = time.time() - t0
    check(st == 200 and len(data) == len(big2), "限速下载完成")
    check(elapsed >= 1.0, "单连接限速生效（250KB@100KB/s，实际 %.1fs）" % elapsed)
    cur.execute("ALTER BUCKET s3demo LIMIT CONN_BPS = 0")

    # ---------- 7b. S3 API 建桶 / 删桶 ----------
    st, hd, data = s3_request("PUT", "/s3demo2", body=b"", ak=ak, sk=sk)
    check(st == 200, "PUT /bucket 创建桶（S3 API）")
    s3_request("PUT", "/s3demo2/o.txt", body=b"o", ak=ak, sk=sk)
    st, hd, data = s3_request("DELETE", "/s3demo2", ak=ak, sk=sk)
    check(st == 409 and b"BucketNotEmpty" in data, "非空桶 DELETE 409 BucketNotEmpty")
    st, hd, data = s3_request("DELETE", "/s3demo2/o.txt", ak=ak, sk=sk)
    check(st in (200, 204), "DELETE 对象")
    st, hd, data = s3_request("DELETE", "/s3demo2", ak=ak, sk=sk)
    check(st in (200, 204), "空桶 DELETE 成功")
    st, hd, data = s3_request("GET", "/s3demo2/o.txt", ak=ak, sk=sk)
    check(st == 404 and b"NoSuchBucket" in data, "桶不存在 → NoSuchBucket")
    try:
        cur.execute("DROP BUCKET s3ver")
        check(False, "非空桶 SQL DROP BUCKET 应报错")
    except pymysql.MySQLError as e:
        check(e.args[0] == 1051, "SQL DROP BUCKET 非空拒绝（1051）")

    # ---------- 8. virtual-host / 自定义域名 ----------
    st, hd, data = s3_request("GET", "/hello.txt", ak=ak, sk=sk, vhost="s3demo.glacier-s3.local:9000")
    check(st == 200 and data == body, "virtual-host 模式（bucket.endpoint）")

    cur.execute("ALTER BUCKET s3demo ADD DOMAIN 'img.s3test.local' ISSUE")
    check(True, "自定义域名接入 + 自签发证书")
    st, hd, data = s3_request("GET", "/hello.txt", ak=ak, sk=sk, vhost="img.s3test.local:9000")
    check(st == 200 and data == body, "自定义域名访问 bucket")
    cur.execute("SHOW S3 DOMAINS")
    domains = cur.fetchall()
    check(any(d[0] == "img.s3test.local" and d[1] == "s3demo" for d in domains), "SHOW S3 DOMAINS 可查")

    # ---------- 9. 上传回调（HMAC）----------
    got = {}

    class CB(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            body_ = self.rfile.read(n)
            got["body"] = body_
            got["sig"] = self.headers.get("X-Glacier-Signature", "")
            self.send_response(200)
            self.end_headers()

        def log_message(self, *a):
            pass

    srv = http.server.HTTPServer(("0.0.0.0", 9001), CB)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    cur.execute("SET S3 CALLBACK s3demo URL 'http://pyclient:9001/cb' SECRET 'cbsecret'")
    s3_request("PUT", "/s3demo/cb.txt", body=b"callback-me", ak=ak, sk=sk)
    for _ in range(30):
        if "body" in got:
            break
        time.sleep(0.3)
    srv.shutdown()
    check("body" in got, "上传回调 POST 到达")
    if "body" in got:
        payload = json.loads(got["body"])
        expect = "sha256=" + hmac.new(b"cbsecret", got["body"], hashlib.sha256).hexdigest()
        check(payload.get("event") == "object_created" and payload.get("key") == "cb.txt",
              "回调载荷正确（event/key）")
        check(got.get("sig") == expect, "回调 HMAC 签名正确")

    # ---------- 10. 计费记账 ----------
    cur.execute("SELECT * FROM s3_usage")
    rows = cur.fetchall()
    check(len(rows) > 0, "s3_usage 记账可查（%d 行）" % len(rows))
    row = None
    for r in rows:
        if r[1] == "s3demo":
            row = r
    check(row is not None and row[2] > 0 and row[3] > 0 and row[6] > 0 and row[7] > 0,
          "s3demo 请求数 / 字节数记账（get=%s put=%s in=%s out=%s）" %
          (row[2] if row else "-", row[3] if row else "-", row[6] if row else "-", row[7] if row else "-"))

    cur.execute("SHOW S3 USAGE FOR 'glacier'")
    check(len(cur.fetchall()) > 0, "SHOW S3 USAGE FOR 用户")

    cur.execute("EXPORT S3 USAGE TO 'usage_test.csv'")
    row = cur.fetchone()
    check(str(row[0]).endswith("usage_test.csv") and int(row[1]) > 0,
          "EXPORT S3 USAGE 落盘 %s（%s 字节）" % (row[0], row[1]))

    # HTTP 导出接口（计费系统拉取）
    st, hd, data = s3_request("GET", "/_glacier/s3/usage", query="format=json", ak=ak, sk=sk)
    check(st == 200 and b"s3demo" in data, "HTTP 导出接口 GET /_glacier/s3/usage")

    # ---------- 11. 生命周期 ----------
    cur.execute("SET S3 LIFECYCLE s3demo PREFIX 'tmp/' DAYS 7")
    cur.execute("SHOW S3 LIFECYCLE")
    rows = cur.fetchall()
    check(any(r[0] == "s3demo" and r[2] == 7 for r in rows), "生命周期规则可查")
    cur.execute("CLEAR S3 LIFECYCLE s3demo")

    # ---------- 12. 密钥生命周期 ----------
    cur.execute("DISABLE S3 KEY '%s'" % ak)
    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk=sk)
    check(st == 403 and b"InvalidAccessKeyId" in data, "DISABLE 后密钥拒绝")
    cur.execute("ENABLE S3 KEY '%s'" % ak)
    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk=sk)
    check(st == 200, "ENABLE 后恢复")

    cur.execute("RENEW S3 KEY '%s'" % ak)
    row = cur.fetchone()
    sk2 = row[1]
    check(len(sk2) == 48 and sk2 != sk, "RENEW 轮换 SK（明文仅一次可见）")
    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk=sk)
    check(st == 403, "旧 SK 失效")
    st, hd, data = s3_request("GET", "/s3demo/hello.txt", ak=ak, sk=sk2)
    check(st == 200, "新 SK 生效")

    cur.execute("DROP S3 KEY '%s'" % ak)
    cur.execute("SHOW S3 KEYS")
    check(all(k[0] != ak for k in cur.fetchall()), "DROP S3 KEY 吊销密钥")

    # ---------- 收尾 ----------
    cur.execute("DROP BUCKET s3demo FORCE")
    cur.execute("DROP BUCKET s3ver FORCE")
    cur.execute("SHOW BUCKETS")
    check(all(r[0] not in ("s3demo", "s3ver") for r in cur.fetchall()), "DROP BUCKET FORCE 清理")

    print("S3: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
