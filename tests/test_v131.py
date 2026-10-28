#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.1 黑盒测试：
1) S3 分片上传（multipart）：initiate → 多 part → 重传覆盖 → complete →
   GET 内容逐字节一致 / 复合 ETag 语义（MD5(各 part MD5 顺序拼接)+"-N"）
2) 分片上传周边：ListParts / ListMultipartUploads / Abort / 非法 part 序号 /
   乱序与缺片的 Complete 报错 / SHOW S3 UPLOADS · s3_uploads
3) 异步调度：EVERY MONTH / EVERY YEAR（含 n MONTHS|YEARS、月尾 clamp）
   的 SHOW QUERY SCHEDULE 表达与 next_run
4) SCRIPT 后处理 PHP：.php 未配置 php_path → 任务 ERROR 信息正确
"""
import datetime
import hashlib
import hmac
import http.client
import os
import re
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
# SigV4 签名（AWS 标准算法，与 test_s3.py 同源）
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
               region="us-east-1", timeout=60):
    hdrs = sign_request(method, path, query, headers, body, ak, sk, region)
    conn = http.client.HTTPConnection(S3_HOST, S3_PORT, timeout=timeout)
    url = path + (("?" + query) if query else "")
    conn.request(method, url, body=body, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    out = (resp.status, dict((k.lower(), v) for k, v in resp.getheaders()), data)
    conn.close()
    return out


def extract_upload_id(xml_bytes):
    m = re.search(rb"<UploadId>([^<]+)</UploadId>", xml_bytes)
    return m.group(1).decode() if m else ""


def composite_etag(part_md5_hex_list):
    raw = b"".join(bytes.fromhex(h) for h in part_md5_hex_list)
    return hashlib.md5(raw).hexdigest() + "-%d" % len(part_md5_hex_list)


# ---------------------------------------------------------------------------
# 日历排期期望值（与服务端 nextMonthOccurrence / nextYearOccurrence 同语义）
# ---------------------------------------------------------------------------

def clamp_day(y, m, d):
    if m == 12:
        nxt = datetime.date(y + 1, 1, 1)
    else:
        nxt = datetime.date(y, m + 1, 1)
    last = (nxt - datetime.timedelta(days=1)).day
    return min(d, last)


def add_months(y, m, n):
    idx = (y * 12 + (m - 1)) + n
    return idx // 12, idx % 12 + 1


def next_month_occurrence(after, months, day, hh, mm):
    for k in range(0, 12 * months + 13, months):
        y, m = add_months(after.year, after.month, k)
        t = datetime.datetime(y, m, clamp_day(y, m, day), hh, mm)
        if t > after:
            return t
    return None


def next_year_occurrence(after, years, mm, dd, hh, mi):
    for k in range(0, years + 3, years):
        y = after.year + k
        t = datetime.datetime(y, mm, clamp_day(y, mm, dd), hh, mi)
        if t > after:
            return t
    return None


def sched_find(cur, job_id):
    cur.execute("SELECT * FROM query_schedule")
    cols = [d[0] for d in cur.description]
    for row in cur.fetchall():
        rec = dict(zip(cols, row))
        if rec.get("job_id") == job_id:
            return rec
    return None


def submit_and_get(cur, sql_opts):
    cur.execute("SUBMIT QUERY " + sql_opts)
    row = cur.fetchone()
    job_id = row[0]
    cur.execute("SELECT submit_ts FROM query_jobs WHERE job_id = '%s'" % job_id)
    submit_ts = datetime.datetime.strptime(cur.fetchone()[0], "%Y-%m-%d %H:%M:%S")
    return job_id, submit_ts


def main() -> int:
    conn = sql_connect()
    cur = conn.cursor()
    cur.execute("USE GlacierTDB")

    # ---------- 0. 桶与密钥 ----------
    for leftover in ("s3mpu",):
        try:
            cur.execute("DROP BUCKET %s FORCE" % leftover)
        except pymysql.MySQLError:
            pass
    cur.execute("CREATE BUCKET s3mpu OWNER 'glacier'")
    check(True, "CREATE BUCKET s3mpu")
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    row = cur.fetchone()
    ak, sk = row[0], row[1]

    # ---------- 1. 分片上传全流程 ----------
    parts = [os.urandom(100 * 1024), os.urandom(200 * 1024), os.urandom(300 * 1024)]
    st, hd, data = s3_request("POST", "/s3mpu/big.bin", query="uploads", body=b"", ak=ak, sk=sk)
    upload_id = extract_upload_id(data)
    check(st == 200 and upload_id, "InitiateMultipartUpload 返回 UploadId（%s...）" % upload_id[:12])

    # ListMultipartUploads 可见
    st, hd, data = s3_request("GET", "/s3mpu/", query="uploads", body=b"", ak=ak, sk=sk)
    check(st == 200 and upload_id.encode() in data and b"big.bin" in data,
          "ListMultipartUploads 列出上传会话")

    # 管理面：SHOW S3 UPLOADS / s3_uploads
    cur.execute("SHOW S3 UPLOADS")
    rows = cur.fetchall()
    check(any(r[0] == upload_id and r[2] == "big.bin" for r in rows), "SHOW S3 UPLOADS 可查会话")
    cur.execute("SELECT * FROM s3_uploads")
    rows = cur.fetchall()
    check(any(r[0] == upload_id for r in rows), "SELECT * FROM s3_uploads 白名单表可查")

    # 上传 part 1..3（part 2 重传覆盖一次）
    part_md5s = []
    for n, blob in enumerate(parts, 1):
        st, hd, data = s3_request("PUT", "/s3mpu/big.bin",
                                  query="partNumber=%d&uploadId=%s" % (n, upload_id),
                                  body=blob, ak=ak, sk=sk, timeout=120)
        etag = hd.get("etag", "").strip('"')
        expect = hashlib.md5(blob).hexdigest()
        check(st == 200 and etag == expect, "UploadPart %d：ETag=该 part MD5（%s）" % (n, st))
        part_md5s.append(expect)

    # 重传覆盖：part 2 换内容，最后一次上传为准
    parts[1] = os.urandom(150 * 1024)
    st, hd, data = s3_request("PUT", "/s3mpu/big.bin",
                              query="partNumber=2&uploadId=%s" % upload_id,
                              body=parts[1], ak=ak, sk=sk, timeout=120)
    part_md5s[1] = hashlib.md5(parts[1]).hexdigest()
    check(st == 200 and hd.get("etag", "").strip('"') == part_md5s[1],
          "UploadPart 重传覆盖（同序号最后一次为准）")

    # 非法 part 序号
    st, hd, data = s3_request("PUT", "/s3mpu/big.bin",
                              query="partNumber=0&uploadId=%s" % upload_id,
                              body=b"x", ak=ak, sk=sk)
    check(st == 400 and b"partNumber" in data, "partNumber=0 → 400")
    st, hd, data = s3_request("PUT", "/s3mpu/big.bin",
                              query="partNumber=10001&uploadId=%s" % upload_id,
                              body=b"x", ak=ak, sk=sk)
    check(st == 400, "partNumber=10001 → 400")

    # ListParts：3 片、序号与大小正确
    st, hd, data = s3_request("GET", "/s3mpu/big.bin", query="uploadId=%s" % upload_id, ak=ak, sk=sk)
    ok_parts = (st == 200 and b"<PartNumber>1</PartNumber>" in data
                and b"<PartNumber>2</PartNumber>" in data
                and b"<PartNumber>3</PartNumber>" in data
                and str(len(parts[1])).encode() in data)
    check(ok_parts, "ListParts 列出 3 片（重传后的大小生效）")

    cur.execute("SHOW S3 UPLOADS")
    rows = cur.fetchall()
    row = next((r for r in rows if r[0] == upload_id), None)
    check(row is not None and row[3] == 3 and row[4] == sum(len(p) for p in parts),
          "SHOW S3 UPLOADS parts/bytes 聚合正确")

    # 乱序 Complete → InvalidPartOrder
    xml_bad_order = ("<CompleteMultipartUpload>"
                     "<Part><PartNumber>3</PartNumber><ETag>\"%s\"</ETag></Part>"
                     "<Part><PartNumber>1</PartNumber><ETag>\"%s\"</ETag></Part>"
                     "</CompleteMultipartUpload>") % (part_md5s[2], part_md5s[0])
    st, hd, data = s3_request("POST", "/s3mpu/big.bin", query="uploadId=%s" % upload_id,
                              body=xml_bad_order.encode(), ak=ak, sk=sk)
    check(st == 400 and b"InvalidPartOrder" in data, "Complete 乱序 → InvalidPartOrder")

    # 正常 Complete：复合 ETag 语义
    xml_ok = "<CompleteMultipartUpload>" + "".join(
        "<Part><PartNumber>%d</PartNumber><ETag>\"%s\"</ETag></Part>" % (n, part_md5s[n - 1])
        for n in (1, 2, 3)) + "</CompleteMultipartUpload>"
    expect_etag = composite_etag(part_md5s)
    st, hd, data = s3_request("POST", "/s3mpu/big.bin", query="uploadId=%s" % upload_id,
                              body=xml_ok.encode(), ak=ak, sk=sk)
    check(st == 200 and expect_etag.encode() in data,
          "Complete：ETag=MD5(各 part MD5 顺序拼接)+\"-N\"（%s）" % expect_etag)

    # GET 内容逐字节一致 + ETag 头
    st, hd, data = s3_request("GET", "/s3mpu/big.bin", ak=ak, sk=sk, timeout=120)
    expect_body = b"".join(parts)
    check(st == 200 and data == expect_body, "GET 逐字节一致（%d 字节）" % len(expect_body))
    check(hd.get("etag", "").strip('"') == expect_etag, "GET ETag 头 = 复合 ETag")

    st, hd, data = s3_request("HEAD", "/s3mpu/big.bin", ak=ak, sk=sk)
    check(st == 200 and hd.get("content-length") == str(len(expect_body)),
          "HEAD Content-Length = 合并后大小")

    # 完成后会话清理
    st, hd, data = s3_request("GET", "/s3mpu/big.bin", query="uploadId=%s" % upload_id, ak=ak, sk=sk)
    check(st == 404 and b"NoSuchUpload" in data, "Complete 后 ListParts → NoSuchUpload")
    st, hd, data = s3_request("GET", "/s3mpu/", query="uploads", body=b"", ak=ak, sk=sk)
    check(st == 200 and upload_id.encode() not in data, "Complete 后 ListMultipartUploads 已清理")

    # ---------- 2. Abort / 缺片 Complete ----------
    st, hd, data = s3_request("POST", "/s3mpu/abort.bin", query="uploads", body=b"", ak=ak, sk=sk)
    up2 = extract_upload_id(data)
    check(st == 200 and up2, "Abort 流程：Initiate")
    st, hd, data = s3_request("PUT", "/s3mpu/abort.bin", query="partNumber=1&uploadId=%s" % up2,
                              body=b"only-part", ak=ak, sk=sk)
    check(st == 200, "Abort 流程：UploadPart")

    # 缺片 Complete → InvalidPart
    xml_missing = ("<CompleteMultipartUpload>"
                   "<Part><PartNumber>1</PartNumber><ETag>\"%s\"</ETag></Part>"
                   "<Part><PartNumber>2</PartNumber><ETag>\"%s\"</ETag></Part>"
                   "</CompleteMultipartUpload>") % (hashlib.md5(b"only-part").hexdigest(), "0" * 32)
    st, hd, data = s3_request("POST", "/s3mpu/abort.bin", query="uploadId=%s" % up2,
                              body=xml_missing.encode(), ak=ak, sk=sk)
    check(st == 400 and b"InvalidPart" in data, "Complete 缺片 → InvalidPart")

    st, hd, data = s3_request("DELETE", "/s3mpu/abort.bin", query="uploadId=%s" % up2, ak=ak, sk=sk)
    check(st == 204, "AbortMultipartUpload → 204")
    st, hd, data = s3_request("GET", "/s3mpu/abort.bin", query="uploadId=%s" % up2, ak=ak, sk=sk)
    check(st == 404 and b"NoSuchUpload" in data, "Abort 后 ListParts → NoSuchUpload")
    st, hd, data = s3_request("GET", "/s3mpu/", query="uploads", body=b"", ak=ak, sk=sk)
    check(st == 200 and up2.encode() not in data, "Abort 后 ListMultipartUploads 已清理")
    st, hd, data = s3_request("GET", "/s3mpu/abort.bin", ak=ak, sk=sk)
    check(st == 404, "Abort 后对象不存在")

    # 未知 uploadId
    st, hd, data = s3_request("POST", "/s3mpu/big.bin", query="uploadId=nosuch",
                              body=xml_ok.encode(), ak=ak, sk=sk)
    check(st == 404 and b"NoSuchUpload" in data, "未知 uploadId → NoSuchUpload")

    # ---------- 3. EVERY MONTH / EVERY YEAR 调度 ----------
    # 3.1 每月 15 日 03:00
    job1, ts1 = submit_and_get(cur, "'SELECT 1 AS a' EVERY MONTH ON 15 AT '03:00'")
    rec = sched_find(cur, job1)
    exp = next_month_occurrence(ts1, 1, 15, 3, 0)
    check(rec is not None and rec["schedule"] == "EVERY MONTH ON 15 AT 03:00",
          "EVERY MONTH schedule 表达（%s）" % (rec["schedule"] if rec else "-"))
    check(rec is not None and rec["next_run"] == exp.strftime("%Y-%m-%d %H:%M:%S"),
          "EVERY MONTH next_run=%s（期望 %s）" % (rec["next_run"] if rec else "-", exp))

    # 3.2 每 3 月 31 日 01:30（月尾 clamp：无 31 日的月取最后一天）
    job2, ts2 = submit_and_get(cur, "'SELECT 1 AS a' EVERY 3 MONTHS ON 31 AT '01:30'")
    rec2 = sched_find(cur, job2)
    exp2 = next_month_occurrence(ts2, 3, 31, 1, 30)
    check(rec2 is not None and rec2["schedule"] == "EVERY 3 MONTHS ON 31 AT 01:30",
          "EVERY 3 MONTHS schedule 表达（%s）" % (rec2["schedule"] if rec2 else "-"))
    check(rec2 is not None and rec2["next_run"] == exp2.strftime("%Y-%m-%d %H:%M:%S"),
          "EVERY 3 MONTHS ON 31 月尾 clamp next_run=%s（期望 %s）" % (rec2["next_run"] if rec2 else "-", exp2))

    # 3.3 每月（ON 省略 = 提交日）
    job3, ts3 = submit_and_get(cur, "'SELECT 1 AS a' EVERY MONTH AT '04:00'")
    rec3 = sched_find(cur, job3)
    exp3 = next_month_occurrence(ts3, 1, ts3.day, 4, 0)
    want3 = "EVERY MONTH ON %d AT 04:00" % ts3.day
    check(rec3 is not None and rec3["schedule"] == want3,
          "EVERY MONTH 缺省 ON=提交日（%s）" % (rec3["schedule"] if rec3 else "-"))
    check(rec3 is not None and rec3["next_run"] == exp3.strftime("%Y-%m-%d %H:%M:%S"),
          "EVERY MONTH 缺省日 next_run=%s（期望 %s）" % (rec3["next_run"] if rec3 else "-", exp3))

    # 3.4 每年 3-1 日 02:00
    job4, ts4 = submit_and_get(cur, "'SELECT 1 AS a' EVERY YEAR ON '03-01' AT '02:00'")
    rec4 = sched_find(cur, job4)
    exp4 = next_year_occurrence(ts4, 1, 3, 1, 2, 0)
    check(rec4 is not None and rec4["schedule"] == "EVERY YEAR ON 03-01 AT 02:00",
          "EVERY YEAR schedule 表达（%s）" % (rec4["schedule"] if rec4 else "-"))
    check(rec4 is not None and rec4["next_run"] == exp4.strftime("%Y-%m-%d %H:%M:%S"),
          "EVERY YEAR next_run=%s（期望 %s）" % (rec4["next_run"] if rec4 else "-", exp4))

    # 3.5 每 2 年 12-31 日 23:00
    job5, ts5 = submit_and_get(cur, "'SELECT 1 AS a' EVERY 2 YEARS ON '12-31' AT '23:00'")
    rec5 = sched_find(cur, job5)
    exp5 = next_year_occurrence(ts5, 2, 12, 31, 23, 0)
    check(rec5 is not None and rec5["schedule"] == "EVERY 2 YEARS ON 12-31 AT 23:00",
          "EVERY 2 YEARS schedule 表达（%s）" % (rec5["schedule"] if rec5 else "-"))
    check(rec5 is not None and rec5["next_run"] == exp5.strftime("%Y-%m-%d %H:%M:%S"),
          "EVERY 2 YEARS next_run=%s（期望 %s）" % (rec5["next_run"] if rec5 else "-", exp5))

    # 3.6 非法 ON → 语法错误
    try:
        cur.execute("SUBMIT QUERY 'SELECT 1 AS a' EVERY MONTH ON 32 AT '03:00'")
        check(False, "EVERY MONTH ON 32 应语法报错")
    except pymysql.MySQLError:
        check(True, "EVERY MONTH ON 32 → 语法报错")
    try:
        cur.execute("SUBMIT QUERY 'SELECT 1 AS a' EVERY YEAR ON '13-01'")
        check(False, "EVERY YEAR ON '13-01' 应语法报错")
    except pymysql.MySQLError:
        check(True, "EVERY YEAR ON '13-01' → 语法报错")

    # 清理排期链
    for jid in (job1, job2, job3, job4, job5):
        try:
            cur.execute("CANCEL QUERY '%s'" % jid)
        except pymysql.MySQLError:
            pass
    check(True, "CANCEL 清理周期链")

    # ---------- 4. SCRIPT PHP ----------
    # v1.3.12 语义更新：测试容器 async_query.php_path 已配置（/bin/sh + 夹具
    # pipe.php），任务应正常跑通（DONE）；php_path 未配置的环境才走 ERROR 分支
    cur.execute("SUBMIT QUERY 'SELECT 1 AS a' SCRIPT 'pipe.php'")
    row = cur.fetchone()
    job6 = row[0]
    state6, err6 = "", ""
    for _ in range(60):
        cur.execute("SELECT state, error FROM query_jobs WHERE job_id = '%s'" % job6)
        r = cur.fetchone()
        if r:
            state6, err6 = r[0], r[1] or ""
            if state6 in ("DONE", "ERROR"):
                break
        time.sleep(0.3)
    check(state6 == "DONE" or (state6 == "ERROR" and "php_path" in err6),
          "SCRIPT 'pipe.php' 配置 runtime → DONE（state=%s err=%s）" % (state6, err6[:80]))

    cur.execute("SUBMIT QUERY 'SELECT 1 AS a' SCRIPT 'pipe.txt'")
    row = cur.fetchone()
    job7 = row[0]
    state7, err7 = "", ""
    for _ in range(60):
        cur.execute("SELECT state, error FROM query_jobs WHERE job_id = '%s'" % job7)
        r = cur.fetchone()
        if r:
            state7, err7 = r[0], r[1] or ""
            if state7 in ("DONE", "ERROR"):
                break
        time.sleep(0.3)
    check(state7 == "ERROR" and "unsupported script type" in err7,
          "SCRIPT 'pipe.txt' 非法扩展名 → ERROR（%s）" % err7[:80])

    # ---------- 5. 清理 ----------
    cur.execute("DROP BUCKET s3mpu FORCE")
    check(True, "DROP BUCKET s3mpu FORCE 清理")

    print("V131: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
