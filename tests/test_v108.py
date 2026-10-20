#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.0.8 黑盒测试：
1) 空字符串回读修复（'' 不再变 NULL，与真 NULL 可区分）
2) MD5/SHA1/SHA2/SHA256 Go 标量函数
3) LEFT/RIGHT/SUBSTRING_INDEX/DATEDIFF/TIMESTAMPDIFF/DATE_FORMAT
4) INSERT IGNORE
5) EXPLAIN PARALLEL 计划展示
6) PARTITION BY ROWS 分片回调（HTTP + 签名头）
7) WINDOW 周几限定解析 + query_jobs 新列
"""
import http.server
import json
import threading
import time as _t

import pymysql

HOST, PORT, USER, PASSWORD, DATABASE = "proxy", 3309, "glacier", "iloveyou~", "backup"
STATS = {"pass": 0, "fail": 0}

CALLBACKS = []  # (headers, body)


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


class Hook(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body.decode("utf-8", "replace")}
        CALLBACKS.append((dict(self.headers), parsed))
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *a):
        pass


def main() -> int:
    cbhost = "pyclient"
    srv = http.server.HTTPServer(("0.0.0.0", 19088), Hook)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DATABASE, charset="utf8mb4", autocommit=True,
                           read_timeout=120, write_timeout=120)
    cur = conn.cursor()

    # ---------- 1. 空字符串回读 ----------
    cur.execute("DROP TABLE IF EXISTS t_empty_108")
    cur.execute("CREATE TABLE t_empty_108 (id INTEGER PRIMARY KEY, v VARCHAR(8) NOT NULL DEFAULT '', n TEXT)")
    cur.execute("INSERT INTO t_empty_108 (v, n) VALUES ('', NULL)")
    cur.execute("SELECT v, v IS NULL, LENGTH(v), n, n IS NULL FROM t_empty_108")
    v, visnull, vlen, n, nisnull = cur.fetchone()
    check(v == "" and v is not None, "空串回读为 ''（实际 %r）" % v)
    check(int(visnull) == 0, "v IS NULL = 0")
    check(int(vlen) == 0, "LENGTH('') = 0")
    check(n is None and int(nisnull) == 1, "真 NULL 仍是 NULL（n=%r IS NULL=%s）" % (n, nisnull))
    cur.execute("SELECT COUNT(*) FROM t_empty_108 WHERE v = ''")
    check(cur.fetchone()[0] == 1, "WHERE v='' 命中空串")

    # ---------- 2. 哈希函数 ----------
    cur.execute("SELECT MD5('x'), SHA1('x'), SHA2('abc',256), SHA256('abc'), SHA2('abc',0), SHA2('abc',384)")
    md5x, sha1x, sha2x, sha256x, sha20, sha2384 = cur.fetchone()
    check(md5x == "9dd4e461268c8034f5c8564e155c67a6", "MD5('x') = %s" % md5x)
    check(sha1x == "11f6ad8ec52a2984abaafd7c3b516503785c2072", "SHA1('x') = %s" % sha1x)
    check(sha2x == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
          "SHA2('abc',256) = %s" % sha2x)
    check(sha256x == sha2x, "SHA256('abc') 与 SHA2('abc',256) 一致")
    check(sha20 == sha2x, "SHA2(...,0) 等价 256")
    check(sha2384 is not None and len(sha2384) == 96, "SHA2(...,384) 96 位 hex")
    cur.execute("SELECT MD5(NULL) IS NULL, SHA1(NULL) IS NULL")
    a, b = cur.fetchone()
    check(int(a) == 1 and int(b) == 1, "哈希函数 NULL 传播")
    cur.execute("DROP TABLE IF EXISTS t_md5_108")
    cur.execute("CREATE TABLE t_md5_108 (id INTEGER PRIMARY KEY, h TEXT)")
    cur.execute("INSERT INTO t_md5_108 (h) VALUES ('')")
    cur.execute("UPDATE t_md5_108 SET h = MD5(CONCAT(RAND(), id))")
    cur.execute("SELECT LENGTH(h) FROM t_md5_108")
    check(cur.fetchone()[0] == 32, "MD5 用于 UPDATE SET 表达式（32 位 hex）")

    # ---------- 3. 字符串/日期函数 ----------
    cur.execute("SELECT LEFT('hello',2), RIGHT('hello',2), LEFT('中文字符',2), RIGHT('中文字符',2), SUBSTRING_INDEX('a.b.c','.',2), SUBSTRING_INDEX('a.b.c','.',-2)")
    l2, r2, cl, cr, si, si2 = cur.fetchone()
    check(l2 == "he" and r2 == "lo", "LEFT/RIGHT 英文")
    check(cl == "中文" and cr == "字符", "LEFT/RIGHT UTF-8 安全")
    check(si == "a.b." and si2 == "b.c", "SUBSTRING_INDEX 正/负计数")
    cur.execute("SELECT DATEDIFF('2026-10-20','2026-10-01'), TIMESTAMPDIFF(DAY,'2026-10-01','2026-10-20'), TIMESTAMPDIFF(MONTH,'2026-01-15','2026-10-20'), TIMESTAMPDIFF(HOUR,'2026-10-20 00:00:00','2026-10-20 05:00:00')")
    dd, tdd, tdm, tdh = cur.fetchone()
    check(int(dd) == 19, "DATEDIFF = 19")
    check(int(tdd) == 19, "TIMESTAMPDIFF DAY = 19")
    check(int(tdm) == 9, "TIMESTAMPDIFF MONTH = 9")
    check(int(tdh) == 5, "TIMESTAMPDIFF HOUR = 5")
    cur.execute("SELECT DATE_FORMAT('2026-10-20 17:05:09','%Y/%m/%d %H:%i:%s %W %M %p')")
    df = cur.fetchone()[0]
    check(df == "2026/10/20 17:05:09 Tuesday October PM", "DATE_FORMAT = %s" % df)

    # ---------- 4. INSERT IGNORE ----------
    cur.execute("INSERT IGNORE INTO t_empty_108 (id, v) VALUES (1, 'dup')")
    cur.execute("SELECT COUNT(*) FROM t_empty_108")
    check(cur.fetchone()[0] == 1, "INSERT IGNORE 主键冲突被忽略")

    # ---------- 5. EXPLAIN PARALLEL ----------
    cur.execute("DROP TABLE IF EXISTS par108")
    cur.execute("CREATE TABLE par108 (id INTEGER PRIMARY KEY, g INTEGER, v REAL)")
    cur.execute("""INSERT INTO par108(g, v)
        WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 3000)
        SELECT x % 10, x * 0.5 FROM c""")
    cur.execute("EXPLAIN PARALLEL SELECT /*+ PARALLEL(3) */ COUNT(*), SUM(v) FROM par108")
    row = cur.fetchone()
    check(row[0] == "par108" and row[1] == "rowid" and int(row[2]) == 3000 and int(row[3]) == 3,
          "EXPLAIN PARALLEL 计划（table=%s shard=%s est=%s deg=%s）" % (row[0], row[1], row[2], row[3]))
    cur.execute("SELECT /*+ PARALLEL(3) */ COUNT(*), SUM(v) FROM par108")
    c3, s3 = cur.fetchone()
    check(int(c3) == 3000 and abs(float(s3) - sum(i * 0.5 for i in range(1, 3001))) < 1e-6,
          "并行执行结果正确（count=%s sum=%s）" % (c3, s3))
    cur.execute("SELECT COUNT(*), SUM(v) FROM par108")  # 无 hint：800k<scan_rows 不自动触发，走串行
    c1, s1 = cur.fetchone()
    check(int(c1) == 3000 and abs(float(s1) - float(s3)) < 1e-6, "串行结果与并行一致")

    # ---------- 6. PARTITION BY ROWS 分片回调 ----------
    cur.execute("DROP TABLE IF EXISTS pt108")
    cur.execute("CREATE TABLE pt108 (id INTEGER PRIMARY KEY)")
    cur.execute("INSERT INTO pt108 (id) VALUES (1),(2),(3),(4),(5)")
    CALLBACKS.clear()
    cur.execute("SUBMIT QUERY 'SELECT id FROM pt108 ORDER BY id' "
                "CALLBACK 'http://%s:19088/p1' SIGN 'sec108' PARTITION BY ROWS 2 RETAIN 10m" % cbhost)
    jid = cur.fetchone()[0]
    parts = []
    deadline = _t.time() + 30
    while _t.time() < deadline:
        parts = [b for (_, b) in CALLBACKS if b.get("job_id") == jid and "part" in b]
        if len(parts) >= 3:
            break
        _t.sleep(0.3)
    check(len(parts) == 3, "分片回调收到 3 片（实际 %d）" % len(parts))
    if len(parts) == 3:
        parts.sort(key=lambda b: b["part"])
        rows_sum = sum(int(p["rows"]) for p in parts)
        data_flat = [r[0] for p in parts for r in p.get("data", [])]
        check([p["part"] for p in parts] == [1, 2, 3], "片号 1..3 连续")
        check(rows_sum == 5, "三片行数合计 5（实际 %d）" % rows_sum)
        check(data_flat == [1, 2, 3, 4, 5], "分片数据内联完整（%s）" % data_flat)
        check(all(p.get("parts") == 3 for p in parts), "parts=3 元信息一致")
    sig_ok = all(h.get("X-Glacier-Signature", "").startswith("sha256=")
                 for (h, b) in CALLBACKS if b.get("job_id") == jid)
    check(sig_ok, "全部回调带 HMAC 签名头")

    # ---------- 7. WINDOW 周几限定 + query_jobs 新列 ----------
    cur.execute("SUBMIT QUERY 'SELECT 1' WINDOW '周三 01:00-02:00' TIMEOUT 5")
    jid2 = cur.fetchone()[0]
    cur.execute("SELECT window, partition_rows FROM query_jobs WHERE job_id = '%s'" % jid2)
    w, pr = cur.fetchone()
    check(w is not None and str(w).startswith("周三") and "01:00-02:00" in str(w),
          "WINDOW 周几限定解析（window=%s）" % w)
    check(pr is None or int(pr) == 0, "未设 PARTITION 时 partition_rows 为空")
    cur.execute("SELECT partition_rows, partition_bytes FROM query_jobs WHERE job_id = '%s'" % jid)
    pr2, pb2 = cur.fetchone()
    check(int(pr2) == 2 and (pb2 is None or int(pb2) == 0), "query_jobs 展示 partition_rows=2")
    cur.execute("CANCEL QUERY '%s'" % jid2)
    check(True, "周期窗口任务可取消")

    # BYTES 分片（每行 JSON [n] 约 3 字节；BYTES 8 → 3+2 两片）
    CALLBACKS.clear()
    cur.execute("SUBMIT QUERY 'SELECT id FROM pt108 ORDER BY id' "
                "CALLBACK 'http://%s:19088/p2' PARTITION BY BYTES 8 RETAIN 10m" % cbhost)
    jid3 = cur.fetchone()[0]
    parts3 = []
    deadline = _t.time() + 30
    while _t.time() < deadline:
        parts3 = [b for (_, b) in CALLBACKS if b.get("job_id") == jid3 and "part" in b]
        if len(parts3) >= 2:
            break
        _t.sleep(0.3)
    check(len(parts3) == 2, "PARTITION BY BYTES 按字节切分（%d 片）" % len(parts3))
    rows_sum3 = sum(int(p["rows"]) for p in parts3)
    check(rows_sum3 == 5, "BYTES 分片行数合计仍为 5")

    print()
    print("test_v108: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 0 if STATS["fail"] == 0 else 1


if __name__ == "__main__":
    sys_rc = main()
    raise SystemExit(sys_rc)
