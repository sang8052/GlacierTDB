#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""异步查询黑盒测试：SUBMIT/状态/结果拉取/CANCEL/AT 指定时刻/EXPIRE 回调/签名回调。"""
import http.server
import json
import sys
import threading
import time as _t

import pymysql

HOST, PORT, USER, PASSWORD, DATABASE = "proxy", 3309, "glacier", "iloveyou~", "backup"
CBHOST = "pyclient"  # 回调地址须从服务端容器可达本测试容器
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


CALLBACKS = []  # (headers, body)


class Hook(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        CALLBACKS.append((dict(self.headers), json.loads(body)))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


def main() -> int:
    srv = http.server.HTTPServer(("0.0.0.0", 18999), Hook)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DATABASE, charset="utf8mb4", autocommit=True,
                           read_timeout=30, write_timeout=30)
    cur = conn.cursor()

    def wait_state(jid, want, timeout=20):
        for _ in range(timeout * 2):
            cur.execute("SELECT state FROM query_jobs WHERE job_id = '%s'" % jid)
            r = cur.fetchone()
            if r and r[0] == want:
                return True
            _t.sleep(0.5)
        return False

    # ---------- 1. 立即执行 + 拉取结果 ----------
    cur.execute("SELECT COUNT(*) FROM t1")
    n = cur.fetchone()[0]
    cur.execute("SUBMIT QUERY 'SELECT id, name, val FROM t1 ORDER BY id'")
    jid = cur.fetchone()[0]
    check(jid.startswith("j_"), "SUBMIT 秒回 job_id（%s）" % jid)
    check(wait_state(jid, "DONE"), "作业到达 DONE")
    cur.execute("SELECT state, rows FROM query_jobs WHERE job_id = '%s'" % jid)
    st, rows = cur.fetchone()
    check(int(rows) == int(n), "结果行数与同步查询一致（%s == %s）" % (rows, n))
    cur.execute("SELECT * FROM query_result('%s')" % jid)
    got = cur.fetchall()
    check(len(got) == int(n), "query_result 拉取全量行（%d）" % len(got))
    if n > 0:
        check(got[0][0] is not None, "结果含数据列（首行首列 %r）" % (got[0][0],))
    cur.execute("FETCH QUERY RESULT '%s' LIMIT 2 OFFSET 0" % jid)
    check(len(cur.fetchall()) == min(2, n), "FETCH LIMIT 2 分页")

    # ---------- 2. HTTP 回调（成功）+ 签名 ----------
    cur.execute("SUBMIT QUERY 'SELECT COUNT(*) FROM t2' CALLBACK 'http://%s:18999/hook1' SIGN 'sec123'" % CBHOST)
    jid2 = cur.fetchone()[0]
    check(wait_state(jid2, "DONE"), "回调作业 DONE")
    _t.sleep(2)
    cb = [c for c in CALLBACKS if c[1].get("job_id") == jid2]
    check(len(cb) == 1, "DONE 状态 HTTP POST 回调到达（1 次）")
    if cb:
        hdrs = cb[0][0]
        check(hdrs.get("X-Glacier-Signature", "").startswith("sha256="),
              "SIGN 开启时带 X-Glacier-Signature 头")
        check(cb[0][1].get("state") == "DONE" and cb[0][1].get("rows") == 1,
              "回调 payload 含 state=DONE 与 rows")

    # ---------- 3. 错误作业：ERROR 也回调 ----------
    CALLBACKS.clear()
    cur.execute("SUBMIT QUERY 'SELECT * FROM no_such_tbl_async' CALLBACK 'http://%s:18999/hook2'" % CBHOST)
    jid3 = cur.fetchone()[0]
    check(wait_state(jid3, "ERROR"), "错误作业到达 ERROR")
    _t.sleep(2)
    cb = [c for c in CALLBACKS if c[1].get("job_id") == jid3]
    check(len(cb) == 1 and cb[0][1].get("state") == "ERROR" and "error" in cb[0][1],
          "ERROR 状态也回调（含 error 字段）")

    # ---------- 4. CANCEL ----------
    cur.execute("SUBMIT QUERY 'SELECT COUNT(*) FROM t_big' AT '23:59' TIMEOUT 300")
    jid4 = cur.fetchone()[0]
    cur.execute("SELECT state FROM query_jobs WHERE job_id = '%s'" % jid4)
    check(cur.fetchone()[0] == "PENDING", "AT 未来时刻 → PENDING 等待")
    cur.execute("CANCEL QUERY '%s'" % jid4)
    cur.execute("SELECT state FROM query_jobs WHERE job_id = '%s'" % jid4)
    check(cur.fetchone()[0] == "CANCELLED", "CANCEL 后状态 CANCELLED")

    # ---------- 5. 超时 EXPIRE 也回调（AT 永不到达 + 短 TIMEOUT）----------
    CALLBACKS.clear()
    cur.execute("SUBMIT QUERY 'SELECT 1' AT '23:59' TIMEOUT 2 CALLBACK 'http://%s:18999/hook3'" % CBHOST)
    jid5 = cur.fetchone()[0]
    check(wait_state(jid5, "EXPIRED", timeout=10), "AT 未到 + 超时 → EXPIRED")
    _t.sleep(2)
    cb = [c for c in CALLBACKS if c[1].get("job_id") == jid5]
    check(len(cb) == 1 and cb[0][1].get("state") == "EXPIRED",
          "EXPIRED 状态也回调（用户要求：expire 也要回调）")

    # ---------- 6. AT 指定时刻：2 秒后执行 ----------
    at_t = _t.strftime("%H:%M:%S", _t.localtime(_t.time() + 2))
    cur.execute("SUBMIT QUERY 'SELECT 42 AS v' AT '%s'" % at_t)
    jid6 = cur.fetchone()[0]
    _t.sleep(1)
    cur.execute("SELECT state FROM query_jobs WHERE job_id = '%s'" % jid6)
    st1 = cur.fetchone()[0]
    check(wait_state(jid6, "DONE", timeout=10), "AT 到点后自动执行 → DONE")
    cur.execute("SELECT * FROM query_result('%s')" % jid6)
    r = cur.fetchone()
    check(r and int(r[0]) == 42, "AT 作业结果正确（42，执行前状态 %s）" % st1)

    # ---------- 7. query_jobs 过滤 ----------
    cur.execute("SELECT job_id FROM query_jobs WHERE state = 'DONE'")
    check(len(cur.fetchall()) >= 3, "query_jobs WHERE state='DONE' 过滤")
    cur.execute("SHOW QUERY JOBS")
    check(len(cur.fetchall()) >= 5, "SHOW QUERY JOBS 列表（≥5 个历史作业）")

    cur.close()
    conn.close()
    srv.shutdown()

    print()
    print("-" * 44)
    print("ASYNC 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("ASYNC: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
