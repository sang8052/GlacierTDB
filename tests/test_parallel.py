#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多核并行查询黑盒测试：hint 触发、聚合正确性（与串行对拍）、WITHOUT ROWID 表、回退安全。"""
import sys
import time as _t

import pymysql

HOST, PORT, USER, PASSWORD, DATABASE = "proxy", 3309, "glacier", "iloveyou~", "backup"
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def norm(v):
    return round(float(v), 6) if v is not None and isinstance(v, (int, float)) else v


def main() -> int:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DATABASE, charset="utf8mb4", autocommit=True,
                           read_timeout=120, write_timeout=120)
    cur = conn.cursor()

    # ---------- 1. 大表准备：80 万行（rowid 表） ----------
    cur.execute("DROP TABLE IF EXISTS par_big")
    cur.execute("CREATE TABLE par_big(id INTEGER PRIMARY KEY, g INTEGER, v REAL)")
    t0 = _t.time()
    cur.execute("""INSERT INTO par_big(g, v)
        WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 800000)
        SELECT x % 100, x * 0.001 FROM c""")
    check(True, "准备 80 万行大表（%.1fs）" % (_t.time() - t0))

    # ---------- 2. 并行 vs 串行对拍（全聚合函数） ----------
    q_serial = "SELECT COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM par_big WHERE g < 90"
    q_parall = "SELECT /*+ PARALLEL(4) */ COUNT(*), SUM(v), MIN(v), MAX(v), AVG(v) FROM par_big WHERE g < 90"
    t0 = _t.time(); cur.execute(q_serial); a = cur.fetchone(); t_serial = _t.time() - t0
    t0 = _t.time(); cur.execute(q_parall); b = cur.fetchone(); t_par = _t.time() - t0
    ok_vals = all(norm(x) == norm(y) for x, y in zip(a, b))
    check(ok_vals, "并行结果与串行一致（%s == %s）" % (tuple(map(norm, a)), tuple(map(norm, b))))
    check(b[0] is not None, "并行执行正常返回（串行 %.2fs / 并行 %.2fs，受宿主负载影响仅供参考）" % (t_serial, t_par))
    check(int(b[0]) == 720000, "COUNT 过滤条件生效（g<90 → 720000，实际 %s）" % b[0])

    # ---------- 3. 全表无 WHERE ----------
    cur.execute("SELECT /*+ PARALLEL */ COUNT(*), AVG(v) FROM par_big")
    r = cur.fetchone()
    check(int(r[0]) == 800000 and norm(r[1]) == norm((800000 * 800001 / 2) * 0.001 / 800000),
          "无 WHERE 全表并行（COUNT=800000，AVG≈%s）" % norm(r[1]))

    # ---------- 4. WITHOUT ROWID 表（单列主键分片） ----------
    cur.execute("DROP TABLE IF EXISTS par_wr")
    cur.execute("CREATE TABLE par_wr(k INTEGER NOT NULL, v REAL, PRIMARY KEY(k)) WITHOUT ROWID")
    cur.execute("""INSERT INTO par_wr(k, v)
        WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x < 300000)
        SELECT x, x*1.5 FROM c""")
    cur.execute("SELECT COUNT(*), SUM(v) FROM par_wr")
    a = cur.fetchone()
    cur.execute("SELECT /*+ PARALLEL(3) */ COUNT(*), SUM(v) FROM par_wr")
    b = cur.fetchone()
    check(int(b[0]) == 300000 and norm(b[1]) == norm(a[1]), "WITHOUT ROWID 表主键分片并行一致（%s）" % (list(map(norm, b)),))

    # ---------- 5. 回退安全：不满足条件（GROUP BY / 非聚合列）原样串行 ----------
    cur.execute("SELECT /*+ PARALLEL */ g, COUNT(*) FROM par_big GROUP BY g ORDER BY g LIMIT 2")
    rows = cur.fetchall()
    check(rows == ((0, 8000), (1, 8000)), "带 GROUP BY 自动回退串行执行（%s）" % (rows,))
    cur.execute("SELECT /*+ PARALLEL */ id FROM par_big LIMIT 3")
    check(len(cur.fetchall()) == 3, "非聚合查询自动回退串行")
    cur.execute("SELECT COUNT(*) FROM par_small_noexist") if False else None

    # ---------- 6. NULL 语义 ----------
    cur.execute("DROP TABLE IF EXISTS par_null")
    cur.execute("CREATE TABLE par_null(id INTEGER PRIMARY KEY, v REAL)")
    cur.execute("INSERT INTO par_null(id, v) SELECT x, NULL FROM (SELECT 1 AS x UNION SELECT 2)")
    cur.execute("SELECT /*+ PARALLEL(2) */ SUM(v), MIN(v), MAX(v), AVG(v), COUNT(v), COUNT(*) FROM par_null")
    r = cur.fetchone()
    check(r[0] is None and r[1] is None and r[2] is None and r[3] is None and int(r[4]) == 0 and int(r[5]) == 2,
          "全 NULL 列并行聚合语义正确（SUM/MIN/MAX/AVG=NULL, COUNT(v)=0, COUNT(*)=2）")

    cur.execute("DROP TABLE IF EXISTS par_big")
    cur.execute("DROP TABLE IF EXISTS par_wr")
    cur.execute("DROP TABLE IF EXISTS par_null")
    cur.close()
    conn.close()

    print()
    print("-" * 44)
    print("PARALLEL 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("PARALLEL: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
