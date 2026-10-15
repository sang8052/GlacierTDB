#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.0.7：EVERY 周期定时任务 + VACUUM（hot 模式，写锁警示字段）。"""
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


def main() -> int:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DATABASE, charset="utf8mb4", autocommit=True,
                           read_timeout=60, write_timeout=60)
    cur = conn.cursor()

    # ---------- 1. EVERY 周期任务（无 CALLBACK = 定时任务）----------
    cur.execute("DROP TABLE IF EXISTS tick")
    cur.execute("CREATE TABLE tick(id INTEGER PRIMARY KEY, at TEXT)")
    cur.execute("SUBMIT QUERY 'INSERT INTO tick(at) SELECT datetime(''now'')' EVERY 2 SECONDS RETAIN 5m TIMEOUT 600")
    jid = cur.fetchone()[0]
    check(jid.startswith("j_"), "EVERY 2 SECONDS 周期任务提交（%s）" % jid)
    _t.sleep(9)  # 等 ≥3 次触发
    cur.execute("SELECT COUNT(*) FROM tick")
    # COUNT 经由普通表——tick 是普通表 ✓
    n = int(cur.fetchone()[0])
    check(n >= 3, "9 秒内周期触发 ≥3 次（实际 %d 次）" % n)
    cur.execute("SELECT recur_group FROM query_jobs WHERE job_id = '%s'" % jid)
    grp = cur.fetchone()[0]
    check(grp and grp.startswith("g_"), "周期链 recur_group 标记（%s）" % grp)
    cur.execute("SELECT COUNT(*) FROM (SELECT 1 FROM query_jobs WHERE recur_group = '%s' AND state = 'DONE')" % grp) if False else None
    cur.execute("SELECT job_id FROM query_jobs WHERE recur_group = '%s' AND state = 'DONE'" % grp)
    done_cnt = len(cur.fetchall())
    check(done_cnt >= 3, "同链 DONE 作业 ≥3 个（实际 %d）" % done_cnt)
    # 取消整链
    cur.execute("CANCEL QUERY '%s'" % jid)
    _t.sleep(3)
    cur.execute("SELECT COUNT(*) FROM tick")
    n2 = int(cur.fetchone()[0])
    _t.sleep(3)
    cur.execute("SELECT COUNT(*) FROM tick")
    n3 = int(cur.fetchone()[0])
    check(n3 - n2 <= 1, "CANCEL 链头后周期停止（%d → %d）" % (n2, n3))
    cur.execute("DROP TABLE IF EXISTS tick")

    # ---------- 2. EVERY DAY AT 'HH:MM'（不实际等待，验证排程字段）----------
    cur.execute("SUBMIT QUERY 'SELECT 1' EVERY DAY AT '23:58' TIMEOUT 3600")
    jid2 = cur.fetchone()[0]
    cur.execute("SELECT state, at FROM query_jobs WHERE job_id = '%s'" % jid2)
    st, at = cur.fetchone()
    check(st == "PENDING" and "23:58" in str(at), "EVERY DAY AT 排程（%s）" % at)
    cur.execute("CANCEL QUERY '%s'" % jid2)

    # ---------- 3. VACUUM（hot 模式 + 写锁警示）----------
    # 场景：建大表 → VACUUM 得基线 → 删大表 → 再 VACUUM 释放
    cur.execute("DROP TABLE IF EXISTS vac_big")
    cur.execute("CREATE TABLE vac_big(id INTEGER PRIMARY KEY, pad TEXT)")
    for _i in range(4):
        cur.execute("INSERT INTO vac_big(pad) SELECT substr(quote(zeroblob(3000)),1,3000) FROM (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) x, (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8) y")
    cur.execute("USE GlacierTDB")
    cur.execute("VACUUM DATABASE backup")
    base = cur.fetchone()
    check(base is not None and len(base) == 5, "VACUUM DATABASE backup 基线返回（%s）" % (base,))
    base_size = int(base[2]) if base else 0
    check(base_size > 1024 * 1024, "基线含大表（size=%s > 1MB）" % base_size)
    check("阻塞" in str(base[4]), "结果含写锁警示说明")
    # 独立新连接执行 DROP 与第二次 VACUUM（排除同连接事务状态干扰）
    c2 = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                         database=DATABASE, charset="utf8mb4", autocommit=True)
    x2 = c2.cursor()
    x2.execute("DROP TABLE vac_big")
    try:
        x2.execute("SELECT COUNT(*) FROM vac_big")
        gone = False
    except pymysql.err.Error:
        gone = True
    check(gone, "大表已确认删除（独立连接）")
    _t.sleep(1)
    x2.execute("USE GlacierTDB")
    x2.execute("VACUUM DATABASE backup")
    r = x2.fetchone()
    x2.close(); c2.close()
    check(r is not None and len(r) == 5, "删表后 VACUUM 执行成功返回（%s）" % (r,))
    cur.execute("USE backup")
    cur.execute("SELECT 1")
    check(cur.fetchone()[0] == 1, "VACUUM 后库可正常读写")

    cur.close()
    conn.close()
    print()
    print("-" * 44)
    print("V107 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("V107: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
