#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.0.10 黑盒测试：
1) 查询 QoS：SET QOS 热改 / SHOW QOS / qos_status 表 / wait 排队生效 / 只读拒绝
2) 周期调度：EVERY WEEK [ON 周X] [AT] / EVERY n WEEKS / PAUSE·RESUME / query_schedule / query_jobs 新列
3) 用户权限系统：CREATE USER / GRANT / REVOKE / SHOW GRANTS / 类型切换 / DROP USER / 系统表
4) 防爆破：失败计数锁定账号（3119/1045）/ SHOW LOCKED / SHOW LOGIN ATTEMPTS / UNLOCK
5) JSON 复查查询：->> / -> 运算符、JSON_LENGTH/CONTAINS/KEYS/SEARCH/MERGE_PATCH、表达式索引
6) Geo：ST_* 函数族（WKT/GeoJSON）、POINT DDL、距离/包含/相交
"""
import json
import time

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


def connect(user=USER, password=PASSWORD, db=DATABASE):
    return pymysql.connect(host=HOST, port=PORT, user=user, password=password,
                           database=db, charset="utf8mb4", autocommit=True,
                           read_timeout=120, write_timeout=120)


def expect_err(cur, sql, codes, msg):
    if not isinstance(codes, (list, tuple)):
        codes = [codes]
    try:
        cur.execute(sql)
        check(False, msg + "（未报错，实际成功）")
    except pymysql.MySQLError as e:
        check(e.args[0] in codes, msg + "（code=%s 期望 %s）" % (e.args[0], codes))


def main() -> int:
    conn_ = connect()
    cur = conn_.cursor()

    # ---------- 1. QoS ----------
    cur.execute("USE GlacierTDB")
    cur.execute("SET QOS DB backup QPS = 2")
    cur.execute("SHOW QOS")
    rows = cur.fetchall()
    scopes = dict((r[0], r) for r in rows)
    check("GLOBAL" in scopes, "SHOW QOS 含 GLOBAL 行")
    check("backup" in scopes and scopes["backup"][1] == 2, "单库 QoS qps=2 热改生效")
    cur.execute("SELECT * FROM qos_status")
    rows = cur.fetchall()
    check(any(r[0] == "backup" for r in rows), "qos_status 表可查（%d 行）" % len(rows))

    cur.execute("USE " + DATABASE)
    t0 = time.time()
    for _ in range(5):
        cur.execute("SELECT 1")
        cur.fetchone()
    elapsed = time.time() - t0
    check(elapsed > 0.9, "QPS=2 限速排队生效（5 连发耗时 %.2fs）" % elapsed)
    cur.execute("USE GlacierTDB")
    cur.execute("SET QOS DB backup CLEAR")
    cur.execute("SHOW QOS")
    scopes = dict((r[0], r) for r in cur.fetchall())
    check("backup" not in scopes, "SET QOS DB backup CLEAR 清除覆盖")
    cur.execute("USE " + DATABASE)
    t0 = time.time()
    for _ in range(5):
        cur.execute("SELECT 1")
        cur.fetchone()
    check(time.time() - t0 < 0.9, "清除后无限速（5 连发 %.2fs）" % (time.time() - t0))

    # ---------- 2. 周期调度 ----------
    cur.execute("USE " + DATABASE)
    cur.execute("SUBMIT QUERY 'SELECT 1 AS wk' EVERY WEEK ON 周五 AT '02:00'")
    jid_wk = cur.fetchone()[0]
    cur.execute("SHOW QUERY SCHEDULE")
    rows = cur.fetchall()
    hit = [r for r in rows if r[1] == jid_wk]
    check(len(hit) == 1, "SHOW QUERY SCHEDULE 含每周任务")
    if hit:
        r = hit[0]
        check(r[3] == "EVERY WEEK ON 周五 AT 02:00", "schedule 表达正确（%s）" % r[3])
        check(r[6] != "" and r[6] > "2026-01-01", "next_run 为未来时刻（%s）" % r[6])
        check(r[9] == "NO", "paused 初始 NO")
    cur.execute("SUBMIT QUERY 'SELECT 2 AS wk2' EVERY 2 WEEKS ON 周一 AT '03:00'")
    jid_w2 = cur.fetchone()[0]
    cur.execute("SELECT schedule FROM query_schedule WHERE job_id = '%s'" % jid_w2)
    r = cur.fetchone()
    check(r and r[0] == "EVERY 2 WEEKS ON 周一 AT 03:00", "每 N 周表达正确（%s）" % (r[0] if r else None))

    cur.execute("PAUSE QUERY '%s'" % jid_wk)
    cur.execute("SELECT paused FROM query_schedule WHERE job_id = '%s'" % jid_wk)
    check(cur.fetchone()[0] == "YES", "PAUSE 后 paused=YES")
    cur.execute("RESUME QUERY '%s'" % jid_wk)
    cur.execute("SELECT paused FROM query_schedule WHERE job_id = '%s'" % jid_wk)
    check(cur.fetchone()[0] == "NO", "RESUME 后 paused=NO")
    cur.execute("SELECT qos_wait_ms, schedule FROM query_jobs WHERE job_id = '%s'" % jid_wk)
    r = cur.fetchone()
    check(r is not None and r[1] == "EVERY WEEK ON 周五 AT 02:00", "query_jobs 新列 qos_wait_ms/schedule")
    cur.execute("CANCEL QUERY '%s'" % jid_wk)
    cur.execute("CANCEL QUERY '%s'" % jid_w2)

    # ---------- 3. 用户权限系统 ----------
    cur.execute("USE GlacierTDB")
    for leftover in ("u110", "u_brute"):  # 幂等：清理上次中断残留
        try:
            cur.execute("DROP USER '%s'" % leftover)
        except pymysql.MySQLError:
            pass
    cur.execute("CREATE USER 'u110'@'%' IDENTIFIED BY 'pw110'")
    cur.execute("GRANT SELECT ON backup.* TO 'u110'")
    u = connect(user="u110", password="pw110")
    uc = u.cursor()
    uc.execute("SELECT COUNT(*) FROM t_esc_109" if False else "SELECT 1")
    check(uc.fetchone()[0] == 1, "新用户登录 + SELECT 授权生效")
    expect_err(uc, "CREATE TABLE t_u110 (id INT)", 1142, "无 CREATE 权限被拒 1142")
    expect_err(uc, "INSERT INTO t_u110 VALUES (1)", 1142, "无 INSERT 权限被拒 1142")
    cur.execute("USE " + DATABASE)
    cur.execute("CREATE TABLE IF NOT EXISTS t_u110 (id INTEGER PRIMARY KEY, v TEXT)")
    cur.execute("DELETE FROM t_u110")
    cur.execute("USE GlacierTDB")
    cur.execute("GRANT INSERT ON backup.t_u110 TO 'u110'")
    uc.execute("INSERT INTO t_u110 VALUES (1, 'a')")
    check(True, "表级 INSERT 授权生效")
    expect_err(uc, "DELETE FROM t_u110 WHERE id = 1", 1142, "无 DELETE 权限被拒")
    cur.execute("SHOW GRANTS FOR 'u110'")
    g = "\n".join(r[0] for r in cur.fetchall())
    check("SELECT" in g and "t_u110" in g, "SHOW GRANTS 输出真实授权（%s）" % g.replace("\n", " | "))
    cur.execute("REVOKE INSERT ON backup.t_u110 FROM 'u110'")
    expect_err(uc, "INSERT INTO t_u110 VALUES (2, 'b')", 1142, "REVOKE 后插入被拒")
    cur.execute("SELECT * FROM users WHERE username = 'u110'")
    r = cur.fetchone()
    check(r and r[1] == "user", "users 表 account_type=user（%r）" % (r[1] if r else None))
    cur.execute("ALTER USER 'u110' READONLY")
    expect_err(uc, "CREATE TABLE t_x110 (id INT)", 1290, "READONLY 类型写拒绝 1290")
    uc.execute("SELECT 1")
    check(True, "READONLY 类型仍可查询")
    cur.execute("DROP USER 'u110'")
    try:
        connect(user="u110", password="pw110")
        check(False, "DROP USER 后不应能登录")
    except pymysql.MySQLError as e:
        check(e.args[0] in (1045, 1141, 1449), "DROP USER 后登录被拒（code=%s）" % e.args[0])
    cur.execute("SELECT COUNT(*) FROM login_attempts")
    check(cur.fetchone()[0] > 0, "login_attempts 审计有记录")

    # ---------- 4. 防爆破（账号锁；用专用账号避免误伤） ----------
    try:
        cur.execute("DROP USER 'u_brute'")
    except pymysql.MySQLError:
        pass
    cur.execute("CREATE USER 'u_brute'@'%' IDENTIFIED BY 'correct-pw'")
    locked = False
    for i in range(6):
        try:
            connect(user="u_brute", password="wrong-pw-%d" % i)
        except pymysql.MySQLError:
            pass
    try:
        connect(user="u_brute", password="correct-pw")
        check(False, "锁定后正确密码不应登录成功")
    except pymysql.MySQLError as e:
        locked = e.args[0] in (3119, 1045)
        check(locked, "失败 6 次后账号锁定（code=%s）" % e.args[0])
    cur.execute("SHOW LOCKED")
    lk = cur.fetchall()
    check(any("u_brute" in r[0] for r in lk), "SHOW LOCKED 显示账号锁（%s）" % [r[0] for r in lk])
    cur.execute("SELECT * FROM login_attempts")
    fails = [r for r in cur.fetchall() if r[4] == "u_brute" and r[5] == "NO"]
    check(len(fails) >= 6, "登录失败审计记录齐全（%d 条）" % len(fails))
    cur.execute("UNLOCK ACCOUNT u_brute")
    cur.execute("SHOW LOCKED")
    lk = cur.fetchall()
    check(not any("u_brute" in r[0] for r in lk), "UNLOCK ACCOUNT 解除锁定")
    if locked:
        u2 = connect(user="u_brute", password="correct-pw")
        u2.close()
        check(True, "解锁后正确密码恢复登录")
    cur.execute("DROP USER 'u_brute'")

    # ---------- 5. JSON 复杂查询 ----------
    cur.execute("USE " + DATABASE)
    cur.execute("SELECT JSON_EXTRACT('{\"a\":{\"b\":1}}', '$.a.b')")
    check(cur.fetchone()[0] == 1, "JSON_EXTRACT（SQLite JSON1 原生）")
    cur.execute("SELECT '{\"a\":\"x\"}' ->> '$.a'")
    check(cur.fetchone()[0] == "x", "->> 运算符取文本（%r）" % cur.fetchone() if False else "->> 运算符取文本")
    cur.execute("SELECT '{\"a\":\"x\"}' -> '$.a'")
    v = cur.fetchone()[0]
    check(v == '"x"', "-> 运算符返回 JSON 形态（%r）" % v)
    cur.execute("SELECT JSON_LENGTH('{\"a\":1,\"b\":2}'), JSON_LENGTH('[1,2,3]'), JSON_LENGTH('{\"a\":1}', '$.a')")
    a, b, c = cur.fetchone()
    check(int(a) == 2 and int(b) == 3, "JSON_LENGTH 对象/数组（%s,%s）" % (a, b))
    cur.execute("SELECT JSON_CONTAINS('{\"a\":{\"b\":1}}', '{\"b\":1}', '$.a'), JSON_CONTAINS('[1,2,3]', '2')")
    a, b = cur.fetchone()
    check(int(a) == 1 and int(b) == 1, "JSON_CONTAINS 子集包含")
    cur.execute("SELECT JSON_KEYS('{\"a\":1,\"b\":2}')")
    k = cur.fetchone()[0]
    check(json.loads(k) == ["a", "b"], "JSON_KEYS（%s）" % k)
    cur.execute("SELECT JSON_SEARCH('[\"hello\",\"world\"]', 'one', 'wor%')")
    p = cur.fetchone()[0]
    check(p is not None and "1" in str(p), "JSON_SEARCH 通配路径（%s）" % p)
    cur.execute("SELECT JSON_MERGE_PATCH('{\"a\":1,\"b\":2}', '{\"b\":null,\"c\":3}')")
    m = json.loads(cur.fetchone()[0])
    check(m == {"a": 1, "c": 3}, "JSON_MERGE_PATCH（null 删键）（%s）" % m)
    cur.execute("DROP TABLE IF EXISTS t_json_110")
    cur.execute("CREATE TABLE t_json_110 (id INTEGER PRIMARY KEY, cfg JSON)")
    for i, cfg in enumerate(['{"deploy_mode": "internal"}', '{"deploy_mode": "edge"}']):
        cur.execute("INSERT INTO t_json_110 VALUES (%s, %s)", (i, cfg))
    cur.execute("SELECT cfg ->> '$.deploy_mode' FROM t_json_110 ORDER BY id")
    vals = [r[0] for r in cur.fetchall()]
    check(vals == ["internal", "edge"], "表内 JSON 列 ->> 查询（%s）" % vals)
    cur.execute("SELECT COUNT(*) FROM t_json_110 WHERE cfg ->> '$.deploy_mode' = 'edge'")
    check(int(cur.fetchone()[0]) == 1, "WHERE 中 ->> 命中")
    cur.execute("CREATE INDEX idx_json_110 ON t_json_110(json_extract(cfg, '$.deploy_mode'))")
    cur.execute("EXPLAIN QUERY PLAN SELECT * FROM t_json_110 WHERE json_extract(cfg, '$.deploy_mode') = 'edge'")
    plan = " ".join(r[3] if len(r) > 3 else "" for r in cur.fetchall())
    check("idx_json_110" in plan, "json_extract 表达式索引命中（%s）" % plan)

    # ---------- 6. Geo ----------
    cur.execute("SELECT ST_AsText(ST_GeomFromText('POINT(1 2)'))")
    check(cur.fetchone()[0] == "POINT(1 2)", "ST_GeomFromText/ST_AsText WKT 往返")
    cur.execute("SELECT ST_X('POINT(1 2)'), ST_Y('POINT(1 2)'), ST_Longitude('POINT(120 30)'), ST_Latitude('POINT(120 30)')")
    x, y, lo, la = cur.fetchone()
    check(float(x) == 1 and float(y) == 2 and float(lo) == 120 and float(la) == 30, "ST_X/Y/Longitude/Latitude")
    cur.execute("SELECT ST_AsText(ST_MakePoint(30, 120))")
    check(cur.fetchone()[0] == "POINT(30 120)", "ST_MakePoint")
    cur.execute("SELECT ST_Distance('POINT(0 0)', 'POINT(3 4)')")
    check(abs(float(cur.fetchone()[0]) - 5) < 1e-9, "ST_Distance 平面距离 5")
    cur.execute("SELECT ST_Distance_Sphere('POINT(0 0)', 'POINT(0 1)')")
    d = float(cur.fetchone()[0])
    check(111000 < d < 112000, "ST_Distance_Sphere 约 111km（%.0f）" % d)
    poly = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
    cur.execute("SELECT ST_Within('POINT(0.5 0.5)', '%s'), ST_Contains('%s', 'POINT(0.5 0.5)'), ST_Within('POINT(5 5)', '%s')" % (poly, poly, poly))
    w, c2, w2 = cur.fetchone()
    check(int(w) == 1 and int(c2) == 1 and int(w2) == 0, "ST_Within/ST_Contains 点在面")
    cur.execute("SELECT ST_Intersects('POINT(0.5 0.5)', '%s'), ST_Equals('POINT(1 1)', 'POINT(1 1)')" % poly)
    it, eq = cur.fetchone()
    check(int(it) == 1 and int(eq) == 1, "ST_Intersects/ST_Equals")
    cur.execute("SELECT ST_GeomFromGeoJSON(ST_AsGeoJSON('POINT(3 4)'))")
    check(cur.fetchone()[0] == "POINT(3 4)", "GeoJSON 往返")
    cur.execute("DROP TABLE IF EXISTS t_geo_110")
    cur.execute("CREATE TABLE t_geo_110 (id INTEGER PRIMARY KEY, loc POINT)")
    cur.execute("INSERT INTO t_geo_110 VALUES (1, ST_GeomFromText('POINT(120 30)'))")
    cur.execute("SELECT ST_AsText(loc) FROM t_geo_110 WHERE id = 1")
    check(cur.fetchone()[0] == "POINT(120 30)", "POINT 列存取")

    conn_.close()
    print()
    print("V110: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 0 if STATS["fail"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
