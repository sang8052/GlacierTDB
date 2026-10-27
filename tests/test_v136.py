#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.6 黑盒测试：多租户强化 + 默认配置 + S3 响应头。

1) SHOW DATABASES / information_schema.SCHEMATA 按授权过滤（USER 账号仅见授权库）
2) CREATE USER ... DEFAULT_DB db / SET USER DEFAULT_DB：登录即落默认库；USE 未授权库 → 1044
3) SET USER QOS QPS/CONCURRENCY：QPS 节流生效、CLEAR 解除、SHOW USERS 可见配额
4) S3：Server 头带版本号、x-amz-request-id 为 36 位 UUID
"""
import re
import sys
import time
import urllib.request
import urllib.error

import pymysql

HOST, PORT = "proxy", 3309
ADMIN, ADMIN_PW = "glacier", "iloveyou~"
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def conn(user=None, pw=None, db=None):
    return pymysql.connect(host=HOST, port=PORT,
                           user=user or ADMIN, password=pw or ADMIN_PW,
                           database=db, charset="utf8mb4", autocommit=True)


def admin_sql(cur, sql):
    cur.execute("USE GlacierTDB")
    cur.execute(sql)
    try:
        return cur.fetchall()
    except Exception:
        return None


def part_tenant(cur):
    print("---- Part 1-3: 多租户强化 ----")
    # 准备库与租户
    cur.execute("CREATE DATABASE IF NOT EXISTS db_ta")
    cur.execute("CREATE DATABASE IF NOT EXISTS db_tb")
    cur.execute("USE db_ta")
    cur.execute("DROP TABLE IF EXISTS t1")
    cur.execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)")
    cur.execute("USE backup")
    admin_sql(cur, "DROP USER 'ta136'")  # 幂等清理（不存在时忽略报错）
    admin_sql(cur, "CREATE USER 'ta136' IDENTIFIED BY 'pwd136' USER DEFAULT_DB db_ta")
    admin_sql(cur, "GRANT SELECT,INSERT ON db_ta.* TO 'ta136'")
    check(True, "CREATE USER ... DEFAULT_DB db_ta 建号")

    # ② 登录即落默认库
    ca = conn("ta136", "pwd136")
    ca_cur = ca.cursor()
    ca_cur.execute("SELECT DATABASE()")
    check(ca_cur.fetchone()[0] == "db_ta", "登录即落 DEFAULT_DB=db_ta")

    # ③ USE 未授权库 → 1044；授权库 OK
    try:
        ca_cur.execute("USE db_tb")
        use_tb = "ok"
    except Exception as e:
        use_tb = e.args[0]
    check(use_tb == 1044, "USE 未授权库 → 1044（got %r）" % (use_tb,))
    ca_cur.execute("USE db_ta")
    ca_cur.execute("SELECT COUNT(*) FROM t1")
    check(True, "USE 授权库 + 查询正常")

    # ① SHOW DATABASES 过滤
    ca_cur.execute("SHOW DATABASES")
    dbs = [r[0] for r in ca_cur.fetchall()]
    check("db_ta" in dbs and "db_tb" not in dbs and "backup" not in dbs,
          "SHOW DATABASES 仅见授权库（%r）" % dbs)
    ca_cur.execute("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")
    schemas = [r[0] for r in ca_cur.fetchall()]
    check("db_ta" in schemas and "db_tb" not in schemas,
          "information_schema.SCHEMATA 同步过滤")

    # 语句层权限回归：REVOKE 后 SELECT 被拒，GRANT 恢复
    admin_sql(cur, "REVOKE SELECT ON db_ta.* FROM 'ta136'")
    try:
        ca_cur.execute("SELECT COUNT(*) FROM t1")
        p = "ok"
    except Exception as e:
        p = e.args[0]
    check(p == 1142, "REVOKE 后 SELECT 1142 拒绝（got %r）" % (p,))
    admin_sql(cur, "GRANT SELECT,INSERT ON db_ta.* TO 'ta136'")
    ca_cur.execute("SELECT COUNT(*) FROM t1")
    check(True, "重新 GRANT 后恢复")

    # SET USER DEFAULT_DB 热改
    admin_sql(cur, "SET USER 'ta136' DEFAULT_DB db_ta")
    check(True, "SET USER DEFAULT_DB 语法可用")

    # ④ 账号级配额
    admin_sql(cur, "SET USER 'ta136' QOS QPS=5 CONCURRENCY=2")
    cur.execute("USE GlacierTDB")
    cur.execute("SELECT * FROM users")
    desc = [d[0] for d in cur.description]
    iu, iq, ic = desc.index("username"), desc.index("qps"), desc.index("concurrency")
    row = [r for r in cur.fetchall() if r[iu] == "ta136"]
    check(row and int(row[0][iq]) == 5 and int(row[0][ic]) == 2,
          "SHOW USERS 可见配额（qps=%s conc=%s）" % (row[0][iq], row[0][ic]))
    t0 = time.time()
    for i in range(6):
        ca_cur.execute("SELECT 1")
    cost = time.time() - t0
    check(cost >= 0.8, "QPS=5 节流生效（6 条耗 %.2fs ≥ 0.8s）" % cost)
    admin_sql(cur, "SET USER 'ta136' QOS CLEAR")
    t0 = time.time()
    for i in range(6):
        ca_cur.execute("SELECT 1")
    check(time.time() - t0 < 0.5, "QOS CLEAR 解除限流")
    cur.execute("USE GlacierTDB")
    cur.execute("SELECT * FROM users")
    desc = [d[0] for d in cur.description]
    iu, iq = desc.index("username"), desc.index("qps")
    row = [r for r in cur.fetchall() if r[iu] == "ta136"]
    check(row and int(row[0][iq]) == 0, "CLEAR 后配额归零（qps=%s）" % row[0][iq])

    ca.close()
    admin_sql(cur, "DROP USER 'ta136'")
    cur.execute("DROP DATABASE IF EXISTS db_ta")
    cur.execute("DROP DATABASE IF EXISTS db_tb")


def part_s3_headers():
    print("---- Part 4: S3 响应头 ----")
    # 未签名请求即可观察头（403 响应同样带头）
    req = urllib.request.Request("http://%s:%d/" % (HOST, 9000), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            h = resp.headers
    except urllib.error.HTTPError as e:
        h = e.headers
    server = h.get("Server", "")
    rid = h.get("x-amz-request-id", "")
    check(re.match(r"^GlacierTDB/\d+\.\d+\.", server) is not None,
          "Server 头带版本号（%r）" % server)
    check(len(rid) == 36 and rid.count("-") == 4,
          "x-amz-request-id 为 36 位 UUID（%r）" % rid)


def main():
    c = conn()
    cur = c.cursor()
    try:
        part_tenant(cur)
        part_s3_headers()
    finally:
        cur.close()
        c.close()
    print("V136: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
