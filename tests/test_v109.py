#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.0.9 黑盒测试：
1) MySQL 转义序列反转义修复：pymysql 参数化写入含双引号/单引号/反斜杠/换行的文本
   不再把转义序列当数据落盘（BUG.txt：JSON 配置读回污染、WHERE 无法命中）
2) 只读账号 glacier_ro：同等登录与查询；写、事务、管理面变更命令一律 1290 拒绝；
   密码支持与管理员一样的热改（SET PASSWORD FOR / ALTER USER ... IDENTIFIED BY）
3) SHOW QUERY VARS：WHEN 监测变量当前值查询
4) DISK_MBPS 服务器级磁盘写速率指标（WHEN 门槛可用；未知指标保持 PENDING）
5) 异步查询路径同样做转义还原
"""
import json
import time

import pymysql

HOST, PORT, USER, PASSWORD, DATABASE = "proxy", 3309, "glacier", "iloveyou~", "backup"
RO_USER, RO_PASSWORD = "glacier_ro", "iloveyou~"
STATS = {"pass": 0, "fail": 0}

BS = chr(92)  # 反斜杠字符（避免源码内转义歧义，一律用拼接构造）


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


def expect_err(cur, sql, code, msg):
    try:
        cur.execute(sql)
        check(False, msg + "（未报错，实际成功）")
    except pymysql.MySQLError as e:
        check(e.args[0] == code, msg + "（code=%s 期望 %d）" % (e.args[0], code))


def wait_job(cur, jid, timeout=120):
    deadline = time.time() + timeout
    state = "UNKNOWN"
    while time.time() < deadline:
        cur.execute("SELECT state FROM query_jobs WHERE job_id = '%s'" % jid)
        row = cur.fetchone()
        if row and row[0] in ("DONE", "ERROR", "EXPIRED", "CANCELLED"):
            return row[0]
        state = row[0] if row else state
        time.sleep(1)
    return state


def main() -> int:
    conn_ = connect()
    cur = conn_.cursor()

    # ---------- 1. 转义修复 ----------
    cur.execute("DROP TABLE IF EXISTS t_esc_109")
    cur.execute("CREATE TABLE t_esc_109 (id INTEGER PRIMARY KEY, cfg TEXT)")
    cases = [
        '{"a": "b"}',
        "it's a " + chr(34) + "quote" + chr(34),
        "C:" + BS + "path" + BS + "to" + BS + "file",
        "line1\nline2\tend",
        "mix " + chr(39) + BS + '"' + " of all",
        '{"deploy_mode": "internal"}',
    ]
    for i, v in enumerate(cases):
        cur.execute("INSERT INTO t_esc_109 VALUES (%s, %s)", (i, v))
    for i, v in enumerate(cases):
        cur.execute("SELECT cfg FROM t_esc_109 WHERE id = %s", (i,))
        got = cur.fetchone()[0]
        check(got == v, "参数化往返一致 id=%d（want %r got %r）" % (i, v, got))

    # 文本协议手工转义形式（mysql CLI / pymysql 驱动层发出的 SQL 形态）：
    # JSON 中的双引号被转义为 反斜杠+双引号 后拼进字面量
    plain_json = '{"a": "b"}'
    esc_sql = "INSERT INTO t_esc_109 VALUES (90, '" + plain_json.replace(chr(34), BS + chr(34)) + "')"
    cur.execute(esc_sql)
    cur.execute("SELECT cfg FROM t_esc_109 WHERE id = 90")
    got = cur.fetchone()[0]
    check(got == plain_json, "显式转义文本协议写入被还原（got %r）" % got)
    check(json.loads(got) == {"a": "b"}, "标准 json 库直接解析成功")
    cur.execute("SELECT COUNT(*) FROM t_esc_109 WHERE cfg = %s", (plain_json,))
    check(int(cur.fetchone()[0]) == 2, "WHERE 等值比较命中（id=0 与 id=90）")
    cur.execute("UPDATE t_esc_109 SET cfg = %s WHERE id = 90", (plain_json,))
    cur.execute("SELECT cfg FROM t_esc_109 WHERE id = 90")
    check(cur.fetchone()[0] == plain_json, "读出→改→写回不叠加转义")

    # ---------- 2. 只读账号 ----------
    ro = connect(user=RO_USER, password=RO_PASSWORD)
    rocur = ro.cursor()
    rocur.execute("SELECT COUNT(*) FROM t_esc_109")
    check(int(rocur.fetchone()[0]) >= 6, "只读账号登录并查询成功")
    expect_err(rocur, "INSERT INTO t_esc_109 VALUES (999, 'x')", 1290,
               "只读账号 INSERT 被拒 1290")
    expect_err(rocur, "UPDATE t_esc_109 SET cfg = 'x' WHERE id = 0", 1290,
               "只读账号 UPDATE 被拒 1290")
    expect_err(rocur, "BEGIN", 1290, "只读账号 BEGIN 被拒 1290")
    expect_err(rocur, "SUBMIT QUERY 'SELECT 1'", 1290, "只读账号 SUBMIT 被拒 1290")
    rocur.execute("USE GlacierTDB")
    rocur.execute("SHOW SERVER CONFIG")
    cfg_rows = rocur.fetchall()
    check(any(r[0] == "auth.readonly_user" and r[1] == RO_USER for r in cfg_rows),
          "管理面可见 auth.readonly_user 配置")
    expect_err(rocur, "SET PASSWORD = 'hack'", 1290, "只读账号 SET PASSWORD 被拒 1290")
    expect_err(rocur, "SET GLOBAL_THREADS = 4", 1290, "只读账号 SET GLOBAL_THREADS 被拒 1290")
    expect_err(rocur, "KILL 1", 1290, "只读账号 KILL 被拒 1290")
    expect_err(rocur, "VACUUM DATABASE backup", 1290, "只读账号 VACUUM 被拒 1290")
    ro.close()

    # 管理员热改只读密码（SET PASSWORD FOR 形式）
    cur.execute("USE GlacierTDB")
    cur.execute("SET PASSWORD FOR %s = 'ro_109_new~'" % RO_USER)
    try:
        connect(user=RO_USER, password=RO_PASSWORD)
        check(False, "只读账号旧密码应失效")
    except pymysql.MySQLError as e:
        check(e.args[0] == 1045, "只读账号旧密码被拒 1045（code=%s）" % e.args[0])
    ro2 = connect(user=RO_USER, password="ro_109_new~")
    ro2cur = ro2.cursor()
    ro2cur.execute("SELECT 1")
    check(ro2cur.fetchone()[0] == 1, "只读账号新密码登录成功")
    ro2.close()
    # 还原（ALTER USER 形式同时被覆盖验证）
    cur.execute("ALTER USER %s IDENTIFIED BY '%s'" % (RO_USER, RO_PASSWORD))
    ro3 = connect(user=RO_USER, password=RO_PASSWORD)
    ro3.cursor().execute("SELECT 1")
    check(True, "ALTER USER 形式热改 + 还原后登录成功")
    ro3.close()

    # ---------- 3. SHOW QUERY VARS ----------
    cur.execute("USE " + DATABASE)
    cur.execute("SHOW QUERY VARS")
    rows = cur.fetchall()
    names = set(r[0] for r in rows)
    check({"CPU", "MEM", "DISK_MBPS", "APP_CPU", "APP_MEM", "IO_MBPS"} <= names,
          "监测变量齐全（%s）" % ",".join(sorted(names)))
    level = dict((r[0], r[2]) for r in rows)
    check(level.get("DISK_MBPS") == "服务器级" and level.get("CPU") == "服务器级",
          "DISK_MBPS / CPU 为服务器级")
    check(level.get("IO_MBPS") == "应用级" and level.get("APP_CPU") == "应用级",
          "IO_MBPS / APP_CPU 为应用级")
    val = dict((r[0], r[1]) for r in rows)
    check(val.get("CPU") is not None, "CPU 已有采样值（%r）" % val.get("CPU"))
    check(val.get("DISK_MBPS") is None or val.get("DISK_MBPS") >= 0,
          "DISK_MBPS 为空（首采基线）或非负（%r）" % val.get("DISK_MBPS"))

    # ---------- 4. DISK_MBPS 作为 WHEN 门槛 ----------
    cur.execute("SUBMIT QUERY 'SELECT 1 AS ok' WHEN DISK_MBPS >= 0")
    jid = cur.fetchone()[0]
    st = wait_job(cur, jid, 120)
    check(st == "DONE", "WHEN DISK_MBPS>=0 任务按时执行（state=%s）" % st)

    cur.execute("SUBMIT QUERY 'SELECT 1 AS ok' WHEN NOPE_METRIC >= 0")
    jid2 = cur.fetchone()[0]
    time.sleep(3)
    cur.execute("SELECT state FROM query_jobs WHERE job_id = '%s'" % jid2)
    row = cur.fetchone()
    check(row and row[0] == "PENDING", "未知指标任务保持 PENDING（保守不满足）")
    cur.execute("CANCEL QUERY '%s'" % jid2)

    # ---------- 5. 异步查询路径转义还原 ----------
    inner = "SELECT '" + plain_json.replace(chr(34), BS + chr(34)) + "' AS v"
    submit_sql = "SUBMIT QUERY '" + inner.replace("'", "''") + "'"
    cur.execute(submit_sql)
    jid3 = cur.fetchone()[0]
    st3 = wait_job(cur, jid3, 60)
    check(st3 == "DONE", "异步转义任务完成（state=%s）" % st3)
    cur.execute("SELECT * FROM query_result('%s')" % jid3)
    row = cur.fetchone()
    check(row and row[0] == plain_json,
          "异步查询结果转义还原（want %r got %r）" % (plain_json, row[0] if row else None))

    cur.execute("DROP TABLE IF EXISTS t_esc_109")
    conn_.close()

    print()
    print("V109: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 0 if STATS["fail"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
