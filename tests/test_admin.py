#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GlacierTDB 管理面黑盒测试：USE GlacierTDB、查配置、查客户端、查事务、KILL、改密码。"""
import sys
import time

import pymysql

HOST, PORT, USER, PASS, DB = "proxy", 3309, "glacier", "iloveyou~", "backup"

STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def connect(pwd=PASS, db=DB):
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=pwd,
                           database=db, charset="utf8mb4", autocommit=True,
                           read_timeout=20, write_timeout=20)


def main() -> int:
    conn = connect()
    cur = conn.cursor()

    # ---------- 1. USE GlacierTDB 进入管理面 ----------
    cur.execute("USE GlacierTDB")
    cur.execute("SELECT DATABASE()")
    check(cur.fetchone()[0] == "GlacierTDB", "USE GlacierTDB 后 SELECT DATABASE()=GlacierTDB")
    cur.execute("SHOW DATABASES")
    dbs = {r[0] for r in cur.fetchall()}
    check("GlacierTDB" in dbs and "backup" in dbs, "SHOW DATABASES 同时列出管理库与数据库")

    # ---------- 2. SHOW SERVER CONFIG / server_config 表 ----------
    cur.execute("SHOW SERVER CONFIG")
    cfg = {r[0]: r[1] for r in cur.fetchall()}
    for key, expect in [("auth.user", "glacier"), ("default_db", "backup")]:
        check(cfg.get(key) == expect, "SHOW SERVER CONFIG %s=%s（实际 %s）" % (key, expect, cfg.get(key)))
    for key in ("memory_limit_mb", "max_connections", "current_connections", "sqlite.max_result_rows"):
        check(key in cfg, "SHOW SERVER CONFIG 含 %s（值 %s）" % (key, cfg.get(key)))
    cur.execute("SELECT value FROM server_config WHERE name = 'listen'")
    check(len(cur.fetchall()) == 1, "SELECT ... FROM server_config WHERE name='listen' 过滤生效")
    cur.execute("SELECT name FROM server_config WHERE name LIKE 'sqlite.%'")
    names = {r[0] for r in cur.fetchall()}
    check({"sqlite.synchronous", "sqlite.max_result_rows"} <= names,
          "server_config LIKE 过滤返回 sqlite.* 配置（%d 项）" % len(names))

    # ---------- 3. SHOW CLIENTS 含自身 ----------
    # 注：管理面表查询忽略投影，固定返回全列
    # (Id,User,Host,DB,AdminMode,ConnectedSec,InTransaction,HoldingWriteLock,LastQuery)
    cur.execute("SELECT Id, DB, AdminMode FROM clients WHERE DB = 'GlacierTDB'")
    self_rows = cur.fetchall()
    check(len(self_rows) >= 1 and any(r[4] == "YES" for r in self_rows),
          "SHOW CLIENTS/clients 表可见管理面连接（%d 个，AdminMode=YES）" % len(self_rows))

    # ---------- 4. 事务观测：连接 B 开事务未提交 ----------
    conn_b = connect()
    cur_b = conn_b.cursor()
    cur_b.execute("BEGIN")
    cur_b.execute("INSERT INTO t1 (name, val) VALUES ('admin_tx_probe', 1.0)")
    cur.execute("SELECT Id, InTransaction, HoldingWriteLock FROM clients WHERE DB = 'backup'")
    tx_rows = [r for r in cur.fetchall()]  # 全列：InTransaction=索引6
    cur.execute("SELECT * FROM transactions")
    txs = cur.fetchall()
    check(any(r[6] == "YES" for r in tx_rows), "SHOW CLIENTS 可见 B 连接 InTransaction=YES")
    check(len(txs) >= 1 and all(r[-1] != "" for r in txs),
          "SHOW TRANSACTIONS 列出持事务连接（%d 个，含最后语句）" % len(txs))
    cur_b.execute("ROLLBACK")
    cur.execute("SELECT Id FROM transactions")
    check(len(cur.fetchall()) == 0, "B ROLLBACK 后 SHOW TRANSACTIONS 清空")

    # ---------- 5. KILL 踢掉连接 B ----------
    cur.execute("SELECT Id FROM clients WHERE DB = 'backup' LIMIT 1")
    bid = cur.fetchone()[0]
    cur.execute("KILL %d" % bid)
    killed = False
    try:
        cur_b.execute("SELECT 1")
    except pymysql.err.Error:
        killed = True
    check(killed, "KILL %d 后被踢连接再查询报连接错误" % bid)
    try:
        conn_b.close()
    except Exception:
        pass
    gone = False
    for _ in range(10):  # 轮询等待服务端异步注销（最多 5s）
        cur.execute("SELECT Id FROM clients WHERE Id = %d" % bid)
        if len(cur.fetchall()) == 0:
            gone = True
            break
        time.sleep(0.5)
    check(gone, "被踢连接从 clients 列表消失")

    # ---------- 6. 管理面拒绝普通 SQL / USE backup 退出 ----------
    try:
        cur.execute("SELECT * FROM t1")
        check(False, "管理面普通 SQL 应被拒绝")
    except pymysql.err.ProgrammingError as e:
        check(e.args[0] == 1064 and "admin database" in e.args[1],
              "管理面普通 SQL 报 1064 并提示管理命令")
    cur.execute("USE backup")
    cur.execute("SELECT COUNT(*) FROM t1")
    check(cur.fetchone()[0] >= 0, "USE backup 退出管理面后恢复正常查询")

    # ---------- 7. SET PASSWORD（最后执行，测试后恢复 changeme） ----------
    cur.execute("USE GlacierTDB")
    cur.execute("SET PASSWORD = 'glacier-tmp-9527'")
    cur.execute("USE backup")
    cur.close()
    conn.close()
    denied = False
    try:
        bad = connect(pwd=PASS)
        bad.close()
    except pymysql.err.OperationalError as e:
        denied = e.args[0] == 1045
    check(denied, "改密后旧密码被拒绝（1045）")
    conn2 = connect(pwd="glacier-tmp-9527")
    cur2 = conn2.cursor()
    cur2.execute("SELECT 1")
    check(cur2.fetchone()[0] == 1, "新密码可正常连接")
    cur2.execute("USE GlacierTDB")
    cur2.execute("SET PASSWORD = '%s'" % PASS)  # 恢复，避免影响后续测试
    cur2.execute("USE backup")
    cur2.close()
    conn2.close()
    conn3 = connect(pwd=PASS)
    conn3.close()
    check(True, "密码已恢复为默认值")

    print()
    print("-" * 44)
    print("ADMIN 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("ADMIN: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
