#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GlacierTDB Navicat 兼容性黑盒测试。

固化 2026-10-15 Navicat 联调踩过的全部坑：
探活/占位符、系统库直查（information_schema/mysql/performance_schema/sys）、
未覆盖 SHOW 变体兜底、DESC/DESCRIBE、库名前缀（backup.tbl / `backup`.`tbl`）、
CREATE DATABASE 各变体（含多行、无 DEFAULT 前缀、CHARSET）。
"""
import sys

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


def ok(cur, sql, msg):
    """执行成功即过（Navicat 只要求不报错）。"""
    try:
        cur.execute(sql)
        return True
    except pymysql.err.Error as e:
        check(False, "%s（%s: %s）" % (msg, e.args[0], str(e.args[1])[:80]))
        return False


def main() -> int:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DATABASE, charset="utf8mb4", autocommit=True,
                           read_timeout=20, write_timeout=20)
    cur = conn.cursor()

    # ---------- 1. Navicat 连接探活 ----------
    for sql, msg in [
        ("select @@version_comment limit 1", "探活 select @@version_comment limit 1"),
        ("SET NAMES utf8mb4", "SET NAMES utf8mb4"),
        ("SET SQL_MODE = ''", "SET SQL_MODE"),
        ("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ", "SET SESSION TRANSACTION ISOLATION"),
    ]:
        if ok(cur, sql, msg):
            check(True, msg)
    cur.execute("select @@version_comment limit 1")
    v = cur.fetchone()
    check(v is not None and len(v) == 1, "探活返回单行（%r）" % (v,))

    # ---------- 2. 占位符拒绝（select $$ 曾致 libmysql 状态机失步）----------
    try:
        cur.execute("select $$")
        check(False, "select $$ 应报 1064 拒绝")
    except pymysql.err.ProgrammingError as e:
        check(e.args[0] == 1064, "select $$ 报 1064（非透传）")

    # ---------- 3. 系统库直查（用户/引擎/元数据界面）----------
    cases = [
        ("SELECT ENGINE, SUPPORT FROM information_schema.ENGINES", "information_schema.ENGINES"),
        ("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA", "information_schema.SCHEMATA"),
        ("SELECT TABLE_NAME FROM information_schema.TABLES WHERE table_schema = DATABASE()", "information_schema.TABLES WHERE table_schema=DATABASE()"),
        ("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_schema = DATABASE() AND table_name = 't1'", "information_schema.COLUMNS WHERE table_name='t1'"),
        ("SELECT COLLATION_NAME FROM information_schema.COLLATIONS WHERE COLLATION_NAME LIKE 'utf8mb4%'", "information_schema.COLLATIONS LIKE"),
        ("SELECT User, Host, plugin FROM mysql.user", "mysql.user"),
        ("SELECT User FROM mysql.user WHERE User = 'glacier'", "mysql.user WHERE User='glacier'"),
        ("SELECT * FROM mysql.db", "mysql.db 空集"),
        ("SELECT * FROM performance_schema.session_variables", "performance_schema 空集"),
        ("SELECT * FROM sys.version", "sys 空集"),
    ]
    for sql, msg in cases:
        if ok(cur, sql, msg):
            rows = cur.fetchall()
            check(True, "%s（%d 行）" % (msg, len(rows)))

    # ---------- 4. 未覆盖 SHOW 变体兜底（绝不 1064）----------
    for sql in [
        "SHOW PROCEDURE STATUS", "SHOW FUNCTION STATUS", "SHOW PLUGINS",
        "SHOW MASTER STATUS", "SHOW SLAVE STATUS", "SHOW REPLICA STATUS",
        "SHOW ENGINE INNODB STATUS", "SHOW EVENTS", "SHOW OPEN TABLES",
        "SHOW PROFILES", "SHOW BINLOG EVENTS", "SHOW SOMETHING NEW",
    ]:
        ok(cur, sql, "兜底 %s" % sql)

    # ---------- 5. DESC / DESCRIBE ----------
    for sql in ["DESC t1", "DESCRIBE t1", "DESC t2"]:
        if ok(cur, sql, sql):
            cols = [r[0] for r in cur.fetchall()]
            check(len(cols) > 0, "%s 返回列定义（%s）" % (sql, cols))

    # ---------- 6. 库名前缀（backup.tbl 三形态）----------
    for sql in [
        "SELECT COUNT(*) FROM backup.t1",
        "SELECT COUNT(*) FROM `backup`.`t1`",
        "SELECT COUNT(*) FROM `backup`.t1",
        "SHOW COLUMNS FROM `backup`.`t1`",
        "SHOW FULL COLUMNS FROM backup.t2",
        "SHOW INDEX FROM backup.t2",
        "SHOW CREATE TABLE backup.t1",
    ]:
        if ok(cur, sql, "库名前缀 %s" % sql):
            check(True, "库名前缀 %s" % sql)

    # ---------- 7. Navicat「新建数据库」各变体 ----------
    cur.execute("DROP DATABASE IF EXISTS navicat_a")
    cur.execute("DROP DATABASE IF EXISTS navicat_b")
    cur.execute("DROP DATABASE IF EXISTS navicat_c")
    # 单行 + DEFAULT（旧版 Navicat）
    if ok(cur, "CREATE DATABASE `navicat_a` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci", "CREATE DATABASE DEFAULT CHARACTER SET"):
        check(True, "CREATE DATABASE DEFAULT CHARACTER SET 变体")
    # 无 DEFAULT 前缀 + 单引号值（用户实测踩坑语句）
    if ok(cur, "CREATE DATABASE `navicat_b` CHARACTER SET 'utf8mb4'", "CREATE DATABASE CHARACTER SET 'utf8mb4'"):
        check(True, "CREATE DATABASE CHARACTER SET 无 DEFAULT 变体")
    # 多行（\n 换行的 DEFAULT 子句）
    try:
        cur.execute("CREATE DATABASE `navicat_c`\nDEFAULT CHARACTER SET utf8mb4\nCOLLATE utf8mb4_0900_ai_ci")
        check(True, "CREATE DATABASE 多行子句变体")
    except pymysql.err.Error as e:
        check(False, "CREATE DATABASE 多行子句变体（%s）" % str(e.args[1])[:80])
    cur.execute("SHOW DATABASES")
    dbs = {r[0] for r in cur.fetchall()}
    check({"navicat_a", "navicat_b", "navicat_c"} <= dbs, "三个测试库全部创建成功")
    # 新库立即可用
    cur.execute("USE navicat_a")
    cur.execute("CREATE TABLE hello(id INTEGER PRIMARY KEY, v TEXT)")
    cur.execute("INSERT INTO hello(v) VALUES ('navicat-ok')")
    cur.execute("SELECT v FROM hello")
    check(cur.fetchone()[0] == "navicat-ok", "新库建表写入读取全通")
    # glacier 保留名
    try:
        cur.execute("CREATE DATABASE glacier")
        check(False, "CREATE DATABASE glacier 应拒绝")
    except pymysql.err.Error as e:
        check("reserved" in str(e.args[1]), "glacier 保留名拒绝")
    # 清理
    cur.execute("USE backup")
    for d in ("navicat_a", "navicat_b", "navicat_c"):
        cur.execute("DROP DATABASE " + d)
    cur.execute("SHOW DATABASES")
    dbs2 = {r[0] for r in cur.fetchall()}
    check(not ({"navicat_a", "navicat_b", "navicat_c"} & dbs2), "测试库清理完成")

    cur.close()
    conn.close()

    print()
    print("-" * 44)
    print("NAVICAT 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("NAVICAT: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
