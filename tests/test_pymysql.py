#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_pymysql.py — PyMySQL 纯文本协议黑盒测试（针对 mysql-tcp-proxy-sqlite）

前置条件：
  * 容器 proxy 在网络 sqlproxy-net 上运行，端口 3309
  * tests/init_schema.sql 已执行

约束（与服务端约定一致）：
  * 服务端不支持 COM_STMT_PREPARE；PyMySQL 的 paramstyle=format 是
    客户端插值，全部走文本协议，天然兼容。
  * 不使用 CONCAT 等 MySQL 专有函数（SQLite 无此函数）。
  * t1 为 AUTOINCREMENT 自增主键（高水位单调），历史删除过行时
    insert_id 可能大于 MAX(id)+1，故断言放宽为「大于插入前 MAX(id)」。

全部断言通过时打印 PYMYSQL: ALL PASS 并 exit 0，否则 exit 1。
"""

import hashlib
import sys
import traceback

import pymysql

HOST = "proxy"
PORT = 3309
USER = "glacier"
PASSWORD = "iloveyou~"
DATABASE = "backup"

STATS = {"pass": 0, "fail": 0}


def ok(msg: str) -> None:
    STATS["pass"] += 1
    print("[PASS] %s" % msg)


def bad(msg: str) -> None:
    STATS["fail"] += 1
    print("[FAIL] %s" % msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        bad(msg)


def connect() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        charset="utf8mb4",       # utf8mb4：中文 + 4 字节 emoji
        autocommit=True,         # 事务用显式 BEGIN/COMMIT/ROLLBACK 文本语句
        read_timeout=60,
        write_timeout=60,
        connect_timeout=15,
    )


def main() -> int:
    conn = connect()
    cur = conn.cursor()

    # ---------- 1. 探活 ----------
    cur.execute("SELECT @@version")
    ver = cur.fetchone()[0]
    check(
        isinstance(ver, str) and any(c.isdigit() for c in ver),
        "SELECT @@version 返回版本号（%r）" % (ver,),
    )

    cur.execute("SHOW VARIABLES LIKE 'char%'")
    rows = cur.fetchall()
    varmap = {r[0]: r[1] for r in rows}
    check(
        "character_set_client" in varmap and "character_set_results" in varmap,
        "SHOW VARIABLES LIKE 'char%' 返回 character_set_* 变量",
    )
    check(
        "utf8" in str(varmap.get("character_set_client", "")).lower(),
        "character_set_client = %r（utf8 系）" % (varmap.get("character_set_client"),),
    )

    # ---------- 2. 清理本脚本历史残留（保证可重复执行） ----------
    cur.execute("DELETE FROM t1 WHERE name LIKE 'pymy%'")
    cur.execute("DELETE FROM t2 WHERE a >= 10000 AND a < 10100")
    cur.execute("DELETE FROM blob_t WHERE id = 9001")

    # ---------- 3. CRUD ----------
    cur.execute("INSERT INTO t1(name, val) VALUES (%s, %s)", ("pymy_行1", 1.5))
    check(cur.rowcount == 1, "INSERT 影响 1 行")

    cur.execute("SELECT name, val FROM t1 WHERE name = %s", ("pymy_行1",))
    row = cur.fetchone()
    check(
        row is not None and row[0] == "pymy_行1" and abs(float(row[1]) - 1.5) < 1e-9,
        "SELECT 读回插入行（name/val 一致）",
    )

    cur.execute("UPDATE t1 SET val = %s WHERE name = %s", (2.75, "pymy_行1"))
    check(cur.rowcount == 1, "UPDATE 影响 1 行")
    cur.execute("SELECT val FROM t1 WHERE name = %s", ("pymy_行1",))
    check(abs(float(cur.fetchone()[0]) - 2.75) < 1e-9, "UPDATE 后 val=2.75")

    cur.execute("DELETE FROM t1 WHERE name = %s", ("pymy_行1",))
    check(cur.rowcount == 1, "DELETE 影响 1 行")
    cur.execute("SELECT COUNT(*) FROM t1 WHERE name = %s", ("pymy_行1",))
    check(cur.fetchone()[0] == 0, "DELETE 后行数归零")

    # 中文表名/列名 + 反引号
    cur.execute("DELETE FROM `订单` WHERE `名称` = %s", ("pymy订单",))
    cur.execute("INSERT INTO `订单`(`名称`) VALUES (%s)", ("pymy订单",))
    cur.execute("SELECT COUNT(*) FROM `订单` WHERE `名称` = %s", ("pymy订单",))
    check(cur.fetchone()[0] == 1, "中文表「订单」读写（反引号标识符）")
    cur.execute("DELETE FROM `订单` WHERE `名称` = %s", ("pymy订单",))

    # ---------- 4. executemany 批量插入 100 行 ----------
    data = [(10000 + i, "bulk_%d" % i) for i in range(100)]
    cur.executemany("INSERT INTO t2(a, b) VALUES (%s, %s)", data)
    check(cur.rowcount == 100, "executemany 受影响行数 = 100（实际 %r）" % (cur.rowcount,))
    cur.execute("SELECT COUNT(*) FROM t2 WHERE a >= 10000 AND a < 10100")
    check(cur.fetchone()[0] == 100, "executemany 插入 100 行全部落库")
    cur.execute("SELECT b FROM t2 WHERE a = %s", (10042,))
    check(cur.fetchone()[0] == "bulk_42", "executemany 数据内容正确（a=10042 → bulk_42）")

    # ---------- 5. 事务：一次回滚、一次提交 ----------
    # 用显式文本协议 BEGIN/ROLLBACK/COMMIT（不依赖驱动的自动事务管理），
    # 同一连接内执行——服务端保证事务落在同一后端 SQLite 连接上。
    cur.execute("SELECT COUNT(*) FROM t1")
    before = cur.fetchone()[0]

    cur.execute("BEGIN")
    cur.execute("INSERT INTO t1(name, val) VALUES (%s, %s)", ("pymy_回滚行", 0.125))
    cur.execute("ROLLBACK")
    cur.execute("SELECT COUNT(*) FROM t1")
    check(cur.fetchone()[0] == before, "BEGIN..INSERT..ROLLBACK 后行数不变")

    cur.execute("BEGIN")
    cur.execute("INSERT INTO t1(name, val) VALUES (%s, %s)", ("pymy_提交行", 0.25))
    cur.execute("COMMIT")
    cur.execute("SELECT COUNT(*) FROM t1")
    check(cur.fetchone()[0] == before + 1, "BEGIN..INSERT..COMMIT 后行数+1")
    cur.execute("DELETE FROM t1 WHERE name = %s", ("pymy_提交行",))

    # ---------- 6. 中文 + emoji（utf8mb4 4 字节）往返 ----------
    text = "中文测试：你好世界 😀🎉 py-mysql-proxy"
    cur.execute("DELETE FROM t1 WHERE name = %s", (text,))
    cur.execute("INSERT INTO t1(name, val) VALUES (%s, %s)", (text, 8.8))
    cur.execute("SELECT name FROM t1 WHERE name = %s", (text,))
    got = cur.fetchone()[0]
    check(got == text, "utf8mb4 中文+4字节 emoji 往返一致（%r）" % (got,))
    cur.execute("DELETE FROM t1 WHERE name = %s", (text,))

    # ---------- 7. BLOB 二进制往返（md5 比对） ----------
    blob = bytes(range(256)) * 4          # 1024 字节，覆盖 0x00-0xFF 全字节
    md5_expect = hashlib.md5(blob).hexdigest()
    cur.execute("INSERT INTO blob_t(id, data) VALUES (9001, %s)", (blob,))
    cur.execute("SELECT data FROM blob_t WHERE id = 9001")
    got_blob = cur.fetchone()[0]
    check(
        isinstance(got_blob, (bytes, bytearray)) and bytes(got_blob) == blob,
        "BLOB 1024 字节（含 0x00/0xFF）二进制往返字节级一致",
    )
    check(
        hashlib.md5(bytes(got_blob)).hexdigest() == md5_expect,
        "BLOB md5 比对一致（%s）" % md5_expect,
    )
    cur.execute("DELETE FROM blob_t WHERE id = 9001")

    # ---------- 8. LAST_INSERT_ID ----------
    cur.execute("SELECT COALESCE(MAX(id), 0) FROM t1")
    before_id = int(cur.fetchone()[0])
    cur.execute("INSERT INTO t1(name, val) VALUES (%s, %s)", ("pymy_lid行", 5.5))
    lid_packet = cur.lastrowid                 # 来自 OK 包 insert_id
    cur.execute("SELECT LAST_INSERT_ID()")     # 文本协议查询
    lid_query = cur.fetchone()[0]
    check(
        lid_packet is not None and int(lid_packet) > before_id,
        "OK 包 insert_id 为新自增 id（>%d，得到 %r）" % (before_id, lid_packet),
    )
    check(
        int(lid_query) == int(lid_packet),
        "SELECT LAST_INSERT_ID() 与 OK 包 insert_id 一致（%r == %r）" % (lid_query, lid_packet),
    )
    cur.execute("DELETE FROM t1 WHERE name = %s", ("pymy_lid行",))

    # ---------- 9. SHOW TABLES 拿到列表 ----------
    cur.execute("SHOW TABLES")
    tables = [r[0] for r in cur.fetchall()]
    for t in ("t1", "t2", "blob_t", "t1_log", "订单"):
        check(t in tables, "SHOW TABLES 列表包含 %s" % t)
    check(isinstance(tables, list) and len(tables) >= 5, "SHOW TABLES 返回表名列表（%d 个）" % len(tables))

    # ---------- 10. 多库：一库一文件 ----------
    cur.execute("SHOW DATABASES")
    dbs = {str(r[0]) for r in cur.fetchall()}
    check("backup" in dbs, "SHOW DATABASES 含 backup（实际: %s）" % sorted(dbs))
    cur.execute("DROP DATABASE IF EXISTS pydb2")
    cur.execute("CREATE DATABASE pydb2")
    cur.execute("USE pydb2")
    cur.execute("CREATE TABLE multi_t(id INTEGER PRIMARY KEY, v TEXT)")
    cur.execute("INSERT INTO multi_t(v) VALUES ('库2数据')")
    cur.execute("SELECT DATABASE()")
    check(str(cur.fetchone()[0]) == "pydb2", "USE pydb2 后 SELECT DATABASE()=pydb2")
    cur.execute("USE backup")
    cur.execute("SELECT COUNT(*) FROM t1")
    t1_cnt = int(cur.fetchone()[0])
    check(t1_cnt >= 1, "切回 backup 后 t1 数据仍在（%d 行）" % t1_cnt)
    cur.execute("SELECT DATABASE()")
    check(str(cur.fetchone()[0]) == "backup", "USE backup 后 SELECT DATABASE()=backup")
    cur.execute("DROP DATABASE pydb2")
    cur.execute("SHOW DATABASES")
    dbs2 = {str(r[0]) for r in cur.fetchall()}
    check("pydb2" not in dbs2, "DROP DATABASE pydb2 后 SHOW DATABASES 不再包含")

    # ---------- 11. information_schema 兼容层 ----------
    cur.execute("SELECT ENGINE, SUPPORT FROM information_schema.ENGINES")
    eng = {r[0]: r[1] for r in cur.fetchall()}
    check("InnoDB" in eng and eng.get("InnoDB") == "DEFAULT",
          "information_schema.ENGINES 可查且 InnoDB=DEFAULT（%d 个引擎）" % len(eng))
    cur.execute("SELECT SCHEMA_NAME FROM information_schema.SCHEMATA")
    schemas = {r[0] for r in cur.fetchall()}
    check("backup" in schemas, "information_schema.SCHEMATA 列出 backup")
    cur.execute("SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES WHERE table_schema = DATABASE()")
    tabs = {r[0]: r[1] for r in cur.fetchall()}
    check("t1" in tabs and tabs.get("t1") == "BASE TABLE", "TABLES WHERE table_schema=DATABASE() 过滤生效")
    cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_schema = DATABASE() AND table_name = 't1'")
    cols_t1 = {r[0] for r in cur.fetchall()}
    check({"id", "name", "val"} <= cols_t1, "COLUMNS WHERE table_name='t1' 投影过滤生效（%s）" % sorted(cols_t1))
    cur.execute("SELECT COLLATION_NAME FROM information_schema.COLLATIONS WHERE COLLATION_NAME LIKE 'utf8mb4%'")
    colls = [r[0] for r in cur.fetchall()]
    check(len(colls) >= 3, "COLLATIONS LIKE 'utf8mb4%%' 过滤生效（%d 条）" % len(colls))

    # ---------- 收尾 ----------
    cur.close()
    conn.close()

    print()
    print("-" * 44)
    print("PYMYSQL 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("PYMYSQL: ALL PASS")
        return 0
    print("PYMYSQL: FAILED")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 —— 打印异常便于排障
        traceback.print_exc()
        print("PYMYSQL: FAILED（异常: %r）" % (exc,))
        sys.exit(1)
