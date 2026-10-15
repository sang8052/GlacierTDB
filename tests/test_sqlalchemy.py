#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_sqlalchemy.py — SQLAlchemy 2.x + PyMySQL 方言黑盒测试
（针对 mysql-tcp-proxy-sqlite）

前置条件：
  * 容器 proxy 在网络 sqlproxy-net 上运行，端口 3309
  * tests/init_schema.sql 已执行

要点：
  * 服务端不支持 COM_STMT_PREPARE；pymysql 方言本身不做服务端
    预处理（参数为客户端插值），全部走文本协议。
  * 只用 text() + 命名绑定参数执行标准 SQL；
    不使用 CONCAT 等 MySQL 专有大写函数（SQLite 无此函数）。
  * SQLAlchemy 2.x 中 Connection 执行 DML 会隐式开启事务，需显式 commit。

全部断言通过时打印 SQLALCHEMY: ALL PASS 并 exit 0，否则 exit 1。
"""

import sys
import traceback

from sqlalchemy import create_engine, inspect, text

URL = "mysql+pymysql://glacier:iloveyou~@proxy:3309/backup?charset=utf8mb4"

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


def main() -> int:
    engine = create_engine(URL)

    with engine.connect() as conn:
        # ---------- 1. 探活 ----------
        ver = conn.execute(text("SELECT @@version")).scalar()
        check(
            isinstance(ver, str) and any(c.isdigit() for c in ver),
            "SELECT @@version 返回版本号（%r）" % (ver,),
        )

        # ---------- 2. 清理本脚本历史残留 ----------
        conn.execute(text("DELETE FROM t1 WHERE name LIKE 'sa%'"))
        conn.commit()

        # ---------- 3. text() CRUD（命名绑定参数） ----------
        conn.execute(
            text("INSERT INTO t1 (name, val) VALUES (:name, :val)"),
            {"name": "sa_行A", "val": 4.5},
        )
        conn.commit()
        row = conn.execute(
            text("SELECT name, val FROM t1 WHERE name = :name"), {"name": "sa_行A"}
        ).fetchone()
        check(
            row is not None and row[0] == "sa_行A" and abs(float(row[1]) - 4.5) < 1e-9,
            "text() INSERT + SELECT 往返一致",
        )

        conn.execute(
            text("UPDATE t1 SET val = :v WHERE name = :n"), {"v": 5.5, "n": "sa_行A"}
        )
        conn.commit()
        v = conn.execute(
            text("SELECT val FROM t1 WHERE name = :n"), {"n": "sa_行A"}
        ).scalar()
        check(abs(float(v) - 5.5) < 1e-9, "text() UPDATE 后 val=5.5")

        conn.execute(text("DELETE FROM t1 WHERE name = :n"), {"n": "sa_行A"})
        conn.commit()
        n = conn.execute(
            text("SELECT COUNT(*) FROM t1 WHERE name = :n"), {"n": "sa_行A"}
        ).scalar()
        check(n == 0, "text() DELETE 后行数归零")

        # 中文读写
        conn.execute(
            text("INSERT INTO t1 (name, val) VALUES (:name, :val)"),
            {"name": "sa_中文行·数据库", "val": 7.25},
        )
        conn.commit()
        got = conn.execute(
            text("SELECT name FROM t1 WHERE name = :n"), {"n": "sa_中文行·数据库"}
        ).scalar()
        check(got == "sa_中文行·数据库", "text() 中文数据往返一致")
        conn.execute(text("DELETE FROM t1 WHERE name = :n"), {"n": "sa_中文行·数据库"})
        conn.commit()

        # ---------- 4. 事务：一次提交、一次回滚 ----------
        before = int(conn.execute(text("SELECT COUNT(*) FROM t1")).scalar())

        # 4.1 提交：engine.begin() 正常落库
        with engine.begin() as tx:
            tx.execute(
                text("INSERT INTO t1 (name, val) VALUES (:n, :v)"),
                {"n": "sa_提交", "v": 1.0},
            )
        after_commit = int(conn.execute(text("SELECT COUNT(*) FROM t1")).scalar())
        check(after_commit == before + 1, "engine.begin() 提交后行数+1（%d -> %d）" % (before, after_commit))

        # 4.2 回滚：块内抛异常 -> 自动 ROLLBACK，行数不变
        try:
            with engine.begin() as tx:
                tx.execute(
                    text("INSERT INTO t1 (name, val) VALUES (:n, :v)"),
                    {"n": "sa_回滚", "v": 2.0},
                )
                raise RuntimeError("force-rollback")
        except RuntimeError:
            pass
        after_rollback = int(conn.execute(text("SELECT COUNT(*) FROM t1")).scalar())
        check(after_rollback == after_commit, "事务异常回滚后行数不变（%d）" % after_rollback)

        conn.execute(text("DELETE FROM t1 WHERE name IN ('sa_提交', 'sa_回滚')"))
        conn.commit()

    # ---------- 5. 反射 inspect(engine) ----------
    insp = inspect(engine)
    tables = insp.get_table_names()
    for t in ("t1", "t2", "blob_t", "t1_log"):
        check(t in tables, "反射 get_table_names() 包含 %s" % t)
    check("订单" in tables, "反射 get_table_names() 包含中文表「订单」")

    cols_t1 = [c["name"] for c in insp.get_columns("t1")]
    check(
        {"id", "name", "val"} <= set(cols_t1),
        "get_columns('t1') 含 id/name/val（实际: %s）" % cols_t1,
    )

    cols_t2 = [c["name"] for c in insp.get_columns("t2")]
    check(
        {"a", "b"} <= set(cols_t2),
        "get_columns('t2') 含复合主键列 a/b（实际: %s）" % cols_t2,
    )

    engine.dispose()

    print()
    print("-" * 44)
    print("SQLALCHEMY 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("SQLALCHEMY: ALL PASS")
        return 0
    print("SQLALCHEMY: FAILED")
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001 —— 打印异常便于排障
        traceback.print_exc()
        print("SQLALCHEMY: FAILED（异常: %r）" % (exc,))
        sys.exit(1)
