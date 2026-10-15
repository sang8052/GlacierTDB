#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
test_concurrent.py — 并发写排队验证（针对 mysql-tcp-proxy-sqlite）

前置条件：
  * 容器 proxy 在网络 sqlproxy-net 上运行，端口 3309
  * tests/init_schema.sql 已执行

验证两点（对应设计：全局写锁串行化、每连接独立 SQLite 连接）：
  1. 双连接 + 双线程并发各插 200 行到 t2（复合主键 a=线程号*1000+i, b='x'），
     结束后总数 = 400，无主键冲突、无丢写。
  2. 连接 A BEGIN..INSERT（不提交，持锁）期间，连接 B INSERT 应排队等待；
     A ROLLBACK 释放后 B 的写入成功落库。

全部断言通过时打印 CONCURRENCY: ALL PASS 并 exit 0，否则 exit 1。
"""

import sys
import threading
import time
import traceback

import pymysql

HOST = "proxy"
PORT = 3309
USER = "glacier"
PASSWORD = "iloveyou~"
DATABASE = "backup"

ROWS_PER_THREAD = 200
HOLD_SECONDS = 1.5   # A 持锁时长

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


def new_conn() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        charset="utf8mb4",
        autocommit=True,
        read_timeout=90,      # 兜底：若服务端写锁异常死等，测试不至于永久挂起
        write_timeout=90,
        connect_timeout=15,
    )


# ---------- 场景 1：双线程并发批量插入 ----------

def writer_thread(thread_no: int, errors: list) -> None:
    """每个线程独立连接，串行插入 200 行：a = 线程号*1000 + i, b = 'x'。"""
    try:
        conn = new_conn()
        cur = conn.cursor()
        for i in range(ROWS_PER_THREAD):
            cur.execute(
                "INSERT INTO t2 (a, b) VALUES (%s, %s)",
                (thread_no * 1000 + i, "x"),
            )
        cur.close()
        conn.close()
    except Exception as exc:  # noqa: BLE001 —— 记录线程内错误
        errors.append((thread_no, repr(exc)))


def test_parallel_writers(cur: pymysql.cursors.Cursor) -> None:
    print(">>> 场景 1：双连接双线程并发插入 %d x 2 行到 t2" % ROWS_PER_THREAD)

    # 清场：删除本场景历史数据（a in [1000, 3000)）
    cur.execute("DELETE FROM t2 WHERE a >= 1000 AND a < 3000")

    errors: list = []
    t1 = threading.Thread(target=writer_thread, args=(1, errors), name="writer-1")
    t2 = threading.Thread(target=writer_thread, args=(2, errors), name="writer-2")
    start = time.time()
    t1.start()
    t2.start()
    t1.join(180)
    t2.join(180)
    elapsed = time.time() - start

    alive = t1.is_alive() or t2.is_alive()
    check(not alive, "两个写线程均已结束（耗时 %.1fs）" % elapsed)
    check(
        not errors,
        "并发写过程无异常/无主键冲突（错误: %s）" % (errors[:3] if errors else "无"),
    )

    cur.execute("SELECT COUNT(*) FROM t2 WHERE a >= 1000 AND a < 3000")
    total = int(cur.fetchone()[0])
    check(total == 2 * ROWS_PER_THREAD, "总行数 = %d（实际 %d）" % (2 * ROWS_PER_THREAD, total))

    cur.execute(
        "SELECT COUNT(DISTINCT a) FROM t2 WHERE a >= 1000 AND a < 3000"
    )
    distinct = int(cur.fetchone()[0])
    check(distinct == total, "无主键冲突/丢失（COUNT = COUNT DISTINCT = %d）" % distinct)

    cur.execute("SELECT COUNT(*) FROM t2 WHERE a >= 1000 AND a < 2000")
    n1 = int(cur.fetchone()[0])
    check(n1 == ROWS_PER_THREAD, "线程 1（a in [1000,2000)）行数 = %d（实际 %d）" % (ROWS_PER_THREAD, n1))

    cur.execute("SELECT COUNT(*) FROM t2 WHERE a >= 2000 AND a < 3000")
    n2 = int(cur.fetchone()[0])
    check(n2 == ROWS_PER_THREAD, "线程 2（a in [2000,3000)）行数 = %d（实际 %d）" % (ROWS_PER_THREAD, n2))


# ---------- 场景 2：A 持锁期间 B 排队 ----------

def test_write_queueing(cur: pymysql.cursors.Cursor) -> None:
    print(">>> 场景 2：A BEGIN..INSERT 持锁不提交，B INSERT 排队等待")

    conn_a = new_conn()
    conn_b = new_conn()
    a_holding = threading.Event()
    a_finished = threading.Event()
    holder_errors: list = []

    def holder() -> None:
        try:
            ac = conn_a.cursor()
            ac.execute("BEGIN")
            ac.execute(
                "INSERT INTO t1 (name, val) VALUES (%s, %s)", ("并发_持锁A", 1.0)
            )
            a_holding.set()          # 已 INSERT，进入持锁窗口
            time.sleep(HOLD_SECONDS)  # 故意不提交，占住写锁
            ac.execute("ROLLBACK")
            ac.close()
        except Exception as exc:  # noqa: BLE001
            holder_errors.append(repr(exc))
        finally:
            a_finished.set()

    th = threading.Thread(target=holder, name="lock-holder")
    th.start()

    if not a_holding.wait(30):
        check(False, "A 未能在 30s 内完成 BEGIN..INSERT（%s）" % holder_errors)
        a_finished.wait(30)
        conn_a.close()
        conn_b.close()
        return

    # B 在 A 持锁期间发起写入：应被服务端写队列挡住，直到 A ROLLBACK 释放
    bc = conn_b.cursor()
    b_error = None
    t0 = time.time()
    try:
        bc.execute("INSERT INTO t2 (a, b) VALUES (%s, %s)", (2999, "queue_b"))
    except Exception as exc:  # noqa: BLE001
        b_error = repr(exc)
    waited = time.time() - t0
    bc.close()

    th.join(60)
    conn_a.close()
    conn_b.close()

    check(not holder_errors, "A 侧 BEGIN..ROLLBACK 无异常（%s）" % (holder_errors or "无"))
    check(b_error is None, "B 在 A 持锁期间的 INSERT 最终成功（错误: %s）" % (b_error,))
    check(
        waited > 0.3,
        "B 的写被排队（阻塞 %.2fs 后成功，A 持锁 %.1fs）" % (waited, HOLD_SECONDS),
    )

    # A 已回滚：其行未落库
    cur.execute("SELECT COUNT(*) FROM t1 WHERE name = %s", ("并发_持锁A",))
    check(int(cur.fetchone()[0]) == 0, "A ROLLBACK 后其插入行未落库")

    # B 是 autocommit：行必须已落库
    cur.execute("SELECT COUNT(*) FROM t2 WHERE a = %s", (2999,))
    check(int(cur.fetchone()[0]) == 1, "B 的行在 A 释放锁后成功落库（a=2999）")


def main() -> int:
    conn = new_conn()
    cur = conn.cursor()

    # 探活：确保服务可达
    cur.execute("SELECT 1")
    check(cur.fetchone()[0] == 1, "探活 SELECT 1")

    test_parallel_writers(cur)
    test_write_queueing(cur)

    cur.close()
    conn.close()

    print()
    print("-" * 44)
    print("CONCURRENCY 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("CONCURRENCY: ALL PASS")
        return 0
    print("CONCURRENCY: FAILED")
    return 1


if __name__ == "__main__":
    rc = 0
    try:
        rc = main()
    except Exception as exc:  # noqa: BLE001 —— 打印异常便于排障
        traceback.print_exc()
        print("CONCURRENCY: FAILED（异常: %r）" % (exc,))
        sys.exit(1)


# ==================== 并发模型 v2：多库并行写 + 配置热改（由 run_all 之外的扩展断言调用） ====================
def test_multidb_parallel_and_admin():
    """跨库写并行、同库写排队、并发配置热改、glacier 保留名。"""
    import time as _t
    import pymysql as _pm

    def conn(db):
        return _pm.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=db, charset="utf8mb4", autocommit=True,
                           read_timeout=30, write_timeout=30)

    stats = {"p": 0, "f": 0}

    def ck(cond, msg):
        if cond:
            stats["p"] += 1
            print("[PASS] %s" % msg)
        else:
            stats["f"] += 1
            print("[FAIL] %s" % msg)

    for stmt in ("DROP DATABASE IF EXISTS pdb_a", "DROP DATABASE IF EXISTS pdb_b",
                 "CREATE DATABASE pdb_a", "CREATE DATABASE pdb_b"):
        c = conn("backup"); c.cursor().execute(stmt); c.close()

    ca, cb = conn("pdb_a"), conn("pdb_b")
    ca.cursor().execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
    cb.cursor().execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")

    # 1) 跨库并行写：A 库 BEGIN 持锁 1.5s，B 库写应立即完成（并行）
    cur_a = ca.cursor()
    cur_a.execute("BEGIN")
    cur_a.execute("INSERT INTO t(v) VALUES('a_hold')")
    t0 = _t.time()
    cb.cursor().execute("INSERT INTO t(v) VALUES('b_fast')")
    dt_cross = _t.time() - t0
    ck(dt_cross < 0.5, "跨库写并行：B 库写在 A 库持锁期间 %.2fs 内完成（<0.5s）" % dt_cross)
    cur_a.execute("ROLLBACK")

    # 2) 同库写排队：A 库 BEGIN 持锁 1.5s 不提交，另一个 A 库连接写要排队
    ca2 = conn("pdb_a")
    cur_a.execute("BEGIN")
    cur_a.execute("INSERT INTO t(v) VALUES('a_hold2')")
    def _slow_writer():
        pass
    t0 = _t.time()
    import threading
    err = []
    def w2():
        try:
            ca2.cursor().execute("INSERT INTO t(v) VALUES('a2_waited')")
        except Exception as e:
            err.append(e)
    th = threading.Thread(target=w2); th.start()
    _t.sleep(1.5)
    cur_a.execute("ROLLBACK")
    th.join(10)
    dt_same = _t.time() - t0
    ck(not err and dt_same >= 1.4, "同库写排队：第二个连接等待 A 释放锁（%.2fs）" % dt_same)

    # 3) 管理面热改并发配置
    cadm = conn("backup")
    cadm.cursor().execute("USE GlacierTDB")
    cadm.cursor().execute("SET GLOBAL_THREADS = 16")
    cadm.cursor().execute("SET PER_DB_READ_THREADS = 8")
    cadm.cursor().execute("SET DB_READ_THREADS pdb_a = 2")
    curadm = cadm.cursor()
    curadm.execute("SELECT name, value FROM server_config")
    rows = {r[0]: r[1] for r in curadm.fetchall()}
    ck(rows.get("concurrency.global_threads") == "16", "热改 global_threads=16 生效（实际 %s）" % rows.get("concurrency.global_threads"))
    ck(rows.get("concurrency.per_db_read_threads") == "8", "热改 per_db_read_threads=8 生效（实际 %s）" % rows.get("concurrency.per_db_read_threads"))
    ck(rows.get("concurrency.db_read_threads.pdb_a") == "2", "热改 db_read_threads.pdb_a=2 生效（实际 %s）" % rows.get("concurrency.db_read_threads.pdb_a"))

    # 4) glacier 保留名禁止创建（在普通库下执行，走真实建库路径）
    cusr = conn("backup")
    try:
        cusr.cursor().execute("CREATE DATABASE glacier")
        ck(False, "CREATE DATABASE glacier 应被拒绝")
    except _pm.err.Error as e:  # 1007 被 pymysql 映射为 ProgrammingError
        ck(e.args[0] in (1007, 1064) and "reserved" in e.args[1],
           "CREATE DATABASE glacier 被拒绝（%s）" % e.args[1][:60])
    cusr.close()
    # USE glacier 是管理库别名
    curadm2 = cadm.cursor()
    curadm2.execute("USE glacier")
    curadm2.execute("SELECT DATABASE()")
    ck(curadm2.fetchone()[0] == "GlacierTDB", "USE glacier 为管理库别名")

    # 清理
    for stmt in ("DROP DATABASE IF EXISTS pdb_a", "DROP DATABASE IF EXISTS pdb_b"):
        try:
            cadm.cursor().execute(stmt)
        except Exception:
            pass
    cadm.cursor().execute("SET GLOBAL_THREADS = 0")
    cadm.cursor().execute("SET PER_DB_READ_THREADS = 0")
    cadm.cursor().execute("SET DB_READ_THREADS pdb_a = 0")
    for c in (ca, cb, ca2, cadm):
        try:
            c.close()
        except Exception:
            pass

    print()
    print("MULTIDB 汇总: PASS=%d FAIL=%d" % (stats["p"], stats["f"]))
    if stats["f"] == 0:
        print("MULTIDB: ALL PASS")
        return 0
    return 1


rc2 = test_multidb_parallel_and_admin()
sys.exit(rc or rc2)
