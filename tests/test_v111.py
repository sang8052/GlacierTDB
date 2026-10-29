#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.1.1 黑盒测试：
1) MySQL 兼容 M3：ON DUPLICATE KEY UPDATE / VALUES(col) / FOR UPDATE 剥离 /
   SET FOREIGN_KEY_CHECKS / CONCAT_WS / LPAD / RPAD / DATE_FORMAT 周编号族
2) caching_sha2_password：CREATE USER IDENTIFIED WITH / 登录成功 / 错误密码拒绝 /
   SET PASSWORD 后缓存失效（旧密码立即失效）/ ALTER USER 切换插件
3) rollup 多粒度预聚合：触发器同遍落 m1/h1/d1 / 长范围聚合自动路由（结果与 raw 精确一致）/
   GROUP BY time(N) 路由 / REBUILD ROLLUP 存量回填
4) GROUP BY 分片并行：/*+ PARALLEL(n) */ 分片-合并与串行结果一致 / EXPLAIN PARALLEL
5) 明细导出并行（方案 B）：rowid 区间并行 scan 结果与串行一致（含 * 展开与 LIMIT）
6) SCRIPT 后处理管道：SUBMIT QUERY ... SCRIPT / 产物输出 / 白名单拒绝
"""
import datetime
import time

import pymysql
import pymysql._auth as _pa

# ---- PyMySQL 2.2.8 客户端缺陷规避 ----
# caching_sha2_password_auth 的 RSA 全认证路径结尾缺少 return（函数隐式返回 None），
# connections.py 主循环拿到 None 后崩溃（对任何 MySQL 8 服务端同样复现）。
# 服务端已正确完成认证并发出 OK 包（被 _roundtrip 消费），此处仅补一个"假 OK 包"
# 让客户端状态机正常收尾；错误路径在 _roundtrip 内已 check_error 抛出。
_orig_caching_sha2 = _pa.caching_sha2_password_auth


class _FakeOK:
    def is_auth_switch_request(self):
        return False

    def is_extra_auth_data(self):
        return False

    def is_ok_packet(self):
        return True

    def check_error(self):
        return None


def _caching_sha2_fixed(conn, pkt):
    r = _orig_caching_sha2(conn, pkt)
    return _FakeOK() if r is None else r


_pa.caching_sha2_password_auth = _caching_sha2_fixed

HOST, PORT, USER, PASSWORD = "proxy", 3309, "glacier", "iloveyou~"
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def connect(user=USER, password=PASSWORD, db="backup"):
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


def week_num(t, monday_first):
    """%U/%u 语义（00-53）：含当年首个周日/周一的那周为第 1 周。"""
    if monday_first:
        off = t.weekday()  # 距本周一
    else:
        off = (t.weekday() + 1) % 7  # 距本周日
    start = t - datetime.timedelta(days=off)
    doy = start.timetuple().tm_yday if start.year == t.year else 0
    return (doy + 6) // 7


def main() -> int:
    conn = connect()
    cur = conn.cursor()

    # ---------- 1. MySQL 兼容 M3 ----------
    cur.execute("SET FOREIGN_KEY_CHECKS = 0")
    check(True, "SET FOREIGN_KEY_CHECKS = 0 接受")

    cur.execute("SELECT CONCAT_WS('-', 'a', NULL, 'b')")
    check(cur.fetchone()[0] == "a-b", "CONCAT_WS 跳过 NULL")
    cur.execute("SELECT CONCAT_WS(NULL, 'a')")
    check(cur.fetchone()[0] is None, "CONCAT_WS sep=NULL → NULL")

    cur.execute("SELECT LPAD('7', 3, '0'), RPAD('7', 3, 'x'), LPAD('abcd', 2, 'x')")
    row = cur.fetchone()
    check(row[0] == "007" and row[1] == "7xx" and row[2] == "ab", "LPAD/RPAD（补齐与截断）")

    # DATE_FORMAT 周编号族：与 Python 参考实现交叉验证
    d = datetime.date(2026, 10, 21)
    cur.execute("SELECT DATE_FORMAT('2026-10-21', '%U'), DATE_FORMAT('2026-10-21', '%u'), "
                "DATE_FORMAT('2026-10-21', '%V'), DATE_FORMAT('2026-10-21', '%X'), "
                "DATE_FORMAT('2026-10-21', '%v'), DATE_FORMAT('2026-10-21', '%x')")
    row = cur.fetchone()
    iso_y, iso_w = d.isocalendar()[0], d.isocalendar()[1]
    check(str(row[0]) == "%02d" % week_num(d, False) and str(row[1]) == "%02d" % week_num(d, True),
          "DATE_FORMAT %%U/%%u 周编号（%s/%s）" % (row[0], row[1]))
    check(str(row[2]) == "%02d" % iso_w and str(row[4]) == "%02d" % iso_w,
          "DATE_FORMAT %%V/%%v 周编号（%s/%s）" % (row[2], row[4]))
    check(str(row[3]) == "%04d" % iso_y and str(row[5]) == "%04d" % iso_y,
          "DATE_FORMAT %%X/%%x 周年份（%s/%s）" % (row[3], row[5]))

    cur.execute("DROP TABLE IF EXISTS t_m3")
    cur.execute("CREATE TABLE t_m3 (id INTEGER PRIMARY KEY, v TEXT)")
    cur.execute("INSERT INTO t_m3 (id, v) VALUES (1, 'a')")
    cur.execute("INSERT INTO t_m3 (id, v) VALUES (1, 'b') ON DUPLICATE KEY UPDATE v = VALUES(v)")
    cur.execute("SELECT v FROM t_m3 WHERE id = 1")
    check(cur.fetchone()[0] == "b", "ON DUPLICATE KEY UPDATE + VALUES(col) → excluded.col")
    cur.execute("INSERT INTO t_m3 (id, v) VALUES (2, 'c') ON DUPLICATE KEY UPDATE v = VALUES(v)")
    cur.execute("SELECT count(*) FROM t_m3")
    check(cur.fetchone()[0] == 2, "ON DUPLICATE 不冲突时正常插入")
    cur.execute("SELECT id, v FROM t_m3 WHERE id = 2 FOR UPDATE")
    check(cur.fetchone()[0] == 2, "SELECT ... FOR UPDATE 剥离后正常执行")

    # ---------- 2. caching_sha2_password ----------
    cur.execute("USE GlacierTDB")
    for u in ("sha2u", "sha2u2"):
        try:
            cur.execute("DROP USER '%s'" % u)
        except pymysql.MySQLError:
            pass
    cur.execute("CREATE USER 'sha2u'@'%' IDENTIFIED WITH caching_sha2_password BY 'sha2pass1'")
    check(True, "CREATE USER ... IDENTIFIED WITH caching_sha2_password")

    c2 = connect(user="sha2u", password="sha2pass1", db="backup")
    c2c = c2.cursor()
    c2c.execute("SELECT 1")
    check(c2c.fetchone()[0] == 1, "caching_sha2_password 登录成功（RSA 全认证路径）")
    c2.close()

    try:
        connect(user="sha2u", password="wrongpass", db="backup")
        check(False, "错误密码应拒绝")
    except pymysql.MySQLError as e:
        check(True, "错误密码拒绝（%s）" % e.args[0])

    # 再次登录建立缓存 → 改密 → 旧密码应立即失效（缓存失效验证）
    c2 = connect(user="sha2u", password="sha2pass1", db="backup")
    c2.close()
    c2 = connect(user="sha2u", password="sha2pass1", db="backup")  # 缓存命中（fast path）
    c2.close()
    cur.execute("SET PASSWORD FOR sha2u = 'sha2pass2'")
    check(True, "SET PASSWORD（触发 caching_sha2 缓存失效）")
    try:
        connect(user="sha2u", password="sha2pass1", db="backup")
        check(False, "改密后旧密码应失效（缓存已失效）")
    except pymysql.MySQLError:
        check(True, "改密后旧密码立即失效")
    c2 = connect(user="sha2u", password="sha2pass2", db="backup")
    c2.close()
    check(True, "新密码登录成功")

    # ALTER USER 切回 native
    cur.execute("ALTER USER 'sha2u' IDENTIFIED WITH mysql_native_password")
    c2 = connect(user="sha2u", password="sha2pass2", db="backup")
    c2c = c2.cursor()
    c2c.execute("SELECT 1")
    check(c2c.fetchone()[0] == 1, "ALTER USER 切回 mysql_native_password 后登录正常")
    c2.close()
    cur.execute("SHOW GRANTS FOR 'sha2u'")

    # ---------- 3. rollup 多粒度预聚合 ----------
    cur.execute("DROP DATABASE IF EXISTS rollupdb")
    cur.execute("CREATE DATABASE rollupdb")
    cur.execute("USE rollupdb")
    cur.execute("CREATE TABLE series (id INTEGER PRIMARY KEY, meas TEXT NOT NULL, tags TEXT NOT NULL)")
    cur.execute("CREATE TABLE points (series_id INTEGER NOT NULL, ts INTEGER NOT NULL, val REAL,"
                " PRIMARY KEY (series_id, ts)) WITHOUT ROWID")

    # 3 个序列 × 每分钟一点 × 120 分钟，起点对齐整分钟（epoch ms, UTC）
    base = 1760000000000  # 2025-10-09T07:33:20Z 附近；取整到分钟
    base = base // 60000 * 60000
    rows = []
    for sid in (1, 2, 3):
        for i in range(120):
            rows.append("(%d, %d, %s)" % (sid, base + i * 60000, (sid * 10 + i % 10) / 2.0))
    for chunk in range(0, len(rows), 60):
        cur.execute("INSERT INTO points (series_id, ts, val) VALUES " + ",".join(rows[chunk:chunk + 60]))

    cur.execute("SELECT count(*) FROM points_m1")
    check(cur.fetchone()[0] > 0, "触发器同遍落 points_m1")
    cur.execute("SELECT count(*), sum(cnt) FROM points_m1")
    row = cur.fetchone()
    check(row[0] == 360 and row[1] == 360, "points_m1 桶数/累计行数正确（360 桶 / 360 行）")
    cur.execute("SELECT count(*) FROM points_h1")
    check(cur.fetchone()[0] > 0, "points_h1 同步生成")
    cur.execute("SELECT count(*) FROM points_d1")
    check(cur.fetchone()[0] > 0, "points_d1 同步生成")

    # 全量精确值（Python 参考）
    all_vals = []
    for sid in (1, 2, 3):
        for i in range(120):
            all_vals.append((sid, base + i * 60000, (sid * 10 + i % 10) / 2.0))
    lo, hi = base, base + 119 * 60000

    # 3a) 范围聚合自动路由（结果必须与 raw 精确一致）
    exp_cnt = len(all_vals)
    exp_sum = sum(v for _, _, v in all_vals)
    exp_min = min(v for _, _, v in all_vals)
    exp_max = max(v for _, _, v in all_vals)
    cur.execute("SELECT count(*), sum(val), min(val), max(val), avg(val) FROM points "
                "WHERE ts >= %d AND ts <= %d" % (lo, hi))
    row = cur.fetchone()
    check(row[0] == exp_cnt and abs(row[1] - exp_sum) < 1e-6 and row[2] == exp_min and row[3] == exp_max,
          "范围聚合 rollup 路由结果精确（count/sum/min/max）")
    check(abs(row[4] - exp_sum / exp_cnt) < 1e-6, "avg 路由结果精确")

    # 3b) 非对齐边缘（前 90 秒）：边缘桶回退 raw，仍精确
    lo2, hi2 = base + 30000, base + 119 * 60000 + 30000  # 部分边缘桶 + 中间满桶（混合路由）
    exp = [v for _, t, v in all_vals if lo2 <= t <= hi2]
    cur.execute("SELECT count(*), sum(val) FROM points WHERE ts >= %d AND ts <= %d" % (lo2, hi2))
    row = cur.fetchone()
    check(row[0] == len(exp) and abs(row[1] - sum(exp)) < 1e-6,
          "非对齐范围（边缘桶回退 raw）结果精确（%d 行）" % row[0])

    # 3c) series 过滤 + GROUP BY time(N) 路由
    exp_groups = {}
    for sid, t, v in all_vals:
        key = (sid, t // 3600000 * 3600000)
        exp_groups.setdefault(key, []).append(v)
    cur.execute("SELECT series_id, time(3600000), avg(val), count(*) FROM points "
                "WHERE ts >= %d AND ts <= %d GROUP BY series_id, time(3600000)" % (lo, hi))
    got = {}
    for sid, bkt, avg_v, cnt in cur.fetchall():
        got[(sid, int(bkt))] = (avg_v, cnt)
    ok = len(got) == len(exp_groups)
    for key, vs in exp_groups.items():
        if key not in got or got[key][1] != len(vs) or abs(got[key][0] - sum(vs) / len(vs)) > 1e-6:
            ok = False
            break
    check(ok, "GROUP BY series_id, time(N) rollup 路由结果精确（%d 组）" % len(got))

    # 3d) REBUILD ROLLUP 存量回填（清空汇总表后重建）
    cur.execute("DELETE FROM points_m1")
    cur.execute("USE GlacierTDB")
    cur.execute("REBUILD ROLLUP rollupdb")
    cur.execute("USE rollupdb")
    cur.execute("SELECT count(*), sum(cnt) FROM points_m1")
    row = cur.fetchone()
    check(row[0] == 360 and row[1] == 360, "REBUILD ROLLUP 从 raw 回填成功")

    # 3e) 触发 REBUILD 权限门
    try:
        c3 = connect(user="sha2u", password="sha2pass2", db="backup")
        c3c = c3.cursor()
        # v1.3.11 起 USER 账号禁入管理面（USE GlacierTDB → 1044）；
        # REBUILD ROLLUP 在业务库直发同样被 super 门禁拒绝
        try:
            c3c.execute("USE GlacierTDB")
            check(False, "普通用户 USE GlacierTDB 应拒绝")
        except pymysql.MySQLError as e:
            check(e.args[0] == 1044, "普通用户禁入管理面（%s）" % e.args[0])
        c3c.execute("REBUILD ROLLUP rollupdb")
        check(False, "普通用户 REBUILD ROLLUP 应拒绝")
        c3.close()
    except pymysql.MySQLError as e:
        check(e.args[0] in (1142, 1290), "普通用户 REBUILD ROLLUP 拒绝（%s）" % e.args[0])

    # ---------- 4. GROUP BY 分片并行 ----------
    cur.execute("DROP TABLE IF EXISTS big_group")
    cur.execute("CREATE TABLE big_group (series_id INTEGER, ts INTEGER, val REAL)")
    rows = []
    for sid in range(20):
        for i in range(300):
            rows.append("(%d, %d, %s)" % (sid, 1000000 + i * 1000, (sid + i % 7) / 3.0))
    for chunk in range(0, len(rows), 500):
        cur.execute("INSERT INTO big_group VALUES " + ",".join(rows[chunk:chunk + 500]))

    q = ("SELECT series_id, ts/60000*60000 AS b, count(*), sum(val), min(val), max(val), avg(val) "
         "FROM big_group GROUP BY series_id, ts/60000*60000")
    cur.execute(q)
    serial = sorted(cur.fetchall(), key=lambda r: (r[0], int(r[1])))
    cur.execute(q.replace("SELECT ", "SELECT /*+ PARALLEL(2) */ ", 1))
    par = sorted(cur.fetchall(), key=lambda r: (r[0], int(r[1])))
    check(len(serial) == len(par) and len(serial) > 0, "GROUP BY 分片并行行数一致（%d 组）" % len(serial))
    same = True
    for a, b in zip(serial, par):
        if a[0] != b[0] or int(a[1]) != int(b[1]) or a[2] != b[2] or \
                abs(a[3] - b[3]) > 1e-6 or a[4] != b[4] or a[5] != b[5] or abs(a[6] - b[6]) > 1e-6:
            same = False
            break
    check(same, "GROUP BY 分片并行结果与串行一致")

    cur.execute("EXPLAIN PARALLEL " + q)
    row = cur.fetchone()
    check("group-shard" in str(row[4]), "EXPLAIN PARALLEL 显示 group-shard 计划")

    # ---------- 5. 明细导出并行（方案 B）----------
    cur.execute("DROP TABLE IF EXISTS big_detail")
    cur.execute("CREATE TABLE big_detail (id INTEGER PRIMARY KEY, v TEXT)")
    rows = ["(%d, 'row-%d')" % (i, i) for i in range(5000)]
    for chunk in range(0, len(rows), 500):
        cur.execute("INSERT INTO big_detail VALUES " + ",".join(rows[chunk:chunk + 500]))

    cur.execute("SELECT id, v FROM big_detail")
    serial = cur.fetchall()
    cur.execute("SELECT /*+ PARALLEL(4) */ id, v FROM big_detail")
    par = cur.fetchall()
    check(len(serial) == 5000 and serial == par, "明细并行（方案 B）与串行完全一致（5000 行）")

    cur.execute("SELECT /*+ PARALLEL(4) */ * FROM big_detail LIMIT 10")
    rows10 = cur.fetchall()
    check(len(rows10) == 10 and rows10[0][0] == 0 and rows10[9][0] == 9,
          "明细并行 * 列展开 + LIMIT 正确")

    cur.execute("SELECT /*+ PARALLEL(2) */ id, v FROM big_detail WHERE id >= 100 AND id < 200")
    sub = cur.fetchall()
    check(len(sub) == 100 and sub[0][0] == 100 and sub[-1][0] == 199, "明细并行 WHERE 过滤正确")

    cur.execute("EXPLAIN PARALLEL SELECT id, v FROM big_detail")
    row = cur.fetchone()
    check("detail shard scan" in str(row[4]), "EXPLAIN PARALLEL 显示 detail shard scan 计划")

    # ---------- 6. SCRIPT 临时屏蔽（v1.3.13）----------
    # SCRIPT 能力自 v1.3.13 起临时停用（安全边界整改，v1.3.14 恢复）：
    # 提交入口显式报错，不再建任务；原「执行完成 / 不存在脚本 ERROR」用例随语义退役
    cur.execute("USE GlacierTDB")  # 回管理面（前面用例切在 rollupdb；DROP 前必须切回，否则当前库被删后语句落入透传）
    for stmt in ("SUBMIT QUERY 'SELECT 1 AS a' SCRIPT 'pipe.js' OUTPUT done",
                 "SUBMIT QUERY 'SELECT 1 AS a' SCRIPT 'no_such_script.js'"):
        err_msg = ""
        try:
            cur.execute(stmt)
            err_msg = "(no error raised)"
        except Exception as e:
            err_msg = str(e)
        check("temporarily disabled" in err_msg,
              "SCRIPT 临时停用显式报错（%s → %s）" % (stmt[22:44], err_msg[:60]))

    cur.execute("DROP DATABASE rollupdb")
    cur.execute("DROP USER 'sha2u'")

    print("V111: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


def quote(v):
    return "'" + str(v).replace("'", "''") + "'"


if __name__ == "__main__":
    raise SystemExit(main())
