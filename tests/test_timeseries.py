#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GlacierTDB 时序能力黑盒测试：schema、时间谓词整数化、time(N) 桶化、索引命中。"""
import sys
import time as _time

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


def main() -> int:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASS,
                           database=DB, charset="utf8mb4", autocommit=True)
    cur = conn.cursor()

    # ---------- 1. 时序 schema（series 维表 + points 聚簇事实表） ----------
    cur.execute("DROP TABLE IF EXISTS points")
    cur.execute("DROP TABLE IF EXISTS series")
    cur.execute("""CREATE TABLE series (
        id INTEGER PRIMARY KEY, meas TEXT NOT NULL, tags TEXT NOT NULL,
        UNIQUE(meas, tags))""")
    cur.execute("""CREATE TABLE points (
        series_id INTEGER NOT NULL, ts INTEGER NOT NULL, val REAL,
        PRIMARY KEY (series_id, ts)) WITHOUT ROWID""")
    cur.execute("CREATE INDEX idx_series_host ON series(json_extract(tags,'$.host'))")
    for i, host in enumerate(["a1", "a2", "a3"]):
        cur.execute("INSERT INTO series(meas, tags) VALUES('cpu.load', '%s')"
                    % ('{"host":"%s","region":"bj"}' % host,))
    check(True, "时序 schema 建表成功")

    # ---------- 2. 灌数据：过去 48h，每序列每分钟 1 点（2880×3=8640 点） ----------
    now_ms = int(_time.time() * 1000)
    minute = 60 * 1000
    start = now_ms - 48 * 3600 * 1000
    # 对齐分钟，保证桶化整齐
    start = (start // minute + 1) * minute
    cur.execute("DROP TABLE IF EXISTS _tmp_pts")
    cur.execute("""CREATE TABLE _tmp_pts AS
        WITH RECURSIVE c(x) AS (SELECT 0 UNION ALL SELECT x+1 FROM c WHERE x < 2879)
        SELECT x FROM c""")
    for sid in (1, 2, 3):
        cur.execute(
            "INSERT INTO points(series_id, ts, val) SELECT %d, %d + x*%d, 50.0 + x*0.01 FROM _tmp_pts"
            % (sid, start, minute))
    cur.execute("SELECT COUNT(*) FROM points")
    check(cur.fetchone()[0] == 3 * 2880, "灌入 8640 点（3 序列 × 2880 分钟）")

    # ---------- 3. NOW() - INTERVAL 时间谓词整数化 ----------
    cur.execute("SELECT COUNT(*) FROM points WHERE series_id=1 AND ts >= NOW() - INTERVAL 1 HOUR")
    n_interval = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM points WHERE series_id=1 AND ts >= %d"
                % (now_ms - 3600 * 1000,))
    n_numeric = cur.fetchone()[0]
    check(abs(n_interval - 60) <= 2,
          "NOW() - INTERVAL 1 HOUR 命中约 60 点（实际 %d）" % n_interval)
    check(n_interval == n_numeric,
          "INTERVAL 改写与数字比较等价（%d == %d）" % (n_interval, n_numeric))

    # ---------- 4. 日期字符串谓词自动整数化 ----------
    mid_ms = start + 24 * 3600 * 1000  # 24h 前
    mid_str = time_ms_to_str(mid_ms)
    cur.execute("SELECT COUNT(*) FROM points WHERE ts >= '%s'" % mid_str)
    n_str = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM points WHERE ts >= %d" % mid_ms)
    n_num = cur.fetchone()[0]
    check(n_str == n_num == 3 * 1440,
          "日期字符串谓词整数化正确（'%s' → %d，%d == %d）" % (mid_str, mid_ms, n_str, n_num))

    # ---------- 5. GROUP BY time(N) 桶化（InfluxDB 风格） ----------
    cur.execute("""SELECT time(3600000), COUNT(*), MIN(val), MAX(val), AVG(val)
        FROM points WHERE series_id=1
          AND ts >= NOW() - INTERVAL 6 HOUR AND ts < NOW() - INTERVAL 1 HOUR
        GROUP BY time(3600000) ORDER BY 1""")
    rows = cur.fetchall()
    # 5 小时窗口（起止非整点）→ 跨 6 个整点桶；NOW() 秒级抖动可能吞掉首/尾部分桶
    #（数据按分钟对齐生成，窗口起点恰逢整点分界时为 5 个满桶）——桶数放宽为 5..6，
    # 桶对齐 / 满桶 60 点 / 窗口合计 300 点为不变量（下方两条严格校验）
    check(5 <= len(rows) <= 6, "time(3600000) 桶数正确（5 小时窗口 → 5-6 个整点桶，实际 %d）" % len(rows))
    ok_bucket = all(int(r[0]) % 3600000 == 0 for r in rows)
    ok_count = max((r[1] for r in rows), default=0) == 60 and sum(r[1] for r in rows) == 300
    check(ok_bucket, "桶起始值对齐小时整点（% 3600000 == 0）")
    check(ok_count, "满桶 60 点、窗口合计 300 点（实际合计 %d）" % sum(r[1] for r in rows))

    # ---------- 6. FROM_UNIXTIME 改写 ----------
    cur.execute("SELECT FROM_UNIXTIME(ts/1000) FROM points WHERE series_id=1 ORDER BY ts LIMIT 1")
    v = cur.fetchone()[0]
    check(isinstance(v, str) and len(v) >= 19 and v[4] == "-" and v[10] == " ",
          "FROM_UNIXTIME 返回可读时间（%r）" % v)

    # ---------- 7. 索引命中：EXPLAIN QUERY PLAN 无全表扫 ----------
    cur.execute("""EXPLAIN QUERY PLAN SELECT series_id, COUNT(*), AVG(val)
        FROM points WHERE series_id=1 AND ts >= %d AND ts < %d GROUP BY series_id"""
                % (start, start + 3600 * 1000))
    plans = [r[3] for r in cur.fetchall()]
    check(any("SEARCH" in p and "PRIMARY KEY" in p for p in plans) and not any("SCAN points" in p for p in plans),
          "范围查询命中 (series_id, ts) 聚簇主键（%s）" % " | ".join(plans))
    # (series_id, ts) 聚簇主键下纯 ts 谓词必然 SCAN；建 ts 二级索引后命中
    cur.execute("CREATE INDEX IF NOT EXISTS idx_points_ts ON points(ts)")
    cur.execute("EXPLAIN QUERY PLAN SELECT COUNT(*) FROM points WHERE ts >= %d AND ts < %d"
                % (start, start + 60000))
    plans2 = [r[3] for r in cur.fetchall()]
    check(any("SEARCH points USING" in p for p in plans2) and not any("SCAN points" in p for p in plans2),
          "建 ts 二级索引后纯时间谓词命中索引（%s）" % " | ".join(plans2))

    # ---------- 收尾 ----------
    cur.execute("DROP TABLE IF EXISTS _tmp_pts")
    cur.close()
    conn.close()

    print()
    print("-" * 44)
    print("TIMESERIES 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("TIMESERIES: ALL PASS")
        return 0
    return 1


def time_ms_to_str(ms):
    t = _time.gmtime(ms // 1000)  # 网关按 UTC 解析日期字符串
    return "%04d-%02d-%02d %02d:%02d:%02d" % t[:6]


if __name__ == "__main__":
    sys.exit(main())
