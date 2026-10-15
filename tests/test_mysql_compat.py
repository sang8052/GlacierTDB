#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MySQL 兼容增强 M1 黑盒测试：LIMIT 双参 / CONCAT / IF / RAND / UUID / DDL 类型映射 / DEFAULT CURRENT_TIMESTAMP。"""
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


def main() -> int:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD,
                           database=DATABASE, charset="utf8mb4", autocommit=True,
                           read_timeout=20, write_timeout=20)
    cur = conn.cursor()

    # ---------- 1. LIMIT offset, count ----------
    cur.execute("DROP TABLE IF EXISTS m1_t")
    cur.execute("CREATE TABLE m1_t(id INTEGER PRIMARY KEY, v TEXT)")
    for i in range(1, 11):
        cur.execute("INSERT INTO m1_t(v) VALUES ('r%d')" % i)
    cur.execute("SELECT id FROM m1_t ORDER BY id LIMIT 2, 3")     # MySQL 双参
    a = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM m1_t ORDER BY id LIMIT 3 OFFSET 2")  # 标准写法
    b = [r[0] for r in cur.fetchall()]
    check(a == b == [3, 4, 5], "LIMIT 2,3 == LIMIT 3 OFFSET 2 == [3,4,5]（实际 %s）" % a)

    # ---------- 2. CONCAT（含嵌套函数与 NULL 传播）----------
    cur.execute("SELECT CONCAT('a', 'b-', UPPER('c'))")
    check(cur.fetchone()[0] == "ab-C", "CONCAT('a','b-',UPPER('c')) = 'ab-C'")
    cur.execute("SELECT CONCAT('x', NULL, 'y')")
    check(cur.fetchone()[0] is None, "CONCAT NULL 传播语义一致（NULL）")
    cur.execute("SELECT CONCAT('n=', (SELECT COUNT(*) FROM m1_t WHERE id <= 3))")
    check(cur.fetchone()[0] == "n=3", "CONCAT 含子查询参数")

    # ---------- 3. IF(c, a, b) ----------
    cur.execute("SELECT IF(1=1, 'yes', 'no'), IF(1=2, 'yes', 'no')")
    r = cur.fetchone()
    check(r[0] == "yes" and r[1] == "no", "IF 三参条件表达式（%s/%s）" % (r[0], r[1]))
    cur.execute("SELECT IFNULL(NULL, 'dflt')")
    check(cur.fetchone()[0] == "dflt", "IFNULL 原生不受影响")

    # ---------- 4. RAND() ----------
    cur.execute("SELECT RAND(), RAND()")
    r = cur.fetchone()
    check(0 <= r[0] < 1 and 0 <= r[1] < 1 and r[0] != r[1], "RAND() ∈ [0,1) 且两次不同")

    # ---------- 5. UUID() ----------
    cur.execute("SELECT UUID()")
    u1 = cur.fetchone()[0]
    cur.execute("SELECT UUID()")
    u2 = cur.fetchone()[0]
    check(len(u1) == 36 and u1[14] == "4" and u1.count("-") == 4,
          "UUID() v4 样式 36 字符（%s）" % u1)
    check(u1 != u2, "UUID() 两次不同")

    # ---------- 6. DDL 类型映射（整段 MySQL DDL 直接可建）----------
    cur.execute("DROP TABLE IF EXISTS m1_ddl")
    cur.execute("""CREATE TABLE m1_ddl (
  `id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(64) NOT NULL,
  `flag` TINYINT DEFAULT 0,
  `score` DOUBLE,
  `amount` DECIMAL(10,2),
  `created` DATETIME DEFAULT CURRENT_TIMESTAMP,
  `data` VARBINARY(128),
  PRIMARY KEY (`id`),
  KEY `idx_name` (`name`),
  UNIQUE KEY `uk_score` (`score`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='mysql ddl paste'""")
    check(True, "MySQL 原生 DDL（UNSIGNED/AUTO_INCREMENT/KEY/ENGINE/COMMENT）建表成功")
    cur.execute("SELECT name, type FROM pragma_table_info('m1_ddl')")
    types = {r[0]: r[1] for r in cur.fetchall()}
    check(types.get("id") == "INTEGER" and types.get("name") == "TEXT"
          and types.get("flag") == "INTEGER" and types.get("score") == "REAL"
          and types.get("amount") == "TEXT" and types.get("created") == "TEXT"
          and types.get("data") == "BLOB",
          "类型映射 INT→INTEGER/VARCHAR→TEXT/TINYINT→INTEGER/DOUBLE→REAL/DECIMAL→TEXT/DATETIME→TEXT/VARBINARY→BLOB（%s）" % types)
    cur.execute("INSERT INTO m1_ddl (name, flag, score) VALUES ('行A', 1, 9.5)")
    check(cur.lastrowid >= 1, "AUTO_INCREMENT 列自增回填 lastrowid=%s" % cur.lastrowid)
    cur.execute("SELECT created FROM m1_ddl WHERE name='行A'")
    created = cur.fetchone()[0]
    check(created is not None and len(str(created)) >= 19, "DEFAULT CURRENT_TIMESTAMP 生效（%s）" % created)

    # ---------- 7. 兼容不越界：原生 SQLite 写法不受影响 ----------
    cur.execute("SELECT COUNT(*) FROM m1_t WHERE v LIKE 'r%' LIMIT 5")
    check(cur.fetchone()[0] == 10, "单参 LIMIT / LIKE 原样透传不受影响")

    cur.execute("DROP TABLE IF EXISTS m1_t")
    cur.execute("DROP TABLE IF EXISTS m1_ddl")
    cur.close()
    conn.close()

    print()
    print("-" * 44)
    print("MYSQL_COMPAT 汇总: PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"] == 0:
        print("MYSQL_COMPAT: ALL PASS")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
