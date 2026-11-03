#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.15 黑盒测试（安全加固 + 四线性能重构）：

1) SQL 错误 MySQL 标准化：1146/1054/1305/1062/1048/3819/1452/1050/1064 错误码与
   标准文案（不再裸透传 SQLite 原文）
2) 安全项：S4 SHOW S3 TOKENS 非 super 脱敏 · S1 scoped AK archive 前缀越权 403 ·
   S5 域名白名单 · S8 打包条目名清洗（zip-slip）
3) S3 内存缓存：纯内存对象（x-glacier-memory）生命周期 · 热点读缓存命中 · SHOW S3 CACHE
4) 打包下载 vhost 三形态修复（冗余桶段剥离 / path 作 prefix / 空 path）+ path-style 回归
5) KV 冒烟：SET/GET/INCR/LPUSH/RPOP + INFO persist_mode/core_mode（sync+shared 默认）
6) MQ 冒烟：durable publish + consume/ack
7) --async / --check-async：kv.persist_mode=async + mq.async_flush 场景（配合 run_all
   步骤 3.9 换配置重启：先写入、优雅停机、再验证重启不丢）
"""
import io
import json
import socket
import sys
import tarfile
import time

import pymysql

sys.path.insert(0, "/tests")
from test_s3 import s3_request, sql_connect, sign_request  # noqa: E402

import redis  # noqa: E402
import pika  # noqa: E402

HOST, PORT = "proxy", 3309
S3_HOST, S3_PORT = "proxy", 9000
ADMIN, ADMIN_PW = "glacier", "iloveyou~"
T = "t%d" % int(time.time())
ASYNC_MODE = "--async" in sys.argv
CHECK_ASYNC = "--check-async" in sys.argv
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def req(method, path, query="", body=b"", extra=None, ak=None, sk=None):
    """path-style S3 请求（glacier 管理密钥或指定 AK/SK）。"""
    return s3_request(method, path, query, extra or {}, body, ak or "", sk or "")


def vhost_req(method, bucket, path, query="", body=b"", extra=None, ak=None, sk=None):
    """virtual-host 请求：Host = <bucket>.glacier-s3.local。"""
    return s3_request(method, path, query, extra or {}, body, ak or "", sk or "",
                      vhost="%s.glacier-s3.local:%d" % (bucket, S3_PORT))


def err_code(cur_fn, sql):
    """执行 SQL 返回 (errno, message)。"""
    try:
        cur_fn(sql)
        return 0, ""
    except pymysql.MySQLError as e:
        return e.args[0], str(e.args[1])


def main() -> int:
    conn = sql_connect()
    cur = conn.cursor()
    # 双连接：backup 库跑普通 SQL（错误码断言/建表），GlacierTDB 跑管理面
    adm = sql_connect()
    acur = adm.cursor()
    acur.execute("USE GlacierTDB")

    # ==================================================================
    # --async 场景：写入数据（配置为 async 落盘），由 run_all 步骤 3.9 重启容器
    # ==================================================================
    if ASYNC_MODE:
        r = redis.Redis(host=HOST, port=PORT, password=ADMIN_PW, decode_responses=True)
        r.set("asyncstr:v1315", "persist-me")
        r.rpush("asyncq:v1315", "a", "b", "c")
        r.set("asyncexp:v1315", "gone-soon", px=60000)
        conn2 = pika.BlockingConnection(pika.ConnectionParameters(
            host=HOST, port=PORT, credentials=pika.PlainCredentials(ADMIN, ADMIN_PW)))
        ch = conn2.channel()
        ch.queue_declare(queue="v1315-async-durable", durable=True)
        ch.basic_publish(exchange="", routing_key="v1315-async-durable",
                         body=b"async-durable-payload",
                         properties=pika.BasicProperties(delivery_mode=2))
        conn2.close()
        print("async scenario: wrote 3 kv keys + 1 durable mq message")
        print("PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
        return 1 if STATS["fail"] else 0

    # --check-async 场景：重启后验证数据仍在
    if CHECK_ASYNC:
        r = redis.Redis(host=HOST, port=PORT, password=ADMIN_PW, decode_responses=True)
        check(r.get("asyncstr:v1315") == "persist-me", "async 重启后 kv 字符串恢复")
        check(r.lrange("asyncq:v1315", 0, -1) == ["a", "b", "c"], "async 重启后队列元素与顺序恢复")
        conn2 = pika.BlockingConnection(pika.ConnectionParameters(
            host=HOST, port=PORT, credentials=pika.PlainCredentials(ADMIN, ADMIN_PW)))
        ch = conn2.channel()
        m, mf, body = ch.basic_get("v1315-async-durable", auto_ack=False)
        check(m is not None and body == b"async-durable-payload", "async 重启后 durable 消息恢复")
        if m is not None:
            ch.basic_ack(m.delivery_tag)
        conn2.close()
        _summary()
        return 1 if STATS["fail"] else 0

    # ==================================================================
    # 1) SQL 错误 MySQL 标准化
    # ==================================================================
    cur.execute("DROP TABLE IF EXISTS e1315_t1")
    cur.execute("DROP TABLE IF EXISTS e1315_t2")
    cur.execute("CREATE TABLE e1315_t1 (id INT PRIMARY KEY, name VARCHAR(20) NOT NULL, "
                "age INT CHECK (age >= 0), UNIQUE KEY uk_name (name))")
    cur.execute("CREATE TABLE e1315_t2 (id INT PRIMARY KEY, "
                "t1id INT, FOREIGN KEY (t1id) REFERENCES e1315_t1(id))")

    code, msg = err_code(cur.execute, "SELECT * FROM no_such_table_v1315")
    check(code == 1146, "表不存在 1146（got %d %s）" % (code, msg[:60]))
    check("doesn't exist" in msg, "1146 文案 'doesn't exist'（%s）" % msg[:70])

    code, msg = err_code(cur.execute, "SELECT no_such_col_v1315 FROM e1315_t1")
    check(code == 1054, "列不存在 1054（got %d）" % code)

    code, msg = err_code(cur.execute, "SELECT no_such_fn_v1315(1)")
    check(code == 1305, "函数不存在 1305（got %d）" % code)

    cur.execute("INSERT INTO e1315_t1 VALUES (1, 'a', 10)")
    code, msg = err_code(cur.execute, "INSERT INTO e1315_t1 VALUES (2, 'a', 10)")
    check(code == 1062, "唯一冲突 1062（got %d %s）" % (code, msg[:60]))
    check("e1315_t1.name" in msg, "1062 带键名（SQLite 报列名）（%s）" % msg[:70])

    code, msg = err_code(cur.execute, "INSERT INTO e1315_t1 VALUES (3, NULL, 10)")
    check(code == 1048, "NOT NULL 1048（got %d）" % code)

    code, msg = err_code(cur.execute, "INSERT INTO e1315_t1 VALUES (4, 'neg', -1)")
    check(code == 3819, "CHECK 违例 3819（got %d）" % code)

    code, msg = err_code(cur.execute, "INSERT INTO e1315_t2 VALUES (1, 999)")
    check(code == 1452, "外键违例 1452（got %d）" % code)

    code, msg = err_code(cur.execute, "CREATE TABLE e1315_t1 (id INT)")
    check(code == 1050, "表已存在 1050（got %d）" % code)

    code, msg = err_code(cur.execute, "SELEC 1")
    check(code == 1064, "语法错 1064（got %d）" % code)
    check("You have an error in your SQL syntax" in msg,
          "1064 标准 MySQL 文案（%s）" % msg[:80])
    check("sqlite" not in msg.lower(), "1064 不再透出 SQLite 字样（%s）" % msg[:80])

    # ==================================================================
    # 2) 安全项
    # ==================================================================
    # S4：非 super 账号 SHOW S3 TOKENS 脱敏
    tkb = T + "-tok"
    if _bucket_exists(acur, tkb):
        acur.execute("DROP BUCKET %s" % tkb)
    acur.execute("CREATE BUCKET %s" % tkb)
    acur.execute("CREATE S3 TOKEN ON %s/doc.txt GET EXPIRE 300" % tkb)
    tok_full = acur.fetchall()[0][0]
    check(len(tok_full) > 8, "super 查看 token 完整（%s...）" % tok_full[:8])

    try:
        acur.execute("DROP USER 'uro140'")
    except pymysql.MySQLError:
        pass
    acur.execute("CREATE USER 'uro140'@'%' IDENTIFIED BY 'uro-pass-1'")
    acur.execute("ALTER USER 'uro140' READONLY")

    ro_conn = pymysql.connect(host=HOST, port=PORT, user="uro140",
                              password="uro-pass-1", database="backup")
    ro_cur = ro_conn.cursor()
    ro_cur.execute("USE GlacierTDB")
    ro_cur.execute("SHOW S3 TOKENS")
    masked = [r[0] for r in ro_cur.fetchall()]
    check(any(m.endswith("****") and not m == tok_full for m in masked),
          "S4 readonly 查看 token 脱敏（%s）" % masked[:2])
    check(tok_full not in masked, "S4 完整 token 不出现")
    ro_conn.close()

    # S1：scoped AK 用 dummy prefix 绕过 archive 前缀 → 403
    sak = T + "s1"
    acur.execute("CREATE S3 KEY FOR 'glacier' BUCKETS ('%s') PREFIXES ('ok/')" % tkb)
    s1ak, s1sk = acur.fetchall()[0][0:2]
    st, _, _ = req("PUT", "/%s/ok/inside.txt" % tkb, body=b"in", ak=s1ak, sk=s1sk)
    check(st == 200, "scoped AK 前缀内 PUT（%d）" % st)
    st, _, body = req("GET", "/%s" % tkb, query="x-glacier-archive=outside/&prefix=ok/",
                      ak=s1ak, sk=s1sk)
    check(st == 403, "S1 dummy-prefix 借 archive 越界 403（%d %s）" % (st, body[:60]))
    st, _, body = req("GET", "/%s" % tkb, query="x-glacier-archive=ok/&prefix=ok/",
                      ak=s1ak, sk=s1sk)
    check(st == 200, "S1 archive 前缀在 scope 内放行（%d）" % st)

    # S5：域名白名单
    code, msg = err_code(acur.execute, "ALTER BUCKET %s ADD DOMAIN '../evil'" % tkb)
    check(code != 0, "S5 域名路径穿越拒绝（%d %s）" % (code, msg[:50]))
    code, msg = err_code(acur.execute, "ALTER BUCKET %s ADD DOMAIN 'bad host!'" % tkb)
    check(code != 0, "S5 非法字符域名拒绝（%d）" % code)
    code, msg = err_code(acur.execute, "ALTER BUCKET %s ADD DOMAIN 'v1315.example.com' ISSUE" % tkb)
    check(code == 0, "S5 合法域名放行（%d %s）" % (code, msg[:60]))

    # ==================================================================
    # 3) S3 内存缓存
    # ==================================================================
    mb = T + "-mem"
    acur.execute("CREATE BUCKET %s" % mb)
    acur.execute("CREATE S3 KEY FOR 'glacier'")
    AK, SK = acur.fetchall()[0][0:2]
    # 纯内存对象：PUT 带 x-glacier-memory: true
    st, hd, _ = req("PUT", "/%s/ephemeral.bin" % mb, body=b"\x01\x02mem-only",
                    extra={"x-glacier-memory": "true", "Content-Type": "application/octet-stream"},
                    ak=AK, sk=SK)
    check(st == 200, "纯内存对象 PUT 200（%d）" % st)
    st, hd, body = req("GET", "/%s/ephemeral.bin" % mb, ak=AK, sk=SK)
    check(st == 200 and body == b"\x01\x02mem-only", "纯内存对象 GET 内容一致（%d）" % st)
    st, hd, _ = req("HEAD", "/%s/ephemeral.bin" % mb, ak=AK, sk=SK)
    check(st == 200, "纯内存对象 HEAD 200（%d）" % st)
    st, _, body = req("GET", "/%s" % mb, ak=AK, sk=SK)
    check(("ephemeral.bin" in body.decode("utf-8", "ignore")),
          "纯内存对象出现在 LIST（%d）" % st)
    st, hd, body = req("DELETE", "/%s/ephemeral.bin" % mb, ak=AK, sk=SK)
    check(st == 204, "纯内存对象 DELETE 204（%d）" % st)
    st, _, _ = req("GET", "/%s/ephemeral.bin" % mb, ak=AK, sk=SK)
    check(st == 404, "删除后 GET 404（%d）" % st)

    # 热点读缓存命中：同对象 GET 两次后 SHOW S3 CACHE hits 增加
    req("PUT", "/%s/hot.bin" % mb, body=b"hot-content-0123456789", ak=AK, sk=SK)
    req("GET", "/%s/hot.bin" % mb, ak=AK, sk=SK)
    acur.execute("SHOW S3 CACHE")
    stats1 = acur.fetchall()[0][0]
    req("GET", "/%s/hot.bin" % mb, ak=AK, sk=SK)
    acur.execute("SHOW S3 CACHE")
    stats2 = acur.fetchall()[0][0]
    hits1, hits2 = _stat_val(stats1, "mem_cache_hits"), _stat_val(stats2, "mem_cache_hits")
    check(hits2 > hits1, "热点对象二次 GET 命中内存缓存（mem_cache_hits %s→%s）" % (hits1, hits2))

    # ==================================================================
    # 4) 打包下载 vhost 三形态 + path-style 回归 + S8 条目清洗
    # ==================================================================
    ab = T + "-arc"
    acur.execute("CREATE BUCKET %s" % ab)
    for k, v in (("p1/a.txt", b"A"), ("p1/sub/b.txt", b"B"), ("p2/c.txt", b"C"),
                 ("../evil.txt", b"E"), ("x/../y.txt", b"Y")):
        req("PUT", "/%s/%s" % (ab, k), body=v, ak=AK, sk=SK)

    q = "x-glacier-archive=p1/&format=tar&mode=stream"
    st, hd, body = vhost_req("GET", ab, "/", query=q, ak=AK, sk=SK)
    check(st == 200, "vhost 空路径打包 200（%d）" % st)
    names = _tar_names(body)
    check("p1/a.txt" in names and "p1/sub/b.txt" in names, "vhost 空路径条目正确（%s）" % names)

    st, hd, body = vhost_req("GET", ab, "/%s" % ab, query=q, ak=AK, sk=SK)
    check(st == 200 and "p1/a.txt" in _tar_names(body),
          "vhost 冗余桶段剥离修复（%d）" % st)

    st, hd, body = vhost_req("GET", ab, "/p1", query="x-glacier-archive=&format=tar&mode=stream", ak=AK, sk=SK)
    check(st == 200 and "p1/a.txt" in _tar_names(body),
          "vhost path 作默认 prefix（%d %s）" % (st, _tar_names(body)))

    st, hd, body = req("GET", "/%s" % ab, query=q, ak=AK, sk=SK)
    check(st == 200 and "p1/a.txt" in _tar_names(body), "path-style 回归（%d）" % st)

    # S8：zip-slip 条目清洗
    st, hd, body = req("GET", "/%s" % ab, query="x-glacier-archive=&format=zip&mode=done", ak=AK, sk=SK)
    check(st == 200, "zip 打包 200（%d）" % st)
    import zipfile
    zf = zipfile.ZipFile(io.BytesIO(body))
    bad = [n for n in zf.namelist() if n.startswith("/") or ".." in n.split("/")]
    check(not bad, "S8 条目名无 ../ 与前导 /（%s）" % zf.namelist())

    # ==================================================================
    # 5) KV 冒烟 + 运行模式观测
    # ==================================================================
    r = redis.Redis(host=HOST, port=PORT, password=ADMIN_PW, decode_responses=True)
    r.set("v1315:smoke", "ok")
    check(r.get("v1315:smoke") == "ok", "KV SET/GET 往返")
    check(r.incr("v1315:cnt") >= 1, "KV INCR")
    r.rpush("v1315:list", "x", "y")
    check(r.rpop("v1315:list") == "y", "KV LPUSH/RPOP")
    info = r.info()
    check(info.get("persist_mode", "sync") == "sync", "INFO persist_mode=sync（默认）")
    check("core_mode" in info, "INFO core_mode 存在（%s）" % info.get("core_mode"))

    # ==================================================================
    # 6) MQ 冒烟
    # ==================================================================
    conn2 = pika.BlockingConnection(pika.ConnectionParameters(
        host=HOST, port=PORT, credentials=pika.PlainCredentials(ADMIN, ADMIN_PW)))
    ch = conn2.channel()
    ch.queue_declare(queue="v1315-smoke", durable=True)
    ch.basic_publish(exchange="", routing_key="v1315-smoke", body=b"hello-1315",
                     properties=pika.BasicProperties(delivery_mode=2))
    m, mf, body = ch.basic_get("v1315-smoke", auto_ack=False)
    check(m is not None and body == b"hello-1315", "MQ durable publish/consume")
    if m is not None:
        ch.basic_ack(m.delivery_tag)
    conn2.close()

    _summary()
    return 1 if STATS["fail"] else 0


def _bucket_exists(cur, name):
    cur.execute("SHOW BUCKETS")  # 管理面语句：调用方传 acur
    return any(r[0] == name for r in cur.fetchall())


def _stat_val(stats_text, key):
    for line in (stats_text or "").splitlines():
        parts = line.split(":", 1)
        if len(parts) == 2 and parts[0].strip() == key:
            try:
                return int(parts[1].strip())
            except ValueError:
                return parts[1].strip()
    return -1


def _tar_names(body):
    try:
        tf = tarfile.open(fileobj=io.BytesIO(body), mode="r:")
        return [m for m in tf.getnames()]
    except Exception as e:
        return ["<parse-error: %s>" % e]


def _summary():
    print("PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"]:
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
