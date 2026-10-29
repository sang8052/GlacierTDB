#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GlacierTDB vs 原生中间件——统一 2H4G 压测客户端（v1.3.14 §5「性能损失与比较」）。

五组对位（与 docs/性能损失与比较.md 对齐）：
    mysql   GlacierTDB 3309 MySQL 协议 vs mysql:8 容器
    redis   GlacierTDB RESP vs redis:7 容器
    tsdb    GlacierTDB points 批写+范围聚合 vs influxdb:2 容器
    mq      GlacierTDB AMQP 持久化消息 vs rabbitmq:3.13 容器
    s3      GlacierTDB SigV4 PUT/GET vs minio 容器

用法（跑在压测机，各被测端点同网）：
    python bench_all.py mysql --host gt --port 3309 --user glacier --pass 'xxx'
    python bench_all.py redis --host gt --port 3309 [--auth 'xxx']
    python bench_all.py s3    --host gt --port 9000 --ak AK --sk SK   （或 --minio 对位）
    python bench_all.py mq    --host gt --port 3309 --user glacier --pass 'xxx'  [--rabbit]
    python bench_all.py tsdb  --mysql-host gt --tsdb-url http://influx:8086 [--token t --org o --bucket b]

输出：单行 JSON（capability, target, scenario, value, unit）——多轮取中位数由编排侧完成。
依赖：PyMySQL / redis / pika / influxdb-client（按组按需）。
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import hmac
import http.client
import io
import json
import socket
import sys
import time

import pymysql


def emit(cap, target, scenario, value, unit):
    print(json.dumps({"capability": cap, "target": target, "scenario": scenario,
                      "value": round(value, 2), "unit": unit}, ensure_ascii=False))


def median(vals):
    s = sorted(vals)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


# ---------------------------------------------------------------------------
# MySQL 对位：简单查询 QPS / 批量插入行每秒 / 并发连接
# ---------------------------------------------------------------------------

def bench_mysql(args):
    def conn():
        return pymysql.connect(host=args.host, port=args.port, user=args.user,
                               password=args.passwd, database=args.db, autocommit=True,
                               charset="utf8mb4", read_timeout=60, write_timeout=60)

    c = conn()
    with c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bench_q")
        cur.execute("CREATE TABLE bench_q (id INTEGER PRIMARY KEY, v TEXT)")
        cur.execute("INSERT INTO bench_q VALUES (1, 'x')")
    # ① 简单查询 QPS（单连接 10s）
    t0, n = time.time(), 0
    with c.cursor() as cur:
        while time.time() - t0 < args.duration:
            cur.execute("SELECT id, v FROM bench_q WHERE id = 1")
            cur.fetchall()
            n += 1
    emit("mysql", args.label, "simple_qps", n / (time.time() - t0), "qps")

    # ② 批量插入行/s（500 行/批 × 10s）
    with c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bench_ins")
        cur.execute("CREATE TABLE bench_ins (id INTEGER PRIMARY KEY, v REAL, t INTEGER)")
    t0, total = time.time(), 0
    batch = 500
    with c.cursor() as cur:
        while time.time() - t0 < args.duration:
            rows = ",".join("(%d, %s, %d)" % (total + i, (i % 100) / 7.0, i) for i in range(batch))
            cur.execute("INSERT INTO bench_ins VALUES " + rows)
            total += batch
    emit("mysql", args.label, "insert_rows", total / (time.time() - t0), "rows/s")

    # ③ 并发连接数（32 连接同时 SELECT 10s 全部存活 = 通过计数）
    def worker(_):
        try:
            cc = conn()
            with cc.cursor() as cur:
                t0 = time.time()
                while time.time() - t0 < args.duration:
                    cur.execute("SELECT 1")
                    cur.fetchall()
            cc.close()
            return 1
        except Exception:
            return 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as ex:
        alive = sum(ex.map(worker, range(32)))
    emit("mysql", args.label, "concurrent_conn_ok", alive, "conns")
    c.close()


# ---------------------------------------------------------------------------
# Redis 对位：GET/SET ops/s / List 队列吞吐
# ---------------------------------------------------------------------------

def resp_cmd(sock, f, *args, auth=None):
    if auth:
        payload = b"*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n" % (len(auth.encode()), auth.encode())
        sock.sendall(payload)
        f.readline()
    payload = b"*%d\r\n" % len(args) + b"".join(
        b"$%d\r\n%s\r\n" % (len(a.encode()), a.encode()) for a in args)
    sock.sendall(payload)
    line = f.readline().strip()
    return line


def bench_redis(args):
    def dial():
        s = socket.create_connection((args.host, args.port), timeout=30)
        f = s.makefile("rb")
        if args.auth:
            resp_cmd(s, f, "AUTH", args.auth)
        return s, f

    # ① SET/GET ops/s（单连接 10s）
    s, f = dial()
    t0, n = time.time(), 0
    while time.time() - t0 < args.duration:
        resp_cmd(s, f, "SET", "bench:k%d" % (n % 1000), "v%d" % n)
        resp_cmd(s, f, "GET", "bench:k%d" % (n % 1000))
        n += 2
    emit("redis", args.label, "getset_ops", n / (time.time() - t0), "ops/s")
    s.close()

    # ② List 队列吞吐（LPUSH+RPOP 往返 10s）
    s, f = dial()
    t0, n = time.time(), 0
    while time.time() - t0 < args.duration:
        resp_cmd(s, f, "LPUSH", "bench:q", "m%d" % n)
        resp_cmd(s, f, "RPOP", "bench:q")
        n += 2
    emit("redis", args.label, "queue_ops", n / (time.time() - t0), "ops/s")
    s.close()


# ---------------------------------------------------------------------------
# S3 对位：PUT/GET ops/s（小对象 8KB ×N）
# ---------------------------------------------------------------------------

class S3Client:
    def __init__(self, host, port, ak, sk, region="us-east-1", secure=False):
        self.host, self.port, self.ak, self.sk, self.region = host, port, ak, sk, region

    def req(self, method, path, body=b"", ctype="application/octet-stream"):
        # 签名头集合与 tests/test_s3.py 同构：host/x-amz-date/x-amz-content-sha256
        #（content-type 不参与签名，避免实现差异）；host 带端口原样传递。
        host = "%s:%d" % (self.host, self.port)
        t = datetime.datetime.now(datetime.timezone.utc)
        amzdate, datestamp = t.strftime("%Y%m%dT%H%M%SZ"), t.strftime("%Y%m%d")
        payload_h = hashlib.sha256(body).hexdigest()
        h = {"host": host, "x-amz-date": amzdate, "x-amz-content-sha256": payload_h}
        signed = sorted(h)
        canon_h = "".join("%s:%s\n" % (k, h[k]) for k in signed)
        canon = "\n".join([method, path, "", canon_h, ";".join(signed), payload_h])
        scope = "%s/%s/s3/aws4_request" % (datestamp, self.region)
        sts = "\n".join(["AWS4-HMAC-SHA256", amzdate, scope, hashlib.sha256(canon.encode()).hexdigest()])
        k = ("AWS4" + self.sk).encode()
        for step in (datestamp, self.region, "s3", "aws4_request"):
            k = hmac.new(k, step.encode(), hashlib.sha256).digest()
        sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        out = dict(h)
        out["Content-Type"] = ctype
        out["Authorization"] = ("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s"
                                % (self.ak, scope, ";".join(signed), sig))
        conn = http.client.HTTPConnection(self.host, self.port, timeout=60)
        conn.request(method, path, body=body, headers=out)
        r = conn.getresponse()
        r.read()
        st = r.status
        conn.close()
        return st


def bench_s3(args):
    c = S3Client(args.host, args.port, args.ak, args.sk)
    body = b"x" * 8192
    t0, n = time.time(), 0
    while time.time() - t0 < args.duration:
        st = c.req("PUT", "/bench-s3/o-%d" % (n % 500), body)
        if st != 200:
            print("put st=%d" % st, file=sys.stderr)
            return
        n += 1
    emit("s3", args.label, "put_ops", n / (time.time() - t0), "ops/s")
    t0, n = time.time(), 0
    while time.time() - t0 < args.duration:
        st = c.req("GET", "/bench-s3/o-%d" % (n % 500))
        if st != 200:
            print("get st=%d" % st, file=sys.stderr)
            return
        n += 1
    emit("s3", args.label, "get_ops", n / (time.time() - t0), "ops/s")


# ---------------------------------------------------------------------------
# MQ 对位：持久化消息 publish+consume+ack 吞吐
# ---------------------------------------------------------------------------

def bench_mq(args):
    import pika
    params = pika.ConnectionParameters(
        host=args.host, port=args.port, virtual_host="/",
        credentials=pika.PlainCredentials(args.user, args.passwd),
        heartbeat=0, blocked_connection_timeout=300)
    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue="bench_q", durable=True)
    body = b"m" * 256
    t0, n = time.time(), 0
    while time.time() - t0 < args.duration:
        ch.basic_publish("", "bench_q", body,
                         pika.BasicProperties(delivery_mode=2))
        n += 1
    emit("mq", args.label, "publish_durable", n / (time.time() - t0), "msg/s")

    got = [0]
    t0 = time.time()

    def cb(_ch, _method, _props, _body):
        _ch.basic_ack(delivery_tag=_method.delivery_tag)
        got[0] += 1
        if got[0] >= n or time.time() - t0 > args.duration * 3:
            _ch.stop_consuming()
    ch.basic_consume("bench_q", cb)
    ch.start_consuming()
    emit("mq", args.label, "consume_ack", got[0] / max(time.time() - t0, 1e-9), "msg/s")
    conn.close()


# ---------------------------------------------------------------------------
# TSDB 对位：批量写入行/s + 范围聚合查询
# ---------------------------------------------------------------------------

def bench_tsdb(args):
    # GlacierTDB 侧：points 批量 INSERT + 范围聚合
    c = pymysql.connect(host=args.mysql_host, port=args.mysql_port or 3309,
                        user=args.user, password=args.passwd,
                        database=args.db or "backup", autocommit=True, charset="utf8mb4")
    with c.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS bench_points")
        cur.execute("CREATE TABLE bench_points (series_id INTEGER NOT NULL, ts INTEGER NOT NULL, val REAL,"
                    " PRIMARY KEY (series_id, ts)) WITHOUT ROWID")
    batch, total, t0 = 500, 0, time.time()
    base = int(time.time() * 1000) // 60000 * 60000
    with c.cursor() as cur:
        while time.time() - t0 < args.duration:
            rows = []
            for i in range(batch):
                sid = total % 8 + 1
                rows.append("(%d, %d, %s)" % (sid, base + (total + i) * 60000 // 500, (i % 97) / 13.0))
            cur.execute("INSERT INTO bench_points VALUES " + ",".join(rows))
            total += batch
    emit("tsdb", args.label, "write_rows", total / (time.time() - t0), "rows/s")
    with c.cursor() as cur:
        t0 = time.time()
        for _ in range(20):
            cur.execute("SELECT series_id, count(*), avg(val) FROM bench_points WHERE ts >= %d AND ts <= %d GROUP BY series_id"
                        % (base, base + total * 120))
            cur.fetchall()
    emit("tsdb", args.label, "range_agg_qps", 20 / (time.time() - t0), "qps")
    c.close()

    # InfluxDB 对位（可选）：line protocol 批写 + Flux range 聚合
    if args.tsdb_url:
        try:
            from influxdb_client import InfluxDBClient, WritePrecision
            from influxdb_client.client.write_api import SYNCHRONOUS
            cli = InfluxDBClient(url=args.tsdb_url, token=args.token or "", org=args.org or "", timeout=60000)
            wapi = cli.write_api(write_options=SYNCHRONOUS)
            total, t0 = 0, time.time()
            lines = []
            while time.time() - t0 < args.duration:
                lines = ["bench,sid=%d v=%d" % (total % 8 + 1, total + i) for i in range(batch)]
                wapi.write(bucket=args.bucket or "bench", record="\n".join(lines), write_precision=WritePrecision.NS)
                total += batch
            emit("tsdb", "influxdb", "write_rows", total / (time.time() - t0), "rows/s")
            qapi = cli.query_api()
            q = ('from(bucket:"%s") |> range(start: 0) |> filter(fn: (r) => r._measurement == "bench") '
                 '|> group(columns: ["sid"]) |> count()' % (args.bucket or "bench"))
            t0 = time.time()
            for _ in range(5):
                qapi.query(query=q)
            emit("tsdb", "influxdb", "range_agg_qps", 5 / (time.time() - t0), "qps")
            cli.close()
        except Exception as e:
            print("influx skip: %s" % e, file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("group", choices=["mysql", "redis", "mq", "s3", "tsdb"])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0)
    ap.add_argument("--user", default="glacier")
    ap.add_argument("--passwd", default="")
    ap.add_argument("--auth", default="")
    ap.add_argument("--db", default="")
    ap.add_argument("--ak", default="")
    ap.add_argument("--sk", default="")
    ap.add_argument("--label", default="glaciertdb")
    ap.add_argument("--duration", type=int, default=10)
    ap.add_argument("--mysql-host", default="127.0.0.1")
    ap.add_argument("--mysql-port", type=int, default=0)
    ap.add_argument("--tsdb-url", default="")
    ap.add_argument("--token", default="")
    ap.add_argument("--org", default="")
    ap.add_argument("--bucket", default="")
    args = ap.parse_args()
    if not args.port:
        args.port = {"mysql": 3306, "redis": 6379, "mq": 5672, "s3": 9000, "tsdb": 3306}[args.group]
    {"mysql": bench_mysql, "redis": bench_redis, "mq": bench_mq,
     "s3": bench_s3, "tsdb": bench_tsdb}[args.group](args)


if __name__ == "__main__":
    main()
