#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.0 黑盒测试（RabbitMQ 能力 · AMQP 0-9-1 基本队列）：
1) 端口复用：AMQP 魔数嗅探（pika 直连）/ mode:rabbitmq 声明行
2) 声明绑定：exchange（direct/fanout/topic）+ queue + binding
3) 生产消费：publish / get / consume / ack / nack(重投) / reject / prefetch
4) 路由语义：direct 精确 / fanout 广播 / topic 通配（* #）
5) 管理命令：SHOW QUEUES / SHOW EXCHANGES / mq_queues / mq_exchanges
6) 持久化：durable 队列 + persistent 消息，重启后不丢（--check-restart）
7) 认证：错误密码拒绝；异步任务 amqp:// 回调自服务回环
"""
import socket
import sys
import time

import pika

HOST, PORT = "proxy", 3309
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def pika_connect(user="glacier", password="iloveyou~"):
    params = pika.ConnectionParameters(
        host=HOST, port=PORT, heartbeat=0, blocked_connection_timeout=30,
        credentials=pika.PlainCredentials(user, password),
        socket_timeout=30)
    return pika.BlockingConnection(params)


def safe_close(conn):
    """pika 1.4.4 关闭握手有已知怪癖（CloseOk 分发抛 KeyError），关闭失败不影响会话内验证。"""
    try:
        conn.close()
    except Exception:
        pass


def mysql_connect():
    import pymysql
    return pymysql.connect(host=HOST, port=3309, user="glacier", password="iloveyou~",
                           database="backup", charset="utf8mb4", autocommit=True,
                           read_timeout=60, write_timeout=60)


def phase_restart_check():
    """--check-restart：重启后 durable 队列消息无损。"""
    conn = pika_connect()
    ch = conn.channel()
    ch.queue_declare(queue="q_persist", durable=True)
    got = []
    for _ in range(2):
        m = ch.basic_get(queue="q_persist", auto_ack=True)
        if m[0] is not None:
            got.append(m[2])
    check(sorted(got) == [b"m1", b"m2"], "重启后 durable 队列消息恢复（%r）" % got)
    safe_close(conn)
    print("V130-RESTART: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


def phase_main():
    # ---------- 1. 端口复用 ----------
    # mode:rabbitmq 声明行（原生 socket）
    s = socket.create_connection((HOST, PORT), timeout=20)
    s.sendall(b"mode:rabbitmq\r\n")
    s.sendall(b"AMQP\x00\x00\x09\x01")
    first = s.recv(8)
    check(first[0] == 1, "mode:rabbitmq 声明 + AMQP 连接（收到 Connection.Start 帧）")
    s.close()

    # pika 直连（AMQP 魔数嗅探路径）
    conn = pika_connect()
    check(conn.is_open, "pika 连接（AMQP 嗅探 + PLAIN 认证）")
    ch = conn.channel()
    check(True, "Channel.Open")

    # 错误密码
    try:
        pika_connect(password="wrongpass")
        check(False, "错误密码应拒绝")
    except Exception:
        check(True, "错误密码拒绝")

    # ---------- 2. 声明绑定 ----------
    ch.exchange_declare(exchange="ex_direct", exchange_type="direct", durable=True)
    ch.exchange_declare(exchange="ex_fanout", exchange_type="fanout", durable=True)
    ch.exchange_declare(exchange="ex_topic", exchange_type="topic", durable=True)
    check(True, "exchange 声明（direct/fanout/topic）")

    for q in ("q_a", "q_b", "q_c"):
        ch.queue_declare(queue=q, durable=True)
    check(True, "queue 声明（durable）")

    ch.queue_bind(queue="q_a", exchange="ex_direct", routing_key="rk1")
    ch.queue_bind(queue="q_b", exchange="ex_direct", routing_key="rk2")
    ch.queue_bind(queue="q_a", exchange="ex_fanout", routing_key="")
    ch.queue_bind(queue="q_b", exchange="ex_fanout", routing_key="")
    ch.queue_bind(queue="q_a", exchange="ex_topic", routing_key="a.*")
    ch.queue_bind(queue="q_b", exchange="ex_topic", routing_key="#")
    ch.queue_bind(queue="q_c", exchange="ex_topic", routing_key="a.b.c")
    check(True, "queue 绑定")

    # ---------- 3. 路由语义 ----------
    ch.basic_publish(exchange="ex_direct", routing_key="rk1", body=b"d1")
    ch.basic_publish(exchange="ex_direct", routing_key="rk2", body=b"d2")
    ch.basic_publish(exchange="ex_direct", routing_key="nope", body=b"drop")
    time.sleep(0.3)
    got_a = ch.basic_get(queue="q_a", auto_ack=True)
    got_b = ch.basic_get(queue="q_b", auto_ack=True)
    check(got_a[0] is not None and got_a[2] == b"d1", "direct 精确路由（rk1→q_a）")
    check(got_b[0] is not None and got_b[2] == b"d2", "direct 精确路由（rk2→q_b）")
    empty = ch.basic_get(queue="q_c", auto_ack=True)
    check(empty[0] is None, "direct 未绑定 routing key 不投递")

    ch.basic_publish(exchange="ex_fanout", routing_key="", body=b"fan1")
    time.sleep(0.3)
    check(ch.basic_get(queue="q_a", auto_ack=True)[2] == b"fan1", "fanout 广播 q_a")
    check(ch.basic_get(queue="q_b", auto_ack=True)[2] == b"fan1", "fanout 广播 q_b")

    ch.basic_publish(exchange="ex_topic", routing_key="a.b", body=b"t-ab")
    ch.basic_publish(exchange="ex_topic", routing_key="a.b.c", body=b"t-abc")
    ch.basic_publish(exchange="ex_topic", routing_key="x.y", body=b"t-xy")
    time.sleep(0.3)
    a_msgs = set()
    while True:
        m = ch.basic_get(queue="q_a", auto_ack=True)
        if m[0] is None:
            break
        a_msgs.add(m[2])
    check(a_msgs == {b"t-ab"}, "topic a.* 只匹配单段（%r）" % a_msgs)
    b_msgs = set()
    while True:
        m = ch.basic_get(queue="q_b", auto_ack=True)
        if m[0] is None:
            break
        b_msgs.add(m[2])
    check(b_msgs == {b"t-ab", b"t-abc", b"t-xy"}, "topic # 全匹配（%r）" % b_msgs)
    c_msg = ch.basic_get(queue="q_c", auto_ack=True)
    check(c_msg[0] is not None and c_msg[2] == b"t-abc", "topic 精确 a.b.c")

    # ---------- 4. consume / ack / prefetch ----------
    ch.basic_publish(exchange="", routing_key="q_a", body=b"c1")  # 默认交换机
    ch.basic_publish(exchange="", routing_key="q_a", body=b"c2")
    ch.basic_publish(exchange="", routing_key="q_a", body=b"c3")
    ch.basic_qos(prefetch_count=1)
    received = []
    for method, props, body in ch.consume(queue="q_a", auto_ack=False, inactivity_timeout=2):
        if method is None:
            break
        received.append(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if len(received) >= 3:
            break
    ch.cancel()
    check(received == [b"c1", b"c2", b"c3"], "consume + ack（顺序 %r）" % received)

    # prefetch：3 条消息 prefetch=1，未 ack 不再投递
    for i in range(3):
        ch.basic_publish(exchange="", routing_key="q_a", body=b"p%d" % i)
    ch.basic_qos(prefetch_count=1)
    it = ch.consume(queue="q_a", auto_ack=False, inactivity_timeout=1)
    first = next(it)
    second = next(it, None)
    check(first[0] is not None and (second is None or second[0] is None),
          "prefetch=1 未 ack 不继续投递")
    ch.basic_ack(delivery_tag=first[0].delivery_tag)
    try:
        it.close()
    except Exception:
        pass
    ch.cancel()
    # 清理剩余
    while True:
        m = ch.basic_get(queue="q_a", auto_ack=True)
        if m[0] is None:
            break

    # nack 重投 + redelivered 标志
    ch.basic_publish(exchange="", routing_key="q_a", body=b"n1")
    time.sleep(0.2)
    m = ch.basic_get(queue="q_a", auto_ack=False)
    check(m[0] is not None and m[0].redelivered is False, "首次投递 redelivered=False")
    ch.basic_nack(delivery_tag=m[0].delivery_tag, requeue=True)
    time.sleep(0.3)
    m2 = ch.basic_get(queue="q_a", auto_ack=False)
    check(m2[0] is not None and m2[0].redelivered is True and m2[2] == b"n1",
          "NACK requeue 重投 + redelivered=True")
    ch.basic_reject(delivery_tag=m2[0].delivery_tag, requeue=False)
    time.sleep(0.2)
    check(ch.basic_get(queue="q_a", auto_ack=True)[0] is None, "REJECT 不重投（消息丢弃）")

    # ---------- 5. 管理面 ----------
    conn2 = mysql_connect()
    cur = conn2.cursor()
    cur.execute("USE GlacierTDB")
    cur.execute("SHOW QUEUES")
    rows = cur.fetchall()
    names = [r[1] for r in rows]
    check("q_a" in names and "q_b" in names, "SHOW QUEUES（%d 队列）" % len(rows))
    qa = [r for r in rows if r[1] == "q_a"][0]
    check(qa[5] > 0 and qa[6] > 0, "队列计数 published=%s delivered=%s" % (qa[5], qa[6]))

    cur.execute("SHOW EXCHANGES")
    rows = cur.fetchall()
    ex = {r[1]: r for r in rows}
    check("ex_topic" in ex and ex["ex_topic"][2] == "topic" and ex["ex_topic"][4] == 3,
          "SHOW EXCHANGES（topic，3 绑定）")

    cur.execute("SELECT * FROM mq_queues WHERE queue = 'q_b'")
    check(cur.fetchone()[1] == "q_b", "mq_queues 表可查")
    cur.execute("SELECT * FROM mq_exchanges WHERE exchange = 'ex_direct'")
    check(cur.fetchone()[2] == "direct", "mq_exchanges 表可查")

    # ---------- 6. 持久化标记（供 --check-restart）----------
    ch.queue_declare(queue="q_persist", durable=True)
    ch.basic_publish(exchange="", routing_key="q_persist", body=b"m1",
                     properties=pika.BasicProperties(delivery_mode=2))
    ch.basic_publish(exchange="", routing_key="q_persist", body=b"m2",
                     properties=pika.BasicProperties(delivery_mode=2))
    time.sleep(0.2)
    check(True, "persistent 消息写入 q_persist")

    # ---------- 7. 异步任务 amqp:// 自服务回环 ----------
    cur.execute("SUBMIT QUERY 'SELECT 42 AS a' CALLBACK 'amqp://glacier:iloveyou~@proxy:3309/' "
                "QUEUE 'glacier_jobs'")
    row = cur.fetchone()
    job_id = row[0]
    state = ""
    for _ in range(60):
        cur.execute("SELECT state FROM query_jobs WHERE job_id = '%s'" % job_id)
        r = cur.fetchone()
        if r:
            state = r[0]
            if state in ("DONE", "ERROR"):
                break
        time.sleep(0.3)
    check(state == "DONE", "异步任务执行（state=%s）" % state)
    time.sleep(1.0)
    m = ch.basic_get(queue="glacier_jobs", auto_ack=True)
    check(m[0] is not None and b"job_id" in m[2], "amqp:// 回调自服务回环（消息入 glacier_jobs）")

    safe_close(conn)
    conn2.close()
    print("V130: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--check-restart":
        return phase_restart_check()
    return phase_main()


if __name__ == "__main__":
    raise SystemExit(main())
