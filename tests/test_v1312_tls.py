#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.12 TLS 专项（独立容器，由 run_all.sh 编排）：
用法：python test_v1312_tls.py <host> <port> <ca.pem>

1) TLS-first RESP：ssl 套接字发 *1 PING → +PONG（3309 双模式接入）
2) 同端口明文 RESP：mode:redis 声明 + PING → +PONG（明文/TLS 共存）
3) MySQL over TLS：pymysql ssl={'ca': ...} 握手 + SELECT 1
4) 同端口明文 MySQL：pymysql 无 ssl 直连（协议嗅探默认路径不受影响）
"""
import socket
import ssl
import sys

import pymysql

STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def tls_resp_ping(host, port, ca):
    ctx = ssl.create_default_context(cafile=ca)
    with socket.create_connection((host, port), timeout=10) as raw:
        with ctx.wrap_socket(raw, server_hostname=host) as s:
            s.sendall(b"*1\r\n$4\r\nPING\r\n")
            return s.recv(64)


def plain_resp_ping(host, port):
    with socket.create_connection((host, port), timeout=10) as s:
        s.sendall(b"mode:redis\n*1\r\n$4\r\nPING\r\n")
        return s.recv(64)


def main():
    host, port, ca = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    try:
        out = tls_resp_ping(host, port, ca)
        check(out.startswith(b"+PONG") or out.startswith(b"-NOAUTH") or b"PONG" in out,
              "TLS-first RESP PING（双模式 3309）→ %r" % out[:32])
    except Exception as e:
        check(False, "TLS-first RESP 失败：%s" % e)

    try:
        out = plain_resp_ping(host, port)
        check(b"PONG" in out, "同端口明文 RESP PING（共存）→ %r" % out[:32])
    except Exception as e:
        check(False, "明文 RESP 失败：%s" % e)

    try:
        cn = pymysql.connect(host=host, port=port, user="glacier", password="iloveyou~",
                             database="t", charset="utf8mb4", autocommit=True,
                             ssl={"ca": ca})
        with cn.cursor() as c:
            c.execute("SELECT 1")
            check(c.fetchone()[0] == 1, "MySQL over TLS（ssl ca 校验）SELECT 1")
        cn.close()
    except Exception as e:
        check(False, "MySQL over TLS 失败：%s" % e)

    try:
        cn = pymysql.connect(host=host, port=port, user="glacier", password="iloveyou~",
                             database="t", charset="utf8mb4", autocommit=True)
        with cn.cursor() as c:
            c.execute("SELECT 1")
            check(c.fetchone()[0] == 1, "同端口明文 MySQL SELECT 1（不受 TLS 影响）")
        cn.close()
    except Exception as e:
        check(False, "明文 MySQL 失败：%s" % e)

    print("\nv1.3.12 TLS: %d pass, %d fail" % (STATS["pass"], STATS["fail"]))
    return 0 if STATS["fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
