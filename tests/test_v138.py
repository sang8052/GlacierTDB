#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.8 黑盒测试：默认静态加密（adiantum，固定口令）。

数据卷已由启动时自动迁移为加密库（.old-plaintext 备份在位）；本测试验证
加密形态下的全协议功能与重启持久化（文件头密文校验由部署侧脚本完成）。
"""
import socket
import sys
import time

import pymysql

HOST, PORT = "proxy", 3309
USER, PW = "glacier", "iloveyou~"
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def main():
    restart = "--check-restart" in sys.argv
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PW,
                           database="backup", charset="utf8mb4", autocommit=True)
    cur = conn.cursor()
    if restart:
        cur.execute("SELECT COUNT(*) FROM v138t")
        check(cur.fetchone()[0] == 3, "重启后加密库数据完好（3 行）")
    else:
        cur.execute("DROP TABLE IF EXISTS v138t")
        cur.execute("CREATE TABLE v138t (id INTEGER PRIMARY KEY, v TEXT)")
        for i in range(3):
            cur.execute("INSERT INTO v138t(v) VALUES('enc-%d')" % i)
        cur.execute("SELECT COUNT(*) FROM v138t")
        check(cur.fetchone()[0] == 3, "加密模式下建表/插入/查询")

        # KV 落加密系统库
        s = socket.create_connection((HOST, PORT), timeout=8)
        s.settimeout(8)
        buf = b""

        def cmd(*args):
            out = b"*%d\r\n" % len(args)
            for a in args:
                b = a.encode()
                out += b"$%d\r\n%s\r\n" % (len(b), b)
            s.sendall(out)
            time.sleep(0.2)
            nonlocal buf
            try:
                buf += s.recv(65536)
            except socket.timeout:
                pass
            return buf

        cmd("AUTH", USER, PW)
        buf = b""
        cmd("SET", "v138:k", "encrypted-ok")
        check(b"+OK" in buf, "加密模式 KV SET")
        buf = b""
        cmd("GET", "v138:k")
        check(b"encrypted-ok" in buf, "加密模式 KV GET")
        s.close()
    cur.close()
    conn.close()
    print("V138: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
