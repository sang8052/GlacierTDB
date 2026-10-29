#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.13 场景测试（独立容器编排，run_all.sh 场景段调用）：

拓扑：
  gt-s13   —— 被测实例（hide_version + heartbeat + s3.sync 规则 → gt-remote）
  gt-remote—— 远端 GlacierTDB（充当云端 S3 端点，作为同步目标）
  pyrecv   —— 心跳 / 同步回调接收器（python HTTP，/dump 返回收集的 JSON）

校验：
 1) 版本号隐藏：MySQL 握手版本 = 8.0.0、S3 Server 头 = GlacierTDB（无版本号）
 2) 数据主动同步：PUT 到 gt-s13/sbox → 自动出现在 gt-remote/rbucket（事件驱动）
 3) sync-origin 断环：远端收到的对象带 x-amz-meta-sync-origin 元数据
 4) 防自旋直环：携带本实例 x-glacier-instance-id 的 PUT → 409 OperationAborted
 5) 心跳上报：pyrecv 收到 heartbeat payload（instance_id / s3 规模 / errors）
 6) 同步回调：pyrecv 收到 sync_result 事件 status=ok
"""
import http.client
import json
import re
import sys
import time
import urllib.request

import pymysql

sys.path.insert(0, "/tests")
from test_s3 import sign_request  # noqa: E402

PROXY_HOST = sys.argv[1] if len(sys.argv) > 1 else "gt-s13"
REMOTE_HOST = sys.argv[2] if len(sys.argv) > 2 else "gt-remote"
RECV_HOST = sys.argv[3] if len(sys.argv) > 3 else "pyrecv"
PW = "iloveyou~"
STATS = {"pass": 0, "fail": 0}
AK = SK = None
RAK = RSK = None


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def sql_on(host, sql):
    conn = pymysql.connect(host=host, port=3309, user="glacier", password=PW,
                           database="backup", connect_timeout=10)
    cur = conn.cursor()
    cur.execute("USE GlacierTDB")
    cur.execute(sql)
    try:
        return cur.fetchall()
    except Exception:
        return None
    finally:
        conn.close()


def s3(host, method, path, body=b"", headers=None, ak=None, sk=None):
    ak = AK if ak is None else ak  # 默认走主实例密钥（远端调用显式传 RAK/RSK）
    sk = SK if sk is None else sk
    hdrs = sign_request(method, path, "", headers or {}, body, ak, sk, "us-east-1", "%s:9000" % host)
    c = http.client.HTTPConnection(host, 9000, timeout=30)
    c.request(method, path, body, hdrs)
    r = c.getresponse()
    data = r.read()
    out = (r.status, {k.lower(): v for k, v in r.getheaders()}, data)
    c.close()
    return out


def recv_dump():
    for _ in range(30):
        try:
            with urllib.request.urlopen("http://%s:8000/dump" % RECV_HOST, timeout=5) as resp:
                return json.loads(resp.read().decode())
        except Exception:
            time.sleep(0.5)
    return {"events": []}


def main():
    global AK, SK, RAK, RSK
    time.sleep(2)

    # 1) 版本号隐藏：MySQL 握手版本 8.0.0
    conn = pymysql.connect(host=PROXY_HOST, port=3309, user="glacier", password=PW,
                           database="backup", connect_timeout=10)
    ver = conn.server_version  # pymysql: 元组 (8,0,0) 或字符串（版本相关）
    ver_ok = str(ver) == "8.0.0" or tuple(ver) == (8, 0, 0)
    check(ver_ok, "hide_version: MySQL handshake version = 8.0.0 (got %s)" % (ver,))
    cur = conn.cursor()
    cur.execute("USE GlacierTDB")
    cur.execute("SELECT @@glaciertdb_instance_id")
    iid = str(cur.fetchall()[0][0])
    check(bool(re.match(r"^[0-9a-f-]{36}$", iid)), "instance id present (%s)" % iid)
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    AK, SK = cur.fetchall()[0][0:2]
    conn.close()
    RAK, RSK = sql_on(REMOTE_HOST, "CREATE S3 KEY FOR 'glacier'")[0][0:2]
    # 源桶 / 远端目标桶（已存在时 409 幂等忽略）
    try:
        sql_on(PROXY_HOST, "CREATE BUCKET sbox")
    except Exception:
        pass
    try:
        sql_on(REMOTE_HOST, "CREATE BUCKET rbucket")
    except Exception:
        pass

    # S3 Server 头隐藏版本 + 实例头
    st, hd, _ = s3(PROXY_HOST, "GET", "/")
    check(st == 200 and hd.get("server") == "GlacierTDB",
          "hide_version: S3 Server header stripped (%r)" % hd.get("server"))
    check(hd.get("x-glacier-instance-id") == iid, "S3 x-glacier-instance-id matches")

    # 2) 事件驱动同步：PUT 本端 → 出现在远端
    payload = b"sync-me-%d" % time.time_ns()
    st, _, _ = s3(PROXY_HOST, "PUT", "/sbox/edge/one.bin", payload, {"Content-Type": "application/octet-stream"}, AK, SK)
    check(st == 200, "PUT to sync bucket (%d)" % st)

    got = None
    last_st = None
    for _ in range(40):
        try:
            st, _, got = s3(REMOTE_HOST, "GET", "/rbucket/edge/one.bin", ak=RAK, sk=RSK)
            last_st = st
            if st == 200:
                break
        except Exception as e:
            last_st = repr(e)[:80]
        time.sleep(0.5)
    check(got == payload, "object synced to remote (event-driven push, last=%s)" % last_st)

    # 3) sync-origin 元数据（多跳断环标记）
    st, hd, _ = s3(REMOTE_HOST, "HEAD", "/rbucket/edge/one.bin", ak=RAK, sk=RSK)
    check(hd.get("x-amz-meta-sync-origin") == iid,
          "remote object carries sync-origin metadata (%s)" % hd.get("x-amz-meta-sync-origin"))

    # 4) 防自旋直环：带本实例 ID 的 PUT → 409
    hdrs = sign_request("PUT", "/sbox/loop.bin", "", {}, b"x", AK, SK, "us-east-1", "%s:9000" % PROXY_HOST)
    hdrs["x-glacier-instance-id"] = iid
    c = http.client.HTTPConnection(PROXY_HOST, 9000, timeout=10)
    c.request("PUT", "/sbox/loop.bin", b"x", hdrs)
    r = c.getresponse()
    body = r.read()
    c.close()
    check(r.status == 409 and b"OperationAborted" in body,
          "anti-spin: self-referencing PUT rejected 409 (%d)" % r.status)

    # 5) 心跳 + 6) 同步回调（pyrecv 已收集）
    time.sleep(6)
    data = recv_dump()
    beats = [e for e in data.get("events", []) if e.get("kind") == "beat"]
    cbs = [e for e in data.get("events", []) if e.get("kind") == "cb"]
    check(len(beats) >= 1, "heartbeat received (%d beats)" % len(beats))
    if beats:
        p = beats[-1]["payload"]
        check(p.get("instance_id") == iid and "s3" in p and "errors" in p,
              "heartbeat payload has instance_id + s3 + errors")
    check(any(c.get("payload", {}).get("event") == "sync_result" and c["payload"].get("status") == "ok"
              for c in cbs), "sync_result callback received with status ok")

    print()
    print("SYNC PASS=%d FAIL=%d" % (STATS["pass"], STATS["fail"]))
    sys.exit(0 if STATS["fail"] == 0 else 1)


if __name__ == "__main__":
    main()
