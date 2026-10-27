#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.10 黑盒测试：图片管道提速后的行为与磁盘缓存、ops 语法修正、防爆破与解锁。

覆盖：
1) x-gimg-ops 结果磁盘缓存（miss→hit→同字节；参数变化/对象覆盖 → 重新 miss）
2) 裸名 op 合法化（grayscale ≡ grayscale()）；非法管道显式 400（不再静默透传）
3) 计时观测头 X-Using-Time / X-Gimg-Ops-Time / X-Gimg-Cache
4) 账号维度防爆破锁定 + 管理面 UNLOCK ACCOUNT
（本机回环豁免、CLI -unlock-user/-show-locks、mysql.enable 关闭由 run_all.sh 的
 shell 段在宿主机 / proxy 网络命名空间内验证）
"""
import base64
import hashlib
import io
import sys

import pymysql
from PIL import Image

sys.path.insert(0, "/tests")
from test_s3 import s3_request  # noqa: E402

HOST = "proxy"
AK, SK = None, None
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def get_ak(cur):
    global AK, SK
    cur.execute("USE GlacierTDB")
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    row = cur.fetchone()
    AK, SK = row[0], row[1]
    cur.execute("USE backup")


def req(method, path, query="", body=b"", extra=None):
    return s3_request(method, path, query=query, headers=extra or {}, body=body, ak=AK, sk=SK)


def png_bytes(img):
    out = io.BytesIO()
    img.save(out, "PNG")
    return out.getvalue()


def admin_sql(cur, sql):
    cur.execute("USE GlacierTDB")
    cur.execute(sql)
    row = cur.fetchone()
    cur.execute("USE backup")
    return row


def main():
    conn = pymysql.connect(host=HOST, port=3309, user="glacier", password="iloveyou~",
                           database="backup", charset="utf8mb4", autocommit=True)
    cur = conn.cursor()
    get_ak(cur)

    print("---- Part 1: 图片管道缓存（x-gimg-ops GET）----")
    req("PUT", "/v1310")
    import random
    random.seed()  # 每次运行生成不同底图：保证 etag 唯一 → 缓存键全新（缓存跨回归持久化）
    img = Image.new("RGB", (160, 120), (30, 60, 200))
    for x in range(0, 160, 4):
        for y in range(120):
            img.putpixel((x, y), (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255)))
    st, _, _ = req("PUT", "/v1310/base.png", body=png_bytes(img))
    check(st == 200, "底图上传")

    ops_q = "grayscale|blur(5)|rectangle(10,10,40,30,0,255,0,2)"
    q = "x-gimg-ops=" + ops_q + "&x-gimg-format=png"
    st1, h1, d1 = req("GET", "/v1310/base.png", query=q)
    check(st1 == 200 and h1.get("x-gimg-cache") == "miss", "首次加工 miss（%s）" % h1.get("x-gimg-cache"))
    check("x-using-time" in h1 and "x-gimg-ops-time" in h1, "计时观测头存在（%s / %s）" %
          (h1.get("x-using-time"), h1.get("x-gimg-ops-time")))

    st2, h2, d2 = req("GET", "/v1310/base.png", query=q)
    check(st2 == 200 and h2.get("x-gimg-cache") == "hit", "二次请求缓存命中（%s）" % h2.get("x-gimg-cache"))
    check(hashlib.md5(d1).hexdigest() == hashlib.md5(d2).hexdigest(), "命中结果与首次字节一致")

    g = Image.open(io.BytesIO(d1)).convert("RGB").getpixel((100, 100))
    check(g[0] == g[1] == g[2], "管道生效：灰度通道一致（%r）" % (g,))
    check(Image.open(io.BytesIO(d1)).convert("RGB").getpixel((30, 10)) == (0, 255, 0),
          "rectangle 画框生效")

    # 参数变化（拼在 ops 段末尾）→ 新键 miss
    st3, h3, _ = req("GET", "/v1310/base.png",
                     query="x-gimg-ops=" + ops_q + "|flip(1)&x-gimg-format=png")
    check(st3 == 200 and h3.get("x-gimg-cache") == "miss", "参数变化重新 miss（%s）" % h3.get("x-gimg-cache"))

    # 对象覆盖（新内容 → 新 etag）→ 旧缓存失效
    img2 = Image.new("RGB", (160, 120), (0, 128, random.randint(0, 255)))
    req("PUT", "/v1310/base.png", body=png_bytes(img2))
    st4, h4, d4 = req("GET", "/v1310/base.png", query=q)
    check(st4 == 200 and h4.get("x-gimg-cache") == "miss", "对象覆盖后缓存失效（%s）" % h4.get("x-gimg-cache"))
    og4 = Image.open(io.BytesIO(d4)).convert("RGB").getpixel((100, 100))
    r, g2c, b = img2.getpixel((100, 100))
    expect = int(0.299 * r + 0.587 * g2c + 0.114 * b)
    check(og4 == (expect, expect, expect), "新底图加工结果正确（%r vs %r）" % (og4, (expect, expect, expect)))

    print("---- Part 2: ops 语法修正 ----")
    # 裸名 grayscale（文档写法）现与 grayscale() 等价
    st, _, d = req("GET", "/v1310/base.png", query="x-gimg-ops=grayscale")
    if st == 200:
        og = Image.open(io.BytesIO(d)).convert("RGB").getpixel((5, 5))
        check(og[0] == og[1] == og[2], "裸名 grayscale 生效（%r）" % (og,))
    else:
        check(False, "裸名 grayscale 返回 %s" % st)
    st, _, d = req("GET", "/v1310/base.png", query="x-gimg-ops=grayscale()")
    og2 = Image.open(io.BytesIO(d)).convert("RGB").getpixel((5, 5))
    check(st == 200 and og2 == og, "grayscale() 与裸名等价")

    # 非法管道：显式 400（不再静默透传）
    st, _, _ = req("GET", "/v1310/base.png", query="x-gimg-ops=grayscal")
    check(st == 400, "未知 op → 400（%s）" % st)
    st, _, _ = req("GET", "/v1310/base.png", query="x-gimg-ops=blur(((")
    check(st == 400, "语法错误 → 400（%s）" % st)
    st, _, _ = req("GET", "/v1310/base.png", query="x-gimg-ops=blur(4)")
    check(st == 400, "blur 偶数核 → 400（%s）" % st)

    print("---- Part 3: 防爆破（账号维度）+ 管理面解锁 ----")
    admin_sql(cur, "CREATE USER 'u1310' IDENTIFIED BY 'pw1310'")
    check(True, "CREATE USER u1310")
    # 连续失败至超阈值（默认账号阈值 5）：期间用错误密码
    locked_seen = False
    for i in range(8):
        try:
            c1 = pymysql.connect(host=HOST, port=3309, user="u1310", password="wrong-%d" % i,
                                 connect_timeout=5)
            c1.close()
        except pymysql.err.OperationalError as e:
            if e.args[0] == 3119:
                locked_seen = True
                break
    check(locked_seen, "连续失败触发账号锁（3119）")
    # 锁定期内正确密码也拒绝
    blocked = False
    try:
        pymysql.connect(host=HOST, port=3309, user="u1310", password="pw1310", connect_timeout=5)
    except pymysql.err.OperationalError as e:
        blocked = e.args[0] == 3119
    check(blocked, "锁定期内正确密码被拒")
    # 管理面解锁后可登录
    admin_sql(cur, "UNLOCK ACCOUNT 'u1310'")
    ok = False
    try:
        c2 = pymysql.connect(host=HOST, port=3309, user="u1310", password="pw1310", connect_timeout=5)
        c2.close()
        ok = True
    except pymysql.err.OperationalError:
        ok = False
    check(ok, "UNLOCK ACCOUNT 后可登录")
    admin_sql(cur, "DROP USER 'u1310'")

    # 留一个待 CLI 解锁的账号锁（run_all.sh shell 段用 -unlock-user 验证）
    for i in range(7):
        try:
            pymysql.connect(host=HOST, port=3309, user="clilock1310", password="bad-%d" % i,
                            connect_timeout=5)
        except pymysql.err.OperationalError:
            pass

    for k in ("base.png",):
        req("DELETE", "/v1310/" + k)
    req("DELETE", "/v1310")
    cur.close()
    conn.close()
    print("V1310: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
