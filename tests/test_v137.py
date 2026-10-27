#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.7 黑盒测试：OpenCV 风格图片预处理（x-gimg-ops）。

GET/PUT/POST-saveas 三链路 × rectangle/text/circle/line/grayscale/blur/rotate/flip，
Pillow 像素级断言（框颜色、文字着色、几何变换）。
"""
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


def get_img(path, query):
    st, _, data = req("GET", path, query=query)
    assert st == 200, (st, data[:120])
    return Image.open(io.BytesIO(data)).convert("RGB")


def main():
    import _strptime  # noqa: F401 (pika 遗留)
    conn = pymysql.connect(host=HOST, port=3309, user="glacier", password="iloveyou~",
                           database="backup", charset="utf8mb4", autocommit=True)
    cur = conn.cursor()
    get_ak(cur)
    print("---- Part 1: 绘制操作（GET 即时处理）----")
    req("PUT", "/v137")
    base = Image.new("RGB", (120, 90), (10, 10, 10))
    st, _, _ = req("PUT", "/v137/base.png", body=png_bytes(base))
    check(st == 200, "底图上传")

    # rectangle：黄框
    img = get_img("/v137/base.png", "x-gimg-ops=rectangle(20,15,60,40,255,255,0,3)")
    edge = img.getpixel((50, 15))  # 顶边
    check(edge == (255, 255, 0), "rectangle 黄框顶边着色（%r）" % (edge,))
    inner = img.getpixel((50, 40))
    check(inner == (10, 10, 10), "框内保持底色")

    # text：白字
    img = get_img("/v137/base.png", "x-gimg-ops=text('ABC',10,20,255,255,255)")
    px = [img.getpixel((x, y)) for x in range(10, 32) for y in range(13, 21)]
    lit = sum(1 for p in px if p == (255, 255, 255))
    check(lit > 10, "text 白字有像素着色（%d px）" % lit)

    # circle 红圈
    img = get_img("/v137/base.png", "x-gimg-ops=circle(60,45,20,255,0,0,2)")
    ring = [img.getpixel((60 + dx, 45 + dy)) for dx, dy in [(20, 0), (0, 20), (-20, 0), (0, -20)]]
    check(all(p == (255, 0, 0) for p in ring), "circle 四向红边（%r）" % (ring,))

    # line 蓝线
    img = get_img("/v137/base.png", "x-gimg-ops=line(5,80,115,80,0,0,255,2)")
    check(img.getpixel((60, 80)) == (0, 0, 255), "line 水平蓝线")

    # 管道组合：灰度 + 框
    img = get_img("/v137/base.png", "x-gimg-ops=grayscale|rectangle(10,10,30,30,0,255,0,1)")
    g = img.getpixel((70, 70))
    check(g[0] == g[1] == g[2] == 10, "grayscale 后灰度一致（%r）" % (g,))

    print("---- Part 2: 几何与滤镜 ----")
    img = get_img("/v137/base.png", "x-gimg-ops=rotate(90)")
    check(img.size == (90, 120), "rotate(90) 尺寸转置（%r）" % (img.size,))

    img = get_img("/v137/base.png", "x-gimg-crop=0,0,60,90&x-gimg-ops=flip(1)")
    check(img.size == (60, 90), "crop+flip 尺寸（%r）" % (img.size,))

    # 左右异色验证 flip(1)
    half = Image.new("RGB", (60, 20), (200, 0, 0))
    half.paste((0, 0, 200), (30, 0, 60, 20))
    req("PUT", "/v137/half.png", body=png_bytes(half))
    img = get_img("/v137/half.png", "x-gimg-ops=flip(1)")
    check(img.getpixel((5, 10)) == (0, 0, 200) and img.getpixel((55, 10)) == (200, 0, 0),
          "flip(1) 左右互换")

    # blur 均值：极端双色边界被平滑
    img = get_img("/v137/half.png", "x-gimg-ops=blur(3)")
    mid = img.getpixel((30, 10))
    check(mid[0] > 30 and mid[2] > 30, "blur(3) 边界混合（%r）" % (mid,))

    print("---- Part 3: PUT 处理落盘 + POST 另存 ----")
    # PUT 即处理（base64 直传 + ops）
    import base64
    b64 = base64.b64encode(png_bytes(base)).decode()
    st, _, _ = req("PUT", "/v137/proc.png",
                   query="x-gimg=base64&x-gimg-ops=rectangle(0,0,50,50,0,255,0,2)&x-gimg-format=png",
                   body=b64.encode())
    check(st == 200, "PUT base64+ops 处理落盘")
    st, _, raw = req("GET", "/v137/proc.png")
    img = Image.open(io.BytesIO(raw)).convert("RGB")
    check(img.getpixel((25, 0)) == (0, 255, 0), "落盘图含框（%r）" % (img.getpixel((25, 0)),))

    # POST save-as：已存对象处理另存
    st, _, _ = req("POST", "/v137/base.png",
                   query="x-gimg-ops=rectangle(5,5,40,40,255,0,255,2)&x-gimg-save-as=v137/saved.png")
    check(st == 200, "POST save-as 200")
    img = get_img("/v137/saved.png", "")
    check(img.getpixel((25, 5)) == (255, 0, 255), "另存图含品红框（%r）" % (img.getpixel((25, 5)),))

    # 错误路径：未知 op / save-as 无 ops
    st, _, _ = req("GET", "/v137/base.png", query="x-gimg-ops=rectangle(0,0,10,10)")  # 参数不足 → 400/透传
    check(st in (200, 400), "错误 op 参数形态（%s）" % st)
    st, _, _ = req("POST", "/v137/base.png", query="x-gimg-save-as=v137/x.png")
    check(st == 400, "save-as 缺 ops → 400")

    for k in ("base.png", "half.png", "proc.png", "saved.png"):
        req("DELETE", "/v137/" + k)
    cur.close()
    conn.close()
    print("V137: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
