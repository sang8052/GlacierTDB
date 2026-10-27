#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.11 黑盒测试：

1) S3 底层存储：8MB 分片直写 + multipart 零拷贝合并 + manifest Range 读（206/416）
2) 图片裁剪融合 blend（同桶源 + 可选裁剪 + alpha）+ save-as 禁桶（安全修复）
3) 管理面异步任务：SUBMIT S3 TEXT/FILE SEARCH / DELETE（DRY RUN）
4) 空间回收：SHOW S3 SPACE / S3 VACUUM（增量 + FULL）
5) 租户隔离安全：copy-source 越权（403）、管理面 USER 禁入（1044）、
   DROP DATABASE 门禁、SUBMIT QUERY 收权（USER 仅 SELECT + 授权表）、
   异步任务属主隔离、WITH 前缀 DML 不再判读、只读账号冷命令拒绝
6) 时序大表：WITH (TIMESERIES, TS_UNIT='s|ms|us') 谓词、ts 秒/毫秒/微秒自动换算
   （存秒查毫秒）、SHOW TS TABLES、声明表 rollup 路由 + 冷压缩、REBUILD ROLLUP <db>.<tbl>
"""
import hashlib
import io
import os
import sys
import time

import pymysql
from PIL import Image

sys.path.insert(0, "/tests")
from test_s3 import s3_request  # noqa: E402
from test_v131 import extract_upload_id, composite_etag  # noqa: E402

HOST, PORT = "proxy", 3309
ADMIN, ADMIN_PW = "glacier", "iloveyou~"
AK, SK = None, None
SEC_T = "sec_t%d" % int(time.time())
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def conn(user=None, pw=None, db=None):
    return pymysql.connect(host=HOST, port=PORT,
                           user=user or ADMIN, password=pw or ADMIN_PW,
                           database=db, charset="utf8mb4", autocommit=True)


def admin_sql(cur, sql):
    cur.execute("USE GlacierTDB")
    cur.execute(sql)
    try:
        return cur.fetchall()
    except Exception:
        return None


def req(method, path, query="", body=b"", extra=None, ak=None, sk=None):
    return s3_request(method, path, query=query, headers=extra or {}, body=body,
                      ak=ak or AK, sk=sk or SK)


def png_bytes(img):
    out = io.BytesIO()
    img.save(out, "PNG")
    return out.getvalue()


def get_img(path, query):
    st, _, data = req("GET", path, query=query)
    assert st == 200, (st, data[:120])
    return Image.open(io.BytesIO(data)).convert("RGB")


def wait_job(cur, keyword, timeout=20):
    """轮询 SHOW QUERY JOBS，返回 (job_id, state, error)。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        cur.execute("USE GlacierTDB")
        cur.execute("SHOW QUERY JOBS")
        rows = cur.fetchall()
        cur.execute("USE backup")
        for r in rows:
            if r[0] == keyword or keyword in (r[2] or ""):
                if r[1] in ("DONE", "ERROR"):
                    return r[0], r[1], r[7] or ""
        time.sleep(0.4)
    return None, "TIMEOUT", ""


def fetch_job(cur, jid, limit=200):
    cur.execute("USE GlacierTDB")
    cur.execute("FETCH QUERY RESULT '%s' LIMIT %d" % (jid, limit))
    rows = cur.fetchall()
    cur.execute("USE backup")
    return rows


# ---------------------------------------------------------------------------
# Part 1: S3 分片存储 + Range
# ---------------------------------------------------------------------------

def part_storage(cur):
    print("---- Part 1: 分片存储（8MB 分片直写 / 零拷贝合并 / Range）----")
    req("PUT", "/v1311")
    blob = os.urandom(8 * 1024 * 1024 + 1000)
    blob2 = os.urandom(8 * 1024 * 1024)
    blob3 = os.urandom(5 * 1024 * 1024 - 77)
    whole = blob + blob2 + blob3

    st, hd, data = req("POST", "/v1311/big.bin", query="uploads", body=b"")
    check(st == 200, "InitiateMultipartUpload")
    uid = extract_upload_id(data)

    parts = [(1, blob), (2, blob2), (3, blob3)]
    md5s = []
    for n, p in parts:
        st, hd, _ = req("PUT", "/v1311/big.bin", query="partNumber=%d&uploadId=%s" % (n, uid), body=p)
        check(st == 200 and hd.get("etag"), "UploadPart %d（%d MB）" % (n, len(p) // 1048576))
        md5s.append(hashlib.md5(p).hexdigest())

    # 分片重传覆盖（S3 语义）
    st, hd, _ = req("PUT", "/v1311/big.bin", query="partNumber=3&uploadId=%s" % uid, body=blob3)
    check(st == 200, "同 partNumber 重传覆盖")

    body_xml = "<CompleteMultipartUpload>" + "".join(
        "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>"
        % (n, hashlib.md5(p).hexdigest()) for n, p in parts) + "</CompleteMultipartUpload>"
    st, hd, data = req("POST", "/v1311/big.bin", query="uploadId=%s" % uid, body=body_xml.encode())
    check(st == 200, "CompleteMultipartUpload 零拷贝合并")
    want_etag = composite_etag(md5s)
    check(want_etag.encode() in data or ('"%s"' % want_etag).encode() in data,
          "复合 ETag 语义保持（%s-3）" % want_etag[:8])

    st, hd, data = req("GET", "/v1311/big.bin")
    check(st == 200 and hashlib.sha256(data).hexdigest() == hashlib.sha256(whole).hexdigest(),
          "GET 全量字节一致（%d B）" % len(whole))

    # Range：206 / Content-Range / 字节切片
    st, hd, data = req("GET", "/v1311/big.bin", extra={"Range": "bytes=100-199"})
    check(st == 206 and data == whole[100:200] and hd.get("content-range") == "bytes 100-199/%d" % len(whole),
          "Range bytes=100-199（206 + 切片一致）")
    # 跨分块边界（默认 8MB）
    st, hd, data = req("GET", "/v1311/big.bin", extra={"Range": "bytes=8388600-8388620"})
    check(st == 206 and data == whole[8388600:8388621], "Range 跨 8MB 分块边界")
    st, hd, data = req("GET", "/v1311/big.bin", extra={"Range": "bytes=-100"})
    check(st == 206 and data == whole[-100:], "Range 后缀 bytes=-100")
    st, hd, data = req("GET", "/v1311/big.bin", extra={"Range": "bytes=5000000-"})
    check(st == 206 and data == whole[5000000:], "Range 开放右端 bytes=5000000-")
    st, hd, data = req("GET", "/v1311/big.bin", extra={"Range": "bytes=999999999-"})
    check(st == 416, "越界 Range → 416")

    # 单 PUT 大对象（走 chunk 路径）
    put10 = os.urandom(10 * 1024 * 1024 + 123)
    st, hd, _ = req("PUT", "/v1311/single.bin", body=put10)
    check(st == 200, "单 PUT 10MB")
    st, hd, data = req("GET", "/v1311/single.bin")
    check(data == put10, "单 PUT 读回一致")
    st, hd, data = req("GET", "/v1311/single.bin", extra={"Range": "bytes=4194304-4194400"})
    check(st == 206 and data == put10[4194304:4194401], "单 PUT Range 切片一致")

    # 管理面：storage=chunk
    rows = admin_sql(cur, "SHOW S3 OBJECTS v1311 LIKE 'big.bin'")
    check(rows and rows[0][5] == "chunk", "SHOW S3 OBJECTS storage=chunk")

    # Abort 清理暂存
    st, _, data = req("POST", "/v1311/ab.bin", query="uploads", body=b"")
    uid2 = extract_upload_id(data)
    req("PUT", "/v1311/ab.bin", query="partNumber=1&uploadId=%s" % uid2, body=os.urandom(1024 * 1024))
    st, _, _ = req("DELETE", "/v1311/ab.bin", query="uploadId=%s" % uid2)
    check(st == 204, "AbortMultipartUpload")
    st, _, _ = req("GET", "/v1311/ab.bin")
    check(st == 404, "中止后对象不存在")
    rows = admin_sql(cur, "SHOW S3 UPLOADS v1311")
    check(not any(r[0] == uid2 for r in rows), "会话已清理")

    # UploadPartCopy（同桶源，合法）
    st, _, data = req("POST", "/v1311/cp.bin", query="uploads", body=b"")
    uid3 = extract_upload_id(data)
    st, _, data = req("PUT", "/v1311/cp.bin",
                      query="partNumber=1&uploadId=%s" % uid3,
                      extra={"X-Amz-Copy-Source": "/v1311/single.bin"})
    check(st == 200 and b"CopyPartResult" in data, "UploadPartCopy（授权源）")
    body_xml = ("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber>"
                "<ETag>%s</ETag></Part></CompleteMultipartUpload>"
                % hashlib.md5(put10).hexdigest()).encode()
    st, _, _ = req("POST", "/v1311/cp.bin", query="uploadId=%s" % uid3, body=body_xml)
    check(st == 200, "Complete UploadPartCopy")
    st, _, data = req("GET", "/v1311/cp.bin")
    check(data == put10, "分片拷贝对象读回一致")


# ---------------------------------------------------------------------------
# Part 2: blend 裁剪融合 + save-as 安全
# ---------------------------------------------------------------------------

def part_blend(cur):
    print("---- Part 2: 图片裁剪融合（blend）+ save-as 禁桶 ----")
    base = Image.new("RGB", (100, 80), (10, 10, 10))
    over = Image.new("RGB", (40, 30), (200, 20, 20))
    st, _, _ = req("PUT", "/v1311/base.png", body=png_bytes(base))
    check(st == 200, "底图上传")
    st, _, _ = req("PUT", "/v1311/over.png", body=png_bytes(over))
    check(st == 200, "融合源上传（同桶）")

    img = get_img("/v1311/base.png", "x-gimg-ops=blend('over.png',10,20)")
    check(img.getpixel((30, 35)) == (200, 20, 20), "blend 贴入位置取源色")
    check(img.getpixel((5, 5)) == (10, 10, 10), "贴入区外保持底色")
    check(img.getpixel((10, 20)) == (200, 20, 20) and img.getpixel((9, 20)) == (10, 10, 10),
          "blend 边界精确（x=10 起）")

    img = get_img("/v1311/base.png", "x-gimg-ops=blend('over.png',0,0,0.5)")
    px = img.getpixel((20, 15))
    check(all(abs(px[i] - (10 * 0.5 + (200, 20, 20)[i] * 0.5)) <= 1 for i in range(3)),
          "blend alpha=0.5 加权融合（%r）" % (px,))

    img = get_img("/v1311/base.png", "x-gimg-ops=blend('over.png',5,5,1,10,10,20,10)")
    check(img.getpixel((10, 12)) == (200, 20, 20), "blend 裁剪融合：裁剪区内取源色")
    check(img.getpixel((30, 15)) == (10, 10, 10), "裁剪融合：裁剪区外不覆盖")

    # 非法参数
    st, _, _ = req("GET", "/v1311/base.png", query="x-gimg-ops=blend('over.png',0,0,1.7)")
    check(st == 400, "alpha 越界 → 400")
    st, _, _ = req("GET", "/v1311/base.png", query="x-gimg-ops=blend('missing.png',0,0)")
    check(st == 400, "融合源不存在 → 400")

    # PUT 回写（上传即融合）
    st, _, _ = req("PUT", "/v1311/composed.png",
                   query="x-gimg-ops=blend('over.png',8,8)", body=png_bytes(base))
    check(st == 200, "PUT 上传即融合落盘")
    img = get_img("/v1311/composed.png", "")
    check(img.getpixel((20, 20)) == (200, 20, 20), "回写对象含融合结果")

    # POST 另存：同桶 key 合法；带 bucket 一律 400（安全修复）
    st, _, _ = req("POST", "/v1311/base.png",
                   query="x-gimg-ops=blend('over.png',1,1)&x-gimg-save-as=derived.png")
    check(st == 200, "save-as 同桶 key 合法")
    st, _, _ = req("GET", "/v1311/derived.png")
    check(st == 200, "另存对象可读")
    req("PUT", "/v1311b")
    st, _, data = req("POST", "/v1311/base.png",
                      query="x-gimg-ops=blend('over.png',1,1)&x-gimg-save-as=v1311b/stolen.png")
    check(st == 400, "save-as 带桶前缀 → 400（越桶写修复）")
    st, _, _ = req("GET", "/v1311b/stolen.png")
    check(st == 404, "越桶写入未发生")


# ---------------------------------------------------------------------------
# Part 3: SUBMIT S3 管理面异步任务
# ---------------------------------------------------------------------------

def part_admin_tasks(cur):
    print("---- Part 3: SUBMIT S3 TEXT/FILE SEARCH / DELETE ----")
    for i, (name, body) in enumerate([
        ("t1.txt", b"hello NEEDLE_1311 world\nsecond line\n"),
        ("t2.log", b"noise\nthe NEEDLE_1311 sits at line two\n"),
        ("t3.txt", b"no match here\n"),
        ("big.bin", os.urandom(64 * 1024)),
    ]):
        st, _, _ = req("PUT", "/v1311/txt/" + name, body=body,
                       extra={"Content-Type": "text/plain" if name.endswith((".txt", ".log")) else "application/octet-stream"})
        check(st == 200, "检索样本上传 %s" % name)

    # FILE SEARCH：LIKE + SIZE
    cur.execute("USE GlacierTDB")
    cur.execute("SUBMIT S3 FILE SEARCH v1311 PREFIX 'txt/' LIMIT 10")
    jid = cur.fetchall()[0][0]
    cur.execute("USE backup")
    _, state, err = wait_job(cur, jid)
    check(state == "DONE", "FILE SEARCH 任务 DONE（%s）" % err)
    rows = fetch_job(cur, jid)
    keys = sorted(r[1] for r in rows)
    check(set(keys) >= {"txt/big.bin", "txt/t1.txt", "txt/t2.log", "txt/t3.txt"}, "FILE SEARCH PREFIX 命中集合（%r）" % keys)

    cur.execute("USE GlacierTDB")
    cur.execute("SUBMIT S3 FILE SEARCH v1311 LIKE 'txt/t%' SIZE BETWEEN 10 AND 40")
    jid = cur.fetchall()[0][0]
    cur.execute("USE backup")
    _, state, _ = wait_job(cur, jid)
    rows = fetch_job(cur, jid)
    check(state == "DONE" and sorted(r[1] for r in rows) == ["txt/t1.txt", "txt/t2.log", "txt/t3.txt"],
          "FILE SEARCH LIKE+SIZE 过滤")

    # TEXT SEARCH：内容级
    cur.execute("USE GlacierTDB")
    cur.execute("SUBMIT S3 TEXT SEARCH v1311 MATCH 'needle_1311' PREFIX 'txt/' LIMIT 20")
    jid = cur.fetchall()[0][0]
    cur.execute("USE backup")
    _, state, err = wait_job(cur, jid)
    rows = fetch_job(cur, jid)
    hits = {(r[1], r[3]) for r in rows}
    check(state == "DONE" and ("txt/t1.txt", 1) in hits and ("txt/t2.log", 2) in hits,
          "TEXT SEARCH 行号命中（大小写不敏感，%r）" % hits)
    check(all("NEEDLE_1311" in (r[4] or "") or "needle_1311" in (r[4] or "").lower() for r in rows),
          "TEXT SEARCH 返回命中行文本")

    # 语法错误
    cur.execute("USE GlacierTDB")
    try:
        cur.execute("SUBMIT S3 TEXT SEARCH v1311 PREFIX 'txt/'")
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] == 1064
    check(ok, "TEXT SEARCH 缺 MATCH → 1064")
    try:
        cur.execute("SUBMIT S3 FILE SEARCH v1311 WHATEVER 1")
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] == 1064
    check(ok, "未知选项 → 1064")

    # DELETE：DRY RUN → 真删
    cur.execute("USE GlacierTDB")
    cur.execute("SUBMIT S3 DELETE v1311 PREFIX 'txt/t' DRY RUN")
    jid = cur.fetchall()[0][0]
    cur.execute("USE backup")
    _, state, _ = wait_job(cur, jid)
    rows = fetch_job(cur, jid)
    check(state == "DONE" and all(r[3] == "dry-run" for r in rows) and len(rows) == 3,
          "DELETE DRY RUN 只报告不删")
    st, _, _ = req("GET", "/v1311/txt/t1.txt")
    check(st == 200, "DRY RUN 后对象仍在")

    cur.execute("USE GlacierTDB")
    cur.execute("SUBMIT S3 DELETE v1311 PREFIX 'txt/t'")
    jid = cur.fetchall()[0][0]
    cur.execute("USE backup")
    _, state, _ = wait_job(cur, jid)
    rows = fetch_job(cur, jid)
    check(state == "DONE" and all(r[3] == "deleted" for r in rows), "DELETE 实删（%r）" % [r[3] for r in rows])
    for k in ("t1.txt", "t2.log", "t3.txt"):
        st, _, _ = req("GET", "/v1311/txt/" + k)
        check(st == 404, "已删对象 %s → 404" % k)

    # WORM 对象跳过（locked）
    req("PUT", "/v1311/txt/locked.txt", body=b"keep me", extra={"Content-Type": "text/plain"})
    admin_sql(cur, "S3 RETAIN v1311/txt/locked.txt UNTIL '2099-01-01' COMPLIANCE")
    cur.execute("USE GlacierTDB")
    cur.execute("SUBMIT S3 DELETE v1311 PREFIX 'txt/locked'")
    jid = cur.fetchall()[0][0]
    cur.execute("USE backup")
    _, state, _ = wait_job(cur, jid)
    rows = fetch_job(cur, jid)
    check(state == "DONE" and rows and rows[0][3] == "locked", "WORM 对象删除被拒（locked）")
    st, _, _ = req("GET", "/v1311/txt/locked.txt")
    check(st == 200, "WORM 对象仍在")


# ---------------------------------------------------------------------------
# Part 4: 空间回收
# ---------------------------------------------------------------------------

def part_vacuum(cur):
    print("---- Part 4: SHOW S3 SPACE / S3 VACUUM ----")
    rows = admin_sql(cur, "SHOW S3 SPACE")
    check(rows and rows[0][0] > 0, "SHOW S3 SPACE 返回页统计（page_size=%s）" % (rows[0][0] if rows else "?",))
    rows2 = admin_sql(cur, "S3 VACUUM FULL")
    check(rows2 and rows2[0][0] == "full", "S3 VACUUM FULL（切换增量模式 + 重建）")
    rows3 = admin_sql(cur, "SHOW S3 SPACE")
    check(rows3 and rows3[0][5] == "incremental", "auto_vacuum 已切增量（%r）" % (rows3[0][5] if rows3 else "?",))
    rows4 = admin_sql(cur, "S3 VACUUM")
    check(rows4 and rows4[0][0] == "incremental", "S3 VACUUM 增量路径")


# ---------------------------------------------------------------------------
# Part 5: 租户隔离安全
# ---------------------------------------------------------------------------

def part_security(cur):
    print("---- Part 5: 租户隔离安全 ----")
    admin_sql(cur, "CREATE DATABASE IF NOT EXISTS db_ta")
    admin_sql(cur, "CREATE DATABASE IF NOT EXISTS db_tb")
    try:
        admin_sql(cur, "DROP USER 'ta1311'")
    except Exception:
        pass
    admin_sql(cur, "CREATE USER 'ta1311' IDENTIFIED BY 'pw1311' USER DEFAULT_DB db_ta")
    admin_sql(cur, "GRANT SELECT,INSERT,CREATE ON db_ta.* TO 'ta1311'")

    tc = conn("ta1311", "pw1311", "db_ta")
    tcur = tc.cursor()

    # ① 管理面禁入（USER）
    try:
        tcur.execute("USE GlacierTDB")
        entered = True
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        entered = e.args[0] != 1044
    check(not entered, "USER 账号 USE GlacierTDB → 1044（管理面禁入）")

    # ② CREATE/DROP DATABASE 门禁
    try:
        tcur.execute("DROP DATABASE db_tb")
        dropped = True
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        dropped = e.args[0] != 1142
    check(not dropped, "USER DROP DATABASE → 1142")
    try:
        tcur.execute("CREATE DATABASE evil_db")
        created = True
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        created = e.args[0] != 1142
    check(not created, "USER CREATE DATABASE → 1142")

    # ③ SET GLOBAL_THREADS：非管理面发不进真实处理器（探活层空应答或报错均可，
    #    关键是后端并发值不被改动——用 SELECT 1 探活确认连接与服务存活）
    try:
        tcur.execute("SET GLOBAL_THREADS = 1")
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError):
        pass
    tcur.execute("SELECT 1")
    check(tcur.fetchone()[0] == 1, "USER SET GLOBAL_THREADS 无特权生效（服务存活）")

    # ④ SUBMIT QUERY 收权：USER 仅 SELECT 且须过授权
    tcur.execute("CREATE TABLE %s (id INTEGER, v TEXT)" % SEC_T)
    tcur.execute("INSERT INTO %s VALUES (1,'a')" % SEC_T)
    try:
        tcur.execute("SUBMIT QUERY 'DELETE FROM %s'" % SEC_T)
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] == 1142
    check(ok, "USER SUBMIT DELETE → 1142（异步不再绕权）")
    try:
        tcur.execute("SUBMIT QUERY 'SELECT * FROM db_tb.anything'")
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] in (1142, 1105)
    check(ok, "USER SUBMIT 未授权库查询 → 拒绝")
    tcur.execute("SUBMIT QUERY 'SELECT COUNT(*) FROM %s'" % SEC_T)
    jid_user = tcur.fetchall()[0][0]
    tcur.execute("SHOW QUERY JOBS")
    own = [r for r in tcur.fetchall()]
    check(any(r[0] == jid_user for r in own), "USER SHOW QUERY JOBS 含自己的任务")

    # ⑤ 属主隔离：super 的任务对 USER 不可见/不可取
    cur.execute("SUBMIT QUERY 'SELECT 1 AS x'")
    jid_super = cur.fetchall()[0][0]
    tcur.execute("SHOW QUERY JOBS")
    check(not any(r[0] == jid_super for r in tcur.fetchall()), "USER 看不到 super 的任务")
    try:
        tcur.execute("FETCH QUERY RESULT '%s'" % jid_super)
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] == 1142
    check(ok, "USER FETCH 他人结果 → 1142")
    try:
        tcur.execute("CANCEL QUERY '%s'" % jid_super)
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] in (1142, 1105)
    check(ok, "USER CANCEL 他人任务 → 拒绝")

    # ⑥ WITH 前缀 DML 不再判读
    try:
        tcur.execute("WITH x AS (SELECT 1 AS n) DELETE FROM %s WHERE id IN (SELECT n FROM x)" % SEC_T)
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] == 1142
    check(ok, "WITH ... DELETE（USER 仅 SELECT）→ 1142")
    tcur.execute("SELECT COUNT(*) FROM %s" % SEC_T)
    check(tcur.fetchone()[0] == 1, "数据未被 WITH-DELETE 改动")

    # ⑦ 只读账号：WITH-DELETE / COMPRESS COLD 拒绝
    try:
        admin_sql(cur, "DROP USER 'ro1311'")
    except Exception:
        pass
    admin_sql(cur, "CREATE USER 'ro1311' IDENTIFIED BY 'pw1311' READONLY DEFAULT_DB db_ta")
    rc = conn("ro1311", "pw1311", "db_ta")
    rcur = rc.cursor()
    try:
        rcur.execute("WITH x AS (SELECT 1 AS n) DELETE FROM %s WHERE id IN (SELECT n FROM x)" % SEC_T)
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] in (1290, 1142)
    check(ok, "只读账号 WITH-DELETE → 拒绝（此前可写！）")
    try:
        rcur.execute("COMPRESS COLD %s BEFORE '2099-01-01'" % SEC_T)
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] in (1290, 1142, 1105)
    check(ok, "只读账号 COMPRESS COLD → 拒绝")
    rc.close()

    # ⑧ S3 copy-source 越权（跨租户读 → 403）
    st, _, data = req("PUT", "/v1311/secret.txt", body=b"tenant secret")
    check(st == 200, "属主上传 secret（glacier 桶）")
    rows = admin_sql(cur, "CREATE S3 KEY FOR 'ta1311'")
    tak, task_sk = rows[0][0], rows[0][1]
    st, _, data = req("PUT", "/v1311b", ak=tak, sk=task_sk)  # ta1311 自己的桶
    st, _, data = req("PUT", "/v1311b/stolen.txt", body=b"x", ak=tak, sk=task_sk,
                      extra={"X-Amz-Copy-Source": "/v1311/secret.txt"})
    check(st == 403, "跨租户 CopyObject → 403（copy-source 源桶校验）")
    st, _, data = req("POST", "/v1311b/mp.bin", query="uploads", body=b"", ak=tak, sk=task_sk)
    uid = extract_upload_id(data)
    st, _, data = req("PUT", "/v1311b/mp.bin", query="partNumber=1&uploadId=%s" % uid,
                      ak=tak, sk=task_sk, extra={"X-Amz-Copy-Source": "/v1311/secret.txt"})
    check(st == 403, "跨租户 UploadPartCopy → 403")
    req("DELETE", "/v1311b/mp.bin", query="uploadId=%s" % uid, ak=tak, sk=task_sk)

    # ⑨ S3 桶 ACL 回归：ta1311 不能直读 glacier 桶
    st, _, _ = req("GET", "/v1311/secret.txt", ak=tak, sk=task_sk)
    check(st == 403, "跨租户 GET → 403（回归）")

    tc.close()


# ---------------------------------------------------------------------------
# Part 6: 时序大表（谓词声明 + 单位自动换算 + 冷/rollup）
# ---------------------------------------------------------------------------

def part_timeseries(cur):
    print("---- Part 6: 时序大表谓词 + ts 单位自动换算 ----")
    cur.execute("CREATE DATABASE IF NOT EXISTS ts1311")
    cur.execute("USE ts1311")
    cur.execute("DROP TABLE IF EXISTS m")
    cur.execute("CREATE TABLE m (series_id INTEGER, ts INTEGER, val REAL, PRIMARY KEY(series_id, ts)) "
                "WITH (TIMESERIES, TS_UNIT='s', TS_COLUMN='ts')")
    check(True, "CREATE TABLE ... WITH (TIMESERIES, TS_UNIT='s') 建表")

    cur.execute("SHOW TS TABLES")
    rows = cur.fetchall()
    check(rows and any(r[1] == "m" and r[3] == "s" for r in rows), "SHOW TS TABLES 声明可见")

    # 非法谓词
    cur.execute("DROP TABLE IF EXISTS bad1")
    try:
        cur.execute("CREATE TABLE bad1 (ts INTEGER) WITH (TIMESERIES, TS_UNIT='hours')")
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] == 1064
    check(ok, "非法 TS_UNIT → 1064")
    cur.execute("DROP TABLE IF EXISTS bad2")
    try:
        cur.execute("CREATE TABLE bad2 (ts INTEGER) WITH (TIMESERIES, FOO=1)")
        ok = False
    except (pymysql.err.OperationalError, pymysql.err.ProgrammingError) as e:
        ok = e.args[0] == 1064
    check(ok, "未知 WITH 选项 → 1064")

    # 存秒：以「当前时刻往前 3 分钟」为中心布点，跨 3 个分钟桶
    now_s = int(time.time())
    points = []
    for k in range(180):
        ts = now_s - 7200 + k * 60  # 3h span (COLD step supports DAY/HOUR)
        points.append((1, ts, float(k)))
        points.append((2, ts, float(k) * 2))
    for p in points:
        cur.execute("INSERT INTO m VALUES (%d,%d,%s)" % p)

    # 存秒查毫秒：13 位毫秒字面量自动 ÷1000
    lo_ms, hi_ms = (now_s - 120) * 1000, (now_s - 60) * 1000
    cur.execute("SELECT COUNT(*) FROM m WHERE ts >= %d AND ts <= %d" % (lo_ms, hi_ms))
    n_ms = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM m WHERE ts >= %d AND ts <= %d" % (now_s - 120, now_s - 60))
    n_s = cur.fetchone()[0]
    check(n_ms == n_s and n_s > 0, "存秒查毫秒 == 存秒查秒（%d == %d）" % (n_ms, n_s))

    # 日期串自动换算
    cur.execute("SELECT COUNT(*) FROM m WHERE ts >= '%s'" % time.strftime("%Y-%m-%d", time.gmtime()))
    check(cur.fetchone()[0] == len(points), "日期串谓词换算（全量命中）")

    # 桶宽自动换算：GROUP BY ts/60000*60000（ms 惯例）在 s 表 → ts/60*60
    cur.execute("SELECT COUNT(*) FROM (SELECT series_id, COUNT(val) FROM m "
                "WHERE ts >= %d AND ts <= %d GROUP BY series_id, (ts/60000*60000)) " % (lo_ms, hi_ms))
    buckets_ms = cur.fetchone()[0]
    check(buckets_ms >= 2, "桶宽 ms→s 换算后聚合可用（%d 桶）" % buckets_ms)

    # rollup 路由（声明表）：路由与非路由结果一致
    q_roll = ("SELECT series_id, COUNT(val), SUM(val), MIN(val), MAX(val) FROM m "
              "WHERE ts >= %d AND ts <= %d GROUP BY series_id, (ts/60000*60000)" % (lo_ms, hi_ms))
    cur.execute(q_roll)
    rolled = sorted(cur.fetchall())
    admin_sql(cur, "REBUILD ROLLUP ts1311.m")
    cur.execute("USE ts1311")
    cur.execute(q_roll)
    rolled2 = sorted(cur.fetchall())
    check(rolled == rolled2 and len(rolled) > 0, "rollup mismatch d=%r r=%r" % (rolled[:6], rolled2[:6]))

    # 冷压缩（声明表 + 秒精度边界换算）
    boundary_ms = (now_s - 60) * 1000
    cur.execute("COMPRESS COLD m BEFORE %d STEP 1 HOUR" % boundary_ms)
    stats = cur.fetchall()
    check(stats and stats[0][1] >= 1, "COMPRESS COLD 声明表（chunks=%s）" % (stats[0][1] if stats else "?",))
    cur.execute("USE ts1311")
    cur.execute("SHOW COLD")
    rows = cur.fetchall()
    check(rows and any(r[0] == "m" for r in rows), "SHOW COLD 列出声明表 m")
    cur.execute("USE ts1311")
    cur.execute("USE ts1311")
    cur.execute("SELECT COUNT(*) FROM m WHERE ts >= %d" % (now_s - 3600))
    n_after = cur.fetchone()[0]
    check(n_after < len(points), "hot rows after compress (%d < %d)" % (n_after, len(points)))
    # 触及冻结区间的查询自动解冻（秒字面量）
    cur.execute("SELECT COUNT(*) FROM m WHERE ts >= %d AND ts <= %d" % (now_s - 120, now_s - 118))
    check(cur.fetchone()[0] > 0, "冻结区间查询自动解冻（存秒口径）")
    cur.execute("SELECT COUNT(*) FROM m")
    check(cur.fetchone()[0] == len(points), "全解冻后行数复原")

    # 微秒表冒烟
    cur.execute("DROP TABLE IF EXISTS us_t")
    cur.execute("CREATE TABLE us_t (series_id INTEGER, ts INTEGER, val REAL) WITH (TIMESERIES, TS_UNIT='us')")
    now_us = int(time.time() * 1000000)
    for k in range(10):
        cur.execute("INSERT INTO us_t VALUES (1,%d,%f)" % (now_us - k * 1000000, float(k)))
    cur.execute("SELECT COUNT(*) FROM us_t WHERE ts >= %d" % ((now_s - 30) * 1000))
    check(cur.fetchone()[0] == 10, "us 表：毫秒字面量查询自动 ×1000")
    cur.execute("USE backup")


def main():
    import _strptime  # noqa: F401
    conn0 = conn()
    cur = conn0.cursor()
    cur.execute("USE GlacierTDB")
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    global AK, SK
    AK, SK = cur.fetchall()[0][0:2]
    cur.execute("USE backup")

    try:
        part_storage(cur)
    except Exception as e:
        print("[FAIL] Part1 exception: %r" % e)
        STATS["fail"] += 1
    try:
        part_blend(cur)
    except Exception as e:
        print("[FAIL] Part2 exception: %r" % e)
        STATS["fail"] += 1
    try:
        part_admin_tasks(cur)
    except Exception as e:
        print("[FAIL] Part3 exception: %r" % e)
        STATS["fail"] += 1
    try:
        part_vacuum(cur)
    except Exception as e:
        print("[FAIL] Part4 exception: %r" % e)
        STATS["fail"] += 1
    try:
        part_security(cur)
    except Exception as e:
        print("[FAIL] Part5 exception: %r" % e)
        STATS["fail"] += 1
    try:
        part_timeseries(cur)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("[FAIL] Part6 exception: %r" % e)
        STATS["fail"] += 1

    print("\n==== v1.3.11 RESULT: pass=%d fail=%d ====" % (STATS["pass"], STATS["fail"]))
    if STATS["fail"]:
        sys.exit(1)
    conn0.close()


if __name__ == "__main__":
    main()
