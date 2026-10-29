#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.14 黑盒测试（权限体系与 S3 能力补全）：

1) 桶共享（§1）：GRANT/REVOKE S3 · ro 读放过写拒 / rw 全权 · ListBuckets 含共享桶
   （SharedPerm 标注）· SHOW S3 GRANTS · 操作者维度记账 SHOW S3 USAGE OPERATORS
2) AK scope（§2）：BUCKETS 限定（含 sc* 通配）/ PREFIXES 收窄（对象 + LIST 前缀）/READONLY
   · CREATE 带范围 / ALTER SET SCOPE / SHOW S3 KEYS scope 列 · 403 细分原因
3) move / copy（§3）：x-glacier-move 原子移动（同桶改名 / 跨桶 / versioning 全版本随迁
   不留 delete marker / WORM 桶内允许跨桶拒绝 / 元数据随行）· CopyObject 零拷贝往返
   · SUBMIT S3 COPY 异步批量（DRY RUN / 前缀映射 / 结果 FETCH）
4) 魔法 Token（§4）：GET/PUT/GIMG 三类动作 · ?token= 免签名直访 · 对象绑定不可跨用
   · EXPIRE ≤1h 强制 · REVOKE 吊销 · 过期失效 · gimg 参数钉死注入 · 对象删除级联失效
"""
import http.client
import io
import json
import sys
import time

import pymysql

sys.path.insert(0, "/tests")
from test_s3 import s3_request, sql_connect, sign_request  # noqa: E402

HOST, PORT = "proxy", 3309
S3_HOST, S3_PORT = "proxy", 9000
ADMIN, ADMIN_PW = "glacier", "iloveyou~"
AK, SK = None, None
T = "t%d" % int(time.time())
STATS = {"pass": 0, "fail": 0}


def check(cond, msg):
    if cond:
        STATS["pass"] += 1
        print("[PASS] %s" % msg)
    else:
        STATS["fail"] += 1
        print("[FAIL] %s" % msg)


def req(method, path, query="", body=b"", extra=None, ak=None, sk=None):
    a, s = ak or AK, sk or SK
    return s3_request(method, path, query=query, headers=extra or {}, body=body, ak=a, sk=s)


def vreq(method, bucket, query, ak=None, sk=None):
    """带查询串的桶级请求（?versions 等——签名按 query 参数规范化）。"""
    return req(method, "/%s/" % bucket, query=query, ak=ak, sk=sk)


def raw_req(method, path, query="", body=b"", ctype="application/octet-stream"):
    """无签名裸请求（魔法 token 直访路径——前端无 AK/SK）。"""
    conn = http.client.HTTPConnection(S3_HOST, S3_PORT, timeout=30)
    url = path + (("?" + query) if query else "")
    hdrs = {"Content-Type": ctype} if body else {}
    if body:
        hdrs["Content-Length"] = str(len(body))
    conn.request(method, url, body=body, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    out = (resp.status, dict((k.lower(), v) for k, v in resp.getheaders()), data)
    conn.close()
    return out


def admin_sql(cur, sql):
    cur.execute("USE GlacierTDB")
    cur.execute(sql)
    try:
        return cur.fetchall()
    except Exception:
        return None


def wait_job(cur, jid, timeout=60):
    for _ in range(timeout * 2):
        cur.execute("SELECT state, error FROM query_jobs WHERE job_id = '%s'" % jid)
        r = cur.fetchone()
        if r and r[0] not in ("PENDING", "RUNNING"):
            return r[0], str(r[1] or "")
        time.sleep(0.5)
    return "TIMEOUT", ""


def tiny_jpeg(w=32, h=32):
    """Pillow 生成的小 JPEG——GIMG 管道夹具（比手搓段更可靠）。"""
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (w, h), (200, 30, 30)).save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def main() -> int:
    global AK, SK
    cur = sql_connect().cursor()
    cur.execute("USE GlacierTDB")

    # ---- 账号与密钥准备：u1（桶属主）/ u2（被共享方）/ glacier（super）----
    for u, p in (("u1s14", "u1pass-hk"), ("u2s14", "u2pass-hk")):
        try:
            cur.execute("DROP USER '%s'" % u)
        except pymysql.MySQLError:
            pass
        cur.execute("CREATE USER '%s'@'%%' IDENTIFIED BY '%s'" % (u, p))
    cur.execute("CREATE S3 KEY FOR 'glacier'")
    AK, SK = cur.fetchall()[0][0:2]
    cur.execute("CREATE S3 KEY FOR 'u1s14'")
    u1ak, u1sk = cur.fetchall()[0][0:2]
    cur.execute("CREATE S3 KEY FOR 'u2s14'")
    u2ak, u2sk = cur.fetchall()[0][0:2]

    b1, b2 = T + "-g1", T + "-g2"
    cur.execute("CREATE BUCKET %s" % b1)
    cur.execute("CREATE BUCKET %s" % b2)
    cur.execute("ALTER BUCKET %s OWNER 'u1s14'" % b1)  # 共享场景：b1 属主 = u1
    st, _, _ = req("PUT", "/%s/shared.txt" % b1, body=b"hello-shared-1", ak=u1ak, sk=u1sk,
                   extra={"Content-Type": "text/plain"})
    check(st == 200, "u1 属主上传基线对象（%d）" % st)

    # ==================================================================
    # 1) 桶共享（§1）
    # ==================================================================
    # 1.1 未共享：u2 对 u1 桶完全不可见
    st, hd, _ = req("GET", "/%s/shared.txt" % b1, ak=u2ak, sk=u2sk)
    check(st == 403, "u2 未共享时 GET 403（%d）" % st)
    st, hd, body = req("GET", "/", ak=u2ak, sk=u2sk)
    check(("<Name>%s</Name>" % b1).encode() not in body, "u2 ListBuckets 未共享不含 b1")

    # 1.2 GRANT READONLY：读过写拒
    cur.execute("GRANT S3 %s TO u2s14 READONLY" % b1)
    st, hd, body = req("GET", "/%s/shared.txt" % b1, ak=u2ak, sk=u2sk)
    check(st == 200 and body == b"hello-shared-1", "ro 共享后 u2 GET 200 内容一致（%d）" % st)
    st, hd, body = req("PUT", "/%s/u2write.txt" % b1, body=b"x", ak=u2ak, sk=u2sk)
    check(st == 403 and b"read-only" in body, "ro 共享 u2 PUT 403 且细分原因（%d %s）" % (st, body[:60]))
    st, hd, body = req("GET", "/", ak=u2ak, sk=u2sk)
    check(("<Name>%s</Name>" % b1).encode() in body and b"<SharedPerm>ro</SharedPerm>" in body,
          "u2 ListBuckets 含共享桶 + SharedPerm=ro 标注")
    st, hd, _ = req("GET", "/%s/" % b1, ak=u2ak, sk=u2sk)  # LIST
    check(st == 200, "ro 共享 u2 LIST 200（%d）" % st)

    # 1.3 覆盖为 READWRITE：写放行
    cur.execute("GRANT S3 %s TO u2s14 READWRITE" % b1)
    st, hd, _ = req("PUT", "/%s/u2write.txt" % b1, body=b"by-u2", ak=u2ak, sk=u2sk)
    check(st == 200, "rw 共享 u2 PUT 200（%d）" % st)
    st, hd, _ = req("DELETE", "/%s/u2write.txt" % b1, ak=u2ak, sk=u2sk)
    check(st == 204, "rw 共享 u2 DELETE 204（%d）" % st)

    # 1.4 SHOW S3 GRANTS 两种过滤
    rows = admin_sql(cur, "SHOW S3 GRANTS FOR u2s14")
    hit = any(r[0] == b1 and r[2] == "rw" for r in (rows or []))
    check(hit, "SHOW S3 GRANTS FOR u2 命中（%s）" % [(r[0], r[2]) for r in (rows or [])])
    rows = admin_sql(cur, "SHOW S3 GRANTS FOR BUCKET %s" % b1)
    check(rows and rows[0][1] == "u2s14", "SHOW S3 GRANTS FOR BUCKET 命中")

    # 1.5 操作者维度记账（OpUsage 读视图合并内存缓冲，无需等落库）
    req("GET", "/%s/shared.txt" % b1, ak=u2ak, sk=u2sk)
    rows = admin_sql(cur, "SHOW S3 USAGE OPERATORS")
    hit = any(r[0] == "u2s14" and r[1] == b1 and (r[2] or 0) >= 1 for r in (rows or []))
    check(hit, "SHOW S3 USAGE OPERATORS 记 u2 的 GET（%s）" %
          [(r[0], r[1], r[2], r[3]) for r in (rows or [])][:4])

    # 1.6 REVOKE：立即拒绝 + ListBuckets 消失
    cur.execute("REVOKE S3 %s FROM u2s14" % b1)
    st, hd, _ = req("GET", "/%s/shared.txt" % b1, ak=u2ak, sk=u2sk)
    check(st == 403, "REVOKE 后 u2 GET 403（%d）" % st)
    st, hd, body = req("GET", "/", ak=u2ak, sk=u2sk)
    check(("<Name>%s</Name>" % b1).encode() not in body, "REVOKE 后 ListBuckets 不含 b1")
    # 属主恢复可见
    st, hd, body = req("GET", "/%s/shared.txt" % b1, ak=u1ak, sk=u1sk)
    check(st == 200, "属主 u1 始终可读（%d）" % st)

    # ==================================================================
    # 2) AK scope（§2）—— scope 桶属主 = u1s14（scope 钥匙都是 u1 的）
    # ==================================================================
    sb = T + "-s"  # 命中下方 BUCKETS('t*-s*') 通配用例
    cur.execute("CREATE BUCKET %s" % sb)
    cur.execute("ALTER BUCKET %s OWNER 'u1s14'" % sb)
    req("PUT", "/%s/report/r1.txt" % sb, body=b"report-1", ak=u1ak, sk=u1sk, extra={"Content-Type": "text/plain"})
    req("PUT", "/%s/other/o1.txt" % sb, body=b"other-1", ak=u1ak, sk=u1sk, extra={"Content-Type": "text/plain"})

    # 2.1 建桶级只读范围钥匙
    cur.execute("CREATE S3 KEY FOR 'u1s14' BUCKETS ('%s') READONLY" % sb)
    sk_ak, sk_sk = cur.fetchall()[0][0:2]
    st, hd, body = req("GET", "/%s/report/r1.txt" % sb, ak=sk_ak, sk=sk_sk)
    check(st == 200 and body == b"report-1", "scope 钥匙范围内 GET 200（%d）" % st)
    st, hd, body = req("PUT", "/%s/report/new.txt" % sb, body=b"x", ak=sk_ak, sk=sk_sk)
    check(st == 403 and b"read-only" in body, "READONLY 钥匙写 403 方法越权细分（%s）" % body[:70])
    st, hd, body = req("GET", "/%s/shared.txt" % b1, ak=sk_ak, sk=sk_sk)
    check(st == 403 and b"does not allow bucket" in body, "桶越权 403 细分（%s）" % body[:70])

    # 2.2 ALTER 放开写
    cur.execute("ALTER S3 KEY %s SET SCOPE BUCKETS ('%s') READWRITE" % (sk_ak, sb))
    st, hd, _ = req("PUT", "/%s/report/new.txt" % sb, body=b"written", ak=sk_ak, sk=sk_sk)
    check(st == 200, "ALTER READWRITE 后 PUT 200（%d）" % st)

    # 2.3 前缀收窄（对象 + LIST）
    cur.execute("CREATE S3 KEY FOR 'u1s14' BUCKETS ('%s') PREFIXES ('report/')" % sb)
    pk_ak, pk_sk = cur.fetchall()[0][0:2]
    st, hd, body = req("GET", "/%s/report/r1.txt" % sb, ak=pk_ak, sk=pk_sk)
    check(st == 200, "前缀内对象 GET 200（%d）" % st)
    st, hd, body = req("GET", "/%s/other/o1.txt" % sb, ak=pk_ak, sk=pk_sk)
    check(st == 403 and b"outside key scope prefixes" in body, "前缀外对象 403 细分（%s）" % body[:70])
    st, hd, _ = req("GET", "/%s/" % sb, query="", ak=pk_ak, sk=pk_sk)  # LIST 无 prefix
    check(st == 403, "LIST 无 prefix 越出 scope 403（%d）" % st)
    st, hd, body = req("GET", "/%s/" % sb, query="prefix=report/", ak=pk_ak, sk=pk_sk)
    check(st == 200 and b"report/r1.txt" in body, "LIST prefix 在 scope 内 200（%d）" % st)

    # 2.4 桶通配
    cur.execute("CREATE S3 KEY FOR 'u1s14' BUCKETS ('%s-s*')" % T)
    wk_ak, wk_sk = cur.fetchall()[0][0:2]
    st, _, _ = req("GET", "/%s/report/r1.txt" % sb, ak=wk_ak, sk=wk_sk)
    check(st == 200, "桶通配 sc* 命中（%d）" % st)

    # 2.5 SHOW S3 KEYS scope 摘要 + CLEAR 恢复全量
    rows = admin_sql(cur, "SHOW S3 KEYS FOR 'u1s14'")
    scopes = [str(r[4]) for r in (rows or [])]
    check(any("buckets[" in s for s in scopes) and any("prefixes[" in s for s in scopes),
          "SHOW S3 KEYS scope 摘要可见（u1 共 %d 把，样例 %s）" % (len(scopes), scopes[:2]))
    cur.execute("ALTER S3 KEY %s SET SCOPE CLEAR" % pk_ak)
    st, _, _ = req("GET", "/%s/other/o1.txt" % sb, ak=pk_ak, sk=pk_sk)
    check(st == 200, "SCOPE CLEAR 恢复全量（%d）" % st)

    # ==================================================================
    # 3) move / copy（§3）
    # ==================================================================
    mb = T + "-mv"
    cur.execute("CREATE BUCKET %s" % mb)
    req("PUT", "/%s/orig.txt" % mb, body=b"move-me-123", extra={"Content-Type": "text/plain", "x-amz-meta-tag": "keep"})
    # 3.1 同桶改名（含元数据随行）
    st, hd, _ = req("PUT", "/%s/renamed.txt" % mb, extra={"X-Amz-Copy-Source": "/" + mb + "/orig.txt", "X-Glacier-Move": "true"})
    check(st == 200, "同桶 move 200（%d）" % st)
    st, hd, body = req("GET", "/%s/renamed.txt" % mb)
    check(st == 200 and body == b"move-me-123", "move 后新 key 内容一致（%d）" % st)
    st, hd, _ = req("HEAD", "/%s/renamed.txt" % mb)
    check(hd.get("x-amz-meta-tag") == "keep", "move 元数据随行（%s）" % hd.get("x-amz-meta-tag"))
    st, hd, _ = req("GET", "/%s/orig.txt" % mb)
    check(st == 404, "move 后源 key 404（%d）" % st)

    # 3.2 跨桶 move
    st, hd, _ = req("PUT", "/%s/moved.txt" % b2, extra={"X-Amz-Copy-Source": "/" + mb + "/renamed.txt", "X-Glacier-Move": "true"})
    check(st == 200, "跨桶 move 200（%d）" % st)
    st, hd, body = req("GET", "/%s/moved.txt" % b2)
    check(st == 200 and body == b"move-me-123", "跨桶 move 内容一致（%d）" % st)
    st, _, _ = req("GET", "/%s/renamed.txt" % mb)
    check(st == 404, "跨桶 move 源 404（%d）" % st)

    # 3.3 versioning 桶：全版本随迁不留 delete marker
    vb = T + "-ver"
    cur.execute("CREATE BUCKET %s VERSIONING ON" % vb)
    req("PUT", "/%s/vk" % vb, body=b"v-one")
    req("PUT", "/%s/vk" % vb, body=b"v-two")
    st, hd, body = vreq("GET", vb, "versions")
    n_before = body.count(b"<Version>")
    check(n_before == 2, "versioning 两版本就位（%d）" % n_before)
    st, hd, _ = req("PUT", "/%s/vk2" % vb, extra={"X-Amz-Copy-Source": "/" + vb + "/vk", "X-Glacier-Move": "true"})
    check(st == 200, "versioning move 200（%d）" % st)
    st, hd, body = req("GET", "/%s/vk2" % vb)
    check(st == 200 and body == b"v-two", "versioning move 后 latest 内容一致（%d %r）" % (st, body[:20]))
    st, hd, body = vreq("GET", vb, "versions")
    check(body.count(b"<Version>") == 2 and
          body.count(b"<Key>vk2</Key>") == 2 and b"DeleteMarker" not in body,
          "版本全数随迁至新 key 且无 delete marker（versions=%d vk2=%d）" %
          (body.count(b"<Version>"), body.count(b"<Key>vk2</Key>")))
    st, _, _ = req("GET", "/%s/vk" % vb)
    check(st == 404, "versioning 源 key 整体消失（%d）" % st)

    # 3.4 WORM：桶内允许 / 跨桶拒绝
    wb = T + "-worm"
    cur.execute("CREATE BUCKET %s OBJECT_LOCK ON" % wb)
    req("PUT", "/%s/locked.txt" % wb, body=b"locked")
    cur.execute("S3 RETAIN %s/locked.txt UNTIL '2030-01-01 00:00:00' COMPLIANCE" % wb)
    st, hd, _ = req("PUT", "/%s/locked2.txt" % wb, extra={"X-Amz-Copy-Source": "/" + wb + "/locked.txt", "X-Glacier-Move": "true"})
    check(st == 200, "WORM 桶内改名允许（%d）" % st)
    st, hd, body = req("PUT", "/%s/esc.txt" % b2, extra={"X-Amz-Copy-Source": "/" + wb + "/locked2.txt", "X-Glacier-Move": "true"})
    check(st == 403 and b"retention" in body, "WORM 跨桶 move 403（%d %s）" % (st, body[:60]))

    # 3.5 CopyObject 零拷贝往返（10MB 毫秒级 + 内容一致）
    big = bytes(range(256)) * 40960  # 10MB
    t0 = time.time()
    st, hd, _ = req("PUT", "/%s/big-src" % b2, body=big)
    up_ms = (time.time() - t0) * 1000
    t0 = time.time()
    st, hd, _ = req("PUT", "/%s/big-dst" % b2, extra={"X-Amz-Copy-Source": "/" + b2 + "/big-src"})
    copy_ms = (time.time() - t0) * 1000
    check(st == 200 and copy_ms < 1500, "10MB CopyObject 零拷贝毫秒级（copy=%.0fms vs put=%.0fms）" % (copy_ms, up_ms))
    st, hd, body = req("GET", "/%s/big-dst" % b2)
    check(st == 200 and body == big, "零拷贝副本内容逐字节一致（%d）" % st)

    # 3.6 SUBMIT S3 COPY：DRY RUN → 实拷（前缀映射）
    cb1, cb2 = T + "-c1", T + "-c2"
    cur.execute("CREATE BUCKET %s" % cb1)
    cur.execute("CREATE BUCKET %s" % cb2)
    for i in range(3):
        req("PUT", "/%s/p/f%d.txt" % (cb1, i), body=b"copy-body-%d" % i)
    cur.execute("SUBMIT S3 COPY %s PREFIX 'p/' TO %s PREFIX 'q/' DRY RUN RETAIN 10m" % (cb1, cb2))
    jid = cur.fetchall()[0][0]
    state, err = wait_job(cur, jid)
    check(state == "DONE", "SUBMIT S3 COPY DRY RUN DONE（%s %s）" % (state, err[:60]))
    cur.execute("FETCH QUERY RESULT '%s'" % jid)
    rows = cur.fetchall()
    check(len(rows) == 3 and all(r[-1] == "dry-run" for r in rows), "DRY RUN 三行不落对象（%s）" % [r[-1] for r in rows])
    cur.execute("SUBMIT S3 COPY %s PREFIX 'p/' TO %s PREFIX 'q/' RETAIN 10m" % (cb1, cb2))
    jid = cur.fetchall()[0][0]
    state, err = wait_job(cur, jid)
    check(state == "DONE", "SUBMIT S3 COPY DONE（%s %s）" % (state, err[:60]))
    ok = True
    for i in range(3):
        st, _, body = req("GET", "/%s/q/f%d.txt" % (cb2, i))
        if st != 200 or body != b"copy-body-%d" % i:
            ok = False
    check(ok, "批量副本内容逐个一致（前缀 p/→q/ 映射）")

    # ==================================================================
    # 4) 魔法 Token（§4）
    # ==================================================================
    tkb = T + "-tk"
    cur.execute("CREATE BUCKET %s" % tkb)
    req("PUT", "/%s/doc.txt" % tkb, body=b"token-doc-1", extra={"Content-Type": "text/plain"})

    # 4.1 GET token 免签名直访
    cur.execute("CREATE S3 TOKEN ON %s/doc.txt GET EXPIRE 120" % tkb)
    tok = cur.fetchall()[0][0]
    check(len(tok) >= 32, "token uuid 签发（%s…）" % tok[:12])
    st, hd, body = raw_req("GET", "/%s/doc.txt" % tkb, query="token=" + tok)
    check(st == 200 and body == b"token-doc-1", "?token= GET 直访成功（%d）" % st)

    # 4.2 token 对象绑定（跨对象拒绝）
    st, hd, body = raw_req("GET", "/%s/big-dst" % tkb, query="token=" + tok)  # 不存在的另一对象
    check(st == 403, "token 跨对象使用 403（%d）" % st)

    # 4.3 动作钉死：GET token 不能 PUT
    st, hd, body = raw_req("PUT", "/%s/doc.txt" % tkb, query="token=" + tok, body=b"evil")
    check(st == 403, "GET token PUT 403 动作不匹配（%d）" % st)

    # 4.4 PUT token 上传直传
    cur.execute("CREATE S3 TOKEN ON %s/upload.bin PUT EXPIRE 300" % tkb)
    ptok = cur.fetchall()[0][0]
    st, hd, body = raw_req("PUT", "/%s/upload.bin" % tkb, query="token=" + ptok, body=b"uploaded-via-token")
    check(st == 200, "PUT token 直传 200（%d）" % st)
    st, hd, body = raw_req("GET", "/%s/upload.bin" % tkb, query="token=" + ptok)
    check(st == 403, "PUT token 无 GET 动作 403（%d）" % st)

    # 4.5 REVOKE 吊销
    cur.execute("REVOKE S3 TOKEN '%s'" % tok)
    st, hd, body = raw_req("GET", "/%s/doc.txt" % tkb, query="token=" + tok)
    check(st == 403, "REVOKE 后 token 403（%d）" % st)

    # 4.6 过期上限与过期失效
    try:
        cur.execute("CREATE S3 TOKEN ON %s/doc.txt GET EXPIRE 9999" % tkb)
        over = "(no error)"
    except pymysql.MySQLError as e:
        over = str(e)
    check("exceeds max" in over or "3600" in over, "EXPIRE>3600 拒绝（%s）" % over[:60])
    cur.execute("CREATE S3 TOKEN ON %s/doc.txt GET EXPIRE 1" % tkb)
    etok = cur.fetchall()[0][0]
    time.sleep(2.5)
    st, hd, body = raw_req("GET", "/%s/doc.txt" % tkb, query="token=" + etok)
    check(st == 403, "过期 token 403（%d）" % st)

    # 4.7 GIMG 参数钉死
    jpg = tiny_jpeg()
    req("PUT", "/%s/img.jpg" % tkb, body=jpg, extra={"Content-Type": "image/jpeg"})
    cur.execute("CREATE S3 TOKEN ON %s/img.jpg GIMG(resize=8,8) EXPIRE 300" % tkb)
    gtok = cur.fetchall()[0][0]
    st, hd, body = raw_req("GET", "/%s/img.jpg" % tkb, query="token=" + gtok)
    check(st == 200 and len(body) != len(jpg) and hd.get("content-type", "").startswith("image/"),
          "GIMG token 注入钉死参数（%d B, %d→%d bytes, %s）" % (st, len(jpg), len(body), hd.get("content-type")))
    # GIMG token 裸读（无处理意图）：注入钉死参数后即处理形态，GIMG 动作放行——
    # 持有人拿不到原图（参数不可剥离，这正是钉死语义）
    st2, hd2, body2 = raw_req("GET", "/%s/img.jpg" % tkb, query="token=" + gtok)
    check(st2 == 200 and hd2.get("content-type", "").startswith("image/"),
          "GIMG token 裸读仍走处理管道（钉死不可剥离，%d）" % st2)
    # 无 GIMG 动作的 GET token 遇 x-gimg 请求参数 → 403（动作不匹配）
    cur.execute("CREATE S3 TOKEN ON %s/img.jpg GET EXPIRE 300" % tkb)
    ngtok = cur.fetchall()[0][0]
    st3, _, _ = raw_req("GET", "/%s/img.jpg" % tkb, query="token=" + ngtok + "&x-gimg-resize=8,8")
    check(st3 == 403, "GET token 带 x-gimg 参数 403（%d）" % st3)

    # 4.8 对象删除级联失效
    cur.execute("CREATE S3 TOKEN ON %s/upload.bin GET EXPIRE 300" % tkb)
    dtok = cur.fetchall()[0][0]
    req("DELETE", "/%s/upload.bin" % tkb)
    st, hd, body = raw_req("GET", "/%s/upload.bin" % tkb, query="token=" + dtok)
    check(st == 403 or st == 404, "对象删除后 token 失效/对象缺失（%d）" % st)

    # 4.9 SHOW S3 TOKENS
    rows = admin_sql(cur, "SHOW S3 TOKENS ON %s" % tkb)
    check(rows and any(r[3] == "GIMG" for r in rows), "SHOW S3 TOKENS 列出（%d 行）" % len(rows))

    # ---- 收尾清理 ----
    for b in (b1, b2, sb, mb, vb, wb, cb1, cb2, tkb):
        try:
            cur.execute("DROP BUCKET %s FORCE" % b)
        except pymysql.MySQLError:
            pass
    cur.execute("DROP USER 'u1s14'")
    cur.execute("DROP USER 'u2s14'")

    print("V1314: pass=%d fail=%d" % (STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
