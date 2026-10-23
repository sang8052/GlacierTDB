#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""v1.3.3 黑盒测试：
1) MySQL 8 兼容 M4：运算符（&& || ! XOR DIV RLIKE BINARY 十六进制 N''）/ 函数族
   （LOCATE/CHAR_LENGTH/REPEAT/REVERSE/STRCMP/FORMAT/MOD/FLOOR/CEIL/POW/TRUNCATE/
    DATE_ADD/DATE_SUB/EXTRACT/TIMESTAMPADD/TRIM/POSITION/SUBSTRING-FROM/GROUP_CONCAT
    SEPARATOR/CAST/CONVERT/UNIX_TIMESTAMP/日期族）/ ANALYZE-CHECK-OPTIMIZE-CHECKSUM
    TABLE / INSERT...SET / UPDATE-DELETE ORDER BY LIMIT / DDL（ENUM/SET/JSON/BIT/
    BLOB 族）/ ALTER ADD-DROP INDEX / MODIFY / CHANGE / RENAME TABLE / 用户变量
2) 列类型映射：DOUBLE/FLOAT TEXT 落盘（typeof='text'）/ 17 位浮点无损往返 /
   DECIMAL 精确文本 / 比较与排序语义 / 协议类型码 / SHOW COLUMNS-CREATE TABLE-
   information_schema 类型回放
3) 时序冷压缩：COMPRESS/THAW/SHOW COLD / 读查询自动解冻 / rollup 不重复计数
4) MQTT（tcp 嗅探 + mode:mqtt + WebSocket）：CONNECT/PUBLISH QoS0-1/SUBSCRIBE 推送/
   通配订阅/保留消息/AMQP 双向桥接/RabbitMQ 版本串/遗嘱
--check-restart：重启后冷块与 MQTT 保留消息仍在
"""
import os
import socket
import struct
import sys
import time

import pika
import pymysql
import redis

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


def mysql_conn(db=None):
    return pymysql.connect(host=HOST, port=PORT, user=USER, password=PW,
                           database=db, charset="utf8mb4", autocommit=True)


def q1(cur, sql):
    cur.execute(sql)
    r = cur.fetchone()
    return r[0] if r is not None else None


# ---------------------------------------------------------------------------
# MQTT 客户端（裸 socket，3.1.1）
# ---------------------------------------------------------------------------

def mqtt_remaining(n):
    out = b""
    while True:
        d = n % 128
        n //= 128
        if n:
            d |= 0x80
        out += bytes([d])
        if not n:
            return out


def mqtt_pkt(byte1, body):
    return bytes([byte1]) + mqtt_remaining(len(body)) + body


def mqtt_str(s):
    b = s.encode() if isinstance(s, str) else s
    return struct.pack(">H", len(b)) + b


class MQTTError(Exception):
    pass


class MQTT:
    def __init__(self, ws=False, ws_path="/mqtt", mode_line=False, timeout=10):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""
        self.ws = ws
        if mode_line:
            self.sock.sendall(b"mode:mqtt\r\n")
        if ws:
            key = "dGhlIHNhbXBsZSBub25jZQ=="  # 任意合法 base64
            req = ("GET %s HTTP/1.1\r\nHost: proxy\r\nUpgrade: websocket\r\n"
                   "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"
                   "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n") % (ws_path, key)
            self.sock.sendall(req.encode())
            # 读到 \r\n\r\n
            head = b""
            while b"\r\n\r\n" not in head:
                chunk = self.sock.recv(4096)
                if not chunk:
                    raise MQTTError("ws handshake closed")
                head += chunk
            self.http_head, rest = head.split(b"\r\n\r\n", 1)
            self.buf = rest
            if b"101" not in self.http_head.split(b"\r\n")[0]:
                raise MQTTError("ws handshake not 101: %r" % self.http_head[:80])

    def _raw_send(self, data):
        if not self.ws:
            self.sock.sendall(data)
            return
        # 客户端帧必须掩码
        mask = b"\x01\x02\x03\x04"
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
        n = len(data)
        if n < 126:
            hdr = bytes([0x82, 0x80 | n])
        elif n < 65536:
            hdr = bytes([0x82, 0x80 | 126]) + struct.pack(">H", n)
        else:
            hdr = bytes([0x82, 0x80 | 127]) + struct.pack(">Q", n)
        self.sock.sendall(hdr + mask + masked)

    def _raw_recv(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise MQTTError("connection closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def _ws_read_frame(self):
        hdr = self._raw_recv(2)
        ln = hdr[1] & 0x7F
        if ln == 126:
            ln = struct.unpack(">H", self._raw_recv(2))[0]
        elif ln == 127:
            ln = struct.unpack(">Q", self._raw_recv(8))[0]
        payload = self._raw_recv(ln) if ln else b""
        return hdr[0] & 0x0F, payload

    def read_packet(self):
        if self.ws:
            while True:
                op, payload = self._ws_read_frame()
                if op == 0x8:
                    raise MQTTError("ws close")
                if op in (0x1, 0x2, 0x0):
                    break
                # ping/pong 忽略
            if not payload:
                raise MQTTError("empty ws frame")
            first = payload[0]
            body = payload[1:]
        else:
            first = self._raw_recv(1)[0]
            rem, mult = 0, 1
            while True:
                b = self._raw_recv(1)[0]
                rem += (b & 0x7F) * mult
                mult *= 128
                if not b & 0x80:
                    break
            body = self._raw_recv(rem) if rem else b""
        return first >> 4, first & 0x0F, body

    def connect(self, client_id, user=USER, pw=PW, clean=True, keepalive=60, expect_code=0):
        flags = 0x02 if clean else 0
        if user:
            flags |= 0x80
        if pw is not None:
            flags |= 0x40
        body = mqtt_str("MQTT") + bytes([4, flags]) + struct.pack(">H", keepalive) + mqtt_str(client_id)
        if user:
            body += mqtt_str(user)
        if pw is not None:
            body += mqtt_str(pw)
        self._raw_send(mqtt_pkt(0x10, body))
        typ, _, resp = self.read_packet()
        code = resp[1] if len(resp) >= 2 else -1
        if expect_code is not None:
            if typ != 2 or code != expect_code:
                raise MQTTError("CONNACK type=%d code=%d (expect %d)" % (typ, code, expect_code))
        return code

    def publish(self, topic, payload, qos=0, retain=False, wait_ack=True):
        flags = (qos << 1) | (1 if retain else 0)
        body = mqtt_str(topic)
        pid = None
        if qos > 0:
            self._pid = getattr(self, "_pid", 0) + 1
            pid = self._pid
            body += struct.pack(">H", pid)
        body += payload.encode() if isinstance(payload, str) else payload
        self._raw_send(mqtt_pkt(0x30 | flags, body))
        if qos == 1 and wait_ack:
            typ, _, resp = self.read_packet()
            if typ != 4:
                raise MQTTError("expect PUBACK got %d" % typ)
        return pid

    def subscribe(self, filters, timeout=5):
        self._pid = getattr(self, "_pid", 0) + 1
        pid = self._pid
        body = struct.pack(">H", pid)
        for f, qos in filters:
            body += mqtt_str(f) + bytes([qos])
        self._raw_send(mqtt_pkt(0x82, body))
        typ, _, resp = self.read_packet()
        if typ != 9:
            raise MQTTError("expect SUBACK got %d" % typ)
        return list(resp[2:])

    def recv_publish(self, timeout=5):
        self.sock.settimeout(timeout)
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                typ, flags, body = self.read_packet()
            except socket.timeout:
                return None
            if typ != 3:
                continue
            qos = (flags >> 1) & 3
            tl = struct.unpack(">H", body[:2])[0]
            topic = body[2:2 + tl].decode()
            off = 2 + tl
            if qos > 0:
                off += 2
            return topic, body[off:]
        return None

    def ping(self):
        self._raw_send(mqtt_pkt(0xC0, b""))
        typ, _, _ = self.read_packet()
        return typ == 13

    def disconnect(self):
        try:
            self._raw_send(mqtt_pkt(0xE0, b""))
        except OSError:
            pass
        self.sock.close()


# ---------------------------------------------------------------------------
# Part 1：MySQL 8 兼容
# ---------------------------------------------------------------------------

def part_mysql_compat(cur):
    print("---- Part 1: MySQL 8 兼容 M4 ----")
    # 运算符
    check(q1(cur, "SELECT 1 && 1") == 1, "&& → AND")
    check(q1(cur, "SELECT 1 || 0") == 1, "|| → OR")
    check(q1(cur, "SELECT !0") == 1, "! → NOT")
    check(q1(cur, "SELECT 1 XOR 0") == 1, "XOR")
    check(q1(cur, "SELECT 7 DIV 2") == 3, "DIV 整除")
    check(q1(cur, "SELECT 'abc' RLIKE '^a'") == 1, "RLIKE → REGEXP")
    b = q1(cur, "SELECT BINARY 'a'")
    check(b == b"a" if isinstance(b, bytes) else b == "a", "BINARY 前缀")
    check(q1(cur, "SELECT 0x4D7953514C") in (b"MySQL", "MySQL"), "0x 十六进制字面量")
    check(q1(cur, "SELECT N'hi'") == "hi", "N'' 字符集引导符")
    # 字符串函数
    check(q1(cur, "SELECT LOCATE('bar', 'foobarbar', 5)") == 7, "LOCATE(substr,str,pos)")
    check(q1(cur, "SELECT CHAR_LENGTH('你好')") == 2, "CHAR_LENGTH 按字符")
    check(q1(cur, "SELECT REPEAT('ab', 3)") == "ababab", "REPEAT")
    check(q1(cur, "SELECT REVERSE('abc')") == "cba", "REVERSE")
    check(q1(cur, "SELECT SPACE(3)") == "   ", "SPACE")
    check(q1(cur, "SELECT STRCMP('a', 'b')") == -1, "STRCMP")
    check(q1(cur, "SELECT ASCII('A')") == 65, "ASCII")
    check(q1(cur, "SELECT FORMAT(1234567.891, 2)") == "1,234,567.89", "FORMAT 千分位")
    check(q1(cur, "SELECT TRIM(LEADING 'x' FROM 'xxabc')") == "abc", "TRIM(LEADING ... FROM)")
    check(q1(cur, "SELECT TRIM(BOTH 'x' FROM 'xabx')") == "ab", "TRIM(BOTH ... FROM)")
    check(q1(cur, "SELECT POSITION('b' IN 'abc')") == 2, "POSITION(x IN y)")
    check(q1(cur, "SELECT SUBSTRING('foobar' FROM 2 FOR 3)") == "oob", "SUBSTRING(x FROM n FOR m)")
    check(q1(cur, "SELECT MID('foobar', 2, 3)") == "oob", "MID")
    check(q1(cur, "SELECT ISNULL(NULL)") == 1, "ISNULL")
    # 数学
    check(q1(cur, "SELECT MOD(7, 3)") == 1, "MOD")
    check(q1(cur, "SELECT FLOOR(1.7)") == 1, "FLOOR")
    check(q1(cur, "SELECT CEIL(1.2)") == 2, "CEIL")
    check(q1(cur, "SELECT POW(2, 10)") == 1024.0, "POW")
    check(q1(cur, "SELECT SQRT(16)") == 4.0, "SQRT")
    check(q1(cur, "SELECT LOG2(8)") == 3.0, "LOG2")
    check(q1(cur, "SELECT TRUNCATE(1.999, 1)") == 1.9, "TRUNCATE")
    check(q1(cur, "SELECT SIGN(-5)") == -1, "SIGN")
    check(abs(q1(cur, "SELECT DEGREES(PI())") - 180.0) < 1e-9, "DEGREES/PI")
    # 日期族
    check(q1(cur, "SELECT YEAR('2026-10-23')") == 2026, "YEAR")
    check(q1(cur, "SELECT MONTH('2026-10-23')") == 10, "MONTH")
    check(q1(cur, "SELECT DAY('2026-10-23')") == 23, "DAY")
    check(q1(cur, "SELECT DAYOFWEEK('2026-10-23')") == 6, "DAYOFWEEK（周五=6）")
    check(q1(cur, "SELECT WEEKDAY('2026-10-23')") == 4, "WEEKDAY（周五=4）")
    check(q1(cur, "SELECT QUARTER('2026-02-01')") == 1, "QUARTER")
    check(q1(cur, "SELECT DAYNAME('2026-10-23')") == "Friday", "DAYNAME")
    check(q1(cur, "SELECT MONTHNAME('2026-03-01')") == "March", "MONTHNAME")
    check(q1(cur, "SELECT DAYOFYEAR('2026-01-02')") == 2, "DAYOFYEAR")
    check(q1(cur, "SELECT LAST_DAY('2026-02-10')") == "2026-02-28", "LAST_DAY")
    check(q1(cur, "SELECT TO_DAYS('1970-01-01')") == 719528, "TO_DAYS 锚点")
    check(q1(cur, "SELECT DATE_ADD('2026-01-31', INTERVAL 1 MONTH)") == "2026-02-28", "DATE_ADD 月末 clamp")
    check(q1(cur, "SELECT DATE_SUB('2026-01-01', INTERVAL 1 DAY)") == "2025-12-31", "DATE_SUB")
    check(q1(cur, "SELECT EXTRACT(YEAR FROM '2026-10-23')") == 2026, "EXTRACT(YEAR FROM)")
    check(q1(cur, "SELECT TIMESTAMPADD(DAY, 1, '2026-01-01')") == "2026-01-02", "TIMESTAMPADD")
    check(q1(cur, "SELECT SEC_TO_TIME(3661)") == "01:01:01", "SEC_TO_TIME")
    check(q1(cur, "SELECT TIME_TO_SEC('01:01:01')") == 3661, "TIME_TO_SEC")
    check(q1(cur, "SELECT UNIX_TIMESTAMP('1970-01-02 00:00:00')") == 86400, "UNIX_TIMESTAMP(x)")
    # CAST / CONVERT
    check(q1(cur, "SELECT CAST('12abc' AS SIGNED)") == 12, "CAST AS SIGNED")
    check(q1(cur, "SELECT CAST(3.7 AS CHAR)") == "3.7", "CAST AS CHAR")
    check(q1(cur, "SELECT CONVERT('12', SIGNED)") == 12, "CONVERT(x, SIGNED)")
    check(q1(cur, "SELECT CONVERT('x' USING utf8mb4)") == "x", "CONVERT USING")
    # DDL 兼容 + 类型回放
    cur.execute("DROP TABLE IF EXISTS t133")
    cur.execute("""CREATE TABLE t133 (
      id INT PRIMARY KEY AUTO_INCREMENT,
      v INT, s VARCHAR(50),
      st ENUM('a','b','c') DEFAULT 'a',
      tags SET('x','y'),
      doc JSON,
      flag BIT(1),
      blob_col MEDIUMBLOB,
      price DECIMAL(8,2),
      created DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='测试表'""")
    cur.execute("INSERT INTO t133(id, v, s, st, tags, doc, flag, blob_col, price) "
                "VALUES (1, 1, 'x', 'a', 'x', '{\"k\":1}', 1, 0xCAFEBABE, 12.34)")
    check(cur.rowcount == 1, "ENUM/SET/JSON/BIT/BLOB 建表与插入")
    cur.execute("SHOW COLUMNS FROM t133")
    types = {r[0]: r[1] for r in cur.fetchall()}
    check(types.get("st", "").upper().startswith("ENUM"), "SHOW COLUMNS 回放 ENUM 类型")
    check(types.get("price", "").upper().startswith("DECIMAL"), "SHOW COLUMNS 回放 DECIMAL")
    check(types.get("flag", "").upper().startswith("BIT"), "SHOW COLUMNS 回放 BIT")
    cur.execute("SHOW CREATE TABLE t133")
    ddl = cur.fetchone()[1]
    check("DECIMAL(8,2)" in ddl and "ENUM" in ddl, "SHOW CREATE TABLE 回放 MySQL 类型")
    cur.execute("SELECT DATA_TYPE FROM information_schema.COLUMNS "
                "WHERE TABLE_NAME='t133' AND COLUMN_NAME='price'")
    check(cur.fetchone()[0].lower() == "decimal", "information_schema 类型回放")
    # GROUP_CONCAT SEPARATOR
    cur.execute("SELECT GROUP_CONCAT(v SEPARATOR '|') FROM t133")
    check(cur.fetchone()[0] == "1", "GROUP_CONCAT SEPARATOR")
    # INSERT ... SET
    cur.execute("INSERT INTO t133 SET id=2, v=2, s='y'")
    check(cur.rowcount == 1, "INSERT ... SET 语法")
    # UPDATE/DELETE ORDER BY LIMIT
    cur.execute("UPDATE t133 SET v = v + 10 ORDER BY id DESC LIMIT 1")
    check(cur.rowcount == 1, "UPDATE ... ORDER BY ... LIMIT")
    cur.execute("SELECT v FROM t133 WHERE id=2")
    check(cur.fetchone()[0] == 12, "UPDATE LIMIT 命中末行")
    cur.execute("DELETE FROM t133 WHERE 1=1 ORDER BY id ASC LIMIT 1")
    check(cur.rowcount == 1, "DELETE ... ORDER BY ... LIMIT")
    cur.execute("SELECT COUNT(*) FROM t133")
    check(cur.fetchone()[0] == 1, "DELETE LIMIT 后余量")
    # ALTER 兼容
    cur.execute("ALTER TABLE t133 ADD INDEX idx_v (v)")
    cur.execute("ALTER TABLE t133 DROP INDEX idx_v")
    cur.execute("ALTER TABLE t133 ADD COLUMN c2 DECIMAL(8,2)")
    cur.execute("ALTER TABLE t133 MODIFY c2 BIGINT")
    cur.execute("ALTER TABLE t133 CHANGE c2 c3 INT")
    cur.execute("ALTER TABLE t133 ADD UNIQUE KEY uk_id (id)")
    check(True, "ALTER ADD/DROP INDEX、ADD/MODIFY/CHANGE COLUMN")
    cur.execute("RENAME TABLE t133 TO t133b")
    cur.execute("SELECT COUNT(*) FROM t133b")
    check(cur.fetchone()[0] == 1, "RENAME TABLE")
    # ANALYZE/CHECK/OPTIMIZE/CHECKSUM
    cur.execute("ANALYZE TABLE t133b")
    check(cur.fetchone()[3] == "OK", "ANALYZE TABLE 应答")
    cur.execute("CHECK TABLE t133b")
    check(cur.fetchone()[3] == "OK", "CHECK TABLE 应答")
    cur.execute("OPTIMIZE TABLE t133b")
    check(cur.fetchone()[3] == "OK", "OPTIMIZE TABLE 应答")
    cur.execute("CHECKSUM TABLE t133b")
    check(cur.fetchone()[0].endswith("t133b"), "CHECKSUM TABLE 应答")
    # 用户变量
    cur.execute("SET @v133a = 42")
    check(q1(cur, "SELECT @v133a") == 42, "用户变量 SET/SELECT 数值")
    cur.execute("SET @v133b = 'bob'")
    check(q1(cur, "SELECT @v133b") == "bob", "用户变量 SET/SELECT 字符串")
    cur.execute("DROP TABLE IF EXISTS t133b")


# ---------------------------------------------------------------------------
# Part 2：列类型映射
# ---------------------------------------------------------------------------

def part_type_mapping(cur):
    print("---- Part 2: 列类型映射（TEXT 落盘 + 还原）----")
    cur.execute("DROP TABLE IF EXISTS t_money")
    cur.execute("""CREATE TABLE t_money (
      id INT PRIMARY KEY AUTO_INCREMENT,
      money DOUBLE,
      rate FLOAT,
      price DECIMAL(20,6),
      tag VARCHAR(20)
    )""")
    v = 0.1 + 0.2  # 0.30000000000000004
    cur.execute("INSERT INTO t_money(id, money, rate, price, tag) VALUES (1, %s, 1.25, '123456789.123456', 'a')" % repr(v))
    check(True, "DOUBLE 插入 17 位有效数字")
    check(q1(cur, "SELECT typeof(money) FROM t_money WHERE id=1") == "text", "DOUBLE 底层 TEXT 落盘")
    got = q1(cur, "SELECT money FROM t_money WHERE id=1")
    check(got == v and isinstance(got, float), "DOUBLE 精确往返（%r）" % got)
    got2 = q1(cur, "SELECT price FROM t_money WHERE id=1")
    check(str(got2) == "123456789.123456", "DECIMAL 精确文本往返（%r）" % got2)
    # 比较语义
    check(q1(cur, "SELECT COUNT(*) FROM t_money WHERE money = 0.30000000000000004") == 1, "WHERE 等值比较（CAST）")
    check(q1(cur, "SELECT COUNT(*) FROM t_money WHERE money > 0.3") == 1, "WHERE 范围比较（CAST）")
    # 排序语义
    cur.execute("INSERT INTO t_money(id, money) VALUES (2, 9), (3, 10), (4, 100)")
    cur.execute("SELECT money FROM t_money WHERE id >= 2 ORDER BY money")
    order = [r[0] for r in cur.fetchall()]
    check(order == [9.0, 10.0, 100.0], "ORDER BY 数值语义（%r）" % order)
    # UPDATE 字面量加引号
    cur.execute("UPDATE t_money SET money = 2.5 WHERE id = 1")
    check(q1(cur, "SELECT typeof(money) FROM t_money WHERE id=1") == "text", "UPDATE 后仍 TEXT 落盘")
    check(q1(cur, "SELECT money FROM t_money WHERE id=1") == 2.5, "UPDATE 值还原")
    # 协议类型码
    cur.execute("SELECT money, price, tag FROM t_money WHERE id=1")
    descs = cur.description
    tmap = {d[0]: d[1] for d in descs}
    check(tmap.get("money") == 5, "结果集类型码 money=DOUBLE(5)（got %r）" % tmap.get("money"))
    check(tmap.get("price") == 246, "结果集类型码 price=NEWDECIMAL(246)（got %r）" % tmap.get("price"))
    # 反射回放
    cur.execute("SHOW COLUMNS FROM t_money")
    types = {r[0]: r[1] for r in cur.fetchall()}
    check(types.get("money", "").upper().startswith("DOUBLE"), "SHOW COLUMNS 回放 DOUBLE")
    check(types.get("price", "").upper().startswith("DECIMAL"), "SHOW COLUMNS 回放 DECIMAL")
    cur.execute("DROP TABLE t_money")


# ---------------------------------------------------------------------------
# Part 3：时序冷压缩
# ---------------------------------------------------------------------------

DAY_MS = 86400000


def part_cold(cur):
    print("---- Part 3: 时序冷压缩与固化 ----")
    cur.execute("DROP TABLE IF EXISTS points")
    cur.execute("CREATE TABLE points (series_id INTEGER NOT NULL, ts INTEGER NOT NULL, val REAL, PRIMARY KEY (series_id, ts))")
    now = int(time.time() * 1000)
    day0 = now // DAY_MS * DAY_MS
    rows = []
    for s in (1, 2, 3):
        for d in range(10):  # d0 = 10 天前 … d9 = 今天
            for i in range(50):
                rows.append((s, day0 - (10 - d) * DAY_MS + i * 60000, s * 100 + i * 0.5))
    cur.executemany("INSERT INTO points(series_id, ts, val) VALUES (%s, %s, %s)", rows)
    total = len(rows)
    check(q1(cur, "SELECT COUNT(*) FROM points") == total, "时序基线 %d 行" % total)
    check(q1(cur, "SELECT SUM(cnt) FROM points_m1") == total, "rollup 基线（懒建触发器）")
    # 压缩 5 天前（不含今天与最近 4 天）
    boundary = day0 - 5 * DAY_MS
    cur.execute("COMPRESS COLD points BEFORE %d" % boundary)
    r = cur.fetchone()
    check(r[1] > 0 and r[3] > 0, "COMPRESS COLD chunks=%s rows=%s" % (r[1], r[3]))
    ratio = float(r[6])
    check(ratio > 1.0, "压缩比 %.2f > 1" % ratio)
    cur.execute("SHOW COLD points")
    cold_rows = cur.fetchall()
    check(len(cold_rows) >= 5, "SHOW COLD 块数 %d" % len(cold_rows))
    # 冻结后查询：全量（自动解冻）
    check(q1(cur, "SELECT COUNT(*) FROM points WHERE ts < %d" % boundary) == 5 * 3 * 50, "冻结区间读查询自动解冻（计数不变）")
    # 重压缩 + 指定解冻
    cur.execute("COMPRESS COLD points BEFORE %d" % boundary)
    cur.execute("THAW COLD points ALL")
    r = cur.fetchone()
    check(r[1] > 0 and r[2] == 5 * 3 * 50, "THAW ALL 还原 %s 行" % r[2])
    check(q1(cur, "SELECT COUNT(*) FROM points") == total, "解冻后总量一致")
    # rollup 不重复计数
    check(q1(cur, "SELECT SUM(cnt) FROM points_m1") == total, "解冻后 rollup 不重复计数")
    # 再压缩留给重启持久化检查
    cur.execute("COMPRESS COLD points BEFORE %d" % boundary)
    check(q1(cur, "SELECT COUNT(*) FROM (SELECT 1 FROM sqlite_master WHERE name='points_cold')") == 1, "归档表在位（重启检查用）")


def part_cold_restart(cur):
    print("---- Part 3R: 冷压缩重启持久化 ----")
    cur.execute("SHOW COLD points")
    rows = cur.fetchall()
    check(len(rows) >= 5, "重启后冷块仍在（%d 块）" % len(rows))
    cur.execute("THAW COLD points ALL")
    cur.fetchone()
    check(q1(cur, "SELECT COUNT(*) FROM points") == 3 * 10 * 50, "重启后解冻总量一致")
    check(q1(cur, "SELECT SUM(cnt) FROM points_m1") == 3 * 10 * 50, "重启后 rollup 一致")
    cur.execute("DROP TABLE IF EXISTS points")


# ---------------------------------------------------------------------------
# Part 4：MQTT
# ---------------------------------------------------------------------------

def part_mqtt(cur):
    print("---- Part 4: MQTT（tcp/ws）接入 RabbitMQ ----")
    # 认证（错误密码用不存在的账号，避免防爆破计数误伤真实账号）
    bad = MQTT()
    code = bad.connect("bad", user="v133ghost", pw="wrong", expect_code=None)
    check(code == 4, "错误密码 CONNACK=4（got %s）" % code)
    bad.sock.close()

    # tcp 嗅探直连
    m = MQTT()
    m.connect("v133-a")
    check(m.ping(), "PINGREQ/PINGRESP")
    m.publish("v133/a/b", "hello", qos=0)
    check(True, "PUBLISH QoS0")
    pid = m.publish("v133/a/b", "hello1", qos=1)
    check(pid == 1, "PUBLISH QoS1 → PUBACK")

    # 订阅推送（第二连接）
    m2 = MQTT()
    m2.connect("v133-b")
    granted = m2.subscribe([("v133/+/b", 1)])
    check(granted == [1], "SUBACK 授予 QoS1（%r）" % granted)
    m.publish("v133/a/b", "push-me", qos=1)
    got = m2.recv_publish()
    check(got is not None and got[0] == "v133/a/b" and got[1] == b"push-me", "通配订阅 + 跨连接推送（%r）" % (got,))
    check(got is not None and m2.recv_publish(timeout=1) is None, "QoS1 推送仅一次（无重复）")

    # 保留消息
    m.publish("v133/retain/t", "keepme", qos=1, retain=True)
    m3 = MQTT()
    m3.connect("v133-c")
    m3.subscribe([("v133/retain/#", 0)])
    got = m3.recv_publish()
    check(got is not None and got[1] == b"keepme", "保留消息补发（%r）" % (got,))
    # AMQP → MQTT 反向桥
    m3.subscribe([("cmd/light", 0)])
    creds = pika.PlainCredentials(USER, PW)
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        host=HOST, port=PORT, virtual_host="/", credentials=creds,
        socket_timeout=10, heartbeat=0))
    ch = conn.channel()
    ch.queue_declare(queue="v133_bridge", durable=False, auto_delete=True)
    ch.queue_bind(queue="v133_bridge", exchange="mqtt", routing_key="v133.x.*")
    m.publish("v133/x/1", "mqtt->amqp", qos=1)
    method, _, body = ch.basic_get("v133_bridge", auto_ack=True)
    check(method is not None and body == b"mqtt->amqp", "MQTT → AMQP 桥（topic→rk）")
    # 反向：AMQP 发布 → MQTT 收
    ch.basic_publish(exchange="mqtt", routing_key="cmd.light", body=b"amqp->mqtt")
    got = m3.recv_publish()
    check(got is not None and got[0] == "cmd/light" and got[1] == b"amqp->mqtt", "AMQP → MQTT 桥（rk→topic）")
    # RabbitMQ 版本串（可配置）
    ver = None
    try:
        ver = (conn._impl.server_properties or {}).get("version")
    except AttributeError:
        try:
            ver = (conn._impl._server_properties or {}).get("version")
        except Exception:
            ver = None
    check(ver == "3.13.0", "RabbitMQ 版本串可配置（got %r）" % ver)
    conn.close()
    m.disconnect()
    m2.disconnect()
    m3.disconnect()

    # WebSocket 传输
    w = MQTT(ws=True)
    code = w.connect("v133-ws")
    check(code == 0, "WebSocket 升级 + CONNECT")
    check(w.ping(), "ws PINGREQ/PINGRESP")
    w.publish("v133/ws/t", "ws-ok", qos=0)
    w.disconnect()

    # mode:mqtt 声明行
    d = MQTT(mode_line=True)
    code = d.connect("v133-mode")
    check(code == 0, "mode:mqtt 声明行进入")
    d.disconnect()


# ---------------------------------------------------------------------------
# Part 5：BUG.txt 反馈修复（v1.3.3）
# ---------------------------------------------------------------------------

class RawResp:
    """极简 RESP2 裸客户端（HELLO/SCAN 探测用）。"""

    def __init__(self, timeout=10):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""

    def _readline(self):
        while b"\r\n" not in self.buf:
            d = self.sock.recv(65536)
            if not d:
                raise EOFError("closed")
            self.buf += d
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _readn(self, n):
        while len(self.buf) < n:
            d = self.sock.recv(65536)
            if not d:
                raise EOFError("closed")
            self.buf += d
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def read(self):
        line = self._readline()
        t, rest = line[:1], line[1:]
        if t in (b"+", b"-"):
            return rest.decode()
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n < 0:
                return None
            v = self._readn(n)
            self._readline()
            return v
        if t == b"*":
            n = int(rest)
            if n < 0:
                return None
            return [self.read() for _ in range(n)]
        raise ValueError("bad frame %r" % line)

    def cmd(self, *args):
        parts = [b"*%d" % len(args)]
        for a in args:
            b = a.encode() if isinstance(a, str) else a
            parts.append(b"$%d" % len(b))
            parts.append(b)
        self.sock.sendall(b"\r\n".join(parts) + b"\r\n")
        return self.read()

    def close(self):
        self.sock.close()


def part_bugfix(cur):
    print("---- Part 5: BUG.txt 反馈修复（HELLO/SCAN）----")
    # ① redis-py ≥8 默认 protocol=3（HELLO 3 AUTH default <pwd>）应可直连
    r8 = redis.Redis(host=HOST, port=3309, username="default", password=PW,
                     socket_timeout=10)  # 不传 protocol（BUG.txt ① 复现场景）
    check(r8.ping(), "redis-py 8.x 默认 protocol=3 直连（HELLO 应答降级）")
    r8.set("v133:bugfix:k1", "hello")
    check(r8.get("v133:bugfix:k1") == b"hello", "redis-py 8.x SET/GET")
    r8.delete("v133:bugfix:k1")
    r8.close()
    # protocol=2 显式路径回归
    r2 = redis.Redis(host=HOST, port=3309, password=PW, protocol=2, socket_timeout=10)
    check(r2.ping() and r2.set("v133:bugfix:k2", "x") and r2.get("v133:bugfix:k2") == b"x",
          "redis-py protocol=2 老路径不受影响")
    r2.delete("v133:bugfix:k2")
    r2.close()

    # ① 裸协议：HELLO 应答 map 形态 + proto=2；HELLO 9 → NOPROTO
    raw = RawResp()
    m = raw.cmd("HELLO", "3", "AUTH", "default", PW)
    asMap = dict(zip(m[::2], m[1::2])) if isinstance(m, list) else {}
    check(asMap.get(b"proto") == b"2" and asMap.get(b"server") == b"redis",
          "HELLO 3 应答 server map 且 proto=2（%r）" % asMap)
    check(raw.cmd("SET", "v133:hello:authed", "1") == "OK" and raw.cmd("GET", "v133:hello:authed") == b"1",
          "HELLO AUTH 后会话已认证可用")
    raw.cmd("DEL", "v133:hello:authed")
    raw2 = RawResp()
    resp = raw2.cmd("HELLO", "9")
    check(isinstance(resp, str) and "NOPROTO" in resp, "HELLO 9 → NOPROTO（%r）" % resp)
    raw.close()
    raw2.close()

    # ③ SCAN：游标恒 0 单批返回 + MATCH；redis-py scan_iter 可用
    r3 = redis.Redis(host=HOST, port=3309, password=PW, protocol=2, socket_timeout=10)
    for i in range(3):
        r3.set("v133:scan:%d" % i, "v")
    r3.rpush("v133:scan:list", "a")
    raw3 = RawResp()
    raw3.cmd("AUTH", USER, PW)
    got = raw3.cmd("SCAN", "0", "MATCH", "v133:scan:*")
    keys = sorted(k.decode() for k in got[1])
    check(got[0] == b"0" and len(keys) == 4, "SCAN 0 MATCH 单批返回（cursor=0, keys=%r）" % keys)
    got2 = raw3.cmd("SCAN", "0", "MATCH", "v133:scan:*", "TYPE", "string")
    keys2 = sorted(k.decode() for k in got2[1])
    check(len(keys2) == 3, "SCAN TYPE string 过滤（%r）" % keys2)
    raw3.close()
    it = sorted(k.decode() for k in r3.scan_iter("v133:scan:*"))
    check(len(it) == 4 and "v133:scan:list" in it, "redis-py scan_iter 可用（%r）" % it)
    for i in range(3):
        r3.delete("v133:scan:%d" % i)
    r3.delete("v133:scan:list")
    r3.close()


def part_mqtt_restart(cur):
    print("---- Part 4R: MQTT 保留消息重启持久化 ----")
    m = MQTT()
    m.connect("v133-r")
    m.subscribe([("v133/retain/#", 0)])
    got = m.recv_publish()
    check(got is not None and got[1] == b"keepme", "重启后保留消息仍补发（%r）" % (got,))
    m.disconnect()
    # 收尾：清掉保留消息与库（完整回归多次运行幂等）
    m2 = MQTT()
    m2.connect("v133-r2")
    m2.publish("v133/retain/t", "", qos=0, retain=True)  # 空保留 = 删除
    m2.disconnect()
    cur.execute("DROP DATABASE IF EXISTS v133db")


def main():
    restart_only = "--check-restart" in sys.argv
    root = mysql_conn()
    with root.cursor() as c:
        c.execute("CREATE DATABASE IF NOT EXISTS v133db")
    root.close()

    conn = mysql_conn("v133db")
    cur = conn.cursor()
    try:
        # 清防爆破锁（历次错误尝试可能累积），保证 MQTT 认证用例不受影响
        cur.execute("USE GlacierTDB")
        try:
            cur.execute("UNLOCK ALL")
        except Exception:
            pass
        cur.execute("USE v133db")
        if restart_only:
            part_cold_restart(cur)
            part_mqtt_restart(cur)
        else:
            part_mysql_compat(cur)
            part_type_mapping(cur)
            part_cold(cur)
            part_mqtt(cur)
            part_bugfix(cur)
            # v133db 保留给 --check-restart（重启后冷块/保留消息仍在）；库表下次运行幂等重建
    finally:
        cur.close()
        conn.close()

    tag = "V133R" if restart_only else "V133"
    print("%s: pass=%d fail=%d" % (tag, STATS["pass"], STATS["fail"]))
    return 1 if STATS["fail"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
