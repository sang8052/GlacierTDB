-- ============================================================================
-- init_schema.sql — mysql-tcp-proxy-sqlite 测试库初始化脚本
--
-- 用法（由 run_all.sh 调用；也可手工执行）:
--   docker run --rm -i --network sqlproxy-net mysql:8 \
--     mysql -h proxy -P 3309 -ubackup -piloveyou~ \
--     --default-auth=mysql_native_password --default-character-set=utf8mb4 \
--     backup < tests/init_schema.sql
--
-- 说明:
--   * 服务端把 COM_QUERY 原样透传给 SQLite，因此这里全部使用 SQLite 语法
--     （AUTOINCREMENT / TEXT / REAL / X'..' BLOB 字面量 / 反引号标识符均可直接用）。
--   * 不需要 CREATE DATABASE：库名在连接串里已指定（backup）。
--   * 可重复执行：先 DROP 再 CREATE。
--   * DELIMITER 是 mysql 客户端本地命令，不会发给服务端；
--     它只用于让触发器体内的分号不被客户端提前截断。
-- ============================================================================

-- ---------- 清理旧对象（保证可重复执行） ----------
DROP TRIGGER IF EXISTS trg1;
DROP VIEW    IF EXISTS v1;
DROP TABLE   IF EXISTS t1;
DROP TABLE   IF EXISTS t2;
DROP TABLE   IF EXISTS blob_t;
DROP TABLE   IF EXISTS t1_log;
DROP TABLE   IF EXISTS `订单`;

-- ---------- 基础表：自增主键 ----------
CREATE TABLE t1 (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    val  REAL
);

-- ---------- 复合主键表 ----------
CREATE TABLE t2 (
    a INTEGER,
    b TEXT,
    PRIMARY KEY (a, b)
);

-- ---------- BLOB 表 ----------
CREATE TABLE blob_t (
    id   INTEGER PRIMARY KEY,
    data BLOB
);

-- ---------- 触发器日志表 ----------
CREATE TABLE t1_log (
    id  INTEGER PRIMARY KEY,
    msg TEXT
);

-- ---------- 中文表名 / 中文列名（反引号） ----------
CREATE TABLE `订单` (
    `编号` INTEGER PRIMARY KEY,
    `名称` TEXT
);

-- ---------- 视图 ----------
CREATE VIEW v1 AS SELECT id, name FROM t1;

-- ---------- 触发器：t1 每插入一行，向 t1_log 写一条日志 ----------
DELIMITER ;;
CREATE TRIGGER trg1
AFTER INSERT ON t1
BEGIN
    INSERT INTO t1_log(msg) VALUES ('t1_insert');
END;;
DELIMITER ;

-- ---------- 初始数据（含中文 / 负数浮点） ----------
INSERT INTO t1(name, val) VALUES
    ('张三', 1.5),
    ('李四', -2.75),
    ('测试用户王五', 100.125);

INSERT INTO t2(a, b) VALUES
    (1, '甲'),
    (2, '乙');

INSERT INTO blob_t(id, data) VALUES
    (1, X'00FF10DEADBEEF');

INSERT INTO `订单`(`编号`, `名称`) VALUES
    (1, '第一笔订单'),
    (2, '第二笔订单·中文数据');
