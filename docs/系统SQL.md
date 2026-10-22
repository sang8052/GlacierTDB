# GlacierTDB 系统 SQL 手册

> GlacierTDB 在标准 SQL 透传之外，提供三类系统级 SQL 能力：
> **管理面私有 SQL**（`USE GlacierTDB`）、**系统库兼容层**（`information_schema` / `mysql` / …）、**探活与兜底**（`SHOW` 家族 / `DESC` / `glacier_version`）。
>
> 登录账号：管理账号固定为 `glacier`；另有只读账号（默认 `glacier_ro`，v1.0.9）——同等认证，
> 仅允许查询与查看（写语句 / 事务 / 管理面变更命令一律 `1290` 拒绝）。连接后默认库为配置的 `default_db`（默认 `backup`）。

---

## 1. 管理面（USE GlacierTDB）

`GlacierTDB` / `glacier` 为**系统内置管理库**：

- 不允许 `CREATE DATABASE glacier`（返回 1007 + `reserved` 提示）；
- `USE GlacierTDB` 与 `USE glacier` 均可进入管理面；
- 管理面内**只接受管理命令**，普通 SQL 返回 `1064` 并提示可用命令；
- `USE <普通库名>` 退出管理面，恢复正常 SQL。

```sql
USE GlacierTDB;      -- 或 USE glacier;
```

### 1.1 查看服务器配置

```sql
SHOW SERVER CONFIG;                                    -- 全量键值表（name, value）
SELECT * FROM server_config;                           -- 等价表形式
SELECT value FROM server_config WHERE name = 'listen';
SELECT name FROM server_config WHERE name LIKE 'sqlite.%';
```

常用配置键：

| 键 | 说明 |
| --- | --- |
| `glacier_version` | GlacierTDB 自身版本（如 `1.0.9.alpha`） |
| `server` / `server_version` | 产品名 / 对外模拟的 MySQL 协议版本（`8.99.999`） |
| `listen` / `db_dir` / `default_db` / `databases` | 监听、库目录、默认库、当前库列表 |
| `auth.user` / `auth.readonly_user` | 管理账号（固定 `glacier`）/ 只读账号名 |
| `memory_limit_mb` / `max_connections` / `current_connections` | 内存软上限、连接上限、当前连接数 |
| `concurrency.global_threads` | 全局并发执行上限（0 = 不限） |
| `concurrency.per_db_read_threads` | 全局默认：单库读并发上限 |
| `concurrency.db_read_threads.<库>` | 单库读并发覆盖值 |
| `sqlite.*` / `log.*` | SQLite 与日志参数 |

### 1.2 连接与事务观测

```sql
SHOW CLIENTS;          SELECT * FROM clients;          -- 连接列表
SHOW TRANSACTIONS;     SELECT * FROM transactions;     -- 活动事务
```

> 表查询支持 `WHERE col = 'v' / col LIKE 'p%' / col IN (...)` 的 AND 组合过滤；
> **忽略列投影**，固定返回全列。

`clients` 列（9 列）：

| 列 | 说明 |
| --- | --- |
| `Id` | 连接 ID（`KILL` 用它） |
| `User` / `Host` | 用户 / 客户端地址 |
| `DB` | 当前库（管理面连接显示 `GlacierTDB`） |
| `AdminMode` | 是否处于管理面（YES/NO） |
| `ConnectedSec` | 连接已建立秒数 |
| `InTransaction` | 是否在事务中 |
| `HoldingWriteLock` | 是否持有该库写锁 |
| `LastQuery` | 最近一条语句（截断 200 字符） |

`transactions` 列（7 列）：`Id, User, Host, DB, TxSec(事务已持续秒), HoldingWriteLock, LastQuery` —— 只列出处于事务中的连接。

### 1.3 踢掉客户端

```sql
KILL <conn_id>;              -- 任何库下均可用，不限管理面
KILL CONNECTION <conn_id>;
KILL QUERY <conn_id>;        -- 等价 CONNECTION（不支持只杀语句）
```

- 成功：返回 OK，目标连接立即断开（其未提交事务自动回滚）；
- 目标不存在：返回 `1094 Unknown thread id`；
- 异步注销，`clients` 列表内约 1 秒内消失。

### 1.4 修改密码

```sql
-- 管理员密码（缺省目标 = glacier 自己）
SET PASSWORD = '<新密码>';                          -- 推荐
SET PASSWORD FOR glacier = '<新密码>';
ALTER USER 'glacier' IDENTIFIED BY '<新密码>';

-- 只读账号密码（v1.0.9，与管理员同等热改/持久化）
SET PASSWORD FOR glacier_ro = '<新密码>';
ALTER USER 'glacier_ro' IDENTIFIED BY '<新密码>';
```

- **热生效**：新连接立即用新密码，旧密码被拒（1045）；
- **持久化**：写入 `<db_dir>/glacier.json`（管理员 `password` / 只读 `readonly_password` 两字段），重启后仍以它为准（优先于配置文件）；
- 空密码拒绝（1819）；只读账号执行本命令返回 `1290`。

### 1.5 并发配置热改（重启失效）

```sql
SET GLOBAL_THREADS = 32;              -- 全局线程数（读写共享，0 = 不限）
SET PER_DB_READ_THREADS = 8;          -- 全局默认：单库读线程数
SET DB_READ_THREADS backup = 8;       -- 单库读线程覆盖
SET DB_READ_THREADS backup = 0;       -- 0 = 删除覆盖，回到全局默认
```

即时生效，可在压测间动态调节；**重启后回到 `config.yaml` 中 `concurrency` 的值**。当前值通过 `server_config` 查看（见 1.1）。

### 1.6 管理面外的全局命令

以下命令在**任意库**下均有效，无需进入管理面：

```sql
SELECT glacier_version;               -- GlacierTDB 自身版本（≠ @@version 的协议版本）
KILL <conn_id>;                       -- 见 1.3
```

---

## 2. 系统库兼容层

客户端工具（Navicat / DBeaver 等）会直查系统库，GlacierTDB 由**网关直接应答**（支持列投影与 `WHERE` 过滤，`DATABASE()` / `SCHEMA()` 自动求值为当前库），**不透传 SQLite**。

### 2.1 information_schema（21 张表）

| 表 | 数据来源 |
| --- | --- |
| `ENGINES` | 静态（InnoDB/MEMORY/MyISAM/CSV/ARCHIVE/BLACKHOLE） |
| `SCHEMATA` | 库目录（`db_dir` 下的 `.db` 文件） |
| `TABLES` | `sqlite_master`（TABLE_SCHEMA/TABLE_NAME/TABLE_TYPE …） |
| `COLUMNS` | 各表 `PRAGMA table_info` 展开（含 ORDINAL_POSITION / COLUMN_KEY / COLUMN_DEFAULT …） |
| `VIEWS` | `sqlite_master` type=view |
| `STATISTICS` | 索引展开（INDEX_NAME / SEQ_IN_INDEX / COLUMN_NAME / NON_UNIQUE …） |
| `TABLE_CONSTRAINTS` | 主键/唯一约束 |
| `KEY_COLUMN_USAGE` | 主键/唯一约束列 |
| `TRIGGERS` | `sqlite_master` type=trigger |
| `COLLATIONS` / `CHARACTER_SETS` | 静态（utf8mb4 系等） |
| `PROCESSLIST` | 当前连接 |
| `ROUTINES` / `PARAMETERS` / `REFERENTIAL_CONSTRAINTS` / `PARTITIONS` / `EVENTS` / `USER_PRIVILEGES` / `SCHEMA_PRIVILEGES` / `TABLE_PRIVILEGES` / `COLUMN_PRIVILEGES` | 空结果集（带列名） |

示例：

```sql
SELECT TABLE_NAME, TABLE_TYPE
FROM information_schema.TABLES
WHERE table_schema = DATABASE();

SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY
FROM information_schema.COLUMNS
WHERE table_schema = 'backup' AND table_name IN ('t1', 't2');
```

### 2.2 mysql / performance_schema / sys

| 查询 | 行为 |
| --- | --- |
| `SELECT User, Host, plugin FROM mysql.user` | 返回单账号 `glacier / % / mysql_native_password`（含全部权限列，`WHERE User='glacier'` 可过滤） |
| 其他 `mysql.*` / `performance_schema.*` / `sys.*` | 返回带请求列的**空结果集**（不报错） |

### 2.3 兼容层限制

- 只读（系统库不接受 INSERT/UPDATE/DELETE，按普通写语句处理或报错）；
- `WHERE` 支持 `=` / `LIKE` / `IN` 与 `AND`；OR / 函数等复杂条件返回全量行由客户端自行处理；
- 未覆盖的表返回空集，非全量 MySQL 系统库实现。

---

## 3. 探活应答与 SHOW 兜底

连接初始化语句逐条应答，客户端不会表现为「连不上」：

```sql
SELECT @@version / @@sql_mode / @@character_set_client / @@transaction_isolation / …;
SET NAMES utf8mb4;  SET SESSION …;  SET @@…;
DO 0;  /* ping */
```

**已支持的 SHOW**：`DATABASES / [FULL] TABLES / [FULL] COLUMNS(FIELDS) / INDEX(INDEXES,KEYS) / CREATE TABLE|VIEW|DATABASE / TABLE STATUS / [GLOBAL|SESSION] VARIABLES / STATUS / COLLATION / CHARACTER SET / ENGINES / WARNINGS / ERRORS / PROCESSLIST / GRANTS / TRIGGERS / PROCEDURE|FUNCTION STATUS / PLUGINS / PRIVILEGES / MASTER STATUS / SLAVE|REPLICA STATUS / ENGINE … STATUS / EVENTS / OPEN TABLES / PROFILES`。

**兜底铁律**：任何未覆盖的 `SHOW <任何内容>` 一律返回空结果集，绝不透传报错。

**其他兼容**：

| 写法 | 行为 |
| --- | --- |
| `DESC 表` / `DESCRIBE 表` | 等价 `SHOW COLUMNS FROM 表` |
| ``SELECT … FROM `库名`.`表` `` / `库名.表` | 自动剥掉**当前库名**前缀（其他库名 = 跨库，不支持） |
| `select $$` / `select ?` 等占位符 | 1064 拒绝（对齐真 MySQL，防客户端状态机失步） |
| `SELECT LAST_INSERT_ID()` | 改写为 `last_insert_rowid()` |
| `NOW() / CURDATE() / CURTIME() / UNIX_TIMESTAMP() / FROM_UNIXTIME()` | 改写为 SQLite 等价 |

时序写法（`NOW() - INTERVAL n UNIT`、`GROUP BY time(N)`、日期字符串谓词整数化）见 readme「时序查询能力」与 `schema/timeseries.sql`。

---

## 4. 异步查询与定时任务（v1.0.5–1.0.8）

> 完整语法、生命周期、回调协议与示例见 **[docs/异步任务.md](异步任务.md)**（v1.0.8 新增专册）。

```sql
SUBMIT QUERY 'SELECT …'
  [CALLBACK '<url>']                    -- http(s)://… | tcp://host:port | amqp://… [QUEUE q] | kafka://broker [TOPIC t]
  [SIGN '<secret>'] [TIMEOUT 3600] [RETAIN 300]
  [AT '17:00' | '2026-10-16 17:00:00']  -- 指定时刻执行
  [WINDOW '17:00-20:00']                -- 每日窗口（EXPIRED 不跨天）
  [WINDOW '周五 17:00-20:00']           -- 周几限定窗口（周一..周日 / MON..SUN，v1.0.8）
  [WHEN CPU < 70 AND MEM < 80 AND APP_CPU < 50 AND IO_MBPS < 10]
  [PARTITION BY ROWS 50000 | BY BYTES 8388608]   -- 分片回调：每片独立 POST（v1.0.8 实装）
  [EVERY DAY [AT '17:00'] | EVERY HOUR | EVERY 30 MINUTES];  -- 周期定时任务

SHOW QUERY JOBS;  SELECT * FROM query_jobs WHERE state='DONE';
SELECT * FROM query_result('j_xxx') LIMIT 1000 OFFSET 5000;
CANCEL QUERY 'j_xxx';  DROP QUERY RESULT 'j_xxx';
VACUUM DATABASE backup;   -- ⚠️ 执行期间该库全部写入阻塞；CLI: glaciertdb -vacuum
```

四态回调（DONE/ERROR/EXPIRED/CANCELLED）；结果默认 60 秒自动删除；`query_jobs` v1.0.8 新增 `partition_rows` / `partition_bytes` 列。

## 4.1 多核并行查询（EXPLAIN PARALLEL）

```sql
SELECT /*+ PARALLEL(4) */ COUNT(*), SUM(v), AVG(v) FROM big_table WHERE g < 90;  -- hint 显式触发
SELECT COUNT(*) FROM big_table;   -- 估算行数 ≥ parallel_query.scan_rows（默认 100 万）自动触发
EXPLAIN PARALLEL SELECT COUNT(*) FROM big_table;   -- 查看拆分计划（分片键/估算行数/并行度/策略）
```

- 适用：单表顶层可合并聚合（COUNT/SUM/MIN/MAX/AVG），无 GROUP BY/ORDER/LIMIT；分片键 rowid 或单列主键；
- v1.0.8 修复：注释剥离曾连带剥掉 `/*+ PARALLEL(n) */` hint（hint 从未生效、静默串行回退），现已保留。

## 5. MySQL 函数兼容（v1.0.3 M1 + v1.0.8 M2）

文本改写（v1.0.3）：`NOW()/CURDATE()/CURTIME()/UNIX_TIMESTAMP()/FROM_UNIXTIME()/LAST_INSERT_ID()/TRUNCATE`、`LIMIT o,n`、`CONCAT`、`IF`、`RAND()`、`UUID()`、DDL 类型映射、`DEFAULT CURRENT_TIMESTAMP`。

Go 标量函数（v1.0.8，注册进每个后端连接，任意表达式位置可用）：

| 函数 | 说明 |
| --- | --- |
| `MD5(s)` / `SHA1(s)` / `SHA256(s)` | 小写 hex；入参 NULL → NULL |
| `SHA2(s, n)` | n∈{0,256,384,512}，0 视为 256；其他 → NULL（MySQL 语义） |
| `LEFT(s,n)` / `RIGHT(s,n)` | UTF-8 安全；n≤0 → ''（MySQL 语义） |
| `SUBSTRING_INDEX(s,d,n)` | n>0 正数第 n 个分隔符之前（含）；n<0 倒数第 \|n\| 个之后 |
| `DATEDIFF(a,b)` | a−b 整天数（时间部分截断） |
| `TIMESTAMPDIFF(unit,a,b)` | b−a；unit∈MICROSECOND/SECOND/MINUTE/HOUR/DAY/WEEK/MONTH/QUARTER/YEAR（日历感知） |
| `DATE_FORMAT(d,fmt)` | `%Y %y %m %c %d %e %H %k %h %I %l %i %s %S %f %p %r %T %W %a %M %b %j %w %%` |

语句改写（v1.0.8 M2）：`INSERT IGNORE INTO` → `INSERT OR IGNORE INTO`；`TIMESTAMPDIFF(MONTH, …)` 单位关键字自动字符串化。

**转义还原（v1.0.9）**：MySQL 协议客户端把参数值按 MySQL 规则转义后拼进 SQL 文本（`"` → `\"`、`'` → `\'`、`\` → `\\`、换行 → `\n`），网关在语句进入 SQLite 前对单引号字面量统一做 MySQL→SQLite 还原，文本值（如 JSON 配置）不再被转义序列污染，`WHERE` 等值比较可正常命中；`\%` / `\_`（LIKE 通配符字面量）保留原样。查询、写语句与异步任务执行路径全部覆盖。

## 6. 快速速查

```sql
-- 日常
SELECT glacier_version;                          -- 1.0.9.alpha
USE GlacierTDB;                                  -- 进管理面
SHOW CLIENTS;                                    -- 看连接
SHOW TRANSACTIONS;                               -- 看事务
KILL 10086;                                      -- 踢人
SET PASSWORD = 'new-pass';                       -- 改密（热生效）
SET PASSWORD FOR glacier_ro = 'ro-pass';         -- 改只读账号密码（v1.0.9）
SHOW QUERY VARS;                                 -- 异步 WHEN 监测变量当前值（v1.0.9）
SET GLOBAL_THREADS = 64; SET PER_DB_READ_THREADS = 16;
USE backup;                                      -- 回业务库
```
