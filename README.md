# GlacierTDB

> **Glacier**（冰川 · 冷数据）+ **TDB**（Time-series Database）
> 以 **SQLite** 为唯一存储内核的**多中间件一体机**：MySQL 协议网关 + 时序冷库 + S3 对象存储 +
> Redis 键值队列 + RabbitMQ 消息队列（**全部已实现**）——**一个静态二进制、一个数据目录、
> 一个端口多协议**。面向工控机 / 物联网边缘环境，一台设备替代 MySQL + Redis + RabbitMQ + MinIO
> 的组合：备份 = 拷贝目录，迁移 = 拷贝文件。

> 🤖 **本项目全程由 GLM5.3 驱动。**

**当前版本：v1.3.5.alpha** · 二进制下载：[Releases](https://github.com/sang8052/GlacierTDB/releases)
（三平台 + SHA256SUMS）· 许可：MIT

| 📄 文档 | 说明 |
| --- | --- |
| [docs/配置说明.md](docs/配置说明.md) | 配置手册：全部配置项、默认值、热改关系、命令行参数、FAQ |
| [docs/系统SQL.md](docs/系统SQL.md) | 系统 SQL 手册：管理面私有 SQL、系统库兼容层、SHOW 兜底 |
| [docs/时序数据库.md](docs/时序数据库.md) | 时序冷库：存储模型、查询改写、rollup 预聚合、压缩路线 |
| [docs/异步任务.md](docs/异步任务.md) | 异步任务：SUBMIT 全量语法、调度、回调协议、SCRIPT 后处理 |
| [docs/S3对象存储.md](docs/S3对象存储.md) | S3 对象存储：SigV4 鉴权、桶/对象/版本/对象锁、限速、策略、证书、计费 |
| [docs/键值队列.md](docs/键值队列.md) | 键值/队列：mode:redis、RESP 命令集、TTL、命名空间、内存治理 |
| [docs/消息队列.md](docs/消息队列.md) | 消息队列：mode:rabbitmq、AMQP 0-9-1、路由语义、持久化、观测 |
| [docs/部署运维.md](docs/部署运维.md) | 部署（Docker/裸机/systemd）、升级、备份恢复、监控、故障排查 |
| [FEATURE.md](FEATURE.md) | 版本路线规划（只放规划；已完成见 CHANGELOG） |
| [CHANGELOG.md](CHANGELOG.md) | 更新日志（各版本完整变更明细） |

---

## 🎯 定位：多中间件一篮子混合部署

工控机 / 物联网边缘环境的痛点：设备资源有限、依赖安装繁琐、多个中间件（MySQL、Redis、
RabbitMQ、MinIO）各自为政——安装、守护、备份、升级都是负担。GlacierTDB 的答案：

- **一个内核**：SQLite（全球部署量最大的数据库引擎）作为唯一存储内核，数据永不损坏、零运维；
- **一个二进制**：纯 Go 无 CGO，五种中间件能力全部内化（约 18 MB 静态产物），零外部依赖；
- **一个端口**：3309 承载三协议——客户端声明服务类型，不声明默认 MySQL：

  | 进入方式 | 说明 |
  | --- | --- |
  | `mode:<service>` 声明行 | 连接首行 ASCII 单行 `mode:mysql` / `mode:redis` / `mode:rabbitmq`（自定义客户端/调试用） |
  | 协议嗅探（兜底） | RESP 首字节（`* + - : $`）→ Redis；`AMQP` 魔数 → RabbitMQ |
  | 默认 | 静默等握手（嗅探窗口 `mux_sniff_ms` 内无数据）→ MySQL——**存量客户端零改动** |

- **一个数据目录**：业务库 `.db` + 系统库（`glacier_auth` / `glacier_s3` / `glacier_kv` /
  `glacier_mq`）+ 密钥与任务产物全在 `db_dir` 下，拷走即迁移。

> S3 对象存储不进 3309 复用：HTTP(S) + virtual-host + 泛域名证书需要独立 TLS 监听器（默认 `:9000`）。

---

## 🧭 能力全景

| 中间件角色 | 入口 | 一句话能力 | 手册 |
| --- | --- | --- | --- |
| **MySQL 网关** | `:3309`（默认） | MySQL 协议 + 标准 SQL，mysql CLI / Navicat / DBeaver / PyMySQL / SQLAlchemy / JDBC 直连；多库、事务、管理面 | 本文 / 系统SQL |
| **时序冷库** | 同 MySQL | `series`+`points` 聚簇 schema、时间谓词整数化、`GROUP BY time(N)`、**rollup 预聚合自动路由**、多核并行 | 时序数据库 |
| **异步任务** | 同 MySQL | `SUBMIT QUERY` 秒回 job_id；AT / WINDOW / WHEN / EVERY 周期调度（含每月/每年）；四通道回调 + HMAC；**SCRIPT 后处理管道** | 异步任务 |
| **S3 对象存储** | `:9000`（HTTP/S） | AWS SigV4（表头/预签名/aws-chunked）；**multipart 分片上传**、版本、对象锁、生命周期、限速、请求策略、上传回调、图片处理、计费 | S3对象存储 |
| **Redis 键值/队列** | `:3309`（mode:redis） | RESP string/list；**BLPOP/BRPOP 阻塞弹出**；TTL 惰性+扫描；**SQLite 落盘重启无损**；每用户命名空间；LRU 淘汰 | 键值队列 |
| **RabbitMQ 队列** | `:3309`（mode:rabbitmq） | AMQP 0-9-1：direct/fanout/topic、publish/consume/ack/nack/prefetch；**confirm 发布确认 + mandatory 回退**；消息与游标落盘重启不丢 | 消息队列 |
| **运维与安全** | 全协议 | 用户权限（库/表/IP 授权）、防爆破自动锁、查询 QoS、caching_sha2_password、SQL 审计日志 | 本文 / 配置说明 |

---

## 🏗 架构

```
                        ┌─────────────────────────────────────────────┐
   MySQL 客户端 ───┐     │ 3309 端口复用层                              │
   redis-cli  ────┤     │  mode:<service> 声明行 优先                   │
   pika/amqp  ────┼────►│  RESP / AMQP 魔数嗅探 兜底                    │
                        │  静默 → MySQL                                 │
                        └───────┬──────────┬──────────┬───────────────┘
                                │          │          │
                    ┌───────────▼──┐  ┌────▼─────┐ ┌──▼──────────┐
                    │ MySQL 协议栈 │  │ RESP 栈  │ │ AMQP 0-9-1  │
                    │ 探活/改写/   │  │ KV 存储  │ │ 交换机/队列 │
                    │ 并行/异步/   │  │ TTL/淘汰 │ │ 路由/确认   │
                    │ 权限/QoS    │  └────┬─────┘ └──┬──────────┘
                    └──────┬──────┘       │          │
                           │       glacier_kv.db  glacier_mq.db
                    glacier_auth.db
                           │
   S3 客户端 ──► :9000 ──► S3 栈（SigV4/桶/版本/限速/证书）──► glacier_s3.db
                           │
                    ┌──────▼──────────────────────────────┐
                    │ SQLite（WAL）：/data/<库名>.db       │
                    │ 每客户端连接 = 1 个专属后端连接      │
                    │ 读并发多线程 / 写单库互斥（多库并行）│
                    └─────────────────────────────────────┘
```

### 并发模型

1. **读 → 并发多线程**：WAL 下读不互斥，可按库限流（`per_db_read_threads` + 按库覆盖），并有全局闸门（`global_threads`）；
2. **写 → 单库单线程**：每库一把互斥锁，**多库写入天然并行**（实测 A 库持锁期间 B 库写入 0.00s 完成，同库排队 1.50s）；
3. **每客户端连接独占一个后端 SQLite 连接**——`BEGIN … COMMIT` 始终落在同一物理连接（SQLite 不支持跨连接事务，连接池会造成**静默数据错乱**，架构上根除）；
4. **SQLite WAL**：读写互不阻塞。

### 启动时 PRAGMA

```sql
PRAGMA journal_mode = WAL;      -- 读写互不阻塞的前提
PRAGMA synchronous  = NORMAL;   -- 可配置 OFF/FULL/EXTRA
PRAGMA foreign_keys = ON;       -- 对齐 MySQL 默认
PRAGMA busy_timeout = 5000;     -- 兜底，非主策略
PRAGMA cache_size   = -256;     -- 每连接 256 KB
```

### 数据高可靠

- SQLite 单文件 + WAL，事务级原子提交：崩溃 / IO 闪断 / 掉电后文件**永不损坏**，已提交事务不丢；
- 没有独立数据库守护进程——不存在"数据库服务挂了"这回事，重启即恢复；
- **无损恢复实测**：KV 键值（含 TTL、队列顺序）与 MQ 消息（含消费游标）容器重启后全部还原（`tests/run_all.sh` 步骤 3.5）。

---

## 📦 能力详解

### 1. MySQL 协议网关

- 完整握手 / 认证 / 结果集封包；**登录算法**：`mysql_native_password`（默认）+ **`caching_sha2_password` / `sha256_password`**（MySQL 8 默认算法，`auth.auth_plugin` 全局配置或 `CREATE USER … IDENTIFIED WITH <plugin>` 账号级，RSA 全认证 + 改密自动失效登录缓存）；
- **探活语句应答表**：`SELECT @@version/@@sql_mode…`、`SHOW TABLES/COLUMNS/INDEX/CREATE TABLE/VARIABLES/PROCESSLIST…`、`SET NAMES`、`USE`、`DO 0`、`/* ping */` 逐条应答；未覆盖 `SHOW` 一律兜底空结果集；`information_schema`（21 表）/ `mysql.user` / `performance_schema` / `sys` 直接应答（支持列投影与 WHERE）；
- **方言改写**（保守：匹配不上原样透传）：`NOW()/CURDATE()/UNIX_TIMESTAMP()/LAST_INSERT_ID()/TRUNCATE`、`NOW() ± INTERVAL n UNIT`、`GROUP BY time(N)`、日期字符串谓词整数化、`DESC 表`、`库名.表` 前缀剥除、`LIMIT o,n`、`CONCAT/IF/RAND/UUID`、`INSERT IGNORE`、**`ON DUPLICATE KEY UPDATE`（含 `VALUES(col)`）**、`FOR UPDATE` 剥离、DDL 类型映射（MySQL 建表直接粘贴）、转义序列反转义、JSON `->`/`->>` 改写；**M4 批（v1.3.3）**：`&&/||/!/XOR/DIV/RLIKE/BINARY/0x…/N'…'`、`DATE_ADD 族/EXTRACT/TRIM 方向/POSITION/GROUP_CONCAT SEPARATOR/CAST/CONVERT`、`ALTER ADD·DROP INDEX/MODIFY/CHANGE`、`RENAME TABLE`、`INSERT … SET`、`UPDATE/DELETE … ORDER BY … LIMIT`、`ANALYZE/CHECK/OPTIMIZE/REPAIR/CHECKSUM TABLE`、用户变量 `@a`；
- **Go 标量函数**（任意表达式位置可用）：`MD5/SHA1/SHA2/SHA256`、`LEFT/RIGHT`、`SUBSTRING_INDEX`、`DATEDIFF/TIMESTAMPDIFF`、`DATE_FORMAT`（含周编号族 `%U %u %V %v %X %x`）、`CONCAT_WS/LPAD/RPAD`；JSON：`JSON_LENGTH/CONTAINS/KEYS/SEARCH/MERGE_PATCH`（原生 `JSON_EXTRACT` 等直通）；Geo：`ST_Distance/ST_Distance_Sphere/ST_Within/ST_Contains/ST_Intersects/ST_AsText/ST_GeomFromText/ST_AsGeoJSON/ST_MakePoint…`（WKT/WKB/GeoJSON 自动识别）；
- **事务与语义对齐**：`BEGIN/COMMIT/ROLLBACK/SAVEPOINT`、`SET AUTOCOMMIT=0` 惰性隐式事务、`insert_id/affected_rows` 回填、错误码映射（1064/1290/1142/3119/1040…）；
- **多核并行查询**：① 单表可合并聚合（COUNT/SUM/MIN/MAX/AVG）hint `/*+ PARALLEL(n) */` 或 `scan_rows` 阈值自动触发；② `GROUP BY series_id, time(N)` 按 series 取模分片并行、Go 侧按桶合并；③ 明细导出按 rowid 区间并行 scan、顺序发送；`EXPLAIN PARALLEL` 查看拆分计划。

### 2. 时序冷库（含 rollup）

- 标准 schema（[schema/timeseries.sql](schema/timeseries.sql)）：`series` 维表 + `points(series_id, ts, val)` 聚簇事实表（`ts` = epoch ms）；tag 表达式索引；
- **rollup 多粒度预聚合**：写 `points` 同遍落 `points_m1/h1/d1` 汇总表（INSERT 触发器）；带时间范围的聚合查询自动路由「汇总表满桶 + 原始表边缘桶」UNION，取最粗可用粒度——**结果与全量扫精确一致**（边缘桶回退原始行）；存量回填 `REBUILD ROLLUP <db>`；
- **冷压缩与固化（v1.3.3）**：`COMPRESS COLD points BEFORE <time> [STEP n DAY|HOUR]` 按块 gzip 固化进 `points_cold`（字节级无损，实测压缩比 ~3.8），`THAW COLD` / `SHOW COLD` 配套；**读写透明**——查询触及冻结区间自动解冻、写入撞冻结区间自动重试；解冻期间摘除 rollup 触发器保证汇总不重复计数；`timeseries.cold_after_days` 后台自动固化策略；
- **列类型映射（v1.3.3）**：DOUBLE/FLOAT/DECIMAL 以 TEXT 落盘（防浮点 15 位格式化截断，17 位有效数字精确往返），查出按系统库 `glacier_meta` 的映射自动还原 float64 并回放 MySQL 类型码；WHERE/ORDER BY 自动 `CAST(col AS REAL)` 保数值语义；SHOW COLUMNS/CREATE TABLE/information_schema 回放建表时的原始类型；
- 回填 ETL 规范、`EXPLAIN QUERY PLAN` 索引命中实测、压缩固化详见 [docs/时序数据库.md](docs/时序数据库.md)。

### 3. 异步任务引擎

- `SUBMIT QUERY '<sql>'` 秒回 job_id，结果 JSONL 落盘分页拉取（`FETCH QUERY RESULT` / `query_result()`）；
- **调度**：`AT 'HH:MM'` 定时、`WINDOW '周五 17:00-20:00'` 每日/周几窗口、`WHEN CPU<70` 资源门槛（CPU/MEM/DISK_MBPS/连接数等）、`EVERY DAY|WEEK|MONTH|YEAR|n SECONDS…` 周期任务（含每周/每 N 周、每月/每年，月尾日自动 clamp 如 1/31→2/28）；`SHOW QUERY SCHEDULE`、`PAUSE/RESUME` 链启停；
- **回调**：HTTP POST（HMAC 签名 + 退避重试）/ TCP 流 / RabbitMQ / Kafka 四通道；`PARTITION BY ROWS|BYTES` 分片回调；
- **SCRIPT 后处理管道**：`SCRIPT 'x.js' OUTPUT out`——结果 JSONL 流入 nodejs/python/php 子进程，产物落 `jobs/<id>.<out>`（`script_dir` 白名单）；
- 详见 [docs/异步任务.md](docs/异步任务.md)。

### 4. 运维与安全

- **用户权限系统**：账号入库（`glacier_auth.db`），`CREATE USER/GRANT/REVOKE/SHOW GRANTS/SET PASSWORD`，库级 + 表级 + IP（CIDR）授权，账号类型 SUPER/READONLY/USER 热切换；默认超级用户 `glacier` + 只读 `glacier_ro`；
- **防爆破**：登录失败窗口计数自动锁 IP/账号（三模式、指数升级、持久化重启不解除）；可选 ufw/iptables/firewall-cmd 系统层封锁；登录失败 HTTP 告警；`SHOW LOGIN ATTEMPTS/SHOW LOCKED/UNLOCK`；
- **查询 QoS**：全系统 + 单库两级配额（qps/concurrency/rows_per_sec，wait/reject）；`SET QOS` 热改、`SHOW QOS` 实时统计；
- **SQL 审计日志**：`log.sql_log_file` JSON 行（语句/状态/耗时/行数/连接），100MB 轮转；慢查询日志标注 QoS 排队。

### 5. S3 对象存储（v1.1.0，`s3.enable` 开启）

AWS Signature **V4** 兼容（表头 + 预签名 URL + `aws-chunked`），boto3 / aws-cli / MinIO Client 直连：

- 一 bucket 一表（`glacier_s3.db`）+ 大对象分块；桶从属用户、命名空间隔离；
- **AK/SK**：AK 32 位 `AS` 前缀 / SK 48 位 `SK` 前缀，签发/轮换/禁用/吊销（SK 明文仅一次可见）；
- PUT/GET/HEAD/DELETE/COPY、ListObjects V1/V2、ListObjectVersions；**multipart 分片上传**（Initiate/UploadPart/Complete/Abort/ListParts/ListMultipartUploads，复合 ETag 对齐 S3 语义）；**UploadPartCopy 拷贝分片（v1.3.5）**——`x-amz-copy-source [+range]` 服务端拷贝免重传，与直传分片混合合并；**未完成上传自动回收**（`s3.abort_incomplete_days`，默认 7 天）；**单对象上限可解除**（`max_object_mb: -1` 不限制，分块存储流式）；
- **多版本 + 对象锁（WORM/legal hold）+ 生命周期自动删除**；
- **下载限速**：单连接 / 用户总带宽（多连接合并）/ 并发上限；**请求策略**（UA/IP/Referer/自定义 header 条件）；**上传回调**（HMAC 签名）；
- **图片处理**：裁剪 / 缩放 / 格式转换 / base64 输出与直传；
- **网络**：region 通配（`'*'`）、virtual-host + path-style、多 endpoint、每 endpoint 泛域名证书 + bucket 自定义域名独立证书（SNI 分发、替换热加载）；
- **计费**：`s3_usage` 按 用户 × 桶记账（请求数/字节/存储/对象数），SQL 直查 + `EXPORT S3 USAGE` + HTTP `/_glacier/s3/usage` 导出。

### 6. Redis 键值/队列（v1.2.0，`kv.enable`）

- RESP 服务（`mode:redis` 或嗅探）：string `GET/SET(EX·PX·NX·XX)/DEL/EXISTS/APPEND/INCR 族/MGET/MSET/KEYS/TYPE` + list 队列 `LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE`；
- **事务与发布订阅（v1.3.5）**：`MULTI/EXEC/DISCARD`（EXEC 返回逐条结果数组）；`SUBSCRIBE/PSUBSCRIBE/PUBLISH`（message/pmessage 推送、同连接 channel+pattern 双收、按账号命名空间隔离）；
- **SCAN 真游标（v1.3.5）**：`SCAN cursor [MATCH][COUNT][TYPE]` 每批 COUNT 个、游标推进，`--scan`/`scan_iter` 多批遍历与 `KEYS` 等价；
- **阻塞弹出（v1.3.2）**：`BLPOP/BRPOP key [key…] timeout`——按序取首个非空队列，Push 立即唤醒、超时返回空数组（`0` = 无限等待）；
- **HELLO/SCAN（v1.3.3）**：`HELLO` 最小应答（server map + `proto:2`，**redis-py ≥ 8 默认 RESP3 握手直连可用**）；`SCAN cursor [MATCH][COUNT][TYPE]` 单批全量 + 游标恒 0（`redis-cli --scan` / `scan_iter` 可用）；**RESP3 空值兼容（v1.3.4）**——`HELLO 3` 连接的空值应答发 RESP3 null 帧 `_`（redis-py ≥8 的 GET miss/MGET/BLPOP 超时不再挂死），RESP2 连接维持 `$-1`/`*-1`；
- **TTL**：惰性过期 + 定期扫描；**变更实时写 SQLite（`glacier_kv.db`）重启无损**；
- **命名空间 = 登录账号**（跨用户不可见）；**内存治理**：`kv.max_memory_mb` + `allkeys-lru/volatile-lru/noeviction`；`SHOW KV STATS` / `kv_stats` 占用与命中率。

### 7. RabbitMQ 队列（v1.3.0，`mq.enable`）

- AMQP 0-9-1（pika / amqp091-go 互通）：exchange（**direct/fanout/topic**，`*`/`#` 通配）+ queue + binding；默认交换机直达；
- `basic.publish/consume/ack/nack(requeue 重投 + redelivered)/reject/get/qos(prefetch)/cancel/recover`；
- **发布确认与回退（v1.3.2）**：`confirm.select` 后逐条 `basic.ack`（失败 `basic.nack`）；`mandatory` 不可路由回退 `basic.return`（312 NO_ROUTE，内容原样）；
- **消息与消费游标落 SQLite（`glacier_mq.db`）重启不丢**；断线未确认自动重入队；`SHOW QUEUES/SHOW EXCHANGES`；
- 异步任务 `CALLBACK 'amqp://…'` 可指向本服务自回环；
- **RabbitMQ 版本串可配置（v1.3.3）**：`mq.server_version`（默认 3.13.0），Connection.Start 原样上报。

### 8. MQTT 接入（v1.3.3，`mq.mqtt.enable`）

- MQTT 3.1 / 3.1.1，**TCP + WebSocket** 双传输：3309 复用（CONNECT 首包嗅探 0x1x / HTTP Upgrade `/mqtt` / `mode:mqtt` 声明行），可选独立监听 `mq.mqtt.listen`；
- 桥接 RabbitMQ：topic `a/b/c` ↔ routing key `a.b.c`（`mqtt` topic 交换机），订阅过滤器 `+`/`#` ↔ `*`/`#`，与 AMQP 客户端双向互通；
- QoS 0/1（PUBACK）、保留消息（落盘重启不丢）、clean_session=0 离线补投、遗嘱消息、keepalive 1.5 倍断开；
- 认证沿用统一账号（CONNECT username/password），命名空间 = 账号。

---

## 🛡 管理面 SQL 速览

`USE GlacierTDB` 进入管理面（超级用户可变更；只读账号仅查看，变更返回 1290）：

```sql
-- 观测
SHOW SERVER CONFIG; SHOW CLIENTS; SHOW TRANSACTIONS; KILL <id>;
-- 账号与权限（v1.0.10）
CREATE USER 'u1'@'%' IDENTIFIED [WITH caching_sha2_password] BY 'pwd';
GRANT SELECT ON backup.* TO 'u1';  SHOW GRANTS FOR 'u1';  DROP USER 'u1';
-- 防爆破
SHOW LOGIN ATTEMPTS LIMIT 50;  SHOW LOCKED;  UNLOCK ACCOUNT u1;  UNLOCK ALL;
-- QoS / 并发热改
SET QOS GLOBAL QPS = 500;  SHOW QOS;  SET GLOBAL_THREADS = 32;
-- 时序 rollup（v1.1.1）
REBUILD ROLLUP backup;
-- 键值 / 消息队列观测（v1.2.0 / v1.3.0）
SHOW KV STATS;  SET KV MAX_MEMORY 64;  SET KV EVICTION allkeys-lru;
SHOW QUEUES;  SHOW EXCHANGES;
-- S3 对象存储（v1.1.0）
CREATE BUCKET demo OWNER 'glacier' VERSIONING ON;   SHOW BUCKETS;
CREATE S3 KEY FOR 'glacier';                        SHOW S3 KEYS;
SHOW S3 USAGE;  EXPORT S3 USAGE TO 'usage.csv';     SHOW S3 OBJECTS demo LIMIT 20;
-- 表形式（支持 WHERE）：server_config / clients / qos_status / users / kv_stats /
-- mq_queues / mq_exchanges / s3_usage / s3_buckets / s3_keys / query_jobs …
SELECT * FROM s3_usage WHERE owner = 'glacier';
```

完整清单见 [docs/系统SQL.md](docs/系统SQL.md)。

---

## 🚀 快速开始

### 方式一：下载 Release 二进制（推荐）

```bash
# https://github.com/sang8052/GlacierTDB/releases
# 每个版本含：glaciertdb-linux-amd64 / linux-arm64 / windows-amd64.exe + config.example.yaml + SHA256SUMS
sha256sum -c SHA256SUMS
./glaciertdb-linux-amd64 -config config.example.yaml -db-dir /srv/glacier/data
```

### 方式二：Docker Compose

```yaml
services:
  glaciertdb:
    image: glaciertdb:latest        # 或 build: .
    container_name: glaciertdb
    ports:
      - "3309:3309"                 # MySQL + Redis + RabbitMQ（端口复用）
      - "9000:9000"                 # S3 对象存储（s3.enable 时）
    volumes:
      # 必须挂「目录」而非单个 .db 文件 —— WAL 需在同目录创建 -wal/-shm
      - /srv/glacier/data:/data
    restart: unless-stopped
```

### 方式三：源码构建

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o glaciertdb .
```

> ⚠️ **数据目录必须可写，且挂载目录而非单个文件**；无 TLS，**仅限内网可信环境**（公网请前置认证代理）；
> 首次启动后 `USE GlacierTDB; SET PASSWORD='<强口令>';`。部署/升级/备份见 [docs/部署运维.md](docs/部署运维.md)。

### 连接测试（五种客户端）

```bash
# MySQL（caching_sha2 客户端无需 --default-auth；native 客户端加 --default-auth=mysql_native_password）
mysql -h <host> -P 3309 -uglacier -p'<密码>' -e "SELECT glacier_version;"

# Redis（RESP 嗅探直接进）
redis-cli -h <host> -p 3309 -a '<密码>' SET hello world

# S3（SigV4，boto3 示例见 docs/S3对象存储.md §12）
```

```python
import pymysql                       # MySQL
pymysql.connect(host="<host>", port=3309, user="glacier", password="<密码>", database="backup")

import redis                        # 键值（pip install redis）
r = redis.Redis(host="<host>", port=3309, password="<密码>")
r.set("k", "v", ex=60)

import pika                         # 消息队列（pip install pika）
conn = pika.BlockingConnection(pika.ConnectionParameters(
    host="<host>", port=3309, heartbeat=0,
    credentials=pika.PlainCredentials("glacier", "<密码>")))
ch = conn.channel()
ch.queue_declare(queue="jobs", durable=True)
```

---

## ⚙️ 配置速览

完整示例（含全部配置段与默认值）见 [docs/配置说明.md](docs/配置说明.md)；仓库内 [config.yaml](config.yaml) 为可直接使用的带注释样例：

```yaml
listen: 0.0.0.0:3309        # 3309 = MySQL + Redis + RabbitMQ（端口复用）
db_dir: /data               # 一个库 = 目录下一个 <库名>.db 文件（系统库不暴露）
default_db: backup
server_version: "8.99.999"  # 对外 MySQL 版本号

mux_sniff_ms: 300           # 端口复用嗅探窗口（识别 mode:/RESP/AMQP，超时按 MySQL）

auth:
  user: glacier
  password: "<强口令>"
  auth_plugin: mysql_native_password   # 或 caching_sha2_password（MySQL 8 默认算法）
  readonly_user: glacier_ro

concurrency: { global_threads: 0, per_db_read_threads: 0 }   # 读并发 / 写单库互斥
parallel_query: { workers: 0, scan_rows: 1000000 }           # 并行触发
rollup: { disable: false }             # 时序预聚合（v1.1.1）
qos: { on_exceed: wait, global: { qps: 0, concurrency: 0, rows_per_sec: 0 } }
lock: { enable: true, mode: mixed, window_sec: 300, max_failures: 5 }   # 防爆破

async_query: { enable: true, workers: 2 }      # 异步任务（AT/WINDOW/WHEN/EVERY/SCRIPT）
kv: { enable: true, max_memory_mb: 64, eviction: allkeys-lru }          # Redis（v1.2.0）
mq: { enable: true, max_message_mb: 32 }                               # RabbitMQ（v1.3.0）
s3:                                        # 对象存储（v1.1.0，默认关闭）
  enable: false
  region: "*"
  endpoints: [{ listen: "0.0.0.0:9000" }]
  limits: { per_conn_bps: 0, per_user_bps: 0, per_user_conns: 0 }

sqlite: { synchronous: NORMAL, busy_timeout_ms: 5000, cache_size_kb: 256, max_result_rows: 1000000 }
log: { level: info, slow_query_ms: 200, sql_log_file: "" }
```

所有项均可省略走默认；命令行 `-config` / `-db-dir` / `-listen` 覆盖，`-vacuum` 离线真空。

---

## 🧪 测试与兼容性

`tests/run_all.sh` 一键全量（Docker 编排：mysql:8 CLI + python:3.12 PyMySQL/SQLAlchemy/pika），
当前 **20 个测试脚本 + KV/MQ 双重启持久化检查，全套 ALL TESTS PASSED（2026-10-23）**：

| 覆盖 | 要点 |
| --- | --- |
| mysql CLI / PyMySQL / SQLAlchemy | 握手、SHOW 全家族、utf8mb4/emoji、BLOB、ORM 反射、事务 |
| 并发 / 多库 / 管理面 | 跨库写并行、同库排队、KILL、改密热生效、information_schema、Navicat 兼容 |
| 时序 / 并行 | INTERVAL/日期谓词整数化、time(N) 桶化、EXPLAIN 索引命中、80 万行并行对拍、GROUP BY 分片并行、明细并行 |
| 异步任务 | AT/WINDOW/WHEN/EVERY/PAUSE、四通道回调 + HMAC、分片回调、SCRIPT 后处理 |
| v1.0.10（51 项） | QoS、每周调度、用户权限、防爆破锁定、JSON 运算符、Geo ST_* |
| S3（69 项） | SigV4/预签名、版本/对象锁/生命周期、限速、策略、图片、virtual-host、计费、上传回调 |
| v1.1.1（37 项） | rollup 精确性对拍、REBUILD、M3 兼容、caching_sha2 登录/缓存失效 |
| Redis（69 项）+ 重启 | 命令集、TTL、命名空间隔离、LRU/noeviction、kv_stats；容器重启无损 |
| RabbitMQ（28 项）+ 重启 | 路由语义、consume/ack/nack/prefetch、SHOW QUEUES、amqp 回调回环；消息重启不丢 |
| v1.3.1（44 项） | multipart 分片上传全流程（复合 ETag/Abort/ListParts）、每月/每年调度（月尾 clamp）、PHP SCRIPT 错误路径 |
| v1.3.2（13 项） | BLPOP/BRPOP 阻塞弹出（唤醒/超时/方向）、publisher confirm 逐条 ack、mandatory 回退 312 NO_ROUTE |
| v1.3.4（16 项） | RESP3 空值帧兼容（`_` null 协商，redis-py ≥8 空值不再挂死）+ RESP2 回归 |
| v1.3.5（35 项） | UploadPartCopy、未完成上传自动回收、上限可解除；MULTI/EXEC、发布订阅、SCAN 真游标 |
| v1.3.3（111 项）+ 重启 | MySQL 8 兼容 M4（运算符/函数/DDL/维护语句/用户变量）、列类型映射（TEXT 落盘/查出还原/类型回放）、时序冷压缩固化（自动解冻）、MQTT ws+tcp 桥接、RabbitMQ 版本串可配置 |

---

## 🗺 版本历史

| 版本 | 主题 | 状态 |
| --- | --- | --- |
| v1.0.3 – v1.0.9 | MySQL 网关打磨：探活/方言/函数、异步引擎、多核并行、VACUUM/目录锁/静态加密、转义修复、只读账号 | ✅ |
| v1.0.10 | 查询 QoS、每周调度 + PAUSE/RESUME、用户权限系统 + 防爆破、JSON、Geo | ✅ |
| v1.1.0 | S3 对象存储（SigV4 / 版本 / 对象锁 / 限速 / 策略 / 证书 / 计费） | ✅ |
| v1.1.1 | 存量余项全清（rollup / 分片并行 / 明细并行 / SCRIPT / M3）+ caching_sha2_password | ✅ |
| v1.2.0 | Redis 键值/队列（RESP / TTL / 落盘 / 命名空间 / 淘汰）+ 3309 端口复用 | ✅ |
| v1.3.0 | RabbitMQ 队列（AMQP 0-9-1 / 路由 / 持久化 / 观测）——五件套愿景交付完毕 | ✅ |
| v1.3.1 | S3 multipart 分片上传、每月/每年调度、SCRIPT PHP 运行时 | ✅ |
| v1.3.2 | Redis BLPOP/BRPOP 阻塞弹出、AMQP confirm 发布确认 + mandatory 回退 | ✅ |
| v1.3.3 | MySQL 8 兼容 M4、列类型映射、时序冷压缩固化、MQTT（ws/tcp）接入 + 版本串可配置 | ✅ |
| v1.3.4 | RESP3 空值帧兼容（redis-py ≥8 空值不再挂死） | ✅ |
| v1.3.5 | S3 拷贝分片/自动回收/上限解除；Redis 事务/发布订阅/SCAN 游标 | ✅ |

各版本完整变更见 [CHANGELOG.md](CHANGELOG.md)，下载见 [Releases](https://github.com/sang8052/GlacierTDB/releases)；
未排期候选与设计红线见 [FEATURE.md](FEATURE.md)。

---

## ⚠️ 已知限制

**协议层**

- 不支持预处理语句（`COM_STMT_*`），客户端走文本协议；不支持 TLS / XA / `LOAD DATA` / `LOCK TABLES` / 复制协议（公网暴露请前置认证代理）；
- S3 multipart 为基础分片上传（无 UploadPartCopy / 未完成上传自动生命周期，未完成会话需 Abort 或 `DROP BUCKET FORCE` 回收）；单对象上限 `max_object_mb`；
- Redis 为 RESP2（`HELLO` 应答 `proto:2` 自动降级不升级 RESP3；BLPOP/BRPOP 单连接串行阻塞，`timeout=0` 无限等待；事务为队列执行语义无 WATCH；SCAN 为快照游标——遍历期间写入可能漏见/重见）；
- AMQP 为基本队列（无镜像/仲裁队列、无事务性队列；confirm 为逐条 ack、无 `immediate` 位；pika 1.4.4 客户端 close() 有自身 bug，见消息队列手册）。

**语义层**

- 系统库兼容层为常用表只读应答，非全量 MySQL 系统库；库间无跨库 JOIN；
- `DROP DATABASE` 直接删文件不可恢复；无 `DECIMAL` 精确类型（金额建议 TEXT）；`ALTER TABLE` 仅限 SQLite 能力集；
- rollup 仅覆盖标准 `points` schema 与可合并聚合，含 `val` 谓词等形态自动回退全量扫（语义优先）。

**部署层**

- 单实例单写者（红线：不做多机/复制）；`db_dir` 必须可写；
- 无 TLS、无列级权限——仅限内网可信环境；
- 备份 = 拷贝 `db_dir`（务必含 `glacier.json` / `glacier_s3.key` / `glacier_rsa.pem` 等密钥文件，见部署运维手册）。

---

## 📁 目录结构

```
GlacierTDB/
├── main.go               # 入口：配置/后端/监听启动（含 -vacuum CLI）
├── config.yaml           # 带注释配置样例（全部配置段）
├── internal/
│   ├── proxy/            # 3309 端口复用（mode 声明 + 嗅探）与连接生命周期
│   ├── handler/          # MySQL Handler：探活→改写→并行/rollup→透传→审计 + 管理面 SQL
│   ├── meta/             # 探活应答表 + 方言改写 + 系统库兼容层
│   ├── sqlite/           # 多库后端、每库写锁、读限流、Go 标量函数（含 JSON/Geo）
│   ├── auth/             # 用户权限系统（GRANT / 防爆破 / caching_sha2 RSA 密钥）
│   ├── asyncq/           # 异步任务引擎（调度 / 回调 / SCRIPT 后处理）
│   ├── s3/               # S3 对象存储（SigV4 / 桶 / 版本 / 限速 / 证书 / 计费）
│   ├── kv/               # Redis 键值/队列（RESP / TTL / 落盘 / 淘汰 / HELLO/SCAN）
│   ├── mq/               # RabbitMQ 队列（AMQP 0-9-1 / 路由 / 持久化）
│   ├── mqtt/             # MQTT 接入（tcp/ws / 桥接 mq / 保留消息 / 遗嘱）
│   ├── colmeta/          # 列类型映射系统库（MySQL 类型登记 / 回放 / 存取还原）
│   ├── admin/  dirlock/  config/  logx/
├── schema/timeseries.sql # 时序标准 schema
├── docs/                 # 八册手册（配置/系统SQL/时序/异步/S3/键值/消息/部署运维）
├── tests/                # 黑盒测试（22 脚本 + run_all.sh 编排 + 重启持久化检查）
├── Dockerfile  docker-compose.yml  LICENSE  README.md  FEATURE.md  CHANGELOG.md
```

## 🔧 技术栈

| 层 | 选型 | 说明 |
| --- | --- | --- |
| MySQL wire server | `go-mysql-org/go-mysql` | 握手 / 认证（native + caching_sha2 + sha256）/ 结果集封包 |
| SQLite 驱动 | `ncruces/go-sqlite3`（wazero） | **纯 Go 无 CGO**，任意平台交叉编译 |
| JSON / 数值 | 标准库 + Go 标量函数注册 | JSON1 直通 + MySQL 函数补齐；Geo/ST 手写实现 |
| AMQP 客户端（回调/测试） | `rabbitmq/amqp091-go` / pika | 服务端协议栈自研（internal/mq） |
| 编译产物 | `CGO_ENABLED=0` + `-trimpath -ldflags="-s -w"` | 三平台静态二进制（linux amd64/arm64、windows amd64） |

## 📄 License

[MIT](LICENSE)
