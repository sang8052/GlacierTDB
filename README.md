# GlacierTDB

> **Glacier**（冰川 · 冷数据）+ **TDB**（Time-series Database）
> 以 **SQLite** 为唯一存储内核的**多中间件一体机**：MySQL 协议网关 + 时序冷库（已实现），
> S3 对象存储 / Redis 键值队列 / RabbitMQ 消息队列（规划中）——**一个静态二进制、一个数据目录**，
> 一个端口多协议复用。面向工控机 / 物联网边缘环境，免去 MySQL + Redis + RabbitMQ + MinIO
> 组合的繁杂依赖：备份 = 拷贝目录，迁移 = 拷贝文件。

> 🤖 **本项目全程由 GLM5.3 驱动。**

| 📄 文档 | 说明 |
| --- | --- |
| [docs/配置说明.md](docs/配置说明.md) | 配置手册：全部配置项、默认值、热改关系、命令行参数、FAQ |
| [docs/系统SQL.md](docs/系统SQL.md) | 系统 SQL 手册：管理面私有 SQL、系统库兼容层、SHOW 兜底 |
| [docs/异步任务.md](docs/异步任务.md) | 异步任务手册：SUBMIT 全量语法、任务类型、回调协议、分片与示例 |
| [docs/时序数据库.md](docs/时序数据库.md) | 时序冷库设计：存储模型、查询改写、rollup 与压缩路线 |
| [docs/S3对象存储.md](docs/S3对象存储.md) | S3 对象存储手册：SigV4 鉴权、桶/对象/版本/对象锁、限速、策略、证书、计费（v1.1.0） |
| [docs/部署运维.md](docs/部署运维.md) | 部署（Docker/裸机/systemd）、升级、备份恢复、监控、故障排查 |
| [FEATURE.md](FEATURE.md) | **版本路线规划**：多中间件一篮子愿景、mode: 端口复用协议、v1.0.10–v1.3.0 路线 |
| [CHANGELOG.md](CHANGELOG.md) | 更新日志 |

**一句话**：把 SQLite 变成一台"多协议服务器"——现有 MySQL 客户端、ORM、运维脚本**不改一行配置**
即可远程读写；后续同一端口上叠加 Redis 键值、消息队列、S3 对象存储，让一台工控机跑完全部中间件。

---

## 🎯 定位：多中间件一篮子混合部署

工控机 / 物联网边缘环境的痛点：设备资源有限、依赖安装繁琐、多个中间件（MySQL、Redis、
RabbitMQ、MinIO）各自为政——安装、守护、备份、升级都是负担。GlacierTDB 的答案：

- **一个内核**：SQLite（全球部署量最大的数据库引擎）作为唯一存储内核，数据永不损坏、零运维；
- **一个二进制**：纯 Go 无 CGO，全部能力内化，不依赖任何外部中间件；单文件备份即全量备份；
- **一个端口**：3309 多协议复用——客户端连接后声明服务类型，不声明默认 MySQL：

```
mode:mysql      -- MySQL 协议（默认，现有客户端零改动）
mode:rabbitmq   -- AMQP 消息队列（规划 v1.3.0）
mode:redis      -- RESP 键值/队列（规划 v1.2.0）
```

  原生客户端无法插声明行时自动协议嗅探（AMQP 魔数 / RESP 特征），识别不出按 MySQL 处理。
- **一个数据目录**：`.db` 文件即全部状态（含规划中的对象、消息、键值），`db_dir` 拷走即迁移。

**当前已实现**：MySQL 协议网关（完整事务/多库/管理面）+ 时序冷库 + 异步查询引擎 + 多核并行查询；
**路线**（详见 [FEATURE.md](FEATURE.md)）：v1.0.10 查询 QoS / 用户权限+防爆破 / JSON / Geo →
v1.1.0 S3 对象存储 → v1.2.0 Redis 键值 → v1.3.0 RabbitMQ。

---

## 🔧 开发与构建环境约定

- **外部资源（GitHub / Go Module / Docker Hub）访问受限时，可配置 HTTP 代理 `<代理地址>`**
  - Docker 构建时通过 `--build-arg HTTP_PROXY=<代理地址> --build-arg HTTPS_PROXY=<代理地址>` 注入；
  - 命令行临时使用：`export http_proxy=<代理地址> https_proxy=<代理地址>`；
  - `git config --global http.proxy <代理地址>`（如需 git clone）。
- 所有编译、测试均在 **Docker 容器内**完成（`golang:1.26-alpine` 构建镜像），**不在本机安装 Go 工具链、不污染宿主机环境**。
- Go 模块下载同时配置 `GOPROXY=https://goproxy.cn,direct` 作为国内加速兜底。

---

## ✨ 项目优势

### 1. MySQL 协议 + 标准 SQL 兼容，存量工具零改造

- 完整实现 MySQL 握手 / 认证 / 结果集封包，`mysql` CLI、Navicat、DBeaver、PyMySQL、SQLAlchemy、JDBC 等主流客户端直接连；
- 内置**探活语句应答表**：`SELECT @@version`、`SHOW DATABASES / TABLES / COLUMNS / CREATE TABLE / VARIABLES …`、`SET NAMES`、`USE` 等连接初始化语句逐条应答，客户端不会表现为"连不上"；未覆盖的 `SHOW` 变体一律兜底应答（空结果集），绝不透传报错；
- 常用 MySQL 函数自动改写为 SQLite 等价形式：`NOW()`、`CURDATE()`、`CURTIME()`、`UNIX_TIMESTAMP()`、`LAST_INSERT_ID()`、`TRUNCATE TABLE`；
- 语义对齐 MySQL：`insert_id` / `affected_rows` 回填、错误码映射（语法错 `1064`、只读拒绝 `1290` 等）、外键默认开启、`DESC/DESCRIBE 表` 映射到 `SHOW COLUMNS`、SQL 中的 `库名.表` 前缀自动剥除；**驱动层转义序列反转义**（v1.0.9）——JSON 等文本值不再被转义序列污染。

### 2. TCP 远程访问，SQLite 不再"只能本机用"

SQLite 天生是嵌入式库，只能被本机进程访问。本网关补齐网络层：任何能连 MySQL 的机器、容器、脚本，都能远程读写同一个 SQLite 文件——归档库回填、冷数据查询、跨主机数据交换一个端口搞定。

### 3. 数据高可靠，不怕 IO 闪断

- SQLite 单文件 + **WAL 日志**，事务级原子提交：进程崩溃、IO 闪断甚至掉电后，数据库文件**永不损坏**，已提交事务不丢；
- 数据库与应用同生命周期，没有独立的数据库守护进程——**不存在"数据库服务挂了"这回事**，重启即恢复；
- 持久化强度可调：默认 `synchronous=NORMAL`（快），对持久化要求极高时可配置为 `FULL`（每事务落盘）。

### 4. 一篮子替代多中间件，极简部署

- 不依赖 MySQL Server、不依赖任何消息队列 / 注册中心 / 配置中心；路线落地后，Redis / RabbitMQ / S3 的角色也由同一二进制承担；
- 纯 Go + 无 CGO，编译产物是**一个静态二进制**（约 16–17 MB），scp 到目标机器直接运行，常驻内存 15–25 MB；
- 数据就是一个 `db_dir` 目录，备份 = 复制文件，迁移 = 拷走文件。

### 5. 信创友好

- SQLite 是**全球部署量最大的数据库**（数十亿台设备在运行），代码极度过分成熟，测试覆盖率为业界罕见水准（官方宣称 100% 分支覆盖、数百万行测试代码），文档完善程度业界公认第一；
- **公有领域（Public Domain）许可**——不是开源协议的"免费使用"，而是无条件无义务，零授权风险、零费用，过信创评审无知识产权障碍；
- 纯 Go 无 CGO，一条命令交叉编译到 arm64（鲲鹏 / 飞腾）等国产平台；无外部中间件依赖，不引入额外国产化适配工作量，**轻松过信创**。

---

## 🏗 架构

```
   MySQL 客户端
   (mysql CLI / Navicat / DBeaver / pymysql / SQLAlchemy / JDBC)
        │
        │  TCP :3309
        ▼
┌──────────────────────────────────────────┐
│  go-mysql server（握手 / 认证 / 封包）    │  ← go-mysql-org/go-mysql
├──────────────────────────────────────────┤
│  探活语句应答表                            │  ← 本项目核心自研部分
│  SELECT @@… / SHOW … / SET … / USE        │
├──────────────────────────────────────────┤
│  方言改写：NOW() / LAST_INSERT_ID() / …   │
├──────────────────────────────────────────┤
│  语句分类：读 / 写                         │
│    读 ──► 并发多线程（可按库限流）           │
│    写 ──► 单库互斥锁（多库写入并行）         │
├──────────────────────────────────────────┤
│  后端：每客户端连接 = 1 个专属 SQLite 连接 │
│        （不池化复用，保证事务不漂移）      │
└──────────────────────────────────────────┘
        │
        ▼
   /data/<库名>.db  (+ -wal / -shm)
```

> **演进架构（规划）**：3309 监听器前增加服务声明/嗅探复用层——`mode:mysql` 走现有栈，
> `mode:redis` / `mode:rabbitmq` 分别进入 RESP / AMQP 协议栈（v1.2.0 / v1.3.0）；
> S3 对象存储独立 HTTP(S) 监听（v1.1.0，virtual-host + 每 endpoint 证书需要独立 TLS 监听器）。
> 详见 [FEATURE.md §0](FEATURE.md)。

### 并发模型

1. **读 → 并发多线程**：WAL 下读不互斥，可按库限流（`per_db_read_threads` + 按库覆盖），并有全局线程数闸门（`global_threads`）；
2. **写 → 单库单线程**：每个库一把互斥锁，**多库写入天然并行**（实测 A 库持锁期间 B 库写入 0.00s 完成，同库第二连接排队 1.50s）；
3. **每客户端连接独占一个后端 SQLite 连接** —— 保证 `BEGIN … COMMIT` 的事务始终落在同一物理连接上；
4. **SQLite WAL 模式** —— 读写互不阻塞。

> **为什么不用连接池**：SQLite 不支持跨连接事务。连接一旦被复用，`BEGIN` 之后的语句漂移到另一个连接，会造成**静默数据错乱且不报错**——本项目从架构上根除这一风险。

### 启动时 PRAGMA 设置

```sql
PRAGMA journal_mode = WAL;      -- 读写互不阻塞的前提
PRAGMA synchronous  = NORMAL;   -- 可配置 OFF/FULL/EXTRA
PRAGMA foreign_keys = ON;       -- 对齐 MySQL 默认
PRAGMA busy_timeout = 5000;     -- 兜底，非主策略
PRAGMA cache_size   = -256;     -- 每连接 256 KB，控制内存
```

---

## ❄️ 时序冷库能力

为冷查询 / 历史归档设计的时序能力：`series` 维表 + `points` (series_id, ts) 聚簇事实表标准 schema，配合网关改写层——`NOW() - INTERVAL n UNIT` / 日期字符串谓词自动整数化（命中聚簇索引）、`GROUP BY time(N)` InfluxDB 风格桶化、`FROM_UNIXTIME()` 转换；实测 8640 点回填、EXPLAIN 确认无全表扫。

**完整设计（存储模型 / 查询改写 / rollup 路线 / 压缩固化 / 与 InfluxDB 差异）见 [docs/时序数据库.md](docs/时序数据库.md)**，建表脚本见 [schema/timeseries.sql](schema/timeseries.sql)。

---

## 🧊 S3 对象存储（v1.1.0，可选）

`config.yaml` 打开 `s3.enable` 即得一台兼容 **AWS Signature V4** 的对象存储（默认 `0.0.0.0:9000`，
boto3 / aws-cli / MinIO Client / 各语言 AWS SDK 直连），替代 MinIO / 简单文件服务器：

- **存储**：一个 bucket 一张表（系统库 `glacier_s3.db`），超阈值内容分块；桶从属用户、命名空间隔离；
- **密钥**：用户多组 AK/SK（**AK 32 位 `AS` 前缀 / SK 48 位 `SK` 前缀**），管理 SQL 签发/轮换/禁用/吊销，
  SK 明文仅签发时可见一次；
- **治理**：下载限速（单连接 / 用户总带宽多连接合并 / 并发上限）、UA·IP·Referer·自定义 header
  请求策略条件、上传回调（HMAC 签名）、多版本、对象锁（WORM/legal hold）、生命周期自动删除；
- **图片**：裁剪 / 缩放 / 格式转换 / base64 输出与直传；
- **网络**：region 通配（`'*'`）、virtual-host + path-style、多 endpoint、每 endpoint 泛域名证书 +
  bucket 自定义域名独立证书（SNI 分发、文件替换热加载）；
- **计费**：`s3_usage` 按 用户 × 桶记账（请求数 / 传输字节 / 存储占用 / 对象数），SQL 直查 +
  `EXPORT S3 USAGE` / HTTP `/_glacier/s3/usage` 导出接口。

**完整手册见 [docs/S3对象存储.md](docs/S3对象存储.md)**；管理 SQL 见 [docs/系统SQL.md §1.10](docs/系统SQL.md)。

---

## 🛡 管理面（私有 SQL）

`GlacierTDB` / `glacier` 为系统内置管理库（**不允许创建同名库**），`USE GlacierTDB` 进入管理面
（登录账号：管理账号 `glacier` 读写；只读账号 `glacier_ro` 可查看不可变更，写与变更命令返回 1290）：

```sql
USE GlacierTDB;

SHOW SERVER CONFIG;                       -- 查看全部服务器配置（键值表）
SELECT * FROM server_config WHERE name LIKE 'concurrency.%';
SHOW CLIENTS;      SELECT * FROM clients;      -- 连接列表（事务/持锁状态/最近语句）
SHOW TRANSACTIONS; SELECT * FROM transactions; -- 活动事务观测
KILL <conn_id>;                           -- 踢掉客户端（任何库下均可用）
SET PASSWORD = '<新密码>';                -- 修改密码（热生效，持久化到系统库 + glacier.json 镜像）
SET PASSWORD FOR glacier_ro = '<新密码>'; -- 指定账号改密

-- 用户权限（v1.0.10）：账号入库、库/表/IP 授权
CREATE USER 'u1'@'%' IDENTIFIED BY 'pwd';
GRANT SELECT, INSERT ON backup.* TO 'u1';
GRANT SELECT ON backup.t1 TO 'u1';
SHOW GRANTS FOR 'u1';
ALTER USER 'u1' READONLY;                 -- 账号类型 SUPER / READONLY / USER 热切换
REVOKE INSERT ON backup.* FROM 'u1';
DROP USER 'u1';

-- 防爆破审计与解锁（v1.0.10）
SHOW LOGIN ATTEMPTS LIMIT 50;             -- 登录审计（成功/失败/原因）
SHOW LOCKED;                              -- 当前锁定（IP / 账号，含到期时间与升级层数）
UNLOCK ACCOUNT u1; UNLOCK IP 10.0.0.9; UNLOCK ALL;

-- 查询 QoS 热改（v1.0.10，重启失效）
SET QOS GLOBAL QPS = 500;
SET QOS DB backup QPS = 100; SET QOS DB backup CLEAR;
SHOW QOS;                                 -- 配置 + 实时统计

-- 并发配置热改（仅运行时生效，重启回到配置文件值）
SET GLOBAL_THREADS = 32;                  -- 全局线程数（读写共享，0=不限）
SET PER_DB_READ_THREADS = 8;              -- 全局默认：单库读线程数
SET DB_READ_THREADS backup = 8;           -- 单库读线程覆盖

USE backup;                               -- 退出管理面，恢复正常 SQL
```

> 内存与连接保护：`memory_limit_mb`（Go 运行时软内存上限）、`max_connections`（最大并发连接，超限拒绝）。
>
> **SQL 审计日志**：`log.sql_log_file` 指定路径即开启（如 `/data/sql.log`），每条语句一行 JSON——`ts / conn / user / remote / db / admin / cost_ms / status(OK|ERROR) / rows / affected / insert_id / sql / error`，便于调试与排障；100MB 自动轮转为 `.old`。

---

## 🚀 快速开始

### 方式一：Docker

```yaml
# docker-compose.yml
services:
  proxy:
    build: .
    image: glaciertdb:latest
    container_name: glaciertdb
    ports:
      - "3309:3309"
    volumes:
      # 必须挂「目录」而非单个文件 —— WAL 模式需要在同目录创建 -wal / -shm
      - ./data:/data
    restart: unless-stopped
```

```bash
docker compose up -d
```

### 方式二：静态二进制（推荐）

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o glaciertdb .
./glaciertdb -config config.yaml -db-dir /data
```

交叉编译到 arm64 只需把 `GOARCH` 改为 `arm64`，产物无任何依赖。

### 连接测试

```bash
mysql -h <host> -P 3309 -uglacier -p<密码> \
      --default-auth=mysql_native_password --default-character-set=utf8mb4
```

```python
import pymysql
conn = pymysql.connect(host="<host>", port=3309,
                       user="glacier", password="<密码>", database="backup")
```

> ⚠️ **数据目录必须可写，且挂载目录而非单个 `.db` 文件。** WAL 模式需要在数据库同目录下创建 `-wal` 与 `-shm` 文件，只挂单个文件会导致「unable to open database file」这类极具误导性的报错。

---

## ⚙️ 配置说明

```yaml
listen: 0.0.0.0:3309        # 监听地址
db_dir: /data               # 库目录：一个库 = 目录下一个 <库名>.db 文件
default_db: backup          # 连接未指定库时的默认库（不存在则自动创建）
server_version: "8.99.999"  # 对外呈现的 MySQL 版本号

memory_limit_mb: 0     # Go 运行时软内存上限（0 = 不限制）
max_connections: 64    # 最大并发客户端连接数（超限直接断开）

# 并发模型：读并发多线程（可按库限流）；写单库单线程互斥（多库写入并行）
# 均可被管理面热改（USE GlacierTDB; SET GLOBAL_THREADS=32; 重启失效回到此处值）
concurrency:
  global_threads: 0              # 全局并发执行上限（读写共享，0=不限）
  per_db_read_threads: 0         # 全局默认：单库读并发上限（0=不限）
  per_db_read_threads_override:  # 按库覆盖（可选）
    # backup: 8

auth:
  user: glacier             # 固定值，配置其他名字会被忽略
  password: "<密码>"         # mysql_native_password，请自行设置强口令
  readonly_user: glacier_ro     # 只读账号（v1.0.9）：同等认证，仅允许查询与查看
  readonly_password: "<密码>"   # 只读账号密码（空 = 禁用只读登录）；支持管理面热改

sqlite:
  synchronous: NORMAL       # OFF | NORMAL | FULL | EXTRA（持久化强度）
  busy_timeout_ms: 5000
  cache_size_kb: 256        # 每连接页缓存
  max_result_rows: 1000000  # 单查询返回行数上限（内存保护）

log:
  level: info               # debug | info | warn | error
  slow_query_ms: 200        # 慢查询阈值
  sql_log_file: ""          # SQL 审计日志（JSON 行：语句/OK|ERROR/错误消息/耗时/行数/连接），空=关闭
```

所有项均可省略，省略时使用内置默认值。命令行可用 `-config` / `-db-dir` / `-listen` 覆盖。

---

## 📦 功能清单

| 模块 | 说明 |
| --- | --- |
| 握手 + 认证 | `mysql_native_password` **用户权限系统（v1.0.10）**：账号入库（`glacier_auth.db` 系统表，旧配置/glacier.json 自动迁移），默认超级用户 `glacier` + 只读 `glacier_ro` + 自定义用户；`CREATE USER` / `GRANT` / `REVOKE` / `SHOW GRANTS` / `SET PASSWORD`，**库级 + 表级 + IP（CIDR）授权**，账号类型 SUPER/READONLY/USER 热切换 |
| 防爆破 | 登录失败窗口计数自动锁 IP / 账号（三模式、指数升级、持久化）；可选 ufw/iptables/firewall-cmd 系统层封锁；登录失败/锁定 HTTP 异步告警；`SHOW LOGIN ATTEMPTS` / `SHOW LOCKED` / `UNLOCK`（v1.0.10） |
| 查询 QoS | 全系统 + 单库两级配额（qps 令牌桶 / concurrency / rows_per_sec，wait/reject）；`SET QOS` 热改、`SHOW QOS` / `qos_status` 实时统计；异步任务统一记账（v1.0.10） |
| 多库管理 | 一个库 = `db_dir` 下一个 `.db` 文件；`SHOW DATABASES` 列出、`USE` 切换后端文件、`CREATE DATABASE` / `DROP DATABASE` 建删文件（库名仅限 `[A-Za-z0-9_-]`）、`SHOW CREATE DATABASE` |
| 系统库兼容层 | `information_schema`（21 张常用表）/ `mysql.user` / `performance_schema` / `sys` 由网关直接应答（支持列投影与 WHERE 过滤），不透传报错 |
| 探活应答表 | `SELECT @@version / @@sql_mode / …`、`SHOW TABLES / COLUMNS / INDEX / CREATE TABLE / TABLE STATUS / VARIABLES / ENGINES / COLLATION / CHARSET / PROCESSLIST / TRIGGERS / PROCEDURE STATUS …`、`SET NAMES / SET SESSION`、`DO 0`、`/* ping */`；**未覆盖的 SHOW 变体一律兜底空结果集** |
| 方言改写 | `NOW()` / `CURDATE()` / `CURTIME()` / `UNIX_TIMESTAMP()` / `LAST_INSERT_ID()` / `TRUNCATE TABLE` / `FROM_UNIXTIME()` / `NOW() ± INTERVAL n UNIT` / `GROUP BY time(N)` / 日期字符串谓词整数化 / `DESC 表` / `库名.表` 前缀剥除；`LIMIT o,n` / `CONCAT` / `IF` / `RAND` / `UUID` / `INSERT IGNORE`；DDL 类型映射（MySQL 建表直接粘贴）；MySQL 转义序列反转义（v1.0.9）；**JSON `->` / `->>` 运算符改写（v1.0.10）** |
| MySQL 函数（Go 标量） | `MD5` / `SHA1` / `SHA2` / `SHA256` / `LEFT` / `RIGHT` / `SUBSTRING_INDEX` / `DATEDIFF` / `TIMESTAMPDIFF` / `DATE_FORMAT`；**JSON：`JSON_LENGTH` / `JSON_CONTAINS` / `JSON_KEYS` / `JSON_SEARCH` / `JSON_MERGE_PATCH`**（v1.0.10，原生 JSON_EXTRACT 等直通）；**Geo：`ST_Distance` / `ST_Distance_Sphere` / `ST_Within` / `ST_Contains` / `ST_Intersects` / `ST_AsText` / `ST_GeomFromText` / `ST_AsGeoJSON` / `ST_MakePoint` 等**（v1.0.10，WKT/WKB/GeoJSON 自动识别）——注册进每个后端连接，任意表达式位置可用 |
| 多核并行查询 | hint `/*+ PARALLEL(n) */` 显式触发 + **自动触发**（估算行数 ≥ `parallel_query.scan_rows`，默认 100 万）；单表可合并聚合分片-并行-合并；`EXPLAIN PARALLEL` 查看拆分计划 |
| 异步查询引擎 | `SUBMIT QUERY` 秒回 job_id；AT 定时 / WINDOW 每日·周几窗口 / WHEN 资源门槛 / EVERY 周期任务（**含每周 `EVERY WEEK ON 周五 AT '02:00'` / 每 N 周，v1.0.10**）；**`SHOW QUERY SCHEDULE` 周期配置查询 / `PAUSE`·`RESUME` 链启停（v1.0.10）**；回调四通道（HTTP+HMAC 签名 / TCP 流 / RabbitMQ / Kafka）；**PARTITION BY ROWS/BYTES 分片回调**；结果 JSONL 落盘分页拉取；**`SHOW QUERY VARS` 监测变量当前值（含服务器级 `DISK_MBPS`）**（详见 docs/异步任务.md） |
| 标准 SQL 透传 | `COM_QUERY` → prepare → 逐行 step → 流式发包，大结果集不整体物化 |
| 并发模型 | 读并发多线程（`global_threads` + `per_db_read_threads` + 按库覆盖）；写单库互斥锁（多库写入并行）；`BEGIN IMMEDIATE` 持锁至 `COMMIT`/`ROLLBACK`/断开 |
| 事务 | `BEGIN` / `COMMIT` / `ROLLBACK` / `SAVEPOINT` / `SET AUTOCOMMIT=0` 惰性隐式事务 |
| 结果回填 | `last_insert_rowid()` → OK 包 `insert_id`；`changes()` → `affected_rows` |
| 错误码包装 | 语法错 → `1064 (42000)`；只读账号写 → `1290`；无权限 → `1142`；账号/主机被锁 → `3119`；约束冲突 / 锁忙等逐一映射 |
| 其他命令 | `COM_PING` / `COM_QUIT` / `COM_INIT_DB` / `COM_FIELD_LIST` |
| 内存保护 | `memory_limit_mb`（Go 软内存上限）、`max_connections`、单查询行数上限 `max_result_rows` |
| SQL 审计日志 | `log.sql_log_file` 开启后逐条记录语句/结果(OK\|ERROR)/错误消息/耗时/行数/连接，100MB 自动轮转 |
| 慢查询日志 | 超过 `slow_query_ms` 的语句记入日志（标注 QoS 排队时长） |
| S3 对象存储（v1.1.0，可选） | AWS SigV4（表头 + 预签名 URL + aws-chunked）；一 bucket 一表 + 分块；AK/SK 签发/轮换/禁用/吊销；PUT/GET/HEAD/DELETE/COPY、ListObjects V1/V2、ListObjectVersions；下载限速（单连接/用户总带宽合并/并发上限）；UA/IP/Referer/Header 请求策略；上传回调（HMAC）；图片裁剪/缩放/转换/base64；多版本 + 对象锁（WORM）+ 生命周期；virtual-host + 自定义域名独立证书（SNI 热加载）；`s3_usage` 计费 SQL 可查 + 导出接口（详见 docs/S3对象存储.md） |

---

## 🗺 版本路线

完整规划（设计细节 / 配置草案 / 协议定义）见 **[FEATURE.md](FEATURE.md)**：

| 版本 | 主题 | 亮点 |
| --- | --- | --- |
| ✅ v1.0.10 | 运维与查询补强 | **已完成**：查询 QoS（全系统/单库）；每周周期调度 + `SHOW QUERY SCHEDULE` + `PAUSE/RESUME`；**用户权限系统**（账号入库、库/表/IP 权限、MySQL 风格 `CREATE USER`/`GRANT`）+ **防爆破**（IP/账号/混合锁定、可选 ufw/iptables/firewall-cmd 系统层封锁、登录失败 HTTP 告警）；**JSON 复杂查询**；**Geo 字段查询** |
| ✅ v1.1.0 | S3 对象存储 | **已完成**：AWS SigV4 兼容（表头/预签名/aws-chunked）；bucket 一张表、用户多组 AK/SK（AS/SK 前缀，SK 明文仅一次可见）；下载限速（单连接/多线程合并/并发上限）；UA/IP/Referer/Header 请求策略 + 上传回调（HMAC）；图片预处理/base64 直传/多版本/对象锁/生命周期；region 通配、virtual-host、多 endpoint 泛域名证书、bucket 自定义域名（SNI 热加载）；SQL 可查的 `s3_usage` 计费 + 导出接口 |
| v1.2.0 | Redis 键值/队列 | `mode:redis` 同端口 RESP；key/队列/TTL/自动落盘；每用户独立 namespace；内存上限 + LRU 淘汰 |
| v1.3.0 | RabbitMQ 队列 | `mode:rabbitmq` 同端口 AMQP 0-9-1；exchange/queue/binding/发布消费；消息持久化、管理面观测 |

端口复用协议：客户端声明 `mode:mysql` / `mode:rabbitmq` / `mode:redis`，**不声明默认 mysql**（原生客户端协议嗅探兜底）——现有全部 MySQL 客户端零改动。

---

## ✅ 兼容性验证

仓库自带黑盒测试（`tests/`），多路客户端全覆盖：

| # | 客户端 | 验证重点 |
| --- | --- | --- |
| 1 | `mysql` CLI | 握手、认证、`SHOW` 全家族、SELECT/INSERT/事务、`LAST_INSERT_ID()` |
| 2 | Python `pymysql` | 常见 PyMySQL 用法、utf8mb4 中文/emoji、BLOB 往返、多库切换 |
| 3 | Python `SQLAlchemy` | ORM 正常工作、inspect 反射、事务提交/回滚 |
| 4 | 并发/多库 | 同库写排队、跨库写并行、并发配置热改、KILL、改密热生效 |

运行方式（需 Docker，被测服务先启动并接入网络）：

```bash
bash tests/run_all.sh
```

附加验证项：`BEGIN … INSERT … ROLLBACK` 数据不落盘、10 万行大结果集内存平稳、BLOB 二进制字段往返无损、中文表名 / 列名（反引号）、`information_schema` / `mysql.user` 系统库兼容层、时序改写与索引命中。

**2026-10-22 实测战果（ALL TESTS PASSED）**：mysql:8 CLI ✅ ｜ PyMySQL ✅（utf8mb4 emoji、BLOB 字节级往返、executemany、多库 CREATE/USE/DROP）｜ SQLAlchemy ✅（inspect 反射、事务提交/回滚）｜ 并发 + 多库模型 ✅（**跨库写并行 0.00s / 同库写排队 1.50s**、三级并发配置热改）｜ 时序 ✅（INTERVAL/日期谓词整数化、time(N) 桶化、索引命中）｜ 管理面 21 项 ✅ ｜ **MySQL 兼容 M1+M2 ✅**（LIMIT 双参/CONCAT/IF/RAND/UUID/DDL 类型映射/INSERT IGNORE + MD5/SHA 族/LEFT/RIGHT/SUBSTRING_INDEX/DATEDIFF/TIMESTAMPDIFF/DATE_FORMAT）｜ **多核并行查询 9 项 ✅**（80 万行聚合对拍一致 + EXPLAIN PARALLEL）｜ **异步查询 ✅**（SUBMIT/AT/窗口/WHEN/EVERY/四通道回调+HMAC/分片回调/JSONL 分页拉取）｜ v1.0.7 周期任务/VACUUM/目录锁/静态加密 ✅ ｜ v1.0.8 共 37 项 ✅（空串回读修复、MD5、分片回调、周窗口）｜ v1.0.9 共 32 项 ✅（引号转义反转义修复、只读账号 glacier_ro、SHOW QUERY VARS、DISK_MBPS）｜ **v1.0.10 共 51 项 ✅**（查询 QoS、每周调度/query_schedule/PAUSE、用户权限系统、防爆破锁定、JSON 运算符与函数、Geo ST_*）。默认监听 **3309**，对外版本 **8.99.999**，默认超级用户 **glacier**。

---

## ⚠️ 已知限制

**协议层**

- 不支持预处理语句（`COM_STMT_EXECUTE` 文本协议路径），客户端须走文本协议；SQLAlchemy、JDBC 需确认降级正常；
- 仅实现 `mysql_native_password`，MySQL 8 客户端需 `--default-auth=mysql_native_password`；
- 不支持 TLS、XA、`LOAD DATA`、`LOCK TABLES`、MySQL replication 协议；
- **多协议复用（`mode:` 声明）、S3 / Redis / RabbitMQ 为规划能力**（见 FEATURE.md），当前版本仅 MySQL 协议。

**语义层**

- 系统库兼容层为**常用表只读应答**（`information_schema` 21 表 + `mysql.user` 等），非全量 MySQL 系统库实现；
- 多库 = 目录下多文件，**库间无跨库 JOIN**（`db1.table` 语法不支持）；
- `DROP DATABASE` 直接删除 `.db`（及 `-wal`/`-shm`）文件，**不可恢复**，慎用；
- `SHOW CREATE TABLE` 生成的 MySQL 方言 DDL 为近似还原（类型沿用 SQLite 声明）；
- 无 `DECIMAL` 精确类型（SQLite 为动态类型），金额类字段建议按 TEXT 存取；
- 无 `TINYINT` / `DATETIME` 等原生类型，日期时间以 TEXT 或 INTEGER 存储，客户端所见列类型标识与真实 MySQL 有差异；
- 日期 / 字符串函数覆盖常用集（`MD5`/`SHA` 族、`LEFT/RIGHT`、`SUBSTRING_INDEX`、`DATEDIFF`/`TIMESTAMPDIFF`、`DATE_FORMAT`），`IFNULL()`、`GROUP_CONCAT()` 等 SQLite 原生可用，其余不保证；
- `ALTER TABLE` 仅支持 SQLite 能力集（`ADD COLUMN` / `RENAME` / `DROP COLUMN`），其余按 `1064` 报错。

**部署层**

- 单实例部署，SQLite 文件不可被其他进程并发写入；
- 数据库所在目录必须可写（WAL 需要 `-wal` / `-shm`）；
- WAL 文件会增长，依赖 SQLite 自动 checkpoint；长期写入且无读者时，可在空闲期执行 `PRAGMA wal_checkpoint(TRUNCATE)`；归档固化后可执行 `PRAGMA journal_mode=DELETE; VACUUM;` 转为单文件再拷贝；
- 认证为用户权限系统（v1.0.10：账号入库、库/表/IP 授权、防爆破自动锁定），无 TLS、无列级权限，适用于**内网可信环境**，不可直接暴露公网（`lock.notify_url` 可配登录失败告警）。

---

## 📁 目录结构

```
GlacierTDB/
├── main.go               # 入口：配置加载、后端初始化、启动监听
├── config.yaml           # 端口、库目录、并发、SQLite 参数、日志
├── internal/
│   ├── proxy/            # TCP 监听与连接生命周期
│   ├── handler/          # go-mysql Handler 实现（探活→改写→分类→透传→审计）
│   ├── admin/            # 连接注册表（SHOW CLIENTS / KILL）
│   ├── meta/             # 探活应答表 + 方言改写 + 系统库兼容层
│   ├── asyncq/           # 异步查询引擎（SUBMIT / 回调 / WHEN 采样）
│   ├── sqlite/           # 多库后端、每库写锁、读限流、事务
│   └── logx/             # 轻量日志 + SQL 审计日志
├── schema/timeseries.sql # 时序冷库标准 schema
├── docs/                 # 文档：配置说明 / 系统SQL / 时序数据库 / 部署运维 / 异步任务
├── FEATURE.md            # 版本路线规划（多中间件一篮子）
├── CHANGELOG.md          # 更新日志
├── tests/                # 黑盒测试（CLI/PyMySQL/SQLAlchemy/并发/多库/时序/管理面/异步）
├── Dockerfile
├── docker-compose.yml
├── LICENSE               # MIT
└── README.md
```

## 🔧 技术栈

| 层 | 选型 | 说明 |
| --- | --- | --- |
| MySQL wire server | `go-mysql-org/go-mysql`（`server` 包） | 握手 / 认证 / 结果集封包 |
| SQLite 驱动 | `ncruces/go-sqlite3` | 基于 wazero，**纯 Go、无 CGO**，性能接近 C 版，任意平台交叉编译无障碍 |
| 认证 | `mysql_native_password` | 双账号（管理 glacier / 只读 glacier_ro），口令热改持久化；库表级权限在 v1.0.10 路线 |
