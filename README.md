# GlacierTDB

把 MySQL、Redis、RabbitMQ、MinIO 装进一个程序里。

GlacierTDB 用 SQLite 做唯一的存储内核，Go 编写，编译产物是一个约 20 MB 的静态二进制，没有任何外部依赖。一台设备装一个它，就能同时提供：

- **MySQL 数据库** —— 标准 SQL，多库、事务、用户权限，mysql CLI / Navicat / PyMySQL / SQLAlchemy 直连；
- **时序数据库** —— 自动预聚合、冷数据压缩，写法还是普通 SQL；
- **Redis 键值/队列** —— string / list，落盘重启不丢；
- **RabbitMQ 消息队列** —— AMQP 0-9-1，另带 MQTT 接入；
- **S3 对象存储** —— 兼容 AWS SigV4，boto3 / aws-cli / MinIO Client 直连。

所有数据都在一个目录里。MySQL/Redis/AMQP 共用端口 3309（按客户端实际说的协议自动分发，存量客户端零改动），S3 单独监听 9000。

它为这样的场景而生：工控机、边缘盒子、小服务器——CPU 弱、内存少、可能是 SD 卡，没人值守，坏了要现场出差。所以它的备份是运行中一条命令（`glaciertdb -backup <目录>`，不阻塞业务），迁移是拷贝目录，掉电后数据文件不会坏。

> 🤖 本项目全程由 GLM 驱动开发。

**当前版本：v1.3.13.beta** · [下载（三平台 + SHA256SUMS）](https://github.com/sang8052/GlacierTDB/releases) · [完整变更记录](CHANGELOG.md) · 许可 [MIT](LICENSE)

> **安全公告**：v1.3.10 及更早版本存在租户隔离越权漏洞（已在 v1.3.11 修复），v1.3.10 已撤版。
> 在用旧版本的请立即升级，只从 Releases 获取最新版。

---

## 快速开始

**1. 下载**（或用 Docker，见下）：

```bash
# 从 https://github.com/sang8052/GlacierTDB/releases 下载对应平台的二进制和 config.example.yaml
sha256sum -c SHA256SUMS
cp config.example.yaml config.yaml   # 改一下 auth.password
./glaciertdb -db-dir /srv/glacier/data
```

配置文件与二进制放同一目录即可自动找到，也可以用 `-config` 指定路径。

**2. 连上试试**（默认账号 `glacier`，密码在 config.yaml 的 `auth.password`）：

```bash
mysql  -h <host> -P 3309 -uglacier -p'密码'   # MySQL（默认协议，直接连）
redis-cli -h <host> -p 3309 -a '密码'          # Redis（发 RESP 命令自动识别）
# S3 在 9000 端口，boto3 / aws-cli 指过去就能用，示例见 docs/S3对象存储.md
```

**3. Docker Compose**：

```yaml
services:
  glaciertdb:
    image: glaciertdb:latest
    ports:
      - "3309:3309"   # MySQL + Redis + RabbitMQ + MQTT
      - "9000:9000"   # S3
    volumes:
      - /srv/glacier/data:/data   # 必须挂目录，不能挂单个 .db 文件（WAL 需要）
    restart: unless-stopped
```

源码构建：`CGO_ENABLED=0 go build -o glaciertdb .`

---

## 五种服务

### MySQL 网关（3309，默认协议）

标准的 MySQL 协议服务：建库建表、事务、`SHOW` 全家族、`information_schema`，错误码也对齐 MySQL。常见语法和函数（`NOW() - INTERVAL`、`ON DUPLICATE KEY UPDATE`、`MD5/DATE_FORMAT/JSON_*`/Geo 函数等）都已适配，MySQL 的建表语句可以直接粘贴执行。大查询会自动按库并行拆分，慢的导出也能吃满多核。

管理不藏在 SQL 里，而是管理面语句（`USE GlacierTDB` 后执行）：用户授权、连接查看、限流配额、S3 桶和密钥管理等，速查见 [docs/系统SQL.md](docs/系统SQL.md)。

### 时序数据库

还是 MySQL 客户端，推荐用两张表：`series`（设备/测点维表）+ `points(series_id, ts, val)`。在这之上自动做了两件事：

- **预聚合**：写入时同步维护分钟/小时/天三级汇总，长时间范围的聚合查询自动走汇总表，结果与逐行计算精确一致；
- **冷压缩**：老数据按块 gzip 固化（无损，压缩比约 3.8），查询碰到冻结区间自动解冻，业务无感。

schema 和细节见 [docs/时序数据库.md](docs/时序数据库.md)。

### Redis（3309，自动识别）

常用命令都有：`GET/SET/INCR/EXPIRE`、list 队列 `LPUSH/RPOP/BLPOP`、事务 `MULTI/EXEC`、发布订阅、`SCAN`。每个账号一个命名空间，互相不可见。所有变更实时落盘，重启后键值、TTL、队列消息原样恢复。

内存超限按 LRU 淘汰；可以用 `KEYPRIO key <0-255>` 给关键键加保护（数字越小越先被赶）。详见 [docs/键值队列.md](docs/键值队列.md)。

### RabbitMQ + MQTT（3309，自动识别）

AMQP 0-9-1 的 direct/fanout/topic 交换机、消费者确认、prefetch、发布确认，消息和消费进度落盘重启不丢。MQTT 3.1/3.1.1（TCP 和 WebSocket）也接在同一个端口上，主题 `a/b/c` 自动映射成 AMQP 的 `a.b.c` 路由键，两边客户端互通。详见 [docs/消息队列.md](docs/消息队列.md)。

### S3 对象存储（9000）

兼容 AWS Signature V4，boto3、aws-cli、MinIO Client 默认配置直连。该有的都有：分片上传、多版本、对象锁（WORM）、生命周期删除、下载限速、上传回调（HMAC 签名）。

几个实用的加分项：

- **图片处理**：GET 时带参数即可裁剪/缩放/画框/加文字/转格式（OpenCV 风格参数），多核加速、结果带磁盘缓存——摄像头抓图场景直接省掉一个图片服务；
- **去重存储**：相同内容的文件只存一份，重复上传、复制、秒传都不占额外空间；
- **秒传**：先算好文件的 SHA-256，`POST /桶/键?quick_transfer=<sha256>` 一问，有就 200 直接收工，没有再全量传；
- **打包下载**：`GET /桶?x-glacier-archive=前缀&format=zip` 把一个前缀打成 tar/zip 拉走；
- **数据北上**：配置 `s3.sync` 规则后，新写入的对象自动推送到远端 S3（云端备份/上云），每个对象的同步结果可以回调通知你的业务。

运维排障：每个响应都带 `x-glacier-request-id`，配合 `s3.error_log: true` 的结构化日志，用户报障拿 ID 一查到底。详见 [docs/S3对象存储.md](docs/S3对象存储.md)。

### 异步任务

长查询不想等？`SUBMIT QUERY 'SELECT ...'` 立刻返回任务号，结果落盘慢慢拉。支持定时（AT）、每日窗口（WINDOW）、资源门槛（WHEN CPU<70）、周期执行（EVERY DAY/WEEK/MONTH），完成后 HTTP/TCP/AMQP/Kafka 回调。详见 [docs/异步任务.md](docs/异步任务.md)。

---

## 它是怎么工作的

```
  mysql / redis-cli / pika ──► 3309（端口复用：按客户端协议自动分发）
  S3 客户端 ────────────────► 9000（HTTP/S，SigV4）
                 │
                 ▼
  协议栈（MySQL / RESP / AMQP / MQTT / S3）
                 │
                 ▼
  SQLite（WAL 模式）：一个库 = 数据目录下一个 .db 文件
  系统库（账号 / S3 / KV / MQ 元数据）在 sys/，业务库在 user/
```

几个值得知道的点：

- **数据文件不会坏**。SQLite + WAL，事务级原子提交，掉电、闪断后已提交的数据都在；没有独立数据库进程，重启即恢复；
- **默认整目录加密**。落盘的 .db 文件是密文，拿 SQLite 工具直接打开看不到内容（口令在 `security.passphrase`，记得改并妥善备份）；
- **并发模型**：读多线程（WAL 读写不阻塞），写按库串行（不同库之间并行）；每个客户端连接独占一个后端连接，保证事务语义正确；
- **单机单实例**。刻意不做集群和复制——边缘场景的容灾靠"数据北上"（备份上云）加换机重部署，盒子本身是可以丢弃的。

v1.3.12 起支持 TLS：3309 同端口明文与 TLS 共存，S3 端点可配独立证书。v1.3.13 起 MySQL/Redis/AMQP 还可以走 Unix domain socket（`listen_unix`）做本地零开销连接，鉴权不变。

---

## 配置

所有配置项都有默认值，最小可用配置只有几行：

```yaml
listen: 0.0.0.0:3309
db_dir: /data
auth:
  password: "改成强口令"
```

仓库里的 [config.yaml](config.yaml) 是带完整注释的样例，[docs/配置说明.md](docs/配置说明.md) 逐项解释。常用开关：

- `kv.enable` / `mq.enable` / `mysql.enable` / `s3.enable` —— 不用的模块可以关掉，省内存；
- `lock:` —— 防爆破锁定（本机回环永不锁定；锁死了用 `glaciertdb -unlock-user <账号>` 解）；
- `security.encrypt` + `passphrase` —— 静态加密（默认开）；
- `tls:` —— 证书配置（默认关，公网暴露建议开或前置反代）。

命令行工具：`-backup <目录>` 在线备份（不停服务）、`-vacuum` 离线回收空间、`-unlock-user` / `-unlock-ip` / `-show-locks` 管理防爆破锁。

---

## 硬件要求

目标环境是低功耗工控机和边缘盒子，不是数据中心。选型一句话：**按最低档选机器，按推荐档选硬盘**——存储介质对写入性能和寿命的影响比 CPU 大得多。

| | 最低 | 推荐 |
| --- | --- | --- |
| x86-64 | 2 核 / 2 GB / 16 GB SSD（五组件可跑，图片处理限小图） | 4 核（N5105 级）/ 4-8 GB / 64 GB SSD |
| ARM64 | 树莓派 4B 或 RK3566：4 GB / 16 GB A1 级 SD 卡 | RK3588 或树莓派 5：8 GB / 64 GB NVMe |
| 极限裁剪 | 1 核 / 1 GB / 8 GB（只开 MySQL + 时序，关掉 S3 和 MQ） | —— |

ARM 仅支持 64 位（arm64）。SD 卡做主存储必须配 v1.3.12 的 WAL 调优项（`sqlite.wal_autocheckpoint` / `journal_size_limit_kb`）。数据目录预留「预估数据量 × 2 + 20%」（WAL 加备份轮转）。

---

## 备份与恢复

- **运行中**：`glaciertdb -backup <目录>`，对全部库做一致性快照，不阻塞业务；可配 systemd timer 定时跑（样例在 deploy/ 目录）；
- **停机**：直接拷贝数据目录（运行中直接拷可能拿到主库与 WAL 不一致的组合，别这么干）；
- 备份包是密文（开了加密时），恢复需要相同版本的服务和口令；密钥文件（`glacier.json`、`glacier_s3.key` 等）在数据目录里，跟着一起备。

详细流程见 [docs/部署运维.md](docs/部署运维.md)。

---

## 已知限制

把不能做的说在前面，免得踩坑：

- 不支持 MySQL 预处理语句（`COM_STMT_*`）、XA、`LOAD DATA`、主从复制；就是单机，没有集群计划；
- Redis 是 RESP2 语义（新客户端会自动兼容），事务没有 WATCH；
- AMQP 是基本队列，没有镜像队列和仲裁队列；
- 库之间没有跨库 JOIN；`DROP DATABASE` 直接删文件，不可恢复；
- 异步任务的 SCRIPT 后处理自 v1.3.13 起临时停用（安全整改，计划 v1.3.14 恢复）。

---

## 文档

| 手册 | 内容 |
| --- | --- |
| [配置说明](docs/配置说明.md) | 全部配置项与默认值 |
| [系统SQL](docs/系统SQL.md) | 管理面语句速查 |
| [时序数据库](docs/时序数据库.md) | schema、rollup、冷压缩 |
| [异步任务](docs/异步任务.md) | SUBMIT 语法、调度、回调 |
| [S3对象存储](docs/S3对象存储.md) | 认证、API、图片处理、数据北上 |
| [键值队列](docs/键值队列.md) | Redis 命令集与内存治理 |
| [消息队列](docs/消息队列.md) | AMQP/MQTT 语义 |
| [部署运维](docs/部署运维.md) | 部署、升级、备份、监控、排障 |

---

## 版本

- **v1.3.13**（当前）：S3 去重存储与秒传、gzip 压缩、打包下载、数据同步上云、KV 驱逐优先级与 SQL 读穿透、实例 ID 与排障头、版本隐藏、心跳上报；
- **v1.3.12**：TLS 全面支持、在线备份 `-backup`、S3 并行上传重构；
- **v1.3.11**：S3 存储层重写（分片直写、零拷贝合并）、租户隔离安全大修（修复 v1.3.10 的 4 个越权漏洞）；
- **v1.3.10**：已撤版。

更早与更细的变更见 [CHANGELOG.md](CHANGELOG.md)。

## License

[MIT](LICENSE)
