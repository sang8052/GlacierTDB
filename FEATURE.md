# FEATURE.md — 版本路线规划

> **愿景：多中间件一篮子混合部署**——以 SQLite 为唯一存储内核，把 MySQL 协议网关、时序冷库、
> S3 对象存储、Redis 键值队列、RabbitMQ 消息队列装进**同一个静态二进制、同一个数据目录**。
> 面向工控机 / 物联网边缘环境：一台设备上替代 MySQL + Redis + RabbitMQ + MinIO 的组合，
> 免去繁杂依赖安装、多进程守护与分布式备份——备份 = 拷贝目录，迁移 = 拷贝文件。
>
> 已实现能力见 [README.md](README.md) 与 [docs/](docs/) 四册、[CHANGELOG.md](CHANGELOG.md)；
> 本文件只放**规划**。版本号发布时沿用 `.alpha` 后缀惯例（如 v1.0.10.alpha）。

---

## 0. 端口复用与服务声明协议（v1.2.0 前随路线逐步落地）

**一个端口承载多协议**：MySQL（现）/ Redis（v1.2.0）/ RabbitMQ（v1.3.0）共用 3309。
客户端连接后**先声明服务类型**，不声明默认 MySQL：

```
mode:mysql
mode:rabbitmq
mode:redis
```

| 机制 | 说明 |
| --- | --- |
| 声明行（首选） | 连接建立后第一个包为 ASCII 单行 `mode:<service>`，网关消费该行后进入对应协议栈；自定义客户端 / 调试 / 透传工具用 |
| 协议嗅探（兜底） | 原生客户端无法插声明行：AMQP 协议头（`AMQP` 魔数）→ RabbitMQ；RESP 命令特征 → Redis；客户端静默等服务端握手 → MySQL。嗅探不匹配任何特征时按 MySQL 处理 |
| 默认 | 不声明、不可识别 = `mode:mysql`——现有全部 MySQL 客户端零改动 |

- 声明行为**连接级一次性**，会话内不切换服务；
- S3 对象存储**不进 3309 复用**：HTTP(S) + virtual-host + 每 endpoint 泛域名证书需要独立 TLS 监听器（v1.1.0，见 §2）；
- QoS、防爆破、审计、权限体系对所有协议统一生效（同一账号体系，见 v1.0.10 §1.3）。

---

## 1. v1.0.10 —— 运维与查询能力补强 ✅（已随 v1.0.10.alpha 发布，2026-10-22）

### 1.1 查询 QoS 查询 / 限制（全系统 · 单库）

对**网关查询**做服务质量控制：全系统与单库两级配额，读 / 写 / 异步任务执行统一记账。

- **配额维度**：`qps`（每秒查询数，令牌桶）、`concurrency`（并发执行数）、`rows_per_sec`（扫描行吞吐，防大查询拖垮磁盘）；
- **范围**：`qos.global`（全系统默认）+ `qos.databases.<库名>`（单库覆盖）；后续可扩展到用户级（随 v1.0.10 权限体系同表记账）；
- **配置（config.yaml）**：

```yaml
qos:
  on_exceed: wait          # wait = 排队等待（默认）| reject = 立即拒绝
  global:
    qps: 0                 # 0 = 不限
    concurrency: 0
    rows_per_sec: 0
  databases:
    backup: { qps: 500, concurrency: 16 }
```

- **管理面热改**（重启失效回到配置文件值，与现有并发热改一致）：
  `SET QOS GLOBAL QPS=500` / `SET QOS DB backup QPS=100` / `SET QOS DB backup CLEAR`；
- **查询**：`SHOW QOS`（配置 + 实时统计：通过 / 排队 / 拒绝计数、当前并发、平均排队时长）；
  `SELECT * FROM qos_status` 表形式（支持 WHERE 过滤）；
- **异步任务**：执行同样消耗配额；`query_jobs` 新增 `qos_wait_ms` 列（排队耗时），`SHOW QUERY JOBS` 可见；
- 慢查询日志标注 QoS 排队时长；拒绝时返回专用错误码与当前配额值，便于客户端退避。

### 1.2 异步任务周期定时增强 + 周期配置查询

- **每周 / 每 N 周调度**（补全周期族）：`EVERY WEEK [ON 周五] [AT '02:00']`、`EVERY 2 WEEKS ON 周一 AT '03:00'`；
  现有 `DAY / HOUR / MINUTE / SECOND / n SECONDS|MINUTES|HOURS` 保留；`ON` 接受 `周一..周日 / MON..SUN`（复用 WINDOW 周几解析）；
- **周期配置查询**：`SHOW QUERY SCHEDULE` / `SELECT * FROM query_schedule`——每个周期链（recur_group）一行：

| 列 | 说明 |
| --- | --- |
| `recur_group` / `job_id` / `sql` | 链 ID / 最近任务 ID / 语句 |
| `schedule` | 人类可读周期表达（`EVERY WEEK ON周五 AT 02:00` / `EVERY DAY AT 17:00` / `EVERY 30 MINUTES`） |
| `window` / `when` | 窗口（含周几前缀）、资源门槛 |
| `next_run` / `last_run` / `last_state` | 下次计划时刻 / 上次执行时刻与状态 |
| `paused` | 是否暂停（YES/NO） |

- **链级启停**：`PAUSE QUERY '<job_id>'` / `RESUME QUERY '<job_id>'`（暂停只挂起排期，不丢链；CANCEL 仍为彻底删除）；
- `query_jobs` 展示 `schedule` 人类可读表达；时区跟随宿主机本地时区（与现有 AT 一致）。

### 1.3 用户权限系统（MySQL 风格）+ 防爆破

**账号入库**：不再用配置文件存用户——系统库表存储（`glacier_auth.db`，随数据目录走）：

| 表 | 内容 |
| --- | --- |
| `users` | 账号、密码（mysql_native_password 口令存储）、账户状态（OK/LOCKED/EXPIRED）、密码策略 |
| `user_grants` | 权限行：范围（全局 `*.*` / 库 `db.*` / 表 `db.t`）、权限集（SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER, ALL）、GRANT OPTION |
| `user_hosts` | IP 授权：`'u'@'host'` 的 host 部分支持 `%`、`192.168.1.%`、精确 IP、CIDR（`192.168.0.0/24`） |
| `login_attempts` | 登录审计：时间、来源 IP、账号、成功/失败、失败原因（异步归档） |
| `lock_state` | 锁定状态：锁对象（IP / 账号 / 账号+IP 组合）、锁定起止、失败计数、升级层数 |

- **兼容与自动升级**：首次启动自动迁移旧版 `config.yaml auth 段 + <db_dir>/glacier.json` → 系统表，
  旧的 `glacier` / `glacier_ro` 策略等价落表；迁移后配置文件仅保留 bootstrap 初始口令；
- **默认超级用户** `glacier`（`ALL PRIVILEGES ON *.* WITH GRANT OPTION`），行为与现状一致；
- **SQL 接口**（MySQL 语法兼容，保守改写）：`CREATE USER 'u'@'host' IDENTIFIED BY 'pwd'` / `ALTER USER` /
  `DROP USER` / `RENAME USER` / `SET PASSWORD` / `GRANT … ON db.* TO …` / `REVOKE` / `SHOW GRANTS FOR …`；
  权限校验在网关层执行（库级 + 表级，语句解析出目标表后比对），拒绝返回 1044/1142/1143 族 MySQL 错误码；
- **防爆破（重点）**：
  - 失败计数维度：**账号**、**来源 IP**、**账号+IP 组合**——三种锁定模式可配：`ip_only`（只锁 IP）/
    `account_only`（只锁账号）/ `mixed`（独立锁 IP 或独立锁账号，任一触发即拒）；
  - 策略可配：窗口失败次数阈值（如 5 次 / 5 分钟）、锁定时长（如 15 分钟，重复触发指数升级 15m→1h→24h）；
  - 锁定状态**持久化**（重启不解除），管理面 `UNLOCK ACCOUNT/IP` 手动解锁；
  - **系统层封锁（可选）**：锁定 IP 时同步下发防火墙规则，驱动可选 `ufw` / `iptables` / `firewall-cmd`，
    不指定自动识别宿主可用驱动；规则带 TTL 自动清理；容器内或无权限时自动降级为网关层软锁；
  - **异步通知**：登录失败 / 账号锁定事件 POST 到配置的告警 URL（HMAC 签名、退避重试，复用异步回调通道），
    `login_attempts` 同步落表可 SQL 查询；
- 管理面新增 `SHOW LOGIN ATTEMPTS` / `SHOW LOCKED` 视图；SQL 审计日志记录认证事件。

### 1.4 JSON 复杂查询

- **MySQL JSON 函数族兼容**：`JSON_EXTRACT` / `->` / `->>` / `JSON_CONTAINS` / `JSON_LENGTH` / `JSON_KEYS` /
  `JSON_ARRAY` / `JSON_OBJECT` / `JSON_VALID` / `JSON_SEARCH` / `JSON_SET` / `JSON_REPLACE` / `JSON_REMOVE` /
  `JSON_MERGE_PATCH`；路径表达式按 MySQL 语义（`$.a` / `$.a[0]` / `$.a[*]` / `$.**`）；
- **运算符改写**：`col->>'$.a'` 等 MySQL JSON 运算符自动改写为 SQLite JSON1 等价形式；
- **底层**：SQLite JSON1 原生能力直通（`json_each` / `json_tree` 表函数可用于展开），改写保守——
  匹配不上原样透传，宁可报 SQLite 错不改错语义；
- **索引**：`json_extract` 表达式索引可用（`CREATE INDEX ON t(json_extract(cfg,'$.a'))`），长范围过滤不全表扫。

### 1.5 Geo 格式字段查询

- **存储与类型映射**：`POINT` / `GEOMETRY` / `LINESTRING` / `POLYGON` → WKT（TEXT）或 WKB（BLOB）存储，
  建表 DDL 自动映射（沿用 DDL 词法映射风格）；GeoJSON 文本可直接存取；
- **函数（Go 标量，注册进每个后端连接，与 MD5/SHA 族同机制）**：
  `ST_Distance` / `ST_Distance_Sphere` / `ST_Within` / `ST_Contains` / `ST_Intersects` / `ST_Equals` /
  `ST_AsText` / `ST_GeomFromText` / `ST_AsGeoJSON` / `ST_GeomFromGeoJSON` / `ST_MakePoint` /
  `ST_Longitude` / `ST_Latitude` / `ST_X` / `ST_Y`；NULL 传播、单位说明（度 / 米）在文档注明；
- **查询策略**：包围盒粗筛 + 精确谓词过滤两级（无空间索引承诺，见设计红线）；GeoJSON / WKT / WKB
  输入输出可互转；与 JSON 查询叠加（GeoJSON 路径提取）。

---

## 2. v1.1.0 —— 兼容 S3 的对象存储（可选功能） ✅（已随 v1.1.0.alpha 发布，2026-10-23）

可选模块（`s3.enable: true` 开启），AWS Signature **V4** 标准签名算法，面向工控机本地对象存储
（替代 MinIO / 简单文件服务器）。实现说明见 [docs/S3对象存储.md](docs/S3对象存储.md)：
除下列 9 项全部落地外，两点实现口径——SK 落表为 **SHA-256 指纹 + AES-256-GCM 密文**
（SigV4 校验需用 SK 派生签名密钥，纯哈希无法验证签名；明文仍仅签发/轮换时返回一次）；
分片上传（multipart）按设计红线首版不承诺。

1. **存储模型**：一个 bucket 一张表（系统库，对象元数据 + 内容；超阈值内容走分块表）；
   bucket 从属用户（`bucket → owner`），命名空间隔离；对象 key / 版本 / 元数据 / ETag / 内容类型齐全；
2. **AK/SK**：一用户可配多组密钥；**AK 32 位（前缀 `AS`）、SK 48 位（前缀 `SK`）**；
   管理 SQL 签发 / 轮换 / 禁用 / 吊销密钥，SK 仅签发时明文可见一次，落表存哈希；
3. **下载限速**：单连接 bytes/s 限速、单用户并发连接上限、**多线程合并限速**（用户级总带宽聚合，
   多连接合并不超过用户总带宽）；可按 bucket 覆盖；
4. **签名鉴权扩展**：请求策略可绑定 **UA / 来源 IP / Referer / 自定义 header** 条件（参考阿里云 OSS
   的 policy 条件匹配），不满足条件拒绝（403）；**上传回调**：对象写入完成后 POST 通知（HMAC 签名、退避重试）；
5. **图片预处理**：上传/读取链路支持裁剪 / 缩放 / 格式转换 / **转 base64 输出**；**支持直接上传 base64
   图片**（解码后二进制落盘）；**多版本**（同 key 版本链，可查历史版本）、**对象锁**（WORM / legal hold）、
   **自动删除策略**（生命周期规则：按天数、按前缀到期删除）；
6. **region**：支持空 region 与 `*`——配置 `region: '*'` 时客户端传任意 region 或不传均可；
7. **virtual-host 模式**：region 非空时 `http(s)://{bucket}.{region}.{endpoint}`，否则 `http(s)://{bucket}.{endpoint}`；
   path-style 兼容保留；支持多 endpoint；HTTPS 独立端口；
8. **TLS 证书能力**：**每个 endpoint 一张独立泛域名证书**（SNI 分发）；**每个 bucket 可配自定义域名 + 独立证书**
   （自定义域名接入即签发/上传证书，SAN 映射到 bucket）；证书支持热加载（文件替换即生效）；
9. **计费统计**：`s3_usage` 表按 用户 × bucket 记账——请求数（GET/PUT/LIST/DELETE 分类）、上传 / 下载字节、
   存储空间占用、对象数；SQL 直接查询（`SELECT * FROM s3_usage WHERE owner='u'`），导出接口供计费系统拉取。

> S3 使用独立 HTTP(S) 监听器（默认 `0.0.0.0:9000`，可配多 endpoint/端口），不进 3309 复用——
> virtual-host 与每 endpoint 证书需要独立 TLS 监听（见 §0）。

---

## 3. v1.2.0 —— key-value 键值库（Redis 基础能力）

通过 `mode:redis` 在 3309 同端口提供 RESP 协议服务：

1. **基础能力**：key 类型（string / list 队列）、`GET/SET/DEL/EXISTS/EXPIRE/TTL/PERSIST/INCR/DECR/APPEND/
   MGET/MSET/KEYS/TYPE`；队列 `LPUSH/RPUSH/LPOP/RPOP/LLEN/LRANGE`（阻塞弹出可选）；
2. **TTL**：惰性过期 + 定期扫描；**自动落盘**：变更实时写 SQLite（WAL），内存为缓存层，重启无损恢复；
3. **命名空间**：**每个用户独立 namespace**（key 自动加用户前缀隔离，跨用户不可见）；
4. **内存治理**：`kv.max_memory_mb` 上限可配，超出按策略淘汰驱逐（`allkeys-lru` / `volatile-lru` /
   `noeviction` 可选）；占用/命中率 SQL 可查（`kv_stats`）。

---

## 4. v1.3.0 —— RabbitMQ 能力（基本队列）

通过 `mode:rabbitmq` 在 3309 同端口提供 AMQP 0-9-1 基本能力：

- exchange（direct / fanout / topic）+ queue + binding 的声明与绑定；`basic.publish` / `basic.consume` /
  `basic.ack` / `basic.nack` / `basic.reject`、prefetch、持久化消息；消息与消费游标落 SQLite，重启不丢；
- 管理面 `SHOW QUEUES` / `SHOW EXCHANGES`（深度、消费者数、入出速率）；
- 与异步任务回调通道互通（`CALLBACK 'amqp://…'` 由本模块自服务回环可选）；
- 定位：**基本队列通知 / 分发**，不做镜像集群、不做事务性队列——单写者模型内够用即可。

---

## 5. 存量路线余项（v1.0.x 小版本穿插消化）

> v1.0.8 前遗留的长范围查询短板与兼容余项，优先级仍在，按小版本穿插：

| 项 | 说明 | 优先级 |
| --- | --- | --- |
| rollup 多粒度预聚合 | 回填同遍落 raw + m1/h1/d1 汇总表，网关按时间范围自动路由最粗可用粒度（InfluxDB 长范围快的本质） | P0，并入 v1.0.10 后首个小版本 |
| GROUP BY 分片并行 | `GROUP BY series_id, time(N)` 按 series_id 取模分片、Go 侧按桶合并 | P1 |
| 明细导出并行（方案 B） | 按 rowid 区间并行 scan，行包并行读、顺序发送 | P1 |
| JS 后处理管道（SCRIPT） | 结果以 JSONL 流入 nodejs 子进程，产物落 `jobs/<id>.<output>`（语法位已预留） | P1 |
| MySQL 兼容 M3 | `VALUES(col)` / `ON DUPLICATE KEY UPDATE` / `FOR UPDATE` 剥离 / `SET FOREIGN_KEY_CHECKS` / `CONCAT_WS` / `LPAD` / `RPAD` / `DATE_FORMAT` 周编号族 | P2，按需 |

---

## 6. 设计红线（始终不做）

- **单写者模型**：不做多主写、不做多机部署 / 复制协议（多机路线已从规划移除）；
- **零外部中间件依赖**：所有能力内化进单一静态二进制，不引入 etcd / consul / 外部队列等；
- **保守改写**：SQL 兼容匹配不上一律原样透传，宁可报后端错误不改错语义；每条进 tests/ 黑盒；
- **不承诺**：空间索引（geo 用包围盒粗筛）、事务性消息队列、Redis 集群协议、S3 分片上传高级语义
  （首版）、纠删码 / 跨机副本——工控机单机场景不为用不到的分布式能力付复杂度。
