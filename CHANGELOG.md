# 更新日志

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号语义化。

## [1.3.11.alpha] - 2026-10-27

> 大版本：S3 底层存储重写（分片直写 + 零拷贝合并 + Range 读）· 图片裁剪融合 ·
> S3 管理面异步任务（文本/文件检索 + 异步删除）· 空间免锁回收 · 时序大表谓词与
> ts 秒/毫秒/微秒自动换算 · **租户隔离安全大修**（4 个 CRITICAL 漏洞修复）· 性能优化。

### 安全（租户隔离专项审查）
- **[CRITICAL] S3 `x-amz-copy-source` 跨租户读**：CopyObject / UploadPartCopy 的源桶
  此前从不校验属主——任何租户凭自己桶里的 PUT + 拷贝头即可整读其他属主的对象（含任意
  版本）。现于请求入口对源桶做与目标桶同等的 `canAccess` + 策略校验，跨属主一律 403。
- **[CRITICAL] `DROP/CREATE DATABASE` 无鉴权**：任何账号（**含只读**）都可删除任意租户
  的整库。现统一走 super + 非只读门禁（`CanAdminDB`）。
- **[CRITICAL] `SUBMIT QUERY` 绕权执行**：异步引擎自开会话直跑 SQL，完全绕过语句权限层。
  现提交口收权：ATTACH/DETACH 一律拒绝；USER 账号仅允许 SELECT 形态且须逐表过授权；
  执行会话落在**提交者当前库**（不再恒用服务端默认库）。
- **[CRITICAL] WITH 前缀 DML 误判为读**：`WITH x AS (…) DELETE …` 走读路径——SELECT
  权限即可写、**只读账号可写**。现按 CTE 后主语句动词分类（`StripWithPrefix`）。
- **[HIGH] 管理面禁入普通租户**：USER 账号 `USE GlacierTDB` → 1044（此前 SHOW S3 KEYS/
  OBJECTS/USAGE、SHOW CLIENTS 等读命令无门禁，可枚举其他租户的桶/对象/密钥/账单）。
- **[HIGH] 异步任务属主隔离**：Job 增加 Owner；SHOW QUERY JOBS 按属主过滤，FETCH/CANCEL/
  DROP/PAUSE/RESUME 校验属主（super 不受限）——任务 ID 可枚举此前可跨租户读结果/杀任务。
- **[HIGH] 只读账号 `COMPRESS/THAW COLD` 写通道关闭**；COM_INIT_DB / 握手默认库补 `USE`
  同等的授权校验（防未授权库整库 schema 经 SHOW TABLES/DESC 泄露）。
- **[MED] `SET GLOBAL_THREADS/PER_DB_READ_THREADS/DB_READ_THREADS` 补 super 门禁**；
  预签名 URL `X-Amz-Date` 不可解析时 fail-closed（此前静默跳过时效校验）、`X-Amz-Expires`
  上限对齐 AWS 7 天。
- 图片处理越桶写修复：`x-gimg-save-as` 只接受**同桶 key**，出现 bucket 前缀一律 400
  （此前可把处理结果写进其他属主的桶）；`blend` 融合源恒限定同桶。

### 新增（S3）
- **图片裁剪融合 `blend`**（OpenCV 风格管道新 op）：`blend('key'[,x,y[,alpha[,cx,cy,cw,ch]]])`
  ——同桶对象作融合源，可选裁剪矩形后按权重贴入（alpha 语义 ≈ `cv2.addWeighted`）；
  GET 即时处理 / PUT 上传即融合 / POST save-as 另存三链路全支持。
- **管理面异步任务**（`SUBMIT S3 ...`，走异步引擎，FETCH/回调/EVERY 周期链与 SQL 任务一致）：
  - `SUBMIT S3 TEXT SEARCH <bucket> MATCH 'text' [PREFIX/LIKE/SIZE/MODIFIED/CREATED/LIMIT ...]`
    ——文本文件**内容级**检索（大小写不敏感，返回行号 + 命中行；content-type 语义 + 字节
    嗅探双判定，>64MB 跳过）；
  - `SUBMIT S3 FILE SEARCH <bucket> [同上过滤]` ——元数据级检索（文件名/大小/修改/创建）；
  - `SUBMIT S3 DELETE <bucket> [过滤] [DRY RUN]` ——异步批量删除（尊重对象锁/WORM，被锁
    对象报告 `locked` 不动）；
  - 过滤语义：SIZE 支持 `>|>=|=|<|<=|BETWEEN`；MODIFIED = 最新版本写入时间；CREATED =
    全版本最早写入时间；LIKE 为 SQL 通配。super 专用。
- **空间回收**：`SHOW S3 SPACE`（页/空闲页统计）、`S3 VACUUM`（增量，每步 256 页短写事务，
  WAL 下读不受影响）、`S3 VACUUM FULL`（排他重建并把存量库切换到增量模式）；后台生命周期
  扫描顺带阈值自动回收（空闲 > 128MB 且 > 10% 文件大小才动手）。
- **Range 读（HTTP 206）**：`bytes=a-b / a- / -n`，`Content-Range`/`Accept-Ranges` 齐备，
  越界 416；分块对象按 manifest 直定位分块，不整读。

### 变更（S3 存储层重写）
- **默认参数**：`max_object_mb` 512→**16384**（16GB，可改，负值不限）；`chunk_kb` 256→
  **8192**（8MB 分片）。
- **分片上传直写 + 零拷贝合并**：分片内容流式按 8MB 分块直接落桶分块表暂存区
  `(key, uploadId, partNumber*2^20+i)`（内存占用与分片大小无关，单分片可达对象上限）；
  Complete 合并**不重写数据**——versioning 桶以 uploadId 为版本号原地转正，非 versioning
  桶一条 UPDATE 改写 version_id；对象行 `manifest` 列记录各 part 分块布局供 Range 直定位
  （旧版稠密布局对象自动兼容读）。
- **写入/删除的版本域收窄**：普通 PUT/DELETE 只清理 `version_id=''` 的正式行，不再误删
  进行中分片上传的暂存行。

### 新增（时序）
- **时序大表建表谓词**：`CREATE TABLE t (...) WITH (TIMESERIES, TS_UNIT='s|ms|us',
  TS_COLUMN='ts')` ——声明后享受全套时序优化红利：**冷压缩/自动解冻/rollup 预聚合路由
  不再固定 points 表名**（任意声明表可用）；`SHOW TS TABLES [ON <db>]` 查声明；
  `REBUILD ROLLUP <db>.<tbl>` 按表回填。
- **ts 三精度自动换算**：存储精度 s/ms/us 任选，查询谓词按数值量纲自动检测
  （≥1e14 微秒 / ≥1e11 毫秒 / ≥1e9 秒）换算到存储精度——**存秒查毫秒 / 存毫秒查秒 /
  日期串谓词**均正确命中；桶宽表达式 `ts/N*N`、`time(N)` 的 N（毫秒惯例）自动换算；
  COMPRESS/THAW 边界与步长同口径换算。

### 性能
- S3 LIST/检索前缀过滤 `substr(key,1,?)=?` → **key 区间扫描**（走 PK 索引，大桶 LIST 从
  全桶扫描+排序降为前缀区间直读）；
- S3 读路径记账（GET/LIST）改**内存缓冲 + 批量落库**（每 256 次或 5 秒；此前每次读都拿
  全局写锁写一行 UPSERT，全部 S3 读相互串行——最大读吞吐瓶颈消除）；
- 对象写锁内聚合查询 2→1 条（size/计数合并为单条 SUM+CASE）；
- `likeMatch`（information_schema / SHOW 过滤）正则编译结果缓存——此前逐行编译 O(rows) 次。

## [1.3.10.beta] - 2026-10-27

> **版本基调转折**：自 1.3.10 起项目转向**稳定性更新**（性能优化 / BUG 修复 / 运维体验），
> 不再新增大特性（README 首页已声明）。

### 新增
- **图片预处理提速与结果缓存**（S3 `x-gimg-*` 链路）：
  - **多核提速**：`blur` 改为可分离两遍滑动窗口（每像素 O(1)，与核大小无关，16.16 定点
    中间量抑制两遍舍入漂移）；`grayscale` / `rotate` / `flip` / `resize` 走 RGBA 字节
    直读直写路径；全部像素级操作按行带并行（≤8 带）——4096×3072 大图多步管道由约 8 秒
    降至亚秒级；
  - **结果磁盘缓存**：GET 加工结果落盘 `<db_dir>/sys/gimg_cache/`，键 = sha256(对象身份 +
    参数规范化串)——对象覆盖/删除自动失效；并发同参数请求单飞只处理一次；容量 `s3.image_cache_mb`
    （默认 1024，`-1` 不限）超限按最久未用淘汰；`s3.image_cache` 可关；
  - **观测响应头**：`X-Using-Time`（请求整体耗时）/ `X-Gimg-Ops-Time`（管道纯处理耗时）/
    `X-Gimg-Cache: miss|hit`；
  - **语法修正**（坑 8）：ops 裸名合法（`grayscale` ≡ `grayscale()`）；非法管道
    （未知 op / 括号不闭合 / 参数非法）显式 400 `InvalidArgument`，不再静默透传原图。
- **防爆破本机回环豁免**（坑 2 场景）：127.x.x.x / ::1 / localhost **永不锁定**（计数、落锁、
  防火墙封锁三层全豁免）——本机客户端密码配错不会把 127.0.0.1 锁死在管理面之外；
  登录审计照记、账号维度计数不受影响。
- **CLI 旁路解锁**：`glaciertdb -unlock-user <账号>` / `-unlock-ip <IP>` / `-show-locks`——
  直连 `glacier_auth` 系统库做小事务（WAL 多进程并发安全、不抢目录锁），**服务运行中可用**；
  账号被锁无法登录管理面时的自救通道。
- **`mysql.enable` 配置项**：false 时 3309 不再服务 MySQL（RESP/AMQP/MQTT 不受影响）；
  与 `kv.enable` / `mq.enable` 同时关闭时不监听 3309，进程保持运行（纯 S3 等独立端口部署形态）。

### 修复
- **boto3 默认 CRC32 校验和在 TLS 端点 BadDigest**（坑 1）：新版 botocore / AWS CLI 对
  PutObject 默认追加 CRC32 并以 `aws-chunked` 流式 trailer 编码（`STREAMING-UNSIGNED-PAYLOAD-TRAILER`
  等）；服务端此前不识别该类声明，把分帧体当净荷校验导致必报 BadDigest。现按 `STREAMING-*`
  前缀统一识别为流式编码、解码后按 `x-amz-decoded-content-length` 校验净荷长度，
  boto3 默认配置（无需 `request_checksum_calculation=when_required`）即可直连；
  multipart UploadPart 同步修复。

### 变更
- **默认配置路径改为 `config.yaml`**（坑 3）：`-config` 缺省时依次查找 `./config.yaml` →
  可执行文件同目录 → `/app/config.yaml`（Docker 惯例）——Windows 裸起 exe 无需显式传参；
  Dockerfile CMD 显式参数不受影响。
- `bugs/` 目录加入 `.gitignore`（多版本/多来源 bug 反馈存档，不进仓库）。
- 全部文档与配置样例清除历史 alpha 版本号标注（历史明细仅保留于 CHANGELOG）。
- `tests/test_v1310.py` 新增（图片缓存 miss/hit/失效、裸名 op、非法管道 400、计时头、
  账号锁定 + 管理面解锁）并纳入 `tests/run_all.sh`；编排新增 3.4 段：本机回环豁免
  （proxy 网络命名空间内 40 次错误密码后正确登录仍成功）、CLI 解锁/查锁、
  `mysql.enable=false` 不监听 3309 冒烟；全套回归 **ALL TESTS PASSED**。

## [1.3.9.alpha] - 2026-10-27

### 变更
- **版本基调转折**：1.3.x 系列已完成 FEATURE 整合（MySQL 网关 / 时序冷库 / S3 /
  Redis / RabbitMQ / MQTT / 多租户 / 静态加密全量交付）——**后续版本转向稳定性维护与
  BUG 调试**，不再新增大特性（README 首页已声明）；`FEATURE.md` 删除（规划归档完成，
  历史见 CHANGELOG）。
- **数据目录分类存储**：`<db_dir>/sys/` 存系统库（glacier_auth / s3 / kv / mq / meta），
  `<db_dir>/user/` 存用户业务库——启动时自动创建子目录并**归类存量平铺库**（含 -wal/-shm
  随迁，幂等），`ListDatabases` / 明文检测 / VACUUM CLI 同步适配；根目录兼容残留库。
- 全套回归 **ALL TESTS PASSED**（27 脚本 + 四重启持久化，加密卷 + 新布局）。

## [1.3.8.alpha] - 2026-10-27

### 新增
- **底层文件默认静态加密（adiantum VFS，固定口令 `GlacierTDB`）**：
  - 默认 `security.encrypt: true`——所有 `.db` 底层文件（业务库 + 全部系统库）落盘即密文，
    SQLite/Navicat 等工具直接打开文件只能看到密文（文件头非 `SQLite format 3`），数据仅经
    3309 协议层口令登录后透明读写；显式 `encrypt: false` 可关闭；
  - **存量明文库启动自动迁移**：检测到明文库时不再拒绝启动——逐库执行
    `wal_checkpoint → VACUUM INTO 加密临时库 → integrity_check + 对象数一致性校验 →
    原子替换`，全程带真实对象数日志；明文原件保留为 `<db>.db.old-plaintext`
    （确认后删除，防误迁移兜底），校验失败自动回滚不影响原库；
  - 实测：6 库测试数据卷（含 34 对象业务库）自动迁移后全协议功能正常，容器重启数据完好。

### 变更
- **Go 依赖全量升级（安全修复）**：`rabbitmq/amqp091-go` v1.10.0→**v1.16.0**、
  `segmentio/kafka-go` v0.4.47→**v0.4.51**、`golang.org/x/image` v0.18.0→**v0.47.0**、
  `golang.org/x/crypto` v0.57.0→**v0.58.0**、`pingcap/tidb/pkg/parser`→**2026-10-27 版**、
  `ncruces/go-sqlite3-wasm/v6` v6.3.35304→**v6.3.35401**（间接依赖 x/net·x/sys·x/text·
  compress 等随升）；go 指令 1.26→**1.27.3**（构建 GOTOOLCHAIN=auto 自动拉取工具链，
  Dockerfile 已适配）；`go-mysql` v1.16.0 与 `ncruces/go-sqlite3` v0.35.6 已是最新；
- `tests/test_v138.py` 新增（加密模式建表/KV + `--check-restart` 加密重启持久化）并纳入
  `tests/run_all.sh`；全套回归 **ALL TESTS PASSED**（27 脚本 + 四重启持久化）。

## [1.3.7.alpha] - 2026-10-27

### 新增
- **OpenCV 风格图片预处理管道（S3 `x-gimg-ops`，纯 Go 实现无 CGO）**：
  - **操作集**（`|` 分隔管道，命名对齐 OpenCV）：`rectangle(x,y,w,h,R,G,B[,thickness])`
    （`thickness=-1` 填充，对齐 `cv2.FILLED`）、`circle(cx,cy,r,...)`、`line(x1,y1,x2,y2,...)`、
    `text('label',x,y,R,G,B[,scale])`（对齐 `cv2.putText`，引号内文本可含逗号）、`grayscale`
    （`cv2.cvtColor` 加权灰度）、`blur(ksize)`（`cv2.blur` 均值滤波）、`rotate(90|180|270)`、
    `flip(0|1|-1)`（`cv2.flip` 语义）；与既有 `x-gimg-crop/resize/format/quality` 组合，
    执行顺序 crop → ops → resize → format；
  - **三条链路**：GET 即时处理返回（预览）；PUT 上传即处理落盘（含 base64 直传）；
    **POST `?x-gimg-ops=...&x-gimg-save-as=bucket/key`** 已有对象处理后另存新对象
    （标注结果持久化）；
  - **文字渲染**：默认内置 bitmap 字体（ASCII）；新配置 `s3.image_font` 指向 ttf/ttc
    文件后经 opentype 加载，支持中文等 TrueType 字形（新依赖 `golang.org/x/image`）。

### 变更
- `tests/test_v137.py` 新增 17 项（Pillow 像素级断言：绘制着色/灰度一致/几何转置与互换/
  blur 边界混合/PUT 落盘/POST 另存/错误路径）；`tests/run_all.sh` 纳入（pip 依赖加 pillow）；
  全套回归 **ALL TESTS PASSED**（26 脚本 + 三重启持久化）。

## [1.3.6.alpha] - 2026-10-27

### 新增
- **多租户强化（补齐 4 项缺口）**：
  - **库可见性过滤**：USER 类型账号 `SHOW DATABASES` / `information_schema.SCHEMATA` 仅显示被授权的库（`GRANT ... ON db.*` 授权集合；`ON *.*` 通配全库）——不再向租户泄露其他库存在性；
  - **CREATE USER ... DEFAULT_DB**：`CREATE USER 't1' IDENTIFIED BY '...' USER DEFAULT_DB db_a`（或 `SET USER 't1' DEFAULT_DB db_a` 热改）——租户登录即落自己的库（不再落全局 `backup`）；
  - **USE 权限校验**：USER 类型 `USE <db>` 无授权报 `1044 Access denied`（此前切库成功仅语句被拒）；
  - **账号级配额**：`SET USER 't1' QOS QPS=5 CONCURRENCY=2`（或单参 / `CLEAR` 清除）——USER 类型每条语句按账号 QPS 最小间隔节流（等待语义）+ 并发执行上限（信号量），配额落系统库 `user_quotas` 热改即生效；`SHOW USERS` / `SELECT * FROM users` 新增 `qps` / `concurrency` 列。
- **S3 响应头**：`Server: GlacierTDB/<版本号>`（如 `GlacierTDB/1.3.6.alpha`）；`x-amz-request-id` 由 12 位短随机改为 **36 位 UUID v4**。

### 变更
- **默认配置仅建一个管理账号**：`auth.password` 默认改为 `GlacierTDB@2026`；`readonly_password` 默认空 = **不创建** `glacier_ro`（需要只读账号时在配置中填密码启用）——此前默认 `iloveyou~` 并自动创建只读账号；
- `tests/test_v136.py` 新增 15 项（可见性过滤 / DEFAULT_DB / USE 1044 / REVOKE-GRANT 回归 / QPS 节流与 CLEAR / S3 头）；全套回归 **ALL TESTS PASSED**（25 脚本 + 三重启持久化）。

## [1.3.5.alpha] - 2026-10-27

### 新增
- **S3 限制清理**：
  - **UploadPartCopy（分片拷贝）**：`PUT ?partNumber=N&uploadId=X` 携带 `x-amz-copy-source`（可带 `versionId`）从源对象拷贝分片，免网络重传；可选 `x-amz-copy-source-range: bytes=first-last`（闭区间）；应答为 `CopyPartResult` XML（ETag/LastModified 在 body）；拷贝分片与直传分片可混合 Complete；源不存在 `NoSuchKey`、越界 Range 400；
  - **未完成上传自动生命周期回收**：新配置 `s3.abort_incomplete_days`（默认 7，0=关闭）——生命周期周期任务（`lifecycle_interval_sec`）自动 Abort 超龄未完成分片会话并回收分片存储；`SHOW SERVER CONFIG` 暴露 `s3.abort_incomplete_days`；
  - **单对象上限可解除**：`s3.max_object_mb` 支持负值（如 `-1`）= 不限制（对象与分片大小校验全部放行，分块存储本为流式不受内存约束）；base64 图片直传路径同步适配。
- **Redis 限制清理（kv 引擎重构为 session 化 dispatch）**：
  - **事务**：`MULTI` / `EXEC` / `DISCARD`——命令入队应答 `QUEUED`，`EXEC` 返回逐条结果数组（数组元素可为任意帧；单条错误不中断后续）；嵌套 MULTI / 事务外 EXEC·DISCARD 报错；`WATCH` 明确报不支持（队列执行语义）；事务内推送帧一并写入应答数组保序；
  - **发布订阅**：`SUBSCRIBE` / `UNSUBSCRIBE` / `PSUBSCRIBE` / `PUNSUBSCRIBE` / `PUBLISH`——确认帧 `[kind, name, :count]`（integer 计数）、消息帧 `[message, ch, payload]` / `[pmessage, pattern, ch, payload]`；同连接 channel+pattern 双收（Redis 语义）；`PUBLISH` 返回去重接收连接数；按命名空间隔离（跨账号不可见）；连接断开自动注销；
  - **SCAN 真游标**：`SCAN cursor [MATCH p] [COUNT n] [TYPE t]`——排序快照 + 索引游标，每批返回 COUNT（默认 10，上限 10000）个匹配 key，游标推进直至 0；遍历期间写入可能漏见/重见（Redis 游标语义允许）；`redis-cli --scan` / `scan_iter` 多批遍历实测与 `KEYS` 等价。

### 变更
- `tests/test_v135.py` 新增 35 项（UploadPartCopy 整对象/Range/越界/混合合并、会话滞留与自动回收配置、MULTI/EXEC 全语义、发布订阅全链路含 redis-py pubsub、SCAN 游标遍历）；`tests/run_all.sh` 纳入；全套回归 **ALL TESTS PASSED**（24 脚本 + 三重启持久化）。

## [1.3.4.alpha] - 2026-10-27

### 修复
- **空值应答挂死 RESP3 客户端（反馈①-补）**：v1.3.3 的 HELLO 修复让 redis-py ≥8（默认 RESP3
  解析器）可直连，但空值仍回 RESP2 帧——RESP3 解析器对 `$-1` 按「读 -1 字节」处理，
  `GET` miss / `MGET` 部分 miss / `BLPOP` 超时 / `LPOP` 空列表等一切空值应答挂死至 socket 超时：
  - 修复：`HELLO 3` 协商后该连接的空值应答改发 RESP3 统一 null 帧 `_`（`RespWriter.Resp3` 标志）；
    协议本体仍 RESP2（HELLO 应答 `proto` 恒 2，非空值帧两种解析器帧格式相同）；
  - RESP2 连接（未 HELLO / `HELLO 2` / `protocol=2`）空值语义维持 `$-1` / `*-1` 不变；
  - `GET miss 0ms 返回 None`（v1.3.3 实测挂死）、`MGET [b'x', None]`、`BLPOP 超时 None` 实测通过。

### 变更
- `tests/test_v134.py` 新增 16 项（RESP3 空值全场景 + 帧形态 `_`/`$-1` 双路断言 + RESP2 回归）；
  `tests/run_all.sh` 纳入；全套回归 **ALL TESTS PASSED**（23 脚本 + 三重启持久化）。

## [1.3.3.alpha] - 2026-10-23

### 新增
- **MySQL 8 兼容 M4 批**（参考 MySQL 8 语法标准逐项补齐）：
  - **运算符**：`&&`→AND、`||`→OR（MySQL 默认语义，先于 CONCAT 展开）、`!`→NOT（非 `!=`）、`XOR`→`<>`、`a DIV b`→整除、`RLIKE`→REGEXP（注册 Go `regexp()` 函数）、`BINARY 'x'`→`CAST('x' AS BLOB)`、`0x1F` 十六进制字面量→`x'..'` blob、`N'..'`/`_utf8mb4'..'` 字符集引导符剥除（跨引号边界安全扫描）；
  - **函数**：LOCATE/CHAR_LENGTH/REPEAT/REVERSE/SPACE/STRCMP/ASCII/UNHEX/FORMAT；MOD/FLOOR/CEIL/POW/SQRT/LOG 族/三角/RADIANS/DEGREES/SIGN/TRUNCATE；YEAR/MONTH/DAY 族/DAYOFWEEK/WEEKDAY/WEEK/QUARTER/DAYNAME/MONTHNAME/DAYOFYEAR/LAST_DAY/TO_DAYS/FROM_DAYS/SEC_TO_TIME/TIME_TO_SEC（Go 注册进 SQLite）；DATE_ADD/DATE_SUB/ADDDATE/SUBDATE/TIMESTAMPADD（月末 clamp 的 MySQL 日历语义）、EXTRACT(unit FROM x)、TRIM(BOTH|LEADING|TRAILING ... FROM)、POSITION(x IN y)、SUBSTRING(x FROM n FOR m)、MID、UNIX_TIMESTAMP(x)→整数、SYSDATE、ISNULL；CAST/CONVERT 类型词根映射（含 `CONVERT(x USING cs)`）；GROUP_CONCAT `SEPARATOR` 语法（幂等改写，防定位器自噬死循环）；
  - **DDL**：`ENUM/SET/JSON/BIT/TINYBLOB/MEDIUMBLOB/LONGBLOB/SERIAL/NVARCHAR` 类型映射；列级 `COLLATE`/`CHARACTER SET`/`USING BTREE` 剥除；表尾 `AUTO_INCREMENT=`/`CHARSET=`/`COLLATE=`/`COMMENT=`/`ROW_FORMAT=` 等属性与 `PARTITION BY` 子句（仅 CREATE TABLE）；`ALTER TABLE ADD|DROP INDEX|KEY`→CREATE/DROP INDEX、`MODIFY/CHANGE COLUMN`、`ALTER ... SET/DROP DEFAULT`/`AUTO_INCREMENT=`/`ENABLE|DISABLE KEYS` 等 no-op 兼容；`RENAME TABLE a TO b [, c TO d]`；`CREATE TABLE t LIKE src`；`INSERT INTO t SET a=1, b=...`；`UPDATE/DELETE ... ORDER BY ... LIMIT n`→rowid 子查询；
  - **维护语句应答**：ANALYZE/CHECK/OPTIMIZE/REPAIR TABLE（四列 OK 结果集）、CHECKSUM TABLE；
  - **用户变量**：`SET @a = expr` 会话登记，语句中 `@a` 字面量替换（`@@系统变量` 不受影响）。
- **时序冷压缩与固化**：`points`（series_id/ts/val 约定）按时间步长分块 gzip 压缩进 `points_cold`（varint ts 增量 + float64 原码，字节级无损），删除热表数据行完成固化——
  - 命令：`COMPRESS COLD points BEFORE <time> [STEP n DAY|HOUR]`、`THAW COLD points [ALL|BEFORE t|CHUNK ms]`、`SHOW COLD [points]`（块数/行数/压缩比）；
  - **透明性**：读查询解析 ts 谓词，触及冻结区间自动解冻（无谓词保守全解）；写入撞冻结区间 PK 冲突自动全解重试；解冻期间临时摘除 rollup 触发器防重复计数（解冻后 `SUM(cnt)` 与全量一致）；
  - 策略：`timeseries.cold_after_days`（>0 时后台周期固化全部库）+ `cold_scan_min_sec`。
- **列类型映射（colmeta 系统库 glacier_meta）**：每库每表登记「列 → MySQL 原始类型」——
  - **落盘**：DOUBLE/FLOAT/DECIMAL 一律 SQLite TEXT 存储（防 15 位浮点格式化截断），INSERT/UPDATE 中这些列的裸数字字面量自动加引号原样保存；
  - **读出**：结果集按映射还原 float64 并回放 MySQL 类型码（DOUBLE=5/NEWDECIMAL=246，pymysql 正确得到 float/Decimal）；WHERE 比较与 ORDER BY 的浮点列引用自动包 `CAST(col AS REAL)`（TEXT 存储下的数值语义）；
  - **反射回放**：SHOW COLUMNS / SHOW CREATE TABLE / information_schema.COLUMNS 优先回放建表时的 MySQL 原始类型（ENUM('a','b')/DECIMAL(8,2)/BIT(1) 原样）；
  - CREATE/ALTER（ADD/DROP/RENAME COLUMN、CHANGE）/DROP/RENAME TABLE/LIKE 全路径维护映射（进程内缓存，写路径失效）。
- **MQTT 接入（ws + tcp，桥接 RabbitMQ）**：MQTT 3.1/3.1.1 over TCP 与 WebSocket（RFC 6455）——
  - 3309 复用：MQTT CONNECT 首包嗅探（0x1x）、HTTP Upgrade→`/mqtt`、`mode:mqtt` 声明行；可选独立监听 `mq.mqtt.listen`（tcp+ws 同端口）；
  - 桥接：topic `a/b/c` ↔ routing key `a.b.c`（`mqtt` topic 交换机），过滤器 `+`/`#` ↔ `*`/`#`；QoS0/1（PUBACK）、保留消息（mqtt_retains 落盘重启不丢）、clean_session=0 离线队列补投、遗嘱消息（异常断开发布）、keepalive（1.5 倍超时）；每连接单队列消费者按过滤器分发（多订阅不吞消息）；
  - 认证沿用统一账号体系（CONNECT username/password）；管理面 `mqtt_retains` 表。
- **RabbitMQ 版本串可配置**：`mq.server_version`（默认 3.13.0），Connection.Start server-properties.version 原样上报。

### 修复
- **AMQP Channel.Close-Ok 方法号错误**（历史 bug）：应答 method 51（误用 Connection.Close-Ok 号）→ 规范值 **41**；此前 pika `connection.close()` 必报 `KeyError(1310771)` 断流；
- MQTT 桥接消息的 content header 缺 2 字节 property-flags（空属性时）导致 AMQP 消费端帧解析崩溃；
- SUBACK 先于保留消息补发（MQTT 顺序语义）；
- **BUG.txt 使用反馈三项**：
  - **redis-py ≥ 8 连接即失败**：`HELLO` 此前直接报 unknown command 且 redis-py 将握手错误视为致命 → 实现最小 HELLO 应答（server map + `proto:2`，AUTH/SETNAME 子参数一并处理，`default` 等价单密码；`HELLO 9` 报 `NOPROTO`）——redis-py 8.1 实测**不传 `protocol` 直连成功**（RESP3 解析器对 RESP2 基础类型帧兼容），显式 `protocol=2` 老路径不受影响；
  - **`async_query` 段缺失静默禁用与文档「默认 true」不一致**：`Default()` 补 `enable: true`（与 kv/mq 行为对齐，段缺失即启用；显式 `enable: false` 仍生效）；
  - **`SCAN` 未实现导致 `redis-cli --scan` 静默无结果**：实现 `SCAN cursor [MATCH p] [COUNT n] [TYPE t]`（简化语义：单批全量返回 + 游标恒 0，真实 Redis 嵌套应答形态 `[cursor, [keys…]]`），`redis-cli --scan` 与 redis-py `scan_iter` 可用。

### 变更
- **DDL 类型映射行为**：`DOUBLE/FLOAT` 由映射 REAL 改为 **TEXT**（v1.3.3 类型映射核心变更；存量 REAL 表不受影响，查出无映射则维持原样）——`tests/test_mysql_compat.py` 断言同步更新；
- 新系统库 `glacier_meta.db`（列类型映射，不对客户端暴露）；`Backend` 新增 `OpenMetaDB`；
- `tests/test_v133.py` 新增 120 项（含 BUG.txt 反馈验证 9 项）+ `--check-restart` 重启持久化 4 项（冷块/rollup 一致性/保留消息）；`tests/run_all.sh` 纳入（pip 依赖加 redis）；全套回归 **ALL TESTS PASSED**（21 脚本 + 三重启持久化）。

## [1.3.2.alpha] - 2026-10-23

### 新增
- **Redis 阻塞弹出（FEATURE 未排期候选项落地）**：`BLPOP key [key ...] timeout` / `BRPOP ...`——
  - 按 key 参数顺序扫描，取首个非空 list 弹出一个元素（BLPOP 左端 / BRPOP 右端），应答 `[key, value]` 二元数组；全部为空则阻塞等待，任一 key 被 LPUSH/RPUSH **立即唤醒**（广播轮换通道，无轮询），超时返回空数组（RESP2 `*-1`）；
  - `timeout` 支持小数秒，`0` = 无限等待；非 list 键按扫描顺序到达时 `WRONGTYPE`；负超时报错；
  - 同 key 多个阻塞客户端按唤醒顺序竞争，未抢到者继续等待（Push 语义对齐 Redis）。
- **AMQP 发布确认与 mandatory 回退（FEATURE 未排期候选项落地）**：
  - **publisher confirm**：`confirm.select`（nowait 支持）后通道进入确认模式，每条 publish 按通道内发布序号逐条 `basic.ack`，发布失败（如超 `max_message_mb`）`basic.nack`——此前仅回 `confirm.select-ok` 不发确认帧；
  - **basic.return**：`basic.publish` 带 `mandatory=true` 且路由到 0 条队列时，以 `basic.return`（**312 NO_ROUTE**，exchange / routing-key 原样）+ 原 header/body 回退发布方；confirm 模式下回退后仍 ack（已处理语义，对齐 RabbitMQ）；未知交换机按不可路由处理。

### 修复
- `docs/消息队列.md` 正文嵌入的 2 个 NUL 字节（AMQP 协议头魔数字面量）导致 grep 按二进制处理——改为转义写法。

### 变更
- `mq.Store.Publish` 返回值从 `error` 改为 `(路由队列数, error)`（回退判定用）；KV `Pop` 抽取 `popLocked` 供阻塞弹出复用；
- `tests/test_v132.py` 新增 13 项（BLPOP/BRPOP 非阻塞命中·方向·多 key·Push 唤醒·超时·WRONGTYPE·非法超时；confirm 逐条 ack；mandatory 回退 312/内容原样·可路由不回退·confirm+回退并存）；`tests/run_all.sh` 纳入；全套回归 ALL TESTS PASSED（20 脚本 + KV/MQ 双重启持久化）。

## [1.3.1.alpha] - 2026-10-23

### 新增
- **S3 分片上传（multipart）**：S3 标准分片上传全操作——
  - **API**：`POST /{bucket}/{key}?uploads`（InitiateMultipartUpload，返回 UploadId）/ `PUT ?partNumber=N&uploadId=`（UploadPart，响应 ETag = 该 part 内容 MD5）/ `POST ?uploadId=`（CompleteMultipartUpload，body 为 Part 列表 XML，ETag = MD5(各 part MD5 顺序拼接)+"-N"，S3 复合 ETag 语义）/ `DELETE ?uploadId=`（Abort，204）/ `GET ?uploadId=`（ListParts）/ `GET /{bucket}?uploads`（ListMultipartUploads）；
  - **校验**：partNumber 1..10000、Complete 所列 part 齐全（缺片 `InvalidPart`）+ 序号严格递增（`InvalidPartOrder`）+ ETag 与已上传分片一致；同序号重传覆盖（最后一次为准）；合并后大小受 `max_object_mb` 约束；未知 uploadId `NoSuchUpload`；
  - **存储**：`glacier_s3.db` 新增 `s3_uploads`（会话，含对象属性）+ `s3_parts`（分片元数据）；分片内容存桶分块表（`chk_<bucket>`，`key='!mpu!<uploadId>'`，`seq=partNumber` 单行 BLOB）；Complete 按 part 顺序合并走对象两段式写入（inline/chunk），成功后清理分片与会话；`DROP BUCKET FORCE` 顺带回收未完成会话；
  - **管理面**：`SHOW S3 UPLOADS [<bucket>]` / `SELECT * FROM s3_uploads`（分片数 / 字节聚合）；记账沿用 `s3_usage`（UploadPart 记 PUT + bytes_in，Abort 记 DELETE）；完成/覆盖写触发上传回调。
- **异步调度补齐每月/每年**：`EVERY MONTH [ON <1-31>] [AT 'HH:MM']`、`EVERY YEAR [ON '<MM-DD>'] [AT 'HH:MM']`、`EVERY n MONTHS|YEARS`——首次排期与周期复发均日历感知（月尾日 clamp，如 1/31 → 2/28；2/29 平年 clamp 到 2/28）；ON 省略取提交日；`SHOW QUERY SCHEDULE` / `query_schedule` 的 schedule 表达与 next_run 展示（`EVERY MONTH ON 15 AT 03:00` 形式）。
- **SCRIPT 后处理支持 PHP**：`.php` 脚本走 `async_query.php_path` 运行时（与 nodejs/python 同机制：script_dir 白名单、TIMEOUT 超时、CANCEL 中断、stderr 摘要进错误消息）；未配置 `php_path` 时任务 ERROR 并提示 `script runtime disabled (set async_query.nodejs_path / python_path / php_path)`。

### 变更
- `config.yaml` `async_query` 段新增 `php_path`；系统库 `glacier_s3.db` 新增 `s3_uploads` / `s3_parts` 表（幂等建表，存量升级无损）；
- `tests/test_v131.py` 新增 44 项（multipart 全流程 / 复合 ETag 语义 / Abort / 每月·每年排期与月尾 clamp / PHP 错误路径）；`tests/run_all.sh` 纳入（SCRIPT 夹具补 `pipe.php`）；全套回归 ALL TESTS PASSED（19 脚本 + KV/MQ 双重启持久化）。

## [1.3.0.alpha] - 2026-10-23

### 新增
- **RabbitMQ 能力 · AMQP 0-9-1 基本队列（mode:rabbitmq，FEATURE §4）**：3309 同端口 AMQP 0-9-1 服务（`mode:rabbitmq` 声明行或 AMQP 魔数嗅探进入）——
  - **声明绑定**：exchange（direct / fanout / topic）+ queue + binding 的声明与绑定（含默认交换机 `""` 直达队列）；`Exchange.Delete` / `Queue.Delete` / `Queue.Purge` / `Queue.Unbind`；
  - **生产消费**：`basic.publish` / `basic.consume` / `basic.ack` / `basic.nack`（requeue 重投 + redelivered 标志）/ `basic.reject` / `basic.get`（轮询）/ `basic.qos`（prefetch 未 ack 不续投）/ `basic.cancel` / `basic.recover`；
  - **topic 路由**：`*` 单词、`#` 零或多词通配（RabbitMQ 语义）；
  - **持久化**：消息与消费游标落 SQLite（`glacier_mq.db`），durable 队列 + persistent 消息**重启不丢**；连接断开/通道关闭未确认消息自动重入队（redelivered）；`mq.max_message_mb` 单条消息上限；
  - **认证**：SASL PLAIN 沿用统一账号体系（host 授权 / 锁定 / 登录审计 / 防爆破一致生效），命名空间按登录账号隔离；
  - **管理面**：`SHOW QUEUES`（深度 / unacked / 消费者数 / published / delivered / acked）、`SHOW EXCHANGES`（类型 / 绑定数）、`SELECT * FROM mq_queues / mq_exchanges`；
  - **异步任务互通**：`SUBMIT QUERY … CALLBACK 'amqp://user:pass@host:3309/' QUEUE '<队列>'` 自服务回环（amqp091-go 客户端实测互通）。
- **端口复用补齐**：`mode:rabbitmq` 声明与 AMQP 魔数嗅探从占位转为实装（FEATURE §0 全部落地：mysql / redis / rabbitmq 三协议同端口）。

### 修复
- **异步回调队列/主题名带引号落库（存量）**：`SUBMIT … QUEUE 'q'` / `TOPIC 't'` 的值此前未剥离单引号，实际发布到名为 `'q'` 的队列（含引号字符）——现与其他选项一致剥离引号。

### 已知问题
- pika 1.4.4 客户端在 `connection.close()` 关闭握手时自身抛 `KeyError`（会话内操作全部正常；对任何 AMQP 服务端同样复现的客户端缺陷），测试中关闭调用已容错；amqp091-go（Go）无此问题。
- `channel.flow` 仅回显、事务（Tx）为最小空实现、`basic.return`（mandatory 回退）未实现——定位为基本队列通知/分发（FEATURE 红线：不做镜像集群、不做事务性队列）。

### 变更
- 系统库新增 `glacier_mq.db`；`config.yaml` 新增 `mq` 段（`enable` / `max_message_mb`）；
- `tests/test_v130.py` 新增 28 项 + 重启持久化 1 项；`tests/run_all.sh` 纳入并扩展步骤 3.5（双模块重启恢复验证）；全套回归 ALL TESTS PASSED。

## [1.2.0.alpha] - 2026-10-23

### 新增
- **Redis 键值/队列基础能力（mode:redis，FEATURE §3）**：3309 同端口 RESP2 协议服务——
  - **基础能力**：string 类型 `GET/SET（EX|PX|NX|XX）/DEL/UNLINK/EXISTS/APPEND/INCR/DECR/INCRBY/DECRBY/MGET/MSET/KEYS/TYPE`；list 队列 `LPUSH/RPUSH/LPOP/RPOP（含 count）/LLEN/LRANGE`（负下标支持）；另有 `PING/ECHO/QUIT/SELECT/INFO/FLUSHDB/COMMAND`；
  - **TTL**：`EXPIRE/PEXPIRE/TTL/PTTL/PERSIST`，惰性过期 + 定期扫描（`kv.scan_interval_sec`）；
  - **自动落盘**：变更实时写 SQLite（`glacier_kv.db`，WAL），内存为缓存层，**重启无损恢复**（docker restart 后字符串/队列/TTL 全部还原，测试覆盖）；
  - **命名空间**：每用户独立 namespace（登录账号隔离，跨用户 key 不可见）；
  - **内存治理**：`kv.max_memory_mb` 上限 + 淘汰策略 `allkeys-lru` / `volatile-lru` / `noeviction`（超限 OOM 拒写）；`SHOW KV STATS` / `SELECT * FROM kv_stats` 查占用、命中率、过期/淘汰计数；`SET KV MAX_MEMORY <mb>` / `SET KV EVICTION <policy>` 管理面热改。
- **3309 端口复用（FEATURE §0，服务声明协议）**：连接首包 `mode:<service>` 声明行（连接级一次性，`mysql`/`redis`/`rabbitmq`）优先；否则协议嗅探——RESP 首字节（`* + - : $`）→ Redis、AMQP 魔数（`AMQP`）→ RabbitMQ（v1.3.0 占位）、嗅探窗口内静默 → MySQL（默认，存量客户端零改动）；`mux_sniff_ms` 配置嗅探窗口（默认 300ms，高 RTT 环境可调大；`kv.enable: false` 可关闭嗅探恢复纯 MySQL 零延迟）。
- **认证统一账号体系**：RESP `AUTH <password>` / `AUTH <user> <password>` 沿用 users 表（host 授权 / 账号锁定 / 登录审计 / 防爆破计数与 MySQL 侧一致）；未认证命令 `NOAUTH` 拒绝。

### 变更
- 系统库新增 `glacier_kv.db`（不对客户端暴露、禁 CREATE/DROP）；`config.yaml` 新增 `kv` 段与顶层 `mux_sniff_ms`；
- `tests/test_v120.py` 新增 69 项 + 重启持久化检查 5 项；`tests/run_all.sh` 新增步骤 3.5（容器重启后数据恢复验证）；全套回归 ALL TESTS PASSED。

## [1.1.1.alpha] - 2026-10-23

### 新增
- **rollup 多粒度预聚合（存量路线 P0）**：时序标准 schema `points(series_id, ts, val)` 写入**同遍落** `points_m1` / `points_h1` / `points_d1` 汇总表（1 分钟 / 1 小时 / 1 天桶，cnt/cntv/sum/min/max，INSERT 触发器驱动、首次写入自动建表建触发器）；网关**自动路由**——带时间范围的聚合查询（COUNT/SUM/MIN/MAX/AVG）改走「汇总表满桶 + 原始表边缘桶」UNION，选取可用的最粗粒度（桶宽整除 GROUP BY 桶宽），**结果与全量扫原始表精确一致**（边缘桶回退原始行，无近似）；`REBUILD ROLLUP <db>` 存量回填（超级用户，写锁长持）；`rollup.disable: true` 可整体关闭。⚠️ 对既有数据启用 rollup 后请执行一次 `REBUILD ROLLUP` 补齐历史。
- **GROUP BY 分片并行（存量路线 P1）**：`GROUP BY <series列>, (ts/N*N)`（含 `time(N)` 原始形态）按 series 列**取模分片**并行执行、Go 侧按 (series, 桶) 键合并部分聚合（COUNT/SUM/MIN/MAX/AVG）；`/*+ PARALLEL(n) */` 显式触发或按 `scan_rows` 阈值自动触发；`EXPLAIN PARALLEL` 展示 `group-shard` 计划。
- **明细导出并行·方案 B（存量路线 P1）**：单表明细 SELECT 按 rowid（或单列主键）**区间并行 scan、行包并行读、按分片顺序顺序发送**；支持 WHERE 过滤、`ORDER BY 分片键`、LIMIT、`*` 列展开（真实列名）；与串行结果完全一致。
- **SCRIPT 后处理管道（存量路线 P1）**：`SUBMIT QUERY '…' SCRIPT 'pipe.js' OUTPUT done`——结果 JSONL 经 stdin 流入脚本（`.js` → `async_query.nodejs_path`、`.py` → `async_query.python_path`，空=禁用），产物落 `jobs/<id>.<OUTPUT>`；脚本强制位于 `async_query.script_dir` 白名单目录内（防任意路径执行）；TIMEOUT 超时中断、CANCEL 可杀、stderr 摘要进错误消息；状态机新增 SCRIPT 阶段。
- **MySQL 兼容 M3（存量路线 P2）**：`INSERT … ON DUPLICATE KEY UPDATE` → SQLite upsert（子句内 `VALUES(col)` → `excluded.col`）；`SELECT … FOR UPDATE` / `FOR SHARE` / `LOCK IN SHARE MODE` 行锁子句剥离；`SET FOREIGN_KEY_CHECKS` 会话设置接受（探活层）；`CONCAT_WS`（跳过 NULL 参数）/ `LPAD` / `RPAD`（UTF-8 字符补齐）Go 标量函数；`DATE_FORMAT` 周编号族 `%U %u %V %v %X %x`。
- **caching_sha2_password 登录算法（MySQL 8 默认）**：`auth.auth_plugin` 配置握手默认算法（`mysql_native_password`（默认）/ `caching_sha2_password` / `sha256_password`）；账号级 `CREATE USER 'u' IDENTIFIED WITH caching_sha2_password BY 'pwd'` / `ALTER USER 'u' IDENTIFIED WITH <plugin> [BY 'pwd']`——与客户端算法不一致时自动 AuthSwitch，支持 fast-auth 缓存与 RSA 全认证（非 TLS 密码交换，RSA 密钥 `<db_dir>/glacier_rsa.pem` 首启自动生成 2048 位）；**改密自动失效登录缓存**（`SET PASSWORD` / `ALTER USER` / `GRANT … IDENTIFIED BY` 后旧密码立即失效）。

### 修复
- 读语句判定容忍前置优化器 hint（`/*+ … */ SELECT …` 不再被误判为写语句返回 OK 包）。
- rollup 懒建缓存失效问题：库被 DROP/CREATE 重建（或汇总表被删）后缓存命中不再跳过建表（命中后 sqlite_master 验证，缺失自动重建）。

### 已知问题
- **PyMySQL 2.x（2.2.8）客户端缺陷**：`caching_sha2_password` RSA 全认证路径函数缺少 return（隐式返回 None），**首次登录（缓存未命中）任何 MySQL 8 服务端都会崩溃**——非本项目问题；规避：使用 `mysql` CLI / JDBC / Go 驱动等客户端、或 `pip install 'PyMySQL<2'`；`tests/test_v111.py` 内置外科手术式修补（仅测试生效）。fast-auth（缓存命中）路径不受影响。

### 变更
- `config.yaml` 新增 `rollup` 段（默认开启，`disable: true` 关闭）与 `auth.auth_plugin`；`tests/test_v111.py` 新增 37 项；全套回归 ALL TESTS PASSED（16 个测试脚本）。

## [1.1.0.alpha] - 2026-10-23

### 新增
- **兼容 S3 的对象存储（可选模块，`s3.enable: true`）**：AWS Signature **V4** 标准签名鉴权（表头鉴权 + 预签名 URL + `aws-chunked` 流式负载解码，负载哈希 `BadDigest` 校验），独立 HTTP(S) 监听器（默认 `0.0.0.0:9000`，不进 3309 复用）；支持 PUT / GET / HEAD / DELETE 对象、`x-amz-copy-source` 复制、ListObjects V1/V2（prefix / delimiter / 分页）、ListObjectVersions、建桶 / 删桶 / HEAD 桶、GetBucketLocation / GetBucketVersioning / PutBucketVersioning；错误码对齐 AWS XML（`NoSuchKey` / `NoSuchBucket` / `SignatureDoesNotMatch` / `InvalidAccessKeyId` / `AccessDenied` / `BucketNotEmpty` / `EntityTooLarge` / `SlowDown` 等）。
- **存储模型（一 bucket 一表）**：系统库 `<db_dir>/glacier_s3.db`（不对客户端暴露）——`s3_buckets` / `s3_keys` / `s3_usage` / `s3_lifecycle` / `s3_domains` / `s3_policies` + 每桶一张对象表（元数据 + 内联内容）与分块表（超过 `inline_max_kb` 阈值的内容按 `chunk_kb` 分块）；对象元数据齐全（key / 版本 / ETag(MD5) / Content-Type / Cache-Control / Content-Disposition / `x-amz-meta-*` 自定义元数据）；桶从属用户（`bucket → owner`），命名空间隔离（super 用户跨桶放行）。
- **AK/SK 密钥体系**：一用户可配多组密钥；**AK 32 位（前缀 `AS`）、SK 48 位（前缀 `SK`）**；`CREATE S3 KEY [FOR u]` 签发 / `RENEW S3 KEY` 轮换 / `DISABLE`·`ENABLE` 禁用启用 / `DROP S3 KEY` 吊销——**SK 明文仅签发 / 轮换时返回一次**，落表存 SHA-256 指纹 + AES-256-GCM 密文（SigV4 需可还原 SK 派生签名密钥；加密主密钥 `<db_dir>/glacier_s3.key` 首启自动生成 0600）。
- **下载限速**：单连接 bytes/s 限速、单用户并发连接上限、**多线程合并限速**（用户级总带宽令牌桶聚合，多连接合计不超过用户总带宽）；`s3.limits` 全局配置 + `s3.bucket_limits` 按桶覆盖 + 管理面 `ALTER BUCKET <b> LIMIT CONN_BPS|USER_BPS|USER_CONNS = n` 行级热改；超并发返回 503 `SlowDown`。
- **请求策略条件（参考阿里云 OSS policy）**：密钥级 / 桶级策略 JSON 绑定 **UA / 来源 IP（精确·CIDR·前缀%）/ Referer / 自定义 header / HTTP 方法**条件，AND 语义全部满足才放行，不满足 403 `AccessDenied`；`SET S3 POLICY (KEY <ak>|BUCKET <b>) '<json>'` / `CLEAR S3 POLICY ...` / `SHOW S3 POLICIES`。
- **上传回调**：对象写入完成后 POST JSON（event / bucket / key / version_id / size / etag / content_type），**HMAC-SHA256 签名**（`X-Glacier-Signature: sha256=...`）+ 退避重试（0/1s/5s/15s）；`SET S3 CALLBACK <bucket> URL '<url>' [SECRET '<s>']` / `CLEAR S3 CALLBACK`。
- **图片预处理**：GET 查询参数 `x-gimg-crop=左,上,宽,高` / `x-gimg-resize=宽,高`（0=等比）/ `x-gimg-format=jpeg|png` / `x-gimg-quality=n` / `x-gimg=base64`（含 data URL 形式）；**base64 图片直传**（`?x-gimg=base64` 上传解码后二进制落盘）；双线性缩放纯手写、无外部依赖（解码 JPEG/PNG/GIF，编码 JPEG/PNG）。
- **多版本 / 对象锁 / 生命周期**：桶级 `VERSIONING ON` 版本链（每次 PUT 新版本号、DELETE 打删除标记、`?versionId=` 取历史版本、ListObjectVersions 列版本链）；**对象锁 WORM**——`S3 RETAIN <bucket>/<key> UNTIL '<date>' (COMPLIANCE|GOVERNANCE)`（GOVERNANCE 可用 `x-amz-bypass-governance-retention: true` 旁路，COMPLIANCE 不可）+ `S3 LEGAL_HOLD <bucket>/<key> ON|OFF`，保护期内删除 / 覆盖写一律 403；**生命周期自动删除**（`SET S3 LIFECYCLE <bucket> PREFIX '<p>' DAYS <n>`，按天数 + 前缀到期删除全部版本，后台周期扫描）。
- **region 与寻址**：`region: '*'` / 空 = 客户端任意 region 或不传均可，具体 region 则签名 scope 必须一致；**virtual-host 模式**（`http(s)://{bucket}[.{region}].{endpoint}`，region 非空时可带 region 段）+ path-style 兼容保留；多 endpoint / 多端口（HTTP 与 HTTPS 独立监听）。
- **TLS 证书能力**：每 endpoint 一张泛域名证书（SNI 兜底）；**每 bucket 可配自定义域名 + 独立证书**（`ALTER BUCKET <b> ADD DOMAIN '<host>' ISSUE` 自签发接入即用 / `CERT FILE '<pem>' KEY FILE '<key>' 上传证书`，SAN 映射 bucket）；证书**热加载**（文件 mtime 变化即重载，替换即生效）。
- **计费统计（`s3_usage`）**：按 用户 × bucket 记账——请求数（GET/PUT/LIST/DELETE 分类）、上传 / 下载字节、存储空间占用、对象数；`SELECT * FROM s3_usage WHERE owner='u'` / `SHOW S3 USAGE [FOR u]` SQL 直查；**导出接口**：`EXPORT S3 USAGE [TO '<file>']`（落 `<db_dir>/exports/`）+ HTTP `GET /_glacier/s3/usage?format=json|csv`（SigV4 鉴权，超管可拉全量、普通用户仅自己）。
- **管理面 SQL（USE GlacierTDB）**：`CREATE BUCKET` / `DROP BUCKET [FORCE]` / `SHOW BUCKETS` / `ALTER BUCKET ...`（VERSIONING / OBJECT_LOCK / OWNER / LIMIT / ADD·DROP DOMAIN）/ `SHOW S3 KEYS|USAGE|LIFECYCLE|DOMAINS|POLICIES|OBJECTS` / `SELECT * FROM s3_usage|s3_buckets|s3_keys|s3_lifecycle|s3_domains`；变更命令仅超级用户；`tests/test_s3.py` 新增 69 项。

### 变更
- `config.yaml` 新增 `s3` 段（默认 `enable: false` 关闭，其余均可省略走默认）；系统库新增 `glacier_s3.db`（与 `glacier_auth.db` 同样不对客户端暴露、禁 CREATE/DROP）。
- `tests/run_all.sh` 纳入 `test_s3.py`；全套回归 ALL TESTS PASSED（15 个测试脚本）。

## [1.0.10.alpha] - 2026-10-22

### 新增
- **查询 QoS 查询 / 限制（全系统 · 单库）**：`qos` 配置段（`qps` 令牌桶 / `concurrency` / `rows_per_sec` 三维配额，`on_exceed: wait|reject`），全系统默认 + 单库覆盖两级；读 / 写 / 异步任务执行统一记账；管理面热改 `SET QOS GLOBAL QPS=500` / `SET QOS DB backup QPS=100` / `SET QOS DB backup CLEAR`（重启失效）；`SHOW QOS` / `SELECT * FROM qos_status` 查配置与实时统计（通过 / 排队 / 拒绝 / 当前并发 / 平均排队毫秒）；`query_jobs` 新增 `qos_wait_ms` 列；慢查询日志标注 QoS 排队时长；超限拒绝返回 1040。
- **异步周期调度增强**：`EVERY WEEK [ON 周五] [AT '02:00']` / `EVERY n WEEKS [ON 周一] [AT '03:00']` 每周/每 N 周调度（首次执行自动排到下一周期时刻）；`PAUSE QUERY '<id>'` / `RESUME QUERY '<id>'` 链级启停（暂停冻结 TIMEOUT 倒计时、不丢链）；`SHOW QUERY SCHEDULE` / `query_schedule` 表查询周期配置（recur_group / schedule / window / when / next_run / last_run / last_state / paused）；`query_jobs` 新增 `schedule` 人类可读周期表达列。
- **用户权限系统（MySQL 风格，账号入库）**：不再用配置文件存用户——系统库 `<db_dir>/glacier_auth.db` 五张表（`users` / `user_grants` / `user_hosts` / `login_attempts` / `lock_state`）；首次启动**自动迁移旧版策略**（config auth 段 + glacier.json → 系统表，旧 glacier / glacier_ro 等价落表，`glacier` 仍为默认超级用户）；SQL 接口：`CREATE USER 'u'@'host' IDENTIFIED BY 'pwd' [SUPER|READONLY|USER]` / `ALTER USER` / `DROP USER` / `RENAME USER` / `SET PASSWORD [FOR u]` / `GRANT privs ON db.tbl TO 'u'` / `REVOKE` / `SHOW GRANTS [FOR 'u']`；**库级 + 表级 + IP 授权**（host 支持 `%` / `192.168.1.%` / 精确 IP / CIDR），权限校验在网关层按语句目标表执行（SELECT/INSERT/UPDATE/DELETE/CREATE/DROP/ALTER/INDEX），拒绝返回 1044/1142 族错误；账号类型 super / readonly / user 三档（`ALTER USER u READONLY` 热切换）；`users` / `user_grants` / `user_hosts` 表可 SQL 查询（不含密码）。
- **防爆破（自动锁异常 IP / 账号）**：登录失败窗口计数（`lock.enable` / `lock.window_sec` / `lock.max_failures`，IP 与账号维度阈值独立，IP 默认 30 次防误杀）；三种锁定模式 `lock.mode: ip_only | account_only | mixed`（独立锁 IP / 独立锁账号 / 混合，任一触发即拒）；锁定时长指数升级（`lock_sec` ×2 至 `lock_sec_max`）；**锁定持久化**（重启不解除）；被锁账号/IP 拒绝登录（3119）；**可选系统层封锁**：`lock.firewall: auto|ufw|iptables|firewall-cmd|off`（不指定自动识别，IP 锁定时下发防火墙 DROP 规则、TTL 到期自动解封，容器内/无权限自动降级为网关层软锁）；**登录失败/锁定事件 HTTP 异步告警**（`lock.notify_url` + HMAC 签名、退避重试）；管理面 `SHOW LOGIN ATTEMPTS [LIMIT n]` / `SHOW LOCKED` / `UNLOCK ACCOUNT u` / `UNLOCK IP x.x.x.x` / `UNLOCK ALL`。
- **JSON 复杂查询**：`->` / `->>` MySQL JSON 运算符自动改写为 SQLite JSON1 等价形式（`->` 保留 JSON 值形态：字符串加引号；支持链式 `a->'$.x'->>'$.y'`）；`JSON_LENGTH` / `JSON_CONTAINS` / `JSON_KEYS` / `JSON_SEARCH`（% / _ 通配）/ `JSON_MERGE_PATCH`（RFC 7386）Go 实现注册进每个后端连接（`JSON_EXTRACT` / `JSON_SET` 等 SQLite JSON1 原生直通）；`json_extract` 表达式索引可用（EXPLAIN 确认命中）。
- **Geo 格式字段查询**：`ST_*` 函数族（Go 标量，与 MD5/SHA 族同机制）——`ST_GeomFromText` / `ST_AsText` / `ST_GeomFromGeoJSON` / `ST_AsGeoJSON` / `ST_MakePoint` / `ST_X` / `ST_Y` / `ST_Longitude` / `ST_Latitude` / `ST_Distance`（平面）/ `ST_Distance_Sphere`（haversine 米）/ `ST_Within` / `ST_Contains` / `ST_Intersects` / `ST_Equals`；WKT / WKB / GeoJSON 三种输入自动识别互转；`POINT` / `GEOMETRY` 等建表类型直通 SQLite（WKT 文本存储）。

### 修复
- **中文周几解析恒失败（存量）**：`parseWeekday` 用 `t[0:1] == "周"` 比较单字节与三字节汉字永远不等——`WINDOW '周五 17:00-20:00'` 与新 `EVERY WEEK ON 周五` 的中文周几写法此前一律解析失败（v1.0.8 引入，测试未覆盖中文形式）。
- **管理面语句被库名前缀剥除误伤（存量）**：`RewriteDBPrefix` 会把管理面 `GRANT ... ON backup.t` 改写成 `ON t` 导致命令失配——管理面语句现跳过前缀剥除。
- 账号类型（`ALTER USER u READONLY|SUPER|USER`）热改后会话内实时生效（此前用连接建立时的快照）。

### 变更
- 认证改走用户权限系统：`glacier.json` 降级为迁移源与 legacy 镜像（改密仍同步写入，便于回退旧版），账号的权威存储为 `glacier_auth.db`；`users` 表为空时自动升级旧版用户策略；
- 管理面变更命令（SET PASSWORD / SET QOS / SET *_THREADS / KILL / VACUUM / CREATE USER / GRANT / REVOKE / UNLOCK）仅超级用户可执行（只读 1290、普通用户 1142）；
- `config.yaml` 新增 `qos` 与 `lock` 段（均可省略走默认）；
- `tests/test_v110.py` 新增 51 项；全套回归 ALL TESTS PASSED（15 个测试脚本）。

## [1.0.9.alpha] - 2026-10-22

### 修复
- **文本值中的引号转义被多余持久化（BUG.txt）**：MySQL 协议客户端（pymysql / mysql CLI / SQLAlchemy 等）把参数值按 MySQL 规则转义后拼进 SQL 文本（`"` → `\"`、`'` → `\'`、`\` → `\\`、换行 → `\n`），SQLite 的 `'...'` 字面量不处理反斜杠，转义序列被原样落盘——JSON 配置存入后读回带 `\` 污染、标准 JSON 库无法解析、`WHERE json_field = ...` 永远无法命中、每次「读出→改→写回」还会再叠加一层转义。网关现统一在语句进入 SQLite 前对单引号字面量做 MySQL→SQLite 还原（按 MySQL 规则解码后按 SQLite 规则重编码 `'` → `''`；`\%` / `\_`（LIKE 通配符字面量）与 `\0`（无法进入 SQLite SQL 文本）保守保留；反引号/双引号段与注释不参与扫描），同步查询、写语句与异步任务执行三条路径全部覆盖，附 Go 单元测试（`internal/meta/escape_test.go`）。⚠️ 修复前已落盘的污染数据不自动清洗，可用 `UPDATE t SET cfg = REPLACE(cfg, '\\"', '"')` 一类语句自行修复。
- **CPU 采样恒为 -1（存量）**：`readGlobalCPUPct` 用 `Fscanln(&line)` 读 `/proc/stat` 首行，按空白分词只读到 `cpu` 一个词，后续数值全解析为 0——CPU 指标自 v1.0.5 起从未产生过有效样本（`WHEN CPU<70` 因 -1<70 恒真而被掩盖）。改按 `strings.Fields` 解析全行；随 `SHOW QUERY VARS` 暴露并修复。
- **`WHEN` 作为 SUBMIT 首个选项解析失败（存量）**：`SUBMIT QUERY '…' WHEN CPU<70` 中 WHEN 紧跟 SQL 引号时，旧正则要求 WHEN 前必须有空白而失配，报 `unknown SUBMIT option near "WHEN"`（此前测试只覆盖过 WHEN 之前还有别的选项的写法）。现 WHEN / PARTITION / SCRIPT 允许出现在选项串开头；WHEN 与 PARTITION/SCRIPT 共存时 WHEN 表达式正确截断到 PARTITION/SCRIPT 之前。

### 新增
- **只读账号（`auth.readonly_user` / `auth.readonly_password`，默认 `glacier_ro`）**：与管理账号 `glacier` 完全同等的认证体系——mysql_native_password 登录、密码可配置、管理面热改（`SET PASSWORD FOR glacier_ro='新密码'` / `ALTER USER glacier_ro IDENTIFIED BY '...'`，与管理员密码同一 `<db_dir>/glacier.json` 持久化，`readonly_password` 字段向后兼容）。权限：查询、`SHOW`、管理面查看（SERVER CONFIG / CLIENTS / TRANSACTIONS / QUERY JOBS / QUERY VARS）放行；数据面写语句、`BEGIN` 事务、`SUBMIT/CANCEL/DROP QUERY`、管理面变更命令（SET PASSWORD / SET *_THREADS / KILL / VACUUM）一律 1290 拒绝。原 `readonly_users` 名单配置移除（单账号模型下实际不可用）。`readonly_password` 置空可禁用只读登录。
- **`SHOW QUERY VARS`**：查看 WHEN 监测变量当前值（Variable_name / Value / Level / Description 四列；值 <0 表示首采只建基线，返回 NULL）。
- **`DISK_MBPS` 服务器级磁盘写速率指标**：`/proc/diskstats` 物理盘（sd/vd/xvd/hd/nvme/mmcblk，分区不计）sectors written 增量折算 MB/s；容器内 `/proc/diskstats` 为宿主机视图，反映整机。WHEN 表达式与 `SHOW QUERY VARS` 均可用。

### 变更
- FEATURE.md：**多机服务冗余（原 §5）从路线移除**——坚持单机单写者与零中间件模型，不做多机部署支持；读扩展/写主备方案不再规划。
- 管理面 `SET PASSWORD` / `ALTER USER` 支持目标账号（`FOR <user>` / `ALTER USER <user>`），命中只读账号名改只读密码，缺省或管理员名改管理员密码；`SHOW SERVER CONFIG` 新增 `auth.readonly_user` 行；SQL 审计日志与 `SHOW CLIENTS` 的 `User` 列记录真实登录账号。
- `tests/test_v109.py` 新增 32 项；全套回归 ALL TESTS PASSED（14 个测试脚本）。

## [1.0.8.alpha] - 2026-10-20

### 修复
- **空字符串回读为 NULL**：go-mysql 结果集编码把 Go 空字符串 `""` 编成 MySQL 协议 NULL（0xfb）——存储层无损（`WHERE v=''` 命中、`LENGTH=0`、`IS NULL=0`），仅结果集编码失真，`ip=''` 列被客户端读成 None 直接打爆 pydantic。网关在编码前统一规范化（`''`/空 `[]byte` → 长度 0 字符串，真 NULL 不动），三个结果集出口全部修复。
- **hint 从未生效的隐藏 bug**：注释剥离（StripComments）会连带剥掉 `/*+ PARALLEL(n) */` 优化器 hint，v1.0.4 以来的并行查询实际一直静默串行回退（历史测试只验证结果正确性，串行也能过）。现保留 `/*+ */` 形式的 hint。
- `DEFAULT ('...')` 括号形式已知限制维持不变（SQLite 视为非常量），继续要求裸 `DEFAULT ''` 写法。

### 新增
- **MySQL 标量函数（Go 实现，注册进每个后端连接）**：`MD5` / `SHA1` / `SHA2(s,n)` / `SHA256`（小写 hex、NULL 传播，任意表达式位置可用，`UPDATE … SET col=MD5(…)` 直通）；`LEFT` / `RIGHT`（UTF-8 安全）；`SUBSTRING_INDEX`（正负计数）；`DATEDIFF` / `TIMESTAMPDIFF`（9 种单位，MONTH/QUARTER/YEAR 日历感知，单位关键字自动字符串化改写）；`DATE_FORMAT`（MySQL 格式符子集）。
- **多核并行查询自动触发**：无 hint 时单表可合并聚合语句按估算行数（rowid/主键跨度）≥ `parallel_query.scan_rows`（默认 100 万，0=关闭）自动并行，并行度 `parallel_query.workers`（0=CPU 核数）。
- **`EXPLAIN PARALLEL <query>`**：查看拆分计划（表 / 分片键 / 估算行数 / 并行度 / 不满足原因）。
- **异步 PARTITION 分片回调实装**：`PARTITION BY ROWS n | BY BYTES n`——任务级元信息 POST（含 parts）后逐片独立 POST（片号/列头/数据内联，超 `callback_max_inline_kb` 改发拉取凭证），每片独立 HMAC 签名与重试；`query_jobs` 新增 `partition_rows` / `partition_bytes` 列。
- **WINDOW 周几限定**：`WINDOW '周五 17:00-20:00'` / `'FRI …'`（周一..周日 / MON..SUN），非指定日等待、TIMEOUT 兜底。
- `INSERT IGNORE INTO` → `INSERT OR IGNORE INTO` 改写（M2 批）。
- 新增第 5 册文档 **docs/异步任务.md**（任务类型 / SUBMIT 全量语法与参数 / 生命周期 / 回调协议 / 分片协议 / 配置与示例）；`tests/test_v108.py` 37 项；全套回归 ALL TESTS PASSED（13 个测试脚本）。

### 变更
- SQLite 物理连接改经 `driver.Open(dsn, hook)` 建立（连接初始化钩子注册兼容函数），行为不变；
- `parallel_query` 配置新增 `workers` / `scan_rows`（原 `disable` 兼容保留）。

## [1.0.7.alpha] - 2026-10-15

### 新增
- **EVERY 周期定时任务**（无 CALLBACK 即定时任务）：`EVERY DAY [AT '17:00']` / `EVERY HOUR` / `EVERY n SECONDS|MINUTES|HOURS`；执行完自动重排；`recur_group` 链标记，`CANCEL` 取消整链（DONE 链头亦可停链）。
- **VACUUM 释放空间**：管理面 `VACUUM DATABASE <db>`——⚠️ 执行期间该库**全部写入阻塞**（结果集含警示列）；`maintenance.no_hot_vacuum: true` 禁止热刷新改用 CLI（`glaciertdb -vacuum -db-dir <dir>`，刷新时服务必须停止）。
- **db 目录唯一锁**：服务与 CLI 互斥、同目录单实例（flock，进程退出自动释放）；`--force-unlock`（拒绝抢活进程的锁）。
- **SQLite 静态加密**：`security.encrypt/passphrase`（ncruces adiantum VFS，4KiB 块 + Argon2id）；首次启用要求空目录/已加密库，存量明文库启动报错列出；实测密文文件头、内容不可 grep、错口令 `file is not a database`。
- 新增 `test_v107.py` 12 项；全套回归 ALL TESTS PASSED（12 大类）。

## [1.0.6.alpha] - 2026-10-15

### 新增
- **回调通道扩展**：HTTP（原有）/ **TCP**（客户端开端口，服务端连接推送元信息+列头+数据流）/ **RabbitMQ**（`amqp://… QUEUE q`）/ **Kafka**（`kafka://broker TOPIC t`）；四态（DONE/ERROR/EXPIRED/CANCELLED）一律推送。
- **结果保存时间**：`async_query.result_retain_sec` 默认 **60 秒**到期自动删除；`RETAIN n|n s|n m|n h` 按任务覆盖；`DROP QUERY RESULT` 主动清除。

## [1.0.5.alpha] - 2026-10-15

### 新增
- **异步查询引擎**：`SUBMIT QUERY` 秒回 job_id；AT 指定时刻 / WINDOW 每日双边窗口（EXPIRED 不跨天）/ WHEN 资源门槛（CPU/MEM/APP_CPU/APP_MEM/IO_MBPS，30s 采样最低 10s）；DONE/ERROR/EXPIRED 回调（可选 HMAC）；结果 JSONL 落盘分页拉取（query_jobs/query_result/FETCH/CANCEL）。

## [1.0.4.alpha] - 2026-10-15

### 新增
- **多核并行查询**：`/*+ PARALLEL(n) */` hint；单表顶层聚合分片-并行-合并（rowid/单列主键分片，AVG 拆 sum+count）；自动回退串行。

## [1.0.3.alpha] - 2026-10-15

### 新增
- **MySQL 兼容增强 M1（高频批）**：`LIMIT offset,count` 双参写法、`CONCAT()`（含嵌套与 NULL 传播）、`IF(c,a,b)`、`RAND()`、`UUID()`（v4 样式）；**DDL 类型映射**——MySQL 建表语句直接粘贴可用（INT 族→INTEGER、VARCHAR 族→TEXT、DATETIME/TIMESTAMP→TEXT、DOUBLE/FLOAT→REAL、DECIMAL→TEXT、BINARY 族→BLOB，UNSIGNED/ZEROFILL/AUTO_INCREMENT/内联 KEY 段/ENGINE 表尾/COMMENT 自动清理）；`DEFAULT CURRENT_TIMESTAMP` → `datetime('now')`。
- 新增第 9 大类黑盒测试 `test_mysql_compat.py`（14 项），全套回归 ALL TESTS PASSED。
- FEATURE.md：异步查询时间策略修正为**每日窗口区间**（双边 `WINDOW '17:00-20:00'`，支持跨午夜；窗外未达标当日 EXPIRED 不跨天滚动）。

## [1.0.2.alpha] - 2026-10-15

首个完整功能版。

### 新增
- **多库模型**：一个库 = `db_dir` 下一个 `.db` 文件；`SHOW DATABASES` / `USE` 切库 / `CREATE` / `DROP DATABASE`；`glacier` 为系统保留名。
- **并发模型**：读并发多线程（`global_threads` + `per_db_read_threads` + 按库覆盖，均可管理面热改）；写单库互斥锁，多库写入并行（实测跨库 0.00s / 同库排队 1.50s）。
- **管理面**：`USE GlacierTDB`——`SHOW SERVER CONFIG` / `server_config` / `clients` / `transactions` 表查询、`KILL` 踢客户端、`SET PASSWORD` 热改密并持久化。
- **系统库兼容层**：`information_schema` 21 张常用表、`mysql.user`、`performance_schema` / `sys` 由网关直接应答（支持投影与 WHERE 过滤）。
- **探活与兜底**：SHOW 家族全量覆盖 + 未覆盖变体一律空结果集；`DESC`/`DESCRIBE`；`库名.表` 前缀剥除；文本协议占位符（`select $$`）拒绝。
- **方言改写**：`NOW()/CURDATE()/CURTIME()/UNIX_TIMESTAMP()/FROM_UNIXTIME()/LAST_INSERT_ID()/TRUNCATE`；时序四件套 `NOW() ± INTERVAL`、日期字符串谓词整数化、`GROUP BY time(N)` 桶化。
- **autocommit 语义**：握手包 `SERVER_STATUS_AUTOCOMMIT` 补位；`SET AUTOCOMMIT=0` 惰性隐式事务。
- **SQL 审计日志**：`log.sql_log_file` JSON 行（语句/OK|ERROR/错误消息/耗时/行数/连接），100MB 轮转。
- **资源保护**：`memory_limit_mb`（GOMEMLIMIT 软上限）、`max_connections`、`sqlite.max_result_rows`。
- **版本体系**：`-v` / `--version`、`SELECT glacier_version`（自身版本，与协议版本 `@@version=8.99.999` 区分）。
- **测试**：8 大类黑盒（mysql CLI 32 / PyMySQL 31 / SQLAlchemy 14 / 并发 12 / 多库 7 / 时序 11 / 管理面 21 / Navicat 兼容 33），`tests/run_all.sh` 一键回归。
- **构建与交付**：Docker 镜像、linux amd64/arm64 与 windows amd64 静态二进制（含 Windows 盘符/中文/空格路径支持）。

### 已知限制
见 readme「已知限制」：不支持预处理语句 / TLS / 跨库 JOIN；系统库兼容为常用子集；单实例部署。
