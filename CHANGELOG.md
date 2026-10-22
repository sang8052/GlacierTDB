# 更新日志

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号语义化。

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
