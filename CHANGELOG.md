# 更新日志

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号语义化。

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
