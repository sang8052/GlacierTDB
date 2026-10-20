# FEATURE.md — 后续功能路线

> 已规划、尚未实现的功能路线统一在此；已实现能力见 [docs/](docs/) 各专项文档与 [CHANGELOG.md](CHANGELOG.md)。
>
> **v1.0.8 路线消化情况**：
> - ✅ §1 并行查询——hint 触发（v1.0.4）+ 自动触发 / `EXPLAIN PARALLEL` / `workers`·`scan_rows` 配置（v1.0.8）；GROUP BY 分片与明细导出并行（方案 B）仍为路线项；
> - ✅ §3 异步查询——策略引擎 / 回调四通道 / EVERY 周期（v1.0.5–1.0.7）+ PARTITION 分片回调 / 周几窗口（v1.0.8）；**JS 后处理管道（SCRIPT）仍为路线项**；
> - ✅ §4 MySQL 兼容——M1 高频批（v1.0.3）+ M2 语句族与函数批（v1.0.8，MD5/SHA 族 / LEFT / RIGHT / SUBSTRING_INDEX / DATEDIFF / TIMESTAMPDIFF / DATE_FORMAT / INSERT IGNORE）；M3 按需仍为路线项；
> - ⏳ §2 rollup 多粒度预聚合（P1）与 §5 多机服务冗余（P2）——未动工。
>
> 当前优先级：**P0 rollup 预聚合**（并行查询落地后的下一块长范围查询短板）→ P1 JS 后处理管道 / GROUP BY 并行分片 → P2 M3 兼容 / 多机冗余。

---

## 1. 并行查询剩余项（P1）

已落地（v1.0.4 + v1.0.8）：hint 与自动触发、可合并聚合分片合并、`EXPLAIN PARALLEL`。剩余：

- **GROUP BY 分片并行**：`GROUP BY series_id, time(N)` 按 series_id 取模分片、Go 侧按桶合并（首/末值需特殊处理）；
- **方案 B（明细导出并行）**：按 rowid 区间并行 scan，行包「并行读、顺序发送」（多 goroutine 写同一连接需按序）；
- 慢查询日志标注各分片耗时分布（现仅总耗时+并行度）。

## 2. rollup 多粒度预聚合（P0，时序）

- 回填 ETL 同一遍落 raw + m1 / h1 / d1 多粒度汇总表——**导入时物化，无后台任务**；
- 网关按请求的时间范围与桶宽，自动路由到**能满足精度的最粗汇总表**："全年按天"扫的是 d1 表几千行，不触达原始点——这正是 InfluxDB 长范围聚合快的本质；
- 与已落地的并行查询叠加：d1 表 + 8 核并行 ≈ 数量级提升；
- 冷库查询模式可预测，Top-N 高频报表可在导入时直接物化成结果表；
- 慢查询日志标注是否命中 rollup 路由，便于回归观察。

> 当前版本：时间谓词整数化 + `time(N)` 桶化已实现（见 [docs/时序数据库.md](docs/时序数据库.md) 第 2 节）；本节为路线项。

## 3. 异步查询剩余项：JS 后处理管道（P1）

已落地（v1.0.5–1.0.8）：SUBMIT / AT / WINDOW（每日+周几）/ WHEN / EVERY / 四通道回调 / PARTITION 分片回调 / 结果拉取。剩余（§3.4 原案）：

- `SCRIPT '/srv/jobs/report.js' [OUTPUT xls|csv]`：查询结果以 JSONL 流入 **nodejs 子进程 stdin**，脚本产物写 stdout，落 `jobs/<id>.<output>`，回调/拉取直接下载成品报表；
- 执行模型：CPU/时长限制（超时 kill）、只给只读数据目录（不注入网关权限）；运行环境缺失或脚本非零退出 → job 转 ERROR 并回因；
- 配置 `async_query.nodejs_path`（空=自动探测）；`SHOW QUERY JOBS` 展示管道阶段（QUERY → SCRIPT → CALLBACK）；
- 语法位已预留（当前提交接受 SCRIPT/OUTPUT 参数但不执行管道）。

## 4. MySQL 兼容 M3（P2，按需）

> M1（v1.0.3）/ M2（v1.0.8）已落地；SQLite 原生已支持的不列（CTE / 窗口函数 / IFNULL / GROUP_CONCAT / REPLACE INTO）。

| 批次 | 内容 |
| --- | --- |
| M3 按需 | `VALUES(col)` 引用；`INSERT … ON DUPLICATE KEY UPDATE` → `ON CONFLICT DO UPDATE`（依表唯一约束改写）；`FOR UPDATE / LOCK IN SHARE MODE`、索引提示（FORCE/USE INDEX）静默剥离；`SET FOREIGN_KEY_CHECKS` 会话开关；`CONCAT_WS` / `LPAD` / `RPAD`；`DATE_FORMAT` 剩余格式符（%U %u %V %v 周编号族） |

原则：改写保守——匹配不上原样透传，宁可报 SQLite 错不改错语义；每条进 tests/ 黑盒。

## 5. 多机服务冗余（P2）

> 服务级冗余（进程/主机故障可用性），非数据多副本。

- **读扩展**（衔接现有固化路线）：库固化（`journal_mode=DELETE; VACUUM`）后 N 个只读实例挂同一份文件——复制文件 + 起容器，无复制协议；
- **写主备**：单写者红线内做主备——备机实时镜像 `db_dir`（文件级增量同步）+ 心跳；主机失联秒级提升备机（冷库场景允许秒级窗口，不做防脑裂强一致——诚实声明）；
- 前置：虚拟 IP 或 L4 代理做入口切换；
- ✗ 不做：外部协调服务（etcd/consul）、多主写、纠删码——违背零中间件与单写者模型。
