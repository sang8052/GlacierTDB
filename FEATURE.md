# FEATURE.md — 后续功能路线

> 已规划、尚未实现的功能路线统一在此；已实现能力见 [docs/](docs/) 各专项文档。
> 优先级：**P0 并行查询**（当前最大短板）→ P1 rollup / 异步查询 → P2 MySQL 兼容 / 多机服务冗余。

---

## 1. 多核并行查询（P0，重点）

**问题**：SQLite 单条查询只能用单核（见时序文档「与 InfluxDB 差异」的 ⚠️ 项）——
长范围聚合、百万行扫描在多核机器上浪费 (N-1)/N 算力。目标：**长范围聚合提升接近核数倍**。

### 方案：网关层分片并行（不改 SQLite）

```
SELECT series_id, count(*), avg(val) FROM points
WHERE ts >= A AND ts < B GROUP BY series_id, time(3600000)
        │  改写层识别：聚合查询 + 大范围（行数估算超阈值）
        ▼  拆成 N 个互不重叠子查询（按 series_id 取模 / rowid 区间）
   ┌────────┬────────┬────────┬────────┐
   │ 子查询1 │ 子查询2 │ 子查询3 │ 子查询4 │   N 个只读会话并行执行
   └────┬───┴────┬───┴────┬───┴────┬───┘   （受 global_threads 闸门）
        ▼        ▼        ▼        ▼
        └──────► Go 侧部分聚合合并 ◄──────┘
     （count/sum/min/max/avg 可合并；first/last/GROUP_CONCAT 不拆）
```

- **可合并聚合**：count / sum / min / max / avg（avg=sum+count 合并）；
- **不拆分**：first/last、无聚合的明细导出（改为方案 B）、带副作用的语句；
- **方案 B（明细导出）**：按 rowid 区间并行 scan，行包并行发送（多 goroutine 写同一连接需按序——改为并行读、顺序发送）；
- 触发条件：`EXPLAIN` 行数估算 > `parallel_scan_rows`（默认 100 万）且聚合可合并；
- 透出：`EXPLAIN PARALLEL` 查看拆分计划；慢查询日志标注并行度与各分片耗时；
- 配置：

```yaml
parallel_query:
  enable: true
  workers: 0            # 0 = CPU 核数
  scan_rows: 1000000    # 触发阈值
```

## 2. rollup 多粒度预聚合（P1，时序）

- 回填 ETL 同一遍落 raw + m1 / h1 / d1 多粒度汇总表——**导入时物化，无后台任务**；
- 网关按请求的时间范围与桶宽，自动路由到**能满足精度的最粗汇总表**："全年按天"扫的是 d1 表几千行，不触达原始点——这正是 InfluxDB 长范围聚合快的本质；
- 与并行查询（第 1 节）叠加：d1 表 + 8 核并行 ≈ 数量级提升；
- 冷库查询模式可预测，Top-N 高频报表可在导入时直接物化成结果表；
- 慢查询日志标注是否命中 rollup 路由，便于回归观察。

> 当前版本：时间谓词整数化 + `time(N)` 桶化已实现（见 [docs/时序数据库.md](docs/时序数据库.md) 第 2 节）；本节为路线项。

## 3. 异步查询（P1，超大数据 + 策略调度 + JS 管道）

提交秒回 job_id；执行可由**时间策略 / 性能策略**门控；结果支持 **HTTP POST 回调（含分片）** 或 **SQL 拉取**；可选 **JS 后处理**直出报表。

### 3.1 提交语法（全量）

```sql
SUBMIT QUERY 'SELECT …'
  [CALLBACK 'http://ops/hook' [SIGN '<secret>']]     -- POST 回调；SIGN 开启 HMAC 签名（可选）
  [TIMEOUT 3600]
  [START AFTER '17:00']                               -- 时间策略：每天 17:00 后执行（也支持 '周X HH:MM'）
  [WHEN CPU < 70 AND DISK_IO < 50 AND MEM < 80]       -- 性能策略门槛（多条件 AND）
  [PARTITION BY ROWS 50000 | BYTES 8388608]           -- 分片回调：每片一次 POST
  [SCRIPT '/srv/jobs/report.js' [OUTPUT xls|csv]];    -- JS 后处理管道，直出表格
```

### 3.2 策略引擎

- **时间策略**：`START AFTER` 定义执行窗口（如"晚上 5 点之后"避峰）；到窗 + 性能门槛满足才触发；
- **性能策略**：CPU 占用 / 磁盘 IO 写入 / 内存使用率三类指标，每条支持**两个层级**：
  - `全局（服务器级）`：整机指标（Linux 读 `/proc` + gopsutil，纯 Go）；
  - `应用级（当前程序）`：本进程 CPU、RSS、累计写盘字节（runtime + /proc/self）；
- **采集器**：独立 goroutine 异步采样，**默认 30s 一次，可调，最低 10s**（配置 `metrics_interval_sec`，低于 10 拒绝启动）；策略判断读最近样本，不在提交路径上产生开销；
- 门槛条件支持 `AND` 组合；持续不满足到 `TIMEOUT` 则 EXPIRED。

### 3.3 HTTP 回调（POST，可选签名，支持分片）

- 一律 **POST** JSON；签名可选：`SIGN` 指定 secret 时携带 `X-Glacier-Signature: sha256=HMAC(secret, body)`，未指定则不带；
- **分片回调**（海量数据）：`PARTITION BY ROWS/BYTES` 将结果切成多片，每片独立 POST：

```json
{ "job_id": "j_x", "part": 3, "parts": 12, "rows": 50000,
  "data_encoding": "base64|jsonl", "signature": "…（可选）" }
```

  单片可内联数据（≤ `callback_max_inline_kb`），超限回调给拉取凭证；分片间重试独立（单片失败退避 3 次不阻塞后续片，末尾补发失败清单）；
- 语义：`2xx` 成功；小结果不设 PARTITION 时单次 POST 元信息 + 数据。

### 3.4 JS 后处理管道（需 nodejs 环境）

- `SCRIPT` 指定 JS 文件：查询结果以 JSONL 流入子进程 **stdin**，脚本处理结果写 **stdout**，产物落 `jobs/<id>.<output>`；
- `OUTPUT xls | csv`：脚本内用 exceljs / 原生拼接直接产出表格文件，回调/拉取直接下载成品报表；
- 执行模型：`nodejs` 子进程，CPU/时长限制（超时 kill）、只给只读数据目录（不注入网关权限）；
- 运行环境缺失（无 nodejs）或脚本非零退出：job 转 ERROR 并回因；`SHOW QUERY JOBS` 可见管道阶段（QUERY → SCRIPT → CALLBACK）。

### 3.5 生命周期（不变部分）

- 状态机：`PENDING(WAIT_TIME/WAIT_RESOURCE) → RUNNING → SCRIPT → CALLBACK → DONE | ERROR | CANCELLED | EXPIRED`；
- 结果 JSONL 落盘流式分页；`async_max_result_rows` 放宽（默认 1 亿，磁盘为界）；TTL / 磁盘水位 / `CANCEL QUERY`（触发 SQLite interrupt）；
- 异步 worker 池与在线查询共享 `global_threads` 闸门。

### 3.6 新增配置（规划）

```yaml
async_query:
  enable: true
  workers: 2
  metrics_interval_sec: 30      # 性能采样间隔，最低 10
  default_timeout_sec: 1800
  result_ttl_hours: 24
  jobs_dir_max_gb: 0
  callback_max_inline_kb: 64
  nodejs_path: ""               # 空=自动探测；启用 JS 管道必须有
```

## 4. MySQL 兼容增强（P2）

> SQLite 原生已支持的不列（CTE / 窗口函数 / IFNULL / GROUP_CONCAT / REPLACE INTO）。

| 批次 | 内容 |
| --- | --- |
| M1 高频 | `LIMIT offset,count`→`LIMIT n OFFSET o`；`CONCAT(a,b,…)`→`(a‖b‖…)`；`IF(c,a,b)`→`CASE WHEN`；`RAND()`/`UUID()`；**DDL 类型映射**（TINYINT/INT 族→INTEGER、VARCHAR 族→TEXT、DATETIME/TIMESTAMP→TEXT、DOUBLE/FLOAT→REAL、DECIMAL→TEXT）；`DEFAULT CURRENT_TIMESTAMP`→`DEFAULT (datetime('now'))` |
| M2 语句族 | `INSERT IGNORE`→`INSERT OR IGNORE`；`INSERT … ON DUPLICATE KEY UPDATE`→`ON CONFLICT DO UPDATE`（依表唯一约束改写）；`SUBSTRING_INDEX` / `DATEDIFF` / `TIMESTAMPDIFF` / `DATE_FORMAT`（格式符映射）/ `LEFT/RIGHT` |
| M3 按需 | `VALUES(col)` 引用；`FOR UPDATE / LOCK IN SHARE MODE`、索引提示（FORCE/USE INDEX）静默剥离；`SET FOREIGN_KEY_CHECKS` 会话开关 |

原则：改写保守——匹配不上原样透传，宁可报 SQLite 错不改错语义；每条进 tests/ 黑盒。

## 5. 多机服务冗余（P2）

> 服务级冗余（进程/主机故障可用性），非数据多副本。

- **读扩展**（衔接现有固化路线）：库固化（`journal_mode=DELETE; VACUUM`）后 N 个只读实例挂同一份文件——复制文件 + 起容器，无复制协议；
- **写主备**：单写者红线内做主备——备机实时镜像 `db_dir`（文件级增量同步）+ 心跳；主机失联秒级提升备机（冷库场景允许秒级窗口，不做防脑裂强一致——诚实声明）；
- 前置：虚拟 IP 或 L4 代理做入口切换；
- ✗ 不做：外部协调服务（etcd/consul）、多主写、纠删码——违背零中间件与单写者模型。
