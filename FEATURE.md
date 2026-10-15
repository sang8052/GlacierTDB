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

## 3. 异步查询（P1，超大数据）

提交秒回 job_id，**HTTP 回调** + **SQL 拉取**双通道；结果 JSONL 落盘流式分页，不占内存。

```sql
SUBMIT QUERY 'SELECT …' [CALLBACK 'http://ops/hook'] [TIMEOUT 3600];   -- 秒回 job_id
SHOW QUERY JOBS;                        -- state/rows/bytes/elapsed/error
FETCH QUERY RESULT 'j_xxx' LIMIT 1000 OFFSET 5000;                      -- 分页拉取
CANCEL QUERY 'j_xxx';
```

- 回调：元信息 + 结果 HMAC 签名（数据本体按凭证拉取；≤64KB 可内联 base64）；失败退避重试 3 次；
- 生命周期：`async_max_result_rows` 放宽（默认 1 亿，磁盘为界）、结果 TTL、jobs 目录磁盘水位、`CANCEL` 触发 SQLite interrupt；
- 并发：异步 worker 池，与在线查询共享 `global_threads` 闸门，防后台大查询饿死在线业务；
- 场景：百万行导出、夜间时序回算、慢查询不占连接。

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
