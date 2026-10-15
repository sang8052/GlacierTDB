# GlacierTDB MySQL 兼容增强路线（后续规划）

> 状态：**设计稿，未实现**（当前已有的兼容能力见 [系统SQL.md](系统SQL.md) 第 3 节）。
> 目标：在不引入解析器重写的前提下，按**词法级改写 + DDL 类型映射**补齐高频 MySQL 写法，
> 让存量 SQL 与 ORM 生成语句零改造运行。SQLite 原生已支持的（CTE / 窗口函数 / IFNULL /
> GROUP_CONCAT / REPLACE INTO）不在本表内。

## M1 高频批（优先）

| MySQL 写法 | 改写/映射 | 备注 |
| --- | --- | --- |
| `LIMIT offset, count` | `LIMIT count OFFSET offset` | MySQL 专有双参写法，ORM 高频 |
| `CONCAT(a, b, …)` | `(a ‖ b ‖ …)` | NULL 传播语义两侧一致 |
| `IF(c, a, b)` | `CASE WHEN c THEN a ELSE b END` | |
| `IFNULL` / `COALESCE` | 原生 | 已可用，仅登记 |
| `RAND()` | `(abs(random())/9223372036854775808.0)` | |
| `UUID()` | 格式化 `hex(randomblob(16))` | v4 样式 |
| **DDL 类型映射**（建表透传前） | `TINYINT/SMALLINT/INT/BIGINT→INTEGER`、`VARCHAR(n)/CHAR/TEXT 族→TEXT`、`DATETIME/TIMESTAMP/DATE→TEXT`、`DOUBLE/FLOAT→REAL`、`DECIMAL→TEXT`（提示精度限制） | 使 `CREATE TABLE` 直接粘 MySQL DDL |
| `DEFAULT CURRENT_TIMESTAMP` | `DEFAULT (datetime('now'))` | 列默认值常见 |

## M2 语句族

| MySQL 写法 | 方案 | 备注 |
| --- | --- | --- |
| `INSERT IGNORE` | `INSERT OR IGNORE` | 词法替换 |
| `INSERT … ON DUPLICATE KEY UPDATE` | `INSERT … ON CONFLICT DO UPDATE`（依据表唯一约束改写冲突目标） | 需查元数据，复杂度中 |
| `SUBSTRING_INDEX(s, d, n)` | 表达式改写（instr/substr 组合） | |
| `DATEDIFF(a,b)` / `TIMESTAMPDIFF(u,a,b)` | julianday 差 / 系数换算 | |
| `DATE_FORMAT(d, fmt)` | `strftime`（格式符映射表 %Y/%m/%d/%H/%i/%s…） | %i→%M 分钟 |
| `LEFT/RIGHT(s,n)` | `substr` 组合 | |
| `FLOOR/CEIL/ROUND/ABS/MOD` | 原生 | 登记 |

## M3 按需评估

- `ON DUPLICATE KEY` 的 `VALUES(col)` 函数引用；
- `LOCK IN SHARE MODE / FOR UPDATE`（单库写锁模型下多为可忽略后缀——静默剥离候选）；
- `FORCE INDEX / USE INDEX` 提示（剥离即可）；
- `SHOW STATUS LIKE` 增强、`SET FOREIGN_KEY_CHECKS=0` 会话开关实装。

## 原则

- 每条改写进 `tests/` 黑盒断言（沿用 mysql:8 CLI + pymysql 双路验证）；
- 改写保守：匹配不上原样透传，宁可报 SQLite 错也不改错语义；
- 类型映射仅作用于 DDL 词法层，不建隐藏 schema。
