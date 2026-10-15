-- ============================================================================
-- GlacierTDB 时序冷库标准 schema（阶段 1 · 时序 schema 约定）
--
-- 约定：
--   * 事实表 points 的主键 (series_id, ts) 为聚簇索引（WITHOUT ROWID），
--     同一序列物理相邻、时间递增 —— 范围扫描即顺序 IO；
--   * ts 为 epoch 毫秒（INTEGER），网关改写层的时间谓词整数化、
--     GROUP BY time(N) 桶化均以该约定为前提；
--   * tags 为 JSON 文本，常用 tag 建表达式索引（单 tag 的“倒排”）；
--   * 回填 ETL：按 series + ts 排序、大事务批量插入（如每 10 万点一个事务）。
--
-- 使用：mysql -h <host> -P 3309 -uglacier -p<password> <db> < schema/timeseries.sql
-- ============================================================================

-- series 维表 = InfluxDB 的 series key（measurement + tagset）
CREATE TABLE IF NOT EXISTS series (
  id   INTEGER PRIMARY KEY,
  meas TEXT NOT NULL,              -- measurement，如 cpu.load
  tags TEXT NOT NULL,              -- JSON: {"host":"a1","region":"bj"}
  UNIQUE (meas, tags)
);

-- 事实表：(series_id, ts) 聚簇主键，时间有序存储
CREATE TABLE IF NOT EXISTS points (
  series_id INTEGER NOT NULL,
  ts        INTEGER NOT NULL,      -- epoch ms
  val       REAL,
  PRIMARY KEY (series_id, ts)
) WITHOUT ROWID;

-- 纯时间范围跨序列查询建议建 ts 二级索引（聚簇主键最左列是 series_id，
-- 不建此索引时纯 ts 谓词会全表扫；按查询模式决定是否需要）
CREATE INDEX IF NOT EXISTS idx_points_ts ON points(ts);

-- 常用 tag 表达式索引（按实际查询模式增减；高基数 tag 注意索引体积）
CREATE INDEX IF NOT EXISTS idx_series_host   ON series(json_extract(tags, '$.host'));
CREATE INDEX IF NOT EXISTS idx_series_region ON series(json_extract(tags, '$.region'));

-- ============================================================================
-- 查询示例（网关已支持以下 MySQL 写法的自动改写）：
--
--   最近 1 小时：
--     SELECT avg(val) FROM points
--     WHERE series_id=1 AND ts >= NOW() - INTERVAL 1 HOUR;
--
--   日期字符串谓词（自动整数化为 epoch ms，UTC）：
--     SELECT count(*) FROM points WHERE ts >= '2026-10-01 00:00:00';
--
--   InfluxDB 风格桶化（每小时一桶）：
--     SELECT time(3600000), min(val), max(val), avg(val)
--     FROM points WHERE series_id=1 GROUP BY time(3600000);
--
--   秒级时间戳转可读时间：
--     SELECT FROM_UNIXTIME(ts/1000), val FROM points LIMIT 10;
-- ============================================================================
