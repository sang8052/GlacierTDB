# FEATURE.md — 版本路线规划

> **愿景：多中间件一篮子混合部署**——以 SQLite 为唯一存储内核，把 MySQL 协议网关、时序冷库、
> S3 对象存储、Redis 键值队列、RabbitMQ 消息队列装进**同一个静态二进制、同一个数据目录**。
> 面向工控机 / 物联网边缘环境：一台设备上替代 MySQL + Redis + RabbitMQ + MinIO 的组合，
> 免去繁杂依赖安装、多进程守护与分布式备份——备份 = 拷贝目录，迁移 = 拷贝文件。
>
> **本文件只放规划**；已实现能力见 [README.md](README.md)、[docs/](docs/) 各册与
> [CHANGELOG.md](CHANGELOG.md)。版本号发布沿用 `.alpha` 后缀惯例（如 v1.2.0.alpha）。

---

## 现状

愿景内五件套（MySQL 网关 / 时序冷库 / S3 / Redis / RabbitMQ）与端口复用协议已于
**v1.0.x – v1.3.0 全部交付**（含 v1.0.10 运维安全补强、v1.1.1 存量余项全清）。
历史内容不再赘述——版本明细见 [CHANGELOG.md](CHANGELOG.md)，能力与用法见 [README.md](README.md) 与 docs/ 各册。

**当前无在途规划项**。未排期候选（来自各册"路线/边界"注记，随时可能调整或放弃）：

| 候选 | 出处 | 说明 |
| --- | --- | --- |
| 时序冷压缩与固化 | docs/时序数据库.md | 明细保留期外分区 Gorilla chunk 压缩（5–10 倍）、`journal_mode=DELETE; VACUUM` 固化只读多实例 |
| Redis 阻塞弹出 | v1.2.0 边界 | `BLPOP` / `BRPOP`（可选项） |
| AMQP 回退与确认 | docs/消息队列.md | `basic.return`（mandatory 回退）、confirm ack 帧 |

---

## 设计红线（始终不做）

- **单写者模型**：不做多主写、不做多机部署 / 复制协议（多机路线已从规划移除）；
- **零外部中间件依赖**：所有能力内化进单一静态二进制，不引入 etcd / consul / 外部队列等；
- **保守改写**：SQL 兼容匹配不上一律原样透传，宁可报后端错误不改错语义；每条进 tests/ 黑盒；
- **不承诺**：空间索引（geo 用包围盒粗筛）、事务性消息队列、Redis 集群协议、S3 分片上传高级语义
  （UploadPartCopy / 未完成上传自动清理；基础 multipart 已于 v1.3.1 交付）、纠删码 / 跨机副本——
  工控机单机场景不为用不到的分布式能力付复杂度。
