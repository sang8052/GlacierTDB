#!/usr/bin/env bash
# v1.3.14 §5「性能损失与比较」——2H4G 资源受限对比测试编排。
#
# 公平起跑规则（需求 §5 定稿）：
#   - 全部容器化，统一 CPU 2 核 + 内存 4GB + IO 两档限速（SD 卡级 / HDD 级，blkio）
#   - 对照镜像固定 tag；同机同内核；每组 3 轮取中位数（bench_all 输出单行 JSON）
#   - GlacierTDB 以真实形态参加（五组件全开单容器）；对照组单一中间件独占同规格
#   - 加测组合部署对照组：五中间件容器共享 2H4G 总预算 vs GlacierTDB 单容器同预算
#
# 用法（构建机 root）：bash bench_2h4g.sh [SD|HDD|NONE]   # IO 档位，默认 SD
set -euo pipefail
IO_MODE="${1:-SD}"
NET="sqlproxy-net"
PY_IMG="python:3.12-slim"
BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"   # 绝对路径（docker -v 需要）
LOG_DIR=$(mktemp -d /tmp/gt-bench.XXXX)
CPUARGS="--cpus=2 --memory=4g"
case "$IO_MODE" in
  SD)  BLKIO="--device-read-bps /dev/sda:15MB --device-write-bps /dev/sda:8MB" ;;
  HDD) BLKIO="--device-read-bps /dev/sda:120MB --device-write-bps /dev/sda:80MB" ;;
  *)   BLKIO="" ;;
esac
echo "== 2H4G 对比测试（IO 档位: $IO_MODE；日志: $LOG_DIR）=="

# 设备名探测（云盘常为 vda/xvda/nvme0n1）
DEV=$(lsblk -ndo NAME,TYPE | awk '$2=="disk"{print "/dev/"$1; exit}')
[ -n "$DEV" ] && BLKIO=$(echo "$BLKIO" | sed "s|/dev/sda|$DEV|g")
echo "IO 限速设备: ${DEV:-none} $BLKIO"

run_py () {  # run_py <net> <script...>
  local net="$1"; shift
  docker run --rm --network "$net" -v "$BENCH_DIR":/bench:ro "$PY_IMG" bash -c "
    pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL redis pika >/dev/null 2>&1
    python /bench/bench_all.py $*" 2>>"$LOG_DIR/err.log"
}

median_of () {  # 3 行 JSON → 中位数 value
  python3 - "$1" <<'EOF'
import json, sys
vals = [json.loads(l)["value"] for l in sys.stdin if l.strip()]
vals.sort()
print(round(vals[len(vals)//2], 2) if vals else 0)
EOF
}

rounds=3
bench_target () {  # bench_target <group> <label> <extra-args...>
  local group="$1" label="$2"; shift 2
  for r in $(seq $rounds); do
    run_py "$NET" "$group" --label "$label" "$@" || true
  done
}

# ---------------------------------------------------------------------------
# ① GlacierTDB（真实形态五组件全开）
# ---------------------------------------------------------------------------
docker rm -f gt-bench >/dev/null 2>&1 || true
docker run -d --name gt-bench $CPUARGS $BLKIO --network "$NET" -p 33309:3309 -p 39000:9000 \
  -v /tmp/gt-bench-data:/data -v /tmp/glacier-async.yaml:/app/config.yaml glaciertdb:latest >/dev/null
sleep 3
GT_PW="iloveyou~"
docker exec gt-bench sh -c 'true' 2>/dev/null || true
AKSK=$(docker run --rm --network "$NET" "$PY_IMG" bash -c "
pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1
python - <<'PYEOF'
import pymysql
c = pymysql.connect(host='gt-bench', port=3309, user='glacier', password='iloveyou~', database='backup')
cur = c.cursor(); cur.execute('USE GlacierTDB'); cur.execute('CREATE S3 KEY FOR glacier')
r = cur.fetchone(); print(r[0], r[1])
PYEOF")
AK=$(echo $AKSK | cut -d' ' -f1); SK=$(echo $AKSK | cut -d' ' -f2)
echo "-- GlacierTDB --"
bench_target mysql glaciertdb --host gt-bench --port 3309 --passwd "$GT_PW" | tee -a "$LOG_DIR/gt.jsonl"
bench_target redis glaciertdb --host gt-bench --port 3309 --auth "$GT_PW" | tee -a "$LOG_DIR/gt.jsonl"
bench_target s3 glaciertdb --host gt-bench --port 9000 --ak "$AK" --sk "$SK" | tee -a "$LOG_DIR/gt.jsonl"
bench_target mq glaciertdb --host gt-bench --port 3309 --passwd "$GT_PW" | tee -a "$LOG_DIR/gt.jsonl"

# ---------------------------------------------------------------------------
# ② 对照组：单一中间件独占同规格
# ---------------------------------------------------------------------------
echo "-- mysql:8 --"
docker rm -f bench-mysql >/dev/null 2>&1 || true
docker run -d --name bench-mysql $CPUARGS $BLKIO --network "$NET" \
  -e MYSQL_ROOT_PASSWORD=rootpw -e MYSQL_DATABASE=bench -p 33306:3306 mysql:8 >/dev/null
until docker run --rm --network "$NET" "$PY_IMG" bash -c "
  pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1
  python -c \"import pymysql; pymysql.connect(host='bench-mysql', port=3306, user='root', password='rootpw', database='bench')\"" 2>/dev/null; do sleep 3; done
bench_target mysql mysql8 --host bench-mysql --port 3306 --user root --passwd rootpw --db bench | tee -a "$LOG_DIR/cmp.jsonl"
docker rm -f bench-mysql >/dev/null

echo "-- redis:7 --"
docker rm -f bench-redis >/dev/null 2>&1 || true
docker run -d --name bench-redis $CPUARGS $BLKIO --network "$NET" -p 36379:6379 redis:7 >/dev/null
sleep 2
bench_target redis redis7 --host bench-redis --port 6379 | tee -a "$LOG_DIR/cmp.jsonl"
docker rm -f bench-redis >/dev/null

echo "-- rabbitmq:3.13 --"
docker rm -f bench-rabbit >/dev/null 2>&1 || true
docker run -d --name bench-rabbit $CPUARGS $BLKIO --network "$NET" \
  -e RABBITMQ_DEFAULT_USER=guest -e RABBITMQ_DEFAULT_PASS=guest -p 35672:5672 rabbitmq:3.13 >/dev/null
until docker run --rm --network "$NET" "$PY_IMG" bash -c "
  pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple pika >/dev/null 2>&1
  python -c \"import pika; pika.BlockingConnection(pika.ConnectionParameters(host='bench-rabbit', port=5672, credentials=pika.PlainCredentials('guest','guest')))\"" 2>/dev/null; do sleep 4; done
bench_target mq rabbitmq313 --host bench-rabbit --port 5672 --user guest --passwd guest | tee -a "$LOG_DIR/cmp.jsonl"
docker rm -f bench-rabbit >/dev/null

echo "-- minio --"
docker rm -f bench-minio >/dev/null 2>&1 || true
docker run -d --name bench-minio $CPUARGS $BLKIO --network "$NET" -p 39001:9000 \
  -e MINIO_ROOT_USER=benchak -e MINIO_ROOT_PASSWORD=benchsk123 minio/minio:RELEASE.2025-04-22T22-12-26Z server /data >/dev/null
sleep 4
docker run --rm --network "$NET" "$PY_IMG" bash -c "
  pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1" || true
# MinIO 用 aws cli 侧建桶不可用——bench_all 的 S3 客户端直接 PUT 首次自动建桶不支持，
# 用 mc 一次性初始化：
docker run --rm --network "$NET" --entrypoint /bin/sh minio/mc <<'EOF' 2>>"$LOG_DIR/err.log" || true
until curl -sf http://bench-minio:9000/minio/health/ready >/dev/null; do sleep 2; done
mc alias set b http://bench-minio:9000 benchak benchsk123
mc mb b/bench-s3 2>/dev/null || true
EOF
bench_target s3 minio --host bench-minio --port 9000 --ak benchak --sk benchsk123 --label minio | tee -a "$LOG_DIR/cmp.jsonl"
docker rm -f bench-minio >/dev/null

docker rm -f gt-bench >/dev/null

echo
echo "== 汇总（3 轮中位数；完整行见 $LOG_DIR）=="
python3 - "$LOG_DIR" <<'EOF'
import json, sys, glob, os
rows = []
for f in glob.glob(os.path.join(sys.argv[1], "*.jsonl")):
    for line in open(f, encoding="utf-8"):
        line = line.strip()
        if line.startswith("{"):
            rows.append(json.loads(line))
        elif line:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
from collections import defaultdict
med = defaultdict(list)
for r in rows:
    med[(r["capability"], r["target"], r["scenario"])].append(r["value"])
print("%-8s %-12s %-18s %12s" % ("cap", "target", "scenario", "median"))
for k in sorted(med):
    v = sorted(med[k])
    print("%-8s %-12s %-18s %12.2f %s" % (k[0], k[1], k[2], v[len(v)//2], rows[0]["unit"] if rows else ""))
EOF
