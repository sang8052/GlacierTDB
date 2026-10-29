#!/usr/bin/env bash
# 补测 GlacierTDB S3 组（桶先建好）
set -euo pipefail
B=/root/GlacierTDB/tests/bench
NET=sqlproxy-net
PY=python:3.12-slim
docker rm -f gt-bench >/dev/null 2>&1 || true
docker run -d --name gt-bench --cpus=2 --memory=4g --network $NET \
  -v /tmp/gt-bench-data:/data -v /tmp/glacier-async.yaml:/app/config.yaml glaciertdb:latest >/dev/null
sleep 3
cat > /tmp/gt_key.py <<'PYEOF'
import pymysql
c = pymysql.connect(host="gt-bench", port=3309, user="glacier", password="iloveyou~", database="backup")
cur = c.cursor(); cur.execute("USE GlacierTDB")
try:
    cur.execute("DROP BUCKET bench-s3 FORCE")
except Exception:
    pass
cur.execute("CREATE BUCKET bench-s3")
cur.execute("CREATE S3 KEY FOR glacier")
r = cur.fetchone()
print("%s %s" % (r[0], r[1]))
PYEOF
KEYS=$(docker run --rm --network $NET -v $B:/bench:ro -v /tmp/gt_key.py:/k.py:ro $PY bash -c "
  pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1 && python /k.py")
AK=$(echo $KEYS | cut -d' ' -f1)
SK=$(echo $KEYS | cut -d' ' -f2)
echo "AK=$AK" >&2
for r in 1 2 3; do
  docker run --rm --network $NET -v $B:/bench:ro -e AK="$AK" -e SK="$SK" $PY bash -c "
    pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1
    python /bench/bench_all.py s3 --host gt-bench --port 9000 --ak \"\$AK\" --sk \"\$SK\" --duration 8"
done
docker rm -f gt-bench >/dev/null
