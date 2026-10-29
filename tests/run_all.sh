#!/usr/bin/env bash
# ============================================================================
# run_all.sh — mysql-tcp-proxy-sqlite 测试总入口
#
# 职责（只做编排与检查，不做 build、不启动被测容器、不装本机软件）:
#   0. 确保 docker network sqlproxy-net 存在（不存在则创建）
#   1. 检查被测容器 proxy 已在 sqlproxy-net 上运行（没运行就提示并退出；
#      镜像 glaciertdb:latest 与容器的构建/启动由使用方负责）
#   2. 用 mysql:8 容器执行 tests/init_schema.sql 建表
#   3. 运行 tests/test_mysql_cli.sh（mysql CLI 黑盒测试）
#   4. 在 python:3.12-slim 容器内安装 PyMySQL/SQLAlchemy 后依次运行
#      test_pymysql.py / test_sqlalchemy.py / test_concurrent.py
#      （pip 优先清华源直连；失败则改走代理 <PROXY_URL>
#        用 pypi 官方源重试）
#   5. 汇总结果：全过 exit 0，否则 exit 1
#
# 用法（任意目录均可，路径以本脚本所在 tests/ 目录为准）:
#   bash tests/run_all.sh
# ============================================================================
set -uo pipefail

TESTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NET="sqlproxy-net"
PROXY="proxy"
PORT="3309"
MYSQL_IMG="mysql:8"
PY_IMG="python:3.12-slim"
PIP_PROXY=""

MYSQL_AUTH_ARGS=(
    -h "$PROXY" -P "$PORT" -uglacier -piloveyou~
    --default-auth=mysql_native_password
    --default-character-set=utf8mb4
)

LOG_DIR="$(mktemp -d /tmp/sqlproxy-test.XXXXXX)"
RESULTS=()

step() {
    echo
    echo "=============================================================="
    echo "== $*"
    echo "=============================================================="
}

record() { RESULTS+=("$1"); }

# ----------------------------------------------------------------------------
# 0. docker 环境与网络
# ----------------------------------------------------------------------------
step "步骤 0/4: 环境检查（docker / network / proxy 容器）"

if ! command -v docker >/dev/null 2>&1; then
    echo "[FATAL] 本机没有 docker 命令。"
    exit 1
fi

if docker network inspect "$NET" >/dev/null 2>&1; then
    echo "[OK] docker network ${NET} 已存在"
else
    if docker network create "$NET"; then
        echo "[OK] 已创建 docker network ${NET}"
    else
        echo "[FATAL] 创建 docker network ${NET} 失败。"
        exit 1
    fi
fi

# 容器名精确匹配（docker ps --filter name= 为子串匹配，不可靠）
running=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -Fx "$PROXY" || true)
if [ -z "$running" ]; then
    echo "[FATAL] 未发现运行中的容器「${PROXY}」。"
    echo "        本脚本不负责构建/启动被测服务。请先："
    echo "        1) 构建镜像 glaciertdb:latest（使用方自行 docker build）"
    echo "        2) 启动容器并命名为 ${PROXY}，接入网络 ${NET}，监听 ${PORT}"
    echo "        例如: docker run -d --name ${PROXY} --network ${NET} \\"
    echo "                 -v <数据目录>:/data glaciertdb:latest"
    echo "        然后重跑本脚本。"
    exit 1
fi

attached=$(docker inspect "$PROXY" --format \
    '{{range $k, $_ := .NetworkSettings.Networks}}{{$k}}{{"\n"}}{{end}}' 2>/dev/null \
    | grep -Fx "$NET" || true)
if [ -z "$attached" ]; then
    echo "[FATAL] 容器 ${PROXY} 在运行，但未接入 network ${NET}。"
    echo "        可用: docker network connect ${NET} ${PROXY}"
    exit 1
fi
echo "[OK] 容器 ${PROXY} 运行中，且已接入 ${NET}"
echo "[OK] 测试日志目录: ${LOG_DIR}"

# ----------------------------------------------------------------------------
# 1. 初始化建表
# ----------------------------------------------------------------------------
step "步骤 1/4: 执行 init_schema.sql 建表（DROP + CREATE，可重复执行）"

if docker run --rm -i --network "$NET" "$MYSQL_IMG" \
        mysql "${MYSQL_AUTH_ARGS[@]}" backup \
        < "$TESTS_DIR/init_schema.sql" >"$LOG_DIR/init_schema.out" 2>&1; then
    echo "[OK] init_schema.sql 执行成功"
    record "init_schema.sql                     : PASS"
else
    echo "[FAIL] init_schema.sql 执行失败（详见 $LOG_DIR/init_schema.out）："
    tail -n 15 "$LOG_DIR/init_schema.out" || true
    record "init_schema.sql                     : FAIL"
fi

# ----------------------------------------------------------------------------
# 2. mysql CLI 黑盒测试
# ----------------------------------------------------------------------------
step "步骤 2/4: mysql CLI 测试（test_mysql_cli.sh）"

if bash "$TESTS_DIR/test_mysql_cli.sh" 2>&1 | tee "$LOG_DIR/test_mysql_cli.out"; then
    record "test_mysql_cli.sh                   : PASS"
else
    record "test_mysql_cli.sh                   : FAIL"
fi

# ----------------------------------------------------------------------------
# 2.5 SCRIPT 后处理测试夹具（test_v111.py 用；写入被测容器 /data 卷的 scripts/ 目录）
# ----------------------------------------------------------------------------
DATA_DIR=$(docker inspect "$PROXY" --format '{{range .Mounts}}{{if eq .Destination "/data"}}{{.Source}}{{end}}{{end}}' 2>/dev/null || true)
if [ -n "$DATA_DIR" ] && [ -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR/scripts"
    printf '#!/bin/sh\ncat >/dev/null\necho SCRIPT_OK\n' > "$DATA_DIR/scripts/pipe.js"
    printf '#!/bin/sh\ncat >/dev/null\necho SCRIPT_OK\n' > "$DATA_DIR/scripts/pipe.php"
    echo "[OK] SCRIPT 测试夹具就绪: $DATA_DIR/scripts/pipe.js（+ pipe.php）"
else
    echo "[SKIP] 未找到被测容器 /data 挂载，SCRIPT 用例将验证错误路径"
fi

# ----------------------------------------------------------------------------
# 3. Python 三件套（pymysql / sqlalchemy / concurrent）
# ----------------------------------------------------------------------------
step "步骤 3/4: Python 测试（PyMySQL / SQLAlchemy / 并发，容器 ${PY_IMG}）"

# 内层脚本：pip 优先清华源（直连）；失败则带代理走 pypi 官方源重试；
# 随后依次跑三个测试脚本，任何一个失败最终 exit 非零。
PYRUN=$(cat <<'EOF'
set -u
if (unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY; \
    pip install --no-cache-dir -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL SQLAlchemy cryptography pika redis pillow); then
    echo "[OK] pip 安装成功（清华镜像源直连）"
elif (export http_proxy="__PIP_PROXY__" https_proxy="__PIP_PROXY__" \
                   HTTP_PROXY="__PIP_PROXY__" HTTPS_PROXY="__PIP_PROXY__"; \
      pip install --no-cache-dir -q PyMySQL SQLAlchemy cryptography pika redis pillow); then
    echo "[OK] pip 安装成功（pypi 官方源 + 代理）"
else
    echo "[FAIL] pip 安装失败（清华源直连 与 官方源+代理 均不可用）"
    exit 99
fi
rc=0
echo "---- test_pymysql.py ----";    python /tests/test_pymysql.py    || rc=1
echo "---- test_sqlalchemy.py ----"; python /tests/test_sqlalchemy.py || rc=1
echo "---- test_concurrent.py ----"; python /tests/test_concurrent.py || rc=1
echo "---- test_timeseries.py ----"; python /tests/test_timeseries.py || rc=1
echo "---- test_admin.py ----"; python /tests/test_admin.py || rc=1
echo "---- test_navicat.py ----"; python /tests/test_navicat.py || rc=1
echo "---- test_mysql_compat.py ----"; python /tests/test_mysql_compat.py || rc=1
echo "---- test_parallel.py ----"; python /tests/test_parallel.py || rc=1
echo "---- test_async.py ----"; python /tests/test_async.py || rc=1
echo "---- test_v107.py ----"; python /tests/test_v107.py || rc=1
echo "---- test_v108.py ----"; python /tests/test_v108.py || rc=1
echo "---- test_v109.py ----"; python /tests/test_v109.py || rc=1
echo "---- test_v110.py ----"; python /tests/test_v110.py || rc=1
echo "---- test_s3.py ----";  python /tests/test_s3.py  || rc=1
echo "---- test_v111.py ----"; python /tests/test_v111.py || rc=1
echo "---- test_v120.py ----"; python /tests/test_v120.py || rc=1
echo "---- test_v130.py ----"; python /tests/test_v130.py || rc=1
echo "---- test_v131.py ----"; python /tests/test_v131.py || rc=1
echo "---- test_v132.py ----"; python /tests/test_v132.py || rc=1
echo "---- test_v133.py ----"; python /tests/test_v133.py || rc=1
echo "---- test_v134.py ----"; python /tests/test_v134.py || rc=1
echo "---- test_v135.py ----"; python /tests/test_v135.py || rc=1
echo "---- test_v136.py ----"; python /tests/test_v136.py || rc=1
echo "---- test_v137.py ----"; python /tests/test_v137.py || rc=1
echo "---- test_v138.py ----"; python /tests/test_v138.py || rc=1
echo "---- test_v1310.py ----"; python /tests/test_v1310.py || rc=1

echo "---- test_v1311.py ----"; python /tests/test_v1311.py || rc=1
echo "---- test_v1312.py ----"; python /tests/test_v1312.py || rc=1
echo "---- test_v1313.py ----"; python /tests/test_v1313.py || rc=1
echo "---- test_v1314.py ----"; python /tests/test_v1314.py || rc=1
exit $rc
EOF
)
PYRUN="${PYRUN//__PIP_PROXY__/$PIP_PROXY}"

if docker run --rm --network "$NET" \
        -v "$TESTS_DIR":/tests:ro \
        -e http_proxy="$PIP_PROXY" -e https_proxy="$PIP_PROXY" \
        --name pyclient-$(date +%s) --network-alias pyclient "$PY_IMG" bash -c "$PYRUN" 2>&1 | tee "$LOG_DIR/python_tests.out"; then
    record "python 三件套（pymysql/sqlalchemy/concurrent）: PASS"
else
    record "python 三件套（pymysql/sqlalchemy/concurrent）: FAIL"
fi

echo
echo "—— 各 Python 测试的结论标记 ——"
grep -E 'PYMYSQL:|SQLALCHEMY:|CONCURRENCY:' "$LOG_DIR/python_tests.out" || \
    echo "（未捕获到结论标记，详见 $LOG_DIR/python_tests.out）"

# ----------------------------------------------------------------------------
# 3.4 v1.3.10：本机回环防爆破豁免 / CLI 解锁 / mysql.enable 开关
# ----------------------------------------------------------------------------
step "步骤 3.4/4: v1.3.10 回环豁免 / CLI 解锁 / mysql.enable"

# ① 本机回环（127.0.0.1）豁免：在 proxy 网络命名空间内连打 40 次错误密码，
#    正确密码登录必须仍成功（IP 维度永不锁定本机地址）
if docker run --rm --network "container:$PROXY" "$PY_IMG" bash -c '
    pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1
    python - <<PYEOF
import pymysql
for i in range(40):
    try:
        pymysql.connect(host="127.0.0.1", port=3309, user="clilock1310",
                        password="bad-%d" % i, connect_timeout=5)
    except Exception:
        pass
try:
    c = pymysql.connect(host="127.0.0.1", port=3309, user="glacier",
                        password="iloveyou~", connect_timeout=5)
    c.close()
    print("LOOPBACK_OK")
except Exception as e:
    print("LOOPBACK_BLOCKED: %r" % (e,))
PYEOF
' 2>&1 | tee "$LOG_DIR/loopback.out" | grep -q LOOPBACK_OK; then
    record "loopback brute-force exemption  : PASS"
else
    record "loopback brute-force exemption  : FAIL"
fi

# ② CLI 解锁（服务运行中旁路解锁）：-unlock-user 清除上面留下的账号锁
UNLOCK_OUT=$(docker exec "$PROXY" /app/glaciertdb -config /app/config.yaml -db-dir /data -unlock-user clilock1310 2>&1 || true)
echo "$UNLOCK_OUT" | tee "$LOG_DIR/cli_unlock.out"
LOCKS_OUT=$(docker exec "$PROXY" /app/glaciertdb -config /app/config.yaml -db-dir /data -show-locks 2>&1 || true)
echo "$LOCKS_OUT" | tee -a "$LOG_DIR/cli_unlock.out"
if echo "$UNLOCK_OUT" | grep -q "已解锁" && ! echo "$LOCKS_OUT" | grep -q "acct:clilock1310"; then
    record "CLI -unlock-user / -show-locks    : PASS"
else
    record "CLI -unlock-user / -show-locks    : FAIL"
fi

# ③ mysql.enable=false 冒烟：3309 不监听（kv/mq 同关 ⇒ 无任何复用服务），
#    进程保持运行（非退出崩溃），tmp 独立数据目录与目录锁
NMX="proxy-nomysql-$$"
rm -rf /tmp/gnm-data && mkdir -p /tmp/gnm-data
cat > /tmp/glacier-nomysql.yaml <<CFG
listen: 0.0.0.0:3309
db_dir: /data
default_db: backup
mysql:
  enable: false
kv:
  enable: false
mq:
  enable: false
security:
  encrypt: true
  passphrase: "GlacierTDB"
CFG
docker rm -f "$NMX" >/dev/null 2>&1 || true
docker run -d --name "$NMX" \
    -v /tmp/glacier-nomysql.yaml:/app/config.yaml \
    -v /tmp/gnm-data:/data glaciertdb:latest >/dev/null 2>&1
sleep 3
NMX_ALIVE=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -Fx "$NMX" || true)
NMX_LOG=$(docker logs "$NMX" 2>&1 || true)
NMX_PORT=$(docker run --rm --network "container:$NMX" "$PY_IMG" python -c "
import socket
try:
    s = socket.create_connection(('127.0.0.1', 3309), 3)
    s.close()
    print('PORT_OPEN')
except ConnectionRefusedError:
    print('PORT_CLOSED')
except Exception as e:
    print('PORT_ERR:%s' % e)
" 2>&1 || true)
echo "$NMX_LOG" | tail -5 | tee "$LOG_DIR/mysql_off.out"
echo "port probe: $NMX_PORT" | tee -a "$LOG_DIR/mysql_off.out"
if [ -n "$NMX_ALIVE" ] && echo "$NMX_LOG" | grep -q "3309 复用服务全部禁用" && [ "$NMX_PORT" = "PORT_CLOSED" ]; then
    record "mysql.enable=false 不监听 3309  : PASS"
else
    record "mysql.enable=false 不监听 3309  : FAIL"
fi
docker rm -f "$NMX" >/dev/null 2>&1 || true

# ----------------------------------------------------------------------------
# 3.5 KV 持久化：重启 proxy 容器后数据无损恢复（v1.2.0）
# ----------------------------------------------------------------------------
step "步骤 3.5/4: KV 持久化（容器重启后 SQLite 恢复）"
if docker restart "$PROXY" >/dev/null 2>&1; then
    sleep 3
    if docker run --rm --network "$NET" -v "$TESTS_DIR":/tests:ro "$PY_IMG"         python /tests/test_v120.py --check-restart; then
        record "test_v120 restart persistence : PASS"
    else
        record "test_v120 restart persistence : FAIL"
    fi
    if docker run --rm --network "$NET" -v "$TESTS_DIR":/tests:ro "$PY_IMG" bash -c         "pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple pika >/dev/null 2>&1 && python /tests/test_v130.py --check-restart"; then
        record "test_v130 restart persistence : PASS"
    else
        record "test_v130 restart persistence : FAIL"
    fi
    if docker run --rm --network "$NET" -v "$TESTS_DIR":/tests:ro "$PY_IMG" bash -c         "pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL pika redis >/dev/null 2>&1 && python /tests/test_v133.py --check-restart"; then
        record "test_v133 restart persistence : PASS"
    else
        record "test_v133 restart persistence : FAIL"
    fi
    if docker run --rm --network "$NET" -v "$TESTS_DIR":/tests:ro "$PY_IMG" bash -c         "pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1 && python /tests/test_v138.py --check-restart"; then
        record "test_v138 encrypted restart persistence : PASS"
    else
        record "test_v138 encrypted restart persistence : FAIL"
    fi
else
    echo "[FAIL] docker restart $PROXY 失败"
    record "test_v120 restart persistence : FAIL"
fi

# ----------------------------------------------------------------------------
# 3.6 v1.3.12：在线备份 CLI + TLS 双模式专项
# ----------------------------------------------------------------------------
step "步骤 3.6/4: v1.3.12 在线备份 CLI / TLS 双模式"

# ① 在线备份（服务运行中）：全部库 VACUUM INTO 快照 → integrity_check 校验
BK_OUT=$(docker exec "$PROXY" /app/glaciertdb -config /app/config.yaml -db-dir /data \
    -backup /data/backup-test -backup-verify=true 2>&1 || true)
echo "$BK_OUT" | tail -8 | tee "$LOG_DIR/backup_cli.out"
BK_FILES=$(docker exec "$PROXY" sh -c 'ls /data/backup-test/*.db 2>/dev/null | wc -l' 2>/dev/null || echo 0)
if echo "$BK_OUT" | grep -q "backup done" && [ "$BK_FILES" -ge 1 ]; then
    record "CLI -backup 在线备份 + 校验     : PASS"
else
    record "CLI -backup 在线备份 + 校验     : FAIL"
fi
# 备份后服务写路径仍健康（REPLACE 保证重复执行幂等）
if docker run --rm -i --network "$NET" "$MYSQL_IMG" mysql "${MYSQL_AUTH_ARGS[@]}" \
        -e "source /dev/stdin" <<< "USE backup; CREATE TABLE IF NOT EXISTS bk_alive_v1312(id INT PRIMARY KEY); REPLACE INTO bk_alive_v1312 VALUES(1);" >/dev/null 2>&1; then
    record "备份后服务写路径健康           : PASS"
else
    record "备份后服务写路径健康           : FAIL"
fi
docker exec "$PROXY" sh -c 'rm -rf /data/backup-test' >/dev/null 2>&1 || true

# ② TLS 双模式：自签证书起独立容器（tls.enable=true），验证
#    TLS-first RESP / 同端口明文 RESP / MySQL over TLS / 明文 MySQL 共存
TLSC=/tmp/gt-tls-$$
rm -rf "$TLSC" && mkdir -p "$TLSC/data" "$TLSC/certs"
openssl req -x509 -newkey rsa:2048 -keyout "$TLSC/certs/key.pem" -out "$TLSC/certs/cert.pem" \
    -days 2 -nodes -subj "/CN=gt-tls" \
    -addext "subjectAltName=DNS:gt-tls,DNS:proxy,DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
cat > "$TLSC/config.yaml" <<CFG
listen: 0.0.0.0:3399
db_dir: /data
default_db: t
auth:
  user: glacier
  password: "iloveyou~"
kv:
  enable: true
mysql:
  enable: true
mq:
  enable: false
security:
  encrypt: false
tls:
  enable: true
  cert_file: /certs/cert.pem
  key_file: /certs/key.pem
CFG
TN="gt-tls-$$"
docker rm -f "$TN" >/dev/null 2>&1 || true
docker run -d --name "$TN" --network "$NET" --network-alias gt-tls \
    -v "$TLSC/config.yaml":/app/config.yaml -v "$TLSC/data":/data \
    -v "$TLSC/certs":/certs glaciertdb:latest >/dev/null 2>&1
sleep 3
if docker run --rm --network "$NET" -v "$TESTS_DIR":/tests:ro -v "$TLSC/certs":/certs:ro "$PY_IMG" bash -c \
    "pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1 && python /tests/test_v1312_tls.py gt-tls 3399 /certs/cert.pem" \
    2>&1 | tee "$LOG_DIR/tls_test.out" | grep -q "0 fail"; then
    record "TLS 双模式（RESP/MySQL/明文共存）: PASS"
else
    record "TLS 双模式（RESP/MySQL/明文共存）: FAIL"
fi
docker logs "$TN" 2>&1 | grep -E "tls:|listening" | head -4 | tee "$LOG_DIR/tls_server.log" || true
docker rm -f "$TN" >/dev/null 2>&1 || true
rm -rf "$TLSC"

# ----------------------------------------------------------------------------
# 3.5 v1.3.13 场景：版本号隐藏 + 心跳上报 + 数据主动同步（独立容器拓扑）
# ----------------------------------------------------------------------------
step "v1.3.13 场景：hide_version + heartbeat + 数据北上同步"

S13C="$(mktemp -d /tmp/gt-s13.XXXXXX)"
mkdir -p "$S13C/remote-data" "$S13C/proxy-data" "$S13C/recv"

# 心跳 / 同步回调接收器
cat > "$S13C/recv/recv.py" <<'PYEOF2'
import json, threading
from http.server import BaseHTTPRequestHandler, HTTPServer
EV = []
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        try:
            payload = json.loads(body.decode())
        except Exception:
            payload = {"raw": body.decode("utf-8", "replace")}
        kind = "cb" if self.headers.get("X-Glacier-Event") == "sync_result" else "beat"
        EV.append({"kind": kind, "event": self.headers.get("X-Glacier-Event", ""), "payload": payload})
        self.send_response(200); self.end_headers()
    def do_GET(self):
        if self.path == "/dump":
            b = json.dumps({"events": EV}).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *a):
        pass
HTTPServer(("0.0.0.0", 8000), H).serve_forever()
PYEOF2

# 远端实例（云端 S3 端点）配置
cat > "$S13C/remote.yaml" <<CFG
listen: 0.0.0.0:3309
db_dir: /data
default_db: backup
auth:
  user: glacier
  password: "iloveyou~"
s3:
  enable: true
  region: "*"
  endpoints:
    - listen: 0.0.0.0:9000
security:
  encrypt: false
CFG

# 被测实例配置：hide_version + heartbeat + sync 规则
cat > "$S13C/proxy.yaml" <<CFG
listen: 0.0.0.0:3309
db_dir: /data
default_db: backup
auth:
  user: glacier
  password: "iloveyou~"
security:
  encrypt: false
  hide_version: true
heartbeat:
  url: http://pyrecv:8000/beat
  interval_s: 10
s3:
  enable: true
  region: "*"
  error_log: true
  endpoints:
    - listen: 0.0.0.0:9000
  sync:
    - name: e2e
      bucket: sbox
      remote_url: http://gt-remote:9000
      remote_bucket: rbucket
      remote_ak: __RAK__
      remote_sk: __RSK__
      callback_url: http://pyrecv:8000/cb
CFG

docker rm -f gt-s13 gt-remote pyrecv >/dev/null 2>&1 || true
docker run -d --name pyrecv --network "$NET" --network-alias pyrecv     -v "$S13C/recv":/srv "$PY_IMG" bash -c "python /srv/recv.py" >/dev/null 2>&1
docker run -d --name gt-remote --network "$NET" --network-alias gt-remote     -v "$S13C/remote.yaml":/app/config.yaml -v "$S13C/remote-data":/data     glaciertdb:latest >/dev/null 2>&1
sleep 3
# 远端建同步目标桶
docker run --rm --network "$NET" "$MYSQL_IMG" mysql -h gt-remote -P 3309 -uglacier -piloveyou~     --default-auth=mysql_native_password -e "USE GlacierTDB; CREATE BUCKET rbucket;" >/dev/null 2>&1
KEYROW="$(docker run --rm --network "$NET" "$MYSQL_IMG" mysql -h gt-remote -P 3309 -uglacier -piloveyou~     --default-auth=mysql_native_password -e "USE GlacierTDB; CREATE S3 KEY FOR 'glacier';" 2>/dev/null | tail -1)"
RAK="$(echo "$KEYROW" | awk '{print $1}')"
RSK="$(echo "$KEYROW" | awk '{print $2}')"
echo "remote key: AK=${RAK:0:8}... SK=${RSK:0:4}..."
sed -i "s/__RAK__/$RAK/; s/__RSK__/$RSK/" "$S13C/proxy.yaml"
docker run -d --name gt-s13 --network "$NET" --network-alias gt-s13     -v "$S13C/proxy.yaml":/app/config.yaml -v "$S13C/proxy-data":/data     glaciertdb:latest >/dev/null 2>&1
sleep 3
if docker run --rm --network "$NET" -v "$TESTS_DIR":/tests:ro "$PY_IMG" bash -c     "pip install -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL >/dev/null 2>&1 && python /tests/test_v1313_sync.py gt-s13 gt-remote pyrecv"     2>&1 | tee "$LOG_DIR/s13_sync_test.out" | grep -q "FAIL=0"; then
    record "v1.3.13 同步/隐藏/心跳场景: PASS"
else
    record "v1.3.13 同步/隐藏/心跳场景: FAIL"
fi
# 实例 ID 重启不变
IID1="$(docker run --rm --network "$NET" "$MYSQL_IMG" mysql -h gt-s13 -P 3309 -uglacier -piloveyou~     --default-auth=mysql_native_password -D backup -e "SELECT @@glaciertdb_instance_id;" 2>/dev/null | tail -1)"
docker restart gt-s13 >/dev/null 2>&1
sleep 3
IID2="$(docker run --rm --network "$NET" "$MYSQL_IMG" mysql -h gt-s13 -P 3309 -uglacier -piloveyou~     --default-auth=mysql_native_password -D backup -e "SELECT @@glaciertdb_instance_id;" 2>/dev/null | tail -1)"
if [ -n "$IID1" ] && [ "$IID1" = "$IID2" ]; then
    record "实例 ID 重启不变 ($IID1): PASS"
else
    record "实例 ID 重启不变 ($IID1 -> $IID2): FAIL"
fi
# S3 结构化错误日志（error_log: true → docker logs 出现 s3_error JSON 行）
docker exec gt-s13 sh -c 'wget -q -O /dev/null http://127.0.0.1:9000/nope --header="Authorization: x" 2>/dev/null' >/dev/null 2>&1 || true
sleep 1
S13LOG="$(docker logs gt-s13 2>&1 || true)" # 先捕获再匹配（pipefail 下 grep -q 提前退出会吃 SIGPIPE 141 误判）
if echo "$S13LOG" | grep -q "s3_error {"; then
    record "S3 结构化错误日志（s3_error JSON 行）: PASS"
else
    record "S3 结构化错误日志（s3_error JSON 行）: FAIL"
fi
docker logs gt-s13 2>&1 | grep "s3_error {" | head -2 | tee "$LOG_DIR/s13_errorlog.out" >/dev/null || true
docker rm -f gt-s13 gt-remote pyrecv >/dev/null 2>&1 || true
rm -rf "$S13C"

# ----------------------------------------------------------------------------
# 4. 汇总
# ----------------------------------------------------------------------------
step "步骤 4/4: 结果汇总"

FAILED=0
for r in "${RESULTS[@]}"; do
    echo "  $r"
    case "$r" in
        *FAIL*) FAILED=1 ;;
    esac
done

echo
echo "完整日志目录: ${LOG_DIR}"
if [ "$FAILED" -eq 0 ]; then
    echo "ALL TESTS PASSED"
    exit 0
else
    echo "SOME TESTS FAILED"
    exit 1
fi
