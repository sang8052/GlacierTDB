#!/usr/bin/env bash
# scenario_v1313.sh — v1.3.13 场景专项（hide_version + heartbeat + 数据北上同步 + 实例 ID 重启不变 + 错误日志）
# 从 run_all.sh 抽取，可独立运行（需要 glaciertdb:latest 镜像与 sqlproxy-net 网络）：
#   bash tests/scenario_v1313.sh
set -uo pipefail
NET="sqlproxy-net"
MYSQL_IMG="mysql:8"
PY_IMG="python:3.12-slim"
TESTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$(mktemp -d /tmp/scenario-s13.XXXXXX)"
step() { echo; echo "== $* =="; }
record() { RESULTS+=("$1"); }
FAILED=0

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
if docker logs gt-s13 2>&1 | grep -q "s3_error {"; then
    record "S3 结构化错误日志（s3_error JSON 行）: PASS"
else
    record "S3 结构化错误日志（s3_error JSON 行）: FAIL"
fi
docker logs gt-s13 2>&1 | grep "s3_error {" | head -2 | tee "$LOG_DIR/s13_errorlog.out" >/dev/null || true
docker rm -f gt-s13 gt-remote pyrecv >/dev/null 2>&1 || true
rm -rf "$S13C"


echo
echo "---- 场景结果 ----"
for r in "${RESULTS[@]}"; do
    echo "  $r"
    case "$r" in *FAIL*) FAILED=1 ;; esac
done
[ "$FAILED" -eq 0 ] && echo "SCENARIO ALL PASSED" || echo "SCENARIO SOME FAILED"
exit $FAILED
