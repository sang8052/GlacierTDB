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
# 3. Python 三件套（pymysql / sqlalchemy / concurrent）
# ----------------------------------------------------------------------------
step "步骤 3/4: Python 测试（PyMySQL / SQLAlchemy / 并发，容器 ${PY_IMG}）"

# 内层脚本：pip 优先清华源（直连）；失败则带代理走 pypi 官方源重试；
# 随后依次跑三个测试脚本，任何一个失败最终 exit 非零。
PYRUN=$(cat <<'EOF'
set -u
if (unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY; \
    pip install --no-cache-dir -q -i https://pypi.tuna.tsinghua.edu.cn/simple PyMySQL SQLAlchemy); then
    echo "[OK] pip 安装成功（清华镜像源直连）"
elif (export http_proxy="__PIP_PROXY__" https_proxy="__PIP_PROXY__" \
                   HTTP_PROXY="__PIP_PROXY__" HTTPS_PROXY="__PIP_PROXY__"; \
      pip install --no-cache-dir -q PyMySQL SQLAlchemy); then
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
