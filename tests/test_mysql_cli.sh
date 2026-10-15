#!/usr/bin/env bash
# ============================================================================
# test_mysql_cli.sh — 用 mysql:8 官方客户端对 proxy:3309 做黑盒功能测试
#
# 前置条件（由 run_all.sh 保证，本脚本不负责 build / 启动容器）:
#   * docker network sqlproxy-net 已存在
#   * 被测容器 proxy 已在该网络中运行（镜像 glaciertdb:latest）
#   * tests/init_schema.sql 已执行（表结构与初始数据就绪）
#
# 每步输出 PASS/FAIL，结尾输出总结；全部通过 exit 0，否则 exit 1。
#
# 注意：
#   * 服务端只透传 SQLite 语法，因此断言只用标准 SQL / 探活语句，
#     不使用 CONCAT 等 MySQL 专有函数。
#   * mysql:8 客户端默认支持 mysql_native_password，这里仍显式带上
#     --default-auth=mysql_native_password 兜底。
# ============================================================================
set -uo pipefail

NET="sqlproxy-net"
HOST="proxy"
PORT="3309"
USER="glacier"
DBPASS="iloveyou~"
DB="backup"
IMG="mysql:8"

PASS=0
FAIL=0

# ---- 基础设施 ---------------------------------------------------------------

# run_sql "SQL..." —— 每次调用 = 一次 docker run = 一条独立连接
run_sql() {
    docker run --rm --network "$NET" "$IMG" \
        mysql -h "$HOST" -P "$PORT" -u"$USER" -p"$DBPASS" \
        --default-auth=mysql_native_password \
        --default-character-set=utf8mb4 \
        "$DB" -e "$1" 2>/dev/null
}

pass() { PASS=$((PASS + 1)); echo "[PASS] $1"; }
fail() { FAIL=$((FAIL + 1)); echo "[FAIL] $1"; }

# assert_contains "描述" "$输出" "子串"
assert_contains() {
    if printf '%s\n' "$2" | grep -qF -- "$3"; then
        pass "$1"
    else
        fail "$1（期望输出包含「$3」，实际: $(printf '%s' "$2" | head -c 200 | tr '\n' '|')）"
    fi
}

# assert_word "描述" "$输出" "词" —— 整词匹配（避免 t1 匹配到 t1_log）
assert_word() {
    if printf '%s\n' "$2" | grep -qw -- "$3"; then
        pass "$1"
    else
        fail "$1（期望输出包含整词「$3」，实际: $(printf '%s' "$2" | head -c 200 | tr '\n' '|')）"
    fi
}

# assert_regex "描述" "$输出" "ERE"
assert_regex() {
    if printf '%s\n' "$2" | grep -Eq -- "$3"; then
        pass "$1"
    else
        fail "$1（期望匹配正则「$3」，实际: $(printf '%s' "$2" | head -c 200 | tr '\n' '|')）"
    fi
}

# assert_eq "描述" "实际" "期望"
assert_eq() {
    if [ "$2" = "$3" ]; then
        pass "$1"
    else
        fail "$1（期望 [$3]，实际 [$2]）"
    fi
}

# 纯无符号整数判断（防止把报错文本喂给算术运算）
is_uint() {
    case "$1" in
        '' | *[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

# ---- 探活：连不上直接退出 ---------------------------------------------------

echo "=== mysql CLI 黑盒测试 ==="
echo "目标: ${HOST}:${PORT}/${DB}  镜像: ${IMG}  网络: ${NET}"
echo

probe=$(run_sql "SELECT 1;")
if [ -z "$probe" ]; then
    echo "[FATAL] 无法连接 ${HOST}:${PORT}（SELECT 1 无输出）。"
    echo "        请确认容器 proxy 已启动并接入网络 ${NET}，且已执行 init_schema.sql。"
    exit 1
fi
pass "探活 SELECT 1 可执行"

# ---- 1. 探活语句应答表 ------------------------------------------------------

out=$(run_sql "SELECT @@version;")
assert_regex "SELECT @@version 返回版本号" "$out" '[0-9]+(\.[0-9]+)+'

out=$(run_sql "SHOW DATABASES;")
assert_contains "SHOW DATABASES 含 backup 库" "$out" "backup"

out=$(run_sql "SHOW TABLES;")
assert_word "SHOW TABLES 含 t1" "$out" "t1"
assert_word "SHOW TABLES 含 t2" "$out" "t2"
assert_word "SHOW TABLES 含 blob_t" "$out" "blob_t"
assert_word "SHOW TABLES 含 t1_log" "$out" "t1_log"
assert_word "SHOW TABLES 含视图 v1" "$out" "v1"
assert_contains "SHOW TABLES 含中文表「订单」" "$out" "订单"

out=$(run_sql "SHOW VARIABLES LIKE 'char%';")
assert_contains "SHOW VARIABLES LIKE 'char%' 返回字符集变量" "$out" "character_set_client"
assert_regex "字符集为 utf8 系" "$out" 'utf8'

out=$(run_sql "SHOW CREATE TABLE t1;")
assert_regex "SHOW CREATE TABLE t1 有 CREATE 语句" "$out" 'CREATE[[:space:]]+(TABLE|VIEW)'
assert_word "SHOW CREATE TABLE t1 含列 name" "$out" "name"
assert_word "SHOW CREATE TABLE t1 含列 val" "$out" "val"

out=$(run_sql "SHOW COLUMNS FROM t2;")
assert_word "SHOW COLUMNS FROM t2 含列 a" "$out" "a"
assert_word "SHOW COLUMNS FROM t2 含列 b" "$out" "b"

out=$(run_sql "SHOW INDEX FROM t2;")
assert_regex "SHOW INDEX FROM t2 含复合主键索引" "$out" 'PRIMARY|autoindex'

out=$(run_sql "SHOW WARNINGS;")
rc=$?
if [ "$rc" -eq 0 ] && [ -z "$out" ]; then
    pass "SHOW WARNINGS 无告警且不报错"
else
    fail "SHOW WARNINGS（rc=$rc, 输出=[$(printf '%s' "$out" | head -c 100)]）"
fi

# ---- 2. CRUD ---------------------------------------------------------------

MARK="cli_测试行"

run_sql "DELETE FROM t1 WHERE name='${MARK}';" >/dev/null   # 清理历史残留

out=$(run_sql "INSERT INTO t1(name, val) VALUES('${MARK}', 3.25); SELECT id, name, val FROM t1 WHERE name='${MARK}';")
assert_contains "INSERT 后能查到该行" "$out" "$MARK"
assert_contains "INSERT 的 val=3.25 正确" "$out" "3.25"

out=$(run_sql "UPDATE t1 SET val=6.5 WHERE name='${MARK}'; SELECT val FROM t1 WHERE name='${MARK}';")
assert_contains "UPDATE 后 val=6.5" "$out" "6.5"

out=$(run_sql "DELETE FROM t1 WHERE name='${MARK}'; SELECT COUNT(*) FROM t1 WHERE name='${MARK}';")
assert_eq "DELETE 后行数归零" "$(printf '%s\n' "$out" | tail -n 1)" "0"

# 初始数据仍在（负数浮点往返）
out=$(run_sql "SELECT name FROM t1 WHERE val < 0;")
assert_contains "初始中文数据/负浮点可读（李四 -2.75）" "$out" "李四"

# ---- 3. LAST_INSERT_ID -----------------------------------------------------
# 说明：t1 用 AUTOINCREMENT（高水位单调递增），历史删除过行时 insert_id 可能
# 大于 MAX(id)+1，因此断言放宽为「大于插入前 MAX(id)」。
before=$(run_sql "SELECT COALESCE(MAX(id), 0) FROM t1;" | tail -n 1)
out=$(run_sql "INSERT INTO t1(name, val) VALUES('cli_lid行', 9.75); SELECT LAST_INSERT_ID();")
got=$(printf '%s\n' "$out" | tail -n 1)
if is_uint "$got" && is_uint "$before" && [ "$got" -gt "$before" ]; then
    pass "LAST_INSERT_ID()=新增自增 id（插入前 MAX(id)=${before}，得到 ${got}）"
else
    fail "LAST_INSERT_ID()（插入前 MAX(id)=${before}，得到 [${got}]）"
fi
run_sql "DELETE FROM t1 WHERE name='cli_lid行';" >/dev/null

# ---- 4. 事务 BEGIN / COMMIT / ROLLBACK ------------------------------------
# 同一条 -e 里的多条语句走同一连接，事务才有意义。

c0=$(run_sql "SELECT COUNT(*) FROM t1;" | tail -n 1)
run_sql "BEGIN; INSERT INTO t1(name, val) VALUES('cli_回滚行', 0.5); ROLLBACK;" >/dev/null
c1=$(run_sql "SELECT COUNT(*) FROM t1;" | tail -n 1)
assert_eq "BEGIN..INSERT..ROLLBACK 后行数不变" "$c1" "$c0"

run_sql "BEGIN; INSERT INTO t1(name, val) VALUES('cli_提交行', 1.25); COMMIT;" >/dev/null
c2=$(run_sql "SELECT COUNT(*) FROM t1;" | tail -n 1)
if is_uint "$c0" && [ "$c2" = "$((c0 + 1))" ]; then
    pass "BEGIN..INSERT..COMMIT 后行数+1（${c0} -> ${c2}）"
else
    fail "BEGIN..INSERT..COMMIT 后行数+1（期望 $((c0 + 1))，实际 [${c2}]）"
fi
run_sql "DELETE FROM t1 WHERE name='cli_提交行';" >/dev/null

# ---- 5. 触发器 -------------------------------------------------------------

l0=$(run_sql "SELECT COUNT(*) FROM t1_log;" | tail -n 1)
run_sql "INSERT INTO t1(name, val) VALUES('cli_触发器行', 2.5);" >/dev/null
l1=$(run_sql "SELECT COUNT(*) FROM t1_log;" | tail -n 1)
if is_uint "$l0" && is_uint "$l1" && [ "$l1" = "$((l0 + 1))" ]; then
    pass "触发器 trg1 使 t1_log 行数+1（${l0} -> ${l1}）"
else
    fail "触发器 trg1 使 t1_log 行数+1（期望 $((l0 + 1))，实际 [${l1}]）"
fi
msg=$(run_sql "SELECT msg FROM t1_log ORDER BY id DESC LIMIT 1;" | tail -n 1)
assert_contains "t1_log 最新日志内容为 t1_insert" "$msg" "t1_insert"
run_sql "DELETE FROM t1 WHERE name='cli_触发器行';" >/dev/null

# ---- 6. 视图 ---------------------------------------------------------------

out=$(run_sql "SELECT name FROM v1 ORDER BY id LIMIT 3;")
assert_contains "视图 v1 可查询（含初始行 张三）" "$out" "张三"

# ---- 7. 中文表名/列名（反引号） --------------------------------------------

run_sql "INSERT INTO \`订单\`(\`名称\`) VALUES('客户端订单');" >/dev/null
out=$(run_sql "SELECT \`编号\`, \`名称\` FROM \`订单\` ORDER BY \`编号\`;")
assert_contains "中文表「订单」可读（初始行 第一笔订单）" "$out" "第一笔订单"
assert_contains "中文表「订单」可写（新行 客户端订单）" "$out" "客户端订单"
out=$(run_sql "SELECT COUNT(*) FROM \`订单\` WHERE \`名称\`='客户端订单';")
assert_eq "中文表条件查询行数=1" "$(printf '%s\n' "$out" | tail -n 1)" "1"
run_sql "DELETE FROM \`订单\` WHERE \`名称\`='客户端订单';" >/dev/null

# ---- 总结 -------------------------------------------------------------------

echo
echo "--------------------------------------------"
echo "MYSQL_CLI 结果: PASS=${PASS} FAIL=${FAIL}"
if [ "$FAIL" -eq 0 ]; then
    echo "MYSQL_CLI: ALL PASS"
    exit 0
else
    echo "MYSQL_CLI: FAILED"
    exit 1
fi
