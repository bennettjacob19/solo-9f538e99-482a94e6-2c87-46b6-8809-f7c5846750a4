#!/usr/bin/env bash
# 端到端自测：在本机用便携 PostgreSQL + uvicorn 跑全链路验证。
# 用法：bash tests/selftest.sh
# 不依赖 Docker；Docker Compose 场景的启动方式见 README。
set -u
cd "$(dirname "$0")/.."
REPO=$PWD

# ------------------------------------------------------------- 环境定位
PGBIN=${PGBIN:-/tmp/pgdeb/extracted/usr/lib/postgresql/15/bin}
PGCLIENT=${PGCLIENT:-/tmp/pgdeb/client/usr/lib/postgresql/15/bin}
export PATH="$PGCLIENT:$PGBIN:$PATH"
PGDATA=${PGDATA:-/tmp/selftest-pgdata}
PGPORT=55433
PGSOCK=/tmp/selftest-pgsock
SQLDIR=/tmp/selftest-sqlite
APPPORT=8011
BASE="http://127.0.0.1:$APPPORT"
SHA64="a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
DSN="host=$PGSOCK port=$PGPORT dbname=postgres user=postgres"
PASS=0; FAIL=0

ok()   { echo "PASS: $1"; PASS=$((PASS+1)); }
bad()  { echo "FAIL: $1"; FAIL=$((FAIL+1)); }
check(){ if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 (got '$1' want '$2')"; fi; }
api()  { curl -s -m 10 "http://127.0.0.1:$APPPORT$1"; }
wait_phase(){ # wait_phase <expected> <timeout_s>
  local want=$1 deadline=$((SECONDS+$2))
  while [ "$SECONDS" -lt "$deadline" ]; do
    [ "$(api /api/progress | jq -r .phase)" = "$want" ] && return 0
    sleep 0.5
  done
  return 1
}
start_app(){ # start_app <sqlite_dir> [init_sleep_ms]
  local dir=$1 sleep_ms=${2:-0}
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$dir"
  PG_DSN="$DSN" SQLITE_PATH="$dir/history.db" CAPTURE_INIT_SLEEP_MS="$sleep_ms" \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
_wait_app_ready(){ # _wait_app_ready <expected_pid>
  local expected=$1
  for _ in $(seq 1 40); do
    if ! kill -0 "$expected" 2>/dev/null; then
      echo "new app exited early:"; cat /tmp/selftest-app.log; return 1
    fi
    if curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1; then
      # 确认响应者确实是新进程而不是占用端口的旧实例
      [ "$(app_pid)" = "$expected" ] && return 0
    fi
    sleep 0.5
  done
  echo "app failed to become ready:"; cat /tmp/selftest-app.log; return 1
}
stop_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill $pid 2>/dev/null || true
  for _ in $(seq 1 20); do
    app_pid | grep -q . || return 0
    sleep 0.5
  done
}
force_kill_app(){
  local pid
  pid=$(app_pid)
  [ -n "$pid" ] && kill -9 $pid 2>/dev/null || true
  sleep 1
}
app_pid(){ ps -eo pid,comm,args | awk '$2=="python3" && /uvicorn app.main:app/ {print $1}'; }
pg_ctl_q(){ pg_ctl -D "$PGDATA" -l /tmp/selftest-pg.log "$@"; }

cleanup(){
  stop_app || true
  pg_ctl_q stop -m fast >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ------------------------------------------------------------- 0. 准备源库
echo "=== 0. 准备全新源 PostgreSQL（wal_level=logical）==="
pg_ctl_q stop -m fast >/dev/null 2>&1 || true
rm -rf "$PGDATA" "$PGSOCK" "$SQLDIR"; mkdir -p "$PGSOCK"
initdb -D "$PGDATA" -U postgres --auth=trust -E UTF8 --no-sync >/dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
unix_socket_directories = '$PGSOCK'
listen_addresses = ''
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF
pg_ctl_q start -w >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -q -f init/01_schema.sql
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc 'select count(*) from artifacts')" \
      "3" "源表初始化 3 行示例数据"

# ------------------------------------------------------------- 1. 初始化未完成不开放历史（503）
echo "=== 1. 初始化窗口历史接口 503 ==="
start_app "$SQLDIR" 8000
sleep 1
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/points")
check "$code" "503" "INITIALIZING 期间 /api/points 返回 503"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=0/1")
check "$code" "503" "INITIALIZING 期间 /api/directory 返回 503"
check "$(api /api/progress | jq -r .phase)" "INITIALIZING" "进度接口暴露 INITIALIZING"
wait_phase STREAMING 15 && ok "初始化完成进入 STREAMING" || bad "初始化超时"
base_lsn=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
[ -n "$base_lsn" ] && ok "基线提交点存在: $base_lsn" || bad "缺少基线提交点"
check "$(api "/api/directory?point=$base_lsn" | jq '.artifacts|length')" "3" "基线目录 3 行"

# ------------------------------------------------------------- 2. 基线前开始、基线后提交的事务不漏
echo "=== 2. 跨基线事务（初始化窗口开启，基线后提交）==="
stop_app; sleep 1
rm -rf "$SQLDIR"
# 同步等待旧槽完全删除，避免新实例误走 resume 分支
for _ in $(seq 1 20); do
  psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select pg_drop_replication_slot(slot_name) from pg_replication_slots" 2>/dev/null || true
  n=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
    "select count(*) from pg_replication_slots")
  [ "$n" = "0" ] && break
  sleep 0.5
done
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from pg_replication_slots")" "0" "旧复制槽已彻底删除，确保走全新基线"
# 清理历史失败运行可能残留的测试行（应用已停，不会再写入），
# 把源目录恢复到与 init/01_schema.sql 一致的 3 行初始状态
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "delete from artifacts where artifact_no not in (1001,1002,1003)" >/dev/null
psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "insert into artifacts values
   (1001,'billing-gateway','1.4.2','a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90'),
   (1002,'risk-engine','2.7.0','0f1e2d3c4b5a69788796a5b4c3d2e1f009182736455463728190aabbccddeeff'),
   (1003,'notify-hub','0.9.3','ffeeddccbbaa9988776655443322110099887766554433221100998877665544')
   on conflict do nothing" >/dev/null
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc "select count(*) from artifacts")" \
      "3" "源目录已重置为 3 行初始状态"
# 用应用内置测试钩子确定性构造"基线前开始、基线后提交"的事务：
# 应用在建槽/基线前打开源事务写入 2001 并持有，基线落盘后才提交。
start_app_hook(){
  stop_app
  for _ in $(seq 1 20); do
    curl -s -m 1 "http://127.0.0.1:$APPPORT/api/progress" >/dev/null 2>&1 || break
    sleep 0.5
  done
  mkdir -p "$SQLDIR"
  PG_DSN="$DSN" SQLITE_PATH="$SQLDIR/history.db" \
    TEST_INFLIGHT_ARTIFACT_NO=2001 TEST_INFLIGHT_SLEEP_S=3 \
    setsid nohup python3 -m uvicorn app.main:app --host 127.0.0.1 \
    --port "$APPPORT" > /tmp/selftest-app.log 2>&1 < /dev/null &
  local new_pid=$!
  echo "$new_pid" > /tmp/selftest-app.pid
  _wait_app_ready "$new_pid"
}
start_app_hook || bad "带钩子的应用启动失败"
wait_phase STREAMING 20 || bad "钩子场景初始化超时"
grep -q "TEST hook: inflight transaction opened" /tmp/selftest-app.log \
  && ok "测试钩子事务已在基线前打开" \
  || bad "测试钩子未生效: $(grep -c 'TEST hook' /tmp/selftest-app.log)"
# 基线快照里不允许出现 2001（事务尚未提交）
new_base=$(api /api/points | jq -r '.points[]|select(.kind=="BASELINE").lsn')
check "$(api "/api/directory?point=$new_base" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "0" "跨基线事务未混入基线快照（不重）"
# 等钩子事务在基线后提交并被复制流捕获
for _ in $(seq 1 30); do
  n=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
  [ "$n" = "1" ] && break
  sleep 1
done
found=$(api /api/history/2001 | jq '[.history[]|select(.change_type=="INSERT")]|length')
check "$found" "1" "基线前开始、基线后提交的事务被复制流补捕获，无遗漏"
sleep 2
inflight_lsn=$(api /api/points | jq -r '.points[-1].lsn')
check "$(api "/api/directory?point=$inflight_lsn" | jq '[.artifacts[]|select(.artifact_no==2001)]|length')" \
      "1" "跨基线事务在其提交点目录中可见"

# ------------------------------------------------------------- 3. 多行事务整体可见 + 删除后重用编号
echo "=== 3. 多行事务原子性与编号重用 ==="
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":3001,\"service_name\":\"g1\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"insert\",\"artifact_no\":3002,\"service_name\":\"g2\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":1002}
 ]}" >/dev/null
sleep 2
points=$(api /api/points | jq '[.points[]|select(.kind=="TXN" and .change_count>=3)]|length')
[ "$points" -ge 1 ] && ok "三行操作只产生一个提交点（整体可见）" || bad "多行事务被拆分"
latest=$(api /api/points | jq -r '.points[-1].lsn')
dir3001=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==3001)]|length')
dir1002=$(api "/api/directory?point=$latest" | jq '[.artifacts[]|select(.artifact_no==1002)]|length')
check "$dir3001" "1" "同事务插入的 3001 在提交点可见"
check "$dir1002" "0" "同事务删除的 1002 在提交点消失"
# 重用 1002
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":1002,\"service_name\":\"reborn\",\"version\":\"2\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
chain=$(api /api/history/1002 | jq -r '[.history[].change_type]|join(",")')
case "$chain" in
  *BASELINE*DELETE*INSERT*) ok "删除后重用编号，旧历史完整保留: $chain" ;;
  *) bad "编号重用历史链异常: $chain" ;;
esac

# ------------------------------------------------------------- 4. 失败整组回滚 + 输入校验
echo "=== 4. 回滚与固定结构校验 ==="
code=$(curl -s -o /tmp/r.json -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"insert\",\"artifact_no\":9001,\"service_name\":\"ghost\",\"version\":\"1\",\"sha256\":\"$SHA64\"},
  {\"op\":\"delete\",\"artifact_no\":999999}
 ]}")
check "$code" "400" "组内失败返回 400"
check "$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select count(*) from artifacts where artifact_no=9001")" "0" "失败整组回滚，源库无残留"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d '{"operations":[{"op":"insert","artifact_no":1,"service_name":"x","version":"1","sha256":"bad"}]}')
check "$code" "422" "非法 sha256 被 pydantic 拒绝"
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/directory?point=not-a-lsn")
check "$code" "400" "非法 LSN 参数 400"

# ------------------------------------------------------------- 5. 强杀应用后续接：无重复、无丢失、槽位不超前落盘
echo "=== 5. 强杀重启一致性 ==="
before_txns=$(api /api/progress | jq .captured_transactions)
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"7.7.7\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
after_txns=$(api /api/progress | jq .captured_transactions)
[ "$after_txns" -gt "$before_txns" ] && ok "新事务已捕获 ($before_txns -> $after_txns)" || bad "新事务未捕获"
kill -9 "$(app_pid)" 2>/dev/null || true; sleep 2
start_app "$SQLDIR"
wait_phase STREAMING 15 || true
sleep 3
check "$(api /api/progress | jq .captured_transactions)" "$after_txns" "强杀重启后事务数不重复"
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="7.7.7")]|length')" "1" "强杀前最后一次更新仍在"
flush=$(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tAc \
  "select confirmed_flush_lsn from pg_replication_slots where slot_name='artifacts_history_slot'")
last=$(api /api/progress | jq -r .last_commit_lsn)
if [ -n "$flush" ] && [ -n "$last" ]; then
  fnum=$(python3 -c "h,l='$flush'.split('/');print((int(h,16)<<32)+int(l,16))")
  lnum=$(python3 -c "h,l='$last'.split('/');print((int(h,16)<<32)+int(l,16))")
  [ "$fnum" -le "$lnum" ] && ok "槽 confirmed_flush_lsn($flush) 不超前已落盘提交点($last)" \
                            || bad "槽位超前落盘位置: $flush > $last"
fi
# 重启后再写一条，验证续接后的流仍然工作
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"8.8.8\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="8.8.8")]|length')" "1" "续接后新变更继续被捕获"

# ------------------------------------------------------------- 6. 源库断连恢复
echo "=== 6. 源库重启后自动重连续接 ==="
pg_ctl_q restart -w >/dev/null
sleep 1
for _ in $(seq 1 20); do
  api /api/progress | jq -e '.phase=="STREAMING"' >/dev/null && break
  sleep 1
done
check "$(api /api/progress | jq -r .phase)" "STREAMING" "源库重启后应用自动恢复 STREAMING"
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",\"version\":\"9.9.9\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
check "$(api /api/history/1001 | jq '[.history[]|select(.version=="9.9.9")]|length')" "1" "断连恢复后的变更未丢失"

# ------------------------------------------------------------- 7. 写入冻结
echo "=== 7. 写入冻结：申请、执行、并发、权限、重启、到期 ==="
PSQL=(psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tA)
sqlq(){ "${PSQL[@]}" -q -c "$1"; }

# 等捕获追平到指定提交点（/api/points 默认隐藏无 artifacts 变更的事务，
# 所以用“提交点出现在列表里”判定，而不是比对 last_commit_lsn）
wait_capture(){
  local want=$1
  for _ in $(seq 1 30); do
    [ "$(api /api/points | jq -r --arg l "$want" '[.points[].lsn]|index($l)')" != "null" ] \
      && return 0
    sleep 0.5
  done
  return 1
}

# 7.1 申请冻结：源库当前值与提交点一致 → 成功
FREEZE_NO=4001
FREEZE_VER="7.0.$RANDOM"
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":$FREEZE_NO,\"service_name\":\"freeze-svc\",
 \"version\":\"$FREEZE_VER\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
freeze_point=$(api /api/points | jq -r '.points[-1].lsn')
wait_capture "$freeze_point" || true
gid=$(curl -s -X POST "$BASE/api/freezes" -H 'Content-Type: application/json' -d "{
 \"point\":\"$freeze_point\",\"artifact_nos\":[$FREEZE_NO],\"duration_seconds\":600,
 \"note\":\"selftest\"}" | jq -r '.group_id')
case "$gid" in
  *-*-*-*-*) ok "按提交点申请冻结成功: $gid" ;;
  *) bad "冻结申请失败: $gid" ;;
esac
check "$(api /api/freezes | jq -r --arg g "$gid" '.freezes[]|select(.group_id==$g)|.status')" \
      "active" "冻结组状态为 active"

# 7.2 网页改冻结编号 → 409 且整笔源事务回滚（同组其他编号也不生效）
code=$(curl -s -o /tmp/fz.json -w '%{http_code}' -X POST "$BASE/api/source/operations" \
  -H 'Content-Type: application/json' -d "{
 \"operations\":[
  {\"op\":\"update\",\"artifact_no\":$FREEZE_NO,\"service_name\":\"freeze-svc\",
   \"version\":\"changed\",\"sha256\":\"$SHA64\"},
  {\"op\":\"update\",\"artifact_no\":1001,\"service_name\":\"billing-gateway\",
   \"version\":\"rollback-check\",\"sha256\":\"$SHA64\"}
 ]}")
check "$code" "409" "网页改冻结编号返回 409"
check "$(jq -r '.detail.code' /tmp/fz.json)" "ARTIFACT_FROZEN" "错误码为 ARTIFACT_FROZEN"
check "$(sqlq "select version from artifacts where artifact_no=$FREEZE_NO")" "$FREEZE_VER" \
      "冻结编号版本未被网页改掉"
check "$(sqlq "select version from artifacts where artifact_no=1001")" "9.9.9" \
      "同组未冻结编号也随整笔源事务回滚"

# 7.3 直接 SQL 改/删冻结编号 → 整笔源事务失败；其他编号仍可写
if sqlq "update artifacts set version='sql-change' where artifact_no=$FREEZE_NO" 2>&1 \
    | grep -q 'ARTIFACT_FROZEN'; then
  ok "直接 SQL UPDATE 冻结编号被拒绝"
else
  bad "直接 SQL UPDATE 冻结编号未被拒绝"
fi
if sqlq "delete from artifacts where artifact_no=$FREEZE_NO" 2>&1 \
    | grep -q 'ARTIFACT_FROZEN'; then
  ok "直接 SQL DELETE 冻结编号被拒绝"
else
  bad "直接 SQL DELETE 冻结编号未被拒绝"
fi
check "$(sqlq "update artifacts set version='other-editable' where artifact_no=1002 returning 1")" \
      "1" "未冻结编号仍可直接 SQL 修改"
check "$(sqlq "select version from artifacts where artifact_no=$FREEZE_NO")" "$FREEZE_VER" \
      "被拒绝的 SQL 未改变冻结编号"

# 7.4 并发改版交错：两种先手顺序都不能出现“已冻结却已换版”
RACE_NO=4002
curl -s -X POST "$BASE/api/source/operations" -H 'Content-Type: application/json' -d "{
 \"operations\":[{\"op\":\"insert\",\"artifact_no\":$RACE_NO,\"service_name\":\"race-svc\",
 \"version\":\"1.0.0\",\"sha256\":\"$SHA64\"}]}" >/dev/null
sleep 2
race_point=$(api /api/points | jq -r '.points[-1].lsn')
wait_capture "$race_point" || true

# 方向 A：改版先拿守卫行锁并持有 3 秒；冻结申请在它持锁期间到达，
#         必须等改版提交后重新校验，发现源库已偏离提交点 → 整组拒绝。
( psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tA > /tmp/raceA.out 2>&1 <<SQL
BEGIN;
SELECT artifact_guard_no($RACE_NO);
SELECT pg_sleep(3);
UPDATE artifacts SET version='race-changed' WHERE artifact_no=$RACE_NO;
COMMIT;
SQL
) &
race_pid=$!
sleep 0.7
code=$(curl -s -o /tmp/raceA.json -w '%{http_code}' -X POST "$BASE/api/freezes" \
  -H 'Content-Type: application/json' -d "{
 \"point\":\"$race_point\",\"artifact_nos\":[$RACE_NO],\"duration_seconds\":600}")
wait "$race_pid"
check "$code" "409" "改版先持锁时，交错到达的冻结申请整组拒绝"
check "$(jq -r '.detail.code' /tmp/raceA.json)" "FREEZE_CONFLICT" \
      "交错冻结申请返回 FREEZE_CONFLICT"
check "$(sqlq "select count(*) from artifact_freezes where artifact_no=$RACE_NO
 and released_at is null and expires_at > clock_timestamp()")" "0" \
      "被拒绝的申请没有留下任何冻结"
check "$(sqlq "select version from artifacts where artifact_no=$RACE_NO")" "race-changed" \
      "改版按预期提交（未冻结）"

# 方向 B：冻结先提交；改版事务在其后开始 → 改版必须失败
sleep 2
race2_point=$(api /api/points | jq -r '.points[-1].lsn'); wait_capture "$race2_point" || true
race2_gid=$(curl -s -X POST "$BASE/api/freezes" -H 'Content-Type: application/json' -d "{
 \"point\":\"$race2_point\",\"artifact_nos\":[$RACE_NO],\"duration_seconds\":600}" \
 | jq -r '.group_id')
[ -n "$race2_gid" ] && [ "$race2_gid" != "null" ] \
  && ok "改版提交后按新提交点重新申请冻结成功" || bad "重新申请冻结失败: $race2_gid"
if psql -h "$PGSOCK" -p "$PGPORT" -U postgres -tA > /tmp/raceB.out 2>&1 <<SQL
BEGIN;
UPDATE artifacts SET version='race-bypass' WHERE artifact_no=$RACE_NO;
COMMIT;
SQL
then :; fi
if grep -q 'ARTIFACT_FROZEN' /tmp/raceB.out; then
  ok "冻结提交后开始的改版事务被拒绝"
else
  bad "冻结提交后改版仍成功: $(cat /tmp/raceB.out)"
fi
check "$(sqlq "select version from artifacts where artifact_no=$RACE_NO")" "race-changed" \
      "并发场景未出现“已冻结却已换版”"
# 并发场景下未冻结编号仍可修改
check "$(sqlq "update artifacts set version='race-other' where artifact_no=1003 returning 1")" \
      "1" "冻结生效期间其他编号仍可修改"
curl -s -X POST "$BASE/api/freezes/$race2_gid/release" >/dev/null

# 7.5 源库当前值与提交点不一致 → 整组申请不生效
sleep 2
after_point=$(api /api/points | jq -r '.points[-1].lsn'); wait_capture "$after_point" || true
code=$(curl -s -o /tmp/fz2.json -w '%{http_code}' -X POST "$BASE/api/freezes" \
  -H 'Content-Type: application/json' -d "{
 \"point\":\"$race_point\",\"artifact_nos\":[$RACE_NO],\"duration_seconds\":600}")
check "$code" "409" "源库与提交点不一致时申请被拒绝"
check "$(jq -r '.detail.code' /tmp/fz2.json)" "FREEZE_CONFLICT" "错误码为 FREEZE_CONFLICT"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/freezes" \
  -H 'Content-Type: application/json' -d "{
 \"point\":\"$after_point\",\"artifact_nos\":[987654],\"duration_seconds\":600}")
check "$code" "400" "该提交点不存在的编号被拒绝"

# 7.6 应用强杀重启后未到期冻结继续生效（冻结事实在源库）
kill -9 "$(app_pid)" 2>/dev/null || true; sleep 2
start_app "$SQLDIR" >/dev/null 2>&1
wait_phase STREAMING 20 || bad "重启后未回到 STREAMING"
check "$(api /api/freezes | jq -r --arg g "$gid" '.freezes[]|select(.group_id==$g)|.status')" \
      "active" "应用强杀重启后冻结仍为 active"
if sqlq "update artifacts set version='after-restart' where artifact_no=$FREEZE_NO" 2>&1 \
    | grep -q 'ARTIFACT_FROZEN'; then
  ok "重启后直接 SQL 仍被冻结拦截"
else
  bad "重启后冻结失效"
fi

# 7.7 释放冻结后可写
curl -s -X POST "$BASE/api/freezes/$gid/release" >/dev/null
check "$(sqlq "update artifacts set version='released' where artifact_no=$FREEZE_NO returning 1")" \
      "1" "释放冻结后可直接 SQL 修改"

# 7.8 到期自动放行（先制造一个新的提交点，保证与源库当前值一致）
sqlq "update artifacts set version='ttl-base' where artifact_no=$FREEZE_NO" >/dev/null
sleep 2
ttl_point=$(api /api/points | jq -r '.points[-1].lsn'); wait_capture "$ttl_point" || true
ttl_gid=$(curl -s -X POST "$BASE/api/freezes" -H 'Content-Type: application/json' \
  -d "{\"point\":\"$ttl_point\",\"artifact_nos\":[$FREEZE_NO],\"duration_seconds\":6}" \
  | jq -r '.group_id')
case "$ttl_gid" in
  *-*-*-*-*) ok "短时冻结申请成功: $ttl_gid" ;;
  *) bad "短时冻结申请失败: $ttl_gid" ;;
esac
check "$(api /api/freezes | jq -r --arg g "$ttl_gid" '.freezes[]|select(.group_id==$g)|.status')" \
      "active" "短时冻结初始为 active"
sleep 8
check "$(api /api/freezes | jq -r --arg g "$ttl_gid" '.freezes[]|select(.group_id==$g)|.status')" \
      "expired" "到期后冻结组状态为 expired"
check "$(sqlq "update artifacts set version='after-expiry' where artifact_no=$FREEZE_NO returning 1")" \
      "1" "冻结到期后自动允许写入"

# ------------------------------------------------------------- 8. 持久化校验
echo "=== 8. 两侧持久化 ==="
[ -f "$SQLDIR/history.db" ] && ok "SQLite 历史库落盘: $SQLDIR/history.db" || bad "SQLite 未落盘"
[ -d "$PGDATA/pg_wal" ] && ok "PostgreSQL 数据目录持久化: $PGDATA" || bad "PG 数据目录缺失"

echo
echo "================ 自测结果: $PASS passed / $FAIL failed ================"
[ "$FAIL" -eq 0 ]
