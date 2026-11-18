# 制品目录历史查询服务（含部署窗口写入冻结）

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统，
并在部署窗口内对选定的制品编号施加写入冻结。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
   │  ④ artifact_freeze_guard 行级触发器 + 守卫行锁执行写入冻结
   ▼
捕获工作线程（FastAPI 进程内 daemon thread）
   │  每个源事务：先整体写 SQLite（行变更 + 提交点同一事务）
   │              落盘成功后才 send_feedback 推进源端槽位
   ▼
SQLite 历史库（WAL，仅增不改）  ←→  FastAPI 网页 / JSON API
```

### 关键不变量

| 需求 | 实现方式 |
| --- | --- |
| 一致基线，普通 SQL 不中断 | 建槽与 `pg_export_snapshot()` 在同一事务完成；基线用 `SET TRANSACTION SNAPSHOT` 读一致性快照，源库无需停机、普通读写照常提交 |
| 基线前开始、基线后提交的事务不漏 | 该事务对导出快照不可见（不进基线），但其提交记录的 LSN 晚于槽起始 LSN，必然出现在逻辑流中被补捕获（自测第 2 组确定性验证） |
| 多行事务整体可见 | 一个源事务的全部行变更与提交点在**同一个 SQLite 事务**落盘，不存在半条事务 |
| 删除后重用编号保留旧历史 | `artifact_history` 仅追加，每行带 `change_type` 与提交点；按编号取“最后一条 ≤ 查询点”的记录重建目录，DELETE 与后续 INSERT 各自留痕 |
| 源断连 / 强杀重启后续接 | 复制槽持久化在源库；应用重启从槽位重放，SQLite 以源事务 xid 为主键幂等去重；断线自动指数/固定退避重连 |
| 不确认尚未落盘的更改 | 严格“先 SQLite commit，后 `send_feedback(flush_lsn=...)`”；keepalive 与无关消息一律不回送反馈。自测断言槽 `confirmed_flush_lsn` 永不超前已落盘提交点 |
| 初始化未完成不开放历史 | 状态机 `INITIALIZING → STREAMING / ERROR`；未到 STREAMING 时 `/api/points`、`/api/directory`、`/api/history/*` 一律 503 |
| 固定结构的增改删 | 网页/API 只接受 `insert/update/delete` 三类操作，pydantic 判别联合按 `op` 分流，仅四列，sha256 强制 64 位十六进制；无任何 DDL/任意 SQL 通道 |
| **冻结期间网页与直接 SQL 都改不动** | 冻结事实写进源库 `artifact_freezes`，由 `artifacts` 上的 BEFORE INSERT/UPDATE/DELETE 触发器强制；触发 `RAISE EXCEPTION` 使整笔源事务失败，同组其他编号也回滚 |
| **申请与并发改版不交错出坏状态** | 见下节“冻结的并发正确性” |
| **冻结持久、到期自动放行** | 冻结行保存在源库（与 `pgdata` 同卷），应用/数据库重启后未到期仍生效；判定使用 `clock_timestamp()`，到期即放行，无需后台任务 |
| 冻结不误伤其他编号 | 只有被冻结编号的写路径会命中 `RAISE EXCEPTION`；其他编号正常提交（自测与压测覆盖） |


### SQLite 表结构（`/data/history.db`）

- `capture_state`：阶段、槽名、基线 LSN、最近提交点/消息 LSN、错误信息。
- `txns`：源事务 xid（主键）、提交 LSN、提交时间。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带 xid、
  提交 LSN 与数值化 LSN，支持按提交点过滤与编号重建。
- 冻结不落在 SQLite：为让应用停机时直接 SQL 也被拦住，冻结事实必须与
  触发器同库，存放在源 PostgreSQL（见下节）。

### 冻结的并发正确性

`init/02_freeze.sql` 里有一张 `artifact_write_guards(artifact_no)` 决策表：

1. **每个改版事务**在触发器里先对受影响编号执行
   `artifact_guard_no(no)`（`INSERT ... ON CONFLICT DO UPDATE`），
   对该守卫行持有行锁直到提交；随后才检查冻结。
2. **每个冻结申请**也按编号升序对同一批守卫行执行 `artifact_guard_no`，
   拿到锁后再读取 `artifacts` 当前四列，与提交点逐列比对，最后写冻结行。
3. 两个方向都被 `pg_advisory_xact_lock`/行锁串行化，因此：
   - 改版先提交 → 冻结申请重新读到的当前值已偏离提交点，整组拒绝
     （`FREEZE_CONFLICT`），源库保持“已改版、未冻结”；
   - 冻结先提交 → READ COMMITTED 的改版事务在拿到守卫行锁后看到新冻结，
     触发器抛 `ARTIFACT_FROZEN`，整笔源事务回滚；REPEATABLE READ /
     SERIALIZABLE 的改版事务则得到 `could not serialize access due to
     concurrent update`（同样整笔回滚）。
   - 两者都不会留下“冻结生效中但版本已被替换”的状态。
4. 同一编号不允许叠加两份生效中的冻结（部分唯一索引
   `uq_artifact_freezes_live`）；到期未释放的旧冻结由新申请先标记释放。
5. 编号搬移（改主键）按 OLD 与 NEW 两个编号分别加锁检查；
   有生效冻结时 `TRUNCATE artifacts` 也被拒绝（TRUNCATE 与冻结申请都先写
   编号 0 的哨兵守卫行，二者同样串行化）。
6. 守卫行在初始化时即为**已存在的所有制品行**预置（`init/02_freeze.sql`
   的 backfill），因此即使是“触发器建立之前就插入”的老数据，
   REPEATABLE READ 事务也无法通过“插入一条新守卫行”绕过检查。

自测与压测结果见第 5 节。

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`（建表 +
  一次性播种 3 行示例数据）与 `init/02_freeze.sql`（冻结表、守卫行、
  触发器与函数）。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite；`restart: unless-stopped`，源库健康检查
  通过后才启动。应用启动时也会按 `init/*.sql` 幂等补齐结构（结构版本
  `schema_meta.init_version` 一致则跳过 DDL），因此旧库升级无需手工迁移。
- 两侧均为命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度**：阶段徽章、复制槽、基线/最近提交 LSN、已捕获事务数、
   历史行数、最近错误，3 秒自动刷新。
2. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一操作失败整组回滚。命中冻结编号返回 409 并说明冻结到期时间。
3. **提交点回看**：列出 BASELINE 与每个源事务提交点（LSN、xid、时间、
   变更行数），点击任一行即查看该时刻整张目录。只写了冻结表的事务不会
   出现在列表里（不干扰值班视图）。
4. **单编号完整历史**：含 BASELINE/INSERT/UPDATE/DELETE 全链，
   直观验证“删除后重用编号，旧历史保留”。
5. **写入冻结**：选择提交点 → 载入该时刻目录 → 勾选若干现存制品 →
   填冻结时长（秒）→ 申请；页面同时列出所有冻结组（active / expired /
   released）及每个成员的期望值，可一键释放。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态与进度（初始化期也可用） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚；冻结编号 409 |
| GET | `/api/points` | 全部可查询提交点（未就绪时 503） |
| GET | `/api/directory?point=0/16B3748` | 该提交点（含）的整目录 |
| GET | `/api/history/{artifact_no}` | 单编号完整变更链 |
| GET | `/api/freezes` | 所有冻结组及成员（active/expired/released） |
| POST | `/api/freezes` | 按提交点申请冻结（整组，原子） |
| POST | `/api/freezes/{group_id}/release` | 释放整组冻结 |

申请冻结只接受“提交点 + 编号 + 时长”，期望的服务名/版本/SHA-256 一律由
服务端从已捕获提交点目录取值（不信任客户端），再与源库当前值逐列比对。
源库与提交点不一致、编号在该点不存在、已有生效冻结、与并发改版冲突时
分别返回 400/404/409，且整组不生效。

请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?point=0/16B3748'
curl -X POST localhost:8000/api/freezes -H 'Content-Type: application/json' -d '{
  "point":"0/16B3748","artifact_nos":[1004],"duration_seconds":1800,"note":"变更窗口"}'
curl -X POST localhost:8000/api/freezes/<group_id>/release
```

## 5. 自测

### 5.1 解析器单元测试

```bash
python3 -m unittest tests.test_decoder -v
```

覆盖 BEGIN/COMMIT、带空格与转义引号的值、DELETE 仅主键、其他表/DDL 忽略。

### 5.2 端到端自测（58 项断言）

`tests/selftest.sh` 在本机自动拉起一个便携 PostgreSQL（不依赖 Docker）、
启停应用并完成全链路验证：

```bash
# 可选：指定本机 PostgreSQL 二进制位置
export PGBIN=/path/to/postgresql/bin PGCLIENT=/path/to/client/bin
bash tests/selftest.sh
```

覆盖场景：

1. 初始化窗口历史接口 503，完成后 STREAMING 与基线目录正确；
2. **跨基线事务**：通过应用内置测试钩子
   （`TEST_INFLIGHT_ARTIFACT_NO` 环境变量，默认关闭）确定性构造
   “基线快照点之后打开、基线落盘之后提交”的事务，断言不进基线、
   不被遗漏、在其提交点可见；
3. 多行事务只产生一个提交点、同事务插入/删除同时生效；
4. 删除后重用编号，旧历史链完整（BASELINE,DELETE,INSERT）；
5. 失败整组回滚、非法 sha256/非法 LSN 参数校验；
6. `kill -9` 强杀应用后重启：事务数不重复、最后更新仍在、
   槽 `confirmed_flush_lsn` 不超前已落盘 LSN、续接后新变更继续捕获；
7. 源 PostgreSQL 重启后应用自动重连恢复 STREAMING，恢复后变更不丢；
8. **写入冻结**：按提交点申请/释放、网页改冻结编号 409 且同组其他编号
   一并回滚、直接 SQL 改/删被拒、未冻结编号仍可写、源库与提交点不一致时
   整组拒绝、**两种先手顺序的并发改版都不出现“已冻结却已换版”**、
   `kill -9` 重启后冻结仍生效、到期后自动放行；
9. SQLite 与 PostgreSQL 数据目录均落盘持久化。

最近一次结果：**58 passed / 0 failed**（本机 PostgreSQL 15.18）。

### 5.3 并发压测脚本

```bash
# 冻结申请 × 直接 SQL 改版并发交错；零“已冻结却已换版”即通过
python3 tests/stress_freeze.py --base http://127.0.0.1:8000 \
  --dsn "host=127.0.0.1 port=5432 dbname=postgres user=postgres" --rounds 50
# 两个方向相反的批量写入并发；零死锁即通过
python3 tests/stress_batch.py --base http://127.0.0.1:8000 --rounds 20
```

最近一次结果：`stress_freeze --rounds 50` → violations=0（18 次冻结成功、
32 次因并发改版整组拒绝，无一次“已冻结却已换版”）；
`stress_batch --rounds 20` → deadlock=0（40 笔反向顺序批量写全部成功）。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数（也用于源库未就绪时的初始化重试） |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽若被人工从源库删除，应用进入 ERROR 并在 `/api/progress` 暴露，
  不会自动重建（重建会使旧 LSN 失效，需人工确认后清空历史库重新基线）。
- 冻结以 `artifacts` 表的行级触发器执行：绕过触发器的路径（`ALTER TABLE
  ... DISABLE TRIGGER`、直接改数据文件）不在防护范围；`TRUNCATE` 已单独
  拦截。有超级用户权限的人当然可以停用触发器，这是数据库权限边界问题。
- 冻结申请按编号升序取锁，`artifacts` 的**批量**写入也统一按升序取锁；
  但外部直接 SQL 若在**同一个事务**里以不同顺序更新多个编号，仍可能被
  源库判定死锁（PostgreSQL 会回滚其中一笔事务），这是数据库的正常语义。
- 冻结组当前不支持“延长/修改成员”，只能释放后重新申请（重新申请会再次
  校验源库与提交点一致）。
- 沙箱环境没有 Docker/容器运行时（无 `docker` 命令、用户命名空间被禁用），
  **`docker compose up` 未经实机执行**。已做的替代验证：`docker-compose.yml`
  通过 compose-spec JSON Schema 校验；按 Dockerfile 的 COPY 集合在干净目录
  中用镜像相同的依赖与 `uvicorn app.main:app --host 0.0.0.0 --port 8000`
  启动，连真实的 TCP 源库走完“申请冻结 → 网页 409 → 直接 SQL 被拒 →
  释放 → 可写”全链路。镜像基础为官方 `python:3.12-slim`、
  `postgres:16-bookworm`。
