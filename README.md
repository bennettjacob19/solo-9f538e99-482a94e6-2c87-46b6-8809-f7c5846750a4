# 制品目录历史查询服务

发布值班工程师追查“误上线时制品目录实际指向哪个版本”的历史查询系统。
源 PostgreSQL 中的制品目录（制品编号 / 服务名 / 版本 / SHA-256）通过
**一致基线 + 逻辑复制**被完整捕获到本地 SQLite 仅增日志，可按任意已捕获
提交点回看整张目录，支持删除后编号重用、多行事务整体可见。

## 1. 架构与正确性设计

```
PostgreSQL 源库 (artifacts 固定四列)
   │  ① 建逻辑复制槽 test_decoding + pg_export_snapshot 导出一致性快照
   │  ② 另开连接 SET TRANSACTION SNAPSHOT 复用快照做整表基线
   │  ③ 从槽 LSN 持续消费逻辑变更流（walsender）
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

### SQLite 表结构（`/data/history.db`）

- `capture_state`：阶段、槽名、基线 LSN、最近提交点/消息 LSN、错误信息。
- `txns`：源事务 xid（主键）、提交 LSN、提交时间。
- `artifact_history`：仅增变更链（BASELINE/INSERT/UPDATE/DELETE），带 xid、
  提交 LSN 与数值化 LSN，支持按提交点过滤与编号重建。

> 仅含制品行变更的源事务才会落盘为提交点；与 `artifacts` 无关的空事务
> （例如启动时 `ensure_schema` 的 `CREATE ... IF NOT EXISTS` DDL）既不落盘
> 也不回送复制反馈，槽位因此始终不超前最后一个已落盘的制品提交点。

### 写入冻结（发布窗口防换版）

冻结的**强制力完全在源 PostgreSQL**，网页与直接 `psql` 一视同仁，无法绕过：

- 表 `artifact_freeze` 持久化每条冻结（编号、依据提交点、四列快照、到期时间、
  释放时间），落在源库命名卷中，**应用重启后未到期冻结继续生效**；
- 行级触发器 `trg_artifacts_freeze_guard` 在 `BEFORE UPDATE OR DELETE` 上
  对每个 `OLD.artifact_no` 检查是否存在“未释放且 `expires_at > now()`”的记录，
  命中即 `RAISE EXCEPTION`（SQLSTATE `P0001`），**整笔源事务回滚**，同事务
  其他编号的改动一并失败；未冻结编号不受影响；
- 语句级触发器 `trg_artifacts_freeze_truncate` 在存在任一在保冻结时禁止
  `TRUNCATE`（TRUNCATE 不触发行级触发器，需单独防护）；
- 到期判定完全由数据库 `now()` 完成，**到期自动放行，无需清扫任务**。

冻结申请的并发正确性（杜绝“已冻结却已换版”）：

1. 整个申请是**单个源事务**，对每个目标编号先 `SELECT ... FOR UPDATE` 锁源行，
   并发换版事务必须排队；
2. 拿到行锁后逐列比对当前四列与申请所依据的已捕获提交点快照，任一不一致
   立即报错并回滚，**整组申请不生效**；
3. 再对 `artifact_freeze` 该编号的未释放记录加 `FOR UPDATE` 键锁，配合部分
   唯一索引 `uq_freeze_unreleased_one`，并发申请互相串行化、不产生重复在保冻结；
4. 全部通过才插入冻结记录并提交。两种交错顺序均安全：
   - 换版先提交 → 冻结申请的快照校验发现版本已变，拒绝（409），不留失实冻结；
   - 冻结先提交 → 排队中的换版事务恢复后被触发器拒绝（P0001），源行保持快照原值。

## 2. 用 Docker Compose 启动（交付方式）

```bash
docker compose up --build
# 网页：http://localhost:8000/
# 源 PostgreSQL：localhost:5432（user/pass/db = postgres/postgres/postgres）
```

- `postgres` 服务：`postgres:16-bookworm`，已开启 `wal_level=logical`，
  命名卷 `pgdata` 持久化；首次启动自动执行 `init/01_schema.sql`
  幂等建表并播种 3 行示例数据。
- `app` 服务：本仓库构建的 Python 3.12 镜像，命名卷 `historydata`
  挂载到 `/data` 持久化 SQLite；`restart: unless-stopped`，源库健康检查
  通过后才启动。
- 两侧均为命名卷，`docker compose down` 不丢数据，`down -v` 才清空。

## 3. 网页功能（单页应用，无前端构建）

1. **捕获进度**：阶段徽章、复制槽、基线/最近提交 LSN、已捕获事务数、
   历史行数、最近错误，3 秒自动刷新。
2. **成组修改源目录**：动态增删 insert/update/delete 行，整组在一个源事务
   提交；任一操作失败整组回滚。命中在保冻结的编号会被源库触发器拒绝。
3. **写入冻结申请**：选择一个已捕获提交点，页面列出该点目录与每个编号当前
   冻结状态；勾选若干现存编号并选择冻结时长（1 分钟～30 天）后整组申请。
   快照四列由后端按提交点重建（前端无法伪造），当前值与提交点不一致时
   整组不生效。
4. **冻结清单**：查看全部冻结（在保 ACTIVE / 已到期 EXPIRED / 已释放
   RELEASED，状态由源库 `now()` 实时判定），可一键释放；可选显示历史记录。
5. **提交点回看**：列出 BASELINE 与每个源事务提交点（LSN、xid、时间、
   变更行数），点击任一行即查看该时刻整张目录。
6. **单编号完整历史**：含 BASELINE/INSERT/UPDATE/DELETE 全链，
   直观验证“删除后重用编号，旧历史保留”。

## 4. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/progress` | 捕获状态与进度（初始化期也可用） |
| GET | `/api/source/artifacts` | 源库当前目录 |
| POST | `/api/source/operations` | 成组增改删，单源事务，失败整组回滚；命中冻结返回 400 |
| POST | `/api/freezes` | 按提交点申请写入冻结，单源事务，任一不过整组不生效（201/409/400/404） |
| GET | `/api/freezes?include_expired=false` | 冻结清单，`active` 由源库实时判定 |
| POST | `/api/freezes/release` | 释放一条冻结（body：`{"freeze_id": 1}`） |
| GET | `/api/points` | 全部可查询提交点（未就绪时 503） |
| GET | `/api/directory?point=0/16B3748` | 该提交点（含）的整目录 |
| GET | `/api/history/{artifact_no}` | 单编号完整变更链 |

冻结申请请求示例：

```bash
curl -X POST localhost:8000/api/freezes -H 'Content-Type: application/json' -d '{
  "point_lsn": "0/16B3748",
  "artifact_nos": [1001, 1002],
  "duration_minutes": 60,
  "reason": "发布窗口 11/18 22:00"
}'
# 成功 -> 201 {"frozen":2,"freeze_ids":[7,8],"duration_seconds":3600,"point_lsn":"0/16B3748"}
# 当前值与提交点不一致 / 已有在保冻结 / 并发冲突 -> 409，整组不生效
curl -X POST localhost:8000/api/freezes/release \
  -H 'Content-Type: application/json' -d '{"freeze_id": 7}'
```

原有成组写请求示例：

```bash
curl -X POST localhost:8000/api/source/operations -H 'Content-Type: application/json' -d '{
  "operations": [
    {"op":"insert","artifact_no":1004,"service_name":"cart-svc","version":"3.1.0",
     "sha256":"abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234"},
    {"op":"delete","artifact_no":1003}
  ]}'
curl 'localhost:8000/api/directory?point=0/16B3748'
```

## 5. 自测

### 5.1 解析器单元测试

```bash
python3 -m unittest tests.test_decoder -v
```

覆盖 BEGIN/COMMIT、带空格与转义引号的值、DELETE 仅主键、其他表/DDL 忽略。

### 5.2 端到端自测（49 项断言）

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
8. **写入冻结强制力**：网页 UPDATE、直接 SQL UPDATE/DELETE 命中在保编号
   均被源库触发器以 P0001 拒绝并整笔回滚；同事务混合改动整体撤销；
   其他编号仍可写；快照不一致/重复申请整组拒绝且不留记录；释放后恢复可写；
9. **冻结到期与重启持久化**：应用重启后未到期冻结继续在保并拦截直接 SQL，
   到期后状态自动转非在保且写入自动放行（无需清扫任务）。

### 5.3 冻结并发交错自测（15 项断言）

`tests/test_freeze_concurrency.py` 用双线程 + 行锁栅栏确定性构造并发交错，
覆盖两种提交顺序与重复申请：

```bash
PGHOST=/tmp/sock PGPORT=55432 BASE=http://127.0.0.1:8000 \
  python3 tests/test_freeze_concurrency.py
```

- 场景 A：换版事务先持有行锁，冻结申请在锁上排队；换版先提交后，申请必须
  因快照不一致被拒（409），换版成功的编号上不存在任何在保冻结；
- 场景 C：冻结先提交，持锁等待的换版事务恢复后被触发器拒绝（P0001），
  源行保持快照原值——两种顺序下都不会出现“已冻结却已换版”；
- 场景 B：两个并发冻结申请串行化，恰好一条在保冻结，另一个冲突拒绝；
- 场景 D：冻结期间其他编号写入不受影响。

最近一次结果：selftest **49 passed / 0 failed**；并发自测 **15 passed / 0 failed**。

## 6. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `PG_DSN` | 本地开发 DSN | 源库 libpq 连接串，Compose 中指向 `postgres` 服务 |
| `SQLITE_PATH` | `/tmp/capture/history.db` | 历史库路径（Compose 中为 `/data/history.db`） |
| `REPLICATION_SLOT` | `artifacts_history_slot` | 逻辑复制槽名 |
| `SOURCE_TABLE` | `artifacts` | 捕获的源表（结构固定） |
| `REPL_STATUS_INTERVAL_S` | `5` | 复制协议 keepalive 间隔 |
| `REPL_RECONNECT_BACKOFF_S` | `2` | 断线重连退避秒数 |
| `CAPTURE_INIT_SLEEP_MS` | `0` | 测试钩子：拉长初始化窗口 |
| `TEST_INFLIGHT_ARTIFACT_NO` | `0`(关) | 测试钩子：跨基线事务编号 |
| `TEST_INFLIGHT_SLEEP_S` | `3` | 测试钩子：在途事务持有秒数 |

## 7. 已知边界 / 未完成项

- 逻辑解码使用 PostgreSQL 内置 `test_decoding`（无需安装插件），解析器
  依赖本项目固定的四列结构；表结构变更不在支持范围内（需求明确禁止 DDL）。
- 冻结表与触发器随 `init/01_schema.sql` 幂等创建。全新 Compose 部署会在
  初始化时自动建立；对**已存在的旧库**，重启应用时 `ensure_schema()` 会
  补建冻结表与触发器（均为 `IF NOT EXISTS` / `CREATE OR REPLACE`，不影响
  既有数据）。
- 冻结到期依赖源库时区下的 `now()`（`timestamptz`，UTC 存储），到期放行
  不需要任何后台清扫进程；已到期未释放的记录在下次对同编号申请时自动标记
  释放并留痕，也可在网页手动释放。
- 历史库为单进程 SQLite，适合值班排查类读多写少场景；如需多副本水平扩展
  需更换存储层。
- 复制槽若被人工从源库删除，应用进入 ERROR 并在 `/api/progress` 暴露，
  不会自动重建（重建会使旧 LSN 失效，需人工确认后清空历史库重新基线）。
- 沙箱环境未提供 Docker 守护进程，Compose 文件未经本机 `docker compose`
  实跑；但应用本体与全部冻结/并发逻辑已用同等配置在本机
  PostgreSQL 15 + uvicorn 下完成端到端验证（49 + 15 项断言全通过），
  Compose 镜像基于官方 `python:3.12-slim` 与 `postgres:16-bookworm`，
  初始化 SQL 仅使用 PG 15/16 通用语法。
