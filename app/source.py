"""源 PostgreSQL 访问：固定结构的增/改/删、快照基线、复制槽推进、写入冻结。

所有写接口都只操作 artifacts 表的四列固定结构，不接受任意 SQL。
冻结事实存放在源库（artifact_freeze_groups / artifact_freezes），由
artifact_freeze_guard 行级触发器强制执行，因此应用停机、重启或有人直接
用 SQL/psql 改表时同样生效。
"""
from __future__ import annotations

import re
import uuid
from contextlib import contextmanager
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

from .config import Settings

FIXED_COLUMNS = ("artifact_no", "service_name", "version", "sha256")

# init/*.sql 的整体结构版本；与源库 schema_meta.init_version 一致时跳过 DDL，
# 避免每次重启都对 artifacts 取排他锁、阻断部署窗口中的正常写入。
SCHEMA_VERSION = "4"

_FROZEN_RE = re.compile(r"ARTIFACT_FROZEN artifact_no=(\d+) expires_at=(\S+)")


class SourceError(Exception):
    pass


class ArtifactFrozen(SourceError):
    """受保护编号被更新/删除，整笔源事务已回滚。"""

    def __init__(self, artifact_no: int, expires_at: str | None) -> None:
        self.artifact_no = artifact_no
        self.expires_at = expires_at
        until = f"，冻结至 {expires_at}" if expires_at else ""
        super().__init__(
            f"制品编号 {artifact_no} 处于冻结期{until}，禁止更新或删除；"
            "整笔源事务已回滚"
        )


class FreezeConflict(SourceError):
    """申请与已有冻结/并发改版冲突，整组申请未生效。"""


class _FrozenTruncate(SourceError):
    pass


class SourceBusy(SourceError):
    """并发事务互锁，PostgreSQL 中止了本笔事务；可安全重试。"""


def translate_pg_error(exc: Exception) -> Exception:
    """把触发器抛出的 ARTIFACT_FROZEN 翻译成带结构化字段的异常。"""
    message = str(exc)
    if "ARTIFACT_FROZEN" not in message:
        if isinstance(
            exc, (psycopg2.errors.DeadlockDetected,
                  psycopg2.errors.SerializationFailure)
        ):
            return SourceBusy(f"与并发事务互锁，本笔事务已回滚，请重试: {exc}")
        return exc
    match = _FROZEN_RE.search(message)
    if match:
        return ArtifactFrozen(int(match.group(1)), match.group(2))
    return _FrozenTruncate("存在生效中的冻结，禁止 TRUNCATE artifacts")


class SourceDB:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @contextmanager
    def connect(self) -> Iterator[psycopg2.extensions.connection]:
        conn = psycopg2.connect(self._settings.pg_dsn)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_schema(self) -> None:
        """按文件名顺序幂等执行 init/*.sql（建表、播种、冻结触发器）。

        源库已记录相同结构版本时直接跳过，不做任何 DDL，重启很快且不会
        与正常写入争抢 artifacts 的排他锁。
        """
        import glob
        import os

        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT to_regclass('public.schema_meta') IS NOT NULL"
            )
            has_meta = cur.fetchone()[0]
            if has_meta:
                cur.execute(
                    "SELECT value FROM schema_meta WHERE key='init_version'"
                )
                row = cur.fetchone()
                if row and row[0] == SCHEMA_VERSION:
                    return

        init_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "init"
        )
        paths = sorted(glob.glob(os.path.join(init_dir, "*.sql")))
        with self.connect() as conn, conn.cursor() as cur:
            for path in paths:
                with open(path, "r", encoding="utf-8") as fh:
                    cur.execute(fh.read())

    # ------------------------------------------------------------- 业务写入
    def apply_operations(self, operations: list[dict[str, Any]]) -> dict[str, Any]:
        """成组修改源目录：全部操作在同一个源事务里，要么全成功要么全回滚。

        冻结由源库触发器拦截；受保护编号的任一操作会抛出异常，整笔事务
        （包括同组内其他编号的修改）一并回滚。
        """
        inserted = updated = deleted = 0
        try:
            with self.connect() as conn, conn.cursor() as cur:
                # 先按编号升序取一次守卫行锁，使批量内多行的锁序与单行写入、
                # 冻结申请一致（否则两个不同顺序的批量事务可能相互死锁）。
                numbers = sorted(
                    {int(op["artifact_no"]) for op in operations}
                )
                for no in numbers:
                    cur.execute("SELECT artifact_guard_no(%s)", (no,))

                for op in operations:
                    kind = op["op"]
                    if kind == "insert":
                        cur.execute(
                            "INSERT INTO artifacts(artifact_no, service_name, version,"
                            " sha256) VALUES (%s, %s, %s, %s)",
                            (
                                op["artifact_no"],
                                op["service_name"],
                                op["version"],
                                op["sha256"],
                            ),
                        )
                        inserted += 1
                    elif kind == "update":
                        cur.execute(
                            "UPDATE artifacts SET service_name=%s, version=%s,"
                            " sha256=%s WHERE artifact_no=%s",
                            (
                                op["service_name"],
                                op["version"],
                                op["sha256"],
                                op["artifact_no"],
                            ),
                        )
                        if cur.rowcount == 0:
                            raise SourceError(
                                f"UPDATE 失败：artifact_no={op['artifact_no']} 不存在"
                            )
                        updated += 1
                    elif kind == "delete":
                        cur.execute(
                            "DELETE FROM artifacts WHERE artifact_no=%s",
                            (op["artifact_no"],),
                        )
                        if cur.rowcount == 0:
                            raise SourceError(
                                f"DELETE 失败：artifact_no={op['artifact_no']} 不存在"
                            )
                        deleted += 1
                    else:  # pydantic 已拦截，这里只是防御
                        raise SourceError(f"不支持的操作类型: {kind}")
        except psycopg2.Error as exc:
            raise translate_pg_error(exc) from exc
        return {"inserted": inserted, "updated": updated, "deleted": deleted}

    def list_artifacts(self) -> list[dict[str, Any]]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT artifact_no, service_name, version, sha256"
                " FROM artifacts ORDER BY artifact_no"
            )
            return [dict(zip(FIXED_COLUMNS, row)) for row in cur.fetchall()]

    # ------------------------------------------------------------- 写入冻结
    def create_freeze(
        self,
        *,
        point_lsn: str,
        point_lsn_num: int,
        entries: list[dict[str, Any]],
        ttl_seconds: int,
        note: str,
    ) -> dict[str, Any]:
        """按提交点申请冻结：整组在一个源事务内，任一条不满足则整组不生效。

        entries 的 service_name/version/sha256 是提交点当时的历史值；
        申请时源库当前值必须与之完全一致，否则拒绝整组申请。
        串行化依赖 artifact_guard_no() 的守卫行锁（见 init/02_freeze.sql）。
        """
        group_id = str(uuid.uuid4())
        numbers = sorted({int(e["artifact_no"]) for e in entries})
        try:
            with self.connect() as conn, conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                # 1) 先按编号升序拿守卫行锁：与所有改版事务严格串行。
                #    全局锁序固定为“守卫行（升序）→ 冻结行”，所以并发申请之间、
                #    申请与改版之间都不会因锁序相反而死锁。
                #    哨兵 0 号行让 TRUNCATE 的语句级守卫也参与同一串行化。
                for no in [0, *numbers]:
                    cur.execute("SELECT artifact_guard_no(%s)", (no,))

                # 2) 释放这些编号上已到期但尚未标记的旧冻结，腾出“未释放”唯一槽位。
                cur.execute(
                    "UPDATE artifact_freezes SET released_at = now()"
                    " WHERE artifact_no = ANY(%s) AND released_at IS NULL"
                    " AND expires_at <= clock_timestamp()",
                    (numbers,),
                )

                # 3) 同一编号不允许叠加两份生效中的冻结。
                cur.execute(
                    "SELECT artifact_no, expires_at FROM artifact_freezes"
                    " WHERE artifact_no = ANY(%s) AND released_at IS NULL"
                    " AND expires_at > clock_timestamp()"
                    " ORDER BY artifact_no",
                    (numbers,),
                )
                conflicts = cur.fetchall()
                if conflicts:
                    detail = "；".join(
                        f"{r['artifact_no']} 已冻结至 {r['expires_at'].isoformat()}"
                        for r in conflicts
                    )
                    raise FreezeConflict(f"以下编号已有生效中的冻结：{detail}")

                # 4) 持锁后读取源库当前值，与提交点逐列比对。
                cur.execute(
                    "SELECT artifact_no, service_name, version, sha256"
                    " FROM artifacts WHERE artifact_no = ANY(%s)",
                    (numbers,),
                )
                current = {r["artifact_no"]: r for r in cur.fetchall()}
                mismatches: list[str] = []
                for entry in entries:
                    no = int(entry["artifact_no"])
                    row = current.get(no)
                    if row is None:
                        mismatches.append(f"{no} 在源库已不存在")
                        continue
                    if (
                        row["service_name"] != entry["service_name"]
                        or row["version"] != entry["version"]
                        or row["sha256"] != entry["sha256"]
                    ):
                        mismatches.append(f"{no} 的服务名/版本/SHA-256 与提交点不一致")
                if mismatches:
                    raise FreezeConflict(
                        "冻结申请未生效（整组拒绝）：" + "；".join(mismatches)
                    )

                # 5) 写入冻结组与成员，与守卫行锁同事务提交。
                cur.execute(
                    "INSERT INTO artifact_freeze_groups"
                    " (group_id, point_lsn, point_lsn_num, note, expires_at)"
                    " VALUES (%s, %s, %s, %s, now() + make_interval(secs => %s))"
                    " RETURNING group_id, created_at, expires_at",
                    (group_id, point_lsn, point_lsn_num, note, ttl_seconds),
                )
                group = cur.fetchone()
                cur.executemany(
                    "INSERT INTO artifact_freezes"
                    " (group_id, artifact_no, service_name, version, sha256, expires_at)"
                    " VALUES (%s, %s, %s, %s, %s, %s)",
                    [
                        (
                            group_id,
                            int(e["artifact_no"]),
                            e["service_name"],
                            e["version"],
                            e["sha256"],
                            group["expires_at"],
                        )
                        for e in entries
                    ],
                )
        except psycopg2.Error as exc:
            raise translate_pg_error(exc) from exc

        return {
            "group_id": group_id,
            "point_lsn": point_lsn,
            "point_lsn_num": point_lsn_num,
            "note": note,
            "created_at": group["created_at"].isoformat(),
            "expires_at": group["expires_at"].isoformat(),
            "artifact_nos": numbers,
        }

    def list_freezes(self) -> list[dict[str, Any]]:
        """列出所有冻结组及成员（含已释放/已到期，按创建时间倒序）。"""
        sql = """
            SELECT g.group_id::text AS group_id, g.point_lsn, g.point_lsn_num,
                   g.note, g.created_at, g.expires_at,
                   f.artifact_no, f.service_name, f.version, f.sha256,
                   f.released_at,
                   (f.released_at IS NOT NULL) AS released,
                   (f.released_at IS NULL
                    AND f.expires_at <= clock_timestamp()) AS expired,
                   (f.released_at IS NULL
                    AND f.expires_at > clock_timestamp()) AS active
            FROM artifact_freeze_groups g
            JOIN artifact_freezes f USING (group_id)
            ORDER BY g.created_at DESC, g.group_id, f.artifact_no
        """
        with self.connect() as conn, conn.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor
        ) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        groups: dict[str, dict[str, Any]] = {}
        for row in rows:
            gid = row["group_id"]
            group = groups.get(gid)
            if group is None:
                group = {
                    "group_id": gid,
                    "point_lsn": row["point_lsn"],
                    "point_lsn_num": row["point_lsn_num"],
                    "note": row["note"],
                    "created_at": row["created_at"].isoformat(),
                    "expires_at": row["expires_at"].isoformat(),
                    "status": "released",
                    "artifacts": [],
                }
                groups[gid] = group
            member = {
                "artifact_no": row["artifact_no"],
                "service_name": row["service_name"],
                "version": row["version"],
                "sha256": row["sha256"],
                "status": (
                    "released"
                    if row["released"]
                    else "expired"
                    if row["expired"]
                    else "active"
                ),
                "released_at": (
                    row["released_at"].isoformat() if row["released_at"] else None
                ),
            }
            group["artifacts"].append(member)
            if member["status"] == "active":
                group["status"] = "active"
            elif member["status"] == "expired" and group["status"] == "released":
                group["status"] = "expired"
        return list(groups.values())

    def release_freeze(self, group_id: str) -> dict[str, Any] | None:
        """释放整组冻结；返回组摘要，组不存在返回 None。"""
        try:
            uuid.UUID(group_id)
        except (ValueError, AttributeError):
            return None
        try:
            with self.connect() as conn, conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            ) as cur:
                cur.execute(
                    "SELECT group_id FROM artifact_freeze_groups"
                    " WHERE group_id = %s",
                    (group_id,),
                )
                if cur.fetchone() is None:
                    return None
                cur.execute(
                    "UPDATE artifact_freezes SET released_at = now()"
                    " WHERE group_id = %s AND released_at IS NULL",
                    (group_id,),
                )
                released = cur.rowcount
        except psycopg2.Error as exc:
            raise translate_pg_error(exc) from exc
        return {"group_id": group_id, "released": released}

    # ------------------------------------------------------------- 复制槽
    def slot_exists(self, slot_name: str) -> bool:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s",
                (slot_name,),
            )
            return cur.fetchone() is not None

    def create_slot_with_snapshot(
        self, slot_name: str
    ) -> tuple[str, psycopg2.extensions.connection, str]:
        """创建逻辑复制槽并返回 (consistent_point LSN, 持有快照的连接, snapshot_id)。

        在一条普通连接的事务里先建槽（拿到槽的一致性 LSN），再
        pg_export_snapshot() 导出同一时刻快照；事务在基线读取完成前保持
        打开，另一条普通连接 SET TRANSACTION SNAPSHOT 复用该快照做基线，
        保证基线与复制流起点严格一致。兼容 PG 14/15/16。
        """
        conn = psycopg2.connect(self._settings.pg_dsn)
        conn.autocommit = False
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute(
                "SELECT lsn::text FROM pg_create_logical_replication_slot(%s, %s)",
                (slot_name, "test_decoding"),
            )
            consistent_lsn = cur.fetchone()[0]
            cur.execute("SELECT pg_export_snapshot()")
            snapshot_id = cur.fetchone()[0]
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise
        return consistent_lsn, conn, snapshot_id

    def baseline_rows_using_snapshot(self, snapshot_id: str) -> list[tuple]:
        """在普通连接上复用导出快照，读取基线时刻整目录。"""
        conn = psycopg2.connect(self._settings.pg_dsn)
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute("SET TRANSACTION SNAPSHOT %s", (snapshot_id,))
                cur.execute(
                    "SELECT artifact_no, service_name, version, sha256"
                    " FROM artifacts ORDER BY artifact_no"
                )
                rows = cur.fetchall()
            conn.rollback()  # 只读事务，回滚即可释放快照引用
            return rows
        finally:
            conn.close()
