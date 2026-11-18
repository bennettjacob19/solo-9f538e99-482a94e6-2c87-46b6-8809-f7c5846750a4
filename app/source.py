"""源 PostgreSQL 访问：固定结构的增/改/删、快照基线、复制槽推进、写入冻结。

所有写接口都只操作 artifacts 表的四列固定结构，不接受任意 SQL。

冻结的强制力完全在源库触发器内（见 init/01_schema.sql）：
- 冻结期间对受保护编号的 UPDATE/DELETE（网页或直接 SQL）都会让整条源事务
  RAISE EXCEPTION 回滚，同事务其他编号的改动一并失败；
- 冻结记录持久化在源库，应用重启后继续生效，到期由数据库 now() 自动放行。
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import psycopg2
import psycopg2.extras

from .config import Settings

FIXED_COLUMNS = ("artifact_no", "service_name", "version", "sha256")

# PostgreSQL 触发器主动抛出的 SQLSTATE（plpgsql RAISE EXCEPTION 默认值）
PG_RAISE_EXCLUSION = "P0001"
# 并发下唯一部分索引冲突
PG_UNIQUE_VIOLATION = "23505"


class SourceError(Exception):
    pass


class FreezeError(SourceError):
    """冻结申请/释放语义错误（快照不一致、重复申请等），整组申请不生效。"""


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
        """幂等建表 + 空表播种（固定结构，无 DDL 变更通道）。"""
        import os

        sql_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "init", "01_schema.sql"
        )
        with self.connect() as conn, conn.cursor() as cur:
            with open(sql_path, "r", encoding="utf-8") as fh:
                cur.execute(fh.read())

    # ------------------------------------------------------------- 业务写入
    def apply_operations(self, operations: list[dict[str, Any]]) -> dict[str, Any]:
        """成组修改源目录：全部操作在同一个源事务里，要么全成功要么全回滚。"""
        inserted = updated = deleted = 0
        with self.connect() as conn, conn.cursor() as cur:
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
        return {"inserted": inserted, "updated": updated, "deleted": deleted}

    @staticmethod
    def translate_db_error(exc: Exception) -> Exception:
        """把源库异常翻译成面向网页的错误；冻结拦截保持 400 语义。"""
        if isinstance(exc, psycopg2.Error) and exc.pgcode == PG_RAISE_EXCLUSION:
            return SourceError(str(exc).strip())
        return exc

    def list_artifacts(self) -> list[dict[str, Any]]:
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT artifact_no, service_name, version, sha256"
                " FROM artifacts ORDER BY artifact_no"
            )
            return [dict(zip(FIXED_COLUMNS, row)) for row in cur.fetchall()]

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

    # ============================================================== 写入冻结
    def apply_freeze(
        self,
        items: list[dict[str, Any]],
        point_lsn: str,
        duration_seconds: int,
        reason: str,
    ) -> dict[str, Any]:
        """成组申请写入冻结；全部检查与写入在同一个源事务内，任一不过整组回滚。

        正确性要点（防“已冻结却已换版”）：
        1. 对每个目标编号先 ``SELECT ... FOR UPDATE`` 锁定 artifacts 行，
           并发的换版事务必须等本事务结束才能写该行；
        2. 行锁拿到后再比对当前四列与申请快照（来自已捕获提交点），
           任何一列不一致即 RAISE，整组不生效；
        3. 再对 artifact_freeze 中该编号的未释放记录加键锁（无行时锁住
           索引间隙/谓词入口），并发冻结申请互相串行化，由唯一索引兜底；
        4. 全部通过才插入冻结记录并提交。提交后触发器立即对所有会话生效，
           而被阻塞的并发换版事务恢复时只能看到冻结事实并被触发器拒绝。
        """
        conn = psycopg2.connect(self._settings.pg_dsn)
        conn.autocommit = False
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN")
                created_ids: list[int] = []
                seen: set[int] = set()
                for item in items:
                    no = item["artifact_no"]
                    if no in seen:
                        raise FreezeError(
                            f"申请中 artifact_no={no} 重复出现，请去重后再提交"
                        )
                    seen.add(no)

                    # 1) 锁定源行并读取当前四列
                    cur.execute(
                        "SELECT artifact_no, service_name, version, sha256"
                        " FROM artifacts WHERE artifact_no=%s FOR UPDATE",
                        (no,),
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise FreezeError(
                            f"artifact_no={no} 在源库当前不存在，"
                            "无法按该提交点申请冻结（整组申请未生效）"
                        )
                    cur_no, cur_svc, cur_ver, cur_sha = row

                    # 2) 当前四列必须与提交点快照逐列一致
                    mismatches: list[str] = []
                    if cur_svc != item["service_name"]:
                        mismatches.append(
                            f"service_name: 提交点={item['service_name']!r}"
                            f" 当前={cur_svc!r}"
                        )
                    if cur_ver != item["version"]:
                        mismatches.append(
                            f"version: 提交点={item['version']!r} 当前={cur_ver!r}"
                        )
                    if cur_sha != item["sha256"]:
                        mismatches.append(
                            f"sha256: 提交点={item['sha256']!r} 当前={cur_sha!r}"
                        )
                    if mismatches:
                        raise FreezeError(
                            f"artifact_no={no} 源库当前值与提交点 {point_lsn}"
                            " 不一致，整组冻结申请不生效：" + "；".join(mismatches)
                        )

                    # 3) 串行化并发冻结申请：锁已有未释放记录
                    cur.execute(
                        "SELECT id FROM artifact_freeze"
                        " WHERE artifact_no=%s AND released_at IS NULL"
                        " ORDER BY id FOR UPDATE",
                        (no,),
                    )
                    existing = cur.fetchall()
                    active = [
                        eid
                        for (eid,) in existing
                        if self._freeze_is_active(cur, eid)
                    ]
                    if active:
                        raise FreezeError(
                            f"artifact_no={no} 已存在在保冻结 id={active[0]}，"
                            "请先释放或等待到期后再申请（整组申请未生效）"
                        )

                    # 历史遗留：未释放但已到期的记录先标记释放，留痕且腾出唯一索引
                    for (eid,) in existing:
                        cur.execute(
                            "UPDATE artifact_freeze SET released_at=now()"
                            " WHERE id=%s AND released_at IS NULL",
                            (eid,),
                        )

                    # 4) 插入新冻结记录
                    expires = datetime.now(timezone.utc) + timedelta(
                        seconds=duration_seconds
                    )
                    cur.execute(
                        "INSERT INTO artifact_freeze("
                        " artifact_no, point_lsn, service_name, version, sha256,"
                        " reason, expires_at)"
                        " VALUES (%s,%s,%s,%s,%s,%s,%s)"
                        " RETURNING id",
                        (
                            no,
                            point_lsn,
                            item["service_name"],
                            item["version"],
                            item["sha256"],
                            reason,
                            expires,
                        ),
                    )
                    created_ids.append(cur.fetchone()[0])
            conn.commit()
        except FreezeError:
            conn.rollback()
            raise
        except psycopg2.Error as exc:
            conn.rollback()
            if exc.pgcode == PG_UNIQUE_VIOLATION:
                raise FreezeError(
                    "并发冻结申请冲突（同一编号已有未释放冻结），整组申请未生效"
                ) from exc
            raise
        finally:
            conn.close()
        return {
            "frozen": len(created_ids),
            "freeze_ids": created_ids,
            "duration_seconds": duration_seconds,
            "point_lsn": point_lsn,
        }

    @staticmethod
    def _freeze_is_active(cur: Any, freeze_id: int) -> bool:
        cur.execute(
            "SELECT (released_at IS NULL AND expires_at > now())"
            " FROM artifact_freeze WHERE id=%s",
            (freeze_id,),
        )
        row = cur.fetchone()
        return bool(row and row[0])

    def list_freezes(self, include_expired: bool) -> list[dict[str, Any]]:
        """列出冻结；默认只返回未释放记录（含在保/已到期未释放），状态实时计算。"""
        sql = (
            "SELECT id, artifact_no, point_lsn, service_name, version, sha256,"
            " reason, created_at, expires_at, released_at,"
            " (released_at IS NULL AND expires_at > now()) AS active"
            " FROM artifact_freeze"
        )
        if not include_expired:
            sql += " WHERE released_at IS NULL"
        sql += " ORDER BY id DESC"
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]

    def release_freeze(self, freeze_id: int) -> dict[str, Any]:
        """释放一条冻结；已释放/不存在返回 FreezeError，到期记录也允许显式释放。"""
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, released_at, expires_at FROM artifact_freeze"
                " WHERE id=%s FOR UPDATE",
                (freeze_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise FreezeError(f"冻结 id={freeze_id} 不存在")
            if row[1] is not None:
                raise FreezeError(
                    f"冻结 id={freeze_id} 已于 {row[1]} 释放，不能重复释放"
                )
            cur.execute(
                "UPDATE artifact_freeze SET released_at=now()"
                " WHERE id=%s AND released_at IS NULL",
                (freeze_id,),
            )
            if cur.rowcount != 1:
                raise FreezeError(f"冻结 id={freeze_id} 释放失败")
            return {"released": freeze_id}
