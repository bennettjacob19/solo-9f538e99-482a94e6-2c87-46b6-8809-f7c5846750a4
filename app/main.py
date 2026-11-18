"""FastAPI 历史查询网页：源目录成组修改、提交点查询、捕获进度、写入冻结。

初始化未完成（phase != STREAMING）时，所有历史查询接口返回 503，
不对外开放尚未完整的历史；源目录修改与冻结申请走源库自身事务，不受此限制。

写入冻结：
- 从已捕获提交点选择若干“该时刻存在”的制品与冻结时长；
- 服务端用提交点目录中的四列（编号/服务名/版本/SHA-256）作为期望值，
  申请时源库当前值必须逐列一致，否则整组申请不生效；
- 冻结由源库触发器强制执行（见 init/02_freeze.sql），网页与直接 SQL 的
  UPDATE/DELETE 都会让整笔源事务失败，其他编号仍可修改；
- 冻结持久化在源库，应用重启后仍生效，到期后自动放行。
"""
from __future__ import annotations

import logging
import os
import re
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Union

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .capture import CaptureWorker
from .config import settings
from .source import (
    ArtifactFrozen,
    FreezeConflict,
    SourceBusy,
    SourceDB,
    SourceError,
)
from .storage import HistoryStore, lsn_to_num

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("web")

SHA256_RE = r"^[0-9a-fA-F]{64}$"


def _check_sha(v: str) -> str:
    if not re.match(SHA256_RE, v):
        raise ValueError("sha256 必须是 64 位十六进制")
    return v


class _Columns(BaseModel):
    """增/改共用的四列固定字段，严格拒绝额外列。"""

    artifact_no: int = Field(gt=0)
    service_name: str = Field(min_length=1, max_length=200)
    version: str = Field(min_length=1, max_length=100)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _sha(cls, v: str) -> str:
        return _check_sha(v)


class InsertOp(_Columns):
    op: Literal["insert"]


class UpdateOp(_Columns):
    op: Literal["update"]


class DeleteOp(BaseModel):
    op: Literal["delete"]
    artifact_no: int = Field(gt=0)


AnyOp = Annotated[
    Union[InsertOp, UpdateOp, DeleteOp], Field(discriminator="op")
]


class BatchRequest(BaseModel):
    operations: list[AnyOp] = Field(min_length=1)


class FreezeRequest(BaseModel):
    """从已捕获提交点选择现存制品申请冻结。"""

    point: str = Field(min_length=1, max_length=32)
    artifact_nos: list[int] = Field(min_length=1, max_length=500)
    duration_seconds: int = Field(ge=5, le=2_592_000)
    note: str = Field(default="", max_length=500)

    @field_validator("artifact_nos")
    @classmethod
    def _no_duplicates(cls, v: list[int]) -> list[int]:
        if any(n <= 0 for n in v):
            raise ValueError("artifact_no 必须是正整数")
        if len(set(v)) != len(v):
            raise ValueError("artifact_nos 不能重复")
        return v


store = HistoryStore(settings.sqlite_path)
source = SourceDB(settings)
worker = CaptureWorker(store, source, settings)


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker.start()
    try:
        yield
    finally:
        worker.stop()
        store.close()


app = FastAPI(title="制品目录历史查询与写入冻结", lifespan=lifespan)


def _require_ready() -> None:
    if not store.is_ready():
        progress = store.get_progress()
        raise HTTPException(
            status_code=503,
            detail={
                "message": "捕获初始化尚未完成，历史暂不开放",
                "phase": progress.get("phase"),
                "last_error": progress.get("last_error"),
            },
        )


# --------------------------------------------------------------------- 页面
@app.get("/", include_in_schema=False)
def index() -> FileResponse:
    return FileResponse(
        os.path.join(os.path.dirname(__file__), "static", "index.html")
    )


# --------------------------------------------------------------------- 进度
@app.get("/api/progress")
def api_progress() -> dict:
    return store.get_progress()


# --------------------------------------------------------------- 源目录读写
@app.get("/api/source/artifacts")
def api_source_list() -> dict:
    return {"artifacts": source.list_artifacts()}


@app.post("/api/source/operations")
def api_batch_write(req: BatchRequest) -> dict:
    """成组修改源目录；整组在一个源事务内提交，失败整体回滚。

    受冻结保护的编号会让整笔源事务失败（包括同组内其他编号的修改）。
    """
    payload = [op.model_dump() for op in req.operations]
    try:
        result = source.apply_operations(payload)
    except ArtifactFrozen as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "code": "ARTIFACT_FROZEN",
                "artifact_no": exc.artifact_no,
                "expires_at": exc.expires_at,
            },
        )
    except SourceBusy as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "message": str(exc),
                "code": "SOURCE_BUSY",
            },
        )
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    result["operations"] = len(payload)
    return result


# ---------------------------------------------------------------- 写入冻结
def _parse_point(point: str) -> tuple[int, dict]:
    try:
        lsn_num = lsn_to_num(point.strip())
    except (ValueError, IndexError):
        raise HTTPException(
            status_code=400, detail="point 必须是 LSN，形如 0/16B3748"
        )
    found = store.get_point(lsn_num)
    if found is None:
        raise HTTPException(
            status_code=404,
            detail="该 LSN 不是已捕获的提交点，请先在提交点列表中选择",
        )
    return lsn_num, found


@app.get("/api/freezes")
def api_freezes() -> dict:
    """所有冻结组（含生效中、已到期、已释放）。"""
    try:
        groups = source.list_freezes()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    return {"freezes": groups}


@app.post("/api/freezes")
def api_create_freeze(req: FreezeRequest) -> JSONResponse:
    """按提交点申请冻结；源库当前四列必须与该提交点一致，否则整组不生效。"""
    _require_ready()
    lsn_num, point = _parse_point(req.point)

    # 期望值一律取自已捕获的提交点目录（不信任客户端提交的列值）。
    directory = {row["artifact_no"]: row for row in store.get_directory_at(lsn_num)}
    missing = [n for n in req.artifact_nos if n not in directory]
    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "以下编号在该提交点不存在，无法冻结："
                + ", ".join(str(n) for n in missing),
                "code": "NOT_AT_POINT",
                "artifact_nos": missing,
            },
        )
    entries = [
        {
            "artifact_no": n,
            "service_name": directory[n]["service_name"],
            "version": directory[n]["version"],
            "sha256": directory[n]["sha256"],
        }
        for n in req.artifact_nos
    ]
    try:
        created = source.create_freeze(
            point_lsn=point["lsn"],
            point_lsn_num=lsn_num,
            entries=entries,
            ttl_seconds=req.duration_seconds,
            note=req.note,
        )
    except FreezeConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"message": str(exc), "code": "FREEZE_CONFLICT"},
        )
    except SourceBusy as exc:
        raise HTTPException(
            status_code=409,
            detail={"message": str(exc), "code": "FREEZE_CONFLICT"},
        )
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    created["artifacts"] = entries
    return JSONResponse(status_code=201, content=created)


@app.post("/api/freezes/{group_id}/release")
def api_release_freeze(group_id: str) -> dict:
    try:
        result = source.release_freeze(group_id)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    if result is None:
        raise HTTPException(status_code=404, detail="冻结组不存在")
    return result


# ---------------------------------------------------------------- 历史查询
@app.get("/api/points")
def api_points() -> dict:
    _require_ready()
    return {"points": store.get_points()}


@app.get("/api/directory")
def api_directory(point: str) -> JSONResponse:
    """按提交点（LSN，如 0/16B3748）查看该时刻的整张目录。"""
    _require_ready()
    try:
        lsn_num = lsn_to_num(point)
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="point 必须是 LSN，形如 0/16B3748")
    if store.get_point(lsn_num) is None:
        raise HTTPException(
            status_code=404,
            detail="该 LSN 不是已捕获的提交点，请先在 /api/points 中选择",
        )
    return JSONResponse(
        {
            "point": point,
            "point_lsn_num": lsn_num,
            "artifacts": store.get_directory_at(lsn_num),
        }
    )


@app.get("/api/history/{artifact_no}")
def api_artifact_history(artifact_no: int) -> dict:
    """单个制品编号的完整变更链；删除后重用编号，旧历史也全部保留。"""
    _require_ready()
    return {
        "artifact_no": artifact_no,
        "history": store.get_history_of(artifact_no),
    }
