"""FastAPI 历史查询网页：源目录成组修改、提交点查询、捕获进度、写入冻结。

初始化未完成（phase != STREAMING）时，所有历史查询接口返回 503，
不对外开放尚未完整的历史；源目录修改走源库自身事务，不受此限制。

写入冻结的强制力在源 PostgreSQL 触发器内，网页与直接 SQL 同等受限；
本模块只负责按已捕获提交点组装申请快照、调用源库事务并呈现冻结状态。
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Annotated, Literal, Union

from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator

from .capture import CaptureWorker
from .config import settings
from .source import FreezeError, SourceDB, SourceError
from .storage import HistoryStore, lsn_to_num

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("web")

SHA256_RE = r"^[0-9a-fA-F]{64}$"


def _check_sha(v: str) -> str:
    import re

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
    """写入冻结申请：依据一个已捕获提交点，冻结若干现存编号。

    快照四列不由前端自报，而是后端从 SQLite 历史库按该提交点重建，
    避免网页伪造与提交点不一致的快照。
    """

    point_lsn: str = Field(min_length=1, max_length=32)
    artifact_nos: list[int] = Field(min_length=1)
    duration_minutes: int = Field(gt=0, le=60 * 24 * 30)  # 上限 30 天
    reason: str = Field(default="", max_length=500)

    @field_validator("point_lsn")
    @classmethod
    def _lsn(cls, v: str) -> str:
        try:
            lsn_to_num(v)
        except (ValueError, IndexError):
            raise ValueError("point_lsn 必须是 LSN，形如 0/16B3748")
        return v

    @field_validator("artifact_nos")
    @classmethod
    def _unique_nos(cls, v: list[int]) -> list[int]:
        if any(n <= 0 for n in v):
            raise ValueError("制品编号必须为正整数")
        if len(set(v)) != len(v):
            raise ValueError("申请中的制品编号不能重复")
        return v


class ReleaseRequest(BaseModel):
    freeze_id: int = Field(gt=0)


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


app = FastAPI(title="制品目录历史查询", lifespan=lifespan)


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

    命中在保冻结时源库触发器 RAISE，整笔源事务回滚，这里翻译成 400。
    """
    payload = [op.model_dump() for op in req.operations]
    try:
        result = source.apply_operations(payload)
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001 - 统一兜底成 502
        translated = SourceDB.translate_db_error(exc)
        if isinstance(translated, SourceError):
            raise HTTPException(status_code=400, detail=str(translated))
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    result["operations"] = len(payload)
    return result


# ---------------------------------------------------------------- 写入冻结
@app.post("/api/freezes")
def api_create_freeze(req: FreezeRequest) -> JSONResponse:
    """按已捕获提交点对若干现存编号申请写入冻结。

    流程：校验提交点存在 → 从历史库重建该点目录并取出申请编号的四列快照
    → 在单个源事务内锁定源行、逐列比对当前值、写入冻结记录。
    任一编号缺失/不一致/已冻结，整组申请不生效（源事务回滚）。
    """
    _require_ready()
    try:
        lsn_num = lsn_to_num(req.point_lsn)
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="point_lsn 必须是 LSN，形如 0/16B3748")

    valid_nums = {p["lsn_num"] for p in store.get_points()}
    if lsn_num not in valid_nums:
        raise HTTPException(
            status_code=404,
            detail="该 LSN 不是已捕获的提交点，请先在 /api/points 中选择",
        )

    directory = {a["artifact_no"]: a for a in store.get_directory_at(lsn_num)}
    items: list[dict] = []
    missing: list[int] = []
    for no in req.artifact_nos:
        snap = directory.get(no)
        if snap is None:
            missing.append(no)
            continue
        items.append(
            {
                "artifact_no": no,
                "service_name": snap["service_name"],
                "version": snap["version"],
                "sha256": snap["sha256"],
            }
        )
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                "以下编号在该提交点的目录中不存在（可能当时已删除），"
                f"整组申请未受理: {missing}"
            ),
        )

    try:
        result = source.apply_freeze(
            items,
            point_lsn=req.point_lsn,
            duration_seconds=req.duration_minutes * 60,
            reason=req.reason,
        )
    except FreezeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:  # noqa: BLE001
        translated = SourceDB.translate_db_error(exc)
        if isinstance(translated, SourceError):
            raise HTTPException(status_code=400, detail=str(translated))
        raise HTTPException(status_code=502, detail=f"源库错误: {exc}")
    return JSONResponse(jsonable_encoder(result), status_code=201)


@app.get("/api/freezes")
def api_list_freezes(include_expired: bool = False) -> JSONResponse:
    """查看冻结；默认只列未释放记录，active 由源库 now() 实时判定。"""
    rows = source.list_freezes(include_expired=include_expired)
    return JSONResponse({"freezes": jsonable_encoder(rows)})


@app.post("/api/freezes/release")
def api_release_freeze(req: ReleaseRequest) -> dict:
    try:
        return source.release_freeze(req.freeze_id)
    except FreezeError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except SourceError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


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
    points = store.get_points()
    valid_nums = {p["lsn_num"] for p in points}
    if lsn_num not in valid_nums:
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
