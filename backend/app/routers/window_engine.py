"""滑动窗口事件时间计算引擎接口。

独立于通用资源路由，挂载在 /api/window-engine 下（须在 /{module} 动态
路由之前注册，避免被模块名捕获）。提供：历史事件接入/重传、事件时间
计算（覆盖分区写出与回查）、窗口补算结算、批次结算记录与对账。
"""
from __future__ import annotations

import datetime as dt
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.window_engine import (
    WindowTemplateSpec,
    parse_align,
    window_engine,
)

router = APIRouter(prefix="/api/window-engine", tags=["窗口事件时间计算"])


class EventPayload(BaseModel):
    event_id: str
    event_time: str  # ISO8601，必须带时区
    amount: float
    payload: dict[str, Any] = Field(default_factory=dict)


class TemplatePayload(BaseModel):
    template_id: str
    width_hours: float
    step_hours: float
    align: str = "00:00"
    timezone: str = "Asia/Shanghai"


def _parse_event_time(raw: str) -> dt.datetime:
    try:
        ts = dt.datetime.fromisoformat(raw)
    except ValueError:
        raise HTTPException(status_code=422, detail=f"事件时间格式非法：{raw}") from None
    if ts.tzinfo is None:
        raise HTTPException(status_code=422, detail="事件时间必须带时区偏移")
    return ts


@router.get("/state")
def engine_state() -> dict[str, Any]:
    """引擎总览：模板规格、事件与接入凭据、分区数、结算批次数。"""
    return window_engine.state_snapshot()


@router.get("/templates")
def list_templates() -> dict[str, Any]:
    return {"items": window_engine.state_snapshot()["templates"]}


@router.post("/templates")
def register_template(payload: TemplatePayload) -> dict[str, Any]:
    align_parsed = parse_align(payload.align)
    if align_parsed is None:
        raise HTTPException(status_code=422, detail=f"对齐基准格式非法：{payload.align}")
    align, tz_from_align = align_parsed
    spec = WindowTemplateSpec(
        template_id=payload.template_id,
        width_hours=payload.width_hours,
        step_hours=payload.step_hours,
        align=align,
        timezone=tz_from_align or payload.timezone,
    )
    window_engine.register_template(spec)
    return {"ok": True, "message": f"模板 {spec.template_id} 已注册"}


@router.get("/events")
def list_events() -> dict[str, Any]:
    """历史事件记录：含唯一接入凭据与重放次数。"""
    return {"items": window_engine.state_snapshot()["events"]}


@router.post("/events")
def ingest_event(payload: EventPayload) -> dict[str, Any]:
    """接入带标识的历史事件；同标识重复提交幂等，不重复累计接入量。"""
    ts = _parse_event_time(payload.event_time)
    try:
        credential, created = window_engine.ingest_event(
            payload.event_id, ts, payload.amount, payload.payload)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    return {
        "ok": True,
        "created": created,
        "message": "事件已接入" if created else "事件已存在，按唯一凭据去重",
        "credential": credential.to_dict(),
    }


@router.post("/events/{event_id}/replay")
def replay_event(event_id: str) -> dict[str, Any]:
    """重传事件：并发安全，接入量只累计一次，仍可回查各计算时段归属。"""
    try:
        credential = window_engine.replay_event(event_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    return {
        "ok": True,
        "message": f"事件 {event_id} 已重传，接入凭据去重",
        "credential": credential.to_dict(),
    }


@router.get("/events/{event_id}/attribution")
def event_attribution(event_id: str) -> dict[str, Any]:
    """回查事件在各计算时段（窗口）的归属分区。"""
    rows = window_engine.event_attribution(event_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"事件 {event_id} 暂无覆盖分区")
    return {"items": [p.to_dict() for p in rows]}


@router.post("/templates/{template_id}/compute")
def compute_partitions(template_id: str) -> dict[str, Any]:
    """事件时间计算：按步长/宽度/时区写出可回溯的覆盖分区数据。"""
    try:
        partitions = window_engine.compute_partitions(template_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    return {
        "ok": True,
        "message": f"模板 {template_id} 事件时间计算完成，覆盖分区 {len(partitions)} 条",
        "partitions": [p.to_dict() for p in partitions],
    }


@router.get("/templates/{template_id}/partitions")
def list_partitions(
    template_id: str,
    window_key: str | None = Query(default=None),
    event_id: str | None = Query(default=None),
) -> dict[str, Any]:
    """可回溯的覆盖分区数据：按窗口或事件过滤。"""
    rows = window_engine.list_partitions(
        template_id, window_key=window_key, event_id=event_id)
    return {"items": [p.to_dict() for p in rows]}


@router.get("/templates/{template_id}/windows")
def list_windows(template_id: str) -> dict[str, Any]:
    """各窗口汇总：分区特征值、已交付金额、待另一窗提供的范围。"""
    try:
        return {"items": window_engine.window_summary(template_id)}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None


@router.post("/templates/{template_id}/windows/{window_key}/settle")
def settle_window(template_id: str, window_key: str) -> dict[str, Any]:
    """补算结算：按覆盖分区实际计算窗口特征值并交付，记录批次结算。"""
    try:
        batch = window_engine.settle_window(template_id, window_key)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
    if batch is None:
        return {"ok": True, "noop": True,
                "message": f"窗口 {window_key} 无新增可交付事件（幂等空操作）"}
    return {
        "ok": True,
        "noop": False,
        "message": (f"窗口 {window_key} 本批交付 {batch.delivered_amount} 元，"
                    f"累计 {batch.cumulative_delivered} 元"),
        "batch": batch.to_dict(),
    }


@router.get("/settlements")
def list_settlements(template_id: str | None = Query(default=None)) -> dict[str, Any]:
    """批次结算记录：已交付金额、交付凭据、关联分区、待另一窗提供范围。"""
    batches = [b for b in window_engine.settlements
               if template_id is None or b.template_id == template_id]
    return {"items": [b.to_dict() for b in batches]}


@router.get("/reconcile")
def reconcile(template_id: str = Query(...)) -> dict[str, Any]:
    """对账：两窗结算合计与重叠事件的唯一接入凭据核对。"""
    if template_id not in window_engine.templates:
        raise HTTPException(status_code=404, detail=f"模板 {template_id} 未注册")
    return window_engine.reconcile(template_id)
