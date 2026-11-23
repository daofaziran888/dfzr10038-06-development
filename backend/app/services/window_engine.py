"""滑动窗口事件时间计算引擎。

把窗口模板的宽度、滑动步长、对齐基准和时区从"参数"变成真正的计算规则：

1. 事件时间计算：按步长/宽度/时区把带标识的历史事件分配到左闭右开
   ``[start, start+width)`` 的滑动窗口，写出可回溯的覆盖分区数据。
2. 幂等接入：同一事件并发重放只累计一次接入量，凭据唯一，仍可回查
   该事件在各计算时段（窗口）的归属。
3. 补算结算：补算任务基于覆盖分区实际计算窗口特征值并交付；批次结算
   记录已交付金额、交付凭据、关联分区，以及尚待另一窗提供的重叠范围。
4. 对账：各窗口结算合计与重叠事件的唯一接入凭据可核对（接入一次、
   多窗各交付一次）。

典型场景（种子数据即按此初始化）：窗口宽 4 小时、步长 2 小时、00:00 对齐、
Asia/Shanghai、左闭右开。前窗 F=[00:00,04:00)，后窗 B=[02:00,06:00)，
重叠区 [02:00,04:00)。E7(03:00, 7元) 落在重叠区迟到，后窗先补算
（05:00 事件 3元 + E7 7元 = 10元），前窗尚未领取；之后启动前窗并重传 E7，
接入不重复累计，前窗交付 7 元；04:00 事件因右开边界不进前窗，与 E7 分别计量。
"""
from __future__ import annotations

import datetime as dt
import re
import threading
from dataclasses import dataclass, field
from typing import Any
from zoneinfo import ZoneInfo

BOUNDARY_RULE = "左闭右开"  # [start, end)：起点计入、终点不计入
DEFAULT_TIMEZONE = "Asia/Shanghai"

# 无 zoneinfo 数据时的兜底：常见时区给固定偏移
_TZ_FALLBACK_OFFSETS = {"Asia/Shanghai": 8, "UTC": 0, "Etc/UTC": 0}


def load_timezone(name: str) -> dt.tzinfo:
    """按名称加载时区；容器缺 tzdata 时对常见时区退化为固定偏移。"""
    try:
        return ZoneInfo(name)
    except Exception:
        hours = _TZ_FALLBACK_OFFSETS.get(name)
        if hours is None:
            raise ValueError(f"未知时区：{name}") from None
        return dt.timezone(dt.timedelta(hours=hours), name)


_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(小时|h|H|分钟|min)?\s*$")
_ALIGN_RE = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?(?:@(.+))?$")


def parse_hours(value: Any) -> float | None:
    """把 "4小时"/"4h"/"4"/4 解析为小时数；无法解析返回 None。"""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    match = _DURATION_RE.match(str(value or ""))
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2)
    if unit in ("分钟", "min"):
        amount /= 60.0
    return amount


def parse_align(value: Any) -> tuple[dt.time, str | None] | None:
    """解析对齐基准 "00:00"、"06:30:00"、"00:00@Asia/Shanghai"。

    返回 (时刻, 可选时区名)；无法解析返回 None。
    """
    match = _ALIGN_RE.match(str(value or "").strip())
    if not match:
        return None
    hour, minute = int(match.group(1)), int(match.group(2))
    second = int(match.group(3) or 0)
    if hour > 23 or minute > 59 or second > 59:
        return None
    return dt.time(hour, minute, second), match.group(4)


@dataclass(frozen=True)
class WindowInterval:
    """一个具体窗口实例：左闭右开 [start, end)。"""

    template_id: str
    index: int  # 相对对齐基准的步数 k
    start: dt.datetime
    end: dt.datetime

    @property
    def key(self) -> str:
        """窗口标识：模板内由起始时刻唯一确定，可安全用于 URL 路径。"""
        return "W" + self.start.strftime("%Y%m%dT%H%M%S")

    def contains(self, ts: dt.datetime) -> bool:
        return self.start <= ts < self.end

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_key": self.key,
            "index": self.index,
            "window_start": self.start.isoformat(),
            "window_end": self.end.isoformat(),
            "boundary": BOUNDARY_RULE,
        }


@dataclass(frozen=True)
class WindowTemplateSpec:
    """窗口模板规格：宽度、步长、对齐基准、时区共同决定事件落窗位置。"""

    template_id: str
    width_hours: float
    step_hours: float
    align: dt.time
    timezone: str = DEFAULT_TIMEZONE
    boundary: str = BOUNDARY_RULE

    def __post_init__(self) -> None:
        if self.width_hours <= 0 or self.step_hours <= 0:
            raise ValueError("窗口宽度和滑动步长必须为正数")

    @property
    def tz(self) -> dt.tzinfo:
        return load_timezone(self.timezone)

    @property
    def _width_s(self) -> int:
        return int(round(self.width_hours * 3600))

    @property
    def _step_s(self) -> int:
        return int(round(self.step_hours * 3600))

    def localize(self, ts: dt.datetime) -> dt.datetime:
        """事件时间必须带时区；换算到模板时区后再参与落窗计算。"""
        if ts.tzinfo is None:
            raise ValueError(f"事件时间缺少时区信息：{ts!r}")
        return ts.astimezone(self.tz)

    def covering_windows(self, event_ts: dt.datetime) -> list[WindowInterval]:
        """按左闭右开计算事件落入的全部窗口（宽度>步长时必然多窗）。

        窗口起点 = 当日对齐基准 + k*步长；命中条件 start <= t < start+width。
        端点规则：t 恰为某窗终点（右开）不计入该窗，恰为起点（左闭）计入。
        """
        t = self.localize(event_ts)
        anchor = dt.datetime.combine(t.date(), self.align, tzinfo=self.tz)
        delta = (t - anchor).total_seconds()
        step_s, width_s = self._step_s, self._width_s
        import math

        k_max = math.floor(delta / step_s)  # 最后一个 start <= t 的窗口
        k_min = math.floor((delta - width_s) / step_s) + 1  # 第一个 t < start+width 的窗口
        windows: list[WindowInterval] = []
        for k in range(k_min, k_max + 1):
            start = anchor + dt.timedelta(seconds=k * step_s)
            interval = WindowInterval(self.template_id, k, start,
                                      start + dt.timedelta(seconds=width_s))
            assert interval.contains(t), "落窗计算与边界规则不一致"
            windows.append(interval)
        return windows

    def window_by_key(self, window_key: str) -> WindowInterval | None:
        """由窗口标识还原窗口实例（用于按标识结算）。"""
        if not window_key.startswith("W"):
            return None
        try:
            start = dt.datetime.strptime(window_key[1:], "%Y%m%dT%H%M%S").replace(
                tzinfo=self.tz)
        except ValueError:
            return None
        anchor = dt.datetime.combine(start.date(), self.align, tzinfo=self.tz)
        k = round((start - anchor).total_seconds() / self._step_s)
        real_start = anchor + dt.timedelta(seconds=k * self._step_s)
        if real_start != start:
            return None  # 起始时刻必须落在步长网格上
        return WindowInterval(self.template_id, k, real_start,
                              real_start + dt.timedelta(seconds=self._width_s))


def spec_from_row(row: dict[str, Any]) -> WindowTemplateSpec | None:
    """从窗口模板记录解析规格；占位数据（无法解析）返回 None。"""
    width = parse_hours(row.get("窗口宽度"))
    step = parse_hours(row.get("滑动步长"))
    align_parsed = parse_align(row.get("对齐基准"))
    if width is None or step is None or align_parsed is None:
        return None
    align, tz_from_align = align_parsed
    timezone = str(row.get("时区") or tz_from_align or DEFAULT_TIMEZONE)
    template_id = str(row.get("模板编号") or f"TEMPLATE-{row.get('id', '')}")
    return WindowTemplateSpec(template_id, width, step, align, timezone)


@dataclass
class EventRecord:
    """带标识的历史事件记录。"""

    event_id: str
    event_time: dt.datetime
    amount: float
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_time": self.event_time.isoformat(),
            "amount": self.amount,
            "payload": self.payload,
        }


@dataclass
class IngestionCredential:
    """事件唯一接入凭据：重放只增加尝试次数，接入量不重复累计。"""

    credential_id: str
    event_id: str
    amount: float
    first_seen_at: str
    attempts: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "credential_id": self.credential_id,
            "event_id": self.event_id,
            "amount": self.amount,
            "first_seen_at": self.first_seen_at,
            "attempts": self.attempts,
        }


@dataclass
class CoveragePartition:
    """覆盖分区数据：一条 (事件 × 窗口) 归属记录，可回溯、可关联结算。"""

    partition_id: str
    template_id: str
    window_key: str
    window_start: str
    window_end: str
    event_id: str
    credential_id: str
    event_time: str
    amount: float
    computed_at: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


@dataclass
class SettlementBatch:
    """补算批次结算：记录本批交付金额、凭据、关联分区与待他窗提供的范围。"""

    batch_id: str
    template_id: str
    window_key: str
    window_start: str
    window_end: str
    delivered_amount: float
    delivered_events: list[dict[str, Any]]
    delivered_partition_ids: list[str]
    pending_ranges: list[dict[str, Any]]
    cumulative_delivered: float
    status: str
    settled_at: str

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class WindowEngine:
    """事件时间计算与补算结算引擎（全部状态内存可回溯）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.templates: dict[str, WindowTemplateSpec] = {}
        self.events: dict[str, EventRecord] = {}
        self.credentials: dict[str, IngestionCredential] = {}  # event_id -> 凭据
        self.partitions: dict[str, CoveragePartition] = {}  # partition_id -> 分区
        self.settlements: list[SettlementBatch] = []
        self._credential_seq = 0
        self._batch_seq = 0

    # ------------------------------------------------------------------ 模板

    def register_template(self, spec: WindowTemplateSpec) -> WindowTemplateSpec:
        with self._lock:
            self.templates[spec.template_id] = spec
        return spec

    def get_template(self, template_id: str) -> WindowTemplateSpec | None:
        return self.templates.get(template_id)

    # ------------------------------------------------------------------ 接入

    def ingest_event(
        self,
        event_id: str,
        event_time: dt.datetime,
        amount: float,
        payload: dict[str, Any] | None = None,
    ) -> tuple[IngestionCredential, bool]:
        """接入事件：按事件标识幂等，并发重放只累计一次接入量。

        返回 (凭据, 是否本次新建)。重放时 attempts +1，金额不重复累计。
        """
        event_id = str(event_id).strip()
        if not event_id:
            raise ValueError("事件标识不能为空")
        if event_time.tzinfo is None:
            raise ValueError("事件时间必须带时区")
        with self._lock:
            existing = self.credentials.get(event_id)
            if existing is not None:
                existing.attempts += 1
                return existing, False
            self._credential_seq += 1
            credential = IngestionCredential(
                credential_id=f"ING-{self._credential_seq:04d}",
                event_id=event_id,
                amount=float(amount),
                first_seen_at=dt.datetime.now().isoformat(timespec="seconds"),
            )
            self.credentials[event_id] = credential
            self.events[event_id] = EventRecord(
                event_id, event_time, float(amount), dict(payload or {}))
            return credential, True

    def replay_event(self, event_id: str) -> IngestionCredential:
        """重传已接入事件：凭据去重，接入量不重复累计。"""
        with self._lock:
            credential = self.credentials.get(event_id)
            if credential is None:
                raise KeyError(f"事件 {event_id} 尚未接入，无法重传")
            credential.attempts += 1
            return credential

    # ------------------------------------------------------------ 事件时间计算

    def compute_partitions(self, template_id: str) -> list[CoveragePartition]:
        """按步长/宽度/时区计算每笔已接入事件在模板中的落窗位置。

        覆盖分区按 (事件, 窗口) 幂等写出：重复计算不会产生重复分区，
        迟到事件补算时只追加其新覆盖的分区，不会因模板错开而漏掉重叠区。
        """
        spec = self.templates.get(template_id)
        if spec is None:
            raise KeyError(f"模板 {template_id} 未注册")
        with self._lock:
            for event in self.events.values():
                for window in spec.covering_windows(event.event_time):
                    partition_id = f"PART-{template_id}-{window.key}-{event.event_id}"
                    if partition_id in self.partitions:
                        continue  # 同一事件同一窗口只写一条分区
                    self.partitions[partition_id] = CoveragePartition(
                        partition_id=partition_id,
                        template_id=template_id,
                        window_key=window.key,
                        window_start=window.start.isoformat(),
                        window_end=window.end.isoformat(),
                        event_id=event.event_id,
                        credential_id=self.credentials[event.event_id].credential_id,
                        event_time=event.event_time.isoformat(),
                        amount=event.amount,
                        computed_at=dt.datetime.now().isoformat(timespec="seconds"),
                    )
            return self.list_partitions(template_id)

    def list_partitions(
        self,
        template_id: str,
        window_key: str | None = None,
        event_id: str | None = None,
    ) -> list[CoveragePartition]:
        """回查覆盖分区：可按窗口（计算时段）或事件过滤。"""
        rows = [p for p in self.partitions.values() if p.template_id == template_id]
        if window_key is not None:
            rows = [p for p in rows if p.window_key == window_key]
        if event_id is not None:
            rows = [p for p in rows if p.event_id == event_id]
        return sorted(rows, key=lambda p: (p.window_start, p.event_id))

    def event_attribution(self, event_id: str) -> list[CoveragePartition]:
        """回查某事件在各计算时段（窗口）的归属。"""
        return sorted(
            (p for p in self.partitions.values() if p.event_id == event_id),
            key=lambda p: p.window_start,
        )

    # ---------------------------------------------------------------- 结算

    def _delivered_event_ids(self, template_id: str, window_key: str) -> set[str]:
        delivered: set[str] = set()
        for batch in self.settlements:
            if batch.template_id == template_id and batch.window_key == window_key:
                delivered.update(item["event_id"] for item in batch.delivered_events)
        return delivered

    def window_delivered_total(self, template_id: str, window_key: str) -> float:
        return round(sum(
            batch.delivered_amount
            for batch in self.settlements
            if batch.template_id == template_id and batch.window_key == window_key
        ), 6)

    def _pending_ranges(
        self, spec: WindowTemplateSpec, window: WindowInterval, event_ids: list[str]
    ) -> list[dict[str, Any]]:
        """计算本窗已交付事件在其他窗口尚未结算的重叠范围。

        每条待提供范围 = 本窗与另一窗的交集区间 + 涉及事件与金额，
        供批次结算记录"尚待另一窗提供的范围"。
        """
        pending: list[dict[str, Any]] = []
        for event_id in event_ids:
            for partition in self.event_attribution(event_id):
                if partition.template_id != spec.template_id:
                    continue
                if partition.window_key == window.key:
                    continue
                other = spec.window_by_key(partition.window_key)
                if other is None:
                    continue
                already = event_id in self._delivered_event_ids(
                    spec.template_id, other.key)
                if already:
                    continue  # 另一窗已交付该事件，不再挂起
                overlap_start = max(window.start, other.start)
                overlap_end = min(window.end, other.end)
                if overlap_start >= overlap_end:
                    continue
                pending.append({
                    "awaiting_window_key": other.key,
                    "awaiting_window_start": other.start.isoformat(),
                    "awaiting_window_end": other.end.isoformat(),
                    "overlap_start": overlap_start.isoformat(),
                    "overlap_end": overlap_end.isoformat(),
                    "event_id": event_id,
                    "amount": self.credentials[event_id].amount,
                })
        return pending

    def settle_window(self, template_id: str, window_key: str) -> SettlementBatch | None:
        """补算结算：用覆盖分区实际计算窗口特征值并交付。

        - 只交付本窗尚未交付过的事件（后续补算不会重复接入同一事件）；
        - 批次记录已交付金额、交付凭据、关联的覆盖分区，以及尚待另一窗
          提供的重叠范围；
        - 没有新增可交付事件时返回 None（幂等空操作）。
        """
        spec = self.templates.get(template_id)
        if spec is None:
            raise KeyError(f"模板 {template_id} 未注册")
        window = spec.window_by_key(window_key)
        if window is None:
            raise KeyError(f"窗口标识 {window_key} 不在模板 {template_id} 的步长网格上")
        with self._lock:
            partitions = self.list_partitions(template_id, window_key=window_key)
            if not partitions:
                raise KeyError(f"窗口 {window_key} 没有覆盖分区，请先执行事件时间计算")
            delivered_before = self._delivered_event_ids(template_id, window_key)
            new_partitions = [p for p in partitions if p.event_id not in delivered_before]
            if not new_partitions:
                return None
            self._batch_seq += 1
            delivered_amount = round(sum(p.amount for p in new_partitions), 6)
            all_event_ids = sorted(delivered_before | {p.event_id for p in new_partitions})
            pending = self._pending_ranges(spec, window, all_event_ids)
            batch = SettlementBatch(
                batch_id=f"SETTLE-{self._batch_seq:04d}",
                template_id=template_id,
                window_key=window_key,
                window_start=window.start.isoformat(),
                window_end=window.end.isoformat(),
                delivered_amount=delivered_amount,
                delivered_events=[
                    {
                        "event_id": p.event_id,
                        "credential_id": p.credential_id,
                        "amount": p.amount,
                        "partition_id": p.partition_id,
                    }
                    for p in new_partitions
                ],
                delivered_partition_ids=[p.partition_id for p in new_partitions],
                pending_ranges=pending,
                cumulative_delivered=round(
                    self.window_delivered_total(template_id, window_key)
                    + delivered_amount, 6),
                status="已交付" if not pending else "部分交付待另一窗",
                settled_at=dt.datetime.now().isoformat(timespec="seconds"),
            )
            self.settlements.append(batch)
            return batch

    # ---------------------------------------------------------------- 视图

    def window_summary(self, template_id: str) -> list[dict[str, Any]]:
        """各窗口汇总：分区特征值、已交付、待他窗提供的范围。"""
        spec = self.templates.get(template_id)
        if spec is None:
            raise KeyError(f"模板 {template_id} 未注册")
        with self._lock:
            keys = sorted({p.window_key for p in self.partitions.values()
                           if p.template_id == template_id})
            summaries = []
            for key in keys:
                window = spec.window_by_key(key)
                partitions = self.list_partitions(template_id, window_key=key)
                assigned = round(sum(p.amount for p in partitions), 6)
                delivered_ids = self._delivered_event_ids(template_id, key)
                pending = self._pending_ranges(spec, window, sorted(delivered_ids)) \
                    if window is not None else []
                summaries.append({
                    "window_key": key,
                    "window_start": window.start.isoformat() if window else None,
                    "window_end": window.end.isoformat() if window else None,
                    "assigned_events": sorted({p.event_id for p in partitions}),
                    "assigned_amount": assigned,
                    "delivered_amount": self.window_delivered_total(template_id, key),
                    "delivered_events": sorted(delivered_ids),
                    "pending_ranges": pending,
                })
            return summaries

    def reconcile(self, template_id: str) -> dict[str, Any]:
        """对账：各窗口结算合计与重叠事件的唯一接入凭据核对。

        接入侧按唯一凭据只累计一次；交付侧同一凭据可在多个窗口各交付
        一次（重叠区口径）。两侧差额必须恰好等于重叠事件的重复交付额。
        """
        with self._lock:
            batches = [b for b in self.settlements if b.template_id == template_id]
            per_event: list[dict[str, Any]] = []
            ingested_total = 0.0
            delivered_total = 0.0
            ok = True
            for event_id, credential in sorted(self.credentials.items()):
                deliveries = [
                    {"batch_id": b.batch_id, "window_key": b.window_key,
                     "amount": item["amount"], "partition_id": item["partition_id"]}
                    for b in batches
                    for item in b.delivered_events
                    if item["event_id"] == event_id
                ]
                # 核对：每次交付金额必须等于凭据金额（同一事件不拆单）
                line_ok = all(d["amount"] == credential.amount for d in deliveries)
                # 核对：同一窗口不重复交付
                window_keys = [d["window_key"] for d in deliveries]
                line_ok = line_ok and len(window_keys) == len(set(window_keys))
                # 核对：交付行必须关联到已写出的覆盖分区
                line_ok = line_ok and all(
                    d["partition_id"] in self.partitions for d in deliveries)
                ok = ok and line_ok
                ingested_total += credential.amount
                delivered_total += sum(d["amount"] for d in deliveries)
                per_event.append({
                    "event_id": event_id,
                    "credential_id": credential.credential_id,
                    "ingested_amount": credential.amount,
                    "ingest_attempts": credential.attempts,
                    "deliveries": deliveries,
                    "delivered_amount": round(sum(d["amount"] for d in deliveries), 6),
                    "ok": line_ok,
                })
            return {
                "template_id": template_id,
                "ingested_total": round(ingested_total, 6),
                "delivered_total": round(delivered_total, 6),
                "overlap_redelivered": round(delivered_total - ingested_total, 6),
                "per_event": per_event,
                "ok": ok,
            }

    def state_snapshot(self) -> dict[str, Any]:
        """引擎总览：模板、事件、分区、结算与对账摘要。"""
        with self._lock:
            return {
                "templates": [
                    {
                        "template_id": spec.template_id,
                        "width_hours": spec.width_hours,
                        "step_hours": spec.step_hours,
                        "align": spec.align.isoformat(),
                        "timezone": spec.timezone,
                        "boundary": spec.boundary,
                    }
                    for spec in self.templates.values()
                ],
                "events": [
                    {**event.to_dict(),
                     "credential": self.credentials[event.event_id].to_dict()}
                    for event in self.events.values()
                ],
                "partitions": len(self.partitions),
                "settlement_batches": len(self.settlements),
            }


# ---------------------------------------------------------------------------
# 种子：按任务叙事初始化到"后窗已补算、前窗尚未领取"的状态
# ---------------------------------------------------------------------------

SCENARIO_TEMPLATE_ID = "TIME-0004"
FRONT_WINDOW_KEY = "W20261123T000000"  # 前窗 [00:00,04:00)
BACK_WINDOW_KEY = "W20261123T020000"  # 后窗 [02:00,06:00)


def seed_window_engine(engine: WindowEngine) -> WindowEngine:
    """初始化演示场景：4h 宽 / 2h 步长 / 00:00 对齐 / 左闭右开。

    - 接入 05:00 事件 E5（3元，后窗独有）与重叠区迟到的 E7（03:00, 7元）；
    - 事件时间计算写出覆盖分区；
    - 后窗 [02:00,06:00) 先补算交付 10 元，重叠区挂起待前窗提供；
    - 前窗 [00:00,04:00) 与 04:00 边界事件留待后续动作演示。
    """
    spec = WindowTemplateSpec(
        template_id=SCENARIO_TEMPLATE_ID,
        width_hours=4,
        step_hours=2,
        align=dt.time(0, 0),
        timezone=DEFAULT_TIMEZONE,
    )
    engine.register_template(spec)
    engine.ingest_event("E5", dt.datetime.fromisoformat("2026-11-23T05:00:00+08:00"), 3.0,
                        {"说明": "后窗独有的 05:00 事件"})
    engine.ingest_event("E7", dt.datetime.fromisoformat("2026-11-23T03:00:00+08:00"), 7.0,
                        {"说明": "两窗重叠区迟到的共同事件"})
    engine.compute_partitions(spec.template_id)
    engine.settle_window(spec.template_id, BACK_WINDOW_KEY)
    return engine


window_engine = seed_window_engine(WindowEngine())
