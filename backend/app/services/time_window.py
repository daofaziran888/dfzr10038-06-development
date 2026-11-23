"""时间窗口领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。

「启动计算」在通用状态流转之外接入事件时间计算引擎：把模板的窗口宽度、
滑动步长、对齐基准解析为计算规格，对已接入的带标识历史事件按左闭右开
写出可回溯的覆盖分区数据。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.services.window_engine import spec_from_row, window_engine


class TimeWindowService(BaseModuleService):
    MODULE = "time_window"
    ENTITY = "窗口模板"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["模板编号", "窗口类型", "窗口宽度", "滑动步长", "对齐基准", "缺值填充", "特征列表", "最近计算"]
    REQUIRED_FIELDS = ["模板编号", "窗口类型", "窗口宽度"]
    STATUS_ORDER = ["草稿", "计算中", "窗口关闭", "已废弃"]
    ACTION_RULES = {"启动计算": "计算中", "关闭窗口": "窗口关闭", "废弃模板": "已废弃"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None or action != "启动计算":
            return entry, message
        spec = spec_from_row(entry)
        if spec is None:
            return entry, message
        window_engine.register_template(spec)
        partitions = window_engine.compute_partitions(spec.template_id)
        entry["最近计算"] = datetime.datetime.now().isoformat(timespec="seconds")
        message += f"；事件时间计算按 {spec.width_hours}h 宽/{spec.step_hours}h 步长写出覆盖分区 {len(partitions)} 条"
        return entry, message
