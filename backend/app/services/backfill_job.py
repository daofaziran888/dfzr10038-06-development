"""特征回溯领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。

「启动回溯」在通用状态流转之外接入事件时间计算引擎：对绑定了窗口标识的
补算任务，先按唯一凭据重传指定事件（并发重放只累计一次接入量），再基于
可回溯的覆盖分区实际计算并交付窗口特征值，批次结算记录已交付金额与尚待
另一窗提供的范围。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.services.window_engine import window_engine


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回溯任务"
    OPERATION_TERM = "计算"
    LIST_FIELDS = ["任务编号", "目标特征", "起始日期", "截止日期", "回溯模式", "并行度", "当前进度", "数据行数"]
    REQUIRED_FIELDS = ["任务编号", "目标特征", "起始日期"]
    STATUS_ORDER = ["待执行", "回溯中", "已完成", "部分缺失"]
    ACTION_RULES = {"启动回溯": "回溯中", "暂停回溯": "已完成", "标记缺失": "部分缺失"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None or action != "启动回溯":
            return entry, message
        template_id = str(entry.get("模板编号") or "")
        window_key = str(entry.get("窗口标识") or "")
        if not template_id or not window_key:
            return entry, message  # 未绑定窗口的任务只走通用流转
        # 重传指定事件：按唯一接入凭据去重，不重复累计接入量
        replayed = []
        for event_id in str(entry.get("重传事件") or "").split(";"):
            event_id = event_id.strip()
            if not event_id:
                continue
            try:
                credential = window_engine.replay_event(event_id)
                replayed.append(f"{event_id}(凭据{credential.credential_id})")
            except KeyError:
                pass
        # 事件时间计算（迟到事件补写覆盖分区）后结算窗口
        window_engine.compute_partitions(template_id)
        batch = window_engine.settle_window(template_id, window_key)
        if batch is None:
            entry["当前进度"] = "100%"
            message += f"；窗口 {window_key} 无新增可交付事件"
            return entry, message
        entry["当前进度"] = "100%"
        entry["数据行数"] = f"{len(batch.delivered_events)}行"
        entry["交付金额"] = f"{batch.delivered_amount}元"
        entry["结算批次"] = batch.batch_id
        message += (
            f"；重传 {len(replayed)} 个事件（{','.join(replayed) or '无'}），"
            f"窗口 {window_key} 批次 {batch.batch_id} 交付 {batch.delivered_amount} 元，"
            f"累计 {batch.cumulative_delivered} 元"
        )
        return entry, message
