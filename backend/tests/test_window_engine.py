"""滑动窗口事件时间计算引擎测试。

场景基线：窗口宽 4 小时、步长 2 小时、00:00 对齐、Asia/Shanghai、左闭右开。
前窗 F=[00:00,04:00) = W20261123T000000；后窗 B=[02:00,06:00) = W20261123T020000；
重叠区 [02:00,04:00)。E7(03:00,7元) 在重叠区迟到；05:00 事件 E5(3元) 为后窗
独有；04:00 事件 E4(4元) 检验右开边界与分别计量。
"""
from __future__ import annotations

import datetime as dt
import unittest
from concurrent.futures import ThreadPoolExecutor

from app.services.window_engine import (
    BACK_WINDOW_KEY,
    FRONT_WINDOW_KEY,
    SCENARIO_TEMPLATE_ID,
    WindowEngine,
    WindowTemplateSpec,
    parse_align,
    parse_hours,
    spec_from_row,
    window_engine,
)

TZ = "Asia/Shanghai"
F_KEY = "W20261123T000000"  # 前窗 [00:00,04:00)
B_KEY = "W20261123T020000"  # 后窗 [02:00,06:00)
NEXT_KEY = "W20261123T040000"  # 下一窗 [04:00,08:00)


def make_spec() -> WindowTemplateSpec:
    return WindowTemplateSpec(
        template_id="TIME-TEST",
        width_hours=4,
        step_hours=2,
        align=dt.time(0, 0),
        timezone=TZ,
    )


def make_engine() -> WindowEngine:
    engine = WindowEngine()
    engine.register_template(make_spec())
    return engine


def ts(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text)


class TestWindowAssignment(unittest.TestCase):
    """事件时间计算：按步长/宽度/时区/边界规则落窗。"""

    def setUp(self) -> None:
        self.spec = make_spec()

    def keys(self, text: str) -> list[str]:
        return [w.key for w in self.spec.covering_windows(ts(text))]

    def test_overlap_event_falls_in_both_windows(self):
        # E7 03:00 落在重叠区 [02:00,04:00)：同时属于前窗与后窗
        self.assertEqual(self.keys("2026-11-23T03:00:00+08:00"), [F_KEY, B_KEY])

    def test_back_window_unique_event(self):
        # 05:00 事件：属于后窗与下一窗，不属于前窗（相对前后两窗为后窗独有）
        self.assertEqual(self.keys("2026-11-23T05:00:00+08:00"), [B_KEY, NEXT_KEY])

    def test_right_open_boundary_excludes_front_window(self):
        # 04:00 恰为前窗终点（右开）→ 不计入前窗；恰为下一窗起点（左闭）→ 计入
        keys = self.keys("2026-11-23T04:00:00+08:00")
        self.assertEqual(keys, [B_KEY, NEXT_KEY])
        self.assertNotIn(F_KEY, keys)

    def test_left_closed_boundary_includes_window_start(self):
        # 00:00 恰为前窗起点（左闭）→ 计入前窗，同时属于前一日的 [22:00,02:00)
        self.assertEqual(
            self.keys("2026-11-23T00:00:00+08:00"),
            ["W20261122T220000", F_KEY],
        )

    def test_timezone_conversion_before_assignment(self):
        # 同一时刻不同时区表示，落窗结果一致（先换算到模板时区）
        self.assertEqual(
            self.keys("2026-11-22T19:00:00+00:00"),  # = 03:00+08:00
            [F_KEY, B_KEY],
        )

    def test_naive_event_time_rejected(self):
        with self.assertRaises(ValueError):
            self.spec.covering_windows(dt.datetime(2026, 11, 23, 3, 0))

    def test_window_key_roundtrip(self):
        window = self.spec.window_by_key(B_KEY)
        self.assertIsNotNone(window)
        self.assertEqual(window.start.isoformat(), "2026-11-23T02:00:00+08:00")
        self.assertEqual(window.end.isoformat(), "2026-11-23T06:00:00+08:00")
        self.assertIsNone(self.spec.window_by_key("W20261123T030000"))  # 不在步长网格上


class TestParamParsing(unittest.TestCase):
    """模板参数解析：宽度/步长/对齐基准从记录变成计算规格。"""

    def test_parse_hours(self):
        self.assertEqual(parse_hours("4小时"), 4.0)
        self.assertEqual(parse_hours("2h"), 2.0)
        self.assertEqual(parse_hours("30分钟"), 0.5)
        self.assertEqual(parse_hours(4), 4.0)
        self.assertIsNone(parse_hours("时间窗口样例1"))

    def test_parse_align(self):
        self.assertEqual(parse_align("00:00"), (dt.time(0, 0), None))
        self.assertEqual(
            parse_align("06:30@Asia/Shanghai"), (dt.time(6, 30), "Asia/Shanghai"))
        self.assertIsNone(parse_align("25:00"))

    def test_spec_from_row(self):
        row = {"模板编号": "TIME-0004", "窗口宽度": "4小时", "滑动步长": "2小时",
               "对齐基准": "00:00", "时区": "Asia/Shanghai"}
        spec = spec_from_row(row)
        self.assertIsNotNone(spec)
        self.assertEqual((spec.width_hours, spec.step_hours, spec.align),
                         (4.0, 2.0, dt.time(0, 0)))
        self.assertIsNone(spec_from_row({"窗口宽度": "时间窗口样例1"}))


class TestIngestionIdempotency(unittest.TestCase):
    """同一事件并发重放只累计一次接入量。"""

    def test_concurrent_replay_counts_once(self):
        engine = make_engine()
        credential, created = engine.ingest_event("E7", ts("2026-11-23T03:00:00+08:00"), 7.0)
        self.assertTrue(created)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: engine.replay_event("E7"), range(20)))
        self.assertEqual(len({r.credential_id for r in results}), 1)
        final = engine.credentials["E7"]
        self.assertEqual(final.credential_id, credential.credential_id)
        self.assertEqual(final.attempts, 21)  # 1 次接入 + 20 次重放
        self.assertEqual(final.amount, 7.0)  # 接入量不重复累计
        self.assertEqual(engine.reconcile("TIME-TEST")["ingested_total"], 7.0)

    def test_reingest_same_event_returns_existing_credential(self):
        engine = make_engine()
        first, _ = engine.ingest_event("E7", ts("2026-11-23T03:00:00+08:00"), 7.0)
        second, created = engine.ingest_event("E7", ts("2026-11-23T03:00:00+08:00"), 7.0)
        self.assertFalse(created)
        self.assertEqual(first.credential_id, second.credential_id)
        self.assertEqual(len(engine.events), 1)

    def test_replay_unknown_event_rejected(self):
        engine = make_engine()
        with self.assertRaises(KeyError):
            engine.replay_event("E404")


class TestBackfillScenario(unittest.TestCase):
    """E7 场景：后窗先补算 10 元，前窗后启动重传 E7，04:00 事件分别计量。"""

    def setUp(self) -> None:
        self.engine = make_engine()
        self.engine.ingest_event("E5", ts("2026-11-23T05:00:00+08:00"), 3.0)
        self.engine.ingest_event("E7", ts("2026-11-23T03:00:00+08:00"), 7.0)
        self.engine.compute_partitions("TIME-TEST")

    def test_partitions_traceable(self):
        # 覆盖分区可回溯：E7 归属前窗+后窗，E5 归属后窗+下一窗
        e7 = self.engine.event_attribution("E7")
        self.assertEqual([p.window_key for p in e7], [F_KEY, B_KEY])
        e5 = self.engine.event_attribution("E5")
        self.assertEqual([p.window_key for p in e5], [B_KEY, NEXT_KEY])
        # 每条分区都挂着唯一接入凭据
        creds = {p.credential_id for p in e7}
        self.assertEqual(len(creds), 1)

    def test_compute_is_idempotent(self):
        before = len(self.engine.partitions)
        self.engine.compute_partitions("TIME-TEST")
        self.assertEqual(len(self.engine.partitions), before)

    def test_back_window_settles_first_with_events_at_hand(self):
        batch = self.engine.settle_window("TIME-TEST", B_KEY)
        # 后窗按到手有效事件先交付：05:00 事件 3 元 + E7 7 元 = 10 元
        self.assertEqual(batch.delivered_amount, 10.0)
        self.assertEqual(batch.cumulative_delivered, 10.0)
        self.assertEqual({e["event_id"] for e in batch.delivered_events}, {"E5", "E7"})
        # 批次结算记下尚待另一窗（前窗）提供的重叠范围
        front_pending = [p for p in batch.pending_ranges
                         if p["awaiting_window_key"] == F_KEY]
        self.assertEqual(len(front_pending), 1)
        self.assertEqual(front_pending[0]["event_id"], "E7")
        self.assertEqual(front_pending[0]["amount"], 7.0)
        self.assertEqual(front_pending[0]["overlap_start"],
                         "2026-11-23T02:00:00+08:00")
        self.assertEqual(front_pending[0]["overlap_end"],
                         "2026-11-23T04:00:00+08:00")
        self.assertEqual(batch.status, "部分交付待另一窗")

    def test_front_window_replay_and_settle(self):
        self.engine.settle_window("TIME-TEST", B_KEY)
        # 启动前窗并重传 E7：接入量不重复累计
        credential = self.engine.replay_event("E7")
        self.assertEqual(credential.attempts, 2)
        self.engine.compute_partitions("TIME-TEST")
        batch = self.engine.settle_window("TIME-TEST", F_KEY)
        # 前窗只交付 E7 的 7 元，交付依据齐全后结清（无待提供范围）
        self.assertEqual(batch.delivered_amount, 7.0)
        self.assertEqual([e["event_id"] for e in batch.delivered_events], ["E7"])
        self.assertEqual(batch.pending_ranges, [])
        self.assertEqual(batch.status, "已交付")
        # 后窗累计保持 10 元
        self.assertEqual(self.engine.window_delivered_total("TIME-TEST", B_KEY), 10.0)
        self.assertEqual(self.engine.window_delivered_total("TIME-TEST", F_KEY), 7.0)
        # 对账：两窗结算合计 17 = 唯一接入 10 + 重叠事件重复交付 7，凭据唯一
        report = self.engine.reconcile("TIME-TEST")
        self.assertTrue(report["ok"])
        self.assertEqual(report["ingested_total"], 10.0)
        self.assertEqual(report["delivered_total"], 17.0)
        self.assertEqual(report["overlap_redelivered"], 7.0)
        e7 = next(e for e in report["per_event"] if e["event_id"] == "E7")
        self.assertEqual(e7["ingested_amount"], 7.0)
        self.assertEqual(len(e7["deliveries"]), 2)
        self.assertEqual({d["window_key"] for d in e7["deliveries"]}, {F_KEY, B_KEY})
        self.assertEqual(len({d["partition_id"] for d in e7["deliveries"]}), 2)

    def test_e4_metered_separately_from_e7(self):
        self.engine.settle_window("TIME-TEST", B_KEY)
        self.engine.replay_event("E7")
        self.engine.compute_partitions("TIME-TEST")
        self.engine.settle_window("TIME-TEST", F_KEY)
        # 04:00 事件迟到：右开边界使其不进前窗，与 E7 分别计量
        self.engine.ingest_event("E4", ts("2026-11-23T04:00:00+08:00"), 4.0)
        self.engine.compute_partitions("TIME-TEST")
        e4 = self.engine.event_attribution("E4")
        self.assertEqual([p.window_key for p in e4], [B_KEY, NEXT_KEY])
        self.assertNotIn(F_KEY, [p.window_key for p in e4])
        self.assertNotEqual(
            self.engine.credentials["E4"].credential_id,
            self.engine.credentials["E7"].credential_id,
        )
        # 前窗分区不含 04:00 事件；后窗累计仍是 10 元（E4 尚未结算）
        front_events = {p.event_id
                        for p in self.engine.list_partitions("TIME-TEST", window_key=F_KEY)}
        self.assertEqual(front_events, {"E7"})
        self.assertEqual(self.engine.window_delivered_total("TIME-TEST", B_KEY), 10.0)
        # 后窗再次补算：只增量交付 E4，不重复接入 E7
        batch2 = self.engine.settle_window("TIME-TEST", B_KEY)
        self.assertEqual(batch2.delivered_amount, 4.0)
        self.assertEqual([e["event_id"] for e in batch2.delivered_events], ["E4"])
        self.assertEqual(batch2.cumulative_delivered, 14.0)
        report = self.engine.reconcile("TIME-TEST")
        e7 = next(e for e in report["per_event"] if e["event_id"] == "E7")
        self.assertEqual(len(e7["deliveries"]), 2)  # 仍是前后两窗各一次
        # 幂等空操作：没有新事件时不再产生批次
        self.assertIsNone(self.engine.settle_window("TIME-TEST", B_KEY))

    def test_settlement_links_source_partitions(self):
        batch = self.engine.settle_window("TIME-TEST", B_KEY)
        # 补算结果关联原覆盖分区
        for partition_id in batch.delivered_partition_ids:
            self.assertIn(partition_id, self.engine.partitions)
        for item in batch.delivered_events:
            partition = self.engine.partitions[
                f"PART-TIME-TEST-{B_KEY}-{item['event_id']}"]
            self.assertEqual(partition.credential_id, item["credential_id"])
            self.assertEqual(partition.amount, item["amount"])


class TestSeededEngineState(unittest.TestCase):
    """种子状态与任务叙事一致：后窗已补算 10 元，前窗尚未领取。"""

    def test_seeded_state(self):
        self.assertIn(SCENARIO_TEMPLATE_ID, window_engine.templates)
        # 后窗已交付 10 元（E5 3元 + E7 7元），前窗尚无结算
        self.assertEqual(
            window_engine.window_delivered_total(SCENARIO_TEMPLATE_ID, BACK_WINDOW_KEY),
            10.0,
        )
        self.assertEqual(
            window_engine.window_delivered_total(SCENARIO_TEMPLATE_ID, FRONT_WINDOW_KEY),
            0.0,
        )
        # E7 接入一次，归属前后两窗
        self.assertEqual(window_engine.credentials["E7"].attempts, 1)
        self.assertEqual(
            [p.window_key for p in window_engine.event_attribution("E7")],
            [FRONT_WINDOW_KEY, BACK_WINDOW_KEY],
        )
        # 后窗批次记下尚待前窗提供的重叠范围
        batch = window_engine.settlements[0]
        self.assertEqual(batch.window_key, BACK_WINDOW_KEY)
        self.assertTrue(any(p["awaiting_window_key"] == FRONT_WINDOW_KEY
                            for p in batch.pending_ranges))


if __name__ == "__main__":
    unittest.main()
