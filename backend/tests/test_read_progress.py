"""空窗确认与读取进度协调的端到端测试。

场景：同一个 11:00~12:00 窗口，来源 A 读取中断且没有结束回执，
来源 B 已正常结束但没有数据。验证：

1. 零行无回执 ⇒ 读取中断，不能当成空窗，也不能补采；
2. 零行有匹配回执 ⇒ 空窗确认，无需补采；
3. 跨源任务分源认领，续办时冲突并逐来源说明下一时段安排；
4. 增量归并只用已认领区间选行，跨源指标只从共同确认范围汇总；
5. 别的来源/时间的回执不能替代确认。
"""
from __future__ import annotations

import pytest

from app.read_progress import ReadProgressCoordinator

WINDOW_START = "2026-11-23T11:00:00"
WINDOW_END = "2026-11-23T12:00:00"
NEXT_START = "2026-11-23T12:00:00"
NEXT_END = "2026-11-23T13:00:00"

SOURCE_A = "SRC-A"
SOURCE_B = "SRC-B"
CROSS_TASK = "TASK-CROSS-01"
B_ONLY_TASK = "TASK-B-ONLY-01"


@pytest.fixture
def coordinator() -> ReadProgressCoordinator:
    """每个测试获得独立协调器（空窗确认仍会同步到全局仓库表）。"""
    return ReadProgressCoordinator()


def _setup_split_window(coordinator: ReadProgressCoordinator) -> None:
    """构造 11:00~12:00 窗口：A 中断无回执，B 零行正常结束。"""
    # A 的读取中断，没有结束回执。
    coordinator.mark_read_interrupted(SOURCE_A, WINDOW_START, WINDOW_END,
                                      read_row_count=0)
    # B 正常结束且没有数据：先交零行结束回执。
    result = coordinator.submit_end_receipt(
        SOURCE_B, WINDOW_START, WINDOW_END, row_count=0,
        receipt_code="RCPT-B-1100",
    )
    assert result["ok"] is True


class TestIdleGapVsInterruption:
    """空窗确认与读取中断的区分。"""

    def test_a_interrupted_without_receipt_is_not_idle_gap(
            self, coordinator: ReadProgressCoordinator) -> None:
        """A 读取中断且无结束回执：零行也不是空窗，进度指出未读位置。"""
        _setup_split_window(coordinator)

        window_a = coordinator.get_window(SOURCE_A, WINDOW_START, WINDOW_END)
        assert window_a["finished"] is False
        assert window_a["is_interrupted"] is True
        assert window_a["is_idle_gap"] is False
        assert window_a["unread_position"] == WINDOW_START

        progress = coordinator.source_progress(SOURCE_A)
        assert progress["state"] == "读取中断"
        assert progress["unread_position"] == WINDOW_START

    def test_b_zero_row_with_receipt_becomes_confirmed_idle_gap(
            self, coordinator: ReadProgressCoordinator) -> None:
        """B 零行 + 匹配的结束回执 ⇒ 有效空窗确认。"""
        _setup_split_window(coordinator)

        window_b = coordinator.get_window(SOURCE_B, WINDOW_START, WINDOW_END)
        assert window_b["finished"] is True
        assert window_b["is_idle_gap"] is True
        assert window_b["unread_position"] == ""

        confirmations = coordinator.list_confirmations(SOURCE_B)
        assert len(confirmations) == 1
        confirmation = confirmations[0]
        assert confirmation["source"] == SOURCE_B
        assert confirmation["window_start"] == WINDOW_START
        assert confirmation["window_end"] == WINDOW_END
        assert confirmation["row_count"] == 0
        assert confirmation["end_receipt_code"] == "RCPT-B-1100"
        assert confirmation["need_backfill"] is False

    def test_confirmed_idle_gap_needs_no_backfill(
            self, coordinator: ReadProgressCoordinator) -> None:
        """B 的空数据窗口已确认，值班无需为它补采。"""
        _setup_split_window(coordinator)
        decision = coordinator.needs_backfill(SOURCE_B, WINDOW_START, WINDOW_END)
        assert decision["need_backfill"] is False
        assert "无需补采" in decision["reason"]

    def test_interrupted_window_must_not_be_backfilled(
            self, coordinator: ReadProgressCoordinator) -> None:
        """A 缺少结束证据时不能凭零行补采，须指出尚未读取的位置。"""
        _setup_split_window(coordinator)
        decision = coordinator.needs_backfill(SOURCE_A, WINDOW_START, WINDOW_END)
        assert decision["need_backfill"] is False
        assert decision["unread_position"] == WINDOW_START
        assert "续读" in decision["reason"]
        # A 此时没有任何空窗确认记录。
        assert coordinator.list_confirmations(SOURCE_A) == []

    def test_other_source_or_window_receipt_cannot_confirm(
            self, coordinator: ReadProgressCoordinator) -> None:
        """别的来源或时间的回执不能替 A 确认。"""
        _setup_split_window(coordinator)

        # B 的回执不能替 A 确认：A 仍是中断状态。
        window_a = coordinator.get_window(SOURCE_A, WINDOW_START, WINDOW_END)
        assert window_a["end_receipt_code"] == ""
        assert window_a["is_interrupted"] is True

        # 用 A 自身、但属于另一个时间窗的零行回执，也不能确认 11~12 窗口。
        other = coordinator.submit_end_receipt(
            SOURCE_A, "2026-11-23T09:00:00", "2026-11-23T10:00:00",
            row_count=0, receipt_code="RCPT-A-0900",
        )
        assert other["ok"] is True
        window_a = coordinator.get_window(SOURCE_A, WINDOW_START, WINDOW_END)
        assert window_a["is_interrupted"] is True
        assert window_a["end_receipt_code"] == ""

        # 行清点不匹配的回执被拒收。
        coordinator.add_source_row(SOURCE_A, "2026-11-23T11:30:00", 7.0)
        rejected = coordinator.submit_end_receipt(
            SOURCE_A, WINDOW_START, WINDOW_END, row_count=0,
            receipt_code="RCPT-A-WRONG",
        )
        assert rejected["ok"] is False
        assert coordinator.get_window(SOURCE_A, WINDOW_START, WINDOW_END)[
            "is_interrupted"] is True


class TestClaimsAndRenewal:
    """分源认领与跨源续办冲突。"""

    def test_cross_source_claim_waits_while_b_only_claim_proceeds(
            self, coordinator: ReadProgressCoordinator) -> None:
        """值班同时提交跨源续办和只消费 B 的申请。"""
        _setup_split_window(coordinator)

        # 跨源任务领取 A/B 的同一窗口：A 未确认，进入等待。
        cross_claim = coordinator.claim_input(
            CROSS_TASK, SOURCE_A, WINDOW_START, WINDOW_END,
            cross_source=True, participant_sources=[SOURCE_A, SOURCE_B],
        )
        assert cross_claim["ok"] is False
        assert cross_claim["status"] == "等待中"
        assert SOURCE_A in cross_claim["wait_reason"]

        # 只消费 B 的申请：B 已确认空窗，可正常领取，并关联空窗确认。
        b_claim = coordinator.claim_input(
            B_ONLY_TASK, SOURCE_B, WINDOW_START, WINDOW_END,
            cross_source=False,
        )
        assert b_claim["ok"] is True
        assert b_claim["status"] == "已认领"
        expected_code = coordinator.list_confirmations(SOURCE_B)[0]["confirm_code"]
        assert b_claim["claim"]["related_confirmation_codes"] == [expected_code]

    def test_renewal_conflict_explains_next_window_schedule(
            self, coordinator: ReadProgressCoordinator) -> None:
        """续办时 B 的有效空窗与 A 未读完冲突，须说明 B 是否排入下一时段。"""
        _setup_split_window(coordinator)
        renewal = coordinator.arrange_renewal(
            CROSS_TASK, WINDOW_START, WINDOW_END,
            participant_sources=[SOURCE_A, SOURCE_B],
            next_window_start=NEXT_START, next_window_end=NEXT_END,
        )
        assert renewal["conflict"] is True
        assert SOURCE_A in renewal["conflict_reason"]

        schedule = renewal["renewal"]["source_schedule"]
        # B 的空窗已确认：无需补采，已为 B 排入下一时段。
        assert schedule[SOURCE_B]["state"] == "有效空窗"
        assert schedule[SOURCE_B]["need_backfill"] is False
        assert schedule[SOURCE_B]["next_window_scheduled"] is True
        assert schedule[SOURCE_B]["next_window"] == f"{NEXT_START}~{NEXT_END}"
        # A 缺少结束证据：未排入下一时段，先按未读位置续读。
        assert schedule[SOURCE_A]["state"] == "读取中断"
        assert schedule[SOURCE_A]["next_window_scheduled"] is False
        assert schedule[SOURCE_A]["unread_position"] == WINDOW_START

    def test_claim_positions_and_wait_reasons_are_queryable(
            self, coordinator: ReadProgressCoordinator) -> None:
        """分别查跨源任务和 B 作业实际领到的位置及等待原因。"""
        _setup_split_window(coordinator)
        coordinator.claim_input(
            CROSS_TASK, SOURCE_A, WINDOW_START, WINDOW_END,
            cross_source=True, participant_sources=[SOURCE_A, SOURCE_B],
        )
        coordinator.claim_input(
            B_ONLY_TASK, SOURCE_B, WINDOW_START, WINDOW_END,
        )

        cross_claims = coordinator.list_claims(CROSS_TASK)
        assert len(cross_claims) == 1
        assert cross_claims[0]["status"] == "等待中"
        assert cross_claims[0]["window_start"] == WINDOW_START
        assert "尚未确认" in cross_claims[0]["wait_reason"]

        b_claims = [
            c for c in coordinator.list_claims(B_ONLY_TASK)
            if c["status"] == "已认领"
        ]
        assert len(b_claims) == 1
        assert b_claims[0]["source"] == SOURCE_B
        assert b_claims[0]["wait_reason"] == ""

    def test_every_claim_is_linked_to_idle_gap_confirmation(
            self, coordinator: ReadProgressCoordinator) -> None:
        """推进协调把每次认领与空窗确认记录关联。"""
        _setup_split_window(coordinator)
        b_claim = coordinator.claim_input(
            B_ONLY_TASK, SOURCE_B, WINDOW_START, WINDOW_END)
        linked = b_claim["claim"]["related_confirmation_codes"]
        assert len(linked) == 1
        confirmation_codes = {
            c["confirm_code"] for c in coordinator.list_confirmations()
        }
        assert linked[0] in confirmation_codes

        # A 的窗口不是空窗，认领记录上没有空窗关联。
        a_claim = coordinator.claim_input(
            B_ONLY_TASK, SOURCE_A, WINDOW_START, WINDOW_END)
        assert a_claim["claim"]["related_confirmation_codes"] == []


class TestDeltaMergeAndCrossSourceAggregation:
    """增量归并选行与跨源共同确认区间汇总。"""

    def test_receipt_repair_allows_next_round_from_noon(
            self, coordinator: ReadProgressCoordinator) -> None:
        """给 A 补入 11:30 金额 7 的行及正确结束回执，下一轮从 12:00 继续。"""
        _setup_split_window(coordinator)

        coordinator.add_source_row(SOURCE_A, "2026-11-23T11:30:00", 7.0)
        receipt = coordinator.submit_end_receipt(
            SOURCE_A, WINDOW_START, WINDOW_END, row_count=1,
            receipt_code="RCPT-A-1100-FIX",
        )
        assert receipt["ok"] is True
        assert receipt["idle_gap_confirmation"] is None  # 有数据，不生成空窗

        fixed = coordinator.get_window(SOURCE_A, WINDOW_START, WINDOW_END)
        assert fixed["finished"] is True
        assert fixed["read_row_count"] == 1
        assert fixed["unread_position"] == ""

        # 下一轮窗口从 12:00 起；12:00 之前没有遗留未读位置。
        progress = coordinator.source_progress(
            SOURCE_A, default_position=NEXT_START)
        assert progress["unread_position"] == NEXT_START

        # B 仍只有空窗确认，不能凭零行给 B 补采。
        assert coordinator.list_confirmations(SOURCE_B)[0]["need_backfill"] is False

    def test_row_at_1201_excluded_from_confirmed_1100_window(
            self, coordinator: ReadProgressCoordinator) -> None:
        """12:01 金额 90 的行不属于已确认的 11:00~12:00 窗口。"""
        _setup_split_window(coordinator)
        coordinator.add_source_row(SOURCE_A, "2026-11-23T11:30:00", 7.0)
        coordinator.add_source_row(SOURCE_A, "2026-11-23T12:01:00", 90.0)
        coordinator.submit_end_receipt(
            SOURCE_A, WINDOW_START, WINDOW_END, row_count=1,
            receipt_code="RCPT-A-1100-FIX",
        )

        interval = coordinator.common_confirmed_interval(
            [SOURCE_A, SOURCE_B], WINDOW_START, WINDOW_END)
        assert interval["all_confirmed"] is True
        assert interval["confirmed_interval"] == [WINDOW_START, WINDOW_END]

        summary = coordinator.aggregate_cross_source(
            CROSS_TASK, [SOURCE_A, SOURCE_B], WINDOW_START, WINDOW_END)
        assert summary["ok"] is True
        # 融合明细只含 11:30 金额 7 一行；12:01 的 90 被排除，B 为空窗。
        assert summary["row_count"] == 1
        assert summary["total_amount"] == 7.0
        detail = summary["fusion_details"][0]
        assert detail["source"] == SOURCE_A
        assert detail["event_time"] == "2026-11-23T11:30:00"
        assert detail["amount"] == 7.0

    def test_delta_merge_selects_only_claimed_windows(
            self, coordinator: ReadProgressCoordinator) -> None:
        """增量归并用已领区间选择行：未认领窗口的行不参与。"""
        _setup_split_window(coordinator)
        coordinator.add_source_row(SOURCE_A, "2026-11-23T11:30:00", 7.0)
        coordinator.add_source_row(SOURCE_A, "2026-11-23T12:01:00", 90.0)
        coordinator.submit_end_receipt(
            SOURCE_A, WINDOW_START, WINDOW_END, row_count=1,
            receipt_code="RCPT-A-1100-FIX",
        )

        # 只认领 B 的 11~12 空窗，B 没有数据行。
        coordinator.claim_input(
            B_ONLY_TASK, SOURCE_B, WINDOW_START, WINDOW_END)
        b_delta = coordinator.select_delta_rows(B_ONLY_TASK)
        assert b_delta["row_count"] == 0
        assert b_delta["total_amount"] == 0

        # 跨源任务在双方确认后认领 11~12 窗口，只能选出金额 7 的行。
        claim_a = coordinator.claim_input(
            CROSS_TASK, SOURCE_A, WINDOW_START, WINDOW_END,
            cross_source=True, participant_sources=[SOURCE_A, SOURCE_B])
        claim_b = coordinator.claim_input(
            CROSS_TASK, SOURCE_B, WINDOW_START, WINDOW_END,
            cross_source=True, participant_sources=[SOURCE_A, SOURCE_B])
        assert claim_a["ok"] is True
        assert claim_b["ok"] is True

        cross_delta = coordinator.select_delta_rows(CROSS_TASK)
        assert cross_delta["row_count"] == 1
        assert cross_delta["total_amount"] == 7.0
        assert cross_delta["selected_rows"][0]["amount"] == 7.0

    def test_cross_source_aggregation_blocked_before_both_confirmed(
            self, coordinator: ReadProgressCoordinator) -> None:
        """A 未确认时共同确认区间为空，跨源指标不能汇总。"""
        _setup_split_window(coordinator)
        summary = coordinator.aggregate_cross_source(
            CROSS_TASK, [SOURCE_A, SOURCE_B], WINDOW_START, WINDOW_END)
        assert summary["ok"] is False
        assert summary["row_count"] == 0
        assert summary["total_amount"] == 0
        assert summary["interval"]["all_confirmed"] is False
        assert summary["interval"]["per_source"][SOURCE_A]["confirmed"] is False
        assert summary["interval"]["per_source"][SOURCE_B]["confirmed"] is True

    def test_renewal_conflict_clears_after_a_repaired(
            self, coordinator: ReadProgressCoordinator) -> None:
        """A 补齐结束回执后续办不再冲突，两个来源都排入下一时段。"""
        _setup_split_window(coordinator)
        coordinator.add_source_row(SOURCE_A, "2026-11-23T11:30:00", 7.0)
        coordinator.submit_end_receipt(
            SOURCE_A, WINDOW_START, WINDOW_END, row_count=1,
            receipt_code="RCPT-A-1100-FIX",
        )
        renewal = coordinator.arrange_renewal(
            CROSS_TASK, WINDOW_START, WINDOW_END,
            participant_sources=[SOURCE_A, SOURCE_B],
            next_window_start=NEXT_START, next_window_end=NEXT_END,
        )
        assert renewal["conflict"] is False
        schedule = renewal["renewal"]["source_schedule"]
        assert schedule[SOURCE_A]["next_window_scheduled"] is True
        assert schedule[SOURCE_B]["next_window_scheduled"] is True


class TestCoordinatorSnapshot:
    """协调器领域快照。"""

    def test_domain_snapshot_counts(self,
                                    coordinator: ReadProgressCoordinator) -> None:
        _setup_split_window(coordinator)
        coordinator.claim_input(
            CROSS_TASK, SOURCE_A, WINDOW_START, WINDOW_END,
            cross_source=True, participant_sources=[SOURCE_A, SOURCE_B])
        coordinator.arrange_renewal(
            CROSS_TASK, WINDOW_START, WINDOW_END,
            participant_sources=[SOURCE_A, SOURCE_B],
            next_window_start=NEXT_START, next_window_end=NEXT_END)

        snapshot = coordinator.domain_snapshot()
        assert snapshot["module"] == "read_progress"
        assert snapshot["windows"] == 2
        assert snapshot["interrupted_windows"] == 1
        assert snapshot["idle_gap_confirmations"] == 1
        assert snapshot["waiting_claims"] == 1
        assert snapshot["renewal_conflicts"] == 1
