"""读取进度协调：空窗确认、结束回执、分源认领与跨源区间汇总。

源批次通常只带行数，既无法判断“零行”是真的没有数据还是读取中断，
也无法判断一段窗口是否已经读完。本模块引入三类领域对象解决该问题：

- :class:`ReadWindow`：单个来源、单个时间窗的读取状态，区分
  「读取中断（未读完、无结束回执）」与「已结束（有结束回执）」。
- :class:`IdleGapConfirmation`：空窗确认记录。零行窗口只有在收到与
  来源、起止时刻一致的结束回执后才成立，成立后无需补采。
- :class:`TaskClaim` / :class:`RenewalRecord`：跨源任务可分源领取输入，
  每次认领都关联到具体的空窗确认记录；续办时须说明是否已为各来源排入
  下一时段。

规则要点：

1. 结束回执按“来源 + 起止时刻”核对，别的来源或别的时间的回执不能替代。
2. 零行 + 无结束回执 ⇒ 读取中断，不能当成空窗，也不能补采。
3. 跨源任务的指标只能从两边都已确认的区间（共同确认区间）汇总。
4. 增量归并用已认领区间选择行，未认领/未读完区间的行不参与。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.store import store


# ---------------------------------------------------------------------------
# 领域对象
# ---------------------------------------------------------------------------


@dataclass
class IdleGapConfirmation:
    """空窗确认记录：用来源和起止时刻核对结束回执后生成。"""

    confirm_id: int
    confirm_code: str
    source: str
    window_start: str
    window_end: str
    row_count: int
    end_receipt_code: str
    confirmed_rows: list[dict[str, Any]] = field(default_factory=list)
    status: str = "已确认"
    need_backfill: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "confirm_id": self.confirm_id,
            "confirm_code": self.confirm_code,
            "source": self.source,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "row_count": self.row_count,
            "end_receipt_code": self.end_receipt_code,
            "confirmed_rows": [dict(r) for r in self.confirmed_rows],
            "status": self.status,
            "need_backfill": self.need_backfill,
        }


@dataclass
class ReadWindow:
    """来源读取窗口：记录一段时间窗的读取进度与结束回执。"""

    source: str
    window_start: str
    window_end: str
    expected_row_count: int = 0
    read_row_count: int = 0
    finished: bool = False
    end_receipt_code: str = ""
    rows: list[dict[str, Any]] = field(default_factory=list)
    # 未读完时指出尚未读取的位置；读完后清空。
    unread_position: str = ""

    @property
    def is_idle_gap(self) -> bool:
        """已结束且零行 ⇒ 有效空窗。"""
        return self.finished and self.read_row_count == 0

    @property
    def is_interrupted(self) -> bool:
        """无结束回执 ⇒ 读取中断（未读完），零行也不能算空窗。"""
        return not self.finished

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "expected_row_count": self.expected_row_count,
            "read_row_count": self.read_row_count,
            "finished": self.finished,
            "end_receipt_code": self.end_receipt_code,
            "unread_position": self.unread_position,
            "is_idle_gap": self.is_idle_gap,
            "is_interrupted": self.is_interrupted,
            "rows": [dict(r) for r in self.rows],
        }


@dataclass
class TaskClaim:
    """任务认领记录：跨源任务按来源分别领取输入。"""

    claim_id: int
    claim_code: str
    task_code: str
    cross_source: bool
    source: str
    window_start: str
    window_end: str
    participant_sources: list[str] = field(default_factory=list)
    status: str = "已认领"
    wait_reason: str = ""
    # 关联的空窗确认记录编号；非空窗窗口为空列表。
    related_confirmation_codes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "claim_code": self.claim_code,
            "task_code": self.task_code,
            "cross_source": self.cross_source,
            "source": self.source,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "participant_sources": list(self.participant_sources),
            "status": self.status,
            "wait_reason": self.wait_reason,
            "related_confirmation_codes": list(self.related_confirmation_codes),
        }


@dataclass
class RenewalRecord:
    """跨源续办安排：说明每个来源是否已排入下一时段。"""

    renewal_id: int
    renewal_code: str
    task_code: str
    window_start: str
    window_end: str
    next_window_start: str
    next_window_end: str
    conflict: bool
    conflict_reason: str = ""
    source_schedule: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "renewal_id": self.renewal_id,
            "renewal_code": self.renewal_code,
            "task_code": self.task_code,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "next_window_start": self.next_window_start,
            "next_window_end": self.next_window_end,
            "conflict": self.conflict,
            "conflict_reason": self.conflict_reason,
            "source_schedule": {k: dict(v) for k, v in self.source_schedule.items()},
        }


# ---------------------------------------------------------------------------
# 读取进度协调器
# ---------------------------------------------------------------------------


class ReadProgressCoordinator:
    """协调多来源的读取确认、认领、续办与跨源汇总。"""

    def __init__(self) -> None:
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._windows: dict[tuple[str, str, str], ReadWindow] = {}
        self._confirmations: dict[int, IdleGapConfirmation] = {}
        self._claims: dict[int, TaskClaim] = {}
        self._renewals: dict[int, RenewalRecord] = {}
        # 与数据仓库中 read_progress 表的既有主键对齐，避免与示例数据撞号。
        existing_ids = (
            int(row.get("id", 0)) for row in store.rows("read_progress")
        )
        self._next_confirmation_id = max(existing_ids, default=0) + 1
        self._next_claim_id = 1
        self._next_renewal_id = 1

    # ---- 基础数据录入 ----------------------------------------------------

    def add_source_row(self, source: str, event_time: str, amount: float,
                       **extra: Any) -> dict[str, Any]:
        """登记一条来源数据（事件时刻用于窗口归属判定）。"""
        row = {"source": source, "event_time": event_time,
               "amount": amount, **extra}
        self._rows.setdefault(source, []).append(row)
        return dict(row)

    def mark_read_interrupted(self, source: str, window_start: str,
                              window_end: str,
                              read_row_count: int = 0) -> dict[str, Any]:
        """登记一次读取中断：没有结束回执，窗口处于未读完状态。

        未读完时进度要指出尚未读取的位置（本窗口起点）。
        """
        window = self._get_or_create_window(source, window_start, window_end)
        window.finished = False
        window.end_receipt_code = ""
        window.read_row_count = read_row_count
        window.unread_position = window_start
        return window.to_dict()

    def submit_end_receipt(self, source: str, window_start: str, window_end: str,
                           row_count: int, receipt_code: str) -> dict[str, Any]:
        """提交结束回执并按来源、起止时刻核对。

        - 窗口内实际行数必须与回执行数一致，否则拒收。
        - 零行回执在核对通过后生成空窗确认，标记为无需补采。
        - 非零行回执正常结束窗口，已读位置推进到窗口终点。
        别的来源或别的时间的回执无法替它确认（键中含来源与起止时刻）。
        """
        window = self._get_or_create_window(source, window_start, window_end)
        actual_rows = [
            r for r in self._rows.get(source, [])
            if window_start <= r["event_time"] < window_end
        ]
        if row_count != len(actual_rows):
            return {
                "ok": False,
                "message": (
                    f"结束回执 {receipt_code} 行数 {row_count} 与来源 {source} "
                    f"窗口 {window_start}~{window_end} 实际行数 {len(actual_rows)} 不一致，拒绝确认"
                ),
            }

        window.finished = True
        window.end_receipt_code = receipt_code
        window.read_row_count = row_count
        window.rows = actual_rows
        window.unread_position = ""

        result: dict[str, Any] = {
            "ok": True,
            "window": window.to_dict(),
        }

        if row_count == 0:
            confirmation = self._create_idle_gap_confirmation(window, receipt_code)
            result["idle_gap_confirmation"] = confirmation.to_dict()
        else:
            result["idle_gap_confirmation"] = None
        return result

    def _create_idle_gap_confirmation(self, window: ReadWindow,
                                      receipt_code: str) -> IdleGapConfirmation:
        cid = self._next_confirmation_id
        self._next_confirmation_id += 1
        confirmation = IdleGapConfirmation(
            confirm_id=cid,
            confirm_code=f"IDLE-{cid:04d}",
            source=window.source,
            window_start=window.window_start,
            window_end=window.window_end,
            row_count=0,
            end_receipt_code=receipt_code,
            confirmed_rows=[],
            status="已确认",
            need_backfill=False,
        )
        self._confirmations[cid] = confirmation
        # 同步到内存数据仓库，供统一资源接口与前端工作台查看。
        store.rows("read_progress").append({
            "id": cid,
            "status": "已确认",
            "pending": False,
            "abnormal": False,
            "确认编号": confirmation.confirm_code,
            "数据源": confirmation.source,
            "窗口起始": confirmation.window_start,
            "窗口截止": confirmation.window_end,
            "回执行数": "0",
            "结束回执": confirmation.end_receipt_code,
            "是否空窗": "是",
            "补采要求": "无需补采",
        })
        return confirmation

    # ---- 分源认领 --------------------------------------------------------

    def claim_input(self, task_code: str, source: str,
                    window_start: str, window_end: str,
                    cross_source: bool = False,
                    participant_sources: list[str] | None = None) -> dict[str, Any]:
        """任务按来源认领输入区间。

        跨源任务（``cross_source=True``）只能在所有参与来源对该窗口都已
        确认（有结束回执或空窗确认）时领取，否则保持等待并给出原因；
        单源作业只看本来源的确认状态。每次认领都会关联命中的空窗确认。
        """
        participants = participant_sources or [source]
        related = self._confirmation_codes(source, window_start, window_end)

        if cross_source:
            missing = [
                s for s in participants
                if not self._is_window_confirmed(s, window_start, window_end)
            ]
            if missing:
                claim = self._record_claim(
                    task_code=task_code, cross_source=True, source=source,
                    window_start=window_start, window_end=window_end,
                    participants=participants, status="等待中",
                    wait_reason=(
                        f"参与来源 {','.join(missing)} 尚未确认窗口 "
                        f"{window_start}~{window_end}（无结束回执或读取中断）"
                    ),
                    related=related,
                )
                return {"ok": False, "status": "等待中",
                        "wait_reason": claim.wait_reason, "claim": claim.to_dict()}

        claim = self._record_claim(
            task_code=task_code, cross_source=cross_source, source=source,
            window_start=window_start, window_end=window_end,
            participants=participants, status="已认领",
            wait_reason="", related=related,
        )
        return {"ok": True, "status": "已认领",
                "wait_reason": "", "claim": claim.to_dict()}

    def _record_claim(self, *, task_code: str, cross_source: bool, source: str,
                      window_start: str, window_end: str,
                      participants: list[str], status: str, wait_reason: str,
                      related: list[str]) -> TaskClaim:
        cid = self._next_claim_id
        self._next_claim_id += 1
        claim = TaskClaim(
            claim_id=cid,
            claim_code=f"CLM-{cid:04d}",
            task_code=task_code,
            cross_source=cross_source,
            source=source,
            window_start=window_start,
            window_end=window_end,
            participant_sources=participants,
            status=status,
            wait_reason=wait_reason,
            related_confirmation_codes=related,
        )
        self._claims[cid] = claim
        return claim

    # ---- 跨源续办 --------------------------------------------------------

    def arrange_renewal(self, task_code: str, window_start: str, window_end: str,
                        participant_sources: list[str],
                        next_window_start: str, next_window_end: str) -> dict[str, Any]:
        """跨源任务续办：核对各来源当前窗口状态并安排下一时段。

        当某来源的有效空窗与另一来源未读完状态同时出现时发生冲突，
        续办安排须逐来源说明此时是否已为它排入下一时段：

        - 已确认空窗的来源：无需补采，直接排入下一时段。
        - 读取中断、缺少结束证据的来源：不排入下一时段，先从
          ``unread_position`` 续读当前窗口。
        """
        schedule: dict[str, dict[str, Any]] = {}
        conflict_sources: list[str] = []
        for source in participant_sources:
            window = self._windows.get((source, window_start, window_end))
            confirmation = self._find_confirmation(source, window_start, window_end)
            if confirmation is not None:
                schedule[source] = {
                    "state": "有效空窗",
                    "need_backfill": False,
                    "next_window_scheduled": True,
                    "next_window": f"{next_window_start}~{next_window_end}",
                    "confirmation_code": confirmation.confirm_code,
                    "unread_position": "",
                }
            elif window is not None and window.is_interrupted:
                conflict_sources.append(source)
                schedule[source] = {
                    "state": "读取中断",
                    "need_backfill": False,
                    "next_window_scheduled": False,
                    "next_window": "",
                    "confirmation_code": "",
                    "unread_position": window.unread_position or window_start,
                }
            elif window is not None and window.finished:
                schedule[source] = {
                    "state": "已结束有数据",
                    "need_backfill": False,
                    "next_window_scheduled": True,
                    "next_window": f"{next_window_start}~{next_window_end}",
                    "confirmation_code": "",
                    "unread_position": "",
                }
            else:
                conflict_sources.append(source)
                schedule[source] = {
                    "state": "无读取记录",
                    "need_backfill": False,
                    "next_window_scheduled": False,
                    "next_window": "",
                    "confirmation_code": "",
                    "unread_position": window_start,
                }

        has_idle = any(item["state"] == "有效空窗" for item in schedule.values())
        conflict = has_idle and len(conflict_sources) > 0
        reason = ""
        if conflict:
            reason = (
                f"有效空窗来源与未读完来源 {','.join(conflict_sources)} 并存："
                "空窗来源无需补采可排入下一时段，未读完来源须先按未读位置续读"
            )

        rid = self._next_renewal_id
        self._next_renewal_id += 1
        renewal = RenewalRecord(
            renewal_id=rid,
            renewal_code=f"RNW-{rid:04d}",
            task_code=task_code,
            window_start=window_start,
            window_end=window_end,
            next_window_start=next_window_start,
            next_window_end=next_window_end,
            conflict=conflict,
            conflict_reason=reason,
            source_schedule=schedule,
        )
        self._renewals[rid] = renewal
        return {"ok": True, "conflict": conflict,
                "conflict_reason": reason, "renewal": renewal.to_dict()}

    # ---- 进度与补采查询 --------------------------------------------------

    def source_progress(self, source: str, default_position: str = "") -> dict[str, Any]:
        """返回来源进度：指出尚未读取的位置与等待原因。"""
        unfinished = [
            w for w in self._windows.values()
            if w.source == source and w.is_interrupted
        ]
        if unfinished:
            earliest = min(unfinished, key=lambda w: w.window_start)
            return {
                "source": source,
                "state": "读取中断",
                "unread_position": earliest.unread_position or earliest.window_start,
                "window": f"{earliest.window_start}~{earliest.window_end}",
                "reason": "缺少结束回执，尚未读完",
            }
        # 没有任何窗口记录时，若已有数据行则从最早事件时刻起算未读。
        rows = self._rows.get(source, [])
        if rows and default_position == "":
            default_position = min(r["event_time"] for r in rows)
        if default_position:
            return {
                "source": source,
                "state": "未开始读取",
                "unread_position": default_position,
                "window": "",
                "reason": "尚无读取窗口记录",
            }
        return {
            "source": source,
            "state": "无待读数据",
            "unread_position": "",
            "window": "",
            "reason": "",
        }

    def needs_backfill(self, source: str, window_start: str,
                       window_end: str) -> dict[str, Any]:
        """空数据窗口已确认则无需补采；未读完不能补采，须续读。"""
        confirmation = self._find_confirmation(source, window_start, window_end)
        if confirmation is not None:
            return {
                "source": source,
                "window": f"{window_start}~{window_end}",
                "need_backfill": False,
                "reason": f"空窗已由 {confirmation.confirm_code} 确认，无需补采",
            }
        window = self._windows.get((source, window_start, window_end))
        if window is not None and window.is_interrupted:
            return {
                "source": source,
                "window": f"{window_start}~{window_end}",
                "need_backfill": False,
                "reason": "读取中断且无结束回执，不能按零行补采，须从未读位置续读",
                "unread_position": window.unread_position or window_start,
            }
        return {
            "source": source,
            "window": f"{window_start}~{window_end}",
            "need_backfill": False,
            "reason": "无已确认空窗，也无中断记录",
        }

    # ---- 增量归并与跨源汇总 ----------------------------------------------

    def select_delta_rows(self, task_code: str,
                          sources: list[str] | None = None) -> dict[str, Any]:
        """增量归并：只用该任务已认领（且窗口已确认）的区间选择行。"""
        claims = [
            c for c in self._claims.values()
            if c.task_code == task_code and c.status == "已认领"
            and (sources is None or c.source in sources)
        ]
        selected: list[dict[str, Any]] = []
        for claim in claims:
            for row in self._rows.get(claim.source, []):
                if claim.window_start <= row["event_time"] < claim.window_end:
                    selected.append(dict(row))
        # 同一行可能被重复认领，按 来源+事件时刻 去重。
        deduped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in selected:
            deduped[(row["source"], row["event_time"])] = row
        rows = list(deduped.values())
        return {
            "ok": True,
            "task_code": task_code,
            "claimed_windows": [c.to_dict() for c in claims],
            "selected_rows": rows,
            "row_count": len(rows),
            "total_amount": sum(float(r["amount"]) for r in rows),
        }

    def common_confirmed_interval(self, sources: list[str],
                                  window_start: str, window_end: str) -> dict[str, Any]:
        """计算多来源对同一窗口的共同确认状态与区间。"""
        per_source: dict[str, dict[str, Any]] = {}
        all_confirmed = True
        for source in sources:
            window = self._windows.get((source, window_start, window_end))
            confirmation = self._find_confirmation(source, window_start, window_end)
            confirmed = window is not None and window.finished
            per_source[source] = {
                "confirmed": confirmed,
                "is_idle_gap": confirmation is not None,
                "confirmation_code": confirmation.confirm_code if confirmation else "",
                "end_receipt_code": window.end_receipt_code if window else "",
                "unread_position": (
                    window.unread_position if window is not None and not confirmed else ""
                ),
            }
            all_confirmed = all_confirmed and confirmed
        return {
            "sources": list(sources),
            "window_start": window_start,
            "window_end": window_end,
            "all_confirmed": all_confirmed,
            # 全部确认时共同确认区间即请求窗口；否则为空。
            "confirmed_interval": (
                [window_start, window_end] if all_confirmed else []
            ),
            "per_source": per_source,
        }

    def aggregate_cross_source(self, task_code: str, sources: list[str],
                               window_start: str, window_end: str,
                               amount_field: str = "amount") -> dict[str, Any]:
        """跨源任务结果只从双方共同确认的范围汇总。"""
        interval = self.common_confirmed_interval(sources, window_start, window_end)
        if not interval["all_confirmed"]:
            return {
                "ok": False,
                "message": "存在未确认来源，共同确认区间为空，不能汇总跨源指标",
                "interval": interval,
                "fusion_details": [],
                "row_count": 0,
                "total_amount": 0,
            }
        details: list[dict[str, Any]] = []
        for source in sources:
            for row in self._rows.get(source, []):
                if window_start <= row["event_time"] < window_end:
                    details.append(dict(row))
        details.sort(key=lambda r: (r["event_time"], r["source"]))
        total = sum(float(r[amount_field]) for r in details)
        return {
            "ok": True,
            "message": "共同确认区间汇总完成",
            "interval": interval,
            "fusion_details": details,
            "row_count": len(details),
            "total_amount": total,
        }

    # ---- 查询辅助 --------------------------------------------------------

    def get_window(self, source: str, window_start: str,
                   window_end: str) -> dict[str, Any] | None:
        window = self._windows.get((source, window_start, window_end))
        return window.to_dict() if window else None

    def list_confirmations(self, source: str | None = None) -> list[dict[str, Any]]:
        items = self._confirmations.values()
        if source is not None:
            items = [c for c in items if c.source == source]
        return [c.to_dict() for c in items]

    def list_claims(self, task_code: str | None = None) -> list[dict[str, Any]]:
        items = self._claims.values()
        if task_code is not None:
            items = [c for c in items if c.task_code == task_code]
        return [c.to_dict() for c in items]

    def list_renewals(self, task_code: str | None = None) -> list[dict[str, Any]]:
        items = self._renewals.values()
        if task_code is not None:
            items = [r for r in items if r.task_code == task_code]
        return [r.to_dict() for r in items]

    # ---- 内部辅助 --------------------------------------------------------

    def _get_or_create_window(self, source: str, window_start: str,
                              window_end: str) -> ReadWindow:
        key = (source, window_start, window_end)
        window = self._windows.get(key)
        if window is None:
            window = ReadWindow(source=source, window_start=window_start,
                                window_end=window_end)
            self._windows[key] = window
        return window

    def _is_window_confirmed(self, source: str, window_start: str,
                             window_end: str) -> bool:
        window = self._windows.get((source, window_start, window_end))
        return window is not None and window.finished

    def _find_confirmation(self, source: str, window_start: str,
                           window_end: str) -> IdleGapConfirmation | None:
        for confirmation in self._confirmations.values():
            if (confirmation.source == source
                    and confirmation.window_start == window_start
                    and confirmation.window_end == window_end):
                return confirmation
        return None

    def _confirmation_codes(self, source: str, window_start: str,
                            window_end: str) -> list[str]:
        confirmation = self._find_confirmation(source, window_start, window_end)
        return [confirmation.confirm_code] if confirmation else []

    def domain_snapshot(self) -> dict[str, Any]:
        """模块级数据质量快照。"""
        windows = list(self._windows.values())
        return {
            "module": "read_progress",
            "entity": "空窗确认",
            "windows": len(windows),
            "interrupted_windows": sum(1 for w in windows if w.is_interrupted),
            "idle_gap_confirmations": len(self._confirmations),
            "claims": len(self._claims),
            "waiting_claims": sum(
                1 for c in self._claims.values() if c.status == "等待中"),
            "renewals": len(self._renewals),
            "renewal_conflicts": sum(
                1 for r in self._renewals.values() if r.conflict),
        }


# 全局单例，供测试与跨模块消费
read_progress_coordinator = ReadProgressCoordinator()
