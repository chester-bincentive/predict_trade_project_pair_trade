from __future__ import annotations

import asyncio
import json
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal, ROUND_UP
from typing import Any, AsyncIterator, Callable

from .config import Settings
from .live_execution import (
    LiveBalanceAllowanceRejected,
    LiveFakNoFill,
    LiveOrderUnknown,
    PolymarketLiveExecutor,
)
from .models import Book, Market, PairSnapshot
from .storage import Store
from .strategy import RollingCalibration, event_bucket, mid_spread


_LIVE_ALERT_POLICIES: dict[str, tuple[str, str, str]] = {
    "LIVE_ORDER_UNKNOWN": (
        "order_outcome_unknown", "Polymarket 下單結果不明",
        "為避免重複下單，該市場後續交易已暫停；請核對錢包訂單與成交，市場到期後新市場會恢復。",
    ),
    "LIVE_EXIT_BELOW_MINIMUM": (
        "position_below_sell_minimum", "持倉低於市場最小賣出量",
        "目前持倉無法送出有效賣單，系統保留部位到市場結束；請核對錢包持倉。",
    ),
    "LIVE_SELL_BALANCE_RETRY": (
        "sell_balance_allowance_rejected", "賣單被餘額／授權拒絕",
        "程式會持續重試賣出；請檢查 token 餘額與 allowance。",
    ),
    "LIVE_MARKET_VERIFY_FAILED": (
        "market_verification_failed", "Polymarket 市場／Token 驗證失敗",
        "此次委託未送出；請檢查市場 condition、token 對應與 SDK metadata。",
    ),
    "LIVE_BLOCKED_NON_POLYMARKET": (
        "non_polymarket_live_order_blocked", "Live 訂單被非 Polymarket 腿安全檢查阻擋",
        "該交易已進入安全鎖定；請檢查選腿與交易狀態。",
    ),
    "LIVE_EXIT_NO_BID": (
        "exit_book_has_no_bid", "退出簿沒有可用 Bid",
        "目前無法成交賣出，系統會等後續行情再重試。",
    ),
    "LIVE_EXIT_BOOK_UNAVAILABLE": (
        "exit_book_unavailable", "退出時沒有可用 Polymarket 行情簿",
        "賣出尚未完成，系統會等行情簿恢復後再重試。",
    ),
    "LIVE_EXIT_BOOK_STALE": (
        "exit_book_stale", "退出用 Polymarket 行情簿已過期",
        "目前不會用過期報價送賣單，系統會等新行情再重試。",
    ),
    "LIVE_ENTRY_SIZE_BLOCKED": (
        "entry_below_market_minimum", "進場股數低於市場最小下單量",
        "本次訊號未送出買單；請檢查市場最小量與設定股數。",
    ),
    "LIVE_ENTRY_AMOUNT_INVALID": (
        "entry_amount_invalid", "買入名目金額無效",
        "本次訊號未送出買單；請檢查 best ask 與下單金額計算。",
    ),
}

# These statuses remain in the ledger, but intentionally do not generate Discord
# alerts. Any order lock they cause is handled independently by the live engine.
for _status in (
    "LIVE_ORDER_UNKNOWN",
    "LIVE_EXIT_BELOW_MINIMUM",
    "LIVE_SELL_BALANCE_RETRY",
    "LIVE_MARKET_VERIFY_FAILED",
    "LIVE_EXIT_NO_BID",
    "LIVE_EXIT_BOOK_UNAVAILABLE",
    "LIVE_EXIT_BOOK_STALE",
):
    _LIVE_ALERT_POLICIES.pop(_status, None)


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def _partial_sweep(book: Book, *, side: str, shares: float, fee_coefficient: float,
                   max_price: float | None = None) -> tuple[float, float, float]:
    levels = book.asks if side == "BUY" else book.bids
    remaining = max(0.0, shares)
    filled = notional = fee = 0.0
    for level in levels:
        if side == "BUY" and max_price is not None and level.price > max_price + 1e-12:
            break
        amount = min(remaining, level.size)
        if amount <= 0:
            continue
        filled += amount
        notional += amount * level.price
        fee += amount * fee_coefficient * level.price * (1.0 - level.price)
        remaining -= amount
        if remaining <= 1e-9:
            break
    return filled, notional, fee


def _live_buy_limit_price(
    best_ask: float,
    tick_size: Decimal | str,
    offset_ticks: int,
) -> Decimal | None:
    """Return the exact ask-plus-ticks BUY limit when it remains on the legal tick grid."""
    ask = Decimal(str(best_ask))
    tick = Decimal(str(tick_size))
    if (
        not ask.is_finite() or not tick.is_finite()
        or not Decimal("0") < ask < 1 or not Decimal("0") < tick < 1
        or offset_ticks < 1
    ):
        return None
    max_price_level = int((Decimal("1") - tick) // tick)
    ask_level = int((ask / tick).to_integral_value(rounding=ROUND_UP))
    price_level = ask_level + offset_ticks
    if price_level > max_price_level:
        return None
    return tick * price_level

class ShadowEngine:
    """Single-leg paper runner with an explicitly armed Polymarket live path."""

    def __init__(self, store: Store, settings: Settings, *, run_id: str | None = None,
                 live_executor: PolymarketLiveExecutor | None = None) -> None:
        self.store = store
        self.settings = settings
        self.run_id = run_id
        self.live_executor = live_executor
        self._risk_alert_callback: Callable[[dict[str, Any]], None] | None = None
        self.calibration = RollingCalibration(
            store, window=settings.history_window_markets, rv_window=settings.rv_history_window_markets,
            bucket_seconds=settings.bucket_seconds
        )
        now = time.time()
        open_trades = store.open_shadow_trades()
        if live_executor is not None:
            for state in open_trades:
                for leg in state.get("legs", []):
                    for order in (
                        leg.get("entry_order") or {}, leg.get("exit_order") or {},
                        leg.get("unwind_order") or {},
                    ):
                        intent_key = str(order.get("live_order_intent_key") or "")
                        if not (intent_key and order.get("live_order_intent_started")
                                and order.get("live_api_started")
                                and not order.get("live_result_recorded")):
                            continue
                        live_executor.register_user_order_intent(
                            intent_key,
                            asset_id=str(order.get("live_order_asset_id") or leg.get("token_id") or ""),
                            condition_id=str(order.get("live_order_condition_id") or ""),
                            side=str(order.get("live_order_side") or ""),
                            requested_qty=float(order.get("live_order_requested")
                                                or order.get("requested", 0.0)),
                            price=order.get("live_order_intent_price"),
                            intent_epoch=float(order.get("live_intent_epoch") or now),
                        )
        for state in open_trades:
            if (state.get("trading_mode") == "live"
                    and (state.get("live_position_unresolved")
                         or state.get("status") == "UNRESOLVED_LIVE_POSITION")):
                state["status"] = "SETTLEMENT_PENDING"
                state.pop("live_position_unresolved", None)
                state["settlement_pending_reason"] = "remainder_below_minimum_sell_size"
                state["closed_at"] = datetime.now(timezone.utc).isoformat()
                store.update_shadow_trade(state)
        self.active: dict[str, Any] | None = next((
            state for state in open_trades
            if not state.get("live_order_quarantined")
            and state.get("status") != "SETTLEMENT_PENDING"
            and not (
                (state.get("live_order_unknown") or self._has_pending_live_entry(state))
                and float(state.get("market_end", float("inf"))) <= now
            )
        ), None)
        self.rng = random.SystemRandom()
        self.current_book_provider: Callable[[str, str], Book | None] | None = None
        self._process_lock = asyncio.Lock()
        self._live_exit_tasks: dict[int, asyncio.Task[None]] = {}
        self._live_entry_tasks: dict[int, asyncio.Task[None]] = {}
        self._live_gtc_discovery_tasks: dict[int, asyncio.Task[None]] = {}
        self._live_exit_inflight_trade_ids: set[int] = set()
        self._live_fak_reconcile_tasks: dict[str, asyncio.Task[None]] = {}
        self._live_presign_tasks: set[asyncio.Task[None]] = set()
        if live_executor is not None and self.active is not None:
            if self.active.get("trading_mode") != "live":
                raise ValueError("live mode will not resume an open paper/shadow trade")
            if any(leg.get("venue") != "polymarket" for leg in self.active.get("legs", [])):
                raise ValueError("live mode will not resume a non-Polymarket position")

    def set_risk_alert_callback(self, callback: Callable[[dict[str, Any]], None]) -> None:
        self._risk_alert_callback = callback

    def _schedule_live_risk_alert(
        self, state: dict[str, Any], status: str, *, phase: str,
        side: str | None = None,
    ) -> None:
        policy = _LIVE_ALERT_POLICIES.get(status)
        callback = self._risk_alert_callback
        if policy is None or callback is None:
            return
        alert_code, title, action = policy
        alert = {
            "dedupe_key": f"{state.get('trade_id', 'unknown')}:{alert_code}:{phase}",
            "alert_code": alert_code,
            "title": title,
            "market_key": state.get("market_key"),
            "phase": phase,
            "side": side,
            "signal": state.get("signal"),
            "action": action,
        }
        try:
            callback(alert)
        except Exception as exc:
            print(f"DISCORD RISK ALERT SCHEDULE ERROR ({type(exc).__name__})", flush=True)

    def _has_pending_live_entry(self, state: dict[str, Any]) -> bool:
        return any(
            bool((leg.get("entry_order") or {}).get("live_gtc_pending"))
            for leg in state.get("legs", [])
        )

    def _live_exit_worker_running(self, trade_id: int | None = None) -> bool:
        if trade_id is None:
            return any(not task.done() for task in self._live_exit_tasks.values())
        task = self._live_exit_tasks.get(int(trade_id))
        return task is not None and not task.done()

    def _live_exit_inflight(self, trade_id: int) -> bool:
        return int(trade_id) in self._live_exit_inflight_trade_ids

    def _mark_live_gtc_pending_at_market_end(
        self, state: dict[str, Any], now_epoch: float,
    ) -> None:
        """Keep an accepted GTC order unresolved until account reconciliation finishes."""
        state["status"] = "UNRESOLVED_LIVE_ORDER"
        state["live_order_unknown"] = True
        state["live_order_unknown_phase"] = "ENTRY"
        state["closed_at"] = None
        state["live_gtc_expired_at"] = float(now_epoch)
        for leg in state.get("legs", []):
            order = leg.get("entry_order") or {}
            if not order.get("live_gtc_pending"):
                continue
            order["live_gtc_next_poll_epoch"] = min(
                float(order.get("live_gtc_next_poll_epoch") or now_epoch),
                float(now_epoch),
            )
            if not order.get("live_gtc_expiry_recorded"):
                self._event(
                    state, "ENTRY", leg, side="BUY",
                    status="LIVE_GTC_PENDING_AT_MARKET_END",
                    requested=float(order.get("requested", state.get("requested_shares", 0.0))),
                    order=order,
                    details={"order_id": order.get("live_gtc_order_id"),
                             "reason": "market ended before GTC fill/cancel reconciliation completed",
                             "reconciliation_continues": True},
                )
                order["live_gtc_expiry_recorded"] = True
        self._persist_state(state)
        self.store.record_runtime_event(
            run_id=self.run_id or "live-entry-watchdog",
            event_type="live_gtc_pending_at_market_end",
            market_key=str(state.get("market_key")), venue="polymarket",
            details={"trade_id": int(state["trade_id"]),
                     "order_ids": [
                         (leg.get("entry_order") or {}).get("live_gtc_order_id")
                         for leg in state.get("legs", [])
                         if (leg.get("entry_order") or {}).get("live_gtc_pending")
                     ],
                     "market_end_epoch": state.get("market_end")},
        )
        if self.active is state:
            self.active = None

    def process(self, snapshot: PairSnapshot, *, evaluate_signals: bool = True) -> str:
        """Synchronous compatibility path for paper callers and the demo."""
        if self.live_executor is not None:
            raise RuntimeError("use await process_async() when live execution is enabled")
        return asyncio.run(self.process_async(snapshot, evaluate_signals=evaluate_signals))

    @asynccontextmanager
    async def _timed_process_lock(
        self, operation: str, *, market_key: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> AsyncIterator[None]:
        queued_at = time.monotonic()
        await self._process_lock.acquire()
        acquired_at = time.monotonic()
        outcome = "completed"
        error_type = None
        try:
            yield
        except BaseException as exc:
            outcome = "raised"
            error_type = type(exc).__name__
            raise
        finally:
            released_at = time.monotonic()
            wait_ms = (acquired_at - queued_at) * 1000.0
            hold_ms = (released_at - acquired_at) * 1000.0
            self._process_lock.release()
            if wait_ms >= 100.0 or hold_ms >= 250.0:
                try:
                    self.store.record_runtime_event(
                        run_id=self.run_id or "process-lock-monitor",
                        event_type="process_lock_delay",
                        market_key=market_key,
                        venue="polymarket",
                        details={"operation": operation,
                                 "wait_ms": round(wait_ms, 3),
                                 "hold_ms": round(hold_ms, 3),
                                 "outcome": outcome,
                                 "error_type": error_type,
                                 **(details or {})},
                    )
                except Exception as exc:
                    print(f"PROCESS LOCK TIMING RECORD ERROR ({type(exc).__name__})", flush=True)

    async def process_async(self, snapshot: PairSnapshot, *, evaluate_signals: bool = True) -> str:
        async with self._timed_process_lock(
            "process_async", market_key=snapshot.market_key,
            details={"evaluate_signals": evaluate_signals},
        ):
            return await self._process_async_unlocked(
                snapshot, evaluate_signals=evaluate_signals,
            )

    async def _process_async_unlocked(
        self, snapshot: PairSnapshot, *, evaluate_signals: bool = True,
    ) -> str:
        now = snapshot.captured_at
        elapsed = (now - snapshot.start_at).total_seconds()
        bucket = event_bucket(elapsed, self.settings.bucket_seconds)
        spread = mid_spread(snapshot)
        if evaluate_signals:
            rv10 = self.calibration.observe(snapshot, spread, bucket)
        else:
            rv10 = None
        # Books stay in memory for signal evaluation and fills. Persist only
        # trade and runtime events, not the high-volume quote/depth snapshots.
        snapshot_ids: dict[tuple[str, str], int] = {}

        if self.active is not None and self.active.get("live_order_unknown"):
            if now.timestamp() >= float(self.active.get("market_end", float("inf"))):
                unknown_state = self.active
                if self._has_pending_live_entry(unknown_state):
                    self._mark_live_gtc_pending_at_market_end(unknown_state, now.timestamp())
                else:
                    unknown_state["status"] = "UNRESOLVED_LIVE_ORDER"
                    unknown_state["closed_at"] = None
                    self._event(
                        unknown_state, "TRADE", None, side=None,
                        status="LIVE_ORDER_DETACHED_AT_MARKET_END",
                        details={"new_trading_allowed": True,
                                 "market_expired": True,
                                 "account_reconciliation_pending": True,
                                 "live_order_unknown_phase": unknown_state.get("live_order_unknown_phase")},
                    )
                    self._persist_state(unknown_state)
                    self.active = None
            else:
                return (
                    "LIVE ORDER RESULT UNKNOWN; duplicate orders paused for this market; "
                    "new markets are not locked after expiry"
                )

        if self.active is not None:

            if self.active["market_key"] != snapshot.market_key:
                if now.timestamp() < float(self.active["market_end"]):
                    prefix = "LIVE" if self.live_executor is not None else "SHADOW"
                    return f"{prefix} OPEN trade {self.active['market_key']}; waiting for its matched market books"
                if self.live_executor is not None and self._has_pending_live_entry(self.active):
                    self._mark_live_gtc_pending_at_market_end(self.active, now.timestamp())
                elif self.live_executor is not None and float(self.active.get("paired_shares", 0.0)) > 1e-9:
                    expired_state = self.active
                    expired_state["status"] = "SETTLEMENT_PENDING"
                    expired_state["live_position_unresolved"] = False
                    expired_state["closed_at"] = now.isoformat()
                    self._event(expired_state, "TRADE", None, side=None,
                                status="SETTLEMENT_PENDING",
                                details={"position_shares": expired_state["paired_shares"],
                                         "settlement_status": "awaiting Polymarket market resolution",
                                         "new_trading_allowed": True})
                    self._persist()
                    self.active = None
                    self._settle_known_resolution(expired_state)
                else:
                    self.active["status"] = "UNRESOLVED_AT_END"
                    self.active["closed_at"] = now.isoformat()
                    unresolved_details = {"net_pnl": None}
                    unresolved_details[
                        "position_shares" if self.active.get("execution_mode") == "polymarket_only"
                        else "paired_shares"
                    ] = self.active.get("paired_shares", 0)
                    self._event(self.active, "TRADE", None, side=None,
                                status="UNRESOLVED_AT_END",
                                details=unresolved_details)
                    self._persist()
                    self.active = None
            else:
                result = await self._advance(snapshot, snapshot_ids, bucket, spread)
                if result:
                    return result

        if not evaluate_signals:
            return "ARRIVAL CHECK complete; signal evaluation skipped"

        stale = [
            f"{venue}/{outcome}={book.age_seconds(snapshot.captured_at):.2f}s"
            for (venue, outcome), book in snapshot.books.items()
            if book.age_seconds(snapshot.captured_at) > self.settings.max_quote_age_seconds
        ]
        if stale:
            return "SKIP stale quotes: " + ", ".join(stale)
        if spread is None:
            return "SKIP missing best bid/ask for UP midpoint"
        if elapsed >= 300.0:
            return "SKIP market has ended"
        if (snapshot.end_at - now).total_seconds() <= self.settings.min_market_time_left_seconds:
            return ("SKIP market is in its final "
                    f"{self.settings.min_market_time_left_seconds}s; no new position")

        counts = self.calibration.counts(before_market_start=snapshot.start_at.timestamp())
        missing = [b for b, count in counts.items() if count < self.settings.min_history_markets]
        if missing:
            progress = ", ".join(f"b{b}:{counts[b]}/{self.settings.min_history_markets}" for b in missing)
            return f"DATA COLLECTION ONLY; insufficient closed-market history ({progress}); signals disabled"

        stats = self.calibration.stats(bucket, before_market_start=snapshot.start_at.timestamp())
        zscore = stats.score(spread)
        if zscore is None:
            return f"SKIP bucket={bucket} spread stdev is zero/undefined"
        ou_stats = self.calibration.ou_stats(
            bucket, before_market_start=snapshot.start_at.timestamp(),
            window=self.settings.history_window_markets,
        )
        if ou_stats.count < self.settings.min_ou_history_markets:
            return (f"DATA COLLECTION ONLY; bucket={bucket} has {ou_stats.count}/"
                    f"{self.settings.min_ou_history_markets} completed-market OU samples")
        ou_score = ou_stats.score(spread)
        if ou_score is None:
            return f"SKIP bucket={bucket} OU score is undefined"
        if abs(ou_score) <= self.settings.ou_entry_threshold:
            return (f"HOLD bucket={bucket} OU={ou_score:+.2f} z={zscore:+.2f} "
                    f"n={ou_stats.count} model={ou_stats.method}")
        rv_q90, rv_history_n = self.calibration.rv_threshold(
            bucket, before_market_start=snapshot.start_at.timestamp(),
            window=self.settings.rv_history_window_markets,
            min_samples=self.settings.min_rv_history_markets,
        )
        if rv_q90 is None:
            return (f"DATA COLLECTION ONLY; bucket={bucket} has {rv_history_n}/"
                    f"{self.settings.min_rv_history_markets} completed-market RV10 samples for q90")
        if rv10 is None:
            return f"HOLD bucket={bucket}; RV10 unavailable until two spread captures within 10s"
        if rv10 <= rv_q90:
            return (f"HOLD bucket={bucket} OU={ou_score:+.2f}; RV10={rv10:.6f} <= "
                    f"historical q90={rv_q90:.6f} (n={rv_history_n})")
        if self.active is not None:
            return "HOLD existing shadow trade; no stacking"
        already_traded = self.store.has_shadow_trade(snapshot.market_key)
        safe_live_reentry = (
            self.live_executor is not None
            and already_traded
            and self.store.can_reenter_live_after_confirmed_no_fill(snapshot.market_key)
        )
        if ((already_traded and not safe_live_reentry)
                or self.store.has_position_or_trade(snapshot.market_key)):
            return f"HOLD one-trade-per-market already used OU={ou_score:+.2f}"

        if ou_score < 0.0:
            signal, chosen = "LOW_SPREAD", (("polymarket", "UP"),)
        else:
            signal, chosen = "HIGH_SPREAD", (("polymarket", "DOWN"),)
        decision_epoch = now.timestamp()
        scenario_key = (
            f"mode=polymarket-only;trading={self._trading_mode};feed=websocket-events;"
            f"retry={self.settings.poll_seconds:g};ou={self.settings.ou_entry_threshold:g};"
            f"roll={self.settings.history_window_markets};minhist={self.settings.min_history_markets};"
            f"minou={self.settings.min_ou_history_markets};bucket={self.settings.bucket_seconds};"
            f"ouexit={self.settings.ou_exit_threshold:g};maxhold={self.settings.max_hold_seconds};"
            f"minleft={self.settings.min_market_time_left_seconds};"
            f"shares={self.settings.min_shares:g};fee={self.settings.taker_fee_coefficient:g};"
            f"entry=absou>{self.settings.ou_entry_threshold:g}&rv10>same-bucket-q90;"
            "exit=directional-ou-threshold-or-maxhold;"
            f"rvroll={self.settings.rv_history_window_markets};"
            f"rvmin={self.settings.min_rv_history_markets};"
            f"livebuy={'gtc-limit-ask-plus-' + str(self.settings.live_buy_price_offset_ticks) + 'ticks-ttl-' + str(self.settings.live_buy_unfilled_ttl_seconds) + 's' if self.live_executor is not None else 'n/a'};"

            f"poly={self.settings.poly_order_network_ms:g}+{self.settings.poly_order_api_ms:g};"
            f"binance={self.settings.binance_order_network_ms:g}+{self.settings.binance_order_api_ms:g};"
            f"jitter=0..{self.settings.order_latency_jitter_ms:g};"
            f"execution={'current-memory-book-immediate-live' if self.live_executor is not None else 'latest-memory-book-at-arrival'}"
        )
        state: dict[str, Any] = {
            "run_id": self.run_id, "execution_mode": "polymarket_only",
            "trading_mode": self._trading_mode,
            "exit_mode": "directional_ou_threshold",
            "market_key": snapshot.market_key, "signal": signal, "zscore": zscore,
            "ou_score": ou_score, "ou_mean": ou_stats.mean, "ou_stdev": ou_stats.stdev,
            "ou_phi": ou_stats.phi, "ou_model": ou_stats.method,
            "ou_history_markets": ou_stats.count,
            "ou_entry_threshold": self.settings.ou_entry_threshold,
            "ou_exit_threshold": self.settings.ou_exit_threshold,
            "rv10": rv10, "rv_q90": rv_q90, "rv_history_markets": rv_history_n,
            "scenario_key": scenario_key,
            "bucket": bucket, "history_markets": ou_stats.count,
            "requested_shares": self.settings.min_shares, "decision_at": now.isoformat(),
            "decision_epoch": decision_epoch, "market_end": snapshot.end_at.timestamp(),
            "resolution_market": {
                "key": snapshot.polymarket.key,
                "condition_id": snapshot.polymarket.condition_id,
                "up_token": snapshot.polymarket.up_token,
                "down_token": snapshot.polymarket.down_token,
                "start_at": snapshot.polymarket.start_at.timestamp(),
                "end_at": snapshot.polymarket.end_at.timestamp(),
            },
            "status": "OPEN", "trade_id": None, "legs": [], "entries_resolved": False,
            "paired_shares": 0.0, "max_hold_due_epoch": None, "exit_triggered": False,
            "entry_cost": 0.0,
            "entry_fee": 0.0, "exit_value": 0.0, "exit_fee": 0.0,
        }
        for index, (venue, outcome) in enumerate(chosen):
            state["legs"].append({
                "name": "position" if len(chosen) == 1 else f"leg{index + 1}",
                "venue": venue, "outcome": outcome,
                "token_id": (snapshot.polymarket.up_token if outcome == "UP"
                             else snapshot.polymarket.down_token),
                "entry_filled": 0.0, "entry_cost": 0.0, "entry_fee": 0.0,
                "entry_attempted": False, "entry_fill_epoch": None,
                "unwind_remaining": 0.0, "unwind_order": None,
                "exit_remaining": 0.0, "exit_order": None,
                "exit_filled": 0.0, "exit_value": 0.0, "exit_fee": 0.0,
            })
        state["trade_id"] = self.store.create_shadow_trade(state)
        self.active = state
        for leg in state["legs"]:
            self._schedule(state, leg, "ENTRY", self.settings.min_shares, decision_epoch)
        self._persist()
        delay_text = self._delay_summary(state)
        result = (f"{self._status_prefix} SIGNAL {signal} OU={ou_score:+.2f} z={zscore:+.2f} "
                f"n={ou_stats.count} model={ou_stats.method}; Polymarket {chosen[0][1]}="
                f"{self.settings.min_shares:g} shares; RV10={rv10:.6f}>q90={rv_q90:.6f} "
                f"(n={rv_history_n}); "
                f"{('pre-signed live GTC limit uses signal-time best ask plus the configured tick offset' if self.live_executor is not None else 'fixed-delay IOC uses memory book at arrival')}; "
                f"{('real FAK execution enabled' if self.live_executor is not None else 'no real orders sent')}; "
                f"{delay_text}")
        if self.live_executor is not None:
            progress = await self._advance(snapshot, snapshot_ids, bucket, spread)
            return result if progress is None else f"{result}; {progress}"
        return result

    @property
    def _trading_mode(self) -> str:
        return "live" if self.live_executor is not None else "paper"

    @property
    def _status_prefix(self) -> str:
        return "LIVE" if self.live_executor is not None else "SHADOW"

    def set_current_book_provider(self, provider: Callable[[str, str], Book | None]) -> None:
        self.current_book_provider = provider

    async def prepare_open_live_sell_orders(
        self, market: Market, tick_size: Decimal | str,
    ) -> int:
        """Restore sell presigns for an open live position when its market feed returns."""
        if self.live_executor is None:
            return 0
        async with self._process_lock:
            state = self.active
            if (
                state is None or state.get("market_key") != market.key
                or not state.get("entries_resolved")
                or float(state.get("paired_shares", 0.0)) <= 1e-9
                or self._live_exit_inflight(int(state.get("trade_id", -1)))
                or self._live_exit_worker_running(int(state.get("trade_id", -1)))
            ):
                return 0
            pending = [
                (leg, float(leg.get("exit_remaining", 0.0)))
                for leg in state["legs"]
                if float(leg.get("exit_remaining", 0.0)) > 1e-9
            ]
        for leg, remaining in pending:
            await self._prepare_live_sell_order(
                state, leg, market, remaining, str(tick_size),
            )
        return len(pending)

    def _current_book(self, snapshot: PairSnapshot | None, leg: dict[str, Any]) -> Book | None:
        if self.live_executor is not None and self.current_book_provider is not None:
            book = self.current_book_provider(leg["venue"], leg["outcome"])
            if book is not None and str(book.token_id) == str(leg["token_id"]):
                return book
        if snapshot is None:
            return None
        book = snapshot.book(leg["venue"], leg["outcome"])
        return book if str(book.token_id) == str(leg["token_id"]) else None

    def _delay_components(self, venue: str) -> tuple[float, float, float]:
        if venue == "polymarket":
            network = self.settings.poly_order_network_ms
            api = self.settings.poly_order_api_ms
        else:
            network = self.settings.binance_order_network_ms
            api = self.settings.binance_order_api_ms
        jitter = self.rng.uniform(0.0, self.settings.order_latency_jitter_ms)
        return network, api, jitter

    def _schedule(self, state: dict[str, Any], leg: dict[str, Any], phase: str,
                  requested: float, created_epoch: float, *,
                  wait_for_update: bool = False) -> dict[str, Any]:
        network, api, jitter = self._delay_components(leg["venue"])
        if self.live_executor is not None:
            network = api = jitter = 0.0
            retry_delay = self.settings.live_retry_interval_seconds if wait_for_update else 0.0
        else:
            retry_delay = 0.0
        order = {"requested": requested, "created": created_epoch,
                 "network_ms": network, "api_ms": api, "jitter_ms": jitter,
                 "arrival": created_epoch + (network + api + jitter) / 1000.0 + retry_delay,
                 "wait_for_update": wait_for_update}
        if phase == "ENTRY":
            leg["entry_order"] = order
        else:
            leg[f"{phase.lower()}_order"] = order
        live = self.live_executor is not None
        self._event(state, phase, leg, side="BUY" if phase == "ENTRY" else "SELL",
                    status="LIVE_ORDER_READY" if live else "SIMULATED_SUBMIT",
                    requested=requested, order=order,
                    details={"note": "a pre-signed Polymarket GTC BUY will be posted when due" if live
                             else "assumed latency only; no API order request was sent",
                             "execution": "wait-for-new-book" if wait_for_update
                             else "immediate-live-submit" if live
                             else "latest-in-memory-book-at-arrival"})
        return order

    def _event(self, state: dict[str, Any], phase: str, leg: dict[str, Any] | None, *,
               side: str | None, status: str, requested: float | None = None,
               filled: float | None = None, notional: float | None = None, fee: float | None = None,
               book: Book | None = None, snapshot_id: int | None = None,
               order: dict[str, Any] | None = None, details: dict[str, Any] | None = None) -> None:
        self.store.record_shadow_event({
            "trade_id": state["trade_id"], "event_at": datetime.now(timezone.utc).isoformat(),
            "phase": phase, "venue": None if leg is None else leg["venue"],
            "outcome": None if leg is None else leg["outcome"], "side": side,
            "snapshot_id": snapshot_id, "requested_shares": requested, "filled_shares": filled,
            "vwap": None if not filled or notional is None else notional / filled,
            "notional": notional, "fee": fee,
            "request_started_at": None if book is None or book.request_started_at is None else book.request_started_at.timestamp(),
            "book_received_at": None if book is None else book.received_at.timestamp(),
            "book_source_at": None if book is None or book.source_at is None else book.source_at.timestamp(),
            "request_rtt_ms": None if book is None else book.request_rtt_ms,
            "source_age_ms": None if book is None else book.age_seconds(book.received_at) * 1000.0,
            "network_delay_ms": None if order is None else order["network_ms"],
            "api_delay_ms": None if order is None else order["api_ms"],
            "jitter_ms": None if order is None else order["jitter_ms"],
            "order_created_at": None if order is None else order["created"],
            "order_arrive_at": None if order is None else order["arrival"],
            "status": status, "details_json": json.dumps(details or {}, separators=(",", ":")),
        })
        alert_status = status
        if status == "LIVE_BOOK_STALE" and phase in {"EXIT", "UNWIND"}:
            alert_status = "LIVE_EXIT_BOOK_STALE"
        if self.live_executor is not None:
            self._schedule_live_risk_alert(
                state, alert_status, phase=phase, side=side,
            )

    async def _attempt(self, state: dict[str, Any], leg: dict[str, Any], phase: str,
                       remaining: float, order: dict[str, Any], snapshot: PairSnapshot | None,
                       snapshot_ids: dict[tuple[str, str], int],
                       market: Market | None = None) -> tuple[float, float, float] | None:
        if self.live_executor is not None:
            if order.get("live_result_recorded"):
                result = order["live_result"]
                return float(result["filled"]), float(result["notional"]), float(result["fee"])
            if order.get("live_gtc_pending"):
                # The independent entry watchdog owns retries when no worker is
                # running. Never turn a pending GTC into a zero-fill result here.
                return None
            if order.get("live_api_started"):
                state["live_order_unknown"] = True
                state["live_order_unknown_phase"] = phase
                state["status"] = "UNRESOLVED_LIVE_ORDER"
                self._event(state, phase, leg, side="BUY" if phase == "ENTRY" else "SELL",
                            status="LIVE_ORDER_UNKNOWN", requested=remaining, order=order,
                            details={"reason": "previous process stopped after persisting order intent",
                                     "manual_account_reconciliation_required": True})
                self._persist()
                return None
        market = market or (None if snapshot is None else snapshot.polymarket)
        book = self._current_book(snapshot, leg)
        wait_for_update = bool(order.get("wait_for_update", False))
        if self.live_executor is not None:
            # Live orders use the newest in-memory book as soon as the strategy
            # evaluates it. Exit retries wait for their cooldown and a later book.
            if book is None:
                if wait_for_update and time.time() + 1e-6 < order["arrival"]:
                    return None
                self._event(
                    state, phase, leg, side="BUY" if phase == "ENTRY" else "SELL",
                    status="LIVE_EXIT_BOOK_UNAVAILABLE" if phase != "ENTRY" else "LIVE_ENTRY_BOOK_UNAVAILABLE",
                    requested=remaining, order=order,
                    details={"reason": "current Polymarket order book is not seeded"},
                )
                self._persist()
                return 0.0, 0.0, 0.0
            if wait_for_update:
                if time.time() + 1e-6 < order["arrival"]:
                    return None
                if book.received_at.timestamp() + 1e-6 < order["created"]:
                    order["arrival"] = time.time() + self.settings.live_retry_interval_seconds
                    self._persist()
                    return None
            fresh = book.age_seconds(datetime.now(timezone.utc)) <= self.settings.live_max_book_age_seconds
            no_book_reason = "current in-memory WebSocket book is stale"
        elif snapshot is None or snapshot.captured_at.timestamp() + 1e-6 < order["arrival"]:
            return None
        elif wait_for_update:
            if book.received_at.timestamp() + 1e-6 < order["arrival"]:
                return None
            fresh = book.age_seconds(book.received_at) <= self.settings.max_quote_age_seconds
            no_book_reason = "no post-arrival WebSocket update"
        else:
            # A first IOC uses only a book already in memory at arrival. A later
            # update must never be allowed to leak backwards into this fill.
            if book.received_at.timestamp() > order["arrival"] + 1e-6:
                self._event(state, phase, leg,
                            side="BUY" if phase == "ENTRY" else "SELL",
                            status="IOC_NO_FILL", requested=remaining, filled=0.0,
                            notional=0.0, fee=0.0, order=order,
                            details={"reason": "no book available in memory as-of order arrival",
                                     "execution_mode": "latest-in-memory-book-at-arrival"})
                return 0.0, 0.0, 0.0
            arrival_at = (datetime.now(timezone.utc) if self.live_executor is not None
                          else datetime.fromtimestamp(order["arrival"], tz=timezone.utc))
            fresh = book.age_seconds(arrival_at) <= self.settings.max_quote_age_seconds
            no_book_reason = "arrival-time memory book is stale"
        if not fresh:
            self._event(state, phase, leg,
                        side="BUY" if phase == "ENTRY" else "SELL",
                        status="IOC_NO_FILL", requested=remaining, filled=0.0,
                        notional=0.0, fee=0.0, book=book,
                        snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])), order=order,
                        details={"reason": no_book_reason,
                                 "execution_mode": "wait-for-post-arrival-book" if wait_for_update
                                 else "latest-in-memory-book-at-arrival"})
            return 0.0, 0.0, 0.0
        side = "BUY" if phase == "ENTRY" else "SELL"
        if self.live_executor is not None:
            if market is None:
                raise RuntimeError("live order processing needs the verified Polymarket market")
            if leg["venue"] != "polymarket":
                state["live_order_unknown"] = True
                state["live_order_unknown_phase"] = phase
                self._event(state, phase, leg, side=side, status="LIVE_BLOCKED_NON_POLYMARKET",
                            requested=remaining, book=book,
                            snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])), order=order,
                            details={"reason": "live executor is restricted to Polymarket positions"})
                self._persist()
                return None
            now = datetime.now(timezone.utc)
            tick_size: Decimal | None = None
            if phase == "ENTRY":
                seconds_left = float(state["market_end"]) - now.timestamp()
                if seconds_left <= self.settings.min_market_time_left_seconds:
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "inside_final_market_cutoff"
                    self._persist()
                    self._event(state, phase, leg, side=side, status="LIVE_ENTRY_CUTOFF",
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={"seconds_left": seconds_left,
                                         "minimum_seconds_left": self.settings.min_market_time_left_seconds})
                    return 0.0, 0.0, 0.0
            try:
                tick_size = await self.live_executor.verify_market(market)
                order["live_tick_size"] = str(tick_size)
                await self.live_executor.prewarm_order_metadata(market)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                order["live_result_recorded"] = True
                order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                order["live_block_reason"] = "account_market_verification_failed"
                order["live_error_type"] = type(exc).__name__
                self._persist()
                self._event(state, phase, leg, side=side, status="LIVE_MARKET_VERIFY_FAILED",
                            requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                            book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                            order=order,
                            details={"error_type": type(exc).__name__,
                                     "reason": "market verification or SDK metadata prewarm failed"})
                return 0.0, 0.0, 0.0
            book = self._current_book(snapshot, leg)
            now = datetime.now(timezone.utc)
            if book is None:
                self._event(
                    state, phase, leg, side=side,
                    status="LIVE_EXIT_BOOK_UNAVAILABLE" if phase != "ENTRY" else "LIVE_ENTRY_BOOK_UNAVAILABLE",
                    requested=remaining, order=order,
                    details={"reason": "Polymarket order book cleared during market verification"},
                )
                self._persist()
                return 0.0, 0.0, 0.0
            if phase == "ENTRY":
                seconds_left = float(state["market_end"]) - now.timestamp()
                if seconds_left <= self.settings.min_market_time_left_seconds:
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "inside_final_market_cutoff_after_verification"
                    self._persist()
                    self._event(state, phase, leg, side=side, status="LIVE_ENTRY_CUTOFF",
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={"seconds_left": seconds_left,
                                         "minimum_seconds_left": self.settings.min_market_time_left_seconds})
                    return 0.0, 0.0, 0.0
            live_book_age = book.age_seconds(now)
            if live_book_age > self.settings.live_max_book_age_seconds:
                order["live_result_recorded"] = True
                order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                order["live_block_reason"] = "live_orderbook_too_old"
                self._persist()
                self._event(state, phase, leg, side=side, status="LIVE_BOOK_STALE",
                            requested=remaining, book=book,
                            snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])), order=order,
                            details={"book_age_ms": round(live_book_age * 1000.0, 3),
                                     "max_book_age_ms": self.settings.live_max_book_age_seconds * 1000.0})
                return 0.0, 0.0, 0.0
            limit_price: Decimal | None = None
            if phase == "ENTRY":
                min_live_shares = max(5.0, self.settings.min_shares,
                                      float(market.min_order_size))
                if remaining + 1e-9 < min_live_shares:
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "below_live_minimum_shares"
                    self._persist()
                    self._event(state, phase, leg, side=side, status="LIVE_ENTRY_SIZE_BLOCKED",
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={"requested_shares": remaining,
                                         "market_minimum_shares": float(market.min_order_size),
                                         "configured_shares": self.settings.min_shares})
                    return 0.0, 0.0, 0.0
                if book.best_ask is None:
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "no_visible_ask"
                    self._persist()
                    self._event(state, phase, leg, side=side, status="LIVE_ENTRY_NO_ASK",
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order, details={"reason": "no visible ask in current memory book"})
                    return 0.0, 0.0, 0.0
                limit_price = _live_buy_limit_price(
                    book.best_ask.price, tick_size,
                    self.settings.live_buy_price_offset_ticks,
                )
                if limit_price is None:
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "invalid_or_out_of_range_buy_limit_price"
                    self._persist()
                    self._event(state, phase, leg, side=side,
                                status="LIVE_ENTRY_LIMIT_PRICE_INVALID",
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, order=order,
                                details={"best_ask": book.best_ask.price,
                                         "requested_shares": remaining,
                                         "offset_ticks": self.settings.live_buy_price_offset_ticks,
                                         "tick_size": str(tick_size)})
                    return 0.0, 0.0, 0.0
            else:
                limit_price = Decimal("0.01")
                min_live_shares = max(5.0, float(market.min_order_size))
                if remaining + 1e-9 < min_live_shares:
                    state["status"] = "SETTLEMENT_PENDING"
                    state.pop("live_position_unresolved", None)
                    state["settlement_pending_reason"] = "remainder_below_minimum_sell_size"
                    state["closed_at"] = datetime.now(timezone.utc).isoformat()
                    self._event(state, phase, leg, side=side, status="LIVE_EXIT_BELOW_MINIMUM",
                                requested=remaining, book=book,
                                snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={"minimum_live_shares": min_live_shares,
                                         "remaining_shares": remaining,
                                         "settlement_status": "awaiting Polymarket market resolution",
                                         "new_trading_allowed": True})
                    self._event(state, "TRADE", None, side=None, status="SETTLEMENT_PENDING",
                                details={"remaining_shares": remaining,
                                         "settlement_status": "awaiting Polymarket market resolution",
                                         "reason": "remainder_below_minimum_sell_size",
                                         "new_trading_allowed": True})
                    self._persist()
                    if self.active is state:
                        self.active = None
                    return None
                estimated_filled, estimated_notional, _ = _partial_sweep(
                    book, side="SELL", shares=remaining,
                    fee_coefficient=self.settings.taker_fee_coefficient,
                )
                if book.best_bid is None or estimated_filled <= 1e-9:
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "no_visible_bid"
                    self._persist()
                    self._event(state, phase, leg, side=side, status="LIVE_EXIT_NO_BID",
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={"reason": "no visible bid in current memory book",
                                         "estimated_book_fill": estimated_filled,
                                         "estimated_book_notional": estimated_notional})
                    return 0.0, 0.0, 0.0
            intent_details = {
                "execution_mode": "live",
                "order_type": "GTC" if phase == "ENTRY" else "FAK",
                "asset_id": leg["token_id"],
                "requested_shares": remaining,
                "post_only": False,
                "book_received_at": book.received_at.timestamp(),
                "book_source_at": None if book.source_at is None else book.source_at.timestamp(),
                "book_age_ms": round(book.age_seconds(now) * 1000.0, 3),
                "tick_size": None if tick_size is None else str(tick_size),
                "fee_accounting": "configured_fee_formula_applied_to_live_vwap",
            }
            if phase == "ENTRY":
                assert limit_price is not None and book.best_ask is not None
                intent_details.update({
                    "best_ask": format(Decimal(str(book.best_ask.price)), "f"),
                    "limit_price": format(limit_price, "f"),
                    "limit_price_basis": "signal_time_best_ask_plus_configured_ticks",
                    "limit_price_offset_ticks": self.settings.live_buy_price_offset_ticks,
                    "unfilled_remainder_seconds": self.settings.live_buy_unfilled_ttl_seconds,
                    "nominal_share_target": remaining,
                })
            else:
                intent_details.update({
                    "limit_price": str(limit_price),
                    "limit_price_basis": "0.01_marketable_sell_floor",
                })
            max_attempts = 1
            for attempt_number in range(1, max_attempts + 1):
                now = datetime.now(timezone.utc)
                if attempt_number == 2:
                    seconds_left = float(state["market_end"]) - now.timestamp()
                    if (book is None or book.best_ask is None
                            or seconds_left <= self.settings.min_market_time_left_seconds
                            or book.age_seconds(now) > self.settings.live_max_book_age_seconds):
                        self._event(
                            state, phase, leg, side=side, status="LIVE_BUY_RETRY_SKIPPED",
                            requested=remaining, book=book, order=order,
                            details={**intent_details, "retry_count": 0,
                                     "reason": "fresh book missing, stale, or inside market cutoff",
                                     "seconds_left": seconds_left},
                        )
                        break
                    assert buy_max_price is not None and market_amount_usdc is not None
                    retry_best_ask = Decimal(str(book.best_ask.price))
                    if not retry_best_ask.is_finite():
                        self._event(
                            state, phase, leg, side=side, status="LIVE_BUY_RETRY_SKIPPED",
                            requested=remaining, book=book, order=order,
                            details={**intent_details, "retry_count": 0,
                                     "reason": "invalid_refreshed_best_ask",
                                     "best_ask": book.best_ask.price,
                                     "original_max_price": format(buy_max_price, "f"),
                                     "original_market_buy_amount_usdc": format(market_amount_usdc, "f")},
                        )
                        break
                    if retry_best_ask > buy_max_price:
                        self._event(
                            state, phase, leg, side=side, status="LIVE_BUY_RETRY_SKIPPED",
                            requested=remaining, book=book, order=order,
                            details={**intent_details, "retry_count": 0,
                                     "reason": "refreshed_best_ask_above_original_max_price",
                                     "retry_best_ask": format(retry_best_ask, "f"),
                                     "original_max_price": format(buy_max_price, "f"),
                                     "original_market_buy_amount_usdc": format(market_amount_usdc, "f")},
                        )
                        break
                    intent_details = {
                        **intent_details,
                        "retry_memory_best_ask": format(retry_best_ask, "f"),
                        "retry_book_received_at": book.received_at.timestamp(),
                        "retry_book_source_at": None if book.source_at is None else book.source_at.timestamp(),
                        "retry_book_age_ms": round(book.age_seconds(now) * 1000.0, 3),
                        "retry_of_market_buy_amount_usdc": format(market_amount_usdc, "f"),
                        "retry_max_price_policy": "reuse_original_signal_cap_and_budget",
                        "attempt_number": 2,
                        "retry_reason": "explicit_fak_no_fill",
                    }

                now = datetime.now(timezone.utc)
                if phase == "EXIT" and now.timestamp() >= float(state["market_end"]):
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "market_ended_before_exit_post"
                    self._persist()
                    self._event(
                        state, phase, leg, side="SELL", status="LIVE_EXIT_MARKET_ENDED",
                        requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                        book=book, order=order,
                        details={"reason": "market ended before the sell request was posted",
                                 "market_end_epoch": state["market_end"]},
                    )
                    return 0.0, 0.0, 0.0
                intent_ns = time.time_ns()
                if attempt_number > 1:
                    trigger_ns = int(order.get("last_no_fill_ns", intent_ns))
                else:
                    trigger_epoch = (
                        state.get("decision_epoch") if phase == "ENTRY"
                        else state.get("exit_trigger_epoch", now.timestamp())
                    )
                    trigger_ns = int(float(trigger_epoch or now.timestamp()) * 1e9)
                timing_context = {
                    "trigger_kind": "strategy_signal" if attempt_number == 1 else "explicit_fak_no_fill_retry",
                    "trigger_ns": trigger_ns,
                    "intent_ns": intent_ns,
                    "attempt_number": attempt_number,
                    "retry_count": attempt_number - 1,
                }
                attempt_details = {**intent_details, "attempt_number": attempt_number,
                                   "retry_count": attempt_number - 1}
                order["live_order_intent_started"] = True
                order["live_order_type"] = "GTC" if phase == "ENTRY" else "FAK"
                order["live_order_asset_id"] = str(leg["token_id"])
                order["live_order_condition_id"] = str(market.condition_id)
                order["live_order_side"] = side
                order["live_order_requested"] = float(remaining)
                order["live_order_intent_price"] = str(limit_price)
                order["live_order_intent_key"] = (
                    f"{int(state['trade_id'])}:{phase}:{attempt_number}"
                )
                order["live_order_intent_details"] = dict(intent_details)
                order["live_order_timing_context"] = dict(timing_context)
                order["live_api_started"] = False
                order["live_attempt_number"] = attempt_number
                order["live_intent_epoch"] = now.timestamp()
                order["live_submit_epoch"] = now.timestamp()
                self._persist()
                self._event(state, phase, leg, side=side, status="LIVE_ORDER_INTENT",
                            requested=remaining, book=book,
                            snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                            order=order,
                            details={**attempt_details, **timing_context,
                                     "intent_recorded_before_api": True})

                def record_signed_order(signed_details: dict[str, Any]) -> None:
                    order["live_api_started"] = True
                    order["live_post_may_have_started"] = True
                    order["live_signed_epoch"] = time.time()
                    order["live_signed_details"] = dict(signed_details)
                    self.live_executor.register_user_order_intent(
                        str(order["live_order_intent_key"]),
                        asset_id=str(leg["token_id"]),
                        condition_id=str(market.condition_id), side=side,
                        requested_qty=float(remaining), price=limit_price,
                        intent_epoch=float(order["live_intent_epoch"]),
                    )
                    self._event(
                        state, phase, leg, side=side, status="LIVE_ORDER_SIGNED",
                        requested=remaining, book=book,
                        snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                        order=order,
                        details={**attempt_details, **signed_details,
                                 "signed_event_recorded_before_post": True},
                    )
                    self._persist()

                try:
                    if phase == "ENTRY":
                        assert limit_price is not None and tick_size is not None
                        accepted, order_id, response_details = await self.live_executor.buy_gtc_limit(
                            asset_id=leg["token_id"], condition_id=market.condition_id,
                            requested_qty=remaining,
                            limit_price=limit_price, tick_size=tick_size,
                            on_order_signed=record_signed_order,
                            timing_context=timing_context,
                        )
                        if accepted:
                            assert order_id is not None
                            response_details["order_id"] = order_id
                            response_details["rest_seconds"] = self.settings.live_buy_unfilled_ttl_seconds
                            self._queue_live_entry_finalization(
                                state, leg, order, market, book,
                                snapshot_ids.get((leg["venue"], leg["outcome"])),
                                attempt_details, timing_context, response_details,
                            )
                            self._event(
                                state, phase, leg, side="BUY", status="LIVE_GTC_ACCEPTED",
                                requested=remaining, book=book,
                                snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={**attempt_details, **response_details,
                                         "note": "unfilled remainder will be canceled after the configured rest interval"},
                            )
                            self._persist()
                            return None
                        details = {**attempt_details, **response_details,
                                   "outcome_unknown": False,
                                   "live_order_timing": dict(timing_context)}
                        order["live_result_recorded"] = True
                        order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                        order["live_response_details"] = details
                        self._event(
                            state, phase, leg, side="BUY", status="LIVE_GTC_REJECTED",
                            requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                            book=book,
                            snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                            order=order, details=details,
                        )
                        self._persist()
                        return 0.0, 0.0, 0.0
                    else:
                        accepted, fill, response_details = await self.live_executor.sell_fak(
                            asset_id=leg["token_id"], requested_qty=remaining,
                            limit_price="0.01", tick_size=tick_size,
                            on_order_signed=record_signed_order,
                            timing_context=timing_context,
                        )
                except asyncio.CancelledError:
                    state["live_order_unknown"] = True
                    state["live_order_unknown_phase"] = phase
                    state["status"] = "UNRESOLVED_LIVE_ORDER"
                    order["live_unknown"] = True
                    self._event(state, phase, leg, side=side, status="LIVE_ORDER_UNKNOWN",
                                requested=remaining, book=book, order=order,
                                details={**attempt_details, **timing_context,
                                         "reason": "task cancelled during order request"})
                    self._persist()
                    raise
                except LiveFakNoFill as exc:
                    balance_allowance_rejected = isinstance(exc, LiveBalanceAllowanceRejected)
                    self.live_executor.forget_user_order_intent(
                        str(order.get("live_order_intent_key") or ""),
                    )
                    details = {**attempt_details, **exc.diagnostic_fields,
                               "error_type": type(exc).__name__, "error": str(exc),
                               "outcome_unknown": False,
                               "live_order_timing": dict(timing_context)}
                    if balance_allowance_rejected:
                        details["retryable"] = phase in {"EXIT", "UNWIND"}
                        details["retry_policy"] = "retry after live interval and a fresh book update"
                    event_status = (
                        "LIVE_SELL_BALANCE_RETRY" if balance_allowance_rejected and side == "SELL"
                        else "LIVE_FAK_NO_FILL"
                    )
                    self._event(state, phase, leg, side=side, status=event_status,
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, order=order, details=details)
                    if phase == "ENTRY" and attempt_number == 1:
                        order["last_no_fill_ns"] = time.time_ns()
                        continue
                    if phase == "EXIT" and not balance_allowance_rejected:
                        order["live_fak_no_fill_confirmed"] = True
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_response_details"] = details
                    self._persist()
                    return 0.0, 0.0, 0.0
                except LiveOrderUnknown as exc:
                    unknown_epoch = time.time()
                    state["live_order_unknown"] = True
                    state["live_order_unknown_phase"] = phase
                    state["status"] = "UNRESOLVED_LIVE_ORDER"
                    order["live_unknown"] = True
                    order["live_error_type"] = type(exc).__name__
                    order["live_unknown_epoch"] = unknown_epoch
                    ambiguous_order_id = str(exc.diagnostic_fields.get("order_id") or "")
                    if str(order.get("live_order_type") or "").upper() == "FAK":
                        order["live_fak_order_id"] = ambiguous_order_id or None
                        order["live_fak_reconcile_pending"] = True
                        order["live_fak_reconcile_side"] = side
                        order["live_fak_reconcile_phase"] = phase
                        order["live_fak_reconcile_requested"] = float(remaining)
                        order["live_fak_reconcile_attempts"] = 0
                        order["live_fak_reconcile_next_epoch"] = time.time()
                    elif str(order.get("live_order_type") or "").upper() == "GTC":
                        order["live_gtc_discovery_pending"] = True
                        order["live_gtc_discovery_next_epoch"] = time.time()
                    order["live_response_details"] = {
                        **dict(order.get("live_response_details") or {}),
                        **exc.diagnostic_fields,
                        "error_type": type(exc).__name__,
                        "outcome_unknown": True,
                    }
                    self._event(state, phase, leg, side=side, status="LIVE_ORDER_UNKNOWN",
                                requested=remaining, book=book, order=order,
                                details={**attempt_details, **exc.diagnostic_fields,
                                         "error_type": type(exc).__name__,
                                         "reason": "API outcome may have reached Polymarket; order intent recovery scheduled"
                                         if order.get("live_fak_reconcile_pending") or order.get("live_gtc_discovery_pending") else
                                         "API outcome may have reached Polymarket but no server order ID was returned"})
                    self._persist()
                    return None

                filled = 0.0 if fill is None else float(fill.filled_qty)
                notional = 0.0 if fill is None else float(fill.notional)
                fee = 0.0 if fill is None else float(fill.fee)
                order["live_fill_epoch"] = time.time()
                result = {"filled": filled, "notional": notional, "fee": fee}
                order["live_result"] = result
                order["live_result_recorded"] = True
                order["live_accepted"] = bool(accepted)
                order["live_response_details"] = response_details
                self.live_executor.forget_user_order_intent(
                    str(order.get("live_order_intent_key") or ""),
                )
                status = (
                    "LIVE_ORDER_PREPARATION_FAILED" if not accepted else
                    "LIVE_FAK_NO_FILL" if filled <= 1e-9 else
                    "LIVE_FAK_PARTIAL_FILL" if filled + 1e-9 < remaining else
                    "LIVE_FAK_FILLED"
                )
                self._event(
                    state, phase, leg, side=side, status=status,
                    requested=remaining, filled=filled, notional=notional, fee=fee,
                    book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                    order=order,
                    details={**attempt_details, **response_details,
                             "live_order_timing": dict(timing_context),
                             "post_response_elapsed_ms": round(
                                 (time.time() - now.timestamp()) * 1000.0, 3)},
                )
                self._persist()
                return filled, notional, fee
            order["live_result_recorded"] = True
            order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
            self._persist()
            return 0.0, 0.0, 0.0
        filled, notional, fee = _partial_sweep(
            book, side=side, shares=remaining, fee_coefficient=self.settings.taker_fee_coefficient
        )
        status = "IOC_FILLED" if filled + 1e-9 >= remaining else ("IOC_PARTIAL_CANCELED" if filled > 0 else "IOC_NO_FILL")
        self._event(state, phase, leg, side=side, status=status, requested=remaining,
                    filled=filled, notional=notional, fee=fee, book=book,
                    snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])), order=order,
                    details={"remaining_canceled": max(0.0, remaining - filled),
                             "book_depth_levels": len(book.asks if side == "BUY" else book.bids),
                             "execution_mode": "wait-for-post-arrival-book" if wait_for_update
                             else "latest-in-memory-book-at-arrival",
                             "book_received_offset_ms": round(
                                 (book.received_at.timestamp() - order["arrival"]) * 1000.0, 3)})
        return filled, notional, fee

    async def _prepare_live_sell_order(
        self,
        state: dict[str, Any],
        leg: dict[str, Any],
        market: Market,
        shares: float,
        tick_size: Any,
    ) -> None:
        if self.live_executor is None or tick_size is None or shares <= 1e-9:
            return
        minimum = max(5.0, float(market.min_order_size))
        if shares + 1e-9 < minimum:
            return
        try:
            result = await self.live_executor.prepare_sell_order_pool(
                market, asset_id=leg["token_id"], shares=shares,
                limit_price="0.01", tick_size=str(tick_size),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            async with self._timed_process_lock("live_sell_presign_failed", market_key=str(state.get("market_key"))):
                self._event(
                    state, "SELL_PRESIGN", leg, side="SELL",
                    status="LIVE_SELL_PRESIGN_FAILED", requested=shares,
                    details={"error_type": type(exc).__name__,
                             "reason": "sell will fall back to on-demand signing"},
                )
                self._persist()
            return
        if result.get("status") == "ready":
            async with self._timed_process_lock("live_sell_presign_ready", market_key=str(state.get("market_key"))):
                self._event(
                    state, "SELL_PRESIGN", leg, side="SELL",
                    status="LIVE_SELL_PRESIGNED", requested=shares,
                    details={**result, "limit_price": "0.01",
                             "note": "signed order only; no sell was submitted"},
                )
                self._persist()

    def _queue_live_sell_presign(
        self, state: dict[str, Any], leg: dict[str, Any], market: Market,
        shares: float, tick_size: Any,
    ) -> None:
        if self.live_executor is None or tick_size is None or shares <= 1e-9:
            return
        task = asyncio.create_task(
            self._prepare_live_sell_order(state, leg, market, shares, tick_size),
            name=f"live-sell-presign-{state.get('trade_id', 'unknown')}",
        )
        self._live_presign_tasks.add(task)

        def discard_finished(done: asyncio.Task[None]) -> None:
            self._live_presign_tasks.discard(done)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                print(f"LIVE SELL PRESIGN WORKER ERROR ({type(error).__name__})", flush=True)

        task.add_done_callback(discard_finished)

    def _live_entry_worker_running(self, order: dict[str, Any]) -> bool:
        trade_id = order.get("live_gtc_trade_id")
        if trade_id is None:
            return False
        task = self._live_entry_tasks.get(int(trade_id))
        return task is not None and not task.done()

    def _queue_live_entry_finalization(
        self,
        state: dict[str, Any],
        leg: dict[str, Any],
        order: dict[str, Any],
        market: Market,
        book: Book,
        snapshot_id: int | None,
        attempt_details: dict[str, Any],
        timing_context: dict[str, Any],
        accepted_details: dict[str, Any],
    ) -> None:
        if self.live_executor is None:
            raise RuntimeError("live entry finalization requires a connected executor")
        trade_id = int(state["trade_id"])
        order_id = str(accepted_details["order_id"])
        self.live_executor.forget_user_order_intent(
            str(order.get("live_order_intent_key") or ""),
        )
        order["live_gtc_pending"] = True
        order["live_gtc_order_id"] = order_id
        order["live_gtc_trade_id"] = trade_id
        order["live_gtc_wait_seconds"] = self.settings.live_buy_unfilled_ttl_seconds
        order["live_gtc_initial_wait_completed"] = False
        accepted_epoch = time.time()
        deadline_epoch = accepted_epoch + self.settings.live_buy_unfilled_ttl_seconds
        order["live_gtc_accepted_epoch"] = accepted_epoch
        order["live_gtc_deadline_epoch"] = deadline_epoch
        order["live_gtc_cancel_worker_scheduled"] = False
        order["live_gtc_worker_started_epoch"] = None
        order["live_gtc_last_progress_epoch"] = None
        order["live_gtc_accepted_details"] = dict(accepted_details)
        order["live_gtc_attempt_details"] = dict(attempt_details)
        order["live_gtc_timing_context"] = dict(timing_context)
        order["live_gtc_next_poll_epoch"] = (
            deadline_epoch + 0.1
        )
        self._persist_state(state)
        try:
            self.live_executor.schedule_gtc_cancel_deadline(order_id, deadline_epoch)
            order["live_gtc_cancel_worker_scheduled"] = True
            self._persist_state(state)
        except Exception as exc:
            self.store.record_runtime_event(
                run_id=self.run_id or "live-entry-worker",
                event_type="live_gtc_cancel_worker_schedule_failed",
                market_key=str(state.get("market_key")), venue="polymarket",
                details={"trade_id": trade_id, "order_id": order_id,
                         "deadline_epoch": deadline_epoch,
                         "error_type": type(exc).__name__},
            )
        self._start_live_entry_finalization(
            state, leg, order, market, book, snapshot_id,
            attempt_details, timing_context, accepted_details,
            wait_seconds=self.settings.live_buy_unfilled_ttl_seconds,
            resumed=False,
        )

    def _start_live_entry_finalization(
        self,
        state: dict[str, Any],
        leg: dict[str, Any],
        order: dict[str, Any],
        market: Market,
        book: Book | None,
        snapshot_id: int | None,
        attempt_details: dict[str, Any],
        timing_context: dict[str, Any],
        accepted_details: dict[str, Any],
        *,
        wait_seconds: float,
        resumed: bool,
    ) -> None:
        trade_id = int(state["trade_id"])
        current = self._live_entry_tasks.get(trade_id)
        if current is not None and not current.done():
            started = float(order.get("live_gtc_worker_started_epoch") or time.time())
            progress = float(order.get("live_gtc_last_progress_epoch") or started)
            stale_after = max(20.0, float(wait_seconds) + 15.0)
            cancel_requested = float(order.get("live_gtc_stale_cancel_requested_epoch") or 0.0)
            if (time.time() - progress > stale_after
                    and time.time() - cancel_requested >= 10.0):
                current.cancel()
                order["live_gtc_stale_cancel_requested_epoch"] = time.time()
                order["live_gtc_next_poll_epoch"] = time.time()
                self.store.record_runtime_event(
                    run_id=self.run_id or "live-entry-watchdog",
                    event_type="live_entry_worker_stale_cancel_requested",
                    market_key=str(state.get("market_key")), venue="polymarket",
                    details={"trade_id": trade_id,
                             "order_id": order.get("live_gtc_order_id"),
                             "idle_seconds": round(time.time() - progress, 3),
                             "retry_after_worker_stops": True},
                )
                self._persist_state(state)
            return
        order_id = str(accepted_details.get("order_id") or order.get("live_gtc_order_id") or "")
        if not order_id:
            self._record_live_entry_reconciliation_retry(
                state, leg, order, RuntimeError("pending GTC entry is missing its order ID"),
            )
            return
        self.store.record_runtime_event(
            run_id=self.run_id or "live-entry-worker",
            event_type="live_entry_worker_queued",
            market_key=str(state["market_key"]),
            venue="polymarket",
            details={
                "trade_id": trade_id,
                "order_id": order_id,
                "leg": str(leg["name"]),
                "wait_seconds": wait_seconds,
                "accepted_epoch": order.get("live_gtc_accepted_epoch"),
                "cancel_deadline_epoch": order.get("live_gtc_deadline_epoch"),
                "cancel_worker_scheduled": order.get("live_gtc_cancel_worker_scheduled"),
                "resumed": resumed,
            },
        )
        task = asyncio.create_task(
            self._run_live_entry_finalization(
                state, leg, order, market, book, snapshot_id,
                attempt_details, timing_context, accepted_details, wait_seconds,
            ),
            name=f"live-entry-order-{trade_id}",
        )
        self._live_entry_tasks[trade_id] = task
        order["live_gtc_worker_started_epoch"] = time.time()
        order["live_gtc_last_progress_epoch"] = time.time()
        order["live_gtc_stale_cancel_requested_epoch"] = None
        self._persist_state(state)

        def report_worker_error(done: asyncio.Task[None]) -> None:
            if self._live_entry_tasks.get(trade_id) is done:
                self._live_entry_tasks.pop(trade_id, None)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                print(f"LIVE ENTRY WORKER ERROR ({type(error).__name__}); order state retained", flush=True)

        task.add_done_callback(report_worker_error)

    async def _run_live_entry_finalization(
        self,
        state: dict[str, Any],
        leg: dict[str, Any],
        order: dict[str, Any],
        market: Market,
        book: Book,
        snapshot_id: int | None,
        attempt_details: dict[str, Any],
        timing_context: dict[str, Any],
        accepted_details: dict[str, Any],
        wait_seconds: float,
    ) -> None:
        assert self.live_executor is not None
        order_id = str(accepted_details["order_id"])
        trade_id = int(state["trade_id"])

        def record_progress(step: str, outcome: str, details: dict[str, Any]) -> None:
            order["live_gtc_last_progress_epoch"] = time.time()
            if step == "user_ws_wait" and outcome == "completed":
                order["live_gtc_initial_wait_completed"] = True
            self._persist_state(state)
            self.store.record_runtime_event(
                run_id=self.run_id or "live-entry-worker",
                event_type="live_gtc_reconcile_step",
                market_key=str(state["market_key"]), venue="polymarket",
                details={"trade_id": trade_id, "order_id": order_id,
                         "step": step, "outcome": outcome, **details},
            )

        try:
            fill, reconciliation = await self.live_executor.cancel_and_reconcile_gtc_buy(
                order_id=order_id,
                asset_id=str(leg["token_id"]),
                condition_id=str(market.condition_id),
                requested_qty=float(order["requested"]),
                wait_seconds=wait_seconds,
                post_order_status=str(accepted_details.get("order_status") or ""),
                progress_callback=record_progress,
            )
            await self.live_executor.forget_gtc_cancel(order_id)
        except asyncio.CancelledError:
            async with self._timed_process_lock("live_gtc_finalizer_cancelled", market_key=str(state.get("market_key"))):
                if order.get("live_gtc_pending"):
                    self._record_live_entry_reconciliation_retry(
                        state, leg, order,
                        RuntimeError("entry finalizer stopped before reconciliation completed"),
                        book=book, snapshot_id=snapshot_id,
                        details={**attempt_details, **timing_context, "order_id": order_id},
                    )
            raise
        except Exception as exc:
            async with self._timed_process_lock("live_gtc_finalizer_failed", market_key=str(state.get("market_key"))):
                if not order.get("live_gtc_pending"):
                    return
                self._record_live_entry_reconciliation_retry(
                    state, leg, order, exc, book=book, snapshot_id=snapshot_id,
                    details={**attempt_details, **timing_context,
                             **getattr(exc, "diagnostic_fields", {}),
                             "order_id": order_id},
                )
            return

        async with self._timed_process_lock("live_gtc_finalizer_completed", market_key=str(state.get("market_key"))):
            if not order.get("live_gtc_pending"):
                return
            filled = 0.0 if fill is None else float(fill.filled_qty)
            notional = 0.0 if fill is None else float(fill.notional)
            fee = 0.0 if fill is None else float(fill.fee)
            order["live_gtc_pending"] = False
            order["live_gtc_cancel_worker_scheduled"] = False
            order["live_gtc_worker_started_epoch"] = None
            order["live_gtc_last_progress_epoch"] = time.time()
            order["live_gtc_stale_cancel_requested_epoch"] = None
            order["live_gtc_next_poll_epoch"] = None
            order["live_gtc_last_error"] = None
            order["live_gtc_reconcile_attempts"] = 0
            order["live_result_recorded"] = True
            order["live_result"] = {"filled": filled, "notional": notional, "fee": fee}
            order["live_accepted"] = True
            order["live_fill_epoch"] = time.time() if filled > 1e-9 else None
            order["live_response_details"] = {**accepted_details, **reconciliation}
            order["live_unknown"] = False
            state["live_order_unknown"] = False
            state.pop("live_order_unknown_phase", None)
            if state.get("status") == "UNRESOLVED_LIVE_ORDER":
                state["status"] = "OPEN"
            state["closed_at"] = None
            status = (
                "LIVE_GTC_NO_FILL" if filled <= 1e-9
                else "LIVE_GTC_PARTIAL_CANCELED"
                if filled + 1e-9 < float(order["requested"])
                else "LIVE_GTC_FILLED"
            )
            self._event(
                state, "ENTRY", leg, side="BUY", status=status,
                requested=float(order["requested"]), filled=filled,
                notional=notional, fee=fee, book=book,
                snapshot_id=snapshot_id, order=order,
                details={
                    **attempt_details, **accepted_details, **reconciliation,
                    "remaining_canceled": max(0.0, float(order["requested"]) - filled),
                    "live_order_timing": dict(timing_context),
                },
            )
            expired = time.time() >= float(state.get("market_end", float("inf")))
            if self.active is not state or expired:
                self._apply_detached_live_entry_result(
                    state, leg, order, filled, notional, fee,
                )
                self.store.record_runtime_event(
                    run_id=self.run_id or "live-entry-worker",
                    event_type="live_entry_worker_finished",
                    market_key=str(state["market_key"]), venue="polymarket",
                    details={"trade_id": trade_id, "order_id": order_id,
                             "filled_shares": filled, "status": status,
                             "late_result_persisted": True},
                )
                return
            self.store.record_runtime_event(
                run_id=self.run_id or "live-entry-worker",
                event_type="live_entry_worker_finished",
                market_key=str(state["market_key"]),
                venue="polymarket",
                details={
                    "trade_id": int(state["trade_id"]),
                    "order_id": order_id,
                    "filled_shares": filled,
                    "status": status,
                },
            )
            self._persist_state(state)
            await self._advance(
                None, {}, int(state.get("bucket", 0)), None,
                market=market, captured_at=datetime.now(timezone.utc),
            )

    def _record_live_entry_reconciliation_retry(
        self, state: dict[str, Any], leg: dict[str, Any], order: dict[str, Any],
        exc: Exception, *, book: Book | None = None, snapshot_id: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        attempts = int(order.get("live_gtc_reconcile_attempts", 0)) + 1
        delay = min(30.0, float(2 ** min(attempts - 1, 5)))
        now = time.time()
        order["live_gtc_reconcile_attempts"] = attempts
        order["live_gtc_pending"] = True
        order["live_gtc_initial_wait_completed"] = True
        order["live_gtc_worker_started_epoch"] = None
        order["live_gtc_last_progress_epoch"] = now
        order["live_gtc_stale_cancel_requested_epoch"] = None
        order["live_gtc_next_poll_epoch"] = now + delay
        order["live_gtc_last_error"] = {
            "error_type": type(exc).__name__,
            "message": str(exc)[:300],
        }
        order["live_unknown"] = True
        state["status"] = "UNRESOLVED_LIVE_ORDER"
        state["live_order_unknown"] = True
        state["live_order_unknown_phase"] = "ENTRY"
        # An unresolved GTC entry is not a closed trade, even if an earlier
        # market-end path had already stamped a close time on this state.
        state["closed_at"] = None
        self._event(
            state, "ENTRY", leg, side="BUY", status="LIVE_GTC_RECONCILE_RETRY",
            requested=float(order.get("requested", state.get("requested_shares", 0.0))),
            book=book, snapshot_id=snapshot_id, order=order,
            details={**(details or {}), "error_type": type(exc).__name__,
                     "error": str(exc)[:300], "retry_attempt": attempts,
                     "retry_in_seconds": delay,
                     "reconciliation_remains_pending": True},
        )
        self._persist_state(state)
        self.store.record_runtime_event(
            run_id=self.run_id or "live-entry-worker",
            event_type="live_entry_worker_retry_scheduled",
            market_key=str(state["market_key"]), venue="polymarket",
            details={"trade_id": int(state["trade_id"]),
                     "order_id": order.get("live_gtc_order_id"),
                     "retry_attempt": attempts, "retry_in_seconds": delay,
                     "error_type": type(exc).__name__},
        )

    def _apply_detached_live_entry_result(
        self, state: dict[str, Any], leg: dict[str, Any], order: dict[str, Any],
        filled: float, notional: float, fee: float,
    ) -> None:
        """Apply a late GTC result to its trade row without touching a newer active trade."""
        fill_epoch = order.get("live_fill_epoch")
        leg["entry_attempted"] = True
        leg["entry_filled"] = filled
        leg["entry_cost"] = notional
        leg["entry_fee"] = fee
        leg["entry_fill_epoch"] = fill_epoch if filled > 1e-9 else None
        leg["unwind_remaining"] = 0.0
        leg["unwind_order"] = None
        leg["exit_remaining"] = filled
        state["entries_resolved"] = True
        state["paired_shares"] = filled
        state["position_shares"] = filled
        state["entry_cost"] = notional
        state["entry_fee"] = fee
        state["live_order_unknown"] = False
        state.pop("live_order_unknown_phase", None)
        state["live_position_unresolved"] = False
        state.pop("live_order_quarantined", None)
        if time.time() >= float(state.get("market_end", float("inf"))):
            state["closed_at"] = datetime.now(timezone.utc).isoformat()
            if filled > 1e-9:
                state["status"] = "SETTLEMENT_PENDING"
                self._event(
                    state, "TRADE", None, side=None, status="SETTLEMENT_PENDING",
                    details={"position_shares": filled,
                             "settlement_status": "awaiting Polymarket market resolution",
                             "reason": "late GTC fill reconciliation after market end",
                             "entry_trade_id": state.get("trade_id")},
                )
            else:
                state["status"] = "NO_POSITION"
                state["net_pnl"] = 0.0
                self._event(
                    state, "TRADE", None, side=None, status="NO_POSITION",
                    details={"position_shares": 0.0,
                             "reason": "GTC order confirmed canceled with zero fills"},
                )
        else:
            state["status"] = "OPEN" if filled > 1e-9 else "NO_POSITION"
        self._persist_state(state)
        if state["status"] == "SETTLEMENT_PENDING":
            self._settle_known_resolution(state)
    def _queue_live_exit_attempt(
        self, state: dict[str, Any], leg: dict[str, Any],
        order: dict[str, Any], market: Market,
    ) -> None:
        if self.live_executor is None:
            return
        trade_id = int(state["trade_id"])
        if self._live_exit_inflight(trade_id):
            return
        current = self._live_exit_tasks.get(trade_id)
        if current is not None and not current.done():
            return
        queued_epoch = time.time()
        self.store.record_runtime_event(
            run_id=self.run_id or "live-exit-worker",
            event_type="live_exit_worker_queued",
            market_key=str(state["market_key"]), venue="polymarket",
            details={"trade_id": int(state["trade_id"]), "leg": str(leg["name"]),
                     "reason": state.get("exit_reason"),
                     "order_arrival_epoch": order.get("arrival")},
        )
        task = asyncio.create_task(
            self._run_live_exit_attempt(
                state, str(leg["name"]), order, market, queued_epoch,
            ),
            name=f"live-exit-order-{trade_id}",
        )
        self._live_exit_tasks[trade_id] = task

        def report_worker_error(done: asyncio.Task[None]) -> None:
            if self._live_exit_tasks.get(trade_id) is done:
                self._live_exit_tasks.pop(trade_id, None)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                print(f"LIVE EXIT WORKER ERROR ({type(error).__name__}); order state retained", flush=True)

        task.add_done_callback(report_worker_error)

    def _mark_live_settlement_pending(self, state: dict[str, Any], reason: str) -> None:
        if state.get("status") != "SETTLEMENT_PENDING":
            state["status"] = "SETTLEMENT_PENDING"
            state["live_position_unresolved"] = False
            state["closed_at"] = datetime.now(timezone.utc).isoformat()
            remaining = sum(float(leg.get("exit_remaining", 0.0)) for leg in state["legs"])
            self._event(
                state, "TRADE", None, side=None, status="SETTLEMENT_PENDING",
                details={"position_shares": remaining,
                         "settlement_status": "awaiting Polymarket market resolution",
                         "reason": reason, "new_trading_allowed": True},
            )
        self._persist_state(state)
        if self.active is state:
            self.active = None
        self._settle_known_resolution(state)

    async def _run_live_exit_attempt(
        self, state: dict[str, Any], leg_name: str, expected_order: dict[str, Any],
        market: Market, queued_epoch: float,
    ) -> None:
        trade_id = int(state["trade_id"])
        leg: dict[str, Any] | None = None
        claimed = False
        remaining = 0.0
        outcome = "not_claimed"
        started = time.monotonic()
        try:
            async with self._timed_process_lock("live_exit_worker_claim", market_key=str(state.get("market_key"))):
                if (
                    state.get("live_order_unknown")
                    or state.get("status") == "SETTLEMENT_PENDING"
                    or self._live_exit_inflight(trade_id)
                ):
                    return
                leg = next((item for item in state["legs"] if str(item["name"]) == leg_name), None)
                if (
                    leg is None or leg.get("exit_order") is not expected_order
                    or float(leg.get("exit_remaining", 0.0)) <= 1e-9
                    or time.time() + 1e-6 < float(expected_order.get("arrival", 0.0))
                ):
                    return
                if time.time() >= float(state["market_end"]):
                    self._mark_live_settlement_pending(state, "exit worker woke after market end")
                    outcome = "market_ended_before_submit"
                    return
                remaining = float(leg["exit_remaining"])
                self._live_exit_inflight_trade_ids.add(trade_id)
                claimed = True
                self._persist_state(state)
                self.store.record_runtime_event(
                    run_id=self.run_id or "live-exit-worker",
                    event_type="live_exit_worker_started",
                    market_key=str(state["market_key"]), venue="polymarket",
                    details={"trade_id": trade_id, "leg": leg_name,
                             "reason": state.get("exit_reason"),
                             "remaining_shares": remaining,
                             "order_arrival_epoch": expected_order.get("arrival"),
                             "queue_wait_ms": round((time.time() - queued_epoch) * 1000.0, 3),
                             "state_lock_released_during_submit": True},
                )

            try:
                attempt = await self._attempt(
                    state, leg, "EXIT", remaining, expected_order, None, {}, market=market,
                )
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            except Exception as exc:
                if expected_order.get("live_result_recorded"):
                    result = expected_order.get("live_result") or {}
                    attempt = (
                        float(result.get("filled", 0.0)),
                        float(result.get("notional", 0.0)),
                        float(result.get("fee", 0.0)),
                    )
                else:
                    async with self._timed_process_lock("live_exit_worker_error", market_key=str(state.get("market_key"))):
                        if self._live_exit_inflight(trade_id):
                            if expected_order.get("live_api_started"):
                                state["live_order_unknown"] = True
                                state["live_order_unknown_phase"] = "EXIT"
                                state["status"] = "UNRESOLVED_LIVE_ORDER"
                                expected_order["live_unknown"] = True
                                diagnostic_fields = getattr(exc, "diagnostic_fields", {})
                                order_id = str(
                                    diagnostic_fields.get("order_id")
                                    or expected_order.get("live_fak_order_id")
                                    or ""
                                )
                                expected_order["live_unknown_epoch"] = time.time()
                                if str(expected_order.get("live_order_type") or "FAK").upper() == "FAK":
                                    expected_order["live_fak_order_id"] = order_id or None
                                    expected_order["live_fak_reconcile_pending"] = True
                                    expected_order["live_fak_reconcile_side"] = "SELL"
                                    expected_order["live_fak_reconcile_phase"] = "EXIT"
                                    expected_order["live_fak_reconcile_requested"] = remaining
                                    expected_order["live_fak_reconcile_next_epoch"] = time.time()
                                    expected_order["live_fak_reconcile_attempts"] = int(
                                        expected_order.get("live_fak_reconcile_attempts", 0)
                                    )
                                expected_order["live_response_details"] = {
                                    **dict(expected_order.get("live_response_details") or {}),
                                    **diagnostic_fields,
                                    "error_type": type(exc).__name__,
                                    "outcome_unknown": True,
                                }
                                self._event(
                                    state, "EXIT", leg, side="SELL", status="LIVE_ORDER_UNKNOWN",
                                    requested=remaining, order=expected_order,
                                    details={"error_type": type(exc).__name__,
                                             "reason": "exit worker failed after persisting API intent",
                                             "account_reconciliation_scheduled": bool(
                                                 expected_order.get("live_fak_reconcile_pending")
                                             ),
                                             **diagnostic_fields},
                                )
                            else:
                                self._event(
                                    state, "EXIT", leg, side="SELL",
                                    status="LIVE_EXIT_WORKER_RETRY", requested=remaining,
                                    order=expected_order,
                                    details={"error_type": type(exc).__name__,
                                             "reason": "failure occurred before API intent; safe to retry"},
                                )
                                expected_order["arrival"] = (
                                    time.time() + self.settings.live_retry_interval_seconds
                                )
                                expected_order["wait_for_update"] = True
                            self._persist_state(state)
                    outcome = "error_unknown" if state.get("live_order_unknown") else "pre_submit_retry"
                    return

            async with self._timed_process_lock("live_exit_worker_result", market_key=str(state.get("market_key"))):
                if (not self._live_exit_inflight(trade_id)
                        or leg.get("exit_order") is not expected_order):
                    outcome = "state_changed"
                    return
                if state.get("live_order_unknown"):
                    outcome = "order_unknown"
                    return
                if attempt is None:
                    if time.time() >= float(state["market_end"]):
                        self._mark_live_settlement_pending(state, "exit attempt completed after market end")
                        outcome = "settlement_pending"
                    else:
                        outcome = "waiting_for_fresh_book"
                        self._persist_state(state)
                    return

                filled, proceeds, fee = attempt
                leg["exit_remaining"] = max(0.0, remaining - filled)
                leg["exit_filled"] = float(leg.get("exit_filled", 0.0)) + filled
                leg["exit_value"] = float(leg.get("exit_value", 0.0)) + proceeds
                leg["exit_fee"] = float(leg.get("exit_fee", 0.0)) + fee
                state["exit_value"] = float(state.get("exit_value", 0.0)) + proceeds
                state["exit_fee"] = float(state.get("exit_fee", 0.0)) + fee

                all_exited = all(float(item.get("exit_remaining", 0.0)) <= 1e-9
                                 for item in state["legs"])
                if all_exited:
                    state["status"] = "CLOSED"
                    state["net_pnl"] = (
                        float(state["exit_value"]) - float(state["exit_fee"])
                        - float(state["entry_cost"]) - float(state["entry_fee"])
                    )
                    state["closed_at"] = datetime.now(timezone.utc).isoformat()
                    self._event(
                        state, "TRADE", None, side=None, status="CLOSED",
                        details={"position_shares": state["paired_shares"],
                                 "entry_cost": state["entry_cost"],
                                 "entry_fee": state["entry_fee"],
                                 "exit_value": state["exit_value"],
                                 "exit_fee": state["exit_fee"],
                                 "net_pnl": state["net_pnl"]},
                    )
                    self._persist_state(state)
                    if self.active is state:
                        self.active = None
                    outcome = "closed"
                    return
                if time.time() >= float(state["market_end"]):
                    self._mark_live_settlement_pending(
                        state, "exit remainder remained when market ended",
                    )
                    outcome = "settlement_pending"
                    return
                self._persist_state(state)

            if float(leg["exit_remaining"]) > 1e-9:
                async with self._timed_process_lock("live_exit_retry", market_key=str(state.get("market_key"))):
                    if (not self._live_exit_inflight(trade_id)
                            or leg.get("exit_order") is not expected_order):
                        outcome = "state_changed_before_retry_schedule"
                        return
                    if time.time() >= float(state["market_end"]):
                        self._mark_live_settlement_pending(
                            state, "exit retry could not be scheduled before market end",
                        )
                        outcome = "settlement_pending"
                        return
                    immediate_retry = bool(
                        expected_order.get("live_accepted")
                        or expected_order.get("live_fak_no_fill_confirmed")
                    )
                    leg["exit_order"] = self._schedule(
                        state, leg, "EXIT", float(leg["exit_remaining"]), time.time(),
                        wait_for_update=not immediate_retry,
                    )
                    self._queue_live_sell_presign(
                        state, leg, market, float(leg["exit_remaining"]),
                        expected_order.get("live_tick_size"),
                    )
                    self._persist_state(state)
                    outcome = "retry_scheduled"
        finally:
            if claimed:
                async with self._timed_process_lock("live_exit_worker_cleanup", market_key=str(state.get("market_key"))):
                    self._live_exit_inflight_trade_ids.discard(trade_id)
                    self.store.record_runtime_event(
                        run_id=self.run_id or "live-exit-worker",
                        event_type="live_exit_worker_finished",
                        market_key=str(state["market_key"]), venue="polymarket",
                        details={"trade_id": trade_id, "outcome": outcome,
                                 "elapsed_ms": round((time.monotonic() - started) * 1000.0, 3),
                                 "queue_to_finish_ms": round((time.time() - queued_epoch) * 1000.0, 3)},
                    )

    async def stop_live_background_tasks(self) -> None:
        entry_tasks = [task for task in self._live_entry_tasks.values() if not task.done()]
        if entry_tasks:
            _, pending = await asyncio.wait(
                entry_tasks,
                timeout=min(15.0, max(10.0, self.settings.live_buy_unfilled_ttl_seconds + 8.0)),
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=2.0)
        tasks: list[asyncio.Task[Any]] = []
        tasks.extend(task for task in self._live_exit_tasks.values() if not task.done())
        tasks.extend(task for task in self._live_gtc_discovery_tasks.values() if not task.done())
        tasks.extend(task for task in self._live_fak_reconcile_tasks.values() if not task.done())
        tasks.extend(task for task in self._live_presign_tasks if not task.done())
        for task in tasks:
            task.cancel()
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=3.0)
            for task in done:
                if not task.cancelled():
                    task.exception()
            if pending:
                print(f"LIVE BACKGROUND WORKERS STILL DRAINING count={len(pending)}", flush=True)

    async def _advance(self, snapshot: PairSnapshot | None,
                       snapshot_ids: dict[tuple[str, str], int],
                       bucket: int, spread: float | None, *,
                       market: Market | None = None,
                       captured_at: datetime | None = None) -> str | None:
        assert self.active is not None
        state = self.active
        market = market or (None if snapshot is None else snapshot.polymarket)
        captured_at = captured_at or (None if snapshot is None else snapshot.captured_at)
        if market is None or captured_at is None:
            raise RuntimeError("trade advancement needs a market and decision timestamp")
        now = captured_at.timestamp()
        if state.get("live_order_unknown"):
            if self.live_executor is not None and now >= float(state["market_end"]):
                if self._has_pending_live_entry(state):
                    self._mark_live_gtc_pending_at_market_end(state, now)
                else:
                    state["status"] = "UNRESOLVED_LIVE_ORDER"
                    state["closed_at"] = None
                    self._event(
                        state, "TRADE", None, side=None,
                        status="LIVE_ORDER_DETACHED_AT_MARKET_END",
                        details={"live_order_unknown_phase": state.get("live_order_unknown_phase"),
                                 "reconciliation_continues": True,
                                 "new_markets_allowed": True},
                    )
                    self._persist_state(state)
                    self.active = None
                return "LIVE ORDER RECONCILIATION pending; expired market detached and new markets continue"
            return "LIVE ORDER RESULT UNKNOWN; this market is waiting for account reconciliation"
        dirty = False
        if now >= float(state["market_end"]):
            if self.live_executor is not None and self._has_pending_live_entry(state):
                self._mark_live_gtc_pending_at_market_end(state, now)
                return "LIVE ENTRY RECONCILIATION pending; expired GTC order remains under account reconciliation"
            if self.live_executor is not None and float(state.get("paired_shares", 0.0)) > 1e-9:
                max_hold_due = state.get("max_hold_due_epoch")
                if (not state.get("exit_triggered") and max_hold_due is not None
                        and float(max_hold_due) <= now):
                    state["exit_triggered"] = True
                    state["exit_reason"] = "max_hold"
                    state["exit_trigger_epoch"] = now
                    self._event(
                        state, "EXIT_RULE", None, side=None,
                        status="MAX_HOLD_EXIT_MISSED_MARKET_END",
                        details={"reason": "max_hold", "trigger_source": "max_hold_clock",
                                 "execution_book_source": "polymarket_only",
                                 "reason_not_closed": "market ended with position still open"},
                    )
                state["status"] = "SETTLEMENT_PENDING"
                state["live_position_unresolved"] = False
                state["closed_at"] = captured_at.isoformat()
                self._event(state, "TRADE", None, side=None,
                            status="SETTLEMENT_PENDING",
                            details={"position_shares": state["paired_shares"],
                                     "settlement_status": "awaiting Polymarket market resolution",
                                     "new_trading_allowed": True})
                self._persist()
                self.active = None
                settled = self._settle_known_resolution(state)
                return ("LIVE SETTLED; official market winner was already recorded" if settled else
                        "LIVE SETTLEMENT PENDING; expired position is retained and new trading continues")
            if self.live_executor is not None:
                state["status"] = "NO_POSITION"
                state["net_pnl"] = 0.0
                state["closed_at"] = (datetime.now(timezone.utc).isoformat()
                                       if self.live_executor is not None
                                       else captured_at.isoformat())
                self._event(state, "TRADE", None, side=None, status="NO_POSITION",
                            details={"position_shares": 0.0,
                                     "reason": "market ended before a live fill"})
                self._persist()
                self.active = None
                return "LIVE NO POSITION; market ended before a live fill"
            state["status"] = "UNRESOLVED_AT_END"
            state["closed_at"] = captured_at.isoformat()
            unresolved_details = {"net_pnl": None}
            unresolved_details[
                "position_shares" if state.get("execution_mode") == "polymarket_only"
                else "paired_shares"
            ] = state.get("paired_shares", 0)
            self._event(state, "TRADE", None, side=None, status="UNRESOLVED_AT_END",
                        details=unresolved_details)
            self._persist()
            self.active = None
            return f"{self._status_prefix} UNRESOLVED: market ended with open/pending exposure; settlement not assumed"

        for leg in state["legs"]:
            if leg["entry_attempted"]:
                continue
            order = leg["entry_order"]
            attempt = await self._attempt(
                state, leg, "ENTRY", float(order["requested"]), order, snapshot, snapshot_ids,
                market=market,
            )
            if state.get("live_order_unknown"):
                return "LIVE ORDER RESULT UNKNOWN; this market is waiting for account reconciliation"
            if attempt is not None:
                filled, notional, fee = attempt
                leg["entry_attempted"] = True
                leg["entry_filled"] = filled
                leg["entry_cost"] = notional
                leg["entry_fee"] = fee
                leg["entry_fill_epoch"] = (
                    float(order.get("live_fill_epoch", order["arrival"])) if filled > 0 else None
                )
                dirty = True

        if not state["entries_resolved"] and all(leg["entry_attempted"] for leg in state["legs"]):
            paired = min(float(leg["entry_filled"]) for leg in state["legs"])
            state["paired_shares"] = paired
            if state.get("execution_mode") == "polymarket_only":
                state["position_shares"] = paired
            state["entry_cost"] = sum(float(leg["entry_cost"]) for leg in state["legs"])
            state["entry_fee"] = sum(float(leg["entry_fee"]) for leg in state["legs"])
            for leg in state["legs"]:
                leg["unwind_remaining"] = max(0.0, float(leg["entry_filled"]) - paired)
                if leg["unwind_remaining"] > 1e-9:
                    leg["unwind_order"] = self._schedule(state, leg, "UNWIND", leg["unwind_remaining"], now)
            fills = [leg["entry_fill_epoch"] for leg in state["legs"] if leg["entry_fill_epoch"] is not None]
            if state.get("exit_mode") == "directional_ou_threshold":
                state["max_hold_due_epoch"] = (
                    None if paired <= 1e-9 else max(fills) + self.settings.max_hold_seconds
                )
                if self.live_executor is not None and paired > 1e-9:
                    state["max_hold_due_epoch"] = min(
                        float(state["max_hold_due_epoch"]), float(state["market_end"]) - 1.0
                    )
            else:
                # Keep an already-open trade from an older version on its stored timer.
                state["hold_due_epoch"] = (
                    None if paired <= 1e-9 else max(fills) + self.settings.hold_seconds
                )
            for leg in state["legs"]:
                leg["exit_remaining"] = paired
            state["entries_resolved"] = True
            dirty = True
            if self.live_executor is not None and paired > 1e-9:
                for leg in state["legs"]:
                    self._queue_live_sell_presign(
                        state, leg, market, float(leg["exit_remaining"]),
                        leg.get("entry_order", {}).get("live_tick_size"),
                    )
            if paired + 1e-9 < float(state["requested_shares"]):
                if state.get("execution_mode") == "polymarket_only":
                    self._event(
                        state, "POSITION", None, side=None,
                        status="POSITION_PARTIAL" if paired > 0 else "NO_POSITION",
                        requested=float(state["requested_shares"]), filled=paired,
                        details={"venue": "polymarket", "outcome": state["legs"][0]["outcome"]},
                    )
                else:
                    self._event(state, "PAIR", None, side=None,
                                status="PAIR_PARTIAL" if paired > 0 else "NO_PAIR",
                                requested=float(state["requested_shares"]), filled=paired,
                                details={"leg_entry_fills": [leg["entry_filled"] for leg in state["legs"]]})

        for leg in state["legs"]:
            remaining = float(leg["unwind_remaining"])
            order = leg.get("unwind_order")
            if remaining > 1e-9 and order is not None:
                attempt = await self._attempt(
                    state, leg, "UNWIND", remaining, order, snapshot, snapshot_ids,
                    market=market,
                )
                if state.get("live_order_unknown"):
                    return "LIVE ORDER RESULT UNKNOWN; this market is waiting for account reconciliation"
                if state.get("status") == "SETTLEMENT_PENDING":
                    return ("LIVE SETTLEMENT PENDING; below-minimum remainder retained for market resolution; "
                            "new trading continues")
                if attempt is not None:
                    filled, proceeds, fee = attempt
                    leg["unwind_remaining"] = max(0.0, remaining - filled)
                    leg["exit_filled"] = float(leg.get("exit_filled", 0.0)) + filled
                    leg["exit_value"] = float(leg.get("exit_value", 0.0)) + proceeds
                    leg["exit_fee"] = float(leg.get("exit_fee", 0.0)) + fee
                    state["exit_value"] += proceeds
                    state["exit_fee"] += fee
                    dirty = True
                    if leg["unwind_remaining"] > 1e-9:
                        leg["unwind_order"] = self._schedule(
                            state, leg, "UNWIND", leg["unwind_remaining"],
                            time.time() if self.live_executor is not None else now,
                            wait_for_update=True,
                        )

        exit_trigger_message = None
        live_exit_dispatch = False
        if state["entries_resolved"] and float(state["paired_shares"]) > 1e-9:
            if state.get("exit_mode") == "directional_ou_threshold":
                exit_ou_score = None
                if spread is not None:
                    ou_stats = self.calibration.ou_stats(
                        bucket, before_market_start=(
                            snapshot.start_at if snapshot is not None else market.start_at
                        ).timestamp(),
                        window=self.settings.history_window_markets,
                    )
                    if ou_stats.count >= self.settings.min_history_markets:
                        exit_ou_score = ou_stats.score(spread)
                entry_score = float(state.get("ou_score", 0.0))
                threshold = float(state.get("ou_exit_threshold", self.settings.ou_exit_threshold))
                crossed = (
                    exit_ou_score is not None
                    and (exit_ou_score >= -threshold if entry_score < 0.0
                         else exit_ou_score <= threshold)
                )
                max_hold_due = state.get("max_hold_due_epoch")
                max_hold_reached = max_hold_due is not None and now >= float(max_hold_due)
                if not state.get("exit_triggered") and (crossed or max_hold_reached):
                    reason = "ou_threshold" if crossed else "max_hold"
                    state["exit_triggered"] = True
                    state["exit_reason"] = reason
                    state["exit_trigger_epoch"] = now
                    state["exit_trigger_ou_score"] = exit_ou_score
                    status = "OU_EXIT_TRIGGERED" if crossed else "MAX_HOLD_EXIT_TRIGGERED"
                    self._event(
                        state, "EXIT_RULE", None, side=None, status=status,
                        details={"reason": reason, "ou_score": exit_ou_score,
                                 "exit_threshold": threshold,
                                 "trigger_source": "paired_ou_signal" if crossed else "max_hold_clock",
                                 "execution_book_source": "polymarket_only",
                                 "max_hold_seconds": self.settings.max_hold_seconds},
                    )
                    exit_trigger_message = (
                        f"{self._status_prefix} EXIT TRIGGER {reason}; OU="
                        f"{exit_ou_score:+.2f}" if exit_ou_score is not None
                        else f"{self._status_prefix} EXIT TRIGGER {reason}; OU unavailable"
                    )
                    dirty = True
                    live_exit_dispatch = self.live_executor is not None
                if state.get("exit_triggered"):
                    for leg in state["legs"]:
                        if leg["exit_order"] is None and float(leg["exit_remaining"]) > 1e-9:
                            leg["exit_order"] = self._schedule(
                                state, leg, "EXIT", float(leg["exit_remaining"]),
                                time.time() if self.live_executor is not None else now,
                            )
                            dirty = True
                            live_exit_dispatch = self.live_executor is not None
            else:
                legacy_due = state.get("hold_due_epoch")
                if (legacy_due is not None and now >= float(legacy_due)
                        and all(leg["exit_order"] is None for leg in state["legs"])):
                    for leg in state["legs"]:
                        if float(leg["exit_remaining"]) > 1e-9:
                            leg["exit_order"] = self._schedule(
                                state, leg, "EXIT", float(leg["exit_remaining"]),
                                time.time() if self.live_executor is not None else now,
                            )
                            dirty = True
                            live_exit_dispatch = self.live_executor is not None
            for leg in state["legs"]:
                remaining = float(leg["exit_remaining"])
                order = leg.get("exit_order")
                if remaining > 1e-9 and order is not None:
                    if self.live_executor is not None:
                        if snapshot is None or live_exit_dispatch:
                            self._queue_live_exit_attempt(state, leg, order, market)
                        continue
                    attempt = await self._attempt(
                        state, leg, "EXIT", remaining, order, snapshot, snapshot_ids,
                        market=market,
                    )
                    if state.get("live_order_unknown"):
                        return "LIVE ORDER RESULT UNKNOWN; this market is waiting for account reconciliation"
                    if state.get("status") == "SETTLEMENT_PENDING":
                        return ("LIVE SETTLEMENT PENDING; below-minimum remainder retained for market resolution; "
                                "new trading continues")
                    if attempt is not None:
                        filled, proceeds, fee = attempt
                        leg["exit_remaining"] = max(0.0, remaining - filled)
                        leg["exit_filled"] = float(leg.get("exit_filled", 0.0)) + filled
                        leg["exit_value"] = float(leg.get("exit_value", 0.0)) + proceeds
                        leg["exit_fee"] = float(leg.get("exit_fee", 0.0)) + fee
                        state["exit_value"] += proceeds
                        state["exit_fee"] += fee
                        dirty = True
                        if leg["exit_remaining"] > 1e-9:
                            if self.live_executor is not None:
                                await self._prepare_live_sell_order(
                                    state, leg, market, float(leg["exit_remaining"]),
                                    order.get("live_tick_size"),
                                )
                            immediate_live_retry = (
                                self.live_executor is not None
                                and (order.get("live_accepted")
                                     or order.get("live_fak_no_fill_confirmed"))
                            )
                            leg["exit_order"] = self._schedule(
                                state, leg, "EXIT", leg["exit_remaining"],
                                time.time() if self.live_executor is not None else now,
                                wait_for_update=not immediate_live_retry,
                            )

        if state["entries_resolved"]:
            done_unwind = all(float(leg["unwind_remaining"]) <= 1e-9 for leg in state["legs"])
            done_exit = all(float(leg["exit_remaining"]) <= 1e-9 for leg in state["legs"])
            if done_unwind and (done_exit or float(state["paired_shares"]) <= 1e-9):
                has_position = float(state["paired_shares"]) > 1e-9
                state["status"] = (
                    "CLOSED" if has_position else
                    "NO_POSITION" if state.get("execution_mode") == "polymarket_only" else "NO_PAIR"
                )
                state["net_pnl"] = (float(state["exit_value"]) - float(state["exit_fee"])
                                    - float(state["entry_cost"]) - float(state["entry_fee"]))
                state["closed_at"] = captured_at.isoformat()
                self._event(state, "TRADE", None, side=None, status=state["status"],
                            details={"position_shares" if state.get("execution_mode") == "polymarket_only"
                                     else "paired_shares": state["paired_shares"],
                                     "entry_cost": state["entry_cost"],
                                     "entry_fee": state["entry_fee"],
                                     "exit_value": state["exit_value"],
                                     "exit_fee": state["exit_fee"],
                                     "net_pnl": state["net_pnl"]})
                self._persist()
                self.active = None
                if state.get("execution_mode") == "polymarket_only":
                    return (f"{self._status_prefix} {state['status']} {state['signal']} "
                            f"position={state['paired_shares']:g}/{state['requested_shares']:g} "
                            f"netPnL=${state['net_pnl']:+.4f}; Polymarket only; "
                            f"{'real Polymarket fills' if self.live_executor is not None else 'paper fills'}")
                return (f"{self._status_prefix} {state['status']} {state['signal']} paired={state['paired_shares']:g}/"
                        f"{state['requested_shares']:g} netPnL=${state['net_pnl']:+.4f}; "
                        f"{'real Polymarket fills' if self.live_executor is not None else 'no real orders sent'}")

        if dirty:
            self._persist()
        if not state["entries_resolved"]:
            done = sum(bool(leg["entry_attempted"]) for leg in state["legs"])
            return (f"{self._status_prefix} ENTRY pending {done}/1 Polymarket order; waiting for GTC cancel and fill reconciliation"
                    if state.get("execution_mode") == "polymarket_only"
                    else f"{self._status_prefix} ENTRY pending {done}/2 legs; waiting for 330ms order arrival")
        if any(float(leg["unwind_remaining"]) > 1e-9 for leg in state["legs"]):
            quantity_label = "position" if state.get("execution_mode") == "polymarket_only" else "paired"
            return (f"{self._status_prefix} SINGLE-LEG EXPOSURE; {quantity_label}={state['paired_shares']:g}, "
                    f"unwind pending; IOC retry waits for a new book after 330ms arrival")
        if state["paired_shares"] > 1e-9:
            is_single_leg = state.get("execution_mode") == "polymarket_only"
            quantity_label = "position" if is_single_leg else "paired"
            if state.get("exit_mode") == "directional_ou_threshold":
                if state.get("exit_triggered"):
                    if self.live_executor is not None:
                        return (f"LIVE EXIT pending {quantity_label}={state['paired_shares']:g}; "
                                "resending remaining shares immediately after an acknowledged zero/partial FAK fill")
                    return exit_trigger_message or (
                        f"{self._status_prefix} EXIT pending {quantity_label}={state['paired_shares']:g}; "
                        "waiting for 330ms order arrival"
                    )
                due = state.get("max_hold_due_epoch")
                left = None if due is None else max(0.0, float(due) - now)
                return (f"{self._status_prefix} OPEN {quantity_label}={state['paired_shares']:g}; waiting for directional "
                        f"OU exit at ±{self.settings.ou_exit_threshold:g}; "
                        f"max hold in {left:.1f}s" if left is not None
                        else f"{self._status_prefix} OPEN {quantity_label}={state['paired_shares']:g}; waiting for OU exit")
            due = state.get("hold_due_epoch")
            quantity_label = "position" if state.get("execution_mode") == "polymarket_only" else "paired"
            if due is not None and now < float(due):
                return f"{self._status_prefix} OPEN {quantity_label}={state['paired_shares']:g}; legacy timer {float(due)-now:.1f}s"
            return f"{self._status_prefix} EXIT pending {quantity_label}={state['paired_shares']:g}; waiting for 330ms order arrival"
        return (f"{self._status_prefix} NO POSITION; Polymarket entry filled zero; recording complete"
                if state.get("execution_mode") == "polymarket_only"
                else f"{self._status_prefix} NO PAIR; entry filled zero; recording complete")

    def _persist_state(self, state: dict[str, Any]) -> None:
        """Persist any trade by its durable trade_id, including detached trades."""
        if state.get("trade_id") is not None:
            self.store.update_shadow_trade(state)

    def _persist(self) -> None:
        if self.active is not None:
            self._persist_state(self.active)

    async def resolve_market_async(self, market: Market, winning_asset_id: str,
                                   resolved_at: datetime) -> bool:
        async with self._timed_process_lock("market_resolution", market_key=market.key):
            return self.resolve_market(market, winning_asset_id, resolved_at)

    def resolve_market(self, market: Market, winning_asset_id: str,
                       resolved_at: datetime) -> bool:
        """Record an official winner and settle a position once expiry is known."""
        winner = str(winning_asset_id)
        resolved_text = resolved_at.astimezone(timezone.utc).isoformat()
        self.store.record_market_resolution(
            market_key=market.key, condition_id=market.condition_id,
            winning_asset_id=winner, resolved_at=resolved_text,
        )
        self.store.record_runtime_event(
            run_id=self.run_id or "resolution-watcher", event_type="market_resolution",
            market_key=market.key, venue="polymarket",
            details={"condition_id": market.condition_id, "winning_asset_id": winner,
                     "resolved_at": resolved_text}, event_at=resolved_text,
        )
        if winner not in {market.up_token, market.down_token}:
            return False

        active = self.active
        if (
            active is not None
            and (self._live_exit_inflight(int(active.get("trade_id", -1)))
                 or self._live_exit_worker_running(int(active.get("trade_id", -1))))
            and active.get("market_key") == market.key
        ):
            # Persist the resolution above, then let the exit worker apply its
            # definitive order response before settling any remaining shares.
            return False

        state = self.store.shadow_trade_for_market(market.key)
        if state is None or state.get("status") == "SETTLED":
            return False
        trade_id = int(state.get("trade_id", -1))
        if self._live_exit_inflight(trade_id) or self._live_exit_worker_running(trade_id):
            return False
        stored_market = state.get("resolution_market") or {}
        if str(stored_market.get("condition_id") or "").lower() != str(market.condition_id or "").lower():
            return False
        if state.get("status") != "SETTLEMENT_PENDING":
            active = self.active
            if (active is None or active.get("market_key") != market.key
                    or active.get("live_order_unknown")
                    or resolved_at.timestamp() < float(active.get("market_end", float("inf")))):
                return False
            state = active
            state["status"] = "SETTLEMENT_PENDING"
            state["live_position_unresolved"] = False
            state["closed_at"] = resolved_text
            self.store.update_shadow_trade(state)
            self.active = None
        self._apply_market_resolution(state, winner, resolved_text)
        return True

    def _settle_known_resolution(self, state: dict[str, Any]) -> bool:
        resolution = self.store.market_resolution(str(state["market_key"]))
        if resolution is None:
            return False
        return self._apply_market_resolution(
            state, str(resolution["winning_asset_id"]), str(resolution["resolved_at"]),
        )

    def _apply_market_resolution(self, state: dict[str, Any], winner: str,
                                  resolved_at: str) -> bool:
        if state.get("status") != "SETTLEMENT_PENDING":
            return False
        market_data = state.get("resolution_market") or {}
        tokens = {str(market_data.get("up_token") or ""), str(market_data.get("down_token") or "")}
        if winner not in tokens or "" in tokens:
            return False
        payout = 0.0
        remaining_shares = 0.0
        winning_shares = 0.0
        for leg in state.get("legs", []):
            remaining = max(0.0, float(leg.get("entry_filled", 0.0))
                            - float(leg.get("exit_filled", 0.0)))
            remaining_shares += remaining
            if str(leg.get("token_id")) == winner:
                payout += remaining
                winning_shares += remaining
        state["settlement_winning_asset_id"] = winner
        state["settlement_remaining_shares"] = remaining_shares
        state["settlement_winning_shares"] = winning_shares
        state["settlement_payout"] = payout
        state["settlement_at"] = resolved_at
        state["status"] = "SETTLED"
        state["live_order_unknown"] = False
        state.pop("live_order_unknown_phase", None)
        state["live_position_unresolved"] = False
        state["net_pnl"] = (
            float(state.get("exit_value", 0.0)) - float(state.get("exit_fee", 0.0))
            + payout - float(state.get("entry_cost", 0.0)) - float(state.get("entry_fee", 0.0))
        )
        state["closed_at"] = resolved_at
        self._event(
            state, "SETTLEMENT", None, side=None, status="SETTLED",
            details={"winning_asset_id": winner, "remaining_shares": remaining_shares,
                     "winning_shares": winning_shares, "payout": payout,
                     "entry_cost": state.get("entry_cost", 0.0),
                     "entry_fee": state.get("entry_fee", 0.0),
                     "prior_exit_value": state.get("exit_value", 0.0),
                     "prior_exit_fee": state.get("exit_fee", 0.0),
                     "net_pnl": state["net_pnl"]},
        )
        self.store.update_shadow_trade(state)
        self.store.record_runtime_event(
            run_id=self.run_id or "resolution-watcher", event_type="market_settlement_applied",
            market_key=str(state["market_key"]), venue="polymarket",
            details={"winning_asset_id": winner, "payout": payout,
                     "remaining_shares": remaining_shares, "net_pnl": state["net_pnl"]},
            event_at=resolved_at,
        )
        if self.active is state:
            self.active = None
        return True

    def _market_for_live_trade(self, state: dict[str, Any]) -> Market | None:
        market_data = state.get("resolution_market") or {}
        try:
            return Market(
                venue="polymarket",
                key=str(market_data["key"]),
                label=str(market_data["key"]),
                start_at=datetime.fromtimestamp(float(market_data["start_at"]), timezone.utc),
                end_at=datetime.fromtimestamp(float(market_data["end_at"]), timezone.utc),
                up_token=str(market_data["up_token"]),
                down_token=str(market_data["down_token"]),
                condition_id=str(market_data["condition_id"]),
            )
        except (KeyError, TypeError, ValueError, OverflowError):
            return None

    def _start_live_gtc_discovery(
        self, state: dict[str, Any], leg: dict[str, Any],
        order: dict[str, Any], market: Market,
    ) -> bool:
        if self.live_executor is None or not order.get("live_gtc_discovery_pending"):
            return False
        trade_id = int(state.get("trade_id", -1))
        current = self._live_gtc_discovery_tasks.get(trade_id)
        if current is not None and not current.done():
            return False
        task = asyncio.create_task(
            self._run_live_gtc_discovery(state, leg, order, market),
            name=f"live-gtc-discovery-{trade_id}",
        )
        self._live_gtc_discovery_tasks[trade_id] = task

        def clear_task(done: asyncio.Task[None]) -> None:
            if self._live_gtc_discovery_tasks.get(trade_id) is done:
                self._live_gtc_discovery_tasks.pop(trade_id, None)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                print(f"LIVE GTC ORDER DISCOVERY ERROR ({type(error).__name__})", flush=True)

        task.add_done_callback(clear_task)
        return True

    async def _run_live_gtc_discovery(
        self, state: dict[str, Any], leg: dict[str, Any],
        order: dict[str, Any], market: Market,
    ) -> None:
        assert self.live_executor is not None
        trade_id = int(state["trade_id"])
        intent_key = str(order.get("live_order_intent_key") or "")
        intent_epoch = float(order.get("live_intent_epoch") or time.time())
        try:
            order_id = self.live_executor.resolved_user_order_intent(intent_key)
            lookup = {"lookup_source": "USER_WS_INTENT_MATCH"} if order_id else {}
            if not order_id:
                order_id, lookup = await self.live_executor.find_open_order_for_intent(
                    asset_id=str(order.get("live_order_asset_id") or leg["token_id"]),
                    condition_id=str(order.get("live_order_condition_id") or market.condition_id),
                    side="BUY", requested_qty=float(order.get("live_order_requested") or order.get("requested", 0.0)),
                    price=order.get("live_order_intent_price"), intent_epoch=intent_epoch,
                )
            if not order_id:
                raise LiveOrderUnknown(
                    "no unique open order matched the unresolved GTC intent",
                    diagnostic_fields={"intent_key": intent_key, **lookup},
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            async with self._timed_process_lock("live_gtc_discovery_retry", market_key=str(state.get("market_key"))):
                if not order.get("live_gtc_discovery_pending"):
                    return
                attempts = int(order.get("live_gtc_discovery_attempts", 0)) + 1
                delay = min(30.0, float(2 ** min(attempts - 1, 5)))
                order["live_gtc_discovery_attempts"] = attempts
                order["live_gtc_discovery_next_epoch"] = time.time() + delay
                order["live_gtc_discovery_last_error"] = {
                    "error_type": type(exc).__name__, "message": str(exc)[:300],
                    **getattr(exc, "diagnostic_fields", {}),
                }
                self._persist_state(state)
                self.store.record_runtime_event(
                    run_id=self.run_id or "live-order-reconciler",
                    event_type="live_gtc_order_discovery_retry_scheduled",
                    market_key=str(state.get("market_key")), venue="polymarket",
                    details={"trade_id": trade_id, "intent_key": intent_key,
                             "attempt": attempts, "retry_in_seconds": delay,
                             "error_type": type(exc).__name__},
                )
            return

        async with self._timed_process_lock("live_gtc_discovery_completed", market_key=str(state.get("market_key"))):
            if not order.get("live_gtc_discovery_pending"):
                return
            order["live_gtc_order_id"] = str(order_id)
            order["live_gtc_trade_id"] = trade_id
            order["live_gtc_pending"] = True
            order["live_gtc_discovery_pending"] = False
            self.live_executor.forget_user_order_intent(intent_key)
            order["live_gtc_accepted_epoch"] = intent_epoch
            order["live_gtc_wait_seconds"] = float(
                order.get("live_gtc_wait_seconds")
                or self.settings.live_buy_unfilled_ttl_seconds
            )
            deadline_epoch = intent_epoch + float(order["live_gtc_wait_seconds"])
            order["live_gtc_deadline_epoch"] = deadline_epoch
            order["live_gtc_cancel_worker_scheduled"] = False
            order["live_gtc_next_poll_epoch"] = deadline_epoch
            accepted = {
                "order_id": str(order_id), "order_status": "unknown",
                "order_id_recovered_from_intent": True, **lookup,
            }
            order["live_gtc_accepted_details"] = accepted
            order["live_gtc_attempt_details"] = dict(order.get("live_order_intent_details") or {})
            order["live_gtc_timing_context"] = dict(order.get("live_order_timing_context") or {})
            self._persist_state(state)
            self.store.record_runtime_event(
                run_id=self.run_id or "live-order-reconciler",
                event_type="live_gtc_order_id_recovered",
                market_key=str(state.get("market_key")), venue="polymarket",
                details={"trade_id": trade_id, "order_id": str(order_id),
                         "intent_key": intent_key, **lookup},
            )
            try:
                self.live_executor.schedule_gtc_cancel_deadline(
                    str(order_id), deadline_epoch,
                )
                order["live_gtc_cancel_worker_scheduled"] = True
                self._persist_state(state)
            except Exception as exc:
                self.store.record_runtime_event(
                    run_id=self.run_id or "live-order-reconciler",
                    event_type="live_gtc_cancel_worker_schedule_failed",
                    market_key=str(state.get("market_key")), venue="polymarket",
                    details={"trade_id": trade_id, "order_id": str(order_id),
                             "deadline_epoch": deadline_epoch,
                             "error_type": type(exc).__name__},
                )
            wait_seconds = max(
                0.0, intent_epoch + float(order["live_gtc_wait_seconds"]) - time.time(),
            )
            self._start_live_entry_finalization(
                state, leg, order, market, None, None,
                dict(order.get("live_gtc_attempt_details") or {}),
                dict(order.get("live_gtc_timing_context") or {}), accepted,
                wait_seconds=wait_seconds, resumed=True,
            )

    def _start_live_fak_reconciliation(
        self, state: dict[str, Any], leg: dict[str, Any],
        order: dict[str, Any], market: Market,
    ) -> bool:
        if self.live_executor is None:
            return False
        order_id = str(order.get("live_fak_order_id") or "")
        if not order.get("live_fak_reconcile_pending"):
            return False
        if not order_id:
            intent_key = str(order.get("live_order_intent_key") or "")
            order_id = str(self.live_executor.resolved_user_order_intent(intent_key) or "")
            if not order_id:
                return False
            order["live_fak_order_id"] = order_id
        task = self._live_fak_reconcile_tasks.get(order_id)
        if task is not None and not task.done():
            started = float(order.get("live_fak_worker_started_epoch") or time.time())
            progress = float(order.get("live_fak_last_progress_epoch") or started)
            cancel_requested = float(order.get("live_fak_stale_cancel_requested_epoch") or 0.0)
            if time.time() - progress > 20.0 and time.time() - cancel_requested >= 10.0:
                task.cancel()
                order["live_fak_stale_cancel_requested_epoch"] = time.time()
                order["live_fak_reconcile_next_epoch"] = time.time()
                self._persist_state(state)
            return False
        order["live_fak_worker_started_epoch"] = time.time()
        order["live_fak_stale_cancel_requested_epoch"] = None
        self._persist_state(state)
        self.store.record_runtime_event(
            run_id=self.run_id or "live-order-reconciler",
            event_type="live_fak_reconciliation_worker_queued",
            market_key=str(state.get("market_key")), venue="polymarket",
            details={"trade_id": int(state.get("trade_id", -1)),
                     "order_id": order_id,
                     "phase": order.get("live_fak_reconcile_phase"),
                     "side": order.get("live_fak_reconcile_side")},
        )
        task = asyncio.create_task(
            self._run_live_fak_reconciliation(state, leg, order, market),
            name=f"live-fak-reconcile-{order_id[:12]}",
        )
        self._live_fak_reconcile_tasks[order_id] = task

        def clear_task(done: asyncio.Task[None]) -> None:
            if self._live_fak_reconcile_tasks.get(order_id) is done:
                self._live_fak_reconcile_tasks.pop(order_id, None)
            if done.cancelled():
                return
            error = done.exception()
            if error is not None:
                print(f"LIVE FAK RECONCILIATION WORKER ERROR ({type(error).__name__})", flush=True)

        task.add_done_callback(clear_task)
        return True

    async def _run_live_fak_reconciliation(
        self, state: dict[str, Any], leg: dict[str, Any],
        order: dict[str, Any], market: Market,
    ) -> None:
        assert self.live_executor is not None
        order_id = str(order["live_fak_order_id"])
        trade_id = int(state["trade_id"])
        phase = str(order.get("live_fak_reconcile_phase") or "EXIT").upper()
        side = str(order.get("live_fak_reconcile_side") or ("BUY" if phase == "ENTRY" else "SELL"))
        requested = float(order.get("live_fak_reconcile_requested") or order.get("requested") or 0.0)

        def record_progress(step: str, outcome: str, fields: dict[str, Any]) -> None:
            order["live_fak_last_progress_epoch"] = time.time()
            self._persist_state(state)
            self.store.record_runtime_event(
                run_id=self.run_id or "live-order-reconciler",
                event_type="live_fak_reconcile_step",
                market_key=str(state.get("market_key")), venue="polymarket",
                details={"trade_id": trade_id, "order_id": order_id,
                         "phase": phase, "step": step, "outcome": outcome, **fields},
            )
        try:
            fill, details = await self.live_executor.reconcile_fak_order(
                order_id=order_id, asset_id=str(leg["token_id"]),
                condition_id=str(market.condition_id), side=side,
                requested_qty=requested, progress_callback=record_progress,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            async with self._timed_process_lock("live_fak_reconciliation_retry", market_key=str(state.get("market_key"))):
                if not order.get("live_fak_reconcile_pending"):
                    return
                attempts = int(order.get("live_fak_reconcile_attempts", 0)) + 1
                delay = min(30.0, float(2 ** min(attempts - 1, 5)))
                now = time.time()
                order["live_fak_reconcile_attempts"] = attempts
                order["live_fak_worker_started_epoch"] = None
                order["live_fak_reconcile_next_epoch"] = now + delay
                order["live_fak_last_error"] = {
                    "error_type": type(exc).__name__, "message": str(exc)[:300],
                    **getattr(exc, "diagnostic_fields", {}),
                }
                state["live_order_unknown"] = True
                state["live_order_unknown_phase"] = phase
                state["status"] = "UNRESOLVED_LIVE_ORDER"
                self._event(
                    state, phase, leg, side=side, status="LIVE_FAK_RECONCILE_RETRY",
                    requested=requested, order=order,
                    details={"order_id": order_id, "retry_attempt": attempts,
                             "retry_in_seconds": delay, "error_type": type(exc).__name__,
                             "error": str(exc)[:300],
                             **getattr(exc, "diagnostic_fields", {})},
                )
                self._persist_state(state)
                self.store.record_runtime_event(
                    run_id=self.run_id or "live-order-reconciler",
                    event_type="live_fak_reconciliation_retry_scheduled",
                    market_key=str(state.get("market_key")), venue="polymarket",
                    details={"trade_id": trade_id, "order_id": order_id,
                             "phase": phase, "attempt": attempts,
                             "retry_in_seconds": delay,
                             "error_type": type(exc).__name__},
                )
            return

        async with self._timed_process_lock("live_fak_reconciliation_completed", market_key=str(state.get("market_key"))):
            if not order.get("live_fak_reconcile_pending"):
                return
            now = time.time()
            order["live_fak_reconcile_pending"] = False
            order["live_fak_worker_started_epoch"] = None
            order["live_fak_stale_cancel_requested_epoch"] = None
            order["live_fak_reconciled_at"] = now
            order["live_fak_reconcile_details"] = dict(details)
            filled = 0.0 if fill is None else float(fill.filled_qty)
            notional = 0.0 if fill is None else float(fill.notional)
            fee = 0.0 if fill is None else float(fill.fee)
            order["live_result_recorded"] = True
            order["live_result"] = {"filled": filled, "notional": notional, "fee": fee}
            order["live_response_details"] = {
                **dict(order.get("live_response_details") or {}), **details,
                "order_id": order_id, "reconciled_after_ambiguous_response": True,
                "trade_ids": list(details.get("trade_ids") or []),
            }
            self.live_executor.forget_user_order_intent(intent_key)
            order["live_unknown"] = False
            if state.get("live_order_unknown_phase") == phase:
                state["live_order_unknown"] = False
                state.pop("live_order_unknown_phase", None)
            remaining_key = "entry_remaining" if phase == "ENTRY" else (
                "unwind_remaining" if phase == "UNWIND" else "exit_remaining"
            )
            if phase == "ENTRY":
                leg["entry_attempted"] = True
                leg["entry_filled"] = filled
                leg["entry_cost"] = notional
                leg["entry_fee"] = fee
                fill_epoch = float(order.get("live_unknown_epoch") or order.get("live_submit_epoch") or now) if filled > 1e-9 else None
                leg["entry_fill_epoch"] = fill_epoch
                order["live_fill_epoch"] = fill_epoch
                leg["exit_remaining"] = filled
                state["entries_resolved"] = True
                state["paired_shares"] = filled
                state["position_shares"] = filled
                state["entry_cost"] = notional
                state["entry_fee"] = fee
                if filled > 1e-9:
                    hold_due = min(
                        (fill_epoch or now) + self.settings.max_hold_seconds,
                        float(state["market_end"]) - 1.0,
                    )
                    state["max_hold_due_epoch"] = hold_due
                    state["status"] = "OPEN"
                    if now < float(state["market_end"]):
                        self._queue_live_sell_presign(
                            state, leg, market, filled, order.get("live_tick_size"),
                        )
                else:
                    state["status"] = "NO_POSITION"
                    state["net_pnl"] = 0.0
                    state["closed_at"] = datetime.now(timezone.utc).isoformat()
                self._event(
                    state, "ENTRY", leg, side="BUY",
                    status="LIVE_FAK_RECONCILED" if filled > 1e-9 else "LIVE_FAK_ZERO_FILL_CONFIRMED",
                    requested=requested, filled=filled, notional=notional, fee=fee,
                    order=order, details={**details, "order_id": order_id,
                                         "accounting_source": "account_trade_reconciliation"},
                )
                if filled > 1e-9 and now >= float(state["market_end"]):
                    self._apply_detached_live_entry_result(state, leg, order, filled, notional, fee)
                elif filled <= 1e-9:
                    if self.active is state:
                        self.active = None
                    self._event(
                        state, "TRADE", None, side=None, status="NO_POSITION",
                        details={"reason": "ambiguous BUY FAK reconciled to terminal zero fill",
                                 "order_id": order_id},
                    )
            else:
                filled = min(filled, float(leg.get(remaining_key, 0.0)))
                leg[remaining_key] = max(0.0, float(leg.get(remaining_key, 0.0)) - filled)
                leg["exit_filled"] = float(leg.get("exit_filled", 0.0)) + (filled if phase == "EXIT" else 0.0)
                leg["exit_value"] = float(leg.get("exit_value", 0.0)) + (notional if phase == "EXIT" else 0.0)
                leg["exit_fee"] = float(leg.get("exit_fee", 0.0)) + (fee if phase == "EXIT" else 0.0)
                if phase == "EXIT":
                    state["exit_value"] = float(state.get("exit_value", 0.0)) + notional
                    state["exit_fee"] = float(state.get("exit_fee", 0.0)) + fee
                self._event(
                    state, phase, leg, side="SELL",
                    status="LIVE_FAK_RECONCILED" if filled > 1e-9 else "LIVE_FAK_ZERO_FILL_CONFIRMED",
                    requested=requested, filled=filled, notional=notional, fee=fee,
                    order=order, details={**details, "order_id": order_id,
                                         "accounting_source": "account_trade_reconciliation"},
                )
                remaining_total = sum(float(item.get("exit_remaining", 0.0)) for item in state["legs"])
                if phase == "EXIT" and remaining_total <= 1e-9:
                    state["status"] = "CLOSED"
                    state["net_pnl"] = (
                        float(state.get("exit_value", 0.0)) - float(state.get("exit_fee", 0.0))
                        - float(state.get("entry_cost", 0.0)) - float(state.get("entry_fee", 0.0))
                    )
                    state["closed_at"] = datetime.now(timezone.utc).isoformat()
                    if self.active is state:
                        self.active = None
                    self._event(state, "TRADE", None, side=None, status="CLOSED",
                                details={"reason": "reconciled FAK exit fills",
                                         "net_pnl": state["net_pnl"]})
                elif now >= float(state["market_end"]):
                    self._mark_live_settlement_pending(
                        state, "FAK result reconciled after market end",
                    )
                else:
                    state["status"] = "OPEN"
                    if phase == "EXIT" and float(leg.get("exit_remaining", 0.0)) > 1e-9:
                        leg["exit_order"] = self._schedule(
                            state, leg, "EXIT", float(leg["exit_remaining"]), now,
                            wait_for_update=True,
                        )
                        self._queue_live_sell_presign(
                            state, leg, market, float(leg["exit_remaining"]),
                            order.get("live_tick_size"),
                        )
            self._persist_state(state)
            self.store.record_runtime_event(
                run_id=self.run_id or "live-order-reconciler",
                event_type="live_fak_reconciliation_completed",
                market_key=str(state.get("market_key")), venue="polymarket",
                details={"trade_id": trade_id, "order_id": order_id,
                         "phase": phase, "filled_shares": filled,
                         "fill_confirmed": fill is not None,
                         "market_ended": now >= float(state["market_end"])},
            )

    async def process_live_entry_reconciliation_due(self) -> int:
        """Reconcile persisted buy GTCs and ambiguous FAKs without market-feed dependency."""
        if self.live_executor is None:
            return 0
        now = time.time()
        queued = 0
        async with self._timed_process_lock("live_entry_reconciliation"):
            active = self.active
            if (
                active is not None
                and now >= float(active.get("market_end", float("inf")))
                and self._has_pending_live_entry(active)
            ):
                self._mark_live_gtc_pending_at_market_end(active, now)

            for stored_state in self.store.open_shadow_trades():
                trade_id = int(stored_state.get("trade_id", -1))
                state = (
                    self.active if self.active is not None
                    and int(self.active.get("trade_id", -2)) == trade_id
                    else stored_state
                )
                market = self._market_for_live_trade(state)
                for leg in state.get("legs", []):
                    order = leg.get("entry_order") or {}
                    for intent_order in (
                        order, leg.get("exit_order") or {}, leg.get("unwind_order") or {},
                    ):
                        intent_key = str(intent_order.get("live_order_intent_key") or "")
                        if (intent_key and intent_order.get("live_order_intent_started")
                                and intent_order.get("live_api_started")
                                and not intent_order.get("live_result_recorded")):
                            self.live_executor.register_user_order_intent(
                                intent_key,
                                asset_id=str(intent_order.get("live_order_asset_id") or leg["token_id"]),
                                condition_id=str(intent_order.get("live_order_condition_id")
                                                 or (None if market is None else market.condition_id) or ""),
                                side=str(intent_order.get("live_order_side") or ""),
                                requested_qty=float(intent_order.get("live_order_requested")
                                                    or intent_order.get("requested", 0.0)),
                                price=intent_order.get("live_order_intent_price"),
                                intent_epoch=float(intent_order.get("live_intent_epoch") or now),
                            )
                    fak_order = order if order.get("live_fak_reconcile_pending") else None
                    if fak_order is not None:
                        fak_due = float(fak_order.get("live_fak_reconcile_next_epoch") or 0.0)
                        if fak_due <= now:
                            if market is None:
                                fak_order["live_fak_reconcile_next_epoch"] = now + 30.0
                                self._persist_state(state)
                            elif self._start_live_fak_reconciliation(state, leg, fak_order, market):
                                queued += 1
                    for phase_key in ("exit_order", "unwind_order"):
                        sell_order = leg.get(phase_key) or {}
                        if not sell_order.get("live_fak_reconcile_pending"):
                            continue
                        fak_due = float(sell_order.get("live_fak_reconcile_next_epoch") or 0.0)
                        if fak_due > now:
                            continue
                        if market is None:
                            sell_order["live_fak_reconcile_next_epoch"] = now + 30.0
                            self._persist_state(state)
                        elif self._start_live_fak_reconciliation(state, leg, sell_order, market):
                            queued += 1
                    order_id = str(order.get("live_gtc_order_id") or "")
                    if order.get("live_gtc_discovery_pending"):
                        discovery_due = float(order.get("live_gtc_discovery_next_epoch") or 0.0)
                        if discovery_due <= now:
                            if market is None:
                                order["live_gtc_discovery_next_epoch"] = now + 30.0
                                self._persist_state(state)
                            elif self._start_live_gtc_discovery(state, leg, order, market):
                                queued += 1
                    if not order.get("live_gtc_pending") or not order_id:
                        continue
                    configured_wait = float(order.get("live_gtc_wait_seconds")
                                            or self.settings.live_buy_unfilled_ttl_seconds)
                    accepted_epoch = float(order.get("live_gtc_accepted_epoch") or 0.0)
                    deadline_epoch = float(
                        order.get("live_gtc_deadline_epoch")
                        or (accepted_epoch + configured_wait if accepted_epoch else now)
                    )
                    if not self.live_executor.has_gtc_cancel(order_id):
                        try:
                            self.live_executor.schedule_gtc_cancel_deadline(
                                order_id, deadline_epoch,
                            )
                            order["live_gtc_deadline_epoch"] = deadline_epoch
                            order["live_gtc_cancel_worker_scheduled"] = True
                            self._persist_state(state)
                        except Exception as exc:
                            self.store.record_runtime_event(
                                run_id=self.run_id or "live-entry-watchdog",
                                event_type="live_gtc_cancel_worker_schedule_failed",
                                market_key=str(state.get("market_key")), venue="polymarket",
                                details={"trade_id": trade_id, "order_id": order_id,
                                         "deadline_epoch": deadline_epoch,
                                         "error_type": type(exc).__name__},
                            )
                    due = float(order.get("live_gtc_next_poll_epoch") or 0.0)
                    if due > now:
                        continue
                    if market is None:
                        order["live_gtc_next_poll_epoch"] = now + 30.0
                        self.store.record_runtime_event(
                            run_id=self.run_id or "live-entry-watchdog",
                            event_type="live_gtc_reconcile_metadata_missing",
                            market_key=str(state.get("market_key")), venue="polymarket",
                            details={"trade_id": trade_id, "order_id": order_id,
                                     "retry_in_seconds": 30.0},
                        )
                        self._persist_state(state)
                        continue
                    accepted = dict(order.get("live_gtc_accepted_details")
                                    or order.get("live_response_details") or {})
                    accepted.setdefault("order_id", order_id)
                    attempt_details = dict(order.get("live_gtc_attempt_details") or {})
                    timing_context = dict(order.get("live_gtc_timing_context") or {})
                    wait_seconds = max(0.0, accepted_epoch + configured_wait - now) if accepted_epoch else (
                        0.0 if order.get("live_gtc_initial_wait_completed") else configured_wait
                    )
                    self._start_live_entry_finalization(
                        state, leg, order, market, None, None,
                        attempt_details, timing_context, accepted,
                        wait_seconds=wait_seconds, resumed=True,
                    )
                    queued += 1
        return queued

    def next_wakeup_epoch(self) -> float | None:
        """Return the next timer deadline that needs no WebSocket update."""
        if self.active is None:
            return None
        state = self.active
        # An unresolved live order cannot be advanced by replaying its stale
        # order deadline; keep receiving fresh market updates for later handling.
        if self.live_executor is not None and state.get("live_order_unknown"):
            return None
        due: list[float] = []
        for leg in state["legs"]:
            if not leg["entry_attempted"]:
                order = leg.get("entry_order")
                if order is not None and not order.get("wait_for_update", False):
                    due.append(float(order["arrival"]))
            for phase, remaining_key in (("unwind", "unwind_remaining"),
                                         ("exit", "exit_remaining")):
                if self.live_executor is not None and phase == "exit":
                    continue
                order = leg.get(f"{phase}_order")
                if (order is not None and float(leg.get(remaining_key, 0.0)) > 1e-9
                        and not order.get("wait_for_update", False)):
                    due.append(float(order["arrival"]))
        if (self.live_executor is None
                and state.get("entries_resolved") and float(state.get("paired_shares", 0.0)) > 1e-9
                and all(leg.get("exit_order") is None for leg in state["legs"])):
            if state.get("exit_mode") == "directional_ou_threshold":
                if not state.get("exit_triggered") and state.get("max_hold_due_epoch") is not None:
                    due.append(float(state["max_hold_due_epoch"]))
            elif state.get("hold_due_epoch") is not None:
                due.append(float(state["hold_due_epoch"]))
        return min(due) if due else None

    def next_live_exit_wakeup_epoch(self) -> float | None:
        """Return a clock deadline for a live max-hold trigger or a live exit retry."""
        state = self.active
        if (
            self.live_executor is None or state is None
            or self._live_exit_inflight(int(state.get("trade_id", -1)))
            or self._live_exit_worker_running(int(state.get("trade_id", -1)))
            or state.get("live_order_unknown")
            or not state.get("entries_resolved")
            or float(state.get("paired_shares", 0.0)) <= 1e-9
        ):
            return None
        if not state.get("exit_triggered"):
            due = state.get("max_hold_due_epoch")
            return None if due is None else float(due)
        deadlines = [
            float(leg["exit_order"]["arrival"])
            for leg in state["legs"]
            if float(leg.get("exit_remaining", 0.0)) > 1e-9
            and leg.get("exit_order") is not None
        ]
        if not deadlines and any(float(leg.get("exit_remaining", 0.0)) > 1e-9 for leg in state["legs"]):
            return time.time()
        return min(deadlines) if deadlines else None

    async def process_live_exit_due(self, market: Market | None) -> str | None:
        """Advance max-hold and live exit retries from the Polymarket book alone."""
        if self.live_executor is None:
            return None
        async with self._timed_process_lock("live_exit_due", market_key=str(self.active.get("market_key")) if self.active else None):
            state = self.active
            if (
                state is None or state.get("live_order_unknown")
                or not state.get("entries_resolved")
                or float(state.get("paired_shares", 0.0)) <= 1e-9
            ):
                return None
            trade_id = int(state.get("trade_id", -1))
            if (self._live_exit_inflight(trade_id)
                    or self._live_exit_worker_running(trade_id)):
                return "LIVE EXIT pending; independent close worker owns the request"
            now = time.time()
            if not state.get("exit_triggered"):
                due = state.get("max_hold_due_epoch")
                if state.get("exit_mode") != "directional_ou_threshold" or due is None or now < float(due):
                    return None
            if market is None or market.key != str(state["market_key"]):
                raw = state.get("resolution_market") or {}
                try:
                    market = Market(
                        venue="polymarket", key=str(raw["key"]), label=str(raw["key"]),
                        start_at=datetime.fromtimestamp(float(raw["start_at"]), timezone.utc),
                        end_at=datetime.fromtimestamp(float(raw["end_at"]), timezone.utc),
                        up_token=str(raw["up_token"]), down_token=str(raw["down_token"]),
                        condition_id=str(raw["condition_id"]),
                    )
                except (KeyError, TypeError, ValueError, OverflowError):
                    return None
            return await self._advance(
                None, {}, int(state.get("bucket", 0)), None,
                market=market, captured_at=datetime.now(timezone.utc),
            )

    @staticmethod
    def _delay_summary(state: dict[str, Any]) -> str:
        if state.get("trading_mode") == "live":
            return "immediate live submit; order API round trip is measured after sending"
        parts = []
        for leg in state["legs"]:
            order = leg["entry_order"]
            delay = order["network_ms"] + order["api_ms"] + order["jitter_ms"]
            parts.append(f"{leg['venue']}={delay:.0f}ms")
        return "fixed assumed order delay " + "/".join(parts)
