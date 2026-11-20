from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Callable

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


def _live_buy_limit(best_ask: float, slippage: float, tick_size: Decimal) -> Decimal | None:
    ask = Decimal(str(best_ask))
    tick = Decimal(str(tick_size))
    if not ask.is_finite() or not tick.is_finite() or tick <= 0 or tick >= 1:
        return None
    requested = ask + Decimal(str(slippage))
    rounded = (requested / tick).to_integral_value(rounding=ROUND_CEILING) * tick
    legal_cap = (Decimal("0.99") / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    limit = min(rounded, legal_cap)
    return limit if limit >= ask and limit > 0 else None


class ShadowEngine:
    """Single-leg paper runner with an explicitly armed Polymarket live path."""

    def __init__(self, store: Store, settings: Settings, *, run_id: str | None = None,
                 live_executor: PolymarketLiveExecutor | None = None) -> None:
        self.store = store
        self.settings = settings
        self.run_id = run_id
        self.live_executor = live_executor
        self.calibration = RollingCalibration(
            store, window=settings.history_window_markets, rv_window=settings.rv_history_window_markets,
            bucket_seconds=settings.bucket_seconds
        )
        self.active: dict[str, Any] | None = next(iter(store.open_shadow_trades()), None)
        self.rng = random.SystemRandom()
        self.current_book_provider: Callable[[str, str], Book | None] | None = None
        self._process_lock = asyncio.Lock()
        if live_executor is not None and self.active is not None:
            if self.active.get("trading_mode") != "live":
                raise ValueError("live mode will not resume an open paper/shadow trade")
            if any(leg.get("venue") != "polymarket" for leg in self.active.get("legs", [])):
                raise ValueError("live mode will not resume a non-Polymarket position")

    def process(self, snapshot: PairSnapshot, *, evaluate_signals: bool = True) -> str:
        """Synchronous compatibility path for paper callers and the demo."""
        if self.live_executor is not None:
            raise RuntimeError("use await process_async() when live execution is enabled")
        return asyncio.run(self.process_async(snapshot, evaluate_signals=evaluate_signals))

    async def process_async(self, snapshot: PairSnapshot, *, evaluate_signals: bool = True) -> str:
        async with self._process_lock:
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

        if self.active is not None:
            if self.active.get("live_order_unknown"):
                return "LIVE ORDER RESULT UNKNOWN; trading locked until the Polymarket account is reconciled"

            if self.active["market_key"] != snapshot.market_key:
                if now.timestamp() < float(self.active["market_end"]):
                    prefix = "LIVE" if self.live_executor is not None else "SHADOW"
                    return f"{prefix} OPEN trade {self.active['market_key']}; waiting for its matched market books"
                if self.live_executor is not None and float(self.active.get("paired_shares", 0.0)) > 1e-9:
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
            f"{venue}/{outcome}={book.age_seconds(book.received_at):.2f}s"
            for (venue, outcome), book in snapshot.books.items()
            if book.age_seconds(book.received_at) > self.settings.max_quote_age_seconds
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
        if self.store.has_shadow_trade(snapshot.market_key) or self.store.has_position_or_trade(snapshot.market_key):
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
            f"liveslip={self.settings.live_max_slippage:g};"

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
                f"{('immediate live FAK uses current memory book' if self.live_executor is not None else 'fixed-delay IOC uses memory book at arrival')}; "
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
            ):
                return 0
            prepared = 0
            for leg in state["legs"]:
                remaining = float(leg.get("exit_remaining", 0.0))
                if remaining > 1e-9:
                    await self._prepare_live_sell_order(
                        state, leg, market, remaining, str(tick_size),
                    )
                    prepared += 1
            return prepared

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
                    details={"note": "actual Polymarket FAK will be sent when due" if live
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

    async def _attempt(self, state: dict[str, Any], leg: dict[str, Any], phase: str,
                       remaining: float, order: dict[str, Any], snapshot: PairSnapshot | None,
                       snapshot_ids: dict[tuple[str, str], int],
                       market: Market | None = None) -> tuple[float, float, float] | None:
        if self.live_executor is not None:
            if order.get("live_result_recorded"):
                result = order["live_result"]
                return float(result["filled"]), float(result["notional"]), float(result["fee"])
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
            # evaluates it. Only a retry waits for its cooldown and a later book.
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
                if phase == "ENTRY":
                    await self.live_executor.prepare_buy_order_pool(
                        market, shares=remaining, tick_size=tick_size,
                    )
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
                                details={"minimum_live_shares": min_live_shares})
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
                assert tick_size is not None
                limit_price = _live_buy_limit(
                    book.best_ask.price, self.settings.live_max_slippage, tick_size,
                )
                if limit_price is None:
                    return 0.0, 0.0, 0.0
                estimated_filled, estimated_notional, _ = _partial_sweep(
                    book, side="BUY", shares=remaining,
                    fee_coefficient=self.settings.taker_fee_coefficient,
                    max_price=float(limit_price),
                )
                if estimated_filled <= 1e-9:
                    order["live_result_recorded"] = True
                    order["live_result"] = {"filled": 0.0, "notional": 0.0, "fee": 0.0}
                    order["live_block_reason"] = "no_visible_ask_within_limit"
                    self._persist()
                    self._event(state, phase, leg, side=side,
                                status="LIVE_ENTRY_PRICE_BLOCKED",
                                requested=remaining, filled=0.0, notional=0.0, fee=0.0,
                                book=book, snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={"best_ask": book.best_ask.price,
                                         "limit_price": str(limit_price),
                                         "estimated_book_fill": estimated_filled,
                                         "estimated_book_notional": estimated_notional})
                    return 0.0, 0.0, 0.0
            else:
                limit_price = Decimal("0.01")
                min_live_shares = max(5.0, float(market.min_order_size))
                if remaining + 1e-9 < min_live_shares:
                    state["live_position_unresolved"] = True
                    state["status"] = "UNRESOLVED_LIVE_POSITION"
                    self._event(state, phase, leg, side=side, status="LIVE_EXIT_BELOW_MINIMUM",
                                requested=remaining, book=book,
                                snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                                order=order,
                                details={"minimum_live_shares": min_live_shares,
                                         "manual_account_reconciliation_required": True})
                    self._persist()
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
                "execution_mode": "live", "order_type": "FAK", "asset_id": leg["token_id"],
                "requested_shares": remaining, "limit_price": str(limit_price),
                "post_only": False, "limit_price_basis": (
                    "best_ask_plus_configured_dollar_offset_capped_at_0.99"
                    if phase == "ENTRY" else "0.01_marketable_sell_floor"
                ),
                "book_received_at": book.received_at.timestamp(),
                "book_source_at": None if book.source_at is None else book.source_at.timestamp(),
                "book_age_ms": round(book.age_seconds(now) * 1000.0, 3),
                "live_max_slippage_usd": self.settings.live_max_slippage if phase == "ENTRY" else None,
                "tick_size": None if tick_size is None else str(tick_size),
                "fee_accounting": "configured_fee_formula_applied_to_live_vwap",
            }
            max_attempts = 2 if phase == "ENTRY" else 1
            for attempt_number in range(1, max_attempts + 1):
                if attempt_number == 2:
                    book = self._current_book(snapshot, leg)
                    now = datetime.now(timezone.utc)
                    seconds_left = float(state["market_end"]) - now.timestamp()
                    if (book.best_ask is None
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
                    assert tick_size is not None
                    retry_limit = _live_buy_limit(
                        book.best_ask.price, self.settings.live_max_slippage, tick_size,
                    )
                    retry_filled, retry_notional, _ = _partial_sweep(
                        book, side="BUY", shares=remaining,
                        fee_coefficient=self.settings.taker_fee_coefficient,
                        max_price=None if retry_limit is None else float(retry_limit),
                    )
                    if retry_limit is None or retry_filled <= 1e-9:
                        self._event(
                            state, phase, leg, side=side, status="LIVE_BUY_RETRY_SKIPPED",
                            requested=remaining, book=book, order=order,
                            details={**intent_details, "retry_count": 0,
                                     "reason": "no visible ask within refreshed limit",
                                     "best_ask": book.best_ask.price,
                                     "retry_limit_price": None if retry_limit is None else str(retry_limit),
                                     "estimated_book_fill": retry_filled,
                                     "estimated_book_notional": retry_notional},
                        )
                        break
                    limit_price = retry_limit
                    intent_details = {
                        **intent_details,
                        "limit_price": str(limit_price),
                        "book_received_at": book.received_at.timestamp(),
                        "book_source_at": None if book.source_at is None else book.source_at.timestamp(),
                        "book_age_ms": round(book.age_seconds(now) * 1000.0, 3),
                        "retry_of_limit_price": str(intent_details["limit_price"]),
                        "attempt_number": 2,
                        "retry_reason": "explicit_fak_no_fill",
                    }

                now = datetime.now(timezone.utc)
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
                order["live_api_started"] = True
                order["live_attempt_number"] = attempt_number
                order["live_intent_epoch"] = now.timestamp()
                self._persist()
                self._event(state, phase, leg, side=side, status="LIVE_ORDER_INTENT",
                            requested=remaining, book=book,
                            snapshot_id=snapshot_ids.get((leg["venue"], leg["outcome"])),
                            order=order,
                            details={**attempt_details, **timing_context,
                                     "intent_recorded_before_api": True})

                def record_signed_order(signed_details: dict[str, Any]) -> None:
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
                        accepted, fill, response_details = await self.live_executor.buy_fak(
                            asset_id=leg["token_id"], requested_qty=remaining,
                            limit_price=limit_price, on_order_signed=record_signed_order,
                            timing_context=timing_context,
                        )
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
                    state["live_order_unknown"] = True
                    state["live_order_unknown_phase"] = phase
                    state["status"] = "UNRESOLVED_LIVE_ORDER"
                    order["live_unknown"] = True
                    order["live_error_type"] = type(exc).__name__
                    self._event(state, phase, leg, side=side, status="LIVE_ORDER_UNKNOWN",
                                requested=remaining, book=book, order=order,
                                details={**attempt_details, **exc.diagnostic_fields,
                                         "error_type": type(exc).__name__,
                                         "reason": "API outcome may have reached Polymarket; automatic retry disabled"})
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
            self._event(
                state, "SELL_PRESIGN", leg, side="SELL",
                status="LIVE_SELL_PRESIGN_FAILED", requested=shares,
                details={"error_type": type(exc).__name__,
                         "reason": "sell will fall back to on-demand signing"},
            )
            self._persist()
            return
        if result.get("status") == "ready":
            self._event(
                state, "SELL_PRESIGN", leg, side="SELL",
                status="LIVE_SELL_PRESIGNED", requested=shares,
                details={**result, "limit_price": "0.01",
                         "note": "signed order only; no sell was submitted"},
            )
            self._persist()

    async def _advance(self, snapshot: PairSnapshot | None,
                       snapshot_ids: dict[tuple[str, str], int],
                       bucket: int, spread: float | None, *,
                       market: Market | None = None,
                       captured_at: datetime | None = None) -> str | None:
        assert self.active is not None
        state = self.active
        if state.get("live_order_unknown"):
            return "LIVE ORDER RESULT UNKNOWN; trading locked until the Polymarket account is reconciled"
        market = market or (None if snapshot is None else snapshot.polymarket)
        captured_at = captured_at or (None if snapshot is None else snapshot.captured_at)
        if market is None or captured_at is None:
            raise RuntimeError("trade advancement needs a market and decision timestamp")
        now = captured_at.timestamp()
        dirty = False
        if now >= float(state["market_end"]):
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

        if state.get("live_position_unresolved"):
            return "LIVE POSITION HELD UNTIL MARKET END; new entries resume on the next market"

        for leg in state["legs"]:
            if leg["entry_attempted"]:
                continue
            order = leg["entry_order"]
            attempt = await self._attempt(
                state, leg, "ENTRY", float(order["requested"]), order, snapshot, snapshot_ids,
                market=market,
            )
            if state.get("live_order_unknown"):
                return "LIVE ORDER RESULT UNKNOWN; trading locked until the Polymarket account is reconciled"
            if state.get("live_position_unresolved"):
                return "LIVE POSITION UNRESOLVED; trading locked pending account reconciliation"
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
                    await self._prepare_live_sell_order(
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
                    return "LIVE ORDER RESULT UNKNOWN; trading locked until the Polymarket account is reconciled"
                if state.get("live_position_unresolved"):
                    return "LIVE POSITION UNRESOLVED; trading locked pending account reconciliation"
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
                if state.get("exit_triggered"):
                    for leg in state["legs"]:
                        if leg["exit_order"] is None and float(leg["exit_remaining"]) > 1e-9:
                            leg["exit_order"] = self._schedule(
                                state, leg, "EXIT", float(leg["exit_remaining"]),
                                time.time() if self.live_executor is not None else now,
                            )
                            dirty = True
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
            for leg in state["legs"]:
                remaining = float(leg["exit_remaining"])
                order = leg.get("exit_order")
                if remaining > 1e-9 and order is not None:
                    attempt = await self._attempt(
                        state, leg, "EXIT", remaining, order, snapshot, snapshot_ids,
                        market=market,
                    )
                    if state.get("live_order_unknown"):
                        return "LIVE ORDER RESULT UNKNOWN; trading locked until the Polymarket account is reconciled"
                    if state.get("live_position_unresolved"):
                        return "LIVE POSITION UNRESOLVED; trading locked pending account reconciliation"
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
                            f"{'real FAK fills' if self.live_executor is not None else 'paper fills'}")
                return (f"{self._status_prefix} {state['status']} {state['signal']} paired={state['paired_shares']:g}/"
                        f"{state['requested_shares']:g} netPnL=${state['net_pnl']:+.4f}; "
                        f"{'real FAK fills' if self.live_executor is not None else 'no real orders sent'}")

        if dirty:
            self._persist()
        if not state["entries_resolved"]:
            done = sum(bool(leg["entry_attempted"]) for leg in state["legs"])
            return (f"{self._status_prefix} ENTRY pending {done}/1 Polymarket order; waiting for order response"
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

    def _persist(self) -> None:
        if self.active is not None:
            self.store.update_shadow_trade(self.active)

    async def resolve_market_async(self, market: Market, winning_asset_id: str,
                                   resolved_at: datetime) -> bool:
        async with self._process_lock:
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

        state = self.store.shadow_trade_for_market(market.key)
        if state is None or state.get("status") == "SETTLED":
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

    def next_wakeup_epoch(self) -> float | None:
        """Return the next timer deadline that needs no WebSocket update."""
        if self.active is None:
            return None
        state = self.active
        # An unresolved live order/position cannot be advanced by replaying its
        # stale order deadline. Let the feed loop keep receiving fresh markets
        # so a later market boundary or account reconciliation can unlock it.
        if self.live_executor is not None and (
            state.get("live_order_unknown") or state.get("live_position_unresolved")
        ):
            return None
        due: list[float] = []
        for leg in state["legs"]:
            if not leg["entry_attempted"]:
                order = leg.get("entry_order")
                if order is not None and not order.get("wait_for_update", False):
                    due.append(float(order["arrival"]))
            for phase, remaining_key in (("unwind", "unwind_remaining"),
                                         ("exit", "exit_remaining")):
                order = leg.get(f"{phase}_order")
                if (order is not None and float(leg.get(remaining_key, 0.0)) > 1e-9
                        and not order.get("wait_for_update", False)):
                    due.append(float(order["arrival"]))
        if (state.get("entries_resolved") and float(state.get("paired_shares", 0.0)) > 1e-9
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
            or state.get("live_order_unknown") or state.get("live_position_unresolved")
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
        async with self._process_lock:
            state = self.active
            if (
                state is None or state.get("live_order_unknown")
                or state.get("live_position_unresolved")
                or not state.get("entries_resolved")
                or float(state.get("paired_shares", 0.0)) <= 1e-9
            ):
                return None
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
