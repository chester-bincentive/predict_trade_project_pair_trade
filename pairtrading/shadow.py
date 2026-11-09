from __future__ import annotations

import json
import random
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .models import Book, PairSnapshot
from .storage import Store
from .strategy import RollingCalibration, event_bucket, mid_spread


def _iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def _partial_sweep(book: Book, *, side: str, shares: float, fee_coefficient: float) -> tuple[float, float, float]:
    levels = book.asks if side == "BUY" else book.bids
    remaining = max(0.0, shares)
    filled = notional = fee = 0.0
    for level in levels:
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


class ShadowEngine:
    """Live market-data shadow runner; never creates or sends a venue order."""

    def __init__(self, store: Store, settings: Settings, *, run_id: str | None = None) -> None:
        self.store = store
        self.settings = settings
        self.run_id = run_id
        self.calibration = RollingCalibration(
            store, window=settings.history_window_markets, rv_window=settings.rv_history_window_markets,
            bucket_seconds=settings.bucket_seconds
        )
        self.active: dict[str, Any] | None = next(iter(store.open_shadow_trades()), None)
        self.rng = random.SystemRandom()

    def process(self, snapshot: PairSnapshot, *, evaluate_signals: bool = True) -> str:
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
            if self.active["market_key"] != snapshot.market_key:
                if now.timestamp() < float(self.active["market_end"]):
                    return f"OPEN shadow trade {self.active['market_key']}; waiting for its matched market books"
                self.active["status"] = "UNRESOLVED_AT_END"
                self.active["closed_at"] = now.isoformat()
                self._event(self.active, "TRADE", None, side=None,
                            status="UNRESOLVED_AT_END",
                            details={"paired_shares": self.active.get("paired_shares", 0),
                                     "net_pnl": None})
                self._persist()
                self.active = None
            else:
                result = self._advance(snapshot, snapshot_ids)
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
        if (snapshot.end_at - now).total_seconds() < self.settings.hold_seconds:
            return f"SKIP less than {self.settings.hold_seconds}s remains in matched market"

        counts = self.calibration.counts(before_market_start=snapshot.start_at.timestamp())
        missing = [b for b, count in counts.items() if count < self.settings.min_history_markets]
        if missing:
            progress = ", ".join(f"b{b}:{counts[b]}/{self.settings.min_history_markets}" for b in missing)
            return f"DATA COLLECTION ONLY; insufficient closed-market history ({progress}); signals disabled"

        stats = self.calibration.stats(bucket, before_market_start=snapshot.start_at.timestamp())
        zscore = stats.score(spread)
        if zscore is None:
            return f"SKIP bucket={bucket} spread stdev is zero/undefined"
        if abs(zscore) < self.settings.z_entry:
            return f"HOLD bucket={bucket} spread={spread:+.4f} z={zscore:+.2f} n={stats.count}"
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
            return (f"HOLD bucket={bucket} z={zscore:+.2f}; RV10={rv10:.6f} <= "
                    f"historical q90={rv_q90:.6f} (n={rv_history_n})")
        if self.active is not None:
            return "HOLD existing shadow trade; no stacking"
        if self.store.has_shadow_trade(snapshot.market_key) or self.store.has_position_or_trade(snapshot.market_key):
            return f"HOLD one-trade-per-market already used z={zscore:+.2f}"

        if zscore < 0.0:
            signal, chosen = "LOW_SPREAD", (("polymarket", "UP"), ("binance", "DOWN"))
        else:
            signal, chosen = "HIGH_SPREAD", (("polymarket", "DOWN"), ("binance", "UP"))
        decision_epoch = now.timestamp()
        scenario_key = (
            f"feed=websocket-events;retry={self.settings.poll_seconds:g};z={self.settings.z_entry:g};"
            f"roll={self.settings.history_window_markets};minhist={self.settings.min_history_markets};"
            f"bucket={self.settings.bucket_seconds};hold={self.settings.hold_seconds};"
            f"shares={self.settings.min_shares:g};fee={self.settings.taker_fee_coefficient:g};"
            f"entry=absz>={self.settings.z_entry:g}&rv10>same-bucket-q90;"
            f"rvroll={self.settings.rv_history_window_markets};"
            f"rvmin={self.settings.min_rv_history_markets};"
            f"poly={self.settings.poly_order_network_ms:g}+{self.settings.poly_order_api_ms:g};"
            f"binance={self.settings.binance_order_network_ms:g}+{self.settings.binance_order_api_ms:g};"
            f"jitter=0..{self.settings.order_latency_jitter_ms:g};"
            "execution=latest-memory-book-at-arrival"
        )
        state: dict[str, Any] = {
            "run_id": self.run_id,
            "market_key": snapshot.market_key, "signal": signal, "zscore": zscore,
            "rv10": rv10, "rv_q90": rv_q90, "rv_history_markets": rv_history_n,
            "scenario_key": scenario_key,
            "bucket": bucket, "history_markets": stats.count,
            "requested_shares": self.settings.min_shares, "decision_at": now.isoformat(),
            "decision_epoch": decision_epoch, "market_end": snapshot.end_at.timestamp(),
            "status": "OPEN", "trade_id": None, "legs": [], "entries_resolved": False,
            "paired_shares": 0.0, "hold_due_epoch": None, "entry_cost": 0.0,
            "entry_fee": 0.0, "exit_value": 0.0, "exit_fee": 0.0,
        }
        for index, (venue, outcome) in enumerate(chosen):
            state["legs"].append({
                "name": f"leg{index + 1}", "venue": venue, "outcome": outcome,
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
        return (f"SHADOW SIGNAL {signal} z={zscore:+.2f} n={stats.count}; requested="
                f"{self.settings.min_shares:g}/leg; RV10={rv10:.6f}>q90={rv_q90:.6f} "
                f"(n={rv_history_n}); fixed-delay IOC uses memory book at arrival; "
                f"no orders sent; {delay_text}")

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
        order = {"requested": requested, "created": created_epoch,
                 "network_ms": network, "api_ms": api, "jitter_ms": jitter,
                 "arrival": created_epoch + (network + api + jitter) / 1000.0,
                 "wait_for_update": wait_for_update}
        if phase == "ENTRY":
            leg["entry_order"] = order
        else:
            leg[f"{phase.lower()}_order"] = order
        self._event(state, phase, leg, side="BUY" if phase == "ENTRY" else "SELL",
                    status="SIMULATED_SUBMIT", requested=requested, order=order,
                    details={"note": "assumed latency only; no API order request was sent",
                             "execution": "wait-for-new-book" if wait_for_update
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

    def _attempt(self, state: dict[str, Any], leg: dict[str, Any], phase: str,
                 remaining: float, order: dict[str, Any], snapshot: PairSnapshot,
                 snapshot_ids: dict[tuple[str, str], int]) -> tuple[float, float, float] | None:
        book = snapshot.book(leg["venue"], leg["outcome"])
        if snapshot.captured_at.timestamp() + 1e-6 < order["arrival"]:
            return None
        wait_for_update = bool(order.get("wait_for_update", False))
        if wait_for_update:
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
            arrival_at = datetime.fromtimestamp(order["arrival"], tz=timezone.utc)
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

    def _advance(self, snapshot: PairSnapshot, snapshot_ids: dict[tuple[str, str], int]) -> str | None:
        assert self.active is not None
        state = self.active
        now = snapshot.captured_at.timestamp()
        dirty = False
        if now >= float(state["market_end"]):
            state["status"] = "UNRESOLVED_AT_END"
            state["closed_at"] = snapshot.captured_at.isoformat()
            self._event(state, "TRADE", None, side=None, status="UNRESOLVED_AT_END",
                        details={"paired_shares": state.get("paired_shares", 0), "net_pnl": None})
            self._persist()
            self.active = None
            return "SHADOW UNRESOLVED: market ended with open/pending exposure; settlement not assumed"

        for leg in state["legs"]:
            if leg["entry_attempted"]:
                continue
            order = leg["entry_order"]
            attempt = self._attempt(state, leg, "ENTRY", float(order["requested"]), order, snapshot, snapshot_ids)
            if attempt is not None:
                filled, notional, fee = attempt
                leg["entry_attempted"] = True
                leg["entry_filled"] = filled
                leg["entry_cost"] = notional
                leg["entry_fee"] = fee
                leg["entry_fill_epoch"] = float(order["arrival"]) if filled > 0 else None
                dirty = True

        if not state["entries_resolved"] and all(leg["entry_attempted"] for leg in state["legs"]):
            paired = min(float(leg["entry_filled"]) for leg in state["legs"])
            state["paired_shares"] = paired
            state["entry_cost"] = sum(float(leg["entry_cost"]) for leg in state["legs"])
            state["entry_fee"] = sum(float(leg["entry_fee"]) for leg in state["legs"])
            for leg in state["legs"]:
                leg["unwind_remaining"] = max(0.0, float(leg["entry_filled"]) - paired)
                if leg["unwind_remaining"] > 1e-9:
                    leg["unwind_order"] = self._schedule(state, leg, "UNWIND", leg["unwind_remaining"], now)
            fills = [leg["entry_fill_epoch"] for leg in state["legs"] if leg["entry_fill_epoch"] is not None]
            state["hold_due_epoch"] = None if paired <= 1e-9 else max(fills) + self.settings.hold_seconds
            for leg in state["legs"]:
                leg["exit_remaining"] = paired
            state["entries_resolved"] = True
            dirty = True
            if paired + 1e-9 < float(state["requested_shares"]):
                self._event(state, "PAIR", None, side=None,
                            status="PAIR_PARTIAL" if paired > 0 else "NO_PAIR",
                            requested=float(state["requested_shares"]), filled=paired,
                            details={"leg_entry_fills": [leg["entry_filled"] for leg in state["legs"]]})

        for leg in state["legs"]:
            remaining = float(leg["unwind_remaining"])
            order = leg.get("unwind_order")
            if remaining > 1e-9 and order is not None:
                attempt = self._attempt(state, leg, "UNWIND", remaining, order, snapshot, snapshot_ids)
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
                            state, leg, "UNWIND", leg["unwind_remaining"], now,
                            wait_for_update=True,
                        )

        if state["entries_resolved"] and float(state["paired_shares"]) > 1e-9:
            if now >= float(state["hold_due_epoch"]):
                for leg in state["legs"]:
                    if leg["exit_order"] is None and float(leg["exit_remaining"]) > 1e-9:
                        leg["exit_order"] = self._schedule(state, leg, "EXIT",
                                                           float(leg["exit_remaining"]), now)
                    dirty = True
            for leg in state["legs"]:
                remaining = float(leg["exit_remaining"])
                order = leg.get("exit_order")
                if remaining > 1e-9 and order is not None:
                    attempt = self._attempt(state, leg, "EXIT", remaining, order, snapshot, snapshot_ids)
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
                            leg["exit_order"] = self._schedule(
                                state, leg, "EXIT", leg["exit_remaining"], now,
                                wait_for_update=True,
                            )

        if state["entries_resolved"]:
            done_unwind = all(float(leg["unwind_remaining"]) <= 1e-9 for leg in state["legs"])
            done_exit = all(float(leg["exit_remaining"]) <= 1e-9 for leg in state["legs"])
            if done_unwind and (done_exit or float(state["paired_shares"]) <= 1e-9):
                state["status"] = "CLOSED" if float(state["paired_shares"]) > 1e-9 else "NO_PAIR"
                state["net_pnl"] = (float(state["exit_value"]) - float(state["exit_fee"])
                                    - float(state["entry_cost"]) - float(state["entry_fee"]))
                state["closed_at"] = snapshot.captured_at.isoformat()
                self._event(state, "TRADE", None, side=None, status=state["status"],
                            details={"paired_shares": state["paired_shares"],
                                     "entry_cost": state["entry_cost"],
                                     "entry_fee": state["entry_fee"],
                                     "exit_value": state["exit_value"],
                                     "exit_fee": state["exit_fee"],
                                     "net_pnl": state["net_pnl"]})
                self._persist()
                self.active = None
                return (f"SHADOW {state['status']} {state['signal']} paired={state['paired_shares']:g}/"
                        f"{state['requested_shares']:g} netPnL=${state['net_pnl']:+.4f}; "
                        "fills use the latest in-memory book at each order arrival; no orders sent")

        if dirty:
            self._persist()
        if not state["entries_resolved"]:
            done = sum(bool(leg["entry_attempted"]) for leg in state["legs"])
            return f"SHADOW ENTRY pending {done}/2 legs; waiting for 330ms order arrival"
        if any(float(leg["unwind_remaining"]) > 1e-9 for leg in state["legs"]):
            return (f"SHADOW SINGLE-LEG EXPOSURE; paired={state['paired_shares']:g}, "
                    f"unwind pending; IOC retry waits for a new book after 330ms arrival")
        if state["paired_shares"] > 1e-9:
            due = state.get("hold_due_epoch")
            if due is not None and now < float(due):
                return f"SHADOW OPEN paired={state['paired_shares']:g}; hold {float(due)-now:.1f}s then delayed exit"
            return f"SHADOW EXIT pending paired={state['paired_shares']:g}; waiting for 330ms order arrival"
        return "SHADOW NO PAIR; entry filled zero; recording complete"

    def _persist(self) -> None:
        if self.active is not None:
            self.store.update_shadow_trade(self.active)

    def next_wakeup_epoch(self) -> float | None:
        """Return the next timer deadline that needs no WebSocket update."""
        if self.active is None:
            return None
        state = self.active
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
                and state.get("hold_due_epoch") is not None
                and all(leg.get("exit_order") is None for leg in state["legs"])):
            due.append(float(state["hold_due_epoch"]))
        return min(due) if due else None

    @staticmethod
    def _delay_summary(state: dict[str, Any]) -> str:
        parts = []
        for leg in state["legs"]:
            order = leg["entry_order"]
            delay = order["network_ms"] + order["api_ms"] + order["jitter_ms"]
            parts.append(f"{leg['venue']}={delay:.0f}ms")
        return "fixed assumed order delay " + "/".join(parts)
