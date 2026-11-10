from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .models import Book, Level, PairSnapshot
from .shadow import ShadowEngine
from .storage import Store
from .strategy import RollingCalibration, event_bucket, mid_spread


@dataclass(frozen=True, slots=True)
class Sweep:
    shares: float
    notional: float
    fee: float

    @property
    def vwap(self) -> float:
        return self.notional / self.shares if self.shares else 0.0


def sweep(book: Book, *, side: str, shares: float, fee_coefficient: float) -> Sweep | None:
    levels = book.asks if side == "BUY" else book.bids
    remaining = shares
    notional = 0.0
    fee = 0.0
    for level in levels:
        taken = min(remaining, level.size)
        notional += taken * level.price
        fee += taken * fee_coefficient * level.price * (1.0 - level.price)
        remaining -= taken
        if remaining <= 1e-9:
            return Sweep(shares, notional, fee)
    return None


class _ImmediatePaperEngine:
    """Deprecated immediate-fill implementation; public PaperEngine no longer uses it."""

    def __init__(self, store: Store, settings: Settings) -> None:
        self.store = store
        self.settings = settings
        self.calibration = RollingCalibration(
            store, window=settings.history_window_markets, rv_window=settings.rv_history_window_markets,
            bucket_seconds=settings.bucket_seconds
        )
        self.positions: dict[str, dict[str, Any]] = {
            row["market_key"]: row for row in store.open_positions()
        }
        self.traded_this_run: set[str] = set()

    def process(self, snapshot: PairSnapshot) -> str:
        now = snapshot.captured_at
        elapsed = (now - snapshot.start_at).total_seconds()
        bucket = event_bucket(elapsed, self.settings.bucket_seconds)
        spread = mid_spread(snapshot)
        self.calibration.observe(snapshot, spread, bucket)

        closed = self._try_close(snapshot)
        if closed:
            return closed

        stale = [
            f"{venue}/{outcome}={book.age_seconds(now):.1f}s"
            for (venue, outcome), book in snapshot.books.items()
            if book.age_seconds(now) > self.settings.max_quote_age_seconds
        ]
        if stale:
            return "SKIP stale quotes: " + ", ".join(stale)
        if spread is None:
            return "SKIP missing best bid/ask for UP midpoint"
        if elapsed >= 300.0:
            return "SKIP market has ended"
        if (snapshot.end_at - now).total_seconds() < self.settings.hold_seconds:
            return f"SKIP less than {self.settings.hold_seconds}s remains in the matched market"

        counts = self.calibration.counts(before_market_start=snapshot.start_at.timestamp())
        missing = [b for b, count in counts.items() if count < self.settings.min_history_markets]
        if missing:
            progress = ", ".join(
                f"b{b}:{counts[b]}/{self.settings.min_history_markets}" for b in missing
            )
            return f"DATA COLLECTION ONLY; insufficient closed-market history ({progress}); signals disabled"

        stats = self.calibration.stats(bucket, before_market_start=snapshot.start_at.timestamp())
        zscore = stats.score(spread)
        if zscore is None:
            return f"SKIP bucket={bucket} spread stdev is zero/undefined"
        if abs(zscore) < self.settings.z_entry:
            return f"HOLD bucket={bucket} spread={spread:+.4f} z={zscore:+.2f} n={stats.count}"
        if snapshot.market_key in self.traded_this_run or self.store.has_position_or_trade(snapshot.market_key):
            return f"HOLD one-trade-per-market already used z={zscore:+.2f}"
        if self.positions:
            return "HOLD existing pair position; do not stack another market"

        if zscore < 0.0:
            signal, legs = "LOW_SPREAD", (("polymarket", "UP"), ("binance", "DOWN"))
        else:
            signal, legs = "HIGH_SPREAD", (("polymarket", "DOWN"), ("binance", "UP"))
        shares = self.settings.min_shares
        fills: list[dict[str, Any]] = []
        total_cost = total_fee = 0.0
        for venue, outcome in legs:
            fill = sweep(snapshot.book(venue, outcome), side="BUY", shares=shares,
                         fee_coefficient=self.settings.taker_fee_coefficient)
            if fill is None:
                return f"SKIP depth insufficient for {shares:g} shares on {venue}/{outcome} ask"
            fills.append({"venue": venue, "outcome": outcome, "token_id": snapshot.book(venue, outcome).token_id,
                          "vwap": fill.vwap, "notional": fill.notional, "fee": fill.fee})
            total_cost += fill.notional
            total_fee += fill.fee

        position = {
            "market_key": snapshot.market_key, "signal": signal, "zscore": zscore,
            "bucket": bucket, "history_markets": stats.count, "shares": shares,
            "entered_epoch": now.timestamp(), "entered_at": now.isoformat(),
            "due_epoch": now.timestamp() + self.settings.hold_seconds,
            "entry_cost": total_cost, "entry_fee": total_fee, "legs": fills,
            "market_end": snapshot.end_at.timestamp(),
        }
        self.store.save_position(snapshot.market_key, position, opened_at=now.timestamp())
        self.positions[snapshot.market_key] = position
        self.traded_this_run.add(snapshot.market_key)
        return (f"PAPER ENTRY {signal} z={zscore:+.2f} n={stats.count} size={shares:g}/leg "
                f"cost=${total_cost:.4f} fee=${total_fee:.4f}; exit target {self.settings.hold_seconds}s")

    def _try_close(self, snapshot: PairSnapshot) -> str | None:
        if not self.positions:
            return None
        for key, position in tuple(self.positions.items()):
            if key != snapshot.market_key:
                return f"OPEN position {key}; waiting for its same-market exit quotes"
            if snapshot.captured_at.timestamp() < float(position["due_epoch"]):
                remaining = float(position["due_epoch"]) - snapshot.captured_at.timestamp()
                return f"OPEN {position['signal']} z={position['zscore']:+.2f}; {remaining:.1f}s to exit"
            total_value = total_fee = 0.0
            for leg in position["legs"]:
                book = snapshot.book(leg["venue"], leg["outcome"])
                fill = sweep(book, side="SELL", shares=float(position["shares"]),
                             fee_coefficient=self.settings.taker_fee_coefficient)
                if fill is None:
                    return f"OPEN exit depth insufficient for {position['shares']:g} shares on {leg['venue']}/{leg['outcome']} bid"
                total_value += fill.notional
                total_fee += fill.fee
            pnl = total_value - total_fee - float(position["entry_cost"]) - float(position["entry_fee"])
            now = snapshot.captured_at
            trade = {
                **position, "exited_at": now.isoformat(), "exited_epoch": now.timestamp(),
                "hold_seconds": now.timestamp() - float(position["entered_epoch"]),
                "exit_value": total_value, "exit_fee": total_fee, "net_pnl": pnl, "exit_type": "quote_20s",
            }
            self.store.record_trade(trade)
            del self.positions[key]
            return (f"PAPER EXIT {position['signal']} netPnL=${pnl:+.4f} "
                    f"(exit=${total_value:.4f}, fees=${float(position['entry_fee']) + total_fee:.4f})")
        return None


class PaperEngine(ShadowEngine):
    """Paper-trade name for the delayed, arrival-time memory-book simulator."""


def demo(settings: Settings) -> None:
    """Deterministic no-network demo of delayed, arrival-time memory-book fills."""
    from datetime import timedelta
    from pathlib import Path
    import tempfile

    from .models import Market
    from .storage import Store

    with tempfile.TemporaryDirectory(prefix="pair-demo-") as temp:
        demo_settings = Settings(
            poll_seconds=settings.poll_seconds,
            match_tolerance_seconds=settings.match_tolerance_seconds,
            max_quote_age_seconds=settings.max_quote_age_seconds,
            z_entry=settings.z_entry,
            ou_entry_threshold=settings.ou_entry_threshold,
            ou_exit_threshold=settings.ou_exit_threshold,
            min_history_markets=settings.min_history_markets,
            history_window_markets=settings.history_window_markets,
            min_ou_history_markets=settings.min_ou_history_markets,
            min_rv_history_markets=settings.min_rv_history_markets,
            rv_history_window_markets=settings.rv_history_window_markets,
            bucket_seconds=settings.bucket_seconds,
            hold_seconds=settings.hold_seconds,
            min_market_time_left_seconds=settings.min_market_time_left_seconds,
            max_hold_seconds=settings.max_hold_seconds,
            min_shares=settings.min_shares,
            taker_fee_coefficient=settings.taker_fee_coefficient,
            poly_order_network_ms=settings.poly_order_network_ms,
            poly_order_api_ms=settings.poly_order_api_ms,
            binance_order_network_ms=settings.binance_order_network_ms,
            binance_order_api_ms=settings.binance_order_api_ms,
            order_latency_jitter_ms=settings.order_latency_jitter_ms,
            db_path=Path(temp) / "demo.sqlite3",
        )
        store = Store(demo_settings.db_path)
        base = datetime.now(timezone.utc).replace(microsecond=0)
        market_start = base - timedelta(seconds=130)
        market_end = market_start + timedelta(seconds=300)
        market_key = "btc-updown-5m-demo"
        engine = ShadowEngine(store, demo_settings)
        poly = Market("polymarket", market_key, "Demo Poly", market_start, market_end, "pu", "pd")
        binance = Market("binance", "demo-binance", "Demo Binance", market_start, market_end, "bu", "bd")

        def make_book(venue: str, outcome: str, token: str, bid: float, ask: float, stamp: datetime) -> Book:
            return Book(venue, outcome, token,
                        (Level(bid, 20), Level(max(0.01, bid - 0.01), 20)),
                        (Level(ask, 20), Level(min(0.99, ask + 0.01), 20)), stamp, stamp)

        entry_at = base
        warmup_at = entry_at - timedelta(seconds=10)
        warmup_books = {
            ("polymarket", "UP"): make_book("polymarket", "UP", "pu", .49, .51, warmup_at),
            ("polymarket", "DOWN"): make_book("polymarket", "DOWN", "pd", .49, .51, warmup_at),
            ("binance", "UP"): make_book("binance", "UP", "bu", .49, .51, warmup_at),
            ("binance", "DOWN"): make_book("binance", "DOWN", "bd", .49, .51, warmup_at),
        }
        entry_books = {
            ("polymarket", "UP"): make_book("polymarket", "UP", "pu", .29, .31, entry_at),
            ("polymarket", "DOWN"): make_book("polymarket", "DOWN", "pd", .69, .71, entry_at),
            ("binance", "UP"): make_book("binance", "UP", "bu", .70, .72, entry_at),
            ("binance", "DOWN"): make_book("binance", "DOWN", "bd", .30, .32, entry_at),
        }
        warmup = PairSnapshot(market_key, poly, binance, warmup_books, warmup_at, 0.0)
        entry = PairSnapshot(market_key, poly, binance, entry_books, entry_at, 0.0)
        print("DEMO ONLY - synthetic fixture data; no network or real orders")
        print("WARMUP:", engine.process(warmup))
        # Synthetic closed-market medians are isolated demo fixtures, never live history.
        bucket_count = (300 + demo_settings.bucket_seconds - 1) // demo_settings.bucket_seconds
        seed_count = max(100, demo_settings.min_history_markets,
                         demo_settings.history_window_markets,
                         demo_settings.min_ou_history_markets,
                         demo_settings.rv_history_window_markets,
                         demo_settings.min_rv_history_markets)
        for bucket in range(bucket_count):
            rv_fixture_rows = []
            for index in range(seed_count):
                value = (index - (seed_count - 1) / 2.0) * 0.0008
                market_end = market_start.timestamp() - (seed_count-index) * 300
                rv_value = 0.02 + index * 0.0001
                store.save_history(f"demo-closed-{bucket}-{index}",
                                   market_end,
                                   bucket, value, 60)
                rv_fixture_rows.append((f"demo-closed-{bucket}-{index}", market_end,
                                        bucket, rv_value, 50))
            store.save_rv_history_batch(rv_fixture_rows)
        print(f"synthetic fixture calibration: {seed_count} closed markets per bucket for spread and RV10 q90 (temporary DB only)")
        print("ENTRY:", engine.process(entry))

        delay_seconds = (demo_settings.poly_order_network_ms + demo_settings.poly_order_api_ms
                         + demo_settings.order_latency_jitter_ms) / 1000.0
        entry_arrival = entry_at + timedelta(seconds=delay_seconds)
        entry_arrival_snapshot = PairSnapshot(
            market_key, poly, binance, entry_books, entry_arrival, 0.0
        )
        print("ENTRY ARRIVAL (same already-received memory books; no new quote):",
              engine.process(entry_arrival_snapshot, evaluate_signals=False))

        exit_at = entry_arrival + timedelta(seconds=10)
        exit_books = {
            ("polymarket", "UP"): make_book("polymarket", "UP", "pu", .49, .51, exit_at),
            ("polymarket", "DOWN"): make_book("polymarket", "DOWN", "pd", .49, .51, exit_at),
            ("binance", "UP"): make_book("binance", "UP", "bu", .49, .51, exit_at),
            ("binance", "DOWN"): make_book("binance", "DOWN", "bd", .49, .51, exit_at),
        }
        exit_trigger_snapshot = PairSnapshot(market_key, poly, binance, exit_books, exit_at, 0.0)
        print("OU EXIT TRIGGER:", engine.process(exit_trigger_snapshot, evaluate_signals=False))
        exit_arrival = exit_at + timedelta(seconds=delay_seconds)
        exit_arrival_snapshot = PairSnapshot(
            market_key, poly, binance, exit_books, exit_arrival, 0.0
        )
        print("EXIT ARRIVAL (same already-received memory books; no new quote):",
              engine.process(exit_arrival_snapshot, evaluate_signals=False))
        store.close()
