from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pairtrading.config import Settings
from pairtrading.models import Book, Level, Market, PairSnapshot
from pairtrading.shadow import ShadowEngine
from pairtrading.storage import Store


class ShadowExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "test.sqlite3")
        self.settings = Settings(
            min_history_markets=50, history_window_markets=200,
            poly_order_network_ms=100, poly_order_api_ms=100,
            binance_order_network_ms=100, binance_order_api_ms=100,
            order_latency_jitter_ms=0, min_shares=5,
            db_path=Path(self.temp.name) / "test.sqlite3",
        )
        self.engine = ShadowEngine(self.store, self.settings)
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.market_start = self.now - timedelta(seconds=30)
        self.market_end = self.market_start + timedelta(seconds=300)
        self.market_key = "test-five-minute-market"
        self.poly = Market("polymarket", self.market_key, "test", self.market_start,
                           self.market_end, "poly-up", "poly-down")
        self.binance = Market("binance", "test-binance", "test", self.market_start,
                              self.market_end, "binance-up", "binance-down")
        for bucket in range(5):
            for index in range(50):
                self.store.save_history(
                    f"closed-{bucket}-{index}", self.market_start.timestamp() - (50-index) * 300,
                    bucket, (index - 24.5) * 0.001, 60,
                )

    def tearDown(self) -> None:
        self.store.close()
        self.temp.cleanup()

    def snapshot(self, captured: datetime, request_started: datetime) -> PairSnapshot:
        def book(venue: str, outcome: str, token: str, bid: float, ask: float,
                 ask_size: float = 20.0) -> Book:
            return Book(venue, outcome, token,
                        (Level(bid, 20.0),), (Level(ask, ask_size),),
                        captured, captured, request_started, 10.0)

        books = {
            ("polymarket", "UP"): book("polymarket", "UP", "poly-up", .19, .20),
            ("polymarket", "DOWN"): book("polymarket", "DOWN", "poly-down", .79, .80),
            ("binance", "UP"): book("binance", "UP", "binance-up", .79, .80),
            # The signaled Binance DOWN leg only has two shares at the ask.
            ("binance", "DOWN"): book("binance", "DOWN", "binance-down", .19, .20, 2.0),
        }
        return PairSnapshot(self.market_key, self.poly, self.binance, books, captured, 0.0)

    def test_waits_for_post_arrival_book_and_records_partial_pair(self) -> None:
        first = self.snapshot(self.now, self.now - timedelta(milliseconds=10))
        status = self.engine.process(first)
        self.assertIn("SHADOW SIGNAL", status)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM shadow_events").fetchone()[0], 2)

        too_early_at = self.now + timedelta(milliseconds=100)
        early = self.snapshot(too_early_at, self.now + timedelta(milliseconds=50))
        status = self.engine.process(early)
        self.assertIn("pending", status)
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM shadow_events WHERE phase='ENTRY' AND status!='SIMULATED_SUBMIT'"
        ).fetchone()[0], 0)

        eligible_at = self.now + timedelta(seconds=1)
        eligible = self.snapshot(eligible_at, eligible_at - timedelta(milliseconds=100))
        status = self.engine.process(eligible)
        self.assertIn("SINGLE-LEG EXPOSURE", status)
        self.assertEqual(self.engine.active["paired_shares"], 2.0)
        self.assertEqual([leg["entry_filled"] for leg in self.engine.active["legs"]], [5.0, 2.0])

        unwind_at = self.now + timedelta(seconds=2)
        status = self.engine.process(self.snapshot(unwind_at, unwind_at - timedelta(milliseconds=100)))
        self.assertIn("hold", status)
        self.assertEqual(self.engine.active["legs"][0]["unwind_remaining"], 0.0)
        self.assertEqual(self.engine.active["legs"][1]["unwind_remaining"], 0.0)

        exit_submit_at = self.now + timedelta(seconds=25)
        status = self.engine.process(self.snapshot(
            exit_submit_at, exit_submit_at - timedelta(milliseconds=100)
        ))
        self.assertIn("EXIT pending", status)
        exit_fill_at = self.now + timedelta(seconds=26)
        status = self.engine.process(self.snapshot(
            exit_fill_at, exit_fill_at - timedelta(milliseconds=100)
        ))
        self.assertIn("SHADOW CLOSED", status)
        self.assertIsNone(self.engine.active)
        trade = self.store.db.execute(
            "SELECT status,paired_shares,net_pnl FROM shadow_trades"
        ).fetchone()
        self.assertEqual(trade["status"], "CLOSED")
        self.assertEqual(trade["paired_shares"], 2.0)
        self.assertIsNotNone(trade["net_pnl"])

        snapshots = self.store.db.execute("SELECT COUNT(*) FROM book_snapshots").fetchone()[0]
        levels = self.store.db.execute("SELECT COUNT(*) FROM book_levels").fetchone()[0]
        self.assertEqual(snapshots, 24)
        self.assertEqual(levels, 48)


if __name__ == "__main__":
    unittest.main()
