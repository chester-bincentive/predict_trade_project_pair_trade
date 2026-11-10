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
            min_history_markets=50,
            history_window_markets=100,
            min_ou_history_markets=100,
            min_rv_history_markets=100,
            rv_history_window_markets=100,
            min_market_time_left_seconds=20,
            max_hold_seconds=60,
            ou_entry_threshold=3.0,
            ou_exit_threshold=1.5,
            poly_order_network_ms=330,
            poly_order_api_ms=0,
            binance_order_network_ms=330,
            binance_order_api_ms=0,
            order_latency_jitter_ms=0,
            min_shares=5,
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

        rv_rows = []
        for index in range(100):
            market_key = f"closed-{index}"
            market_end = self.market_start.timestamp() - (100 - index) * 300
            for bucket in range(5):
                self.store.save_history(
                    market_key, market_end, bucket, (index - 49.5) * 0.001, 60,
                )
                rv_rows.append((market_key, market_end, bucket, 0.001, 60))
        self.store.save_rv_history_batch(rv_rows)

    def tearDown(self) -> None:
        self.store.close()
        self.temp.cleanup()

    def snapshot(
        self,
        captured: datetime,
        request_started: datetime,
        *,
        spread: float = -0.6,
        book_received_at: datetime | None = None,
    ) -> PairSnapshot:
        received_at = book_received_at or captured
        binance_up_mid = 0.795
        poly_up_mid = binance_up_mid + spread

        def book(venue: str, outcome: str, token: str, mid: float,
                 ask_size: float = 20.0) -> Book:
            return Book(
                venue, outcome, token,
                (Level(mid - 0.005, 20.0),),
                (Level(mid + 0.005, ask_size),),
                received_at, received_at, request_started, 10.0,
            )

        books = {
            ("polymarket", "UP"): book("polymarket", "UP", "poly-up", poly_up_mid),
            ("polymarket", "DOWN"): book("polymarket", "DOWN", "poly-down", 1.0 - poly_up_mid),
            ("binance", "UP"): book("binance", "UP", "binance-up", binance_up_mid),
            # The signaled Binance DOWN leg has only two shares at the ask.
            ("binance", "DOWN"): book(
                "binance", "DOWN", "binance-down", 1.0 - binance_up_mid, ask_size=2.0,
            ),
        }
        return PairSnapshot(self.market_key, self.poly, self.binance, books, captured, 0.0)

    def test_ou_rv_entry_partial_pair_and_directional_exit(self) -> None:
        first = self.snapshot(self.now, self.now - timedelta(milliseconds=10), spread=-0.6)
        status = self.engine.process(first)
        self.assertIn("RV10 unavailable", status)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM shadow_trades").fetchone()[0], 0)

        signal_at = self.now + timedelta(milliseconds=100)
        signal = self.snapshot(
            signal_at, signal_at - timedelta(milliseconds=10), spread=-0.7,
        )
        status = self.engine.process(signal)
        self.assertIn("SHADOW SIGNAL LOW_SPREAD", status)
        self.assertLess(self.engine.active["ou_score"], -3.0)
        self.assertEqual(self.engine.active["ou_history_markets"], 100)
        self.assertEqual(self.engine.active["rv_history_markets"], 100)
        self.assertAlmostEqual(self.engine.active["legs"][0]["entry_order"]["arrival"],
                               signal_at.timestamp() + 0.330)
        self.assertEqual(self.engine.active["legs"][0]["entry_order"]["jitter_ms"], 0)
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM shadow_events WHERE phase='ENTRY' AND status='SIMULATED_SUBMIT'"
        ).fetchone()[0], 2)

        too_early_at = self.now + timedelta(milliseconds=200)
        status = self.engine.process(self.snapshot(
            too_early_at, signal_at, spread=-0.7, book_received_at=signal_at,
        ))
        self.assertIn("pending", status.lower())
        self.assertEqual(self.store.db.execute(
            "SELECT COUNT(*) FROM shadow_events WHERE phase='ENTRY' AND status='IOC_FILLED'"
        ).fetchone()[0], 0)

        eligible_at = self.now + timedelta(milliseconds=500)
        status = self.engine.process(self.snapshot(
            eligible_at, signal_at, spread=-0.7, book_received_at=signal_at,
        ))
        self.assertIn("SINGLE-LEG EXPOSURE", status)
        self.assertEqual(self.engine.active["paired_shares"], 2.0)
        self.assertEqual([leg["entry_filled"] for leg in self.engine.active["legs"]], [5.0, 2.0])
        self.assertAlmostEqual(
            self.engine.active["max_hold_due_epoch"],
            max(leg["entry_fill_epoch"] for leg in self.engine.active["legs"]) + 60.0,
        )

        unwind_arrival = self.engine.active["legs"][0]["unwind_order"]["arrival"]
        unwind_fill_at = self.now + timedelta(seconds=2)
        unwind_book_at = signal_at
        self.assertGreater(unwind_fill_at.timestamp(), unwind_arrival)
        self.assertLess(unwind_book_at.timestamp(), unwind_arrival)
        status = self.engine.process(self.snapshot(
            unwind_fill_at, unwind_book_at - timedelta(milliseconds=10),
            spread=-0.7, book_received_at=unwind_book_at,
        ))
        self.assertEqual(
            self.engine.active["legs"][0]["unwind_remaining"], 0.0,
            msg=f"status={status}; leg={self.engine.active['legs'][0]}",
        )
        self.assertEqual(self.engine.active["legs"][1]["unwind_remaining"], 0.0)

        exit_trigger_at = self.now + timedelta(seconds=5)
        status = self.engine.process(self.snapshot(
            exit_trigger_at, exit_trigger_at - timedelta(milliseconds=10), spread=0.0,
        ))
        self.assertIn("SHADOW EXIT TRIGGER ou_threshold", status)
        self.assertEqual(self.engine.active["exit_reason"], "ou_threshold")

        exit_arrival = self.engine.active["legs"][0]["exit_order"]["arrival"]
        exit_early_at = self.now + timedelta(seconds=5, milliseconds=200)
        status = self.engine.process(self.snapshot(
            exit_early_at, exit_trigger_at, spread=0.0, book_received_at=exit_trigger_at,
        ))
        self.assertIn("pending", status.lower())
        self.assertGreater(exit_arrival, exit_early_at.timestamp())

        exit_fill_at = self.now + timedelta(seconds=5, milliseconds=500)
        status = self.engine.process(self.snapshot(
            exit_fill_at, exit_trigger_at, spread=0.0, book_received_at=exit_trigger_at,
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
        self.assertEqual(snapshots, 0)
        self.assertEqual(levels, 0)

    def test_entry_is_skipped_with_less_than_20_seconds_left(self) -> None:
        self.market_start = self.now - timedelta(seconds=281)
        self.market_end = self.market_start + timedelta(seconds=300)
        self.poly = Market("polymarket", self.market_key, "test", self.market_start,
                           self.market_end, "poly-up", "poly-down")
        self.binance = Market("binance", "test-binance", "test", self.market_start,
                              self.market_end, "binance-up", "binance-down")

        status = self.engine.process(self.snapshot(
            self.now, self.now - timedelta(milliseconds=10), spread=-0.7,
        ))
        self.assertIn("less than 20s remains", status)
        self.assertEqual(self.store.db.execute("SELECT COUNT(*) FROM shadow_trades").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
