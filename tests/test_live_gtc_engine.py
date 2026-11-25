from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from pairtrading.config import Settings
from pairtrading.models import Book, Level, Market, PairSnapshot
from pairtrading.shadow import ShadowEngine
from pairtrading.storage import Store


class MovingBookExecutor:
    """Mock live executor; it never opens a network client or sends an order."""

    def __init__(self, set_up_book: Callable[[float], None]) -> None:
        self.set_up_book = set_up_book
        self.limit_price: Decimal | None = None
        self.requested_qty = 0.0
        self.order_count = 0
        self.wait_seconds: float | None = None
        self.cancel_deadlines: list[tuple[str, float]] = []

    def register_user_order_intent(self, *args: Any, **kwargs: Any) -> None:
        pass

    def forget_user_order_intent(self, intent_key: str) -> None:
        pass

    def schedule_gtc_cancel_deadline(self, order_id: str, deadline_epoch: float) -> None:
        self.cancel_deadlines.append((order_id, deadline_epoch))

    def has_gtc_cancel(self, order_id: str) -> bool:
        return any(existing_id == order_id for existing_id, _ in self.cancel_deadlines)

    async def forget_gtc_cancel(self, order_id: str) -> None:
        self.cancel_deadlines = [
            item for item in self.cancel_deadlines if item[0] != order_id
        ]

    async def verify_market(self, market: Market) -> Decimal:
        return Decimal("0.01")

    async def prewarm_order_metadata(self, market: Market) -> dict[str, Any]:
        # The market moves while verification/metadata preparation is in progress.
        self.set_up_book(0.12)
        return {"cache_hit": True}

    async def buy_gtc_limit(self, **kwargs: Any) -> tuple[bool, str, dict[str, Any]]:
        self.order_count += 1
        self.limit_price = Decimal(str(kwargs["limit_price"]))
        self.requested_qty = float(kwargs["requested_qty"])
        callback = kwargs.get("on_order_signed")
        if callback is not None:
            callback({
                "sdk_order_type": "GTC", "sdk_post_only": False,
                "sdk_limit_price": format(self.limit_price, "f"),
                "sdk_requested_share_qty": f"{self.requested_qty:.6f}",
                "sdk_order_prepare_source": "pre_signed_gtc_buy_pool",
            })
        # The ask jumps after the signed price is fixed but before the POST returns.
        self.set_up_book(0.20)
        order_id = "mock-gtc-order"
        return True, order_id, {
            "order_id": order_id, "order_status": "live",
            "sdk_order_type": "GTC", "sdk_post_only": False,
            "sdk_limit_price": format(self.limit_price, "f"),
            "sdk_requested_share_qty": f"{self.requested_qty:.6f}",
            "sdk_order_prepare_source": "pre_signed_gtc_buy_pool",
            "outcome_unknown": False,
        }

    async def cancel_and_reconcile_gtc_buy(self, **kwargs: Any) -> tuple[None, dict[str, Any]]:
        self.wait_seconds = float(kwargs["wait_seconds"])
        return None, {
            "order_id": kwargs["order_id"], "order_status": "canceled",
            "size_matched": "0", "remaining_canceled": kwargs["requested_qty"],
            "cancellation_confirmed": True,
        }


class LiveGtcBookRaceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name) / "test.sqlite3")
        self.settings = Settings(
            min_history_markets=50, history_window_markets=100,
            min_ou_history_markets=100, min_rv_history_markets=100,
            rv_history_window_markets=100, min_market_time_left_seconds=60,
            max_hold_seconds=60, ou_entry_threshold=3.0, ou_exit_threshold=1.5,
            min_shares=5, live_buy_price_offset_ticks=3,
            live_buy_unfilled_ttl_seconds=2.0,
            db_path=Path(self.temp.name) / "test.sqlite3",
        )
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.market_start = self.now - timedelta(seconds=30)
        self.market_end = self.market_start + timedelta(seconds=300)
        self.poly = Market(
            "polymarket", "race-market", "race", self.market_start, self.market_end,
            "poly-up", "poly-down", condition_id="race-condition",
        )
        self.binance = Market(
            "binance", "race-binance", "race", self.market_start, self.market_end,
            "binance-up", "binance-down",
        )
        self.books: dict[tuple[str, str], Book] = {}
        self.executor = MovingBookExecutor(self.set_up_up_book)
        self.engine = ShadowEngine(self.store, self.settings, live_executor=self.executor)
        self.engine.set_current_book_provider(
            lambda venue, outcome: self.books.get((venue, outcome))
        )
        history_rows = []
        rv_rows = []
        for index in range(100):
            market_key = f"closed-{index}"
            market_end = self.market_start.timestamp() - (100 - index) * 300
            for bucket in range(5):
                history_rows.append((market_key, market_end, bucket, (index - 49.5) * 0.001, 60))
                rv_rows.append((market_key, market_end, bucket, 0.001, 60))
        for row in history_rows:
            self.store.save_history(*row)
        self.store.save_rv_history_batch(rv_rows)
        self.set_up_up_book(0.10)

    def tearDown(self) -> None:
        self.store.close()
        self.temp.cleanup()

    def set_up_up_book(self, ask: float) -> None:
        received = datetime.now(timezone.utc)
        self.books[("polymarket", "UP")] = Book(
            "polymarket", "UP", "poly-up",
            (Level(max(0.001, ask - 0.01), 20.0),), (Level(ask, 20.0),),
            received, received,
        )

    def snapshot(self, captured: datetime, spread: float) -> PairSnapshot:
        received = captured
        binance_up_mid = 0.795
        poly_up_mid = binance_up_mid + spread

        def book(venue: str, outcome: str, token: str, mid: float) -> Book:
            return Book(
                venue, outcome, token,
                (Level(mid - 0.005, 20.0),), (Level(mid + 0.005, 20.0),),
                received, received,
            )

        return PairSnapshot(
            "race-market", self.poly, self.binance,
            {
                ("polymarket", "UP"): book("polymarket", "UP", "poly-up", poly_up_mid),
                ("polymarket", "DOWN"): book("polymarket", "DOWN", "poly-down", 1.0 - poly_up_mid),
                ("binance", "UP"): book("binance", "UP", "binance-up", binance_up_mid),
                ("binance", "DOWN"): book("binance", "DOWN", "binance-down", 1.0 - binance_up_mid),
            },
            captured, 0.0,
        )

    async def test_latest_ws_ask_sets_fixed_limit_and_post_jump_is_not_chased(self) -> None:
        status = await self.engine.process_async(self.snapshot(self.now, -0.6))
        self.assertIn("RV10 unavailable", status)

        signal_at = self.now + timedelta(milliseconds=100)
        status = await self.engine.process_async(self.snapshot(signal_at, -0.7))
        self.assertIn("LIVE SIGNAL LOW_SPREAD", status)

        attempt_at = self.now + timedelta(milliseconds=500)
        status = await self.engine.process_async(self.snapshot(attempt_at, -0.7))
        self.assertIn("ENTRY pending", status)
        self.assertEqual(self.executor.order_count, 1)
        self.assertEqual(self.executor.requested_qty, 5.0)
        self.assertEqual(self.executor.limit_price, Decimal("0.15"))
        self.assertEqual(self.books[("polymarket", "UP")].best_ask.price, 0.20)
        self.assertIn("gtc-limit-ask-plus-3ticks-ttl-2.0s", self.engine.active["scenario_key"])

        intent = self.store.db.execute(
            "SELECT details_json FROM shadow_events WHERE phase='ENTRY' "
            "AND status='LIVE_ORDER_INTENT' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        intent_details = json.loads(intent["details_json"])
        self.assertEqual(intent_details["best_ask"], "0.12")
        self.assertEqual(intent_details["limit_price"], "0.15")
        self.assertEqual(intent_details["limit_price_offset_ticks"], 3)

        trade_id = int(self.engine.active["trade_id"])
        worker = self.engine._live_entry_tasks[trade_id]
        self.assertIsNotNone(worker)
        await worker
        self.assertEqual(self.executor.wait_seconds, 2.0)
        self.assertIsNone(self.engine.active)
        no_fill = self.store.db.execute(
            "SELECT requested_shares,filled_shares FROM shadow_events "
            "WHERE phase='ENTRY' AND status='LIVE_GTC_NO_FILL' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(no_fill["requested_shares"], 5.0)
        self.assertEqual(no_fill["filled_shares"], 0.0)
        trade = self.store.db.execute(
            "SELECT status,paired_shares FROM shadow_trades ORDER BY id DESC LIMIT 1"
        ).fetchone()
        self.assertEqual(trade["status"], "NO_POSITION")
        self.assertEqual(trade["paired_shares"], 0.0)
        await self.engine.stop_live_background_tasks()


if __name__ == "__main__":
    unittest.main()
