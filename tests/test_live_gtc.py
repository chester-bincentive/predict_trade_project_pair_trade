from __future__ import annotations

import time
import unittest
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

from pairtrading.live_execution import LiveOrderUnknown, PolymarketLiveExecutor
from pairtrading.shadow import _live_buy_limit_price


class FakePaginator:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    async def iter_items(self):
        for row in self.rows:
            yield row


class FakeClobClient:
    def __init__(self) -> None:
        self.created_orders: list[dict[str, Any]] = []
        self.posted_orders: list[Any] = []
        self.order_id = "order-gtc-1"
        self.order_status = "canceled"
        self.size_matched = "0"
        self.trades: list[Any] = []
        self.cancel_confirmed = True
        self.cancel_calls = 0
        self.get_calls = 0
        self.on_post = None

    async def create_limit_order(self, **kwargs: Any) -> Any:
        self.created_orders.append(dict(kwargs))
        return SimpleNamespace(
            order_type="GTC", post_only=kwargs.get("post_only", False),
            side=kwargs["side"], token_id=kwargs["asset_id"],
            maker_amount="1", taker_amount="5",
        )

    async def post_order(self, signed_order: Any) -> Any:
        self.posted_orders.append(signed_order)
        if self.on_post is not None:
            self.on_post(signed_order)
        return SimpleNamespace(
            order_id=self.order_id, status="live", ok=True, trade_ids=(),
        )

    async def cancel_order(self, *, order_id: str) -> Any:
        self.cancel_calls += 1
        canceled = [order_id] if self.cancel_confirmed else []
        not_canceled = {} if self.cancel_confirmed else {order_id: "still live"}
        return SimpleNamespace(canceled=canceled, not_canceled=not_canceled)

    async def get_order(self, *, order_id: str) -> Any:
        self.get_calls += 1
        return SimpleNamespace(status=self.order_status, size_matched=self.size_matched)

    def list_account_trades(self, **kwargs: Any) -> FakePaginator:
        return FakePaginator(self.trades)


class LiveGtcSimulationTests(unittest.IsolatedAsyncioTestCase):
    def make_executor(self) -> tuple[PolymarketLiveExecutor, FakeClobClient]:
        executor = PolymarketLiveExecutor(fee_rate=0.07)
        client = FakeClobClient()
        executor.client = client
        return executor, client

    def seed_presigned_order(
        self, executor: PolymarketLiveExecutor, *, asset_id: str, price: Decimal,
    ) -> None:
        size = "5.000000"
        key = executor._prepared_buy_order_key(asset_id, size, price)
        pool_key = ("test-condition", size, "0.01", "3")
        signed_order = SimpleNamespace(
            order_type="GTC", post_only=False, side="BUY", token_id=asset_id,
            maker_amount="5", taker_amount="2.75",
        )
        executor._prepared_buy_orders[key] = (
            signed_order, time.time_ns() + 30_000_000_000, pool_key,
        )

    async def test_rising_ask_keeps_signed_cap_and_expires_unfilled_remainder(self) -> None:
        executor, client = self.make_executor()
        signal_ask = Decimal("0.50")
        latest_ask_before_signing = Decimal("0.52")
        limit_price = _live_buy_limit_price(float(latest_ask_before_signing), "0.01", 3)
        self.assertEqual(limit_price, Decimal("0.55"))
        self.assertNotEqual(
            limit_price,
            _live_buy_limit_price(float(signal_ask), "0.01", 3),
            "the latest WebSocket ask before signing should set the cap",
        )
        self.seed_presigned_order(executor, asset_id="yes-token", price=limit_price)
        market = {"ask": Decimal("0.52")}
        client.on_post = lambda _: market.update(ask=Decimal("0.60"))

        accepted, order_id, details = await executor.buy_gtc_limit(
            asset_id="yes-token", requested_qty=5, limit_price=limit_price,
            tick_size="0.01", condition_id="test-condition",
        )

        self.assertTrue(accepted)
        self.assertEqual(order_id, client.order_id)
        self.assertEqual(len(client.created_orders), 0, "the presigned pool order should be reused")
        self.assertEqual(len(client.posted_orders), 1)
        self.assertEqual(details["sdk_order_prepare_source"], "pre_signed_gtc_buy_pool")
        self.assertEqual(details["sdk_order_type"], "GTC")
        self.assertFalse(details["sdk_post_only"])
        self.assertEqual(details["sdk_limit_price"], "0.55")
        self.assertEqual(market["ask"], Decimal("0.60"))
        self.assertEqual(client.posted_orders[0].order_type, "GTC")

        fill, reconciliation = await executor.cancel_and_reconcile_gtc_buy(
            order_id=order_id, asset_id="yes-token", condition_id="test-condition",
            requested_qty=5, wait_seconds=0,
        )
        self.assertIsNone(fill)
        self.assertEqual(reconciliation["size_matched"], "0")
        self.assertEqual(reconciliation["remaining_canceled"], 5)
        self.assertEqual(client.cancel_calls, 1)

    async def test_falling_ask_can_fill_below_limit(self) -> None:
        executor, client = self.make_executor()
        limit_price = _live_buy_limit_price(0.52, "0.01", 3)
        self.assertEqual(limit_price, Decimal("0.55"))
        market = {"ask": Decimal("0.52")}
        client.on_post = lambda _: market.update(ask=Decimal("0.51"))

        client.trades = [SimpleNamespace(
            id="trade-1", taker_order_id=client.order_id,
            size="5", price="0.51", maker_orders=(),
        )]

        accepted, order_id, _ = await executor.buy_gtc_limit(
            asset_id="yes-token", requested_qty=5, limit_price=limit_price,
            tick_size="0.01", condition_id="test-condition",
        )
        self.assertTrue(accepted)
        self.assertEqual(market["ask"], Decimal("0.51"))
        client.order_status = "matched"
        client.size_matched = "5"
        fill, reconciliation = await executor.cancel_and_reconcile_gtc_buy(
            order_id=order_id or "", asset_id="yes-token", condition_id="test-condition",
            requested_qty=5, wait_seconds=0,
        )
        self.assertIsNotNone(fill)
        self.assertAlmostEqual(fill.filled_qty, 5.0)
        self.assertAlmostEqual(fill.vwap, 0.51)
        self.assertAlmostEqual(fill.notional, 2.55)
        self.assertEqual(reconciliation["remaining_canceled"], 0)

    async def test_partial_maker_fill_is_reconciled_before_remainder_cancel(self) -> None:
        executor, client = self.make_executor()
        limit_price = Decimal("0.55")
        accepted, order_id, _ = await executor.buy_gtc_limit(
            asset_id="yes-token", requested_qty=5, limit_price=limit_price,
            tick_size="0.01", condition_id="test-condition",
        )
        self.assertTrue(accepted)
        client.size_matched = "2"
        client.trades = [SimpleNamespace(
            id="trade-maker-1", taker_order_id="counterparty",
            size="5", price="0.54",
            maker_orders=[{"order_id": order_id, "matched_amount": "2"}],
        )]

        fill, reconciliation = await executor.cancel_and_reconcile_gtc_buy(
            order_id=order_id or "", asset_id="yes-token", condition_id="test-condition",
            requested_qty=5, wait_seconds=0,
        )
        self.assertIsNotNone(fill)
        self.assertAlmostEqual(fill.filled_qty, 2.0)
        self.assertAlmostEqual(fill.vwap, 0.54)
        self.assertAlmostEqual(fill.notional, 1.08)
        self.assertEqual(reconciliation["remaining_canceled"], 3)

    async def test_still_open_after_cancel_is_unknown_not_zero_fill(self) -> None:
        executor, client = self.make_executor()
        client.cancel_confirmed = False
        client.order_status = "live"
        with self.assertRaises(LiveOrderUnknown):
            await executor.cancel_and_reconcile_gtc_buy(
                order_id=client.order_id, asset_id="yes-token",
                condition_id="test-condition", requested_qty=5, wait_seconds=0,
            )
        self.assertEqual(client.get_calls, 3)
        self.assertEqual(client.cancel_calls, 3)

    def test_limit_price_respects_market_tick_ceiling(self) -> None:
        self.assertEqual(_live_buy_limit_price(0.96, "0.01", 3), Decimal("0.99"))
        self.assertIsNone(_live_buy_limit_price(0.97, "0.01", 3))


if __name__ == "__main__":
    unittest.main()
