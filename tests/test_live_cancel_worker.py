from __future__ import annotations

import asyncio
import time
import unittest
from typing import Any

from pairtrading.live_cancel_worker import LiveGtcCancelWorker, _CancelJob


class FakeCancelClient:
    def __init__(self, *, confirms_cancel: bool = True) -> None:
        self.confirms_cancel = confirms_cancel
        self.cancel_calls = 0

    async def cancel_order(self, *, order_id: str) -> dict[str, Any]:
        self.cancel_calls += 1
        if self.confirms_cancel:
            return {"canceled": [order_id], "not_canceled": {}}
        return {"canceled": [], "not_canceled": {order_id: "still live"}}


class TestableLiveGtcCancelWorker(LiveGtcCancelWorker):
    def __init__(self, client: FakeCancelClient) -> None:
        super().__init__(
            private_key="test-key", wallet=None,
            relayer_key="test-relayer-key", relayer_address="test-address",
        )
        self.fake_client = client

    async def _connect(self) -> None:
        self._client = self.fake_client


class LiveGtcCancelWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_deadline_cancel_runs_while_calling_loop_is_blocked(self) -> None:
        client = FakeCancelClient()
        worker = TestableLiveGtcCancelWorker(client)
        await worker.start()
        try:
            future = worker.schedule_cancel("order-1", time.time() + 0.05)
            # Simulate the market-processing loop being unable to run its timers.
            time.sleep(0.15)
            self.assertTrue(future.done())
            result = future.result(timeout=1.0)
            self.assertEqual(result["outcome"], "canceled")
            self.assertEqual(client.cancel_calls, 1)
        finally:
            await worker.close()

    async def test_unconfirmed_cancel_uses_bounded_attempts_and_backoff(self) -> None:
        client = FakeCancelClient(confirms_cancel=False)
        worker = LiveGtcCancelWorker(
            private_key="test-key", wallet=None,
            relayer_key="test-relayer-key", relayer_address="test-address",
        )
        worker._client = client
        job = _CancelJob("order-2", time.time(), asyncio.Event())

        result = await worker._cancel_at_deadline(job)

        self.assertEqual(result["outcome"], "retry_scheduled")
        self.assertEqual(client.cancel_calls, worker._MAX_ATTEMPTS)
        self.assertGreaterEqual(result["retry_delay_seconds"], 5.0)
        self.assertGreater(result["next_retry_epoch"], time.time())


if __name__ == "__main__":
    unittest.main()
