from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from dataclasses import dataclass
from typing import Any


def _field(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(value, dict) and name in value:
            return value[name]
        if hasattr(value, name):
            return getattr(value, name)
    return default


def _ids(value: Any) -> set[str]:
    if isinstance(value, dict):
        return {str(key) for key in value}
    if isinstance(value, (tuple, list, set)):
        return {
            str(_field(item, "order_id", "orderId", "id", default=item))
            for item in value
        }
    return set()


@dataclass
class _CancelJob:
    order_id: str
    deadline_epoch: float
    wake: asyncio.Event
    retry_round: int = 0
    task: asyncio.Task[dict[str, Any]] | None = None
    cancel_started: bool = False
    request_in_flight: bool = False
    shutdown_requested: bool = False


class LiveGtcCancelWorker:
    """Send GTC cancellation deadlines on an event loop isolated from market processing."""

    _CALL_TIMEOUT_SECONDS = 4.0
    _MAX_ATTEMPTS = 3

    def __init__(self, *, private_key: str, wallet: str | None,
                 relayer_key: str, relayer_address: str) -> None:
        self._private_key = private_key
        self._wallet = wallet
        self._relayer_key = relayer_key
        self._relayer_address = relayer_address
        self._startup: concurrent.futures.Future[None] = concurrent.futures.Future()
        self._thread = threading.Thread(
            target=self._thread_main,
            name="polymarket-gtc-cancel-loop",
            daemon=True,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._context: Any | None = None
        self._client: Any | None = None
        self._jobs: dict[str, _CancelJob] = {}
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._thread.start()
        await asyncio.wait_for(asyncio.wrap_future(self._startup), timeout=30.0)

    def schedule_cancel(
        self, order_id: str, deadline_epoch: float, *, retry_round: int = 0,
    ) -> concurrent.futures.Future[dict[str, Any]]:
        loop = self._loop
        if loop is None or not self._thread.is_alive():
            raise RuntimeError("independent GTC cancel worker is not running")
        return asyncio.run_coroutine_threadsafe(
            self._schedule_on_worker(str(order_id), float(deadline_epoch), int(retry_round)), loop,
        )

    def cancel_now(self, order_id: str, *, retry_round: int = 0) -> concurrent.futures.Future[dict[str, Any]]:
        return self.schedule_cancel(str(order_id), time.time(), retry_round=retry_round)

    def disarm(self, order_id: str) -> concurrent.futures.Future[bool]:
        loop = self._loop
        if loop is None or not self._thread.is_alive():
            raise RuntimeError("independent GTC cancel worker is not running")
        return asyncio.run_coroutine_threadsafe(
            self._disarm_on_worker(str(order_id)), loop,
        )

    async def close(self, *, drain_seconds: float = 15.0) -> None:
        loop = self._loop
        if loop is None or not self._thread.is_alive():
            return
        close_future = asyncio.run_coroutine_threadsafe(
            self._close_on_worker(drain_seconds), loop,
        )
        try:
            await asyncio.wait_for(asyncio.wrap_future(close_future), timeout=drain_seconds + 5.0)
        finally:
            loop.call_soon_threadsafe(loop.stop)
            await asyncio.to_thread(self._thread.join, 5.0)
            self._loop = None

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        try:
            loop.run_until_complete(self._connect())
        except BaseException as exc:
            if not self._startup.done():
                self._startup.set_exception(exc)
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.close()
            return

        if not self._startup.done():
            self._startup.set_result(None)
        print("LIVE GTC CANCEL WORKER READY isolated_loop=true", flush=True)
        loop.run_forever()

        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    async def _connect(self) -> None:
        from polymarket import AsyncSecureClient, RelayerApiKey

        self._context = await AsyncSecureClient.create(
            private_key=self._private_key,
            wallet=self._wallet,
            api_key=RelayerApiKey(key=self._relayer_key, address=self._relayer_address),
        )
        self._client = await self._context.__aenter__()
        # Retain only what the SDK needs for this isolated cancellation session.
        self._private_key = ""
        self._relayer_key = ""

    async def _schedule_on_worker(
        self, order_id: str, deadline_epoch: float, retry_round: int,
    ) -> dict[str, Any]:
        existing = self._jobs.get(order_id)
        if existing is not None and existing.task is not None and not existing.task.done():
            if deadline_epoch < existing.deadline_epoch:
                existing.deadline_epoch = deadline_epoch
                existing.wake.set()
            return await asyncio.shield(existing.task)

        job = _CancelJob(order_id, deadline_epoch, asyncio.Event(), retry_round)
        job.task = asyncio.create_task(
            self._cancel_at_deadline(job),
            name=f"gtc-cancel-deadline-{order_id[:10]}",
        )
        self._jobs[order_id] = job
        print(
            f"LIVE GTC CANCEL DEADLINE ARMED order_id={order_id} "
            f"deadline_epoch={deadline_epoch:.6f} worker=isolated-loop",
            flush=True,
        )
        try:
            return await asyncio.shield(job.task)
        finally:
            if job.task.done() and self._jobs.get(order_id) is job:
                self._jobs.pop(order_id, None)

    async def _disarm_on_worker(self, order_id: str) -> bool:
        job = self._jobs.get(order_id)
        if (job is None or job.task is None or job.task.done()
                or job.request_in_flight):
            return False
        self._jobs.pop(order_id, None)
        job.task.cancel()
        await asyncio.gather(job.task, return_exceptions=True)
        print(f"LIVE GTC CANCEL DEADLINE DISARMED order_id={order_id} reason=order_reconciled", flush=True)
        return True

    async def _cancel_at_deadline(self, job: _CancelJob) -> dict[str, Any]:
        while True:
            job.wake.clear()
            deadline_epoch = job.deadline_epoch
            remaining = deadline_epoch - time.time()
            if remaining <= 0:
                break
            sleep_task = asyncio.create_task(asyncio.sleep(min(remaining, 1.0)))
            wake_task = asyncio.create_task(job.wake.wait())
            try:
                done, pending = await asyncio.wait(
                    {sleep_task, wake_task}, return_when=asyncio.FIRST_COMPLETED,
                )
            except asyncio.CancelledError:
                for task in (sleep_task, wake_task):
                    task.cancel()
                await asyncio.gather(sleep_task, wake_task, return_exceptions=True)
                raise
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if sleep_task in done:
                continue

        started_epoch = time.time()
        started_monotonic = time.monotonic()
        job.cancel_started = True
        print(
            f"LIVE GTC DEADLINE CANCEL STARTED order_id={job.order_id} "
            f"deadline_epoch={deadline_epoch:.6f} started_epoch={started_epoch:.6f} "
            f"lateness_ms={max(0.0, (started_epoch - deadline_epoch) * 1000.0):.1f}",
            flush=True,
        )
        last_error: Exception | None = None
        last_outcome = "error"
        for attempt in range(1, self._MAX_ATTEMPTS + 1):
            try:
                assert self._client is not None
                job.request_in_flight = True
                try:
                    call_task = asyncio.ensure_future(self._client.cancel_order(order_id=job.order_id))
                    try:
                        done, _ = await asyncio.wait({call_task}, timeout=self._CALL_TIMEOUT_SECONDS)
                    except asyncio.CancelledError:
                        call_task.cancel()
                        call_task.add_done_callback(self._consume_late_result)
                        raise
                    if not done:
                        call_task.cancel()
                        call_task.add_done_callback(self._consume_late_result)
                        raise asyncio.TimeoutError(
                            f"cancel_order exceeded {self._CALL_TIMEOUT_SECONDS:.1f}s"
                        )
                    response = call_task.result()
                finally:
                    job.request_in_flight = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                last_error = exc
                print(
                    f"LIVE GTC DEADLINE CANCEL ERROR order_id={job.order_id} "
                    f"attempt={attempt}/{self._MAX_ATTEMPTS} "
                    f"error_type={type(exc).__name__}",
                    flush=True,
                )
                if attempt < self._MAX_ATTEMPTS:
                    await self._retry_pause(job, 0.2 * attempt)
                continue

            canceled = _ids(_field(response, "canceled", "cancelled", default=()))
            not_canceled = _ids(_field(response, "not_canceled", "notCanceled", default={}))
            if job.order_id in canceled:
                outcome = "canceled"
            elif job.order_id in not_canceled:
                outcome = "not_canceled"
            else:
                outcome = "unconfirmed_response"
            last_outcome = outcome
            result = {
                "order_id": job.order_id,
                "outcome": outcome,
                "cancellation_confirmed": job.order_id in canceled,
                "canceled": [job.order_id] if job.order_id in canceled else [],
                "not_canceled": {job.order_id: outcome} if job.order_id in not_canceled else {},
                "canceled_order_count": len(canceled),
                "not_canceled_count": len(not_canceled),
                "attempt": attempt,
                "deadline_epoch": job.deadline_epoch,
                "cancel_started_epoch": started_epoch,
                "cancel_completed_epoch": time.time(),
                "elapsed_ms": round((time.monotonic() - started_monotonic) * 1000.0, 3),
            }
            print(
                f"LIVE GTC DEADLINE CANCEL RESULT order_id={job.order_id} outcome={outcome} "
                f"attempt={attempt} elapsed_ms={result['elapsed_ms']:.1f}",
                flush=True,
            )
            if outcome == "canceled":
                return result
            last_error = RuntimeError(f"cancel response outcome={outcome}")
            if attempt < self._MAX_ATTEMPTS:
                await self._retry_pause(job, 0.2 * attempt)

        retry_delay = min(60.0, 5.0 * (2 ** min(max(job.retry_round, 0), 4)))
        retry_after = time.time() + retry_delay
        result = {
            "order_id": job.order_id,
            "outcome": "retry_scheduled",
            "last_response_outcome": last_outcome,
            "last_error_type": type(last_error).__name__ if last_error else None,
            "attempt": self._MAX_ATTEMPTS,
            "retry_round": job.retry_round + 1,
            "retry_delay_seconds": retry_delay,
            "next_retry_epoch": retry_after,
            "deadline_epoch": job.deadline_epoch,
            "cancel_started_epoch": started_epoch,
            "cancel_completed_epoch": time.time(),
            "elapsed_ms": round((time.monotonic() - started_monotonic) * 1000.0, 3),
        }
        print(
            f"LIVE GTC CANCEL RETRY SCHEDULED order_id={job.order_id} "
            f"last_outcome={last_outcome} retry_delay_seconds={retry_delay:.1f} "
            f"attempts={self._MAX_ATTEMPTS}",
            flush=True,
        )
        return result

    async def _retry_pause(self, job: _CancelJob, delay_seconds: float) -> None:
        if job.shutdown_requested:
            return
        job.wake.clear()
        try:
            await asyncio.wait_for(job.wake.wait(), timeout=delay_seconds)
        except asyncio.TimeoutError:
            pass

    @staticmethod
    def _consume_late_result(task: asyncio.Future[Any]) -> None:
        if task.cancelled():
            return
        try:
            task.exception()
        except Exception:
            pass

    async def _close_on_worker(self, drain_seconds: float) -> None:
        pending = []
        for job in self._jobs.values():
            if job.task is None or job.task.done():
                continue
            job.shutdown_requested = True
            job.deadline_epoch = min(job.deadline_epoch, time.time())
            job.wake.set()
            pending.append(job.task)
        if pending:
            _, not_done = await asyncio.wait(pending, timeout=max(0.0, drain_seconds))
            for task in not_done:
                task.cancel()
            if not_done:
                await asyncio.gather(*not_done, return_exceptions=True)
        if self._context is not None:
            context, self._context = self._context, None
            await context.__aexit__(None, None, None)
        self._client = None
