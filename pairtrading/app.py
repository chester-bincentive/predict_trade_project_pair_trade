from __future__ import annotations

import argparse
import asyncio
import csv
from collections import defaultdict
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import uuid

import httpx
from dotenv import load_dotenv

from .config import Settings
from .discord_pnl import DiscordPnLNotifier
from .feeds import FeedError, PairMarketFeed
from .paper import demo
from .shadow import ShadowEngine
from .storage import Store
from .strategy import RollingCalibration


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BTC 5-minute Polymarket x Binance Predict shadow pair-trading demo")
    parser.add_argument("mode", choices=("demo", "live", "import-history", "history-status", "clear-history", "report"), nargs="?", default="demo")
    parser.add_argument("--history-csv", type=Path, help="normalized closed-market bucket medians CSV")
    parser.add_argument("--duration-seconds", type=float, default=0.0, help="stop live shadow scan after N seconds; 0 runs until Ctrl+C")
    return parser.parse_args()


def _import_history(path: Path, store: Store, settings: Settings) -> int:
    required = {"market_key", "market_end_time", "bucket", "median_spread"}
    inserted = 0
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise ValueError("CSV columns required: market_key,market_end_time,bucket,median_spread[,sample_count]")
        for row in reader:
            end = row["market_end_time"]
            try:
                end_epoch = float(end)
                if end_epoch > 100_000_000_000:
                    end_epoch /= 1000.0
            except ValueError:
                end_epoch = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()
            bucket = int(row["bucket"])
            bucket_count = (300 + settings.bucket_seconds - 1) // settings.bucket_seconds
            if not 0 <= bucket < bucket_count:
                raise ValueError(f"bucket must be 0..{bucket_count - 1}, got {bucket}")
            store.save_history(row["market_key"], end_epoch, bucket, float(row["median_spread"]),
                               int(row.get("sample_count") or 1))
            inserted += 1
    return inserted


def _line(snapshot, message: str) -> str:
    elapsed = max(0.0, (snapshot.captured_at - snapshot.start_at).total_seconds())
    pu, bu = snapshot.book("polymarket", "UP"), snapshot.book("binance", "UP")
    def side(book, key):
        level = getattr(book, key)
        return "--" if level is None else f"{level.price:.3f}x{level.size:g}"
    spread = "--" if pu.mid is None or bu.mid is None else f"{pu.mid - bu.mid:+.4f}"
    return (f"{snapshot.captured_at.isoformat(timespec='seconds')}  {snapshot.market_key}  t={elapsed:5.1f}s "
            f"skew={snapshot.start_skew_seconds:.2f}s  PolyUP {side(pu, 'best_bid')}/{side(pu, 'best_ask')} "
            f"BinUP {side(bu, 'best_bid')}/{side(bu, 'best_ask')}  S={spread}  {message}")


LIVE_STATUS_LOG_INTERVAL_SECONDS = 60.0
LIVE_STATUS_CHANGE_LOG_INTERVAL_SECONDS = 5.0


def _status_log_key(message: str) -> str:
    if message.startswith("SKIP stale quotes:"):
        return "SKIP stale quotes"
    if message.startswith("DATA COLLECTION ONLY; insufficient closed-market history"):
        return "DATA COLLECTION ONLY; insufficient closed-market history"
    if message.startswith("DATA COLLECTION ONLY; bucket="):
        return message.split(" has ", 1)[0]
    if message.startswith("SHADOW ENTRY pending"):
        return message.split(";", 1)[0]
    for prefix in (
        "SHADOW OPEN",
        "SHADOW SINGLE-LEG EXPOSURE",
        "SHADOW EXIT pending",
    ):
        if message.startswith(prefix):
            return prefix
    if message.startswith("OPEN shadow trade"):
        return message.split(";", 1)[0]
    return message


class _LiveStatusLogger:
    """Keep live snapshots useful without logging every WebSocket update."""

    def __init__(self) -> None:
        self.last_key: str | None = None
        self.last_logged_at = 0.0

    def log(self, snapshot, message: str) -> None:
        now = time.monotonic()
        key = _status_log_key(message)
        changed = key != self.last_key
        heartbeat_due = now - self.last_logged_at >= LIVE_STATUS_LOG_INTERVAL_SECONDS
        important_change = changed and key.startswith((
            "SHADOW ENTRY",
            "SHADOW SINGLE-LEG",
            "SHADOW OPEN",
            "SHADOW EXIT",
            "SHADOW CLOSED",
            "SHADOW NO PAIR",
            "SHADOW UNRESOLVED",
            "OPEN shadow trade",
        ))
        ordinary_change_due = changed and now - self.last_logged_at >= LIVE_STATUS_CHANGE_LOG_INTERVAL_SECONDS
        if self.last_key is None or heartbeat_due or important_change or ordinary_change_due:
            print(_line(snapshot, message), flush=True)
            self.last_key = key
            self.last_logged_at = now


class _FeedErrorLogger:
    """Collapse repeated identical feed errors while preserving recovery signals."""

    def __init__(self) -> None:
        self.last_key: str | None = None
        self.last_logged_at = 0.0

    def report(self, exc: Exception) -> None:
        now = time.monotonic()
        key = f"{type(exc).__name__}: {exc}"
        if key != self.last_key or now - self.last_logged_at >= LIVE_STATUS_LOG_INTERVAL_SECONDS:
            print(f"FEED ERROR ({type(exc).__name__}): {exc}; retrying", flush=True)
            self.last_key = key
            self.last_logged_at = now

    def recovered(self) -> None:
        if self.last_key is not None:
            print("FEED RECOVERED; live market data resumed.", flush=True)
            self.last_key = None
            self.last_logged_at = 0.0


async def _live(settings: Settings, duration: float) -> None:
    store = Store(settings.db_path)
    run_id = uuid.uuid4().hex
    start = time.monotonic()
    store.record_runtime_event(
        run_id=run_id, event_type="run_started",
        details={
            "mode": "live_shadow",
            "ou_entry_threshold": settings.ou_entry_threshold,
            "ou_exit_threshold": settings.ou_exit_threshold,
            "min_ou_history_markets": settings.min_ou_history_markets,
            "spread_history_window_markets": settings.history_window_markets,
            "min_history_markets": settings.min_history_markets,
            "rv_history_window_markets": settings.rv_history_window_markets,
            "min_rv_history_markets": settings.min_rv_history_markets,
            "min_market_time_left_seconds": settings.min_market_time_left_seconds,
            "max_hold_seconds": settings.max_hold_seconds,
            "shares_per_leg": settings.min_shares,
            "fee_coefficient": settings.taker_fee_coefficient,
            "max_quote_age_seconds": settings.max_quote_age_seconds,
            "order_delay_ms": {
                "polymarket": settings.poly_order_network_ms + settings.poly_order_api_ms,
                "binance": settings.binance_order_network_ms + settings.binance_order_api_ms,
            },
        },
    )

    def record_subscription(venue: str, market_key: str, details: dict[str, Any]) -> None:
        store.record_runtime_event(
            run_id=run_id, event_type="market_subscription",
            market_key=market_key, venue=venue, details=details,
        )

    engine = ShadowEngine(store, settings, run_id=run_id)
    feed = PairMarketFeed(settings, subscription_recorder=record_subscription)
    next_clock_sync = start + 3600.0
    notifier: DiscordPnLNotifier | None = None
    notifier_task: asyncio.Task[None] | None = None
    status_logger = _LiveStatusLogger()
    feed_error_logger = _FeedErrorLogger()
    print("SHADOW MODE ONLY | live WebSocket L2 data; zero real orders are sent to either venue.")
    print(f"rolling={settings.history_window_markets} completed markets, "
          f"min-spread-history={settings.min_history_markets}, "
          f"min-OU-history={settings.min_ou_history_markets}, bucket={settings.bucket_seconds}s, "
          f"shares={settings.min_shares:g}, fee=%.3g*p*(1-p)" % settings.taker_fee_coefficient)
    print(f"Entry filter: |OU score| > {settings.ou_entry_threshold:g} and "
          "RV10 > same-bucket historical q90 "
          f"(last {settings.rv_history_window_markets} completed markets; "
          f"minimum {settings.min_rv_history_markets}).")
    print(f"Exit rule: directional OU score returns to ±{settings.ou_exit_threshold:g}; "
          f"maximum hold {settings.max_hold_seconds}s; entry requires at least "
          f"{settings.min_market_time_left_seconds}s left in the market.")
    print("Paper fill events retain WebSocket source-data age; raw quote/orderbook snapshots are not stored. "
          "Order-book request RTT is unavailable from the stream.")
    print("Simulated order delay: fixed 330ms from submit to arrival, with no jitter.")
    print("At arrival, the first IOC uses the newest in-memory WebSocket book received by that instant; "
          "it does not wait for a new quote. IOC remainder retries wait for a later book update.")
    try:
        await feed.start()
        if settings.discord_webhook_url:
            notifier = DiscordPnLNotifier(
                store, settings.discord_webhook_url, settings.discord_pnl_interval_seconds
            )
            notifier_task = asyncio.create_task(notifier.run(), name="discord-pnl-notifier")
            print(f"Discord PnL notifications enabled; interval={settings.discord_pnl_interval_seconds}s.")
        while True:
            now = time.monotonic()
            if duration > 0 and now - start >= duration:
                break
            if now >= next_clock_sync:
                await feed.binance.sync_clock()
                next_clock_sync = now + 3600.0
            try:
                remaining = None if duration <= 0 else max(0.0, duration - (now - start))
                due = engine.next_wakeup_epoch()
                wall_now = time.time()
                if due is not None and due <= wall_now:
                    arrival_snapshot = feed.snapshot_at(due)
                    if arrival_snapshot is not None:
                        status = engine.process(arrival_snapshot, evaluate_signals=False)
                        status_logger.log(arrival_snapshot, status)
                        continue
                until_due = 1.0 if due is None or due <= wall_now else due - wall_now
                timeout = min(1.0, until_due)
                if remaining is not None:
                    timeout = min(timeout, remaining)
                snapshot = await feed.capture(timeout_seconds=timeout)
                if snapshot is None:
                    due = engine.next_wakeup_epoch()
                    if due is not None and due <= time.time():
                        arrival_snapshot = feed.snapshot_at(due)
                        if arrival_snapshot is not None:
                            status = engine.process(arrival_snapshot, evaluate_signals=False)
                            status_logger.log(arrival_snapshot, status)
                    continue
                feed_error_logger.recovered()
                # If a WebSocket update and an order timer race, replay every
                # due arrival against its exact as-of memory-book snapshot first.
                due = engine.next_wakeup_epoch()
                while due is not None and due <= snapshot.captured_at.timestamp():
                    arrival_snapshot = feed.snapshot_at(due)
                    if arrival_snapshot is None:
                        break
                    status = engine.process(arrival_snapshot, evaluate_signals=False)
                    status_logger.log(arrival_snapshot, status)
                    due = engine.next_wakeup_epoch()
                status = engine.process(snapshot)
                status_logger.log(snapshot, status)
                engine.calibration.close_completed(snapshot.captured_at)
            except (FeedError, httpx.HTTPError, OSError, TimeoutError, ValueError) as exc:
                feed_error_logger.report(exc)
                await asyncio.sleep(settings.poll_seconds)
    finally:
        try:
            if notifier_task is not None:
                notifier_task.cancel()
                await asyncio.gather(notifier_task, return_exceptions=True)
        finally:
            try:
                if notifier is not None:
                    await notifier.close()
            finally:
                try:
                    await feed.close()
                finally:
                    try:
                        store.record_runtime_event(
                            run_id=run_id, event_type="run_stopped",
                            details={"duration_seconds": round(time.monotonic() - start, 3)},
                        )
                    finally:
                        store.close()


def main() -> None:
    load_dotenv()
    args = _arguments()
    settings = Settings.from_env()
    if args.mode == "demo":
        demo(settings)
        return
    if args.mode == "import-history":
        if args.history_csv is None:
            raise SystemExit("import-history needs --history-csv")
        store = Store(settings.db_path)
        try:
            count = _import_history(args.history_csv, store, settings)
            print(f"Imported {count} rows into {settings.db_path} (duplicate market/bucket rows are ignored).")
        finally:
            store.close()
        return
    if args.mode == "history-status":
        store = Store(settings.db_path)
        try:
            RollingCalibration(
                store, window=settings.history_window_markets,
                rv_window=settings.rv_history_window_markets,
                bucket_seconds=settings.bucket_seconds,
            )
            spread_rows = {bucket: (count, latest) for bucket, count, latest in store.history_status()}
            rv_rows = {bucket: (count, latest) for bucket, count, latest in
                       store.rv_history_status(before_market_start=time.time())}
            print(f"Calibration database: {settings.db_path}")
            bucket_count = (300 + settings.bucket_seconds - 1) // settings.bucket_seconds
            for bucket in range(bucket_count):
                count, latest = spread_rows.get(bucket, (0, None))
                last_text = "none" if latest is None else datetime.fromtimestamp(latest, timezone.utc).isoformat()
                state = "READY" if count >= settings.min_history_markets else "WARMUP"
                ou_state = "READY" if count >= settings.min_ou_history_markets else "WARMUP"
                bucket_end = min(300, (bucket + 1) * settings.bucket_seconds)
                print(f"bucket {bucket} ({bucket * settings.bucket_seconds}-{bucket_end}s): "
                      f"spread-history={min(count, settings.history_window_markets)}/"
                      f"{settings.history_window_markets} ({state}); "
                      f"OU-entry-history={min(count, settings.history_window_markets)}/"
                      f"{settings.min_ou_history_markets} ({ou_state}); latest={last_text}")
            for bucket in range(bucket_count):
                count, latest = rv_rows.get(bucket, (0, None))
                state = "READY" if count >= settings.min_rv_history_markets else "WARMUP"
                latest_text = "none" if latest is None else datetime.fromtimestamp(
                    latest, timezone.utc
                ).isoformat()
                print(f"RV10 q90 bucket {bucket}: stored={count}; rolling-window="
                      f"{min(count, settings.rv_history_window_markets)}/"
                      f"{settings.rv_history_window_markets}; min={settings.min_rv_history_markets}; "
                      f"{state}; latest={latest_text}")
            if not any(count for count, _ in spread_rows.values()):
                print("No spread calibration history yet. Run live mode to collect market data.")
        finally:
            store.close()
        return
    if args.mode == "clear-history":
        store = Store(settings.db_path)
        try:
            deleted_rows = store.clear_history()
        finally:
            store.close()
        seed_csv = Path(__file__).resolve().parents[1] / "data" / "market_history.csv"
        removed_seed = seed_csv.exists()
        if removed_seed:
            seed_csv.unlink()
        print(f"Deleted {deleted_rows} calibration rows from {settings.db_path}.")
        print(f"Removed old bundled market_history.csv: {'yes' if removed_seed else 'no'}.")
        print("Legacy quote/orderbook rows and paper-trade logs were preserved; live mode no longer writes raw snapshots.")
        print("RV10 history was cleared too; preserved legacy quote ticks will not rebuild it automatically.")
        return
    if args.mode == "report":
        store = Store(settings.db_path)
        try:
            rows = store.shadow_trade_rows()
            total = len(rows)
            scenario_keys = sorted({str(row["scenario_key"]) for row in rows})
            closed = [row for row in rows if row["status"] == "CLOSED"]
            completed = [row for row in rows if row["status"] in {"CLOSED", "NO_PAIR"}
                         and row["net_pnl"] is not None]
            no_pair = sum(row["status"] == "NO_PAIR" for row in rows)
            unresolved = sum(str(row["status"]).startswith("UNRESOLVED") for row in rows)
            full = sum(float(row["paired_shares"]) + 1e-9 >= float(row["requested_shares"])
                       for row in rows if float(row["requested_shares"]) > 0)
            any_pair = sum(float(row["paired_shares"]) > 1e-9 for row in rows)
            mean_pair_ratio = (sum(min(1.0, float(row["paired_shares"]) / float(row["requested_shares"]))
                                   for row in rows if float(row["requested_shares"]) > 0) / total) if total else None
            closed_pnls = [float(row["net_pnl"]) for row in completed]
            win_rate = (sum(pnl > 0 for pnl in closed_pnls) / len(closed_pnls)) if closed_pnls else None
            mean_pnl = sum(closed_pnls) / len(closed_pnls) if closed_pnls else None
            daily: dict[str, list[float]] = defaultdict(list)
            for row in completed:
                if row["net_pnl"] is not None:
                    day = datetime.fromtimestamp(float(row["decision_epoch"]), timezone.utc).date().isoformat()
                    daily[day].append(float(row["net_pnl"]))
            print(f"Shadow report: {settings.db_path}")
            print(f"Signals={total}; at least some paired={any_pair}/{total}; fully paired={full}/{total}; "
                  f"NO_PAIR={no_pair}; unresolved at market end={unresolved}")
            for scenario in scenario_keys:
                group = [row for row in rows if row["scenario_key"] == scenario]
                group_done = [row for row in group if row["status"] in {"CLOSED", "NO_PAIR"}
                              and row["net_pnl"] is not None]
                group_pnls = [float(row["net_pnl"]) for row in group_done]
                group_rate = (sum(value > 0 for value in group_pnls) / len(group_pnls)) if group_pnls else None
                print(f"Scenario [{scenario}] trades={len(group)}; completed={len(group_done)}; "
                      f"net={sum(group_pnls):+.4f}; mean="
                      f"{(sum(group_pnls)/len(group_pnls)):+.4f}" if group_pnls else
                      f"Scenario [{scenario}] trades={len(group)}; completed=0; PnL=n/a")
                if group_rate is not None:
                    print(f"  win rate={group_rate:.1%}")
            print(f"Mean pair fill ratio={mean_pair_ratio:.1%}" if mean_pair_ratio is not None
                  else "Mean pair fill ratio: n/a")
            if closed_pnls:
                print(f"Closed-only net PnL: total={sum(closed_pnls):+.4f}; mean/trade={mean_pnl:+.4f}; "
                      f"win rate={win_rate:.1%}; completed trades={len(closed_pnls)} "
                      f"(paired exits={len(closed)}, no-pair/unwind={no_pair})")
                print("Daily closed PnL (UTC): " + "; ".join(
                    f"{day} {sum(values):+.4f} ({len(values)} trades, "
                    f"{sum(value > 0 for value in values)/len(values):.0%} wins)"
                    for day, values in sorted(daily.items())
                ))
            else:
                print("Closed-only net PnL / win rate: n/a")
            print("Simulated order delay: fixed "
                  f"{settings.poly_order_network_ms + settings.poly_order_api_ms:g}ms per order; "
                  f"jitter=0..{settings.order_latency_jitter_ms:g}ms; "
                  "first IOC uses latest in-memory book as of arrival")
            print("Live mode keeps books in memory; the database stores trade and runtime events only.")
            if unresolved:
                print("Unresolved trades are excluded from PnL; no settlement result is invented.")
        finally:
            store.close()
        return
    try:
        asyncio.run(_live(settings, args.duration_seconds))
    except KeyboardInterrupt:
        print("\nStopped.")
    except FeedError as exc:
        raise SystemExit(str(exc)) from exc
