from __future__ import annotations

import argparse
import asyncio
import csv
from collections import defaultdict
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

from .config import Settings
from .discord_pnl import DiscordPnLNotifier
from .feeds import FeedError, PairMarketFeed
from .paper import demo
from .shadow import ShadowEngine
from .storage import Store


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


async def _live(settings: Settings, duration: float) -> None:
    store = Store(settings.db_path)
    engine = ShadowEngine(store, settings)
    feed = PairMarketFeed(settings)
    start = time.monotonic()
    next_clock_sync = start + 3600.0
    notifier: DiscordPnLNotifier | None = None
    notifier_task: asyncio.Task[None] | None = None
    print("SHADOW MODE ONLY | live WebSocket L2 data; zero real orders are sent to either venue.")
    print(f"rolling={settings.history_window_markets} completed markets, min={settings.min_history_markets}, "
          f"bucket={settings.bucket_seconds}s, z-entry={settings.z_entry:g}, "
          f"hold={settings.hold_seconds}s, min-shares={settings.min_shares:g}, fee=%.3g*p*(1-p)" % settings.taker_fee_coefficient)
    print("Measured: WebSocket receive/source-data age; order-book request RTT is unavailable from the stream. "
          "Simulated assumptions per order: "
          f"Poly network/API={settings.poly_order_network_ms:g}/{settings.poly_order_api_ms:g}ms, "
          f"Binance={settings.binance_order_network_ms:g}/{settings.binance_order_api_ms:g}ms, "
          f"uniform jitter=0..{settings.order_latency_jitter_ms:g}ms.")
    print("Each simulated order can fill only from a fresh WebSocket book update received after its arrival; "
          "IOC remainder is canceled/retried only for exposure unwind and exit.")
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
                snapshot = await feed.capture(timeout_seconds=1.0 if remaining is None else min(1.0, remaining))
                if snapshot is None:
                    continue
                status = engine.process(snapshot)
                print(_line(snapshot, status), flush=True)
                engine.calibration.close_completed(snapshot.captured_at)
            except (FeedError, httpx.HTTPError, OSError, TimeoutError, ValueError) as exc:
                print(f"FEED ERROR ({type(exc).__name__}): {exc}; retrying", flush=True)
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
            rows = store.history_status()
            print(f"Calibration database: {settings.db_path}")
            for bucket, count, latest in rows:
                last_text = "none" if latest is None else datetime.fromtimestamp(latest, timezone.utc).isoformat()
                state = "READY" if count >= settings.min_history_markets else "WARMUP"
                bucket_end = min(300, (bucket + 1) * settings.bucket_seconds)
                print(f"bucket {bucket} ({bucket * settings.bucket_seconds}-{bucket_end}s): "
                      f"stored={count}; rolling-window={min(count, settings.history_window_markets)}/"
                      f"{settings.history_window_markets}; {state}; latest={last_text}")
            if not rows:
                print("No calibration history yet. Run live mode; it will collect quotes without trading until warm-up completes.")
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
        print("Quote ticks, full-depth book snapshots, shadow logs, and paper records were preserved.")
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
            snapshots, levels, events = store.orderbook_storage_stats()
            n, rtt_count, avg_rtt, med_rtt, avg_age, med_age, source_count = store.latency_summary()
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
            def fmt(value):
                return "n/a" if value is None else f"{value:.1f}ms"
            print(f"Measured live data ({n} book snapshots): legacy REST RTT rows={rtt_count}, "
                  f"avg/median="
                  f"{fmt(avg_rtt)}/{fmt(med_rtt)}; source age avg/median="
                  f"{fmt(avg_age)}/{fmt(med_age)} (source timestamps present {source_count}/{n})")
            print("Simulated order latency assumptions: "
                  f"Poly network/API={settings.poly_order_network_ms:g}/{settings.poly_order_api_ms:g}ms; "
                  f"Binance={settings.binance_order_network_ms:g}/{settings.binance_order_api_ms:g}ms; "
                  f"jitter=0..{settings.order_latency_jitter_ms:g}ms")
            print(f"Stored {snapshots} per-outcome full-depth snapshots, {levels} levels, {events} shadow events.")
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
