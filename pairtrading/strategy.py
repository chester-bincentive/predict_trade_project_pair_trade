from __future__ import annotations

import math
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import PairSnapshot
from .storage import Store


@dataclass(frozen=True, slots=True)
class ZStats:
    count: int
    mean: float | None
    stdev: float | None

    def score(self, value: float) -> float | None:
        if self.mean is None or self.stdev is None or self.stdev <= 1e-12:
            return None
        return (value - self.mean) / self.stdev


class _RV10Accumulator:
    """sqrt(sum of squared adjacent spread changes over the trailing 10s)."""

    def __init__(self) -> None:
        self.bucket: int | None = None
        self.last: tuple[float, float] | None = None
        self.deltas: deque[tuple[float, float]] = deque()
        self.sum_squares = 0.0

    def update(self, timestamp: float, bucket: int, spread: float) -> float | None:
        if self.bucket != bucket:
            self.bucket = bucket
            self.last = None
            self.deltas.clear()
            self.sum_squares = 0.0
        if self.last is not None and 0.0 <= timestamp - self.last[0] <= 10.0 + 1e-9:
            change = spread - self.last[1]
            square = change * change
            self.deltas.append((timestamp, square))
            self.sum_squares += square
        while self.deltas and self.deltas[0][0] < timestamp - 10.0:
            _, square = self.deltas.popleft()
            self.sum_squares -= square
        self.last = (timestamp, spread)
        return math.sqrt(max(0.0, self.sum_squares)) if self.deltas else None


def _linear_percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile / 100.0
    lower, upper = math.floor(position), math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def event_bucket(elapsed_seconds: float, bucket_seconds: int) -> int:
    bucket_count = (300 + bucket_seconds - 1) // bucket_seconds
    return min(bucket_count - 1, max(0, int(max(0.0, elapsed_seconds) // bucket_seconds)))


class RollingCalibration:
    """Market-equal-weighted spread/RV calibration; only closed markets enter history."""

    def __init__(self, store: Store, *, window: int, bucket_seconds: int,
                 rv_window: int = 100, min_samples_per_bucket: int = 5) -> None:
        self.store = store
        self.window = window
        self.rv_window = rv_window
        self.bucket_seconds = bucket_seconds
        self.min_samples_per_bucket = min_samples_per_bucket
        self.bucket_count = (300 + bucket_seconds - 1) // bucket_seconds
        self._current_key: str | None = None
        self._current_end = 0.0
        self._samples: dict[int, list[float]] = defaultdict(list)
        self._rv_samples: dict[int, list[float]] = defaultdict(list)
        self._rv = _RV10Accumulator()
        if not store.rv_history_backfill_disabled():
            self._backfill_rv_history()
        store.prune_calibration_history(spread_window=self.window, rv_window=self.rv_window)

    def observe(self, snapshot: PairSnapshot, spread: float | None, bucket: int) -> float | None:
        key = snapshot.market_key
        if self._current_key is not None and key != self._current_key:
            self._finish_current()
        if self._current_key != key:
            self._current_key = key
            self._current_end = snapshot.end_at.timestamp()
            self._samples = defaultdict(list)
            self._rv_samples = defaultdict(list)
            self._rv = _RV10Accumulator()
        if spread is not None and math.isfinite(spread):
            self._samples[bucket].append(spread)
            rv10 = self._rv.update(snapshot.captured_at.timestamp(), bucket, spread)
            if rv10 is not None:
                self._rv_samples[bucket].append(rv10)
            return rv10
        return None

    def _finish_current(self) -> None:
        if self._current_key is None:
            return
        for bucket, samples in self._samples.items():
            if len(samples) < self.min_samples_per_bucket:
                continue
            median = statistics.median(samples)
            self.store.save_history(self._current_key, self._current_end, bucket, median, len(samples))
        rv_rows = [
            (self._current_key, self._current_end, bucket, statistics.median(samples), len(samples))
            for bucket, samples in self._rv_samples.items() if samples
        ]
        if rv_rows:
            self.store.save_rv_history_batch(rv_rows)
        self.store.prune_calibration_history(spread_window=self.window, rv_window=self.rv_window)

    def rv_threshold(self, bucket: int, *, before_market_start: float,
                      window: int, min_samples: int) -> tuple[float | None, int]:
        rows = self.store.rv_history(bucket, before_market_start=before_market_start, limit=window)
        values = [row[2] for row in reversed(rows)]
        if len(values) < min_samples:
            return None, len(values)
        return _linear_percentile(values, 90.0), len(values)

    def _backfill_rv_history(self) -> None:
        """Derive missing per-market RV history from previously stored quote ticks."""
        now = time.time()
        for market_key in self.store.quote_markets_without_rv_history():
            rows = self.store.quote_rows_for_market(market_key)
            if not rows:
                continue
            market_start = min(observed_at - elapsed for observed_at, elapsed, _, _ in rows)
            market_end = market_start + 300.0
            if market_end > now:
                continue
            accumulator = _RV10Accumulator()
            samples: dict[int, list[float]] = defaultdict(list)
            for observed_at, _, bucket, spread in rows:
                rv10 = accumulator.update(observed_at, bucket, spread)
                if rv10 is not None:
                    samples[bucket].append(rv10)
            rv_rows = [
                (market_key, market_end, bucket, statistics.median(values), len(values))
                for bucket, values in samples.items() if values
            ]
            if rv_rows:
                self.store.save_rv_history_batch(rv_rows)

    def stats(self, bucket: int, *, before_market_start: float) -> ZStats:
        rows = self.store.history(bucket, before_market_start=before_market_start, limit=self.window)
        values = [row[2] for row in reversed(rows)]
        if len(values) < 2:
            return ZStats(len(values), None, None)
        return ZStats(len(values), statistics.mean(values), statistics.stdev(values))

    def counts(self, *, before_market_start: float) -> dict[int, int]:
        return {
            bucket: len(self.store.history(bucket, before_market_start=before_market_start, limit=self.window))
            for bucket in range(self.bucket_count)
        }

    def close_completed(self, now: datetime) -> None:
        if self._current_key is not None and self._current_end <= now.timestamp():
            self._finish_current()
            self._current_key = None


def mid_spread(snapshot: PairSnapshot) -> float | None:
    poly = snapshot.book("polymarket", "UP").mid
    binance = snapshot.book("binance", "UP").mid
    if poly is None or binance is None:
        return None
    return poly - binance


def quote_row(snapshot: PairSnapshot, *, elapsed: float, bucket: int, spread: float | None) -> dict[str, Any]:
    p, b = snapshot.book("polymarket", "UP"), snapshot.book("binance", "UP")
    pb, pa, bb, ba = p.best_bid, p.best_ask, b.best_bid, b.best_ask
    return {
        "observed_at": snapshot.captured_at.timestamp(), "market_key": snapshot.market_key,
        "elapsed_seconds": elapsed, "bucket": bucket,
        "poly_up_mid": p.mid, "binance_up_mid": b.mid, "spread": spread,
        "poly_bid": None if pb is None else pb.price, "poly_ask": None if pa is None else pa.price,
        "binance_bid": None if bb is None else bb.price, "binance_ask": None if ba is None else ba.price,
        "poly_bid_size": None if pb is None else pb.size, "poly_ask_size": None if pa is None else pa.size,
        "binance_bid_size": None if bb is None else bb.size, "binance_ask_size": None if ba is None else ba.size,
        "source_skew_seconds": snapshot.start_skew_seconds,
    }
