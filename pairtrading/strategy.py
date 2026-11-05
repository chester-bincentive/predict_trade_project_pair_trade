from __future__ import annotations

import math
import statistics
from collections import defaultdict
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


def event_bucket(elapsed_seconds: float, bucket_seconds: int) -> int:
    bucket_count = (300 + bucket_seconds - 1) // bucket_seconds
    return min(bucket_count - 1, max(0, int(max(0.0, elapsed_seconds) // bucket_seconds)))


class RollingCalibration:
    """Market-equal-weighted rolling distribution; only closed markets enter."""

    def __init__(self, store: Store, *, window: int, bucket_seconds: int, min_samples_per_bucket: int = 5) -> None:
        self.store = store
        self.window = window
        self.bucket_seconds = bucket_seconds
        self.min_samples_per_bucket = min_samples_per_bucket
        self.bucket_count = (300 + bucket_seconds - 1) // bucket_seconds
        self._current_key: str | None = None
        self._current_end = 0.0
        self._samples: dict[int, list[float]] = defaultdict(list)

    def observe(self, snapshot: PairSnapshot, spread: float | None, bucket: int) -> None:
        key = snapshot.market_key
        if self._current_key is not None and key != self._current_key:
            self._finish_current()
        if self._current_key != key:
            self._current_key = key
            self._current_end = snapshot.end_at.timestamp()
            self._samples = defaultdict(list)
        if spread is not None and math.isfinite(spread):
            self._samples[bucket].append(spread)

    def _finish_current(self) -> None:
        if self._current_key is None:
            return
        for bucket, samples in self._samples.items():
            if len(samples) < self.min_samples_per_bucket:
                continue
            median = statistics.median(samples)
            self.store.save_history(self._current_key, self._current_end, bucket, median, len(samples))

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
