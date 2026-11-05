from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True, slots=True)
class Level:
    price: float
    size: float


@dataclass(frozen=True, slots=True)
class Book:
    venue: str
    outcome: str
    token_id: str
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    received_at: datetime
    source_at: datetime | None = None
    request_started_at: datetime | None = None
    request_rtt_ms: float | None = None

    @property
    def best_bid(self) -> Level | None:
        return self.bids[0] if self.bids else None

    @property
    def best_ask(self) -> Level | None:
        return self.asks[0] if self.asks else None

    @property
    def mid(self) -> float | None:
        bid, ask = self.best_bid, self.best_ask
        if bid is None or ask is None:
            return None
        return (bid.price + ask.price) / 2.0

    def age_seconds(self, now: datetime) -> float:
        anchor = self.source_at or self.received_at
        return max(0.0, (now - anchor).total_seconds())


@dataclass(frozen=True, slots=True)
class Market:
    venue: str
    key: str
    label: str
    start_at: datetime
    end_at: datetime
    up_token: str
    down_token: str
    up_market_id: str | None = None
    down_market_id: str | None = None


@dataclass(frozen=True, slots=True)
class PairSnapshot:
    market_key: str
    polymarket: Market
    binance: Market
    books: dict[tuple[str, str], Book]
    captured_at: datetime
    start_skew_seconds: float

    @property
    def start_at(self) -> datetime:
        return max(self.polymarket.start_at, self.binance.start_at)

    @property
    def end_at(self) -> datetime:
        return min(self.polymarket.end_at, self.binance.end_at)

    def book(self, venue: str, outcome: str) -> Book:
        return self.books[(venue, outcome)]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
