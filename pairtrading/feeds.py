from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import random
import re
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import urlencode

import httpx
import websockets

from .models import Book, Level, Market, PairSnapshot, utc_now


GAMMA_URL = "https://gamma-api.polymarket.com"
BINANCE_URL = "https://api.binance.com"
BINANCE_WS_URL = "wss://api.binance.com/sapi/wss"
POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_SLUG_RE = re.compile(r"btc-updown-5m-(\d+)")
_LOGGER = logging.getLogger(__name__)


class FeedError(RuntimeError):
    pass


def _value(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if row.get(key) is not None:
            return row[key]
    return default


def _timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        result = value
    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.replace(".", "", 1).isdigit()):
        number = float(value)
        if abs(number) >= 100_000_000_000:
            number /= 1000.0
        result = datetime.fromtimestamp(number, tz=timezone.utc)
    elif isinstance(value, str):
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise FeedError(f"unrecognized market timestamp: {value!r}")
    if result.tzinfo is None or result.utcoffset() is None:
        raise FeedError("market timestamp is missing a timezone")
    return result.astimezone(timezone.utc)


def _market_outcomes(raw: Mapping[str, Any]) -> tuple[str, str, str, str]:
    outcomes = _json_array(raw.get("outcomes"))
    tokens = _json_array(raw.get("clobTokenIds"))
    if len(outcomes) != len(tokens):
        raise FeedError("Polymarket outcomes/token IDs do not align")
    mapped = {str(name).strip().upper(): str(token) for name, token in zip(outcomes, tokens)}
    up = next((mapped[k] for k in ("UP", "YES") if k in mapped), None)
    down = next((mapped[k] for k in ("DOWN", "NO") if k in mapped), None)
    if not up or not down:
        raise FeedError(f"could not identify Polymarket UP/DOWN tokens: {list(mapped)}")
    up_name = "UP" if "UP" in mapped else "YES"
    down_name = "DOWN" if "DOWN" in mapped else "NO"
    return up, down, up_name, down_name


def _json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    raise FeedError("expected a JSON array")


def _parse_poly_market(row: Mapping[str, Any]) -> Market:
    slug = str(row.get("slug", ""))
    match = _SLUG_RE.fullmatch(slug)
    if not match:
        raise FeedError(f"not a BTC 5-minute Polymarket slug: {slug}")
    start = datetime.fromtimestamp(int(match.group(1)), tz=timezone.utc)
    end = _timestamp(_value(row, "endDate", "endDateIso"))
    if abs((end - start).total_seconds() - 300.0) > 2.0:
        end = start + timedelta(seconds=300)
    up, down, _, _ = _market_outcomes(row)
    return Market("polymarket", slug, slug, start, end, up, down)


def _parse_binance_market(topic: Mapping[str, Any]) -> Market:
    market_key = str(_value(topic, "marketTopicId", "market_topic_id", default=""))
    start = _timestamp(_value(topic, "startDate", "start_time", "startTime"))
    end = _timestamp(_value(topic, "endDate", "end_time", "endTime"))
    if not market_key or abs((end - start).total_seconds() - 300.0) > 2.0:
        raise FeedError("Binance topic is not a 5-minute market")
    if str(topic.get("symbol", "BTCUSDT")).upper() not in {"", "BTCUSDT"}:
        raise FeedError("Binance topic is not BTCUSDT")
    markets = topic.get("markets")
    if not isinstance(markets, Sequence) or isinstance(markets, (str, bytes)):
        raise FeedError("Binance topic has no markets array")

    records: dict[str, Mapping[str, Any]] = {}
    tokens: dict[str, str] = {}
    for entry in markets:
        if not isinstance(entry, Mapping):
            continue
        title = str(_value(entry, "title", "marketTitle", "name", default="")).upper()
        outcomes = entry.get("outcomes")
        named_tokens: dict[str, str] = {}
        if isinstance(outcomes, Sequence) and not isinstance(outcomes, (str, bytes)):
            for outcome in outcomes:
                if not isinstance(outcome, Mapping):
                    continue
                name = str(outcome.get("name", "")).strip().upper()
                token = _value(outcome, "tokenId", "token_id")
                if token is not None and name:
                    named_tokens[name] = str(token)

        # Binance models each direction as its own internal market in the
        # current response shape (market title UP/DOWN, outcome token YES).
        # Also accept an explicitly labelled UP/DOWN or YES/NO outcome pair.
        direction = None
        if "UP" in title and "DOWN" not in title:
            direction = "UP"
        elif "DOWN" in title and "UP" not in title:
            direction = "DOWN"
        if direction:
            token = named_tokens.get(direction) or named_tokens.get("YES")
            records[direction] = entry
            if token:
                tokens[direction] = token
        else:
            up_token = named_tokens.get("UP") or named_tokens.get("YES")
            down_token = named_tokens.get("DOWN") or named_tokens.get("NO")
            if up_token and down_token:
                records["UP"] = records["DOWN"] = entry
                tokens["UP"], tokens["DOWN"] = up_token, down_token

    if not tokens.get("UP") or not tokens.get("DOWN"):
        raise FeedError("Binance topic does not expose unambiguous UP and DOWN token IDs")
    up_id = _value(records.get("UP", {}), "marketId", "market_id")
    down_id = _value(records.get("DOWN", {}), "marketId", "market_id")
    return Market(
        "binance", market_key, str(_value(topic, "slug", "title", default=market_key)),
        start, end, tokens["UP"], tokens["DOWN"],
        None if up_id is None else str(up_id), None if down_id is None else str(down_id),
    )


def _levels(value: Any, *, reverse: bool) -> tuple[Level, ...]:
    if isinstance(value, Mapping):
        value = [[price, size] for price, size in value.items()]
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    result: list[Level] = []
    for item in value:
        try:
            if isinstance(item, Mapping):
                price = float(item["price"])
                size = float(_value(item, "size", "quantity", "qty"))
            else:
                price, size = float(item[0]), float(item[1])
            if math.isfinite(price) and math.isfinite(size) and 0.0 < price < 1.0 and size > 0.0:
                result.append(Level(price, size))
        except (KeyError, TypeError, ValueError, IndexError):
            continue
    return tuple(sorted(result, key=lambda level: level.price, reverse=reverse))


def _source_timestamp(payload: Mapping[str, Any], *keys: str) -> datetime | None:
    raw_ts = _value(payload, *keys)
    if raw_ts is None:
        return None
    try:
        return _timestamp(raw_ts)
    except (ValueError, OSError, OverflowError, FeedError):
        return None


def _ws_book(venue: str, outcome: str, token: str, bids: Any, asks: Any,
             received: datetime, source_at: datetime | None) -> Book:
    return Book(
        venue, outcome, token,
        _levels(bids, reverse=True), _levels(asks, reverse=False),
        received, source_at, received, None,
    )


def _complement_binary_book(up_book: Book, down_token: str) -> Book:
    """Derive the complementary DOWN book from a binary market's UP book."""
    down_bids = tuple(sorted(
        (Level(1.0 - level.price, level.size) for level in up_book.asks),
        key=lambda level: level.price,
        reverse=True,
    ))
    down_asks = tuple(sorted(
        (Level(1.0 - level.price, level.size) for level in up_book.bids),
        key=lambda level: level.price,
    ))
    return Book(
        "binance", "DOWN", down_token, down_bids, down_asks,
        up_book.received_at, up_book.source_at,
        up_book.request_started_at, up_book.request_rtt_ms,
    )


class PolymarketFeed:
    """Read-only Gamma market discovery; CLOB books arrive from its market WebSocket."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self.client = client

    async def markets(self, now: datetime) -> tuple[Market, ...]:
        epoch = int(now.timestamp())
        base = epoch - epoch % 300
        markets: list[Market] = []
        for start in (base - 300, base, base + 300, base + 600):
            slug = f"btc-updown-5m-{start}"
            response = await self.client.get(f"{GAMMA_URL}/markets", params={"slug": slug})
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                continue
            for row in payload:
                if not isinstance(row, Mapping):
                    continue
                try:
                    market = _parse_poly_market(row)
                    tradable = row.get("active") is not False and row.get("acceptingOrders") is not False
                    if tradable and row.get("closed") is not True:
                        markets.append(market)
                except (FeedError, ValueError, TypeError):
                    continue
        return tuple(markets)

class BinancePredictFeed:
    """Read-only signed Binance Wallet Predict metadata and order-book WebSocket."""

    MARKET_LIST = "/sapi/v1/w3w/wallet/prediction/market/list"
    MARKET_DETAIL = "/sapi/v1/w3w/wallet/prediction/market/detail"
    def __init__(self, client: httpx.AsyncClient, api_key: str, api_secret: str) -> None:
        self.client = client
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        self.time_offset_ms = 0

    def _signed_params(self, params: Mapping[str, Any]) -> dict[str, Any]:
        if not self.api_key or not self.api_secret:
            raise FeedError("Set BINANCE_API_KEY and BINANCE_API_SECRET for Binance Predict market data")
        values = dict(params)
        values.setdefault("timestamp", int(time.time() * 1000) + self.time_offset_ms)
        values.setdefault("recvWindow", 5000)
        query = urlencode(values)
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        values["signature"] = signature
        return values

    async def _get(self, path: str, params: Mapping[str, Any], *, signed: bool = True) -> Mapping[str, Any]:
        headers = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}
        request_params = self._signed_params(params) if signed else params
        response = await self.client.get(
            f"{BINANCE_URL}{path}", params=request_params, headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, Mapping):
            raise FeedError(f"Binance Predict returned a non-object for {path}")
        if "code" in payload:
            try:
                code = int(payload["code"])
            except (ValueError, TypeError):
                code = payload["code"]
            if code != 0:
                raise FeedError(f"Binance Predict API error {code}: {payload.get('msg', '')}")
        return payload

    async def sync_clock(self) -> None:
        started = time.time_ns()
        payload = await self._get("/api/v3/time", {}, signed=False)
        finished = time.time_ns()
        server_ms = int(payload["serverTime"])
        self.time_offset_ms = server_ms - ((started + finished) // 2 // 1_000_000)

    async def markets(self, now: datetime) -> tuple[Market, ...]:
        payload = await self._get(
            self.MARKET_LIST,
            {"l1Category": "crypto", "l2Category": "up-down", "sortBy": "END_DATE",
             "orderBy": "ASC", "offset": 0, "limit": 100},
        )
        rows = payload.get("marketTopics", ())
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            raise FeedError("Binance market list is missing marketTopics")
        selected: list[Mapping[str, Any]] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            symbol = str(row.get("symbol", "BTCUSDT")).upper()
            title = str(_value(row, "title", "slug", default="")).upper()
            if symbol == "BTCUSDT" or "BTC" in title:
                selected.append(row)

        async def resolve(row: Mapping[str, Any]) -> Market | None:
            candidate: Mapping[str, Any] = row
            try:
                market = _parse_binance_market(candidate)
            except (FeedError, ValueError, TypeError, KeyError):
                topic_id = _value(row, "marketTopicId", "market_topic_id")
                if topic_id is None:
                    return None
                detail = await self._get(self.MARKET_DETAIL, {"marketTopicId": str(topic_id)})
                nested = detail.get("data")
                if isinstance(nested, Mapping):
                    detail = nested
                candidate = {**row, **detail}
                try:
                    market = _parse_binance_market(candidate)
                except (FeedError, ValueError, TypeError, KeyError):
                    return None
            if market.end_at <= now - timedelta(seconds=1) or market.start_at > now + timedelta(seconds=900):
                return None
            if str(row.get("status", "")).upper() in {"RESOLVED", "CLOSED", "SETTLED", "REMOVED"}:
                return None
            return market

        resolved = await asyncio.gather(*(resolve(row) for row in selected))
        return tuple(sorted((market for market in resolved if market is not None), key=lambda market: market.start_at))

    def websocket_url(self, market_ids: Sequence[str]) -> str:
        if not self.api_key or not self.api_secret:
            raise FeedError("Set BINANCE_API_KEY and BINANCE_API_SECRET for Binance Predict WebSocket")
        topics = "|".join(f"web3_prediction_orderbook_{market_id}" for market_id in market_ids)
        params = {
            "random": secrets.token_hex(16),
            "topic": topics,
            "recvWindow": 30000,
            "timestamp": int(time.time() * 1000) + self.time_offset_ms,
        }
        query = urlencode(sorted(params.items()))
        signature = hmac.new(self.api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        return f"{BINANCE_WS_URL}?{query}&signature={signature}"


class PairMarketFeed:
    """Refresh market metadata over REST and stream paired L2 books over WebSockets."""

    def __init__(self, settings: Any, *,
                 subscription_recorder: Callable[[str, str, dict[str, Any]], None] | None = None) -> None:
        timeout = httpx.Timeout(8.0, connect=5.0)
        self.client = httpx.AsyncClient(timeout=timeout)
        self.settings = settings
        self.subscription_recorder = subscription_recorder
        self.poly = PolymarketFeed(self.client)
        self.binance = BinancePredictFeed(
            self.client, os.getenv("BINANCE_API_KEY", ""), os.getenv("BINANCE_API_SECRET", "")
        )
        self._markets_loaded_at = 0.0
        self._poly_markets: tuple[Market, ...] = ()
        self._binance_markets: tuple[Market, ...] = ()
        self._closed = False
        self._books: dict[tuple[str, str], Book] = {}
        self._book_history: dict[tuple[str, str], list[Book]] = {}
        self._poly_state: dict[str, dict[str, Any]] = {}
        self._binance_update_ms: dict[str, int] = {}
        self._binance_id_outcomes: dict[str, str] = {}
        self._binance_shared_market_id = False
        self._book_updated = asyncio.Event()
        self._stream_key: tuple[str, ...] | None = None
        self._stream_tasks: tuple[asyncio.Task[None], ...] = ()
        self._active_pair: tuple[Market, Market, float] | None = None

    def _record_subscription(self, venue: str, market: Market,
                             details: dict[str, Any]) -> None:
        if self.subscription_recorder is not None:
            self.subscription_recorder(venue, market.key, details)

    async def start(self) -> None:
        if not self.binance.api_key or not self.binance.api_secret:
            raise FeedError("Copy .env.example to .env and set BINANCE_API_KEY and BINANCE_API_SECRET first")
        await self.binance.sync_clock()

    async def close(self) -> None:
        self._closed = True
        for task in self._stream_tasks:
            task.cancel()
        if self._stream_tasks:
            await asyncio.gather(*self._stream_tasks, return_exceptions=True)
        self._stream_tasks = ()
        await self.client.aclose()

    async def capture(self, timeout_seconds: float | None = None) -> PairSnapshot | None:
        deadline = None if timeout_seconds is None else time.monotonic() + max(0.0, timeout_seconds)
        while not self._closed:
            if deadline is not None and time.monotonic() >= deadline:
                return None
            now = utc_now()
            if (time.monotonic() - self._markets_loaded_at >= 10.0
                    or not self._poly_markets or not self._binance_markets):
                self._poly_markets, self._binance_markets = await asyncio.gather(
                    self.poly.markets(now), self.binance.markets(now)
                )
                self._markets_loaded_at = time.monotonic()
            pair = self._select_pair(now)
            if pair is None:
                raise FeedError(
                    f"No active BTC 5m pair matched within {self.settings.match_tolerance_seconds:g}s "
                    f"(Polymarket markets={len(self._poly_markets)}, Binance markets={len(self._binance_markets)})"
                )
            await self._ensure_streams(*pair)

            if self._book_updated.is_set():
                # Coalesce bursts from the two venues while preserving event-driven processing.
                coalesce_for = 0.1 if deadline is None else min(0.1, max(0.0, deadline - time.monotonic()))
                if coalesce_for <= 0.0:
                    return None
                await asyncio.sleep(coalesce_for)
                if deadline is not None and time.monotonic() >= deadline:
                    return None
                self._book_updated.clear()
                snapshot = self._snapshot_if_ready()
                if snapshot is not None:
                    return snapshot
                continue

            until_refresh = max(0.05, 10.0 - (time.monotonic() - self._markets_loaded_at))
            until_deadline = float("inf") if deadline is None else max(0.0, deadline - time.monotonic())
            if until_deadline <= 0.0:
                return None
            try:
                await asyncio.wait_for(self._book_updated.wait(), timeout=min(1.0, until_refresh, until_deadline))
            except TimeoutError:
                stale = [
                    f"{venue}/{outcome}={book.age_seconds(utc_now()):.2f}s"
                    for (venue, outcome), book in self._books.items()
                    if book.age_seconds(utc_now()) > self.settings.max_quote_age_seconds
                ]
                if stale:
                    raise FeedError("WebSocket order book stale: " + ", ".join(stale))
                if deadline is not None and time.monotonic() >= deadline:
                    return None
        raise FeedError("Feed is closed")

    def _select_pair(self, now: datetime) -> tuple[Market, Market, float] | None:
        pairs: list[tuple[float, Market, Market]] = []
        for poly in self._poly_markets:
            for binance in self._binance_markets:
                skew = abs((poly.start_at - binance.start_at).total_seconds())
                overlap = min(poly.end_at, binance.end_at) > max(poly.start_at, binance.start_at)
                active = poly.start_at <= now < poly.end_at and binance.start_at <= now < binance.end_at
                if skew <= self.settings.match_tolerance_seconds and overlap and active:
                    pairs.append((skew, poly, binance))
        if not pairs:
            return None
        skew, poly, binance = min(pairs, key=lambda item: item[0])
        return poly, binance, skew

    async def _ensure_streams(self, poly_market: Market, binance_market: Market, skew: float) -> None:
        key = (
            poly_market.up_token, poly_market.down_token,
            binance_market.up_token, binance_market.down_token,
            binance_market.up_market_id or "", binance_market.down_market_id or "",
        )
        if key == self._stream_key:
            self._active_pair = (poly_market, binance_market, skew)
            return
        for task in self._stream_tasks:
            task.cancel()
        if self._stream_tasks:
            await asyncio.gather(*self._stream_tasks, return_exceptions=True)
        self._books.clear()
        self._book_history.clear()
        self._poly_state.clear()
        self._binance_update_ms.clear()
        self._book_updated.clear()
        up_id, down_id = binance_market.up_market_id, binance_market.down_market_id
        if not up_id or not down_id:
            raise FeedError("Binance market metadata is missing the outcome marketId needed for WebSocket topics")
        if str(up_id) == str(down_id):
            if binance_market.up_token == binance_market.down_token:
                raise FeedError("Binance binary market exposes the same token ID for UP and DOWN")
            # Current Binance metadata provides one binary marketId with UP and DOWN
            # outcome tokens. Its orderbook stream identifies only that marketId;
            # the stream's single book tracks UP, so derive DOWN at complementary prices.
            self._binance_id_outcomes = {str(up_id): "UP"}
            self._binance_shared_market_id = True
        else:
            self._binance_id_outcomes = {str(up_id): "UP", str(down_id): "DOWN"}
            self._binance_shared_market_id = False
        self._stream_key = key
        self._active_pair = (poly_market, binance_market, skew)
        self._stream_tasks = (
            asyncio.create_task(self._run_polymarket(poly_market), name="polymarket-orderbook-ws"),
            asyncio.create_task(self._run_binance(binance_market), name="binance-predict-orderbook-ws"),
        )

    def _snapshot_if_ready(self) -> PairSnapshot | None:
        if self._active_pair is None:
            return None
        poly_market, binance_market, skew = self._active_pair
        keys = (("polymarket", "UP"), ("polymarket", "DOWN"), ("binance", "UP"), ("binance", "DOWN"))
        if any(key not in self._books for key in keys):
            return None
        return PairSnapshot(
            poly_market.key, poly_market, binance_market,
            {key: self._books[key] for key in keys}, utc_now(), skew,
        )

    def snapshot_at(self, at_epoch: float) -> PairSnapshot | None:
        """Return the newest in-memory order books received by this instant."""
        if self._active_pair is None:
            return None
        poly_market, binance_market, skew = self._active_pair
        keys = (("polymarket", "UP"), ("polymarket", "DOWN"),
                ("binance", "UP"), ("binance", "DOWN"))
        books: dict[tuple[str, str], Book] = {}
        for key in keys:
            eligible = [book for book in self._book_history.get(key, ())
                        if book.received_at.timestamp() <= at_epoch + 1e-9]
            if not eligible:
                return None
            books[key] = eligible[-1]
        return PairSnapshot(
            poly_market.key, poly_market, binance_market, books,
            datetime.fromtimestamp(at_epoch, tz=timezone.utc), skew,
        )

    def _remember_book(self, key: tuple[str, str], book: Book) -> None:
        history = self._book_history.setdefault(key, [])
        history.append(book)
        cutoff = book.received_at.timestamp() - 30.0
        while len(history) > 1 and history[0].received_at.timestamp() < cutoff:
            history.pop(0)

    def _clear_venue_books(self, venue: str) -> None:
        for key in tuple(self._books):
            if key[0] == venue:
                del self._books[key]
        for key in tuple(self._book_history):
            if key[0] == venue:
                del self._book_history[key]
        if venue == "polymarket":
            self._poly_state.clear()
        else:
            self._binance_update_ms.clear()
        self._book_updated.set()

    async def _run_polymarket(self, market: Market) -> None:
        delay = 1.0
        token_outcomes = {market.up_token: "UP", market.down_token: "DOWN"}
        while not self._closed:
            self._clear_venue_books("polymarket")
            connected_at = time.monotonic()
            try:
                async with websockets.connect(
                    POLYMARKET_WS_URL, ping_interval=None, open_timeout=10, close_timeout=3,
                ) as ws:
                    await ws.send(json.dumps({
                        "type": "market", "assets_ids": [market.up_token, market.down_token],
                        "custom_feature_enabled": True,
                    }))
                    self._record_subscription("polymarket", market, {
                        "status": "subscribed", "token_ids": [market.up_token, market.down_token],
                    })
                    heartbeat = asyncio.create_task(self._polymarket_heartbeat(ws))
                    try:
                        async for raw in ws:
                            if isinstance(raw, str) and raw.upper() in {"PING", "PONG"}:
                                if raw.upper() == "PING":
                                    await ws.send("PONG")
                                continue
                            self._handle_polymarket_message(raw, token_outcomes)
                    finally:
                        heartbeat.cancel()
                        await asyncio.gather(heartbeat, return_exceptions=True)
                raise ConnectionError("Polymarket market WebSocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._clear_venue_books("polymarket")
                _LOGGER.warning("Polymarket orderbook WebSocket disconnected (%s); reconnecting in %.1fs",
                                type(exc).__name__, delay)
                await asyncio.sleep(delay + random.uniform(0.0, min(1.0, delay * 0.25)))
                delay = 1.0 if time.monotonic() - connected_at >= 30.0 else min(delay * 2.0, 30.0)

    async def _polymarket_heartbeat(self, ws: Any) -> None:
        while True:
            await asyncio.sleep(10.0)
            await ws.send("PING")

    def _handle_polymarket_message(self, raw: Any, token_outcomes: Mapping[str, str]) -> None:
        try:
            decoded = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise FeedError("Polymarket sent malformed WebSocket JSON") from exc
        messages = decoded if isinstance(decoded, list) else [decoded]
        for message in messages:
            if not isinstance(message, Mapping):
                continue
            event_type = str(_value(message, "event_type", "eventType", default="")).lower()
            if event_type == "book":
                token = str(_value(message, "asset_id", "assetId", "token_id", default=""))
                outcome = token_outcomes.get(token)
                if outcome is None:
                    continue
                source_at = _source_timestamp(message, "timestamp", "updateTimestampMs")
                previous = self._poly_state.get(token)
                if previous and source_at and previous.get("last_source_at") and source_at < previous["last_source_at"]:
                    continue
                received = utc_now()
                bids = {level.price: level.size for level in _levels(message.get("bids"), reverse=True)}
                asks = {level.price: level.size for level in _levels(message.get("asks"), reverse=False)}
                self._poly_state[token] = {
                    "bids": bids, "asks": asks, "source_at": source_at, "last_source_at": source_at,
                }
                self._publish_ws_book("polymarket", outcome, token, bids, asks, received, source_at)
            elif event_type == "price_change":
                changes = _value(message, "price_changes", "priceChanges", default=())
                if not isinstance(changes, Sequence) or isinstance(changes, (str, bytes)):
                    continue
                event_source_at = _source_timestamp(message, "timestamp", "updateTimestampMs")
                received = utc_now()
                touched: set[str] = set()
                for change in changes:
                    if not isinstance(change, Mapping):
                        continue
                    token = str(_value(change, "asset_id", "assetId", "token_id", default=""))
                    outcome = token_outcomes.get(token)
                    state = self._poly_state.get(token)
                    if outcome is None or state is None:
                        continue  # wait for a full book seed after connect
                    source_at = _source_timestamp(change, "timestamp", "updateTimestampMs") or event_source_at
                    if source_at and state.get("last_source_at") and source_at < state["last_source_at"]:
                        continue
                    try:
                        price, size = float(change["price"]), float(change["size"])
                    except (KeyError, TypeError, ValueError):
                        continue
                    if not math.isfinite(price) or not math.isfinite(size) or not 0.0 < price < 1.0 or size < 0.0:
                        continue
                    side = str(change.get("side", "")).upper()
                    levels = state["bids"] if side == "BUY" else state["asks"] if side == "SELL" else None
                    if levels is None:
                        continue
                    if size == 0.0:
                        levels.pop(price, None)
                    else:
                        levels[price] = size
                    state["source_at"] = source_at
                    if source_at is not None:
                        state["last_source_at"] = source_at
                    touched.add(token)
                for token in touched:
                    state = self._poly_state[token]
                    self._publish_ws_book(
                        "polymarket", token_outcomes[token], token, state["bids"], state["asks"],
                        received, state.get("source_at"),
                    )

    def _handle_binance_message(self, raw: Any) -> None:
        try:
            envelope = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            raise FeedError("Binance sent malformed WebSocket JSON") from exc
        messages = envelope if isinstance(envelope, list) else [envelope]
        for message in messages:
            if not isinstance(message, Mapping):
                continue
            payload: Any = message.get("data", message)
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError as exc:
                    if str(message.get("type", "")).upper() == "COMMAND":
                        try:
                            ack_code = int(message.get("code"))
                        except (TypeError, ValueError):
                            ack_code = None
                        if ack_code == 0:
                            continue
                        if ack_code is not None:
                            raise FeedError(
                                f"Binance orderbook subscription failed with code {ack_code}"
                            ) from exc
                    raise FeedError("Binance orderbook envelope contains malformed data JSON") from exc
            updates = payload if isinstance(payload, list) else [payload]
            for update in updates:
                if not isinstance(update, Mapping) or update.get("msgType") != "orderbook":
                    continue
                market_id = str(_value(update, "marketId", "market_id", default=""))
                outcome = self._binance_id_outcomes.get(market_id)
                if outcome is None:
                    continue
                raw_timestamp = _value(update, "updateTimestampMs", "timestamp")
                try:
                    update_ms = int(raw_timestamp)
                except (ValueError, TypeError):
                    continue
                if update_ms < self._binance_update_ms.get(outcome, -1):
                    continue
                self._binance_update_ms[outcome] = update_ms
                token = self._active_pair[1].up_token if outcome == "UP" else self._active_pair[1].down_token
                received = utc_now()
                source_at = _source_timestamp(update, "updateTimestampMs", "timestamp")
                self._publish_ws_book(
                    "binance", outcome, token, update.get("bids"), update.get("asks"), received, source_at,
                )
                if self._binance_shared_market_id and outcome == "UP":
                    up_book = self._books[("binance", "UP")]
                    down_token = self._active_pair[1].down_token
                    down_book = _complement_binary_book(up_book, down_token)
                    self._books[("binance", "DOWN")] = down_book
                    self._remember_book(("binance", "DOWN"), down_book)
                    self._book_updated.set()

    def _publish_ws_book(self, venue: str, outcome: str, token: str, bids: Any, asks: Any,
                         received: datetime, source_at: datetime | None) -> None:
        key = (venue, outcome)
        book = _ws_book(venue, outcome, token, bids, asks, received, source_at)
        self._books[key] = book
        self._remember_book(key, book)
        self._book_updated.set()

    async def _run_binance(self, market: Market) -> None:
        assert market.up_market_id is not None and market.down_market_id is not None
        market_ids = tuple(dict.fromkeys((str(market.up_market_id), str(market.down_market_id))))
        delay = 1.0
        while not self._closed:
            self._clear_venue_books("binance")
            connected_at = time.monotonic()
            try:
                url = self.binance.websocket_url(market_ids)
                async with websockets.connect(
                    url, additional_headers={"X-MBX-APIKEY": self.binance.api_key},
                    ping_interval=30.0, ping_timeout=15.0, open_timeout=10, close_timeout=3,
                ) as ws:
                    self._record_subscription("binance", market, {
                        "status": "subscribed", "market_ids": list(market_ids),
                    })
                    async for raw in ws:
                        self._handle_binance_message(raw)
                raise ConnectionError("Binance Predict orderbook WebSocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._clear_venue_books("binance")
                _LOGGER.warning("Binance Predict orderbook WebSocket disconnected (%s); reconnecting in %.1fs",
                                type(exc).__name__, delay)
                await asyncio.sleep(delay + random.uniform(0.0, min(1.0, delay * 0.25)))
                delay = 1.0 if time.monotonic() - connected_at >= 30.0 else min(delay * 2.0, 30.0)
