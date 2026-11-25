from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import time
from collections import OrderedDict
from contextvars import ContextVar, Token
from dataclasses import replace
from decimal import Decimal, InvalidOperation, ROUND_UP
from typing import Any, Callable

from .credstore_bridge import fetch_private_key
from .models import Fill, Market

logger = logging.getLogger(__name__)

_ACTIVE_ORDER_TRACE: ContextVar[dict[str, Any] | None] = ContextVar(
    "active_polymarket_order_trace", default=None
)
_ORDER_HTTP_ATTEMPT_EXTENSION = "paper_btc_live_order_http_attempt"
_METADATA_HTTP_ATTEMPT_EXTENSION = "paper_btc_live_metadata_http_attempt"
_MARKET_BOOK_HTTP_ATTEMPT_EXTENSION = "paper_btc_live_market_book_http_attempt"
_HTTPCORE_TRACE_EVENT_PREFIXES = (
    "connection.connect_tcp.",
    "connection.start_tls.",
    "http2.send_connection_init.",
    "http2.send_request_headers.",
    "http2.send_request_body.",
    "http2.receive_response_headers.",
    "http11.send_request_headers.",
    "http11.send_request_body.",
    "http11.receive_response_headers.",
)


def _market_buy_amount_for_shares(shares: str | Decimal, max_price: str | Decimal) -> Decimal:
    """Return a cent-rounded-up USDC budget for the requested shares at the cap."""
    amount = Decimal(str(shares)) * Decimal(str(max_price))
    return amount.quantize(Decimal("0.01"), rounding=ROUND_UP)


class LiveOrderUnknown(RuntimeError):
    """The request may have reached Polymarket, but its fill is not known."""

    def __init__(self, message: str, *, diagnostic_fields: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.diagnostic_fields = dict(diagnostic_fields or {})


class LiveFakNoFill(RuntimeError):
    """A FAK received an explicit rejection stating that it matched nothing."""

    def __init__(self, message: str, *, diagnostic_fields: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.diagnostic_fields = dict(diagnostic_fields or {})


class LiveBalanceAllowanceRejected(LiveFakNoFill):
    """A sell was definitively rejected because shares or allowance were unavailable."""


_SENSITIVE_HEX_RE = re.compile(r"(?i)\b0x[a-f0-9]{40,64}\b")
_FAK_NO_FILL_MESSAGE = (
    "no orders found to match with fak order. "
    "fak orders are partially filled or killed if no match is found."
)
_FAK_NO_FILL_MESSAGE_PREFIX = "no orders found to match with fak order"
_FAK_NO_FILL_CODES = frozenset({"fak_not_filled", "no_matching_orders"})


def _safe_diagnostic_text(value: Any, *, limit: int = 500) -> str:
    text = _SENSITIVE_HEX_RE.sub("0x[redacted]", str(value))
    return text[:limit]


def _request_rejection_diagnostics(exc: Exception) -> dict[str, Any]:
    fields: dict[str, Any] = {"sdk_error_type": type(exc).__name__}
    if not hasattr(exc, "status"):
        return fields
    for name in ("status", "code", "retry_after", "restriction"):
        if hasattr(exc, name):
            fields[f"sdk_{name}"] = getattr(exc, name)
    fields["sdk_error_message"] = _safe_diagnostic_text(exc)
    return fields


def _is_explicit_fak_no_fill(exc: Exception) -> bool:
    if getattr(exc, "status", None) != 400:
        return False
    if str(getattr(exc, "code", "") or "").strip().casefold() in _FAK_NO_FILL_CODES:
        return True
    message = " ".join(str(exc).strip().casefold().split())
    return message == _FAK_NO_FILL_MESSAGE or message.startswith(_FAK_NO_FILL_MESSAGE_PREFIX)


def _is_explicit_fak_no_fill_response(response: Any) -> bool:
    code = str(_field(response, "code", default="") or "").strip().casefold()
    if code in _FAK_NO_FILL_CODES:
        return True
    message = " ".join(
        str(_field(response, "message", "error", default="") or "").strip().casefold().split()
    )
    return message == _FAK_NO_FILL_MESSAGE or message.startswith(_FAK_NO_FILL_MESSAGE_PREFIX)


_BALANCE_ALLOWANCE_REJECTION_PHRASES = (
    "not enough balance / allowance",
    "balance is not enough",
    "allowance is not enough",
    "insufficient balance",
    "insufficient allowance",
    "not enough balance",
    "not enough allowance",
)


def _contains_balance_allowance_rejection(value: Any) -> bool:
    message = " ".join(str(value or "").strip().casefold().split())
    return any(phrase in message for phrase in _BALANCE_ALLOWANCE_REJECTION_PHRASES)


def _is_explicit_balance_allowance_rejection(exc: Exception) -> bool:
    return getattr(exc, "status", None) == 400 and _contains_balance_allowance_rejection(exc)


def _is_explicit_balance_allowance_rejection_response(response: Any) -> bool:
    message = _field(response, "message", "error", "error_msg", "errorMsg", default="")
    return _contains_balance_allowance_rejection(message)


def _field(value: Any, *names: str, default: Any = None) -> Any:
    for name in names:
        if isinstance(value, dict) and name in value:
            return value[name]
        candidate = getattr(value, name, None)
        if candidate is not None:
            return candidate
    return default


def _signed_order_diagnostics(signed_order: Any, side: str) -> dict[str, Any]:
    maker_amount = _field(signed_order, "maker_amount", "makerAmount")
    taker_amount = _field(signed_order, "taker_amount", "takerAmount")
    share_amount = maker_amount if side.upper() == "SELL" else taker_amount
    fields: dict[str, Any] = {
        "sdk_signed_maker_amount": str(maker_amount) if maker_amount is not None else None,
        "sdk_signed_taker_amount": str(taker_amount) if taker_amount is not None else None,
        "sdk_order_price_source": "signed_order_amount_ratio",
    }
    try:
        maker = Decimal(str(maker_amount))
        taker = Decimal(str(taker_amount))
        shares = Decimal(str(share_amount)) / Decimal("1000000")
        if maker <= 0 or taker <= 0:
            raise InvalidOperation
        # SELL offers outcome shares (maker) for collateral (taker); BUY does the reverse.
        price = taker / maker if side.upper() == "SELL" else maker / taker
        if not price.is_finite() or price <= 0:
            raise InvalidOperation
    except (InvalidOperation, TypeError, ValueError, ZeroDivisionError):
        fields["sdk_order_price"] = None
        fields["sdk_signed_share_qty"] = None
    else:
        fields["sdk_order_price"] = float(price)
        fields["sdk_signed_share_qty"] = format(shares, "f")
    return fields


class PolymarketLiveExecutor:
    """Authenticated Polymarket order execution through the unified Python SDK."""

    _HTTP_KEEPALIVE_INTERVAL_SECONDS = 20.0
    _HTTP_KEEPALIVE_WARNING_INTERVAL_SECONDS = 300.0
    _MAX_PREPARED_BUY_ORDERS_PER_MARKET = 256
    _PREPARED_BUY_ORDER_YIELD_EVERY = 1
    _USER_EVENT_CACHE_LIMIT = 512
    _USER_TRADE_CACHE_PER_ORDER = 64
    _USER_STREAM_RECONNECT_MAX_SECONDS = 30.0
    _GTC_REST_CALL_TIMEOUT_SECONDS = 4.0

    def __init__(self, fee_rate: float) -> None:
        self.fee_rate = float(fee_rate)
        self.client: Any | None = None
        self._client_context: Any | None = None
        self._verified_markets: dict[str, tuple[set[str], set[str], Decimal]] = {}
        self._timing_hook_clients: set[int] = set()
        self._http_keepalive_task: asyncio.Task[Any] | None = None
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._last_keepalive_warning_monotonic = 0.0
        self._prepared_buy_orders: dict[
            tuple[str, str, str], tuple[Any, int, tuple[str, str, str, str]]
        ] = {}
        self._prepared_buy_pool_specs: dict[
            tuple[str, str, str, str], tuple[Market, str, Decimal, int]
        ] = {}
        self._prepared_buy_pool_ready: set[tuple[str, str, str, str]] = set()
        self._prepared_buy_pool_preparing: set[tuple[str, str, str, str]] = set()
        self._prepared_buy_refills_inflight: set[tuple[str, str, str]] = set()
        self._prepared_sell_orders: dict[
            tuple[str, str, str, str], tuple[Any, int]
        ] = {}
        self._user_stream_task: asyncio.Task[Any] | None = None
        self._user_stream_connected = False
        self._user_state_changed = asyncio.Event()
        self._user_order_states: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._user_trade_rows: OrderedDict[str, OrderedDict[str, dict[str, Any]]] = OrderedDict()
        self._tracked_gtc_orders: OrderedDict[str, dict[str, Any]] = OrderedDict()

    async def connect(self) -> None:
        from polymarket import AsyncSecureClient, RelayerApiKey

        try:
            private_key = fetch_private_key()
            logger.info("LIVE_SIGNER_KEY_RESOLVED source=credstore")
        except Exception as exc:
            # Match the existing trader: prefer credstore, with .env as an
            # optional local fallback. Never log credential payloads.
            private_key = os.environ.get("POLYMARKET_PRIVATE_KEY", "").strip()
            if private_key:
                logger.warning(
                    "LIVE_SIGNER_KEY_CREDSTORE_FALLBACK error_type=%s",
                    type(exc).__name__,
                )
        wallet = os.environ.get("POLYMARKET_WALLET_ADDRESS", "").strip() or None
        relayer_key = os.environ.get("POLYMARKET_RELAYER_API_KEY", "").strip()
        relayer_address = os.environ.get("POLYMARKET_RELAYER_API_KEY_ADDRESS", "").strip()
        missing = [
            name
            for name, value in (
                ("POLYMARKET_PRIVATE_KEY", private_key),
                ("POLYMARKET_RELAYER_API_KEY", relayer_key),
                ("POLYMARKET_RELAYER_API_KEY_ADDRESS", relayer_address),
            )
            if not value
        ]
        if missing:
            raise ValueError("live mode requires credentials: " + ", ".join(missing))

        context = await AsyncSecureClient.create(
            private_key=private_key,
            wallet=wallet,
            api_key=RelayerApiKey(key=relayer_key, address=relayer_address),
        )
        self._client_context = context
        self.client = await context.__aenter__()
        self._install_http_timing_hooks()
        self._http_keepalive_task = asyncio.create_task(
            self._keep_order_http_connection_alive(),
            name="polymarket-order-http-keepalive",
        )
        connected_wallet = str(_field(self.client, "wallet", default=wallet or "unknown"))
        connected_type = str(_field(self.client, "wallet_type", default="unknown"))
        if wallet and connected_wallet.lower() != wallet.lower():
            await self.close()
            raise ValueError("connected Polymarket wallet does not match POLYMARKET_WALLET_ADDRESS")
        logger.info("LIVE_ACCOUNT_CONNECTED wallet=%s wallet_type=%s", connected_wallet, connected_type)
        self._user_stream_task = asyncio.create_task(
            self._run_user_stream(), name="polymarket-user-order-stream"
        )

    def _remember_user_order(self, order_id: str, payload: Any, source: str) -> None:
        if not order_id:
            return
        state = dict(self._user_order_states.get(order_id, {}))
        status = str(_field(payload, "status", default="") or "").lower()
        terminal = {"matched", "filled", "canceled", "cancelled", "expired"}
        old_status = str(state.get("status", "")).lower()
        if status and not (old_status in terminal and status not in terminal):
            state["status"] = status
        raw = _field(payload, "size_matched", "sizeMatched")
        if raw is not None:
            try:
                qty = Decimal(str(raw))
                previous = Decimal(str(state.get("size_matched", "-1")))
                if qty.is_finite() and qty >= 0:
                    state["size_matched"] = format(max(qty, previous), "f")
            except (InvalidOperation, TypeError, ValueError):
                pass
        state["source"] = source
        self._user_order_states[order_id] = state
        self._user_order_states.move_to_end(order_id)
        while len(self._user_order_states) > self._USER_EVENT_CACHE_LIMIT:
            self._user_order_states.popitem(last=False)
        self._user_state_changed.set()

    def _remember_user_trade(self, order_id: str, trade_id: str, size: Any,
                             price: Any, status: str, source: str) -> None:
        if not order_id or not trade_id:
            return
        try:
            qty, px = Decimal(str(size)), Decimal(str(price))
        except (InvalidOperation, TypeError, ValueError):
            return
        if not (qty.is_finite() and qty > 0 and px.is_finite() and px > 0):
            return
        trades = self._user_trade_rows.setdefault(order_id, OrderedDict())
        status = str(status or "").upper()
        previous = trades.get(trade_id, {})
        previous_status = str(previous.get("status", "")).upper()
        terminal_statuses = {"FAILED", "MINED", "CONFIRMED"}
        if previous_status in terminal_statuses and status not in terminal_statuses:
            status = previous_status
        trades[trade_id] = {"size": qty, "price": px,
                            "status": status, "source": source}
        trades.move_to_end(trade_id)
        while len(trades) > self._USER_TRADE_CACHE_PER_ORDER:
            trades.popitem(last=False)
        self._user_trade_rows.move_to_end(order_id)
        while len(self._user_trade_rows) > self._USER_EVENT_CACHE_LIMIT:
            self._user_trade_rows.popitem(last=False)
        self._user_state_changed.set()

    def _cached_user_trades(self, order_id: str) -> dict[str, tuple[str, Decimal, Decimal, str]]:
        rows = {}
        for trade_id, item in self._user_trade_rows.get(order_id, {}).items():
            status = str(item.get("status", "")).upper()
            if status == "FAILED":
                continue
            if item.get("source") == "REST" or status in {"MINED", "CONFIRMED"}:
                rows[trade_id] = (trade_id, item["size"], item["price"], status)
        return rows

    async def _record_user_event(self, event: Any) -> None:
        kind = str(_field(event, "event_type", "eventType", "type", default="") or "").lower()
        if kind == "order":
            oid = str(_field(event, "id", "order_id", "orderId", default="") or "")
            self._remember_user_order(oid, event, "USER_WS")
            return
        if kind != "trade":
            return
        tid = str(_field(event, "id", "trade_id", "tradeId", default="") or "")
        status = str(_field(event, "status", default="") or "").upper()
        size, price = _field(event, "size", "amount"), _field(event, "price")
        taker = str(_field(event, "taker_order_id", "takerOrderId", default="") or "")
        if taker:
            self._remember_user_trade(taker, tid, size, price, status, "USER_WS")
        for maker in (_field(event, "maker_orders", "makerOrders", default=()) or ()):
            oid = str(_field(maker, "order_id", "orderId", default="") or "")
            qty = _field(maker, "matched_amount", "matchedAmount", "size")
            if oid:
                self._remember_user_trade(oid, tid, qty, price, status, "USER_WS")

    async def _fetch_account_trade_rows(self, order_id: str, asset_id: str,
                                        condition_id: str) -> dict[str, tuple[str, Decimal, Decimal, str]]:
        if self.client is None:
            raise RuntimeError("Polymarket account is disconnected")
        paginator = self.client.list_account_trades(asset_id=asset_id, market=condition_id)
        rows = {}
        async for trade in paginator.iter_items():
            if str(_field(trade, "status", default="") or "").upper() == "FAILED":
                continue
            tid = str(_field(trade, "id", "trade_id", "tradeId", default="") or "")
            taker = str(_field(trade, "taker_order_id", "takerOrderId", default="") or "")
            qty = _field(trade, "size", "amount") if taker == order_id else None
            if qty is None:
                for maker in (_field(trade, "maker_orders", "makerOrders", default=()) or ()):
                    if str(_field(maker, "order_id", "orderId", default="") or "") == order_id:
                        qty = _field(maker, "matched_amount", "matchedAmount", "size")
                        break
            if qty is None:
                continue
            try:
                size, price = Decimal(str(qty)), Decimal(str(_field(trade, "price")))
            except (InvalidOperation, TypeError, ValueError):
                continue
            if not (size.is_finite() and size > 0 and price.is_finite() and price > 0):
                continue
            if not tid:
                continue
            rows[tid] = (tid, size, price, str(_field(trade, "status", default="") or "").upper())
        return rows

    async def _reconcile_user_stream_gap(self) -> None:
        if self.client is None:
            return
        try:
            async for order in self.client.list_open_orders().iter_items():
                oid = str(_field(order, "id", "order_id", "orderId", default="") or "")
                self._remember_user_order(oid, order, "REST_OPEN_ORDERS")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("LIVE_USER_WS_RECONNECT_OPEN_ORDERS_FAILED error_type=%s", type(exc).__name__)
        for oid, context in list(self._tracked_gtc_orders.items()):
            if self.client is None:
                return
            try:
                rows = await self._fetch_account_trade_rows(
                    oid, context["asset_id"], context["condition_id"]
                )
                for tid, qty, price, status in rows.values():
                    self._remember_user_trade(oid, tid, qty, price, status, "REST")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("LIVE_USER_WS_RECONNECT_TRADE_RECONCILIATION_FAILED error_type=%s", type(exc).__name__)

    async def _run_user_stream(self) -> None:
        from polymarket.streams import UserSpec
        retry = 1.0
        while self.client is not None:
            try:
                subscription = await self.client.subscribe(UserSpec())
                async with subscription as events:
                    self._user_stream_connected = True
                    retry = 1.0
                    logger.info("LIVE_USER_WS_CONNECTED markets=all")
                    await self._reconcile_user_stream_gap()
                    async for event in events:
                        try:
                            await self._record_user_event(event)
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            logger.warning("LIVE_USER_WS_EVENT_IGNORED error_type=%s", type(exc).__name__)
                if self.client is not None:
                    raise ConnectionError("Polymarket User WebSocket stream ended")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._user_stream_connected = False
                logger.warning("LIVE_USER_WS_DISCONNECTED error_type=%s retry_seconds=%.1f", type(exc).__name__, retry)
                await asyncio.sleep(retry)
                retry = min(self._USER_STREAM_RECONNECT_MAX_SECONDS, retry * 2.0)
            finally:
                self._user_stream_connected = False

    async def _wait_gtc_fill_or_timeout(self, order_id: str,
                                        requested_qty: float, seconds: float) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, float(seconds))
        while True:
            self._user_state_changed.clear()
            state = self._user_order_states.get(order_id, {})
            status = str(state.get("status", "")).lower()
            try:
                matched = Decimal(str(state.get("size_matched")))
            except (InvalidOperation, TypeError, ValueError):
                matched = Decimal("0")
            if status in {"matched", "filled"} or matched >= Decimal(str(requested_qty)) - Decimal("0.0001"):
                return
            remaining = deadline - loop.time()
            if remaining <= 0:
                return
            try:
                await asyncio.wait_for(self._user_state_changed.wait(), remaining)
            except asyncio.TimeoutError:
                return

    def _install_http_timing_hooks(self) -> None:
        """Attach lightweight hooks to the SDK's existing HTTP/2 clients."""
        if self.client is None:
            return
        context = getattr(self.client, "_ctx", None)
        if context is None:
            logger.warning("LIVE_ORDER_HTTP_TIMING_HOOKS_UNAVAILABLE reason=sdk_context_missing")
            return
        for service_name in ("clob", "secure_clob"):
            transport = getattr(context, service_name, None)
            http_client = getattr(transport, "_client", None)
            if http_client is None or id(http_client) in self._timing_hook_clients:
                continue
            hooks = getattr(http_client, "event_hooks", None)
            if not isinstance(hooks, dict):
                logger.warning(
                    "LIVE_ORDER_HTTP_TIMING_HOOKS_UNAVAILABLE service=%s reason=event_hooks_missing",
                    service_name,
                )
                continue

            async def on_request(request: Any, *, _service: str = service_name) -> None:
                self._capture_http_request(_service, request)

            async def on_response(response: Any, *, _service: str = service_name) -> None:
                self._capture_http_response(_service, response)

            hooks.setdefault("request", []).append(on_request)
            hooks.setdefault("response", []).append(on_response)
            self._timing_hook_clients.add(id(http_client))
        logger.info(
            "LIVE_ORDER_HTTP_TIMING_HOOKS_READY hooked_clients=%d",
            len(self._timing_hook_clients),
        )

    async def _keep_order_http_connection_alive(self) -> None:
        """Keep both SDK CLOB HTTP/2 transports warm for book reads and order posts."""
        while self.client is not None:
            context = getattr(self.client, "_ctx", None)
            for service_name in ("clob", "secure_clob"):
                try:
                    transport = getattr(context, service_name, None)
                    get_json = getattr(transport, "get_json", None)
                    if not callable(get_json):
                        raise RuntimeError(f"SDK {service_name} transport has no get_json method")
                    await get_json("/time")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    now = time.monotonic()
                    if (
                        now - self._last_keepalive_warning_monotonic
                        >= self._HTTP_KEEPALIVE_WARNING_INTERVAL_SECONDS
                    ):
                        logger.warning(
                            "LIVE_ORDER_HTTP_KEEPALIVE_FAILED service=%s error_type=%s",
                            service_name,
                            type(exc).__name__,
                        )
                        self._last_keepalive_warning_monotonic = now
            await asyncio.sleep(self._HTTP_KEEPALIVE_INTERVAL_SECONDS)

    @staticmethod
    def _prepared_buy_order_key(
        asset_id: str,
        size: str,
        price: Decimal | str,
    ) -> tuple[str, str, str]:
        return (str(asset_id), size, format(Decimal(str(price)), "f"))

    def _schedule_prepared_buy_order_refill(
        self,
        key: tuple[str, str, str],
        pool_key: tuple[str, str, str, str],
    ) -> None:
        if self.client is None or key in self._prepared_buy_refills_inflight:
            return
        spec = self._prepared_buy_pool_specs.get(pool_key)
        if spec is None:
            return
        market, size, _, _ = spec
        if market.end_ns <= time.time_ns():
            return
        self._prepared_buy_refills_inflight.add(key)
        task = asyncio.create_task(
            self._refill_prepared_buy_order(key, pool_key, market, size),
            name=f"polymarket-buy-order-refill-{market.slug}",
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _refill_prepared_buy_order(
        self,
        key: tuple[str, str, str],
        pool_key: tuple[str, str, str, str],
        market: Market,
        size: str,
    ) -> None:
        trace_token = _ACTIVE_ORDER_TRACE.set(None)
        try:
            # Let the just-submitted POST leave the event loop's hot path first.
            await asyncio.sleep(1.0)
            if self.client is None or market.end_ns <= time.time_ns():
                return
            signed_order = await self.client.create_limit_order(
                asset_id=key[0], side="BUY", price=key[2], size=size, post_only=False,
            )
            if self.client is not None and market.end_ns > time.time_ns():
                self._prepared_buy_orders.setdefault(key, (signed_order, market.end_ns, pool_key))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "LIVE_BUY_ORDER_PRESIGN_REFILL_FAILED market=%s error_type=%s",
                market.slug,
                type(exc).__name__,
            )
        finally:
            self._prepared_buy_refills_inflight.discard(key)
            _ACTIVE_ORDER_TRACE.reset(trace_token)

    async def prepare_sell_order_pool(
        self,
        market: Market,
        *,
        asset_id: str,
        shares: float,
        limit_price: Decimal | str,
        tick_size: Decimal | str,
    ) -> dict[str, Any]:
        """Pre-sign a one-use SELL FAK for this exact position size and floor price."""
        if self.client is None:
            raise RuntimeError("Polymarket account is not connected")
        self._prune_expired_prepared_sell_orders()
        size = f"{shares:.6f}"
        price = Decimal(str(limit_price))
        tick = Decimal(str(tick_size))
        if not price.is_finite() or not Decimal("0") < price < Decimal("1"):
            raise ValueError("SELL FAK limit price must be between 0 and 1")
        if not tick.is_finite() or tick <= 0 or tick >= 1 or price % tick != 0:
            raise ValueError("SELL FAK limit price must conform to the current tick size")
        verified = self._verified_markets.get(market.condition_id.lower())
        if verified is None or str(asset_id) not in verified[0] | verified[1]:
            raise ValueError("market condition and outcome asset must be account-verified before presigning")
        key = self._prepared_sell_order_key(asset_id, size, price, tick)
        if key in self._prepared_sell_orders:
            return {"status": "already_ready", "prepared_orders": 0, "tick_size": format(tick, "f")}
        if market.end_ns <= time.time_ns():
            return {"status": "market_expired", "prepared_orders": 0, "tick_size": format(tick, "f")}

        started_ns = time.perf_counter_ns()
        trace_token = _ACTIVE_ORDER_TRACE.set(None)
        try:
            signed_order = await self.client.create_limit_order(
                asset_id=str(asset_id), side="SELL", price=format(price, "f"),
                size=size, post_only=False,
            )
            self._prepared_sell_orders[key] = (
                replace(signed_order, order_type="FAK"), market.end_ns,
            )
            return {
                "status": "ready", "prepared_orders": 1,
                "tick_size": format(tick, "f"),
                "prepare_elapsed_ms": (time.perf_counter_ns() - started_ns) / 1_000_000.0,
            }
        finally:
            _ACTIVE_ORDER_TRACE.reset(trace_token)

    @staticmethod
    def _prepared_sell_order_key(
        asset_id: str, size: str, price: Decimal | str, tick_size: Decimal | str,
    ) -> tuple[str, str, str, str]:
        return (
            str(asset_id), size, format(Decimal(str(price)), "f"),
            format(Decimal(str(tick_size)), "f"),
        )

    def _take_prepared_sell_order(
        self, asset_id: str, size: str, price: Decimal, tick_size: Decimal | str | None,
    ) -> Any | None:
        if tick_size is None:
            return None
        key = self._prepared_sell_order_key(asset_id, size, price, tick_size)
        cached = self._prepared_sell_orders.pop(key, None)
        if cached is None:
            return None
        order, expires_ns = cached
        if expires_ns <= time.time_ns():
            return None
        return order

    def _prune_expired_prepared_sell_orders(self) -> None:
        now_ns = time.time_ns()
        for key, (_, expires_ns) in list(self._prepared_sell_orders.items()):
            if expires_ns <= now_ns:
                self._prepared_sell_orders.pop(key, None)

    async def prepare_buy_order_pool(
        self,
        market: Market,
        *,
        shares: float,
        tick_size: Decimal | str,
        offset_ticks: int = 3,
    ) -> dict[str, Any]:
        """Pre-sign BUY GTC limit orders for each possible ask-derived limit price."""
        if self.client is None:
            raise RuntimeError("Polymarket account is not connected")
        self._prune_expired_prepared_buy_orders()
        size = f"{shares:.6f}"
        tick = Decimal(str(tick_size))
        if not tick.is_finite() or tick <= 0 or tick >= 1:
            raise ValueError("market tick size must be between 0 and 1")
        if offset_ticks < 1:
            raise ValueError("offset_ticks must be at least one")
        pool_key = (
            market.condition_id.lower(), size, format(tick, "f"), str(offset_ticks)
        )
        if pool_key in self._prepared_buy_pool_ready:
            return {
                "status": "already_ready",
                "prepared_orders": 0,
                "tick_size": format(tick, "f"),
                "offset_ticks": offset_ticks,
            }
        if pool_key in self._prepared_buy_pool_preparing:
            return {
                "status": "already_preparing",
                "prepared_orders": 0,
                "tick_size": format(tick, "f"),
                "offset_ticks": offset_ticks,
            }

        verified = self._verified_markets.get(market.condition_id.lower())
        if (
            verified is None
            or str(market.up_asset_id) not in verified[0]
            or str(market.down_asset_id) not in verified[1]
        ):
            raise ValueError("market condition and outcome assets must be account-verified before presigning")
        self._prepared_buy_pool_preparing.add(pool_key)
        self._prepared_buy_pool_specs[pool_key] = (market, size, tick, offset_ticks)
        started_ns = time.perf_counter_ns()
        prepared_count = 0
        try:
            max_price_level = int((Decimal("1") - tick) // tick)
            if max_price_level <= offset_ticks:
                raise ValueError("tick size and offset leave no legal BUY limit price")
            max_price_levels = sorted({
                tick * (ask_level + offset_ticks)
                for ask_level in range(1, max_price_level - offset_ticks + 1)
            })
            candidate_count = len(max_price_levels) * 2
            if candidate_count > self._MAX_PREPARED_BUY_ORDERS_PER_MARKET:
                self._prepared_buy_pool_ready.add(pool_key)
                return {
                    "status": "skipped_candidate_limit",
                    "prepared_orders": 0,
                    "candidate_orders": candidate_count,
                    "candidate_limit": self._MAX_PREPARED_BUY_ORDERS_PER_MARKET,
                    "tick_size": format(tick, "f"),
                    "offset_ticks": offset_ticks,
                    "prepare_elapsed_ms": (time.perf_counter_ns() - started_ns) / 1_000_000.0,
                }

            assets = (str(market.up_asset_id), str(market.down_asset_id))
            for asset_id in assets:
                for price in max_price_levels:
                    if self.client is None or market.end_ns <= time.time_ns():
                        break
                    price_text = format(price, "f")
                    key = self._prepared_buy_order_key(asset_id, size, price_text)
                    if key in self._prepared_buy_orders:
                        continue
                    signed_order = await self.client.create_limit_order(
                        asset_id=asset_id, side="BUY", price=price_text, size=size, post_only=False,
                    )
                    self._prepared_buy_orders[key] = (signed_order, market.end_ns, pool_key)
                    prepared_count += 1
                    if prepared_count % self._PREPARED_BUY_ORDER_YIELD_EVERY == 0:
                        await asyncio.sleep(0)
            self._prepared_buy_pool_ready.add(pool_key)
            return {
                "status": "ready",
                "prepared_orders": prepared_count,
                "max_price_levels_per_asset": len(max_price_levels),
                "tick_size": format(tick, "f"),
                "offset_ticks": offset_ticks,
                "minimum_limit_price": format(min(max_price_levels), "f"),
                "maximum_limit_price": format(max_price_levels[-1], "f"),
                "prepare_elapsed_ms": (time.perf_counter_ns() - started_ns) / 1_000_000.0,
            }
        finally:
            self._prepared_buy_pool_preparing.discard(pool_key)

    def _prune_expired_prepared_buy_orders(self) -> None:
        now_ns = time.time_ns()
        for key, (_, expires_ns, _) in list(self._prepared_buy_orders.items()):
            if expires_ns <= now_ns:
                self._prepared_buy_orders.pop(key, None)
        expired_pools = {
            key
            for key, (market, _, _, _) in self._prepared_buy_pool_specs.items()
            if market.end_ns <= now_ns
        }
        for key in expired_pools:
            self._prepared_buy_pool_specs.pop(key, None)
            self._prepared_buy_pool_ready.discard(key)
        self._prune_expired_prepared_sell_orders()

    @staticmethod
    def _capture_http_request(service: str, request: Any) -> None:
        trace = _ACTIVE_ORDER_TRACE.get()
        if trace is None:
            return
        method = str(getattr(request, "method", "")).upper()
        path = str(getattr(getattr(request, "url", None), "path", ""))
        if method == "POST" and path.rstrip("/") == "/order" and trace.get("phase") == "order":
            attempt: dict[str, Any] = {
                "service": service,
                "request_started_ns": time.time_ns(),
                "_started_perf_ns": time.perf_counter_ns(),
            }
            trace.setdefault("post_http_attempts", []).append(attempt)
            request.extensions[_ORDER_HTTP_ATTEMPT_EXTENSION] = attempt
            try:
                event_times: dict[str, int] = {}
                attempt["_httpcore_event_times"] = event_times

                async def on_httpcore_trace(event_name: str, _info: Any) -> None:
                    try:
                        name = str(event_name)
                        if name.startswith(_HTTPCORE_TRACE_EVENT_PREFIXES):
                            event_times[name] = time.perf_counter_ns()
                    except Exception:
                        # Diagnostics must never interrupt order submission.
                        return

                if "trace" not in request.extensions:
                    request.extensions["trace"] = on_httpcore_trace
            except Exception:
                # Diagnostics must never interrupt order submission.
                pass
            PolymarketLiveExecutor._export_order_timing(trace)
            return

        if method == "GET" and path.rstrip("/") == "/book" and trace.get("phase") == "order":
            attempt = {
                "service": service,
                "path": path,
                "request_started_ns": time.time_ns(),
                "_started_perf_ns": time.perf_counter_ns(),
            }
            trace.setdefault("market_book_http_attempts", []).append(attempt)
            request.extensions[_MARKET_BOOK_HTTP_ATTEMPT_EXTENSION] = attempt
            PolymarketLiveExecutor._export_order_timing(trace)
            return

        if method == "GET" and (
            path.startswith("/markets-by-token/") or path.startswith("/clob-markets/")
        ):
            attempt = {
                "service": service,
                "path": path,
                "request_started_ns": time.time_ns(),
                "_started_perf_ns": time.perf_counter_ns(),
            }
            trace.setdefault("metadata_http_attempts", []).append(attempt)
            request.extensions[_METADATA_HTTP_ATTEMPT_EXTENSION] = attempt

    @staticmethod
    def _capture_http_response(service: str, response: Any) -> None:
        request = getattr(response, "request", None)
        if request is None:
            return
        trace = _ACTIVE_ORDER_TRACE.get()
        if trace is None:
            return
        now_perf_ns = time.perf_counter_ns()
        now_ns = time.time_ns()
        attempt = request.extensions.get(_ORDER_HTTP_ATTEMPT_EXTENSION)
        if isinstance(attempt, dict):
            attempt.update(
                {
                    "service": service,
                    "response_received_ns": now_ns,
                    "elapsed_ms": (now_perf_ns - attempt["_started_perf_ns"]) / 1_000_000.0,
                    "status_code": getattr(response, "status_code", None),
                    "http_version": getattr(response, "http_version", None),
                }
            )
            PolymarketLiveExecutor._attach_httpcore_timing(attempt)
            trace["post_response_perf_ns"] = now_perf_ns
            PolymarketLiveExecutor._export_order_timing(trace)
            return
        attempt = request.extensions.get(_MARKET_BOOK_HTTP_ATTEMPT_EXTENSION)
        if isinstance(attempt, dict):
            attempt.update(
                {
                    "service": service,
                    "path": str(getattr(getattr(request, "url", None), "path", "")),
                    "response_received_ns": now_ns,
                    "elapsed_ms": (now_perf_ns - attempt["_started_perf_ns"]) / 1_000_000.0,
                    "status_code": getattr(response, "status_code", None),
                    "http_version": getattr(response, "http_version", None),
                }
            )
            PolymarketLiveExecutor._export_order_timing(trace)
            return
        attempt = request.extensions.get(_METADATA_HTTP_ATTEMPT_EXTENSION)
        if isinstance(attempt, dict):
            attempt.update(
                {
                    "service": service,
                    "response_received_ns": now_ns,
                    "elapsed_ms": (now_perf_ns - attempt["_started_perf_ns"]) / 1_000_000.0,
                    "status_code": getattr(response, "status_code", None),
                }
            )
            PolymarketLiveExecutor._export_metadata_timing(trace)

    @staticmethod
    def _attach_httpcore_timing(attempt: dict[str, Any]) -> None:
        event_times = attempt.get("_httpcore_event_times")
        if not isinstance(event_times, dict) or not event_times:
            return

        def duration_ms(stage: str) -> float | None:
            started = event_times.get(f"{stage}.started")
            finished = event_times.get(f"{stage}.complete")
            if finished is None:
                finished = event_times.get(f"{stage}.failed")
            if not isinstance(started, int) or not isinstance(finished, int):
                return None
            return round((finished - started) / 1_000_000.0, 3)

        def span_ms(start_event: str, end_event: str) -> float | None:
            started = event_times.get(start_event)
            finished = event_times.get(end_event)
            if not isinstance(started, int) or not isinstance(finished, int):
                return None
            return round((finished - started) / 1_000_000.0, 3)

        protocol = (
            "http2" if any(name.startswith("http2.") for name in event_times)
            else "http11" if any(name.startswith("http11.") for name in event_times)
            else None
        )
        metrics: dict[str, Any] = {
            "tcp_connect_ms": duration_ms("connection.connect_tcp"),
            "tls_handshake_ms": duration_ms("connection.start_tls"),
            "http_protocol_trace": protocol,
            "connection_setup_observed": any(
                name in event_times
                for name in (
                    "connection.connect_tcp.started",
                    "connection.start_tls.started",
                )
            ),
        }
        if protocol is not None:
            send_headers = f"{protocol}.send_request_headers"
            send_body = f"{protocol}.send_request_body"
            receive_headers = f"{protocol}.receive_response_headers"
            metrics.update(
                {
                    "http2_connection_init_ms": duration_ms("http2.send_connection_init") if protocol == "http2" else None,
                    "http_send_headers_ms": duration_ms(send_headers),
                    "http_send_body_ms": duration_ms(send_body),
                    "http_send_total_ms": span_ms(
                        f"{send_headers}.started", f"{send_body}.complete"
                    ),
                    "http_receive_response_headers_ms": duration_ms(receive_headers),
                    "http_body_complete_to_response_headers_ms": span_ms(
                        f"{send_body}.complete", f"{receive_headers}.complete"
                    ),
                }
            )
        first_event_ns = min(event_times.values())
        request_started_ns = attempt.get("_started_perf_ns")
        if isinstance(request_started_ns, int):
            metrics["request_hook_to_httpcore_ms"] = round(
                (first_event_ns - request_started_ns) / 1_000_000.0, 3
            )
        metrics["trace_event_count"] = len(event_times)
        attempt["httpcore_timing_ms"] = metrics

    @staticmethod
    def _export_metadata_timing(trace: dict[str, Any]) -> None:
        target = trace.get("export_target")
        if not isinstance(target, dict):
            return
        attempts = trace.get("metadata_http_attempts", [])
        target["metadata_http_requests"] = [
            {
                key: attempt[key]
                for key in ("service", "path", "request_started_ns", "response_received_ns", "elapsed_ms", "status_code")
                if key in attempt
            }
            for attempt in attempts
        ]

    @staticmethod
    def _finish_incomplete_http_attempts(trace: dict[str, Any]) -> None:
        now_perf_ns = time.perf_counter_ns()
        for key in ("post_http_attempts", "metadata_http_attempts", "market_book_http_attempts"):
            for attempt in trace.get(key, []):
                if "elapsed_ms" not in attempt:
                    attempt["elapsed_ms"] = (
                        now_perf_ns - attempt["_started_perf_ns"]
                    ) / 1_000_000.0
                if key == "post_http_attempts":
                    PolymarketLiveExecutor._attach_httpcore_timing(attempt)

    @staticmethod
    def _export_order_timing(trace: dict[str, Any]) -> None:
        target = trace.get("export_target")
        if not isinstance(target, dict):
            return
        attempts = trace.get("post_http_attempts", [])
        market_book_attempts = trace.get("market_book_http_attempts", [])
        completed = [attempt for attempt in attempts if "response_received_ns" in attempt]
        measured = [attempt for attempt in attempts if "elapsed_ms" in attempt]
        completed_market_books = [
            attempt for attempt in market_book_attempts if "response_received_ns" in attempt
        ]
        last_response = completed[-1] if completed else {}
        target.update(
            {
                "order_create_ms": trace.get("order_create_ms"),
                "order_prepare_source": trace.get("order_prepare_source"),
                "signed_event_persist_ms": target.get("signed_event_persist_ms"),
                "sdk_post_order_call_ms": trace.get("sdk_post_order_call_ms"),
                "post_http_attempt_count": len(attempts),
                "post_http_response_count": len(completed),
                "post_http_attempts": [
                    {
                        key: attempt[key]
                        for key in (
                            "request_started_ns",
                            "response_received_ns",
                            "elapsed_ms",
                            "status_code",
                            "http_version",
                            "httpcore_timing_ms",
                        )
                        if key in attempt
                    }
                    for attempt in attempts
                ],
                "post_http_total_ms": (
                    sum(float(item["elapsed_ms"]) for item in completed)
                    if completed
                    else None
                ),
                "post_http_first_ms": (
                    float(attempts[0]["elapsed_ms"])
                    if attempts and "response_received_ns" in attempts[0]
                    else None
                ),
                "post_http_wait_ms": sum(float(item["elapsed_ms"]) for item in measured),
                "market_book_http_request_count": len(market_book_attempts),
                "market_book_http_requests": [
                    {
                        key: attempt[key]
                        for key in (
                            "service", "path", "request_started_ns", "response_received_ns",
                            "elapsed_ms", "status_code", "http_version",
                        )
                        if key in attempt
                    }
                    for attempt in market_book_attempts
                ],
                "market_book_http_total_ms": (
                    sum(float(item["elapsed_ms"]) for item in completed_market_books)
                    if completed_market_books
                    else None
                ),
                "post_http_status_code": last_response.get("status_code"),
                "post_http_version": last_response.get("http_version"),
                "post_request_started_ns": attempts[0].get("request_started_ns") if attempts else None,
                "post_response_received_ns": last_response.get("response_received_ns"),
            }
        )
        first_request_ns = target.get("post_request_started_ns")
        trigger_ns = target.get("trigger_ns")
        intent_ns = target.get("intent_ns")
        if first_request_ns is not None and trigger_ns is not None:
            target["trigger_to_post_start_ms"] = (first_request_ns - trigger_ns) / 1_000_000.0
        if first_request_ns is not None and intent_ns is not None:
            target["intent_to_post_start_ms"] = (first_request_ns - intent_ns) / 1_000_000.0
        target["metadata_http_total_ms"] = sum(
            float(attempt.get("elapsed_ms", 0.0))
            for attempt in trace.get("metadata_http_attempts", [])
        )

    async def close(self) -> None:
        user_task = self._user_stream_task
        self._user_stream_task = None
        if user_task is not None:
            user_task.cancel()
            await asyncio.gather(user_task, return_exceptions=True)
        self._user_stream_connected = False
        keepalive_task = self._http_keepalive_task
        self._http_keepalive_task = None
        if keepalive_task is not None:
            keepalive_task.cancel()
            await asyncio.gather(keepalive_task, return_exceptions=True)
        background_tasks = list(self._background_tasks)
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        self._prepared_buy_orders.clear()
        self._prepared_buy_pool_specs.clear()
        self._prepared_buy_pool_ready.clear()
        self._prepared_buy_pool_preparing.clear()
        self._prepared_buy_refills_inflight.clear()
        self._prepared_sell_orders.clear()
        context = self._client_context
        self.client = None
        self._client_context = None
        if context is not None:
            exit_method = getattr(context, "__aexit__", None)
            if exit_method is not None:
                await exit_method(None, None, None)

    def _take_prepared_buy_order(
        self,
        asset_id: str,
        size: str,
        price: Decimal,
    ) -> Any | None:
        key = self._prepared_buy_order_key(asset_id, size, price)
        cached = self._prepared_buy_orders.pop(key, None)
        if cached is None:
            return None
        order, expires_ns, pool_key = cached
        if expires_ns <= time.time_ns():
            return None
        self._schedule_prepared_buy_order_refill(key, pool_key)
        return order

    @staticmethod
    def _outcome_ids(outcome: Any) -> set[str]:
        values = {
            _field(outcome, "position_id", "positionId"),
            _field(outcome, "token_id", "tokenId"),
        }
        return {str(value) for value in values if value is not None}

    async def get_collateral_balance(self) -> Decimal:
        """Read the authenticated CLOB collateral balance, converted from 6-decimal base units."""
        if self.client is None:
            raise RuntimeError("Polymarket account is not connected")
        result = await self.client.get_balance_allowance(asset_type="COLLATERAL")
        raw_balance = _field(result, "balance")
        if isinstance(raw_balance, bool) or raw_balance is None:
            raise ValueError("Polymarket collateral balance response is missing")
        try:
            base_units = Decimal(str(raw_balance))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("Polymarket collateral balance is invalid") from exc
        if (
            not base_units.is_finite()
            or base_units < 0
            or base_units != base_units.to_integral_value()
        ):
            raise ValueError("Polymarket collateral balance is invalid")
        return base_units / Decimal("1000000")

    async def verify_market(self, market: Market) -> Decimal:
        if self.client is None:
            raise RuntimeError("Polymarket account is not connected")
        cached = self._verified_markets.get(market.condition_id.lower())
        if cached is not None:
            if str(market.up_asset_id) in cached[0] and str(market.down_asset_id) in cached[1]:
                return cached[2]
            self._verified_markets.pop(market.condition_id.lower(), None)

        markets = [
            item
            async for item in self.client.list_markets(condition_ids=market.condition_id).iter_items()
        ]
        matches = [
            item
            for item in markets
            if str(_field(item, "condition_id", "conditionId", default="")).lower()
            == market.condition_id.lower()
        ]
        if len(matches) != 1:
            raise ValueError(
                f"account market lookup returned {len(matches)} exact matches for {market.condition_id}"
            )
        sdk_market = matches[0]
        outcomes = _field(sdk_market, "outcomes")
        yes_ids = self._outcome_ids(_field(outcomes, "yes"))
        no_ids = self._outcome_ids(_field(outcomes, "no"))
        if str(market.up_asset_id) not in yes_ids or str(market.down_asset_id) not in no_ids:
            raise ValueError(
                f"Gamma and authenticated CLOB market disagree for {market.condition_id}"
            )
        trading = _field(sdk_market, "trading")
        raw_tick_size = _field(trading, "minimum_tick_size", "minimumTickSize")
        try:
            tick_size = Decimal(str(raw_tick_size))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("authenticated CLOB market has no valid minimum tick size") from exc
        if not tick_size.is_finite() or tick_size <= 0:
            raise ValueError("authenticated CLOB market has no valid minimum tick size")
        self._verified_markets[market.condition_id.lower()] = (yes_ids, no_ids, tick_size)
        return tick_size

    async def prewarm_order_metadata(self, market: Market) -> dict[str, Any]:
        """Populate the SDK's order metadata cache before a market can signal."""
        if self.client is None:
            raise RuntimeError("Polymarket account is not connected")
        context = getattr(self.client, "_ctx", None)
        cache = getattr(context, "order_metadata", None)
        condition_cache = getattr(cache, "_conditions", None)
        cache_condition = getattr(condition_cache, "set", None)
        resolve_market = getattr(cache, "resolve_market", None)
        if context is None or not callable(cache_condition) or not callable(resolve_market):
            raise RuntimeError("Polymarket SDK order metadata cache is unavailable")

        verified = self._verified_markets.get(market.condition_id.lower())
        if (
            verified is None
            or str(market.up_asset_id) not in verified[0]
            or str(market.down_asset_id) not in verified[1]
        ):
            raise ValueError("market condition and outcome assets must be account-verified before prewarm")

        # Gamma supplied this condition ID and verify_market confirmed that its
        # authenticated CLOB outcomes map to these exact two assets. Seed the
        # SDK's token->condition cache so its cold resolver can skip
        # /markets-by-token and fetch only the complete /clob-markets metadata.
        condition_id = market.condition_id.lower()
        cache_condition(market.up_asset_id, condition_id)
        cache_condition(market.down_asset_id, condition_id)

        trace: dict[str, Any] = {"phase": "metadata_prewarm", "metadata_http_attempts": []}
        token: Token[dict[str, Any] | None] = _ACTIVE_ORDER_TRACE.set(trace)
        started_perf_ns = time.perf_counter_ns()
        try:
            metadata = await resolve_market(context, token_id=market.up_asset_id)
        finally:
            self._finish_incomplete_http_attempts(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
        elapsed_ms = (time.perf_counter_ns() - started_perf_ns) / 1_000_000.0
        metadata_requests = [
            {
                key: attempt[key]
                for key in (
                    "service",
                    "path",
                    "request_started_ns",
                    "response_received_ns",
                    "elapsed_ms",
                    "status_code",
                )
                if key in attempt
            }
            for attempt in trace.get("metadata_http_attempts", [])
        ]
        result = {
            "cache_hit": not metadata_requests,
            "elapsed_ms": elapsed_ms,
            "metadata_http_requests": metadata_requests,
            "tick_size": str(_field(metadata, "tick_size", "minimum_tick_size", "minimumTickSize", default="unknown")),
        }
        logger.info(
            "LIVE_ORDER_METADATA_PREWARMED market=%s elapsed_ms=%.3f metadata_http_requests=%d",
            market.slug,
            elapsed_ms,
            len(result["metadata_http_requests"]),
        )
        return result

    async def buy_gtc_limit(
        self,
        *,
        asset_id: str,
        condition_id: str,
        requested_qty: float,
        limit_price: Decimal | str | float,
        tick_size: Decimal | str,
        on_order_signed: Callable[[dict[str, Any]], None] | None = None,
        timing_context: dict[str, Any] | None = None,
    ) -> tuple[bool, str | None, dict[str, Any]]:
        """Post a pre-signed or locally signed BUY GTC limit order."""
        if self.client is None:
            raise RuntimeError("Polymarket account is not connected")
        price = Decimal(str(limit_price))
        tick = Decimal(str(tick_size))
        size_text = f"{requested_qty:.6f}"
        if (
            not price.is_finite() or not Decimal("0") < price < 1
            or not tick.is_finite() or tick <= 0 or tick >= 1 or price % tick != 0
            or requested_qty <= 0
        ):
            raise ValueError("BUY GTC price, tick size, and share quantity must be valid")
        trace: dict[str, Any] = {
            "phase": "order",
            "post_http_attempts": [],
            "metadata_http_attempts": [],
            "market_book_http_attempts": [],
            "export_target": timing_context,
        }
        token = _ACTIVE_ORDER_TRACE.set(trace)
        started_ns = time.perf_counter_ns()
        try:
            signed_order = self._take_prepared_buy_order(asset_id, size_text, price)
            if signed_order is not None:
                source = "pre_signed_gtc_buy_pool"
            else:
                signed_order = await self.client.create_limit_order(
                    asset_id=asset_id,
                    side="BUY",
                    price=format(price, "f"),
                    size=size_text,
                    post_only=False,
                )
                source = "on_demand_gtc_limit_sign"
            signed_order_type = str(_field(signed_order, "order_type", default="")).upper()
            signed_post_only = bool(_field(signed_order, "post_only", default=False))
            if signed_order_type != "GTC" or signed_post_only:
                raise ValueError("prepared BUY order must be GTC with post_only=False")
            trace["order_prepare_source"] = source
            signed_details = _signed_order_diagnostics(signed_order, "BUY")
            signed_details.update({
                "sdk_order_type": signed_order_type,
                "sdk_post_only": signed_post_only,
                "sdk_limit_price": format(price, "f"),
                "sdk_requested_share_qty": size_text,
                "sdk_order_prepare_source": source,
                "sdk_tick_size": format(tick, "f"),
            })
            if on_order_signed is not None:
                on_order_signed(signed_details)
        except asyncio.CancelledError:
            trace["order_create_ms"] = (time.perf_counter_ns() - started_ns) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            raise
        except Exception as exc:
            trace["order_create_ms"] = (time.perf_counter_ns() - started_ns) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            details = _request_rejection_diagnostics(exc)
            details.update({
                "sdk_order_stage": "create_limit_order",
                "sdk_limit_price": format(price, "f"),
                "sdk_requested_share_qty": size_text,
                "outcome_unknown": False,
                "error_type": type(exc).__name__,
                "error_message": _safe_diagnostic_text(exc),
                "live_order_timing": dict(timing_context or {}),
            })
            logger.error("LIVE_CLOB_GTC_BUY_PREPARATION_FAILED error_type=%s", type(exc).__name__)
            return False, None, details

        trace["order_create_ms"] = (time.perf_counter_ns() - started_ns) / 1_000_000.0
        self._export_order_timing(trace)
        if timing_context is not None:
            timing_context["order_prepare_source"] = source
        try:
            post_started_ns = time.perf_counter_ns()
            response = await self.client.post_order(signed_order)
            trace["sdk_post_order_call_ms"] = (
                time.perf_counter_ns() - post_started_ns
            ) / 1_000_000.0
            self._export_order_timing(trace)
        except asyncio.CancelledError:
            if "post_started_ns" in locals():
                trace["sdk_post_order_call_ms"] = (
                    time.perf_counter_ns() - post_started_ns
                ) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            raise
        except Exception as exc:
            if "post_started_ns" in locals():
                trace["sdk_post_order_call_ms"] = (
                    time.perf_counter_ns() - post_started_ns
                ) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            details = _request_rejection_diagnostics(exc)
            details.update(signed_details)
            details.update({
                "sdk_order_stage": "post_order",
                "outcome_unknown": True,
                "live_order_timing": dict(timing_context or {}),
            })
            raise LiveOrderUnknown(
                f"BUY GTC POST outcome is unknown ({type(exc).__name__})",
                diagnostic_fields=details,
            ) from exc

        order_id = _field(response, "order_id", "orderId", "id")
        status = str(_field(response, "status", default="unknown") or "unknown").lower()
        accepted = bool(_field(response, "ok", "success", default=False)) or order_id is not None
        returned_trade_ids = [
            str(item) for item in (_field(response, "trade_ids", "tradeIds", default=()) or ())
        ]
        details = {
            "order_id": None if order_id is None else str(order_id),
            "order_status": status,
            "trade_ids": returned_trade_ids,
            "matched_without_trade_ids": status in {"matched", "filled"} and not returned_trade_ids,
            "user_ws_connected_at_response": self._user_stream_connected,
            "rejection_code": _field(response, "code"),
            "rejection_message": _field(response, "message", "error_msg", "errorMsg"),
            "outcome_unknown": False,
            **signed_details,
            "live_order_timing": dict(timing_context or {}),
        }
        if accepted and order_id is not None:
            oid = str(order_id)
            self._tracked_gtc_orders[oid] = {
                "asset_id": str(asset_id), "condition_id": str(condition_id),
                "requested_qty": Decimal(size_text),
            }
            self._tracked_gtc_orders.move_to_end(oid)
            self._remember_user_order(oid, {
                "status": status,
                "size_matched": _field(response, "size_matched", "sizeMatched"),
            }, "POST_RESPONSE")
        _ACTIVE_ORDER_TRACE.reset(token)
        if not accepted:
            return False, None, details
        if order_id is None:
            details["outcome_unknown"] = True
            raise LiveOrderUnknown(
                "accepted BUY GTC response omitted its order ID",
                diagnostic_fields=details,
            )
        return True, str(order_id), details

    async def cancel_and_reconcile_gtc_buy(
        self, *, order_id: str, asset_id: str, condition_id: str,
        requested_qty: float, wait_seconds: float,
        progress_callback: Callable[[str, str, dict[str, Any]], None] | None = None,
    ) -> tuple[Fill | None, dict[str, Any]]:
        """Use User WS for timely state and REST trades to reconcile every GTC fill."""
        if self.client is None:
            raise LiveOrderUnknown("Polymarket account is not connected during GTC reconciliation")

        def report(step: str, outcome: str, **details: Any) -> None:
            if progress_callback is None:
                return
            try:
                progress_callback(step, outcome, details)
            except Exception as exc:
                logger.warning("LIVE_GTC_PROGRESS_CALLBACK_FAILED step=%s error_type=%s",
                               step, type(exc).__name__)

        async def timed_call(step: str, attempt: int, call: Callable[[], Any]) -> Any:
            report(step, "started", attempt=attempt,
                   timeout_seconds=self._GTC_REST_CALL_TIMEOUT_SECONDS)
            started = time.monotonic()
            try:
                result = await asyncio.wait_for(
                    call(), timeout=self._GTC_REST_CALL_TIMEOUT_SECONDS,
                )
            except asyncio.CancelledError:
                report(step, "cancelled", attempt=attempt,
                       elapsed_ms=round((time.monotonic() - started) * 1000.0, 3))
                raise
            except Exception as exc:
                report(step, "error", attempt=attempt,
                       elapsed_ms=round((time.monotonic() - started) * 1000.0, 3),
                       error_type=type(exc).__name__,
                       error=_safe_diagnostic_text(exc))
                raise
            report(step, "completed", attempt=attempt,
                   elapsed_ms=round((time.monotonic() - started) * 1000.0, 3))
            return result

        context = self._tracked_gtc_orders.setdefault(order_id, {})
        context.update({"asset_id": asset_id, "condition_id": condition_id,
                        "requested_qty": Decimal(str(requested_qty))})
        report("user_ws_wait", "started", wait_seconds=wait_seconds)
        await self._wait_gtc_fill_or_timeout(order_id, requested_qty, wait_seconds)
        ws = dict(self._user_order_states.get(order_id, {}))
        ws_status = str(ws.get("status", "") or "").lower()
        report("user_ws_wait", "completed", order_status=ws_status or None,
               size_matched=ws.get("size_matched"))
        try:
            ws_qty = Decimal(str(ws.get("size_matched")))
            if not ws_qty.is_finite() or ws_qty < 0:
                ws_qty = Decimal("-1")
        except (InvalidOperation, TypeError, ValueError):
            ws_qty = Decimal("-1")
        full_seen = ws_status in {"matched", "filled"} or ws_qty >= Decimal(str(requested_qty)) - Decimal("0.0001")
        cancellation_confirmed = False
        cancel_error = None
        not_canceled: Any = {}
        if not full_seen:
            try:
                response = await timed_call(
                    "cancel_order", 1,
                    lambda: self.client.cancel_order(order_id=order_id),
                )
                canceled = _field(response, "canceled", default=()) or ()
                not_canceled = _field(response, "not_canceled", "notCanceled", default={}) or {}
                cancellation_confirmed = order_id in {str(x) for x in canceled}
                report("cancel_order", "observed", attempt=1,
                       cancellation_confirmed=cancellation_confirmed,
                       canceled_order_count=len(canceled),
                       not_canceled_count=len(not_canceled))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                cancel_error = f"{type(exc).__name__}: {_safe_diagnostic_text(exc)}"

        order_state = None
        order_error = None
        terminal = {"canceled", "cancelled", "matched", "filled", "expired"}
        for attempt in range(1, 4):
            try:
                order_state = await timed_call(
                    "get_order", attempt,
                    lambda: self.client.get_order(order_id=order_id),
                )
                order_error = None
                observed_status = str(_field(order_state, "status", default="") or "").lower()
                report("get_order", "observed", attempt=attempt,
                       order_status=observed_status or None,
                       size_matched=_field(order_state, "size_matched", "sizeMatched"))
                if observed_status in terminal:
                    break
                if attempt < 3:
                    response = await timed_call(
                        "cancel_order", attempt + 1,
                        lambda: self.client.cancel_order(order_id=order_id),
                    )
                    canceled = _field(response, "canceled", default=()) or ()
                    this_cancel_confirmed = order_id in {str(x) for x in canceled}
                    cancellation_confirmed = cancellation_confirmed or this_cancel_confirmed
                    not_canceled = _field(response, "not_canceled", "notCanceled", default={}) or {}
                    report("cancel_order", "observed", attempt=attempt + 1,
                           cancellation_confirmed=this_cancel_confirmed,
                           canceled_order_count=len(canceled),
                           not_canceled_count=len(not_canceled))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                order_error = f"{type(exc).__name__}: {_safe_diagnostic_text(exc)}"
            if attempt < 3:
                await asyncio.sleep(0.15 * attempt)

        rest_status = str(_field(order_state, "status", default="") or "").lower() if order_state is not None else ""
        raw_rest_qty = _field(order_state, "size_matched", "sizeMatched") if order_state is not None else None
        try:
            rest_qty = Decimal(str(raw_rest_qty))
            if not rest_qty.is_finite() or rest_qty < 0:
                rest_qty = Decimal("-1")
        except (InvalidOperation, TypeError, ValueError):
            rest_qty = Decimal("-1")

        # Always query account trades, including when get_order parsing failed.
        rows: dict[str, tuple[str, Decimal, Decimal, str]] = {}
        rest_rows_found = False
        trade_error = None
        delays = (0.15, 0.25, 0.4, 0.65, 1.0, 1.0)
        for attempt in range(1, len(delays) + 2):
            if self.client is not None:
                try:
                    current = await timed_call(
                        "account_trades", attempt,
                        lambda: self._fetch_account_trade_rows(
                            order_id, asset_id, condition_id,
                        ),
                    )
                    rest_rows_found = rest_rows_found or bool(current)
                    rows.update(current)
                    for tid, qty, price, status in current.values():
                        self._remember_user_trade(order_id, tid, qty, price, status, "REST")
                    report(
                        "account_trades", "observed", attempt=attempt,
                        row_count=len(current),
                        matched_qty=format(sum((row[1] for row in current.values()), Decimal("0")), "f"),
                        statuses=sorted({row[3] or "UNKNOWN" for row in current.values()}),
                    )
                    trade_error = None
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    trade_error = f"{type(exc).__name__}: {_safe_diagnostic_text(exc)}"
            rows.update(self._cached_user_trades(order_id))
            total = sum((row[1] for row in rows.values()), Decimal("0"))
            confirmed_total = sum(
                (row[1] for row in rows.values() if row[3] in {"MINED", "CONFIRMED"}),
                Decimal("0"),
            )
            current_ws = dict(self._user_order_states.get(order_id, {}))
            try:
                current_ws_qty = Decimal(str(current_ws.get("size_matched")))
                if not current_ws_qty.is_finite() or current_ws_qty < 0:
                    current_ws_qty = Decimal("-1")
            except (InvalidOperation, TypeError, ValueError):
                current_ws_qty = Decimal("-1")
            hint_values = [q for q in (rest_qty, current_ws_qty) if q >= 0]
            hint = max(hint_values) if hint_values else None
            fully_confirmed_from_trades = (
                total >= Decimal(str(requested_qty)) - Decimal("0.0001")
                and confirmed_total >= Decimal(str(requested_qty)) - Decimal("0.0001")
            )
            if fully_confirmed_from_trades or (
                total > 0 and (hint is None or abs(total - hint) <= Decimal("0.0001"))
            ):
                break
            if attempt <= len(delays):
                self._user_state_changed.clear()
                try:
                    await asyncio.wait_for(self._user_state_changed.wait(), delays[attempt - 1])
                except asyncio.TimeoutError:
                    pass

        total = sum((row[1] for row in rows.values()), Decimal("0"))
        latest_ws = dict(self._user_order_states.get(order_id, {}))
        latest_ws_status = str(latest_ws.get("status", "") or "").lower()
        status = rest_status if rest_status in terminal else latest_ws_status if latest_ws_status in terminal else ""
        confirmed_trade_qty = sum(
            (row[1] for row in rows.values() if row[3] in {"MINED", "CONFIRMED"}),
            Decimal("0"),
        )
        inferred_terminal_from_trades = (
            total >= Decimal(str(requested_qty)) - Decimal("0.0001")
            and confirmed_trade_qty >= Decimal(str(requested_qty)) - Decimal("0.0001")
        )
        if not status and inferred_terminal_from_trades:
            status = "matched"
        if not status and cancellation_confirmed:
            status = "canceled"
        if (rest_status and rest_status not in terminal
                and not inferred_terminal_from_trades and not cancellation_confirmed):
            status = ""
        if not status:
            raise LiveOrderUnknown(
                f"BUY GTC order did not reach a confirmed terminal state order_id={order_id}",
                diagnostic_fields={
                    "order_id": order_id, "rest_order_status": rest_status or None,
                    "user_ws_order_status": latest_ws_status or None,
                    "rest_size_matched": raw_rest_qty,
                    "user_ws_size_matched": latest_ws.get("size_matched"),
                    "cancel_error": cancel_error, "order_query_error": order_error,
                    "trade_query_error": trade_error, "trade_size_matched": format(total, "f"),
                    "cancellation_confirmed": cancellation_confirmed, "not_canceled": not_canceled,
                },
            )
        hints = [q for q in (rest_qty, ws_qty) if q >= 0]
        if inferred_terminal_from_trades or cancellation_confirmed:
            hints.append(total)
        matched = max(hints) if hints else total
        if matched > Decimal(str(requested_qty)) + Decimal("0.0001"):
            raise LiveOrderUnknown("BUY GTC returned an invalid matched quantity", diagnostic_fields={"order_id": order_id})
        if matched <= Decimal("0.000001") and total <= Decimal("0.000001"):
            if status in {"matched", "filled"}:
                raise LiveOrderUnknown(
                    f"BUY GTC order is matched but fill details are not available order_id={order_id}",
                    diagnostic_fields={"order_id": order_id, "order_status": status,
                                       "trade_query_error": trade_error, "trade_ids_empty": True},
                )
            self._tracked_gtc_orders.pop(order_id, None)
            return None, {"order_id": order_id, "order_status": status,
                          "size_matched": format(matched, "f"),
                          "remaining_canceled": max(0.0, requested_qty - float(matched)),
                          "cancel_error": cancel_error, "order_query_error": order_error,
                          "trade_query_error": trade_error,
                          "cancellation_confirmed": cancellation_confirmed,
                          "user_ws_used": bool(latest_ws), "trade_ids_empty": True}
        if abs(total - matched) > Decimal("0.0001"):
            raise LiveOrderUnknown(
                f"BUY GTC fills did not reconcile to order size_matched order_id={order_id}",
                diagnostic_fields={"order_id": order_id, "order_status": status,
                                   "order_size_matched": format(matched, "f"),
                                   "trade_size_matched": format(total, "f"),
                                   "trade_query_error": trade_error},
            )
        notional = sum((qty * px for _, qty, px, _ in rows.values()), Decimal("0"))
        filled = float(total)
        value = float(notional)
        vwap = value / filled
        if not (math.isfinite(vwap) and 0 < vwap < 1):
            raise LiveOrderUnknown("BUY GTC fills returned an invalid VWAP", diagnostic_fields={"order_id": order_id, "vwap": vwap})
        fill = Fill(
            requested_qty=requested_qty, filled_qty=filled, notional=value,
            fee=filled * self.fee_rate * vwap * (1.0 - vwap), vwap=vwap,
            levels=tuple({"price": float(px), "available_size": float(qty), "filled_size": float(qty)}
                         for _, qty, px, _ in rows.values()),
        )
        self._tracked_gtc_orders.pop(order_id, None)
        return fill, {"order_id": order_id, "order_status": status,
                      "size_matched": format(matched, "f"), "trade_count": len(rows),
                      "trade_notional": format(notional, "f"),
                      "remaining_canceled": max(0.0, requested_qty - filled),
                      "cancel_error": cancel_error, "order_query_error": order_error,
                      "trade_query_error": trade_error,
                      "cancellation_confirmed": cancellation_confirmed,
                      "user_ws_used": bool(latest_ws),
                      "order_status_inferred_from_confirmed_trades": inferred_terminal_from_trades,
                      "fill_source": "REST_ACCOUNT_TRADES" if rest_rows_found else "USER_WS_CONFIRMED_TRADE"}

    async def buy_fak(
        self,
        *,
        asset_id: str,
        requested_qty: float,
        market_amount_usdc: Decimal | str | float,
        max_price: Decimal | str | float,
        tick_size: Decimal | str,
        on_order_signed: Callable[[dict[str, Any]], None] | None = None,
        timing_context: dict[str, Any] | None = None,
    ) -> tuple[bool, Fill | None, dict[str, Any]]:
        return await self._place_fak(
            side="BUY",
            asset_id=asset_id,
            requested_qty=requested_qty,
            market_amount_usdc=market_amount_usdc,
            max_price=max_price,
            tick_size=tick_size,
            on_order_signed=on_order_signed,
            timing_context=timing_context,
        )

    async def sell_fak(
        self,
        *,
        asset_id: str,
        requested_qty: float,
        limit_price: str = "0.01",
        tick_size: Decimal | str | None = None,
        on_order_signed: Callable[[dict[str, Any]], None] | None = None,
        timing_context: dict[str, Any] | None = None,
    ) -> tuple[bool, Fill | None, dict[str, Any]]:
        return await self._place_fak(
            side="SELL",
            asset_id=asset_id,
            requested_qty=requested_qty,
            shares=requested_qty,
            limit_price=limit_price,
            tick_size=tick_size,
            on_order_signed=on_order_signed,
            timing_context=timing_context,
        )

    async def _place_fak(
        self,
        *,
        side: str,
        asset_id: str,
        requested_qty: float,
        shares: float | None = None,
        market_amount_usdc: Decimal | str | float | None = None,
        max_price: Decimal | str | float | None = None,
        limit_price: Decimal | str | float | None = None,
        tick_size: Decimal | str | None = None,
        on_order_signed: Callable[[dict[str, Any]], None] | None = None,
        timing_context: dict[str, Any] | None = None,
    ) -> tuple[bool, Fill | None, dict[str, Any]]:
        if self.client is None:
            raise RuntimeError("Polymarket account is not connected")
        is_buy = side.upper() == "BUY"
        market_amount_decimal: Decimal | None = None
        max_price_decimal: Decimal | None = None
        limit_price_decimal: Decimal | None = None
        if is_buy:
            if market_amount_usdc is None:
                raise ValueError("market_amount_usdc is required for BUY market FAK orders")
            market_amount_decimal = Decimal(str(market_amount_usdc))
            if not market_amount_decimal.is_finite() or market_amount_decimal <= 0:
                raise ValueError("BUY market FAK amount must be a positive USDC amount")
            if max_price is None:
                raise ValueError("max_price is required for BUY market FAK orders")
            max_price_decimal = Decimal(str(max_price))
            if not max_price_decimal.is_finite() or not Decimal("0") < max_price_decimal < 1:
                raise ValueError("BUY max_price must be between 0 and 1")
            if tick_size is not None:
                tick_decimal = Decimal(str(tick_size))
                if tick_decimal <= 0 or tick_decimal >= 1 or max_price_decimal % tick_decimal != 0:
                    raise ValueError("BUY max_price must conform to the current tick size")
        else:
            if shares is None or limit_price is None:
                raise ValueError("shares and limit_price are required for SELL FAK orders")
            limit_price_decimal = Decimal(str(limit_price))
            if not limit_price_decimal.is_finite() or not Decimal("0") < limit_price_decimal < Decimal("1"):
                raise ValueError("FAK limit price must be between 0 and 1")
        trace: dict[str, Any] = {
            "phase": "order",
            "post_http_attempts": [],
            "metadata_http_attempts": [],
            "market_book_http_attempts": [],
            "export_target": timing_context,
        }
        token: Token[dict[str, Any] | None] = _ACTIVE_ORDER_TRACE.set(trace)
        order_create_started_ns = time.perf_counter_ns()
        try:
            if is_buy:
                size_text = f"{requested_qty:.6f}"
                assert max_price_decimal is not None
                fak_order = self._take_prepared_buy_order(
                    asset_id, size_text, max_price_decimal,
                )
                if fak_order is not None:
                    order_prepare_source = "pre_signed_market_buy_pool"
                else:
                    # With max_price the SDK uses its prewarmed market metadata
                    # and signs locally without fetching a fresh /book.
                    fak_order = await self.client.create_market_order(
                        asset_id=asset_id,
                        side="BUY",
                        amount=format(market_amount_decimal, "f"),
                        max_price=format(max_price_decimal, "f"),
                        order_type="FAK",
                    )
                    order_prepare_source = "protected_market_order_signed_on_demand"
            else:
                size_text = f"{shares:.6f}"
                fak_order = self._take_prepared_sell_order(
                    asset_id, size_text, limit_price_decimal,
                    None if tick_size is None else Decimal(str(tick_size)),
                )
                if fak_order is not None:
                    order_prepare_source = "pre_signed_pool"
                else:
                    signed_order = await self.client.create_limit_order(
                        asset_id=asset_id,
                        side="SELL",
                        price=format(limit_price_decimal, "f"),
                        size=size_text,
                        post_only=False,
                    )
                    fak_order = replace(signed_order, order_type="FAK")
                    order_prepare_source = "on_demand_sign"
            trace["order_prepare_source"] = order_prepare_source
            if timing_context is not None:
                timing_context["order_prepare_source"] = order_prepare_source
        except asyncio.CancelledError:
            trace["order_create_ms"] = (time.perf_counter_ns() - order_create_started_ns) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            self._export_metadata_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            raise
        except Exception as exc:
            trace["order_create_ms"] = (time.perf_counter_ns() - order_create_started_ns) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            self._export_metadata_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            diagnostics = _request_rejection_diagnostics(exc)
            diagnostics.update(
                {
                    "sdk_order_stage": "create_market_order" if is_buy else "create_limit_order",
                    "sdk_market_amount_usdc": (
                        format(market_amount_decimal, "f") if is_buy else None
                    ),
                    "sdk_explicit_max_price": (
                        format(max_price_decimal, "f") if is_buy else None
                    ),
                    "sdk_limit_price": (
                        None if is_buy else format(limit_price_decimal, "f")
                    ),
                    "sdk_error_type": type(exc).__name__,
                    "sdk_error_message": _safe_diagnostic_text(exc),
                    "outcome_unknown": False,
                    "live_order_timing": dict(timing_context or {}),
                }
            )
            logger.error(
                "LIVE_CLOB_ORDER_PREPARATION_FAILED side=%s error_type=%s",
                side,
                type(exc).__name__,
            )
            return False, None, diagnostics
        trace["order_create_ms"] = (time.perf_counter_ns() - order_create_started_ns) / 1_000_000.0
        self._export_order_timing(trace)
        self._export_metadata_timing(trace)

        signed_details = _signed_order_diagnostics(fak_order, side)
        signed_details.update(
            {
                "sdk_order_type": "FAK",
                "sdk_post_only": signed_post_only,
                "sdk_explicit_max_price": (
                    format(max_price_decimal, "f") if is_buy else None
                ),
                "sdk_limit_price": None if is_buy else format(limit_price_decimal, "f"),
                "sdk_market_amount_usdc": (
                    format(market_amount_decimal, "f") if is_buy else None
                ),
                "sdk_market_price_source": (
                    "explicit_max_price_no_fresh_book" if is_buy else None
                ),
                "sdk_order_prepare_source": trace.get("order_prepare_source", "on_demand_sign"),
            }
        )
        if signed_details.get("sdk_signed_share_qty") is not None:
            signed_details["sdk_requested_share_qty"] = format(
                Decimal(f"{requested_qty:.6f}"), "f"
            )
            signed_details["sdk_share_qty_rounding_delta"] = format(
                Decimal(f"{requested_qty:.6f}")
                - Decimal(str(signed_details["sdk_signed_share_qty"])),
                "f",
            )
        if on_order_signed is not None:
            signed_event_started_ns = time.perf_counter_ns()
            try:
                on_order_signed(signed_details)
            except BaseException:
                _ACTIVE_ORDER_TRACE.reset(token)
                raise
            if timing_context is not None:
                timing_context["signed_event_persist_ms"] = (
                    time.perf_counter_ns() - signed_event_started_ns
                ) / 1_000_000.0
                self._export_order_timing(trace)
        try:
            post_order_started_ns = time.perf_counter_ns()
            response = await self.client.post_order(fak_order)
            trace["sdk_post_order_call_ms"] = (
                time.perf_counter_ns() - post_order_started_ns
            ) / 1_000_000.0
            self._export_order_timing(trace)
        except asyncio.CancelledError:
            if "post_order_started_ns" in locals():
                trace["sdk_post_order_call_ms"] = (
                    time.perf_counter_ns() - post_order_started_ns
                ) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            self._export_metadata_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            raise
        except Exception as exc:
            if "post_order_started_ns" in locals():
                trace["sdk_post_order_call_ms"] = (
                    time.perf_counter_ns() - post_order_started_ns
                ) / 1_000_000.0
            self._finish_incomplete_http_attempts(trace)
            self._export_order_timing(trace)
            self._export_metadata_timing(trace)
            _ACTIVE_ORDER_TRACE.reset(token)
            diagnostics = _request_rejection_diagnostics(exc)
            diagnostics.update(signed_details)
            diagnostics["live_order_timing"] = dict(timing_context or {})
            if side.upper() == "SELL" and _is_explicit_balance_allowance_rejection(exc):
                diagnostics["sdk_rejection_kind"] = "insufficient_balance_or_allowance"
                diagnostics["outcome_unknown"] = False
                logger.warning(
                    "LIVE_CLOB_SELL_BALANCE_ALLOWANCE_REJECTED status=%s; sell will retry",
                    diagnostics.get("sdk_status"),
                )
                raise LiveBalanceAllowanceRejected(
                    "SELL FAK was explicitly rejected because balance or allowance was unavailable",
                    diagnostic_fields=diagnostics,
                ) from exc
            if _is_explicit_fak_no_fill(exc):
                diagnostics["sdk_rejection_kind"] = "fak_not_filled"
                diagnostics["outcome_unknown"] = False
                logger.warning(
                    "LIVE_CLOB_FAK_NO_FILL side=%s status=%s sdk_order_price=%s sdk_error_message=%s",
                    side,
                    diagnostics.get("sdk_status"),
                    diagnostics.get("sdk_order_price"),
                    diagnostics.get("sdk_error_message"),
                )
                raise LiveFakNoFill(
                    f"{side} FAK was explicitly rejected without a match",
                    diagnostic_fields=diagnostics,
                ) from exc
            if hasattr(exc, "status"):
                logger.error(
                    "LIVE_CLOB_REQUEST_REJECTED side=%s status=%s code=%s restriction=%s retry_after=%s sdk_order_price=%s",
                    side,
                    diagnostics.get("sdk_status"),
                    diagnostics.get("sdk_code"),
                    diagnostics.get("sdk_restriction"),
                    diagnostics.get("sdk_retry_after"),
                    diagnostics.get("sdk_order_price"),
                )
            raise LiveOrderUnknown(
                f"{side} FAK request outcome is unknown ({type(exc).__name__})",
                diagnostic_fields=diagnostics,
            ) from exc

        accepted = bool(_field(response, "ok", default=False))
        order_id = _field(response, "order_id", "orderId")
        status = str(_field(response, "status", default="unknown")).lower()
        details = {
            "order_id": str(order_id) if order_id is not None else None,
            "order_status": status,
            "trade_ids": [
                str(item)
                for item in (_field(response, "trade_ids", "tradeIds", default=()) or ())
            ],
            "transactions_hashes": [
                str(item)
                for item in (
                    _field(response, "transactions_hashes", "transactionsHashes", default=()) or ()
                )
            ],
            "rejection_code": _field(response, "code"),
            "rejection_message": _field(response, "message"),
            "fee_accounting": "paper_fee_formula_applied_to_live_vwap",
            **signed_details,
        }
        self._export_order_timing(trace)
        self._export_metadata_timing(trace)
        details["live_order_timing"] = dict(timing_context or {})
        _ACTIVE_ORDER_TRACE.reset(token)
        if not accepted:
            if side.upper() == "SELL" and _is_explicit_balance_allowance_rejection_response(response):
                details["sdk_rejection_kind"] = "insufficient_balance_or_allowance"
                details["outcome_unknown"] = False
                logger.warning("LIVE_CLOB_SELL_BALANCE_ALLOWANCE_REJECTED response; sell will retry")
                raise LiveBalanceAllowanceRejected(
                    "SELL FAK was explicitly rejected because balance or allowance was unavailable",
                    diagnostic_fields=details,
                )
            if _is_explicit_fak_no_fill_response(response):
                details["sdk_rejection_kind"] = "fak_not_filled"
                details["outcome_unknown"] = False
                logger.warning(
                    "LIVE_CLOB_FAK_NO_FILL side=%s status=%s code=%s sdk_order_price=%s sdk_error_message=%s",
                    side,
                    details.get("sdk_status"),
                    details.get("rejection_code"),
                    details.get("sdk_order_price"),
                    _safe_diagnostic_text(details.get("rejection_message", "")),
                )
                raise LiveFakNoFill(
                    f"{side} FAK was explicitly rejected without a match",
                    diagnostic_fields=details,
                )
            return False, None, details
        if status in {"", "delayed", "live", "unmatched", "unknown", "none"}:
            raise LiveOrderUnknown(
                f"accepted {side} FAK has unresolved status={status} order_id={order_id}",
                diagnostic_fields=details,
            )

        making = _field(response, "making_amount", "makingAmount", "maker_amount", "makerAmount")
        taking = _field(response, "taking_amount", "takingAmount", "taker_amount", "takerAmount")
        if making is None or taking is None:
            raise LiveOrderUnknown(
                f"accepted {side} FAK response omitted matched amounts order_id={order_id}",
                diagnostic_fields=details,
            )
        try:
            making_amount, taking_amount = float(making), float(taking)
        except (TypeError, ValueError, OverflowError) as exc:
            raise LiveOrderUnknown(
                f"accepted {side} FAK returned unparseable amounts order_id={order_id}",
                diagnostic_fields=details,
            ) from exc
        if not (math.isfinite(making_amount) and math.isfinite(taking_amount)):
            raise LiveOrderUnknown(
                f"accepted {side} FAK returned invalid amounts order_id={order_id}",
                diagnostic_fields=details,
            )
        if side == "BUY":
            filled_qty, notional = taking_amount, making_amount
        else:
            filled_qty, notional = making_amount, taking_amount
        if filled_qty <= 1e-9 or notional <= 1e-9:
            return True, None, details
        vwap = notional / filled_qty
        if not (math.isfinite(vwap) and 0 < vwap < 1):
            raise LiveOrderUnknown(
                f"{side} FAK returned invalid VWAP order_id={order_id}",
                diagnostic_fields=details,
            )
        fee = filled_qty * self.fee_rate * vwap * (1.0 - vwap)
        fill = Fill(
            requested_qty=requested_qty,
            filled_qty=filled_qty,
            notional=notional,
            fee=fee,
            vwap=vwap,
            levels=({"price": vwap, "available_size": filled_qty, "filled_size": filled_qty},),
        )
        return True, fill, details
