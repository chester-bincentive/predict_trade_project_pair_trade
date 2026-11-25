from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class Settings:
    poll_seconds: float = 1.0
    match_tolerance_seconds: float = 2.0
    max_quote_age_seconds: float = 3.0
    # Legacy threshold retained only for the deprecated immediate-fill engine.
    z_entry: float = 2.5
    ou_entry_threshold: float = 3.0
    ou_exit_threshold: float = 1.5
    min_history_markets: int = 50
    history_window_markets: int = 100
    min_ou_history_markets: int = 100
    min_rv_history_markets: int = 100
    rv_history_window_markets: int = 100
    bucket_seconds: int = 60
    # Legacy fixed-hold setting retained for the deprecated immediate-fill engine.
    hold_seconds: int = 20
    min_market_time_left_seconds: int = 60
    max_hold_seconds: int = 60
    min_shares: float = 5.0
    taker_fee_coefficient: float = 0.07
    # Fixed 330ms total assumed delay; keep the legacy components for stored scenarios.
    poly_order_network_ms: float = 330.0
    poly_order_api_ms: float = 0.0
    binance_order_network_ms: float = 330.0
    binance_order_api_ms: float = 0.0
    order_latency_jitter_ms: float = 0.0
    trading_mode: str = "paper"
    live_db_path: Path = Path("data/live_pair_trading.sqlite3")
    live_max_book_age_seconds: float = 2.0
    live_buy_price_offset_ticks: int = 3
    live_buy_unfilled_ttl_seconds: float = 2.0
    live_retry_interval_seconds: float = 1.0
    resolution_grace_seconds: float = 900.0
    discord_webhook_url: str = ""
    discord_mention_user_id: str = "1560213041126187019"
    discord_pnl_interval_seconds: int = 900
    db_path: Path = Path("data/pair_trading.sqlite3")

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        default_db = Path(__file__).resolve().parents[1] / "data" / "pair_trading.sqlite3"
        trading_mode = os.getenv("PAIR_TRADING_MODE", "paper").strip().lower()
        if trading_mode not in {"paper", "live"}:
            raise ValueError("PAIR_TRADING_MODE must be paper or live")
        settings = cls(
            poll_seconds=_float_env("PAIR_POLL_SECONDS", 1.0, minimum=0.1),
            match_tolerance_seconds=_float_env("PAIR_MATCH_TOLERANCE_SECONDS", 2.0, minimum=0.0),
            max_quote_age_seconds=_float_env("PAIR_MAX_QUOTE_AGE_SECONDS", 3.0, minimum=0.1),
            z_entry=_float_env("PAIR_Z_ENTRY", 2.5, minimum=0.1),
            ou_entry_threshold=_float_env("PAIR_OU_ENTRY_THRESHOLD", 3.0, minimum=0.1),
            ou_exit_threshold=_float_env("PAIR_OU_EXIT_THRESHOLD", 1.5, minimum=0.1),
            min_history_markets=_int_env("PAIR_MIN_HISTORY_MARKETS", 50, minimum=2),
            history_window_markets=_int_env("PAIR_HISTORY_WINDOW_MARKETS", 100, minimum=2),
            min_ou_history_markets=_int_env("PAIR_MIN_OU_HISTORY_MARKETS", 100, minimum=2),
            min_rv_history_markets=_int_env("PAIR_MIN_RV_HISTORY_MARKETS", 100, minimum=2),
            rv_history_window_markets=_int_env("PAIR_RV_HISTORY_WINDOW_MARKETS", 100, minimum=2),
            bucket_seconds=_int_env("PAIR_BUCKET_SECONDS", 60, minimum=1),
            hold_seconds=_int_env("PAIR_HOLD_SECONDS", 20, minimum=1),
            min_market_time_left_seconds=max(
                60, _int_env("PAIR_MIN_MARKET_TIME_LEFT_SECONDS", 60, minimum=1)
            ),
            max_hold_seconds=_int_env("PAIR_MAX_HOLD_SECONDS", 60, minimum=1),
            min_shares=_float_env("PAIR_MIN_SHARES", 5.0, minimum=1.0),
            taker_fee_coefficient=_float_env("PAIR_TAKER_FEE_COEFFICIENT", 0.07, minimum=0.0),
            poly_order_network_ms=_float_env("PAIR_POLY_ORDER_NETWORK_MS", 330.0, minimum=0.0),
            poly_order_api_ms=_float_env("PAIR_POLY_ORDER_API_MS", 0.0, minimum=0.0),
            binance_order_network_ms=_float_env("PAIR_BINANCE_ORDER_NETWORK_MS", 330.0, minimum=0.0),
            binance_order_api_ms=_float_env("PAIR_BINANCE_ORDER_API_MS", 0.0, minimum=0.0),
            order_latency_jitter_ms=_float_env("PAIR_ORDER_LATENCY_JITTER_MS", 0.0, minimum=0.0),
            trading_mode=trading_mode,
            live_db_path=Path(os.getenv(
                "PAIR_LIVE_DB_PATH", str(default_db.with_name("live_pair_trading.sqlite3"))
            )),
            live_max_book_age_seconds=_float_env("PAIR_LIVE_MAX_BOOK_AGE_SECONDS", 2.0, minimum=0.1),
            live_buy_price_offset_ticks=_int_env(
                "PAIR_LIVE_BUY_PRICE_OFFSET_TICKS", 3, minimum=1
            ),
            live_buy_unfilled_ttl_seconds=_float_env(
                "PAIR_LIVE_BUY_UNFILLED_TTL_SECONDS", 2.0, minimum=0.1
            ),
            live_retry_interval_seconds=_float_env(
                "PAIR_LIVE_RETRY_INTERVAL_SECONDS", 1.0, minimum=0.1
            ),
            resolution_grace_seconds=_float_env(
                "PAIR_RESOLUTION_GRACE_SECONDS", 900.0, minimum=0.0
            ),
            discord_webhook_url=os.getenv("PAIR_DISCORD_WEBHOOK_URL", "").strip(),
            discord_mention_user_id=(
                os.getenv("PAIR_DISCORD_MENTION_USER_ID", "1560213041126187019").strip()
                or "1560213041126187019"
            ),
            discord_pnl_interval_seconds=_int_env(
                "PAIR_DISCORD_PNL_INTERVAL_SECONDS", 900, minimum=60
            ),
            db_path=Path(os.getenv("PAIR_DB_PATH", str(default_db))),
        )
        if settings.min_history_markets > settings.history_window_markets:
            raise ValueError("PAIR_MIN_HISTORY_MARKETS cannot exceed PAIR_HISTORY_WINDOW_MARKETS")
        if settings.min_ou_history_markets > settings.history_window_markets:
            raise ValueError("PAIR_MIN_OU_HISTORY_MARKETS cannot exceed PAIR_HISTORY_WINDOW_MARKETS")
        if settings.min_rv_history_markets > settings.rv_history_window_markets:
            raise ValueError("PAIR_MIN_RV_HISTORY_MARKETS cannot exceed PAIR_RV_HISTORY_WINDOW_MARKETS")
        if settings.ou_exit_threshold >= settings.ou_entry_threshold:
            raise ValueError("PAIR_OU_EXIT_THRESHOLD must be below PAIR_OU_ENTRY_THRESHOLD")
        if not settings.discord_mention_user_id.isdigit():
            raise ValueError("PAIR_DISCORD_MENTION_USER_ID must contain only digits")
        if settings.live_db_path.resolve() == settings.db_path.resolve():
            raise ValueError("PAIR_LIVE_DB_PATH must be separate from PAIR_DB_PATH")
        return settings


def _float_env(name: str, default: float, *, minimum: float) -> float:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else float(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value


def _int_env(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else int(raw)
    if value < minimum:
        raise ValueError(f"{name} must be >= {minimum}")
    return value
