from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable

import httpx

from .storage import Store


class DiscordPnLNotifier:
    """Periodically post realized PnL and live collateral to a Discord webhook."""

    def __init__(
        self,
        store: Store,
        webhook_url: str,
        interval_seconds: int,
        *,
        trading_mode: str = "paper",
        collateral_balance_provider: Callable[[], Awaitable[Decimal]] | None = None,
        mention_user_id: str = "",
    ) -> None:
        self.store = store
        self.webhook_url = webhook_url
        self.interval_seconds = interval_seconds
        self.trading_mode = trading_mode
        self.collateral_balance_provider = collateral_balance_provider
        self.mention_user_id = str(mention_user_id).strip()
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0))
        self.baseline = self._stats()
        self._alert_tasks: set[asyncio.Task[None]] = set()
        self._sent_alert_keys: set[str] = set()

    async def run(self) -> None:
        while True:
            try:
                await self._send_summary()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # HTTP client errors can contain the secret webhook URL; log only the type.
                print(f"DISCORD NOTIFY ERROR ({type(exc).__name__}); will retry", flush=True)
            await asyncio.sleep(self.interval_seconds)

    async def close(self) -> None:
        if self._alert_tasks:
            await asyncio.gather(*self._alert_tasks, return_exceptions=True)
        await self.client.aclose()

    def schedule_alert(self, alert: dict[str, Any]) -> None:
        """Send a non-blocking, deduplicated alert and ping only the configured user."""
        dedupe_key = str(alert.get("dedupe_key") or "").strip()
        if dedupe_key and dedupe_key in self._sent_alert_keys:
            return
        if dedupe_key:
            self._sent_alert_keys.add(dedupe_key)
        task = asyncio.create_task(self._post_alert(alert), name="discord-live-risk-alert")
        self._alert_tasks.add(task)
        task.add_done_callback(self._alert_tasks.discard)

    async def _post_alert(self, alert: dict[str, Any]) -> None:
        user_id = self.mention_user_id if self.mention_user_id.isdigit() else ""
        mention = f"<@{user_id}> " if user_id else ""
        lines = [
            f"{mention}**⚠️ Live 交易／行情風險提醒**",
            f"狀況：{str(alert.get('title') or '交易暫停')[:180]}",
        ]
        for label, key in (
            ("市場", "market_key"), ("階段", "phase"), ("方向", "side"), ("策略訊號", "signal"),
            ("原因", "reason"), ("程式處理", "action"),
        ):
            value = alert.get(key)
            if value not in (None, ""):
                lines.append(f"{label}：{str(value)[:300]}")
        payload: dict[str, Any] = {"content": "\n".join(lines), "allowed_mentions": {"parse": []}}
        if user_id:
            payload["allowed_mentions"]["users"] = [user_id]

        alert_code = str(alert.get("alert_code") or "unknown")
        for attempt in range(1, 4):
            try:
                response = await self.client.post(self.webhook_url, json=payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"DISCORD RISK ALERT ERROR ({type(exc).__name__}); attempt={attempt}/3", flush=True)
            else:
                if response.is_success:
                    print(f"DISCORD RISK ALERT SENT code={alert_code}", flush=True)
                    return
                print(f"DISCORD RISK ALERT ERROR (HTTP {response.status_code}); attempt={attempt}/3", flush=True)
            if attempt < 3:
                await asyncio.sleep(float(attempt * 2))

    async def _send_summary(self) -> None:
        stats = self._stats()
        session_count = max(0, stats["completed"] - self.baseline["completed"])
        session_wins = max(0, stats["wins"] - self.baseline["wins"])
        session_pnl = stats["pnl"] - self.baseline["pnl"]
        mode_line = (
            "Live mode；已啟用真實下單。"
            if self.trading_mode == "live"
            else "Shadow mode；不會送出真實訂單。"
        )
        lines = [
            f"**BTC 5 分鐘 Polymarket 單腿 {self.trading_mode.title()} 損益**",
            f"更新時間（UTC）：{datetime.now(timezone.utc).isoformat(timespec='seconds')}",
            f"本次啟動後：已實現 ${session_pnl:+.4f}｜完成 {session_count} 筆｜"
            f"勝率 {self._win_rate(session_wins, session_count)}",
            f"資料庫累計：已實現 ${stats['pnl']:+.4f}｜完成 {stats['completed']} 筆｜"
            f"勝率 {self._win_rate(stats['wins'], stats['completed'])}",
            f"目前交易：未平倉 {stats['open']}｜未解決 {stats['unresolved']}｜未建立持倉 {stats['no_position']}",
        ]
        if self.trading_mode == "live":
            try:
                if self.collateral_balance_provider is None:
                    raise RuntimeError("live collateral balance provider is unavailable")
                balance = await self.collateral_balance_provider()
                lines.append(f"Collateral balance (pUSD)：${balance:,.6f}")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                lines.append("Collateral balance (pUSD)：暫時讀取失敗")
                print(f"DISCORD BALANCE READ ERROR ({type(exc).__name__}); will retry", flush=True)
        lines.append(mode_line)
        message = "\n".join(lines)
        response = await self.client.post(self.webhook_url, json={"content": message})
        if not response.is_success:
            print(f"DISCORD NOTIFY ERROR (HTTP {response.status_code}); will retry", flush=True)

    def _stats(self) -> dict[str, float | int]:
        rows = self.store.shadow_trade_rows()
        completed = [
            row for row in rows
            if row["status"] in {"CLOSED", "SETTLED", "NO_PAIR", "NO_POSITION"}
            and row["net_pnl"] is not None
        ]
        pnls = [float(row["net_pnl"]) for row in completed]
        return {
            "pnl": sum(pnls),
            "completed": len(completed),
            "wins": sum(pnl > 0 for pnl in pnls),
            "open": sum(row["status"] == "OPEN" for row in rows),
            "unresolved": sum(str(row["status"]).startswith("UNRESOLVED") for row in rows),
            "no_position": sum(row["status"] in {"NO_PAIR", "NO_POSITION"} for row in rows),
        }

    @staticmethod
    def _win_rate(wins: int, completed: int) -> str:
        return "n/a" if completed == 0 else f"{wins / completed:.1%}"
