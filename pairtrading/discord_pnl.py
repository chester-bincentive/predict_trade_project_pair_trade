from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx

from .storage import Store


class DiscordPnLNotifier:
    """Periodically post realized shadow PnL to a Discord incoming webhook."""

    def __init__(self, store: Store, webhook_url: str, interval_seconds: int) -> None:
        self.store = store
        self.webhook_url = webhook_url
        self.interval_seconds = interval_seconds
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0))
        self.baseline = self._stats()

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
        await self.client.aclose()

    async def _send_summary(self) -> None:
        stats = self._stats()
        session_count = max(0, stats["completed"] - self.baseline["completed"])
        session_wins = max(0, stats["wins"] - self.baseline["wins"])
        session_pnl = stats["pnl"] - self.baseline["pnl"]
        message = "\n".join(
            (
                "**BTC 5 分鐘 Polymarket 單腿 Shadow 損益**",
                f"更新時間（UTC）：{datetime.now(timezone.utc).isoformat(timespec='seconds')}",
                f"本次啟動後：已實現 ${session_pnl:+.4f}｜完成 {session_count} 筆｜"
                f"勝率 {self._win_rate(session_wins, session_count)}",
                f"資料庫累計：已實現 ${stats['pnl']:+.4f}｜完成 {stats['completed']} 筆｜"
                f"勝率 {self._win_rate(stats['wins'], stats['completed'])}",
                f"目前交易：未平倉 {stats['open']}｜未解決 {stats['unresolved']}｜未建立持倉 {stats['no_position']}",
                "Shadow mode；不會送出真實訂單。",
            )
        )
        response = await self.client.post(self.webhook_url, json={"content": message})
        if not response.is_success:
            print(f"DISCORD NOTIFY ERROR (HTTP {response.status_code}); will retry", flush=True)

    def _stats(self) -> dict[str, float | int]:
        rows = self.store.shadow_trade_rows()
        completed = [
            row for row in rows
            if row["status"] in {"CLOSED", "NO_PAIR", "NO_POSITION"} and row["net_pnl"] is not None
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
