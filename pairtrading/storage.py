from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS quote_ticks (
                observed_at REAL NOT NULL,
                market_key TEXT NOT NULL,
                elapsed_seconds REAL NOT NULL,
                bucket INTEGER NOT NULL,
                poly_up_mid REAL,
                binance_up_mid REAL,
                spread REAL,
                poly_bid REAL, poly_ask REAL, binance_bid REAL, binance_ask REAL,
                poly_bid_size REAL, poly_ask_size REAL, binance_bid_size REAL, binance_ask_size REAL,
                source_skew_seconds REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS quote_ticks_market_time ON quote_ticks(market_key, observed_at);
            CREATE TABLE IF NOT EXISTS bucket_history (
                market_key TEXT NOT NULL,
                market_end REAL NOT NULL,
                bucket INTEGER NOT NULL,
                median_spread REAL NOT NULL,
                sample_count INTEGER NOT NULL,
                PRIMARY KEY(market_key, bucket)
            );
            CREATE INDEX IF NOT EXISTS bucket_history_window ON bucket_history(bucket, market_end DESC);
            CREATE TABLE IF NOT EXISTS bucket_rv_history (
                market_key TEXT NOT NULL,
                market_end REAL NOT NULL,
                bucket INTEGER NOT NULL,
                median_rv10 REAL NOT NULL,
                sample_count INTEGER NOT NULL,
                PRIMARY KEY(market_key, bucket)
            );
            CREATE INDEX IF NOT EXISTS bucket_rv_history_window
                ON bucket_rv_history(bucket, market_end DESC);
            CREATE TABLE IF NOT EXISTS app_metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_positions (
                market_key TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                payload TEXT NOT NULL,
                opened_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_trades (
                market_key TEXT PRIMARY KEY,
                signal TEXT NOT NULL,
                zscore REAL NOT NULL,
                bucket INTEGER NOT NULL,
                history_markets INTEGER NOT NULL,
                shares REAL NOT NULL,
                entered_at TEXT NOT NULL,
                exited_at TEXT NOT NULL,
                hold_seconds REAL NOT NULL,
                entry_cost REAL NOT NULL,
                entry_fee REAL NOT NULL,
                exit_value REAL NOT NULL,
                exit_fee REAL NOT NULL,
                net_pnl REAL NOT NULL,
                exit_type TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS book_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sampled_at REAL NOT NULL, market_key TEXT NOT NULL,
                venue TEXT NOT NULL, outcome TEXT NOT NULL, token_id TEXT NOT NULL,
                request_started_at REAL, received_at REAL NOT NULL, source_at REAL,
                request_rtt_ms REAL, source_age_ms REAL NOT NULL,
                best_bid REAL, best_bid_size REAL, best_ask REAL, best_ask_size REAL,
                start_skew_seconds REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS book_snapshots_market_time
                ON book_snapshots(market_key, sampled_at, venue, outcome);
            CREATE TABLE IF NOT EXISTS book_levels (
                snapshot_id INTEGER NOT NULL REFERENCES book_snapshots(id),
                side TEXT NOT NULL, level_no INTEGER NOT NULL,
                price REAL NOT NULL, size REAL NOT NULL,
                PRIMARY KEY(snapshot_id, side, level_no)
            );
            CREATE TABLE IF NOT EXISTS shadow_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, market_key TEXT NOT NULL, scenario_key TEXT NOT NULL,
                signal TEXT NOT NULL, decision_at TEXT NOT NULL,
                decision_epoch REAL NOT NULL, bucket INTEGER NOT NULL, zscore REAL NOT NULL,
                history_markets INTEGER NOT NULL, requested_shares REAL NOT NULL,
                status TEXT NOT NULL, paired_shares REAL NOT NULL DEFAULT 0,
                entry_cost REAL NOT NULL DEFAULT 0, entry_fee REAL NOT NULL DEFAULT 0,
                exit_value REAL NOT NULL DEFAULT 0, exit_fee REAL NOT NULL DEFAULT 0,
                entry_legs_json TEXT NOT NULL DEFAULT '[]',
                exit_legs_json TEXT NOT NULL DEFAULT '[]',
                net_pnl REAL, closed_at TEXT, payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS shadow_trades_status ON shadow_trades(status, decision_epoch);
            CREATE TABLE IF NOT EXISTS shadow_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL REFERENCES shadow_trades(id),
                event_at TEXT NOT NULL, phase TEXT NOT NULL, venue TEXT, outcome TEXT, side TEXT,
                snapshot_id INTEGER, requested_shares REAL, filled_shares REAL,
                vwap REAL, notional REAL, fee REAL,
                request_started_at REAL, book_received_at REAL, book_source_at REAL,
                request_rtt_ms REAL, source_age_ms REAL,
                network_delay_ms REAL, api_delay_ms REAL, jitter_ms REAL,
                order_created_at REAL, order_arrive_at REAL, status TEXT NOT NULL,
                details_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS shadow_events_trade ON shadow_events(trade_id, id);
            CREATE TABLE IF NOT EXISTS runtime_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL, event_at TEXT NOT NULL, event_type TEXT NOT NULL,
                market_key TEXT, venue TEXT, details_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS runtime_events_run_time ON runtime_events(run_id, id);
            """
        )
        trade_columns = {row["name"] for row in self.db.execute("PRAGMA table_info(shadow_trades)")}
        if "scenario_key" not in trade_columns:
            self.db.execute("ALTER TABLE shadow_trades ADD COLUMN scenario_key TEXT NOT NULL DEFAULT 'legacy'")
        if "run_id" not in trade_columns:
            self.db.execute("ALTER TABLE shadow_trades ADD COLUMN run_id TEXT")
        if "entry_legs_json" not in trade_columns:
            self.db.execute("ALTER TABLE shadow_trades ADD COLUMN entry_legs_json TEXT NOT NULL DEFAULT '[]'")
        if "exit_legs_json" not in trade_columns:
            self.db.execute("ALTER TABLE shadow_trades ADD COLUMN exit_legs_json TEXT NOT NULL DEFAULT '[]'")
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def record_quote(self, row: dict[str, Any]) -> None:
        self.db.execute(
            """INSERT INTO quote_ticks VALUES
            (:observed_at,:market_key,:elapsed_seconds,:bucket,:poly_up_mid,:binance_up_mid,:spread,
             :poly_bid,:poly_ask,:binance_bid,:binance_ask,:poly_bid_size,:poly_ask_size,
             :binance_bid_size,:binance_ask_size,:source_skew_seconds)""",
            row,
        )
        self.db.commit()

    def record_orderbooks(self, snapshot: Any) -> dict[tuple[str, str], int]:
        ids: dict[tuple[str, str], int] = {}
        with self.db:
            for key, book in snapshot.books.items():
                bid, ask = book.best_bid, book.best_ask
                cursor = self.db.execute(
                    """INSERT INTO book_snapshots
                    (sampled_at,market_key,venue,outcome,token_id,request_started_at,received_at,
                     source_at,request_rtt_ms,source_age_ms,best_bid,best_bid_size,best_ask,
                     best_ask_size,start_skew_seconds)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (snapshot.captured_at.timestamp(), snapshot.market_key, book.venue, book.outcome,
                     book.token_id,
                     None if book.request_started_at is None else book.request_started_at.timestamp(),
                     book.received_at.timestamp(), None if book.source_at is None else book.source_at.timestamp(),
                     book.request_rtt_ms, book.age_seconds(book.received_at) * 1000.0,
                     None if bid is None else bid.price, None if bid is None else bid.size,
                     None if ask is None else ask.price, None if ask is None else ask.size,
                     snapshot.start_skew_seconds),
                )
                snapshot_id = int(cursor.lastrowid)
                ids[key] = snapshot_id
                self.db.executemany(
                    "INSERT INTO book_levels(snapshot_id,side,level_no,price,size) VALUES (?,?,?,?,?)",
                    [(snapshot_id, side, index, level.price, level.size)
                     for side, levels in (("BID", book.bids), ("ASK", book.asks))
                     for index, level in enumerate(levels)],
                )
        return ids

    def create_shadow_trade(self, payload: dict[str, Any]) -> int:
        with self.db:
            cursor = self.db.execute(
                """INSERT INTO shadow_trades
                (run_id,market_key,scenario_key,signal,decision_at,decision_epoch,bucket,zscore,history_markets,
                 requested_shares,status,paired_shares,entry_legs_json,exit_legs_json,payload_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,'OPEN',0,'[]','[]',?)""",
                (payload.get("run_id"), payload["market_key"], payload["scenario_key"], payload["signal"],
                 payload["decision_at"], payload["decision_epoch"], payload["bucket"], payload["zscore"],
                 payload["history_markets"], payload["requested_shares"],
                 json.dumps(payload, separators=(",", ":"))),
            )
        return int(cursor.lastrowid)

    def update_shadow_trade(self, payload: dict[str, Any]) -> None:
        entry_legs = self._trade_leg_fills(payload, "entry")
        exit_legs = self._trade_leg_fills(payload, "exit")
        with self.db:
            self.db.execute(
                """UPDATE shadow_trades SET status=?,paired_shares=?,entry_cost=?,entry_fee=?,
                exit_value=?,exit_fee=?,entry_legs_json=?,exit_legs_json=?,net_pnl=?,closed_at=?,
                payload_json=? WHERE id=?""",
                (payload["status"], payload.get("paired_shares", 0), payload.get("entry_cost", 0),
                 payload.get("entry_fee", 0), payload.get("exit_value", 0), payload.get("exit_fee", 0),
                 json.dumps(entry_legs, separators=(",", ":")),
                 json.dumps(exit_legs, separators=(",", ":")),
                 payload.get("net_pnl"), payload.get("closed_at"),
                 json.dumps(payload, separators=(",", ":")), payload["trade_id"]),
            )

    @staticmethod
    def _trade_leg_fills(payload: dict[str, Any], phase: str) -> list[dict[str, Any]]:
        rows = []
        for leg in payload.get("legs", []):
            if phase == "entry":
                filled = float(leg.get("entry_filled", 0.0))
                notional = float(leg.get("entry_cost", 0.0))
                fee = float(leg.get("entry_fee", 0.0))
            else:
                filled = float(leg.get("exit_filled", 0.0))
                notional = float(leg.get("exit_value", 0.0))
                fee = float(leg.get("exit_fee", 0.0))
            rows.append({
                "venue": leg.get("venue"), "outcome": leg.get("outcome"),
                "requested_shares": (float(payload.get("requested_shares", 0.0)) if phase == "entry"
                                     else float(leg.get("entry_filled", 0.0))),
                "filled_shares": filled,
                "vwap": None if filled <= 1e-12 else notional / filled,
                "notional": notional, "fee": fee,
            })
        return rows

    def record_shadow_event(self, row: dict[str, Any]) -> None:
        fields = ("trade_id", "event_at", "phase", "venue", "outcome", "side", "snapshot_id",
                  "requested_shares", "filled_shares", "vwap", "notional", "fee",
                  "request_started_at", "book_received_at", "book_source_at", "request_rtt_ms",
                  "source_age_ms", "network_delay_ms", "api_delay_ms", "jitter_ms",
                  "order_created_at", "order_arrive_at", "status", "details_json")
        with self.db:
            self.db.execute(
                f"INSERT INTO shadow_events ({','.join(fields)}) VALUES ({','.join('?' for _ in fields)})",
                tuple(row.get(field) for field in fields),
            )

    def record_runtime_event(self, *, run_id: str, event_type: str,
                             market_key: str | None = None, venue: str | None = None,
                             details: dict[str, Any] | None = None,
                             event_at: str | None = None) -> None:
        if event_at is None:
            from datetime import datetime, timezone
            event_at = datetime.now(timezone.utc).isoformat()
        with self.db:
            self.db.execute(
                """INSERT INTO runtime_events
                   (run_id,event_at,event_type,market_key,venue,details_json)
                   VALUES (?,?,?,?,?,?)""",
                (run_id, event_at, event_type, market_key, venue,
                 json.dumps(details or {}, separators=(",", ":"))),
            )

    def open_shadow_trades(self) -> list[dict[str, Any]]:
        rows = self.db.execute(
            "SELECT payload_json FROM shadow_trades WHERE status='OPEN' ORDER BY id"
        ).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]

    def has_shadow_trade(self, market_key: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM shadow_trades WHERE market_key=? LIMIT 1", (market_key,)
        ).fetchone() is not None

    def shadow_trade_rows(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT scenario_key,status,paired_shares,requested_shares,net_pnl,decision_epoch "
            "FROM shadow_trades ORDER BY id"
        ).fetchall()

    def latency_summary(self) -> tuple[int, int, float | None, float | None, float | None, float | None, int]:
        row = self.db.execute(
            "SELECT COUNT(*),AVG(request_rtt_ms),AVG(CASE WHEN source_at IS NOT NULL THEN source_age_ms END) "
            "FROM book_snapshots"
        ).fetchone()
        rtt_count = int(self.db.execute(
            "SELECT COUNT(*) FROM book_snapshots WHERE request_rtt_ms IS NOT NULL"
        ).fetchone()[0])
        rtt_median = None if not rtt_count else self.db.execute(
            "SELECT request_rtt_ms FROM book_snapshots WHERE request_rtt_ms IS NOT NULL "
            "ORDER BY request_rtt_ms LIMIT 1 OFFSET ?", ((rtt_count - 1) // 2,)
        ).fetchone()[0]
        age_count = int(self.db.execute(
            "SELECT COUNT(*) FROM book_snapshots WHERE source_at IS NOT NULL"
        ).fetchone()[0])
        age_median = None if not age_count else self.db.execute(
            "SELECT source_age_ms FROM book_snapshots WHERE source_at IS NOT NULL "
            "ORDER BY source_age_ms LIMIT 1 OFFSET ?",
            ((age_count - 1) // 2,),
        ).fetchone()[0]
        return (int(row[0]), rtt_count, row[1], rtt_median, row[2], age_median, age_count)

    def orderbook_storage_stats(self) -> tuple[int, int, int]:
        snapshots = int(self.db.execute("SELECT COUNT(*) FROM book_snapshots").fetchone()[0])
        levels = int(self.db.execute("SELECT COUNT(*) FROM book_levels").fetchone()[0])
        events = int(self.db.execute("SELECT COUNT(*) FROM shadow_events").fetchone()[0])
        return snapshots, levels, events

    def save_history(self, market_key: str, market_end: float, bucket: int, median: float, sample_count: int) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO bucket_history VALUES (?,?,?,?,?)",
            (market_key, market_end, bucket, median, sample_count),
        )
        self.db.commit()

    def history(self, bucket: int, *, before_market_start: float, limit: int) -> list[tuple[str, float, float]]:
        rows = self.db.execute(
            """SELECT market_key, market_end, median_spread FROM bucket_history
               WHERE bucket=? AND market_end<=? ORDER BY market_end DESC LIMIT ?""",
            (bucket, before_market_start, limit),
        ).fetchall()
        return [(str(row["market_key"]), float(row["market_end"]), float(row["median_spread"])) for row in rows]

    def save_rv_history(self, market_key: str, market_end: float, bucket: int,
                        median_rv10: float, sample_count: int) -> None:
        self.db.execute(
            "INSERT OR IGNORE INTO bucket_rv_history VALUES (?,?,?,?,?)",
            (market_key, market_end, bucket, median_rv10, sample_count),
        )
        self.db.commit()

    def save_rv_history_batch(self, rows: list[tuple[str, float, int, float, int]]) -> None:
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO bucket_rv_history VALUES (?,?,?,?,?)", rows
            )

    def prune_calibration_history(self, *, spread_window: int, rv_window: int) -> None:
        """Keep only the bounded per-market summaries used by the rolling strategy."""
        with self.db:
            for table, limit in (("bucket_history", spread_window),
                                 ("bucket_rv_history", rv_window)):
                if limit < 1:
                    continue
                buckets = [row[0] for row in self.db.execute(
                    f"SELECT DISTINCT bucket FROM {table}"
                ).fetchall()]
                for bucket in buckets:
                    self.db.execute(
                        f"""DELETE FROM {table} WHERE bucket=? AND market_key NOT IN (
                               SELECT market_key FROM {table} WHERE bucket=?
                               ORDER BY market_end DESC, market_key DESC LIMIT ?
                           )""",
                        (bucket, bucket, limit),
                    )

    def rv_history(self, bucket: int, *, before_market_start: float,
                   limit: int) -> list[tuple[str, float, float]]:
        rows = self.db.execute(
            """SELECT market_key, market_end, median_rv10 FROM bucket_rv_history
               WHERE bucket=? AND market_end<=? ORDER BY market_end DESC LIMIT ?""",
            (bucket, before_market_start, limit),
        ).fetchall()
        return [(str(row["market_key"]), float(row["market_end"]), float(row["median_rv10"]))
                for row in rows]

    def rv_history_status(self, *, before_market_start: float) -> list[tuple[int, int, float | None]]:
        rows = self.db.execute(
            """SELECT bucket, COUNT(DISTINCT market_key) AS markets, MAX(market_end) AS latest_end
               FROM bucket_rv_history WHERE market_end<=? GROUP BY bucket ORDER BY bucket""",
            (before_market_start,),
        ).fetchall()
        return [(int(row["bucket"]), int(row["markets"]),
                 None if row["latest_end"] is None else float(row["latest_end"])) for row in rows]

    def quote_markets_without_rv_history(self) -> list[str]:
        rows = self.db.execute(
            """SELECT DISTINCT q.market_key FROM quote_ticks q
               WHERE NOT EXISTS (
                   SELECT 1 FROM bucket_rv_history r WHERE r.market_key=q.market_key
               ) ORDER BY q.market_key"""
        ).fetchall()
        return [str(row["market_key"]) for row in rows]

    def rv_history_backfill_disabled(self) -> bool:
        row = self.db.execute(
            "SELECT value FROM app_metadata WHERE key='rv_history_backfill_disabled'"
        ).fetchone()
        return row is not None and str(row["value"]) == "1"

    def quote_rows_for_market(self, market_key: str) -> list[tuple[float, float, int, float]]:
        rows = self.db.execute(
            """SELECT observed_at, elapsed_seconds, bucket, spread FROM quote_ticks
               WHERE market_key=? AND spread IS NOT NULL ORDER BY observed_at""",
            (market_key,),
        ).fetchall()
        return [(float(row["observed_at"]), float(row["elapsed_seconds"]),
                 int(row["bucket"]), float(row["spread"])) for row in rows]

    def history_status(self) -> list[tuple[int, int, float | None]]:
        rows = self.db.execute(
            "SELECT bucket, COUNT(DISTINCT market_key) AS markets, MAX(market_end) AS latest_end "
            "FROM bucket_history GROUP BY bucket ORDER BY bucket"
        ).fetchall()
        return [(int(row["bucket"]), int(row["markets"]),
                 None if row["latest_end"] is None else float(row["latest_end"])) for row in rows]

    def clear_history(self) -> int:
        with self.db:
            spread_cursor = self.db.execute("DELETE FROM bucket_history")
            rv_cursor = self.db.execute("DELETE FROM bucket_rv_history")
            self.db.execute(
                "INSERT OR REPLACE INTO app_metadata(key,value) VALUES ('rv_history_backfill_disabled','1')"
            )
        deleted = int(spread_cursor.rowcount) + int(rv_cursor.rowcount)
        self.db.execute("VACUUM")
        return deleted

    def has_position_or_trade(self, market_key: str) -> bool:
        row = self.db.execute(
            "SELECT 1 FROM paper_positions WHERE market_key=? UNION SELECT 1 FROM paper_trades WHERE market_key=? LIMIT 1",
            (market_key, market_key),
        ).fetchone()
        return row is not None

    def save_position(self, market_key: str, payload: dict[str, Any], *, opened_at: float) -> None:
        now = opened_at
        self.db.execute(
            """INSERT INTO paper_positions VALUES (?, 'OPEN', ?, ?, ?)
               ON CONFLICT(market_key) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at""",
            (market_key, json.dumps(payload, separators=(",", ":")), opened_at, now),
        )
        self.db.commit()

    def open_positions(self) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT payload FROM paper_positions WHERE status='OPEN'").fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def record_trade(self, row: dict[str, Any]) -> None:
        fields = (
            "market_key", "signal", "zscore", "bucket", "history_markets", "shares", "entered_at", "exited_at",
            "hold_seconds", "entry_cost", "entry_fee", "exit_value", "exit_fee", "net_pnl", "exit_type",
        )
        placeholders = ",".join("?" for _ in fields)
        values = tuple(row[field] for field in fields)
        with self.db:
            self.db.execute(
                f"INSERT OR IGNORE INTO paper_trades ({','.join(fields)}) VALUES ({placeholders})", values
            )
            self.db.execute(
                "UPDATE paper_positions SET status='CLOSED', updated_at=? WHERE market_key=?",
                (row["exited_epoch"], row["market_key"]),
            )
