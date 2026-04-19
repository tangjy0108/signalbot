from __future__ import annotations

import os
import sqlite3


DB_PATH = os.getenv("BOT_DB_PATH", "live_s5_bot.db")


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def ensure_position_columns(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "positions"):
        return
    existing = {row[1] for row in conn.execute("PRAGMA table_info(positions)").fetchall()}
    columns = {
        "strategy_id": "TEXT NOT NULL DEFAULT 's5'",
        "strategy_name": "TEXT NOT NULL DEFAULT 'S5'",
        "setup_session": "TEXT",
        "setup_type": "TEXT",
    }
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE positions ADD COLUMN {name} {ddl}")
    conn.commit()


def print_rows(title: str, headers: list[str], rows: list[sqlite3.Row]) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    if not rows:
        print("no rows")
        return

    widths = [len(h) for h in headers]
    data = []
    for row in rows:
        values = [str(row[h]) for h in headers]
        data.append(values)
        widths = [max(widths[i], len(values[i])) for i in range(len(headers))]

    print(" | ".join(headers[i].ljust(widths[i]) for i in range(len(headers))))
    print("-+-".join("-" * w for w in widths))
    for values in data:
        print(" | ".join(values[i].ljust(widths[i]) for i in range(len(headers))))


def main() -> None:
    if not os.path.exists(DB_PATH):
        print(f"DB not found: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_position_columns(conn)

    if not table_exists(conn, "positions"):
        print(f"No positions table in DB: {DB_PATH}")
        return

    closed = conn.execute(
        """
        SELECT
            symbol,
            strategy_id,
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins,
            ROUND(100.0 * SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate_pct,
            ROUND(SUM(pnl_usdt), 4) AS pnl_usdt,
            ROUND(AVG(pnl_usdt), 4) AS avg_pnl_usdt
        FROM positions
        WHERE status='CLOSED'
        GROUP BY symbol, strategy_id
        ORDER BY symbol, strategy_id
        """
    ).fetchall()

    today = conn.execute(
        """
        SELECT
            symbol,
            strategy_id,
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins,
            ROUND(100.0 * SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate_pct,
            ROUND(SUM(pnl_usdt), 4) AS pnl_usdt
        FROM positions
        WHERE status='CLOSED' AND substr(exit_time_utc,1,10)=date('now')
        GROUP BY symbol, strategy_id
        ORDER BY symbol, strategy_id
        """
    ).fetchall()

    open_positions = conn.execute(
        """
        SELECT
            id,
            symbol,
            strategy_id,
            side,
            ROUND(entry_price, 6) AS entry_price,
            ROUND(stop_price, 6) AS stop_price,
            ROUND(tp_price, 6) AS tp_price,
            entry_time_utc,
            setup_session,
            setup_type
        FROM positions
        WHERE status='OPEN'
        ORDER BY symbol, strategy_id, id
        """
    ).fetchall()

    print(f"DB: {DB_PATH}")
    print_rows(
        "All Closed Trades",
        ["symbol", "strategy_id", "trades", "wins", "win_rate_pct", "pnl_usdt", "avg_pnl_usdt"],
        closed,
    )
    print_rows(
        "Today Closed Trades",
        ["symbol", "strategy_id", "trades", "wins", "win_rate_pct", "pnl_usdt"],
        today,
    )
    print_rows(
        "Open Positions",
        [
            "id",
            "symbol",
            "strategy_id",
            "side",
            "entry_price",
            "stop_price",
            "tp_price",
            "entry_time_utc",
            "setup_session",
            "setup_type",
        ],
        open_positions,
    )


if __name__ == "__main__":
    main()
