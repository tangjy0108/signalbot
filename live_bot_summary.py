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


def ensure_atm_signal_columns(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "atm_signals"):
        return
    existing = {row[1] for row in conn.execute("PRAGMA table_info(atm_signals)").fetchall()}
    columns = {
        "signal_key": "TEXT NOT NULL DEFAULT ''",
        "symbol": "TEXT NOT NULL DEFAULT ''",
        "direction": "TEXT NOT NULL DEFAULT ''",
        "interaction": "TEXT NOT NULL DEFAULT ''",
        "signal_time_utc": "TEXT NOT NULL DEFAULT ''",
        "created_time_utc": "TEXT NOT NULL DEFAULT ''",
        "entry_price": "REAL NOT NULL DEFAULT 0",
        "sl_price": "REAL NOT NULL DEFAULT 0",
        "tp1_price": "REAL NOT NULL DEFAULT 0",
        "tp2_price": "REAL NOT NULL DEFAULT 0",
        "rr_tp1": "REAL",
        "rr_tp2": "REAL",
        "status": "TEXT NOT NULL DEFAULT 'OPEN'",
        "max_favorable_stage": "INTEGER NOT NULL DEFAULT 0",
        "final_outcome": "TEXT",
        "close_reason": "TEXT",
        "close_price": "REAL",
        "closed_time_utc": "TEXT",
        "tp1_hit_time_utc": "TEXT",
        "tp1_hit_price": "REAL",
        "tp2_hit_time_utc": "TEXT",
        "tp2_hit_price": "REAL",
        "sl_hit_time_utc": "TEXT",
        "sl_hit_price": "REAL",
        "last_price": "REAL",
        "last_price_time_utc": "TEXT",
    }
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE atm_signals ADD COLUMN {name} {ddl}")
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
    ensure_atm_signal_columns(conn)

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

    ict_session_side = conn.execute(
        """
        SELECT
            COALESCE(NULLIF(TRIM(setup_session), ''), 'Unknown') AS session,
            side,
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins,
            ROUND(100.0 * SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate_pct,
            ROUND(SUM(pnl_usdt), 4) AS pnl_usdt,
            ROUND(AVG(pnl_usdt), 4) AS avg_pnl_usdt
        FROM positions
        WHERE status='CLOSED' AND strategy_id='ict_killzone_opt3'
        GROUP BY COALESCE(NULLIF(TRIM(setup_session), ''), 'Unknown'), side
        ORDER BY session, side
        """
    ).fetchall()

    s5_side = conn.execute(
        """
        SELECT
            side,
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins,
            ROUND(100.0 * SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_rate_pct,
            ROUND(SUM(pnl_usdt), 4) AS pnl_usdt,
            ROUND(AVG(pnl_usdt), 4) AS avg_pnl_usdt
        FROM positions
        WHERE status='CLOSED' AND strategy_id='s5'
        GROUP BY side
        ORDER BY side
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

    overall = conn.execute(
        """
        SELECT
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) AS wins,
            ROUND(COALESCE(SUM(pnl_usdt), 0), 4) AS total_pnl_usdt,
            ROUND(COALESCE(SUM(CASE WHEN pnl_usdt < 0 THEN pnl_usdt ELSE 0 END), 0), 4) AS total_loss_usdt,
            ROUND(COALESCE(AVG(pnl_usdt), 0), 4) AS avg_pnl_usdt
        FROM positions
        WHERE status='CLOSED'
        """
    ).fetchone()

    overall_rows = []
    if overall is not None:
        trades = int(overall["trades"] or 0)
        wins = int(overall["wins"] or 0)
        win_rate = round((wins / trades * 100), 1) if trades else 0.0
        overall_rows.append(
            {
                "trades": trades,
                "wins": wins,
                "win_rate_pct": win_rate,
                "total_pnl_usdt": overall["total_pnl_usdt"],
                "total_loss_usdt": overall["total_loss_usdt"],
                "avg_pnl_usdt": overall["avg_pnl_usdt"],
            }
        )

    atm_overall_rows = []
    atm_today_rows = []
    atm_open_rows = []
    if table_exists(conn, "atm_signals"):
        atm_overall = conn.execute(
            """
            SELECT
                COUNT(*) AS signals,
                SUM(CASE WHEN final_outcome IS NOT NULL THEN 1 ELSE 0 END) AS closed_signals,
                SUM(CASE WHEN final_outcome IN ('TP1', 'TP2') THEN 1 ELSE 0 END) AS wins,
                ROUND(
                    100.0 * SUM(CASE WHEN final_outcome IN ('TP1', 'TP2') THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN final_outcome IS NOT NULL THEN 1 ELSE 0 END), 0),
                    1
                ) AS win_rate_pct,
                SUM(CASE WHEN final_outcome='SL' THEN 1 ELSE 0 END) AS final_sl,
                SUM(CASE WHEN final_outcome='TP1' THEN 1 ELSE 0 END) AS final_tp1,
                SUM(CASE WHEN final_outcome='TP2' THEN 1 ELSE 0 END) AS final_tp2,
                SUM(CASE WHEN tp1_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_tp1,
                SUM(CASE WHEN tp2_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_tp2,
                SUM(CASE WHEN sl_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_sl,
                SUM(CASE WHEN status IN ('OPEN', 'TP1_HIT') THEN 1 ELSE 0 END) AS open_signals
            FROM atm_signals
            """
        ).fetchone()
        if atm_overall is not None and int(atm_overall["signals"] or 0) > 0:
            atm_overall_rows.append(atm_overall)

        atm_today = conn.execute(
            """
            SELECT
                COUNT(*) AS signals,
                SUM(CASE WHEN final_outcome IS NOT NULL THEN 1 ELSE 0 END) AS closed_signals,
                SUM(CASE WHEN final_outcome IN ('TP1', 'TP2') THEN 1 ELSE 0 END) AS wins,
                ROUND(
                    100.0 * SUM(CASE WHEN final_outcome IN ('TP1', 'TP2') THEN 1 ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN final_outcome IS NOT NULL THEN 1 ELSE 0 END), 0),
                    1
                ) AS win_rate_pct,
                SUM(CASE WHEN final_outcome='SL' THEN 1 ELSE 0 END) AS final_sl,
                SUM(CASE WHEN final_outcome='TP1' THEN 1 ELSE 0 END) AS final_tp1,
                SUM(CASE WHEN final_outcome='TP2' THEN 1 ELSE 0 END) AS final_tp2,
                SUM(CASE WHEN tp1_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_tp1,
                SUM(CASE WHEN tp2_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_tp2,
                SUM(CASE WHEN sl_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_sl,
                SUM(CASE WHEN status IN ('OPEN', 'TP1_HIT') THEN 1 ELSE 0 END) AS open_signals
            FROM atm_signals
            WHERE date(signal_time_utc, '+8 hours') = date('now', '+8 hours')
            """
        ).fetchone()
        if atm_today is not None and int(atm_today["signals"] or 0) > 0:
            atm_today_rows.append(atm_today)

        atm_open_rows = conn.execute(
            """
            SELECT
                id,
                symbol,
                direction,
                status,
                ROUND(entry_price, 2) AS entry_price,
                ROUND(sl_price, 2) AS sl_price,
                ROUND(tp1_price, 2) AS tp1_price,
                ROUND(tp2_price, 2) AS tp2_price,
                signal_time_utc,
                final_outcome
            FROM atm_signals
            WHERE status IN ('OPEN', 'TP1_HIT')
            ORDER BY id
            """
        ).fetchall()

    print(f"DB: {DB_PATH}")
    print_rows(
        "Overall Closed Trades",
        ["trades", "wins", "win_rate_pct", "total_pnl_usdt", "total_loss_usdt", "avg_pnl_usdt"],
        overall_rows,
    )
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
        "ICT Killzone Session x Side (Closed)",
        ["session", "side", "trades", "wins", "win_rate_pct", "pnl_usdt", "avg_pnl_usdt"],
        ict_session_side,
    )
    print_rows(
        "S5 Side Stats (Closed)",
        ["side", "trades", "wins", "win_rate_pct", "pnl_usdt", "avg_pnl_usdt"],
        s5_side,
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
    print_rows(
        "ATM Overall",
        [
            "signals",
            "closed_signals",
            "wins",
            "win_rate_pct",
            "final_sl",
            "final_tp1",
            "final_tp2",
            "hit_tp1",
            "hit_tp2",
            "hit_sl",
            "open_signals",
        ],
        atm_overall_rows,
    )
    print_rows(
        "ATM Today",
        [
            "signals",
            "closed_signals",
            "wins",
            "win_rate_pct",
            "final_sl",
            "final_tp1",
            "final_tp2",
            "hit_tp1",
            "hit_tp2",
            "hit_sl",
            "open_signals",
        ],
        atm_today_rows,
    )
    print_rows(
        "ATM Open Signals",
        [
            "id",
            "symbol",
            "direction",
            "status",
            "entry_price",
            "sl_price",
            "tp1_price",
            "tp2_price",
            "signal_time_utc",
            "final_outcome",
        ],
        atm_open_rows,
    )


if __name__ == "__main__":
    main()
