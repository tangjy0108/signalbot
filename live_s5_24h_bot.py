+from __future__ import annotations
+
+import os
+import time
+import sqlite3
+from dataclasses import dataclass
+from datetime import datetime, timezone
+from zoneinfo import ZoneInfo
+from typing import Optional
+
+import pandas as pd
+import requests
+
+from research import combined_signals
+from live_practical_session_report import apply_slippage
+
+
+BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
+
+S5_PARAMS = {
+    "lookback": 10,
+    "fvg_window": 20,
+    "rsi_period": 10,
+    "rsi_ob": 65,
+    "rsi_os": 35,
+    "atr_mult_sl": 1.0,
+    "rr_ratio": 1.5,  # best balance from current RR sweep
+}
+
+
+@dataclass
+class Config:
+    symbol: str = os.getenv("BOT_SYMBOL", "BTCUSDT")
+    interval: str = os.getenv("BOT_INTERVAL", "15m")
+    kline_limit: int = int(os.getenv("BOT_KLINE_LIMIT", "600"))
+    db_path: str = os.getenv("BOT_DB_PATH", "live_s5_bot.db")
+    loop_seconds: int = int(os.getenv("BOT_LOOP_SECONDS", "20"))
+    heartbeat_minutes: int = int(os.getenv("BOT_HEARTBEAT_MINUTES", "60"))
+
+    # paper-trade assumptions
+    notional_usdt: float = float(os.getenv("BOT_NOTIONAL_USDT", "200"))
+    fee_per_side: float = float(os.getenv("BOT_FEE_PER_SIDE", "0.0004"))
+    slippage_per_side: float = float(os.getenv("BOT_SLIPPAGE_PER_SIDE", "0.0002"))
+
+    # optional Telegram
+    tg_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
+    tg_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
+
+
+def now_utc() -> datetime:
+    return [datetime.now](http://datetime.now)(timezone.utc)
+
+
+def fmt_ts(dt: datetime) -> str:
+    tw = dt.astimezone(ZoneInfo("Asia/Taipei"))
+    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} UTC / {tw.strftime('%Y-%m-%d %H:%M:%S')} Asia/Taipei"
+
+
+def send_telegram(cfg: Config, msg: str) -> None:
+    if not [cfg.tg](http://cfg.tg)_token or not [cfg.tg](http://cfg.tg)_chat_id:
+        return
+    try:
+        url = f"https://api.telegram.org/bot{cfg.tg_token}/sendMessage"
+        [requests.post](http://requests.post)(url, json={"chat_id": [cfg.tg](http://cfg.tg)_chat_id, "text": msg}, timeout=15)
+    except Exception:
+        # keep bot running even if notify fails
+        pass
+
+
+def init_db(conn: sqlite3.Connection) -> None:
+    conn.execute(
+        """
+        CREATE TABLE IF NOT EXISTS bot_state (
+            k TEXT PRIMARY KEY,
+            v TEXT NOT NULL
+        )
+        """
+    )
+    conn.execute(
+        """
+        CREATE TABLE IF NOT EXISTS positions (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            symbol TEXT NOT NULL,
+            side TEXT NOT NULL,
+            entry_time_utc TEXT NOT NULL,
+            entry_price REAL NOT NULL,
+            stop_price REAL NOT NULL,
+            tp_price REAL NOT NULL,
+            signal_bar_time_utc TEXT NOT NULL,
+            status TEXT NOT NULL,
+            exit_time_utc TEXT,
+            exit_price REAL,
+            exit_reason TEXT,
+            pnl_usdt REAL
+        )
+        """
+    )
+    conn.execute(
+        """
+        CREATE TABLE IF NOT EXISTS events (
+            id INTEGER PRIMARY KEY AUTOINCREMENT,
+            event_time_utc TEXT NOT NULL,
+            level TEXT NOT NULL,
+            message TEXT NOT NULL
+        )
+        """
+    )
+    conn.commit()
+
+
+def db_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
+    row = conn.execute("SELECT v FROM bot_state WHERE k=?", (key,)).fetchone()
+    return row[0] if row else default
+
+
+def db_set(conn: sqlite3.Connection, key: str, value: str) -> None:
+    conn.execute(
+        "INSERT INTO bot_state(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
+        (key, value),
+    )
+    conn.commit()
+
+
+def log_event(conn: sqlite3.Connection, level: str, message: str) -> None:
+    conn.execute(
+        "INSERT INTO events(event_time_utc, level, message) VALUES(?, ?, ?)",
+        (now_utc().isoformat(), level, message),
+    )
+    conn.commit()
+
+
+def fetch_klines(cfg: Config) -> pd.DataFrame:
+    r = requests.get(
+        BINANCE_KLINES_URL,
+        params={"symbol": cfg.symbol, "interval": cfg.interval, "limit": cfg.kline_limit},
+        timeout=20,
+    )
+    r.raise_for_status()
+    raw = r.json()
+    df = pd.DataFrame(
+        raw,
+        columns=[
+            "open_time",
+            "open",
+            "high",
+            "low",
+            "close",
+            "volume",
+            "close_time",
+            "quote_asset_volume",
+            "num_trades",
+            "taker_buy_base",
+            "taker_buy_quote",
+            "ignore",
+        ],
+    )
+    for c in ["open", "high", "low", "close", "volume"]:
+        df[c] = [pd.to](http://pd.to)_numeric(df[c], errors="coerce")
+    df["open_time"] = [pd.to](http://pd.to)_numeric(df["open_time"], errors="coerce")
+    df["timestamp"] = [pd.to](http://pd.to)_datetime(df["open_time"], unit="ms", utc=True)
+    df = df[["timestamp", "open", "high", "low", "close", "volume"]].dropna()
+    return df.set_index("timestamp").sort_index()
+
+
+def valid_entry_prices(direction: int, entry_px: float, stop_px: float, tp_px: float) -> bool:
+    if entry_px <= 0 or stop_px <= 0 or tp_px <= 0:
+        return False
+    if direction == 1:
+        return entry_px > stop_px and tp_px > entry_px
+    return entry_px < stop_px and tp_px < entry_px
+
+
+def recompute_tp_from_entry(direction: int, entry_px: float, stop_px: float, rr_ratio: float) -> float:
+    if direction == 1:
+        risk = entry_px - stop_px
+        return entry_px + rr_ratio * risk
+    risk = stop_px - entry_px
+    return entry_px - rr_ratio * risk
+
+
+def get_open_position(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
+    conn.row_factory = sqlite3.Row
+    row = conn.execute(
+        "SELECT * FROM positions WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
+    ).fetchone()
+    conn.row_factory = None
+    return row
+
+
+def open_position(
+    conn: sqlite3.Connection,
+    cfg: Config,
+    side: str,
+    entry_time: datetime,
+    entry_price: float,
+    stop_price: float,
+    tp_price: float,
+    signal_bar_time: datetime,
+) -> None:
+    conn.execute(
+        """
+        INSERT INTO positions(
+            symbol, side, entry_time_utc, entry_price, stop_price, tp_price, signal_bar_time_utc, status
+        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
+        """,
+        (
+            cfg.symbol,
+            side,
+            entry_time.isoformat(),
+            float(entry_price),
+            float(stop_price),
+            float(tp_price),
+            signal_bar_time.isoformat(),
+        ),
+    )
+    conn.commit()
+
+
+def close_position(
+    conn: sqlite3.Connection,
+    cfg: Config,
+    pos_id: int,
+    side: str,
+    entry_price: float,
+    exit_price: float,
+    exit_reason: str,
+) -> float:
+    if side == "long":
+        gross_ret = (exit_price - entry_price) / entry_price
+    else:
+        gross_ret = (entry_price - exit_price) / entry_price
+    net_ret = gross_ret - 2 * cfg.fee_per_side
+    pnl = cfg.notional_usdt * net_ret
+    conn.execute(
+        """
+        UPDATE positions
+           SET status='CLOSED', exit_time_utc=?, exit_price=?, exit_reason=?, pnl_usdt=?
+         WHERE id=?
+        """,
+        (now_utc().isoformat(), float(exit_price), exit_reason, float(pnl), pos_id),
+    )
+    conn.commit()
+    return pnl
+
+
+def main():
+    cfg = Config()
+    conn = sqlite3.connect(cfg.db_path)
+    init_db(conn)
+    log_event(conn, "INFO", "bot started")
+    send_telegram(cfg, f"❤️ S5 機器人啟動\n標的: {cfg.symbol}\n週期: {cfg.interval}\n時間: {fmt_ts(now_utc())}")
+
+    while True:
+        try:
+            df = fetch_klines(cfg)
+            if len(df) < 220:
+                time.sleep(cfg.loop_seconds)
+                continue
+
+            # Binance returns the current in-progress candle as the last row.
+            closed_idx = len(df) - 2
+            open_idx = len(df) - 1
+            closed_ts = df.index[closed_idx].to_pydatetime()
+            current_open_ts = df.index[open_idx].to_pydatetime()
+
+            # 1) manage current open position using latest CLOSED candle range.
+            pos = get_open_position(conn)
+            if pos is not None:
+                side = pos["side"]
+                stop_px = float(pos["stop_price"])
+                tp_px = float(pos["tp_price"])
+                entry_px = float(pos["entry_price"])
+
+                low = float(df["low"].iloc[closed_idx])
+                high = float(df["high"].iloc[closed_idx])
+
+                raw_exit = None
+                reason = None
+                if side == "long":
+                    hit_sl = low <= stop_px
+                    hit_tp = high >= tp_px
+                    if hit_sl and hit_tp:
+                        raw_exit, reason = stop_px, "both_hit_stop_first"
+                    elif hit_sl:
+                        raw_exit, reason = stop_px, "stop"
+                    elif hit_tp:
+                        raw_exit, reason = tp_px, "tp"
+                else:
+                    hit_sl = high >= stop_px
+                    hit_tp = low <= tp_px
+                    if hit_sl and hit_tp:
+                        raw_exit, reason = stop_px, "both_hit_stop_first"
+                    elif hit_sl:
+                        raw_exit, reason = stop_px, "stop"
+                    elif hit_tp:
+                        raw_exit, reason = tp_px, "tp"
+
+                if raw_exit is not None:
+                    direction = 1 if side == "long" else -1
+                    exit_px = apply_slippage(raw_exit, direction, is_entry=False)
+                    pnl = close_position(conn, cfg, int(pos["id"]), side, entry_px, exit_px, reason)
+                    msg = (
+                        f"💰 平倉\n"
+                        f"幣種: {cfg.symbol}\n"
+                        f"方向: {'做多' if side == 'long' else '做空'}\n"
+                        f"原因: {reason}\n"
+                        f"進場: {entry_px:.4f}\n"
+                        f"出場: {exit_px:.4f}\n"
+                        f"盈虧: {pnl:+.2f} USDT\n"
+                        f"時間: {fmt_ts(now_utc())}"
+                    )
+                    log_event(conn, "TRADE_EXIT", msg)
+                    send_telegram(cfg, msg)
+
+            # 2) open new position when flat and there's a new closed bar signal.
+            pos = get_open_position(conn)
+            last_processed_signal_ts = db_get(conn, "last_signal_bar_utc", "")
+            closed_ts_iso = closed_ts.replace(tzinfo=timezone.utc).isoformat()
+
+            if pos is None and last_processed_signal_ts != closed_ts_iso:
+                signals = combined_signals(df, S5_PARAMS)
+                # combined_signals returns bar=i+1 (execute on next bar open),
+                # so on live stream we should match the current open bar index.
+                target = [s for s in signals if int(s["bar"]) == open_idx]
+                if target:
+                    s = target[0]
+                    direction = int(s["direction"])
+                    side = "long" if direction == 1 else "short"
+                    entry_raw = float(df["open"].iloc[open_idx])
+                    entry_px = apply_slippage(entry_raw, direction, is_entry=True)
+                    stop_px = float(s["stop"])
+                    signal_tp_px = float(s["tp"])
+                    signal_entry_px = float(s.get("entry", entry_raw))
+                    # Keep RR consistent with *actual* entry fill, not signal candle close.
+                    tp_px = recompute_tp_from_entry(direction, entry_px, stop_px, S5_PARAMS["rr_ratio"])
+
+                    if valid_entry_prices(direction, entry_px, stop_px, tp_px):
+                        open_position(
+                            conn=conn,
+                            cfg=cfg,
+                            side=side,
+                            entry_time=current_open_ts,
+                            entry_price=entry_px,
+                            stop_price=stop_px,
+                            tp_price=tp_px,
+                            signal_bar_time=closed_ts,
+                        )
+                        msg = (
+                            f"✅ 開倉成功\n"
+                            f"幣種: {cfg.symbol}\n"
+                            f"方向: {'做多' if side == 'long' else '做空'}\n"
+                            f"進場價: {entry_px:.4f}\n"
+                            f"止損價: {stop_px:.4f}\n"
+                            f"止盈價: {tp_px:.4f}\n"
+                            f"策略: S5 (BOS+FVG+RSI, RR=1.5)\n"
+                            f"時間: {fmt_ts(now_utc())}"
+                        )
+                        log_event(conn, "TRADE_ENTRY", msg)
+                        send_telegram(cfg, msg)
+                        log_event(
+                            conn,
+                            "ENTRY_DEBUG",
+                            (
+                                f"signal_entry={signal_entry_px:.6f} live_entry={entry_px:.6f} "
+                                f"entry_drift_pct={(entry_px / (signal_entry_px + 1e-12) - 1) * 100:.3f}% "
+                                f"signal_tp={signal_tp_px:.6f} live_tp={tp_px:.6f}"
+                            ),
+                        )
+                    else:
+                        log_event(
+                            conn,
+                            "SIGNAL_SKIP",
+                            f"skip invalid price relation symbol={cfg.symbol} dir={direction} entry={entry_px:.6f} stop={stop_px:.6f} tp={tp_px:.6f}",
+                        )
+
+                db_set(conn, "last_signal_bar_utc", closed_ts_iso)
+
+            # 3) heartbeat
+            last_hb = db_get(conn, "last_heartbeat_utc", "")
+            send_hb = True
+            if last_hb:
+                last_dt = datetime.fromisoformat(last_hb)
+                mins = (now_utc() - last_dt).total_seconds() / 60
+                send_hb = mins >= cfg.heartbeat_minutes
+
+            if send_hb:
+                conn.row_factory = sqlite3.Row
+                today = now_utc().date().isoformat()
+                row = conn.execute(
+                    """
+                    SELECT
+                        COALESCE(SUM(pnl_usdt), 0) AS pnl,
+                        COUNT(*) AS trades,
+                        COALESCE(SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END), 0) AS wins
+                    FROM positions
+                    WHERE status='CLOSED' AND substr(exit_time_utc,1,10)=?
+                    """,
+                    (today,),
+                ).fetchone()
+                open_cnt = conn.execute("SELECT COUNT(*) AS c FROM positions WHERE status='OPEN'").fetchone()["c"]
+                conn.row_factory = None
+
+                trades = int(row["trades"]) if row else 0
+                wins = int(row["wins"]) if row else 0
+                win_rate = (wins / trades * 100) if trades else 0.0
+                pnl_today = float(row["pnl"]) if row else 0.0
+                hb_msg = (
+                    f"❤️ 機器人運作正常\n"
+                    f"時間: {fmt_ts(now_utc())}\n"
+                    f"今日盈虧: {pnl_today:+.2f} USDT\n"
+                    f"今日交易: {trades} 筆 (勝率 {win_rate:.1f}%)\n"
+                    f"目前持倉: {open_cnt}"
+                )
+                send_telegram(cfg, hb_msg)
+                db_set(conn, "last_heartbeat_utc", now_utc().isoformat())
+
+        except Exception as e:
+            msg = f"⚠️ bot error: {type(e).__name__} {e}"
+            log_event(conn, "ERROR", msg)
+            send_telegram(cfg, msg)
+
+        time.sleep(cfg.loop_seconds)
+
+
+if __name__ == "__main__":
+    main()
