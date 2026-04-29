"""
S5 24h signal bot (paper-trade style)
-------------------------------------
- Polls exchange klines (Binance or Bitget, default: BTCUSDT 15m)
- Uses research.py::combined_signals with S5 params (rr=1.5)
- Opens/closes a simulated position and logs to SQLite
- Sends Telegram messages for entry/exit/heartbeat (optional)

This script does NOT place real exchange orders.
"""

from __future__ import annotations

import logging
import os
import time
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional

import pandas as pd
import requests

from research import combined_signals
from live_practical_session_report import apply_slippage


BINANCE_KLINES_URLS = (
    "https://api.binance.com/api/v3/klines",
    "https://data-api.binance.vision/api/v3/klines",
)
BITGET_KLINES_URL = "https://api.bitget.com/api/v2/mix/market/candles"

S5_PARAMS = {
    "lookback": 10,
    "fvg_window": 20,
    "rsi_period": 10,
    "rsi_ob": 65,
    "rsi_os": 35,
    "atr_mult_sl": 1.0,
    "rr_ratio": 1.5,  # best balance from current RR sweep
}


LOGGER = logging.getLogger("live_s5_bot")


def load_env_file(env_path: str = ".env") -> None:
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def setup_logging() -> None:
    if LOGGER.handlers:
        return

    log_level_name = os.getenv("BOT_LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)
    log_path = os.getenv("BOT_LOG_PATH", "live_s5_bot.log")

    LOGGER.setLevel(log_level)
    LOGGER.propagate = False

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    stream_handler.setFormatter(formatter)
    LOGGER.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


load_env_file()
setup_logging()


@dataclass
class Config:
    exchange: str = os.getenv("BOT_EXCHANGE", "binance").strip().lower()
    symbol: str = (
        os.getenv("BOT_SYMBOL")
        or os.getenv("BOT_SYMBOLS", "BTCUSDT").split(",")[0].strip()
        or "BTCUSDT"
    )
    interval: str = os.getenv("BOT_INTERVAL", "15m")
    kline_limit: int = int(os.getenv("BOT_KLINE_LIMIT", "600"))
    db_path: str = os.getenv("BOT_DB_PATH", "live_s5_bot.db")
    loop_seconds: int = int(os.getenv("BOT_LOOP_SECONDS", "20"))
    heartbeat_minutes: int = int(os.getenv("BOT_HEARTBEAT_MINUTES", "60"))
    log_level: str = os.getenv("BOT_LOG_LEVEL", "INFO")
    log_path: str = os.getenv("BOT_LOG_PATH", "live_s5_bot.log")
    bitget_product_type: str = os.getenv("BOT_BITGET_PRODUCT_TYPE", "USDT-FUTURES")

    # paper-trade assumptions
    notional_usdt: float = float(os.getenv("BOT_NOTIONAL_USDT", "200"))
    fee_per_side: float = float(os.getenv("BOT_FEE_PER_SIDE", "0.0004"))
    slippage_per_side: float = float(os.getenv("BOT_SLIPPAGE_PER_SIDE", "0.0002"))

    # optional Telegram
    tg_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    tw = dt.astimezone(ZoneInfo("Asia/Taipei"))
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} UTC / {tw.strftime('%Y-%m-%d %H:%M:%S')} Asia/Taipei"


def send_telegram(cfg: Config, msg: str) -> None:
    if not cfg.tg_token or not cfg.tg_chat_id:
        return
    try:
        url = f"https://api.telegram.org/bot{cfg.tg_token}/sendMessage"
        requests.post(url, json={"chat_id": cfg.tg_chat_id, "text": msg}, timeout=15)
    except Exception:
        # keep bot running even if notify fails
        pass


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bot_state (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_time_utc TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            tp_price REAL NOT NULL,
            signal_bar_time_utc TEXT NOT NULL,
            status TEXT NOT NULL,
            exit_time_utc TEXT,
            exit_price REAL,
            exit_reason TEXT,
            pnl_usdt REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_utc TEXT NOT NULL,
            level TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """
    )
    conn.commit()


def db_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT v FROM bot_state WHERE k=?", (key,)).fetchone()
    return row[0] if row else default


def db_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO bot_state(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    conn.commit()


def log_event(conn: sqlite3.Connection, level: str, message: str) -> None:
    conn.execute(
        "INSERT INTO events(event_time_utc, level, message) VALUES(?, ?, ?)",
        (now_utc().isoformat(), level, message),
    )
    conn.commit()

    logger_level = getattr(logging, level.upper(), logging.INFO)
    LOGGER.log(logger_level, message)


def bitget_granularity(interval: str) -> str:
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H",
        "6h": "6H",
        "12h": "12H",
        "1d": "1D",
        "1w": "1W",
    }
    normalized = interval.strip().lower()
    if normalized not in mapping:
        raise ValueError(f"unsupported Bitget interval: {interval}")
    return mapping[normalized]


def parse_binance_klines(raw: list[object]) -> pd.DataFrame:
    df = pd.DataFrame(
        raw,
        columns=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "num_trades",
            "taker_buy_base",
            "taker_buy_quote",
            "ignore",
        ],
    )
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]].dropna()
    return df.set_index("timestamp").sort_index()


def fetch_binance_klines(cfg: Config) -> pd.DataFrame:
    LOGGER.debug(
        "fetching Binance klines symbol=%s interval=%s limit=%s",
        cfg.symbol,
        cfg.interval,
        cfg.kline_limit,
    )

    last_error: Optional[Exception] = None
    for url in BINANCE_KLINES_URLS:
        try:
            r = requests.get(
                url,
                params={"symbol": cfg.symbol, "interval": cfg.interval, "limit": cfg.kline_limit},
                timeout=20,
            )
            r.raise_for_status()
            return parse_binance_klines(r.json())
        except requests.HTTPError as exc:
            last_error = exc
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code in {403, 451}:
                LOGGER.warning("Binance endpoint blocked status=%s url=%s", status_code, url)
                continue
            raise
        except requests.RequestException as exc:
            last_error = exc
            LOGGER.warning("Binance request failed url=%s error=%s", url, exc)
            continue

    if isinstance(last_error, requests.HTTPError):
        status_code = last_error.response.status_code if last_error.response is not None else None
        if status_code == 451:
            raise RuntimeError(
                "Binance API returned 451 Unavailable For Legal Reasons. "
                "Your VM region cannot access Binance market data. "
                "Set BOT_EXCHANGE=bitget or run the bot from a region where Binance is reachable."
            ) from last_error
        if status_code == 403:
            raise RuntimeError(
                "Binance API returned 403 Forbidden. "
                "The current network blocks Binance market data. "
                "Set BOT_EXCHANGE=bitget or use a reachable network."
            ) from last_error

    raise RuntimeError(f"failed to fetch Binance klines for {cfg.symbol}") from last_error


def fetch_bitget_klines(cfg: Config) -> pd.DataFrame:
    granularity = bitget_granularity(cfg.interval)
    LOGGER.debug(
        "fetching Bitget klines symbol=%s interval=%s limit=%s productType=%s",
        cfg.symbol,
        granularity,
        cfg.kline_limit,
        cfg.bitget_product_type,
    )
    r = requests.get(
        BITGET_KLINES_URL,
        params={
            "symbol": cfg.symbol,
            "productType": cfg.bitget_product_type,
            "granularity": granularity,
            "limit": min(cfg.kline_limit, 1000),
        },
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") not in {None, "00000"}:
        raise RuntimeError(
            f"Bitget API error code={payload.get('code')} msg={payload.get('msg', '')}"
        )

    rows = payload.get("data") or []
    parsed_rows = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        parsed_rows.append(
            {
                "timestamp": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                "open": pd.to_numeric(row[1], errors="coerce"),
                "high": pd.to_numeric(row[2], errors="coerce"),
                "low": pd.to_numeric(row[3], errors="coerce"),
                "close": pd.to_numeric(row[4], errors="coerce"),
                "volume": pd.to_numeric(row[5], errors="coerce"),
            }
        )

    df = pd.DataFrame(parsed_rows)
    if df.empty:
        raise RuntimeError(f"Bitget returned no usable candle data for {cfg.symbol}")

    df = df.dropna().drop_duplicates(subset=["timestamp"])
    return df.set_index("timestamp").sort_index()


def fetch_klines(cfg: Config) -> pd.DataFrame:
    if cfg.exchange == "binance":
        return fetch_binance_klines(cfg)
    if cfg.exchange == "bitget":
        return fetch_bitget_klines(cfg)
    raise ValueError(f"unsupported BOT_EXCHANGE: {cfg.exchange}")


def get_open_position(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM positions WHERE status='OPEN' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.row_factory = None
    return row


def open_position(
    conn: sqlite3.Connection,
    cfg: Config,
    side: str,
    entry_time: datetime,
    entry_price: float,
    stop_price: float,
    tp_price: float,
    signal_bar_time: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO positions(
            symbol, side, entry_time_utc, entry_price, stop_price, tp_price, signal_bar_time_utc, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """,
        (
            cfg.symbol,
            side,
            entry_time.isoformat(),
            float(entry_price),
            float(stop_price),
            float(tp_price),
            signal_bar_time.isoformat(),
        ),
    )
    conn.commit()


def close_position(
    conn: sqlite3.Connection,
    cfg: Config,
    pos_id: int,
    side: str,
    entry_price: float,
    exit_price: float,
    exit_reason: str,
) -> float:
    if side == "long":
        gross_ret = (exit_price - entry_price) / entry_price
    else:
        gross_ret = (entry_price - exit_price) / entry_price
    net_ret = gross_ret - 2 * cfg.fee_per_side
    pnl = cfg.notional_usdt * net_ret
    conn.execute(
        """
        UPDATE positions
           SET status='CLOSED', exit_time_utc=?, exit_price=?, exit_reason=?, pnl_usdt=?
         WHERE id=?
        """,
        (now_utc().isoformat(), float(exit_price), exit_reason, float(pnl), pos_id),
    )
    conn.commit()
    return pnl


def main():
    cfg = Config()
    conn = sqlite3.connect(cfg.db_path)
    init_db(conn)
    log_event(
        conn,
        "INFO",
        (
            f"bot started exchange={cfg.exchange} symbol={cfg.symbol} interval={cfg.interval} "
            f"loop={cfg.loop_seconds}s db={cfg.db_path} log={cfg.log_path}"
        ),
    )
    send_telegram(cfg, f"❤️ S5 機器人啟動\n標的: {cfg.symbol}\n週期: {cfg.interval}\n時間: {fmt_ts(now_utc())}")

    while True:
        try:
            tick_started = time.time()
            df = fetch_klines(cfg)
            if len(df) < 220:
                LOGGER.warning("insufficient bars=%s, sleeping %ss", len(df), cfg.loop_seconds)
                time.sleep(cfg.loop_seconds)
                continue

            # Binance returns the current in-progress candle as the last row.
            closed_idx = len(df) - 2
            open_idx = len(df) - 1
            closed_ts = df.index[closed_idx].to_pydatetime()
            current_open_ts = df.index[open_idx].to_pydatetime()
            LOGGER.debug(
                "tick closed_bar=%s current_open=%s close=%.4f",
                closed_ts.isoformat(),
                current_open_ts.isoformat(),
                float(df["close"].iloc[closed_idx]),
            )

            # 1) manage current open position using latest CLOSED candle range.
            pos = get_open_position(conn)
            if pos is not None:
                side = pos["side"]
                stop_px = float(pos["stop_price"])
                tp_px = float(pos["tp_price"])
                entry_px = float(pos["entry_price"])

                low = float(df["low"].iloc[closed_idx])
                high = float(df["high"].iloc[closed_idx])

                raw_exit = None
                reason = None
                if side == "long":
                    hit_sl = low <= stop_px
                    hit_tp = high >= tp_px
                    if hit_sl and hit_tp:
                        raw_exit, reason = stop_px, "both_hit_stop_first"
                    elif hit_sl:
                        raw_exit, reason = stop_px, "stop"
                    elif hit_tp:
                        raw_exit, reason = tp_px, "tp"
                else:
                    hit_sl = high >= stop_px
                    hit_tp = low <= tp_px
                    if hit_sl and hit_tp:
                        raw_exit, reason = stop_px, "both_hit_stop_first"
                    elif hit_sl:
                        raw_exit, reason = stop_px, "stop"
                    elif hit_tp:
                        raw_exit, reason = tp_px, "tp"

                if raw_exit is not None:
                    direction = 1 if side == "long" else -1
                    exit_px = apply_slippage(raw_exit, direction, is_entry=False)
                    pnl = close_position(conn, cfg, int(pos["id"]), side, entry_px, exit_px, reason)
                    msg = (
                        f"💰 平倉\n"
                        f"幣種: {cfg.symbol}\n"
                        f"方向: {'做多' if side == 'long' else '做空'}\n"
                        f"原因: {reason}\n"
                        f"進場: {entry_px:.4f}\n"
                        f"出場: {exit_px:.4f}\n"
                        f"盈虧: {pnl:+.2f} USDT\n"
                        f"時間: {fmt_ts(now_utc())}"
                    )
                    log_event(conn, "TRADE_EXIT", msg)
                    send_telegram(cfg, msg)

            # 2) open new position when flat and there's a new closed bar signal.
            pos = get_open_position(conn)
            last_processed_signal_ts = db_get(conn, "last_signal_bar_utc", "")
            closed_ts_iso = closed_ts.replace(tzinfo=timezone.utc).isoformat()

            if pos is None and last_processed_signal_ts != closed_ts_iso:
                signals = combined_signals(df, S5_PARAMS)
                LOGGER.debug("generated signals count=%s closed_idx=%s", len(signals), closed_idx)
                target = [s for s in signals if int(s["bar"]) == closed_idx]
                if target:
                    s = target[0]
                    direction = int(s["direction"])
                    side = "long" if direction == 1 else "short"
                    entry_raw = float(df["open"].iloc[open_idx])
                    entry_px = apply_slippage(entry_raw, direction, is_entry=True)
                    stop_px = float(s["stop"])
                    tp_px = float(s["tp"])

                    if entry_px > 0 and stop_px > 0 and tp_px > 0:
                        open_position(
                            conn=conn,
                            cfg=cfg,
                            side=side,
                            entry_time=current_open_ts,
                            entry_price=entry_px,
                            stop_price=stop_px,
                            tp_price=tp_px,
                            signal_bar_time=closed_ts,
                        )
                        msg = (
                            f"✅ 開倉成功\n"
                            f"幣種: {cfg.symbol}\n"
                            f"方向: {'做多' if side == 'long' else '做空'}\n"
                            f"進場價: {entry_px:.4f}\n"
                            f"止損價: {stop_px:.4f}\n"
                            f"止盈價: {tp_px:.4f}\n"
                            f"策略: S5 (BOS+FVG+RSI, RR=1.5)\n"
                            f"時間: {fmt_ts(now_utc())}"
                        )
                        log_event(conn, "TRADE_ENTRY", msg)
                        send_telegram(cfg, msg)

                db_set(conn, "last_signal_bar_utc", closed_ts_iso)
            else:
                LOGGER.debug("no new signal processed flat=%s last_signal=%s", pos is None, last_processed_signal_ts)

            # 3) heartbeat
            last_hb = db_get(conn, "last_heartbeat_utc", "")
            send_hb = True
            if last_hb:
                last_dt = datetime.fromisoformat(last_hb)
                mins = (now_utc() - last_dt).total_seconds() / 60
                send_hb = mins >= cfg.heartbeat_minutes

            if send_hb:
                conn.row_factory = sqlite3.Row
                today = now_utc().date().isoformat()
                row = conn.execute(
                    """
                    SELECT
                        COALESCE(SUM(pnl_usdt), 0) AS pnl,
                        COUNT(*) AS trades,
                        COALESCE(SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END), 0) AS wins
                    FROM positions
                    WHERE status='CLOSED' AND substr(exit_time_utc,1,10)=?
                    """,
                    (today,),
                ).fetchone()
                open_cnt = conn.execute("SELECT COUNT(*) AS c FROM positions WHERE status='OPEN'").fetchone()["c"]
                conn.row_factory = None

                trades = int(row["trades"]) if row else 0
                wins = int(row["wins"]) if row else 0
                win_rate = (wins / trades * 100) if trades else 0.0
                pnl_today = float(row["pnl"]) if row else 0.0
                hb_msg = (
                    f"❤️ 機器人運作正常\n"
                    f"時間: {fmt_ts(now_utc())}\n"
                    f"今日盈虧: {pnl_today:+.2f} USDT\n"
                    f"今日交易: {trades} 筆 (勝率 {win_rate:.1f}%)\n"
                    f"目前持倉: {open_cnt}"
                )
                send_telegram(cfg, hb_msg)
                db_set(conn, "last_heartbeat_utc", now_utc().isoformat())

            LOGGER.debug("tick finished in %.2fs", time.time() - tick_started)

        except Exception as e:
            msg = f"⚠️ bot error: {type(e).__name__} {e}"
            log_event(conn, "ERROR", msg)
            LOGGER.exception("tick failed")
            send_telegram(cfg, msg)

        time.sleep(cfg.loop_seconds)


if __name__ == "__main__":
    main()
