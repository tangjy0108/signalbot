"""
Multi-strategy live paper signal bot
------------------------------------
- Polls spot klines from configured exchange (Binance/KuCoin)
- Runs S5 and ICT Killzone Opt3 paper signals
- Opens/closes simulated positions and logs to SQLite
- Sends Telegram messages for entry/exit/heartbeat (optional)

This script does NOT place real exchange orders.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from ict_killzone_opt3_core import DEFAULT_PARAMS as KILLZONE_PARAMS
from ict_killzone_opt3_core import killzone_opt3_signals, should_force_flat_after_ny
from s5_strategy_core import combined_signals


BINANCE_KLINES_URL = "https://api.binance.com/api/v3/klines"
KUCOIN_CANDLES_URL = "https://api.kucoin.com/api/v1/market/candles"
LOGGER = logging.getLogger("multi_signal_bot")

SUPPORTED_EXCHANGES = {"binance", "kucoin"}
KUCOIN_INTERVAL_MAP = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1hour",
    "2h": "2hour",
    "4h": "4hour",
    "6h": "6hour",
    "8h": "8hour",
    "12h": "12hour",
    "1d": "1day",
    "1w": "1week",
}

S5_PARAMS = {
    "lookback": 10,
    "fvg_window": 20,
    "rsi_period": 10,
    "rsi_ob": 65,
    "rsi_os": 35,
    "atr_mult_sl": 1.0,
    "rr_ratio": 1.5,
}

STRATEGY_NAMES = {
    "s5": "S5 (BOS+FVG+RSI, RR=1.5)",
    "ict_killzone_opt3": "ICT Killzone Opt3",
}


def load_env_file(path: Optional[str] = None) -> None:
    env_path = path or os.getenv("BOT_ENV_PATH", ".env")
    if not env_path:
        return

    try:
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        os.environ[key] = value


load_env_file()


@dataclass
class Config:
    symbols_raw: str = os.getenv("BOT_SYMBOLS", "BTCUSDT,ETHUSDT,ADAUSDT")
    strategies_raw: str = os.getenv("BOT_STRATEGIES", "s5,ict_killzone_opt3")
    exchange_raw: str = os.getenv("BOT_EXCHANGE", "kucoin")
    interval: str = os.getenv("BOT_INTERVAL", "15m")
    kline_limit: int = int(os.getenv("BOT_KLINE_LIMIT", "600"))
    db_path: str = os.getenv("BOT_DB_PATH", "live_s5_bot.db")
    loop_seconds: int = int(os.getenv("BOT_LOOP_SECONDS", "20"))
    heartbeat_minutes: int = int(os.getenv("BOT_HEARTBEAT_MINUTES", "60"))
    log_level: str = os.getenv("BOT_LOG_LEVEL", "DEBUG")
    log_path: str = os.getenv("BOT_LOG_PATH", "live_s5_bot.log")
    ssl_verify_raw: str = os.getenv("BOT_SSL_VERIFY", "true")
    ca_bundle_path: str = os.getenv("BOT_CA_BUNDLE", "")

    # paper-trade assumptions
    notional_usdt: float = float(os.getenv("BOT_NOTIONAL_USDT", "200"))
    fee_per_side: float = float(os.getenv("BOT_FEE_PER_SIDE", "0.0004"))
    slippage_per_side: float = float(os.getenv("BOT_SLIPPAGE_PER_SIDE", "0.0002"))

    # optional Telegram
    tg_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")

    @property
    def symbols(self) -> list[str]:
        return [s.strip().upper() for s in self.symbols_raw.split(",") if s.strip()]

    @property
    def strategies(self) -> list[str]:
        enabled = [s.strip().lower() for s in self.strategies_raw.split(",") if s.strip()]
        return [s for s in enabled if s in STRATEGY_NAMES]

    @property
    def exchange(self) -> str:
        exchange = self.exchange_raw.strip().lower()
        return exchange if exchange in SUPPORTED_EXCHANGES else "binance"

    @property
    def ssl_verify(self) -> bool:
        return self.ssl_verify_raw.strip().lower() not in {"0", "false", "no", "off"}

    @property
    def requests_verify(self) -> bool | str:
        if not self.ssl_verify:
            return False
        if self.ca_bundle_path.strip():
            return self.ca_bundle_path.strip()
        return True


def setup_logging(cfg: Config) -> None:
    level = getattr(logging, cfg.log_level.upper(), logging.DEBUG)
    fmt = "%(asctime)s %(levelname)s %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if cfg.log_path:
        handlers.append(logging.FileHandler(cfg.log_path, encoding="utf-8"))
    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)
    # Keep bot debug logs, but do not print full third-party HTTP URLs.
    # Telegram tokens are embedded in bot API URLs, so urllib3 DEBUG logs are sensitive.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    tw = dt.astimezone(ZoneInfo("Asia/Taipei"))
    return f"{dt.strftime('%Y-%m-%d %H:%M:%S')} UTC / {tw.strftime('%Y-%m-%d %H:%M:%S')} Asia/Taipei"


def send_telegram(cfg: Config, msg: str) -> None:
    if not cfg.tg_token or not cfg.tg_chat_id:
        LOGGER.debug("telegram skipped: TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set")
        return
    try:
        url = f"https://api.telegram.org/bot{cfg.tg_token}/sendMessage"
        res = requests.post(url, json={"chat_id": cfg.tg_chat_id, "text": msg}, timeout=15)
        if res.ok:
            LOGGER.debug("telegram sent")
        else:
            LOGGER.warning("telegram send failed: status=%s", res.status_code)
    except Exception as exc:
        LOGGER.warning("telegram send failed: %s", type(exc).__name__)


def apply_slippage(price: float, direction: int, is_entry: bool, slippage_per_side: float) -> float:
    # long: pay up on entry, sell lower on exit
    # short: sell lower on entry, buy higher on exit
    if direction == 1 and is_entry:
        return price * (1 + slippage_per_side)
    if direction == 1 and not is_entry:
        return price * (1 - slippage_per_side)
    if direction == -1 and is_entry:
        return price * (1 - slippage_per_side)
    return price * (1 + slippage_per_side)


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
            strategy_id TEXT NOT NULL DEFAULT 's5',
            strategy_name TEXT NOT NULL DEFAULT 'S5',
            side TEXT NOT NULL,
            entry_time_utc TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            tp_price REAL NOT NULL,
            signal_bar_time_utc TEXT NOT NULL,
            setup_session TEXT,
            setup_type TEXT,
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
    ensure_position_columns(conn)
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_positions_open_strategy_symbol
        ON positions(status, strategy_id, symbol)
        """
    )
    conn.commit()


def ensure_position_columns(conn: sqlite3.Connection) -> None:
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


def db_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT v FROM bot_state WHERE k=?", (key,)).fetchone()
    return row[0] if row else default


def db_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO bot_state(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        (key, value),
    )
    conn.commit()


def state_key(strategy_id: str, symbol: str, key: str) -> str:
    return f"{strategy_id}:{symbol}:{key}"


def log_event(conn: sqlite3.Connection, level: str, message: str) -> None:
    conn.execute(
        "INSERT INTO events(event_time_utc, level, message) VALUES(?, ?, ?)",
        (now_utc().isoformat(), level, message),
    )
    conn.commit()


def to_kucoin_symbol(symbol: str) -> str:
    if "-" in symbol:
        return symbol.upper()
    if symbol.endswith("USDT") and len(symbol) > 4:
        return f"{symbol[:-4]}-USDT"
    if symbol.endswith("USDC") and len(symbol) > 4:
        return f"{symbol[:-4]}-USDC"
    if symbol.endswith("BTC") and len(symbol) > 3:
        return f"{symbol[:-3]}-BTC"
    return symbol


def to_kucoin_interval(interval: str) -> str:
    key = interval.strip().lower()
    if key not in KUCOIN_INTERVAL_MAP:
        supported = ", ".join(sorted(KUCOIN_INTERVAL_MAP.keys()))
        raise ValueError(f"Unsupported interval for KuCoin: {interval}. supported={supported}")
    return KUCOIN_INTERVAL_MAP[key]


def fetch_klines_binance(symbol: str, interval: str, limit: int, verify: bool | str = True) -> pd.DataFrame:
    r = requests.get(
        BINANCE_KLINES_URL,
        params={"symbol": symbol, "interval": interval, "limit": limit},
        verify=verify,
        timeout=20,
    )
    r.raise_for_status()
    raw = r.json()
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


def fetch_klines_kucoin(symbol: str, interval: str, limit: int, verify: bool | str = True) -> pd.DataFrame:
    kucoin_symbol = to_kucoin_symbol(symbol)
    kucoin_interval = to_kucoin_interval(interval)
    r = requests.get(
        KUCOIN_CANDLES_URL,
        params={"symbol": kucoin_symbol, "type": kucoin_interval},
        verify=verify,
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") != "200000":
        raise ValueError(f"KuCoin API error symbol={kucoin_symbol} code={payload.get('code')} msg={payload.get('msg')}")

    raw = payload.get("data", [])
    df = pd.DataFrame(raw, columns=["open_time", "open", "close", "high", "low", "volume", "turnover"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="s", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]].dropna()
    df = df.set_index("timestamp").sort_index()
    if limit > 0:
        df = df.tail(limit)
    return df


def fetch_klines(exchange: str, symbol: str, interval: str, limit: int, verify: bool | str = True) -> pd.DataFrame:
    if exchange == "kucoin":
        return fetch_klines_kucoin(symbol, interval, limit, verify=verify)
    return fetch_klines_binance(symbol, interval, limit, verify=verify)


def get_open_position(conn: sqlite3.Connection, symbol: str, strategy_id: str) -> Optional[sqlite3.Row]:
    old_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT * FROM positions
        WHERE status='OPEN' AND symbol=? AND strategy_id=?
        ORDER BY id DESC LIMIT 1
        """,
        (symbol, strategy_id),
    ).fetchone()
    conn.row_factory = old_factory
    return row


def open_position(
    conn: sqlite3.Connection,
    symbol: str,
    strategy_id: str,
    strategy_name: str,
    side: str,
    entry_time: datetime,
    entry_price: float,
    stop_price: float,
    tp_price: float,
    signal_bar_time: datetime,
    setup_session: str = "",
    setup_type: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO positions(
            symbol, strategy_id, strategy_name, side, entry_time_utc, entry_price,
            stop_price, tp_price, signal_bar_time_utc, setup_session, setup_type, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """,
        (
            symbol,
            strategy_id,
            strategy_name,
            side,
            entry_time.isoformat(),
            float(entry_price),
            float(stop_price),
            float(tp_price),
            signal_bar_time.isoformat(),
            setup_session,
            setup_type,
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


def strategy_signals(strategy_id: str, df: pd.DataFrame) -> list[dict[str, Any]]:
    if strategy_id == "s5":
        signals = combined_signals(df, S5_PARAMS)
        for signal in signals:
            signal["setup_session"] = ""
            signal["setup_type"] = "S5"
        return signals
    if strategy_id == "ict_killzone_opt3":
        return killzone_opt3_signals(df, KILLZONE_PARAMS)
    return []


def valid_entry_prices(direction: int, entry_px: float, stop_px: float, tp_px: float) -> bool:
    if entry_px <= 0 or stop_px <= 0 or tp_px <= 0:
        return False
    if direction == 1:
        return entry_px > stop_px and tp_px > entry_px
    return entry_px < stop_px and tp_px < entry_px


def manage_position(
    conn: sqlite3.Connection,
    cfg: Config,
    pos: sqlite3.Row,
    df: pd.DataFrame,
    closed_idx: int,
    closed_ts: datetime,
) -> None:
    symbol = str(pos["symbol"])
    strategy_id = str(pos["strategy_id"])
    strategy_name = str(pos["strategy_name"])
    side = str(pos["side"])
    stop_px = float(pos["stop_price"])
    tp_px = float(pos["tp_price"])
    entry_px = float(pos["entry_price"])

    low = float(df["low"].iloc[closed_idx])
    high = float(df["high"].iloc[closed_idx])
    close = float(df["close"].iloc[closed_idx])
    LOGGER.debug(
        "open position symbol=%s strategy=%s id=%s side=%s entry=%.4f stop=%.4f tp=%.4f low=%.4f high=%.4f",
        symbol,
        strategy_id,
        pos["id"],
        side,
        entry_px,
        stop_px,
        tp_px,
        low,
        high,
    )

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

    if raw_exit is None and strategy_id == "ict_killzone_opt3":
        if should_force_flat_after_ny(pd.Timestamp(closed_ts), KILLZONE_PARAMS):
            raw_exit, reason = close, "flat_after_ny"

    if raw_exit is None:
        LOGGER.debug("position still active symbol=%s strategy=%s id=%s", symbol, strategy_id, pos["id"])
        return

    direction = 1 if side == "long" else -1
    exit_px = apply_slippage(raw_exit, direction, is_entry=False, slippage_per_side=cfg.slippage_per_side)
    pnl = close_position(conn, cfg, int(pos["id"]), side, entry_px, exit_px, reason)
    LOGGER.info(
        "position closed symbol=%s strategy=%s id=%s reason=%s exit=%.4f pnl=%+.2f",
        symbol,
        strategy_id,
        pos["id"],
        reason,
        exit_px,
        pnl,
    )
    msg = (
        f"💰 平倉\n"
        f"幣種: {symbol}\n"
        f"策略: {strategy_name}\n"
        f"方向: {'做多' if side == 'long' else '做空'}\n"
        f"原因: {reason}\n"
        f"進場: {entry_px:.4f}\n"
        f"出場: {exit_px:.4f}\n"
        f"盈虧: {pnl:+.2f} USDT\n"
        f"時間: {fmt_ts(now_utc())}"
    )
    log_event(conn, "TRADE_EXIT", msg)
    send_telegram(cfg, msg)


def try_open_new_position(
    conn: sqlite3.Connection,
    cfg: Config,
    symbol: str,
    strategy_id: str,
    df: pd.DataFrame,
    closed_idx: int,
    open_idx: int,
    closed_ts: datetime,
    current_open_ts: datetime,
) -> None:
    strategy_name = STRATEGY_NAMES[strategy_id]
    key = state_key(strategy_id, symbol, "last_signal_bar_utc")
    closed_ts_iso = closed_ts.replace(tzinfo=timezone.utc).isoformat()
    last_processed_signal_ts = db_get(conn, key, "")
    if last_processed_signal_ts == closed_ts_iso:
        LOGGER.debug("skip signal check: already processed symbol=%s strategy=%s", symbol, strategy_id)
        return

    LOGGER.debug("checking signals symbol=%s strategy=%s closed_bar=%s", symbol, strategy_id, closed_ts_iso)
    signals = strategy_signals(strategy_id, df)
    target = [s for s in signals if int(s["bar"]) == open_idx]
    LOGGER.debug(
        "signals symbol=%s strategy=%s total=%s target_on_next_open=%s",
        symbol,
        strategy_id,
        len(signals),
        len(target),
    )

    if target:
        signal = target[0]
        direction = int(signal["direction"])
        side = "long" if direction == 1 else "short"
        entry_raw = float(df["open"].iloc[open_idx])
        entry_px = apply_slippage(entry_raw, direction, is_entry=True, slippage_per_side=cfg.slippage_per_side)
        stop_px = float(signal["stop"])
        tp_px = float(signal["tp"])
        setup_session = str(signal.get("setup_session", ""))
        setup_type = str(signal.get("setup_type", ""))

        if valid_entry_prices(direction, entry_px, stop_px, tp_px):
            LOGGER.info(
                "opening position symbol=%s strategy=%s side=%s entry=%.4f stop=%.4f tp=%.4f setup=%s/%s",
                symbol,
                strategy_id,
                side,
                entry_px,
                stop_px,
                tp_px,
                setup_session or "-",
                setup_type or "-",
            )
            open_position(
                conn=conn,
                symbol=symbol,
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                side=side,
                entry_time=current_open_ts,
                entry_price=entry_px,
                stop_price=stop_px,
                tp_price=tp_px,
                signal_bar_time=closed_ts,
                setup_session=setup_session,
                setup_type=setup_type,
            )
            msg = (
                f"✅ 開倉成功\n"
                f"幣種: {symbol}\n"
                f"策略: {strategy_name}\n"
                f"方向: {'做多' if side == 'long' else '做空'}\n"
                f"進場價: {entry_px:.4f}\n"
                f"止損價: {stop_px:.4f}\n"
                f"止盈價: {tp_px:.4f}\n"
                f"Session: {setup_session or '-'}\n"
                f"Setup: {setup_type or '-'}\n"
                f"時間: {fmt_ts(now_utc())}"
            )
            log_event(conn, "TRADE_ENTRY", msg)
            send_telegram(cfg, msg)
        else:
            LOGGER.warning(
                "signal skipped invalid prices symbol=%s strategy=%s direction=%s entry=%.4f stop=%.4f tp=%.4f",
                symbol,
                strategy_id,
                direction,
                entry_px,
                stop_px,
                tp_px,
            )
    else:
        LOGGER.debug("no new signal symbol=%s strategy=%s", symbol, strategy_id)

    db_set(conn, key, closed_ts_iso)


def send_heartbeat(conn: sqlite3.Connection, cfg: Config) -> None:
    last_hb = db_get(conn, "last_heartbeat_utc", "")
    send_hb = True
    if last_hb:
        last_dt = datetime.fromisoformat(last_hb)
        mins = (now_utc() - last_dt).total_seconds() / 60
        send_hb = mins >= cfg.heartbeat_minutes

    if not send_hb:
        LOGGER.debug("heartbeat not due yet")
        return

    LOGGER.debug("sending heartbeat")
    old_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    today = now_utc().date().isoformat()
    rows = conn.execute(
        """
        SELECT
            symbol,
            strategy_id,
            COALESCE(SUM(pnl_usdt), 0) AS pnl,
            COUNT(*) AS trades,
            COALESCE(SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END), 0) AS wins
        FROM positions
        WHERE status='CLOSED' AND substr(exit_time_utc,1,10)=?
        GROUP BY symbol, strategy_id
        ORDER BY symbol, strategy_id
        """,
        (today,),
    ).fetchall()
    overall = conn.execute(
        """
        SELECT
            COALESCE(SUM(pnl_usdt), 0) AS total_pnl,
            COALESCE(SUM(CASE WHEN pnl_usdt < 0 THEN pnl_usdt ELSE 0 END), 0) AS total_loss,
            COUNT(*) AS total_trades,
            COALESCE(SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END), 0) AS total_wins
        FROM positions
        WHERE status='CLOSED'
        """
    ).fetchone()
    open_cnt = conn.execute("SELECT COUNT(*) AS c FROM positions WHERE status='OPEN'").fetchone()["c"]
    conn.row_factory = old_factory

    lines = [
        "❤️ 機器人運作正常",
        f"時間: {fmt_ts(now_utc())}",
        f"掃描幣種: {', '.join(cfg.symbols)}",
        f"策略: {', '.join(cfg.strategies)}",
        f"目前持倉: {open_cnt}",
        f"累計已平倉損益: {float(overall['total_pnl']):+.2f} USDT",
        f"累計總虧損: {float(overall['total_loss']):.2f} USDT",
    ]
    total_trades = int(overall["total_trades"])
    total_wins = int(overall["total_wins"])
    total_win_rate = (total_wins / total_trades * 100) if total_trades else 0.0
    lines.append(f"累計交易: {total_trades} 筆 / 勝率 {total_win_rate:.1f}%")
    if rows:
        lines.append("今日已平倉:")
        for row in rows:
            trades = int(row["trades"])
            wins = int(row["wins"])
            win_rate = (wins / trades * 100) if trades else 0.0
            lines.append(
                f"- {row['symbol']} {row['strategy_id']}: {float(row['pnl']):+.2f} USDT / {trades} 筆 / 勝率 {win_rate:.1f}%"
            )
    else:
        lines.append("今日已平倉: 0 筆")

    send_telegram(cfg, "\n".join(lines))
    db_set(conn, "last_heartbeat_utc", now_utc().isoformat())


def main() -> None:
    cfg = Config()
    setup_logging(cfg)
    if cfg.requests_verify is False:
        LOGGER.warning("SSL verify disabled for HTTPS requests (BOT_SSL_VERIFY=false)")
    elif isinstance(cfg.requests_verify, str):
        LOGGER.info("Using custom CA bundle: %s", cfg.requests_verify)
    conn = sqlite3.connect(cfg.db_path)
    init_db(conn)
    log_event(conn, "INFO", "bot started")
    LOGGER.info(
        "bot started exchange=%s symbols=%s strategies=%s interval=%s loop=%ss db=%s telegram=%s log=%s ssl_verify=%s",
        cfg.exchange,
        ",".join(cfg.symbols),
        ",".join(cfg.strategies),
        cfg.interval,
        cfg.loop_seconds,
        cfg.db_path,
        "enabled" if cfg.tg_token and cfg.tg_chat_id else "disabled",
        cfg.log_path or "terminal-only",
        cfg.requests_verify,
    )
    send_telegram(
        cfg,
        f"❤️ Multi-strategy 機器人啟動\n"
        f"交易所: {cfg.exchange}\n"
        f"幣種: {', '.join(cfg.symbols)}\n"
        f"策略: {', '.join(cfg.strategies)}\n"
        f"週期: {cfg.interval}\n"
        f"時間: {fmt_ts(now_utc())}",
    )

    tick = 0
    while True:
        tick += 1
        started = time.monotonic()
        try:
            for symbol in cfg.symbols:
                LOGGER.debug("tick=%s fetching klines exchange=%s symbol=%s", tick, cfg.exchange, symbol)
                df = fetch_klines(cfg.exchange, symbol, cfg.interval, cfg.kline_limit, verify=cfg.requests_verify)
                if len(df) < 220:
                    LOGGER.warning("tick=%s symbol=%s not enough bars: bars=%s need>=220", tick, symbol, len(df))
                    continue

                # Binance returns the current in-progress candle as the last row.
                closed_idx = len(df) - 2
                open_idx = len(df) - 1
                closed_ts = df.index[closed_idx].to_pydatetime()
                current_open_ts = df.index[open_idx].to_pydatetime()
                closed_close = float(df["close"].iloc[closed_idx])
                LOGGER.info(
                    "alive tick=%s symbol=%s bars=%s closed_bar=%s close=%.4f current_open=%s",
                    tick,
                    symbol,
                    len(df),
                    closed_ts.isoformat(),
                    closed_close,
                    current_open_ts.isoformat(),
                )

                for strategy_id in cfg.strategies:
                    pos = get_open_position(conn, symbol, strategy_id)
                    if pos is not None:
                        manage_position(conn, cfg, pos, df, closed_idx, closed_ts)

                    pos = get_open_position(conn, symbol, strategy_id)
                    if pos is None:
                        try_open_new_position(
                            conn=conn,
                            cfg=cfg,
                            symbol=symbol,
                            strategy_id=strategy_id,
                            df=df,
                            closed_idx=closed_idx,
                            open_idx=open_idx,
                            closed_ts=closed_ts,
                            current_open_ts=current_open_ts,
                        )
                    else:
                        LOGGER.debug("skip signal check: open position symbol=%s strategy=%s", symbol, strategy_id)

            send_heartbeat(conn, cfg)

        except Exception as e:
            msg = f"⚠️ bot error: {type(e).__name__} {e}"
            LOGGER.exception("tick=%s failed: %s", tick, type(e).__name__)
            log_event(conn, "ERROR", msg)
            send_telegram(cfg, msg)

        elapsed = time.monotonic() - started
        LOGGER.debug("tick=%s finished in %.2fs; sleeping %ss", tick, elapsed, cfg.loop_seconds)
        time.sleep(cfg.loop_seconds)


if __name__ == "__main__":
    main()
