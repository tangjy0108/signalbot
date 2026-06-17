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
from ict_killzone_opt3_core import DEFAULT_PARAMS as KILLZONE_PARAMS
from ict_killzone_opt3_core import killzone_opt3_signals, should_force_flat_after_ny

try:
    from intraday_edge_hunt import add_indicators as add_edge_indicators
    from intraday_edge_hunt import build_conditions
except ModuleNotFoundError:
    def add_edge_indicators(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        close = out["close"]
        high = out["high"]
        low = out["low"]
        volume = out["volume"]

        out["ema20"] = close.ewm(span=20, adjust=False).mean()
        out["ema50"] = close.ewm(span=50, adjust=False).mean()
        out["ema200"] = close.ewm(span=200, adjust=False).mean()
        out["ema20_slope"] = out["ema20"].diff(3)
        out["ema50_slope"] = out["ema50"].diff(5)

        delta = close.diff()
        gain = delta.clip(lower=0).ewm(span=14, adjust=False).mean()
        loss = (-delta.clip(upper=0)).ewm(span=14, adjust=False).mean()
        out["rsi14"] = 100 - 100 / (1 + gain / (loss + 1e-9))

        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False).mean()
        out["macd_hist"] = macd - signal
        out["macd_hist_d1"] = out["macd_hist"].diff(1)

        prev_close = close.shift(1)
        true_range = pd.concat(
            [high - low, (high - prev_close).abs(), (low - prev_close).abs()],
            axis=1,
        ).max(axis=1)
        out["atr14"] = true_range.ewm(span=14, adjust=False).mean()
        out["atr_pct"] = out["atr14"] / close

        out["vol_ratio"] = volume / (volume.rolling(20).mean() + 1e-9)
        out["hh20_prev"] = high.shift(1).rolling(20).max()
        out["ll20_prev"] = low.shift(1).rolling(20).min()

        out["hour"] = out.index.hour
        out["session_asia"] = out["hour"].between(0, 7)
        out["session_london"] = out["hour"].between(8, 12)
        out["session_overlap"] = out["hour"].between(13, 16)
        out["session_ny"] = out["hour"].between(17, 21)
        return out


    def build_conditions(df: pd.DataFrame):
        median_atr = df["atr_pct"].rolling(200).median()

        long_conditions = {
            "trend_up": (df["ema20"] > df["ema50"]) & (df["ema50"] > df["ema200"]),
            "trend_up_soft": df["ema20"] > df["ema50"],
            "ema_slope_up": (df["ema20_slope"] > 0) & (df["ema50_slope"] > 0),
            "pullback_near_ema20": (df["close"] < df["ema20"] * 1.003)
            & (df["close"] > df["ema50"] * 0.997),
            "rsi_reclaim_50": (df["rsi14"].shift(1) <= 50) & (df["rsi14"] > 50),
            "rsi_mid_up": df["rsi14"].between(50, 62),
            "macd_turn_up": (df["macd_hist"] < 0) & (df["macd_hist_d1"] > 0),
            "macd_positive": df["macd_hist"] > 0,
            "breakout_20h": df["close"] > df["hh20_prev"],
            "vol_expand": df["vol_ratio"] > 1.2,
            "high_vol": df["atr_pct"] > median_atr,
            "session_overlap": df["session_overlap"],
            "session_ny": df["session_ny"],
        }

        short_conditions = {
            "trend_down": (df["ema20"] < df["ema50"]) & (df["ema50"] < df["ema200"]),
            "trend_down_soft": df["ema20"] < df["ema50"],
            "ema_slope_down": (df["ema20_slope"] < 0) & (df["ema50_slope"] < 0),
            "pullup_near_ema20": (df["close"] > df["ema20"] * 0.997)
            & (df["close"] < df["ema50"] * 1.003),
            "rsi_lose_50": (df["rsi14"].shift(1) >= 50) & (df["rsi14"] < 50),
            "rsi_mid_down": df["rsi14"].between(38, 50),
            "macd_turn_down": (df["macd_hist"] > 0) & (df["macd_hist_d1"] < 0),
            "macd_negative": df["macd_hist"] < 0,
            "breakdown_20h": df["close"] < df["ll20_prev"],
            "vol_expand": df["vol_ratio"] > 1.2,
            "high_vol": df["atr_pct"] > median_atr,
            "session_overlap": df["session_overlap"],
            "session_ny": df["session_ny"],
        }

        return long_conditions, short_conditions


BINANCE_KLINES_URLS = (
    "https://api.binance.com/api/v3/klines",
    "https://data-api.binance.vision/api/v3/klines",
)
BITGET_KLINES_URL = "https://api.bitget.com/api/v2/mix/market/candles"
BINGX_KLINES_URL  = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"

# Display name → actual BINGx perpetual contract symbol
BINGX_SYMBOL_MAP: dict[str, str] = {
    "NASDAQ100USD": "NCSINASDAQ1002USD-USDT",
    "SP500USD":     "NCSISP5002USD-USDT",
    "DOWJONESUSD":  "NCSIDOWJONES2USD-USDT",
}

def to_bingx_symbol(symbol: str) -> str:
    if symbol in BINGX_SYMBOL_MAP:
        return BINGX_SYMBOL_MAP[symbol]
    if symbol.endswith("USDT"):
        return symbol[:-4] + "-USDT"
    return symbol

S5_PARAMS = {
    "lookback": 10,
    "fvg_window": 20,
    "rsi_period": 10,
    "rsi_ob": 65,
    "rsi_os": 35,
    "atr_mult_sl": 1.0,
    "rr_ratio": 1.5,  # best balance from current RR sweep
}

EDGE_NY_VOL_LONG_PARAMS = {
    "atr_stop_mult": 1.0,
    "rr_ratio": 1.2,
}

EDGE_BREAKDOWN_VOL_SHORT_PARAMS = {
    "atr_stop_mult": 1.0,
    "rr_ratio": 1.2,
}

ACTIVE_STRATEGIES = (
    "s5",
    "ict_killzone_opt3",
    "edge_ny_vol_long",
    "edge_breakdown_vol_short",
)

ATM_STRATEGY_ID = "atm_asia"
ATM_STRATEGY_NAME = "ATM Asia"

STRATEGY_NAMES = {
    "s5": "S5",
    "ict_killzone_opt3": "ICT Killzone Opt3",
    "edge_ny_vol_long": "NY Volume Long",
    "edge_breakdown_vol_short": "Breakdown Volume Short",
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
    exchange: str = os.getenv("BOT_EXCHANGE", "bingx").strip().lower()
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

    # BingX auto-trade (ATM signals)
    atm_auto_trade: bool = os.getenv("ATM_AUTO_TRADE", "false").lower() == "true"
    bingx_api_key: str = os.getenv("BINGX_API_KEY", "")
    bingx_api_secret: str = os.getenv("BINGX_API_SECRET", "")

    # ATM monitor
    atm_monitor_seconds: float = float(os.getenv("ATM_MONITOR_SECONDS", "10"))
    atm_monitor_interval: str = os.getenv("ATM_MONITOR_INTERVAL", "1m")
    atm_daily_summary_hour: int = int(os.getenv("ATM_DAILY_SUMMARY_HOUR", "23"))
    atm_daily_summary_minute: int = int(os.getenv("ATM_DAILY_SUMMARY_MINUTE", "0"))


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
            strategy_id TEXT NOT NULL DEFAULT 's5',
            strategy_name TEXT NOT NULL DEFAULT 'S5',
            setup_session TEXT,
            setup_type TEXT,
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS atm_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_key TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            interaction TEXT NOT NULL,
            signal_time_utc TEXT NOT NULL,
            created_time_utc TEXT NOT NULL,
            entry_price REAL NOT NULL,
            sl_price REAL NOT NULL,
            tp1_price REAL NOT NULL,
            tp2_price REAL NOT NULL,
            rr_tp1 REAL,
            rr_tp2 REAL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            max_favorable_stage INTEGER NOT NULL DEFAULT 0,
            final_outcome TEXT,
            close_reason TEXT,
            close_price REAL,
            closed_time_utc TEXT,
            tp1_hit_time_utc TEXT,
            tp1_hit_price REAL,
            tp2_hit_time_utc TEXT,
            tp2_hit_price REAL,
            sl_hit_time_utc TEXT,
            sl_hit_price REAL,
            last_price REAL,
            last_price_time_utc TEXT
        )
        """
    )
    ensure_position_columns(conn)
    ensure_atm_signal_columns(conn)
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
    conn.commit()


def ensure_atm_signal_columns(conn: sqlite3.Connection) -> None:
    existing = {row[1] for row in conn.execute("PRAGMA table_info(atm_signals)").fetchall()}
    if not existing:
        return
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
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_atm_signals_signal_key ON atm_signals(signal_key)"
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


def fetch_bingx_klines(cfg: Config) -> pd.DataFrame:
    bingx_sym = to_bingx_symbol(cfg.symbol)
    LOGGER.debug(
        "fetching BINGx klines symbol=%s (%s) interval=%s limit=%s",
        cfg.symbol, bingx_sym, cfg.interval, cfg.kline_limit,
    )
    r = requests.get(
        BINGX_KLINES_URL,
        params={
            "symbol":   bingx_sym,
            "interval": cfg.interval,
            "limit":    min(cfg.kline_limit, 1000),
        },
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") != 0:
        raise RuntimeError(
            f"BINGx API error code={payload.get('code')} msg={payload.get('msg', '')}"
        )

    rows = payload.get("data") or []
    parsed_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        parsed_rows.append({
            "timestamp": pd.to_datetime(int(row["time"]), unit="ms", utc=True),
            "open":   pd.to_numeric(row.get("open"),   errors="coerce"),
            "high":   pd.to_numeric(row.get("high"),   errors="coerce"),
            "low":    pd.to_numeric(row.get("low"),    errors="coerce"),
            "close":  pd.to_numeric(row.get("close"),  errors="coerce"),
            "volume": pd.to_numeric(row.get("volume"), errors="coerce"),
        })

    df = pd.DataFrame(parsed_rows)
    if df.empty:
        raise RuntimeError(f"BINGx returned no usable candle data for {bingx_sym}")

    df = df.dropna().drop_duplicates(subset=["timestamp"])
    return df.set_index("timestamp").sort_index()


def fetch_klines(cfg: Config) -> pd.DataFrame:
    if cfg.exchange == "binance":
        return fetch_binance_klines(cfg)
    if cfg.exchange == "bitget":
        return fetch_bitget_klines(cfg)
    if cfg.exchange == "bingx":
        return fetch_bingx_klines(cfg)
    raise ValueError(f"unsupported BOT_EXCHANGE: {cfg.exchange}")


def strategy_name(strategy_id: str) -> str:
    return STRATEGY_NAMES.get(strategy_id, strategy_id)


def edge_strategy_signals(df: pd.DataFrame, strategy_id: str) -> list[dict[str, object]]:
    enriched = add_edge_indicators(df)
    long_c, short_c = build_conditions(enriched)
    atr = enriched["atr14"]
    close = enriched["close"]

    signals: list[dict[str, object]] = []
    for i in range(1, len(enriched) - 1):
        atr_value = float(atr.iloc[i]) if pd.notna(atr.iloc[i]) else float("nan")
        close_value = float(close.iloc[i]) if pd.notna(close.iloc[i]) else float("nan")
        if not pd.notna(atr_value) or atr_value <= 0 or not pd.notna(close_value) or close_value <= 0:
            continue

        if strategy_id == "edge_ny_vol_long":
            is_signal = bool(long_c["vol_expand"].iloc[i] and long_c["session_ny"].iloc[i])
            if not is_signal:
                continue
            stop = close_value - atr_value * EDGE_NY_VOL_LONG_PARAMS["atr_stop_mult"]
            if close_value <= stop:
                continue
            tp = close_value + (close_value - stop) * EDGE_NY_VOL_LONG_PARAMS["rr_ratio"]
            signals.append(
                {
                    "bar": i + 1,
                    "direction": 1,
                    "entry": close_value,
                    "stop": float(stop),
                    "tp": float(tp),
                    "setup_session": "New York",
                    "setup_type": "NY Volume Long",
                }
            )

        if strategy_id == "edge_breakdown_vol_short":
            is_signal = bool(short_c["breakdown_20h"].iloc[i] and short_c["vol_expand"].iloc[i])
            if not is_signal:
                continue
            stop = close_value + atr_value * EDGE_BREAKDOWN_VOL_SHORT_PARAMS["atr_stop_mult"]
            if close_value >= stop:
                continue
            tp = close_value - (stop - close_value) * EDGE_BREAKDOWN_VOL_SHORT_PARAMS["rr_ratio"]
            signals.append(
                {
                    "bar": i + 1,
                    "direction": -1,
                    "entry": close_value,
                    "stop": float(stop),
                    "tp": float(tp),
                    "setup_session": "Any",
                    "setup_type": "Breakdown Volume Short",
                }
            )

    return signals


def strategy_signals(df: pd.DataFrame, strategy_id: str) -> list[dict[str, object]]:
    if strategy_id == "s5":
        return combined_signals(df, S5_PARAMS)
    if strategy_id == "ict_killzone_opt3":
        return killzone_opt3_signals(df, KILLZONE_PARAMS)
    if strategy_id in {"edge_ny_vol_long", "edge_breakdown_vol_short"}:
        return edge_strategy_signals(df, strategy_id)
    return []


def get_open_position(conn: sqlite3.Connection, symbol: str, strategy_id: str) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT *
        FROM positions
        WHERE status='OPEN' AND symbol=? AND strategy_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (symbol, strategy_id),
    ).fetchone()
    conn.row_factory = None
    return row


def open_position(
    conn: sqlite3.Connection,
    cfg: Config,
    strategy_id: str,
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
            symbol, side, entry_time_utc, entry_price, stop_price, tp_price, signal_bar_time_utc,
            status, strategy_id, strategy_name, setup_session, setup_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?)
        """,
        (
            cfg.symbol,
            side,
            entry_time.isoformat(),
            float(entry_price),
            float(stop_price),
            float(tp_price),
            signal_bar_time.isoformat(),
            strategy_id,
            strategy_name(strategy_id),
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


def _to_utc_iso(ts: str) -> str:
    return datetime.fromisoformat(ts).astimezone(timezone.utc).isoformat()


def insert_atm_signal(
    conn: sqlite3.Connection,
    symbol: str,
    signal: dict[str, object],
) -> bool:
    signal_key = str(signal.get("signal_key") or signal.get("signal_time") or "")
    if not signal_key:
        return False

    row = conn.execute(
        "SELECT 1 FROM atm_signals WHERE signal_key=? LIMIT 1",
        (signal_key,),
    ).fetchone()
    if row is not None:
        return False

    signal_time_utc = _to_utc_iso(str(signal["signal_time"]))
    created_time_utc = now_utc().isoformat()
    conn.execute(
        """
        INSERT INTO atm_signals(
            signal_key, symbol, direction, interaction, signal_time_utc, created_time_utc,
            entry_price, sl_price, tp1_price, tp2_price, rr_tp1, rr_tp2, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """,
        (
            signal_key,
            symbol,
            str(signal["direction"]),
            str(signal["interaction"]),
            signal_time_utc,
            created_time_utc,
            float(signal["entry"]),
            float(signal["sl"]),
            float(signal["tp1"]),
            float(signal["tp2"]),
            float(signal.get("rr1") or 0),
            float(signal.get("rr2") or 0),
        ),
    )
    conn.commit()
    return True


def get_active_atm_signals(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT *
        FROM atm_signals
        WHERE status IN ('OPEN', 'TP1_HIT')
        ORDER BY id
        """
    ).fetchall()
    conn.row_factory = None
    return rows


def _atm_hit_levels(row: sqlite3.Row | dict[str, object], price: float) -> tuple[bool, bool, bool]:
    direction = str(row["direction"]).upper()
    sl_price = float(row["sl_price"])
    tp1_price = float(row["tp1_price"])
    tp2_price = float(row["tp2_price"])

    if direction == "LONG":
        return price >= tp1_price, price >= tp2_price, price <= sl_price
    return price <= tp1_price, price <= tp2_price, price >= sl_price


def evaluate_atm_signal_progress(
    row: sqlite3.Row | dict[str, object],
    price: float,
    checked_at: datetime,
) -> dict[str, object]:
    hit_tp1, hit_tp2, hit_sl = _atm_hit_levels(row, price)
    checked_at_iso = checked_at.isoformat()
    updates: dict[str, object] = {}
    notifications: list[str] = []

    tp1_hit = bool(row["tp1_hit_time_utc"])
    tp2_hit = bool(row["tp2_hit_time_utc"])
    sl_hit = bool(row["sl_hit_time_utc"])
    max_stage = int(row["max_favorable_stage"] or 0)

    def mark_tp1(notify: bool) -> None:
        nonlocal tp1_hit, max_stage
        if tp1_hit:
            return
        updates["tp1_hit_time_utc"] = checked_at_iso
        updates["tp1_hit_price"] = float(price)
        updates["status"] = "TP1_HIT"
        updates["max_favorable_stage"] = max(max_stage, 1)
        tp1_hit = True
        max_stage = max(max_stage, 1)
        if notify:
            notifications.append("TP1")

    def finalize_tp2() -> None:
        nonlocal tp2_hit, max_stage
        if not tp1_hit:
            mark_tp1(notify=False)
        if tp2_hit:
            return
        updates["tp2_hit_time_utc"] = checked_at_iso
        updates["tp2_hit_price"] = float(price)
        updates["status"] = "CLOSED"
        updates["max_favorable_stage"] = 2
        updates["final_outcome"] = "TP2"
        updates["close_reason"] = "TP2"
        updates["close_price"] = float(price)
        updates["closed_time_utc"] = checked_at_iso
        tp2_hit = True
        max_stage = 2
        notifications.append("TP2")

    def finalize_sl() -> None:
        nonlocal sl_hit
        if not sl_hit:
            updates["sl_hit_time_utc"] = checked_at_iso
            updates["sl_hit_price"] = float(price)
            sl_hit = True
            notifications.append("SL")
        if max_stage >= 2 or tp2_hit:
            final_outcome = "TP2"
            close_reason = "SL_AFTER_TP2"
        elif max_stage >= 1 or tp1_hit:
            final_outcome = "TP1"
            close_reason = "SL_AFTER_TP1"
        else:
            final_outcome = "SL"
            close_reason = "SL"
        updates["status"] = "CLOSED"
        updates["final_outcome"] = final_outcome
        updates["close_reason"] = close_reason
        updates["close_price"] = float(price)
        updates["closed_time_utc"] = checked_at_iso

    if hit_tp2 and not tp2_hit:
        finalize_tp2()
        return {"updates": updates, "notifications": notifications}

    if hit_tp1 and not tp1_hit:
        mark_tp1(notify=True)

    if hit_sl and not sl_hit:
        finalize_sl()

    return {"updates": updates, "notifications": notifications}


def update_atm_signal_row(conn: sqlite3.Connection, signal_id: int, updates: dict[str, object]) -> None:
    if not updates:
        return
    columns = list(updates.keys())
    sql = ", ".join(f"{col}=?" for col in columns)
    values = [updates[col] for col in columns]
    values.append(signal_id)
    conn.execute(f"UPDATE atm_signals SET {sql} WHERE id=?", values)
    conn.commit()


def build_atm_progress_message(
    row: sqlite3.Row,
    level: str,
    price: float,
    updates: dict[str, object],
) -> str:
    direction = "📈 LONG" if str(row["direction"]).upper() == "LONG" else "📉 SHORT"
    signal_time = str(row["signal_time_utc"]).replace("T", " ")[:19]
    final_outcome = str(updates.get("final_outcome") or "")

    if level == "TP1":
        return (
            f"🎯 ATM TP1 已達成\n"
            f"方向: {direction}\n"
            f"標的: {row['symbol']}\n"
            f"目前價: `{price:.2f}`\n"
            f"TP1: `{float(row['tp1_price']):.2f}`\n"
            f"TP2: `{float(row['tp2_price']):.2f}`\n"
            f"SL: `{float(row['sl_price']):.2f}`\n"
            f"訊號時間: {signal_time}\n"
            f"狀態: 已達 TP1，繼續監看 TP2 / SL"
        )
    if level == "TP2":
        return (
            f"🏁 ATM TP2 已達成\n"
            f"方向: {direction}\n"
            f"標的: {row['symbol']}\n"
            f"目前價: `{price:.2f}`\n"
            f"本筆結果: `{final_outcome or 'TP2'}`\n"
            f"訊號時間: {signal_time}"
        )
    if final_outcome == "TP1":
        return (
            f"🛑 ATM SL 已觸發\n"
            f"方向: {direction}\n"
            f"標的: {row['symbol']}\n"
            f"目前價: `{price:.2f}`\n"
            f"本筆結果仍記為: `TP1`\n"
            f"原因: 先到 TP1，之後才碰到 SL"
        )
    if final_outcome == "TP2":
        return (
            f"🛑 ATM SL 已觸發\n"
            f"方向: {direction}\n"
            f"標的: {row['symbol']}\n"
            f"目前價: `{price:.2f}`\n"
            f"本筆結果仍記為: `TP2`\n"
            f"原因: 先到 TP2，之後才碰到 SL"
        )
    return (
        f"🛑 ATM SL 已達成\n"
        f"方向: {direction}\n"
        f"標的: {row['symbol']}\n"
        f"目前價: `{price:.2f}`\n"
        f"SL: `{float(row['sl_price']):.2f}`\n"
        f"本筆結果: `SL`"
    )


def fetch_atm_live_price(symbol: str, interval: str) -> float:
    r = requests.get(
        BINGX_KLINES_URL,
        params={"symbol": symbol, "interval": interval, "limit": 2},
        timeout=10,
    )
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") != 0:
        raise RuntimeError(f"BINGx error code={payload.get('code')} msg={payload.get('msg')}")
    rows = payload.get("data", [])
    if not rows:
        raise RuntimeError("empty BINGx price payload")
    latest = rows[-1]
    if not isinstance(latest, dict) or "close" not in latest:
        raise RuntimeError("unexpected BINGx price payload")
    return float(latest["close"])


def atm_signal_stats(conn: sqlite3.Connection, where_sql: str = "", params: tuple[object, ...] = ()) -> sqlite3.Row:
    conn.row_factory = sqlite3.Row
    sql = f"""
        SELECT
            COUNT(*) AS signals,
            SUM(CASE WHEN final_outcome IS NOT NULL THEN 1 ELSE 0 END) AS closed_signals,
            SUM(CASE WHEN final_outcome IN ('TP1', 'TP2') THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN final_outcome='SL' THEN 1 ELSE 0 END) AS final_sl,
            SUM(CASE WHEN final_outcome='TP1' THEN 1 ELSE 0 END) AS final_tp1,
            SUM(CASE WHEN final_outcome='TP2' THEN 1 ELSE 0 END) AS final_tp2,
            SUM(CASE WHEN tp1_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_tp1,
            SUM(CASE WHEN tp2_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_tp2,
            SUM(CASE WHEN sl_hit_time_utc IS NOT NULL THEN 1 ELSE 0 END) AS hit_sl,
            SUM(CASE WHEN status IN ('OPEN', 'TP1_HIT') THEN 1 ELSE 0 END) AS open_signals
        FROM atm_signals
        {where_sql}
    """
    row = conn.execute(sql, params).fetchone()
    conn.row_factory = None
    return row


def build_atm_summary_message(conn: sqlite3.Connection, now_tw: datetime) -> str:
    today_tw = now_tw.strftime("%Y-%m-%d")
    overall = atm_signal_stats(conn)
    today = atm_signal_stats(
        conn,
        "WHERE date(signal_time_utc, '+8 hours') = ?",
        (today_tw,),
    )

    def win_rate(row: sqlite3.Row) -> float:
        closed = int(row["closed_signals"] or 0)
        wins = int(row["wins"] or 0)
        return round((wins / closed * 100), 1) if closed else 0.0

    return (
        f"📊 ATM 夜間統計 {today_tw}\n"
        f"今日 signals: {int(today['signals'] or 0)}  已完成: {int(today['closed_signals'] or 0)}  開放中: {int(today['open_signals'] or 0)}\n"
        f"今日勝率: {win_rate(today):.1f}%  (TP1/TP2 視為 win)\n"
        f"今日最終結果: TP2 {int(today['final_tp2'] or 0)} / TP1 {int(today['final_tp1'] or 0)} / SL {int(today['final_sl'] or 0)}\n"
        f"今日觸及次數: TP1 {int(today['hit_tp1'] or 0)} / TP2 {int(today['hit_tp2'] or 0)} / SL {int(today['hit_sl'] or 0)}\n"
        f"歷史 signals: {int(overall['signals'] or 0)}  已完成: {int(overall['closed_signals'] or 0)}\n"
        f"歷史勝率: {win_rate(overall):.1f}%\n"
        f"歷史最終結果: TP2 {int(overall['final_tp2'] or 0)} / TP1 {int(overall['final_tp1'] or 0)} / SL {int(overall['final_sl'] or 0)}\n"
        f"歷史觸及次數: TP1 {int(overall['hit_tp1'] or 0)} / TP2 {int(overall['hit_tp2'] or 0)} / SL {int(overall['hit_sl'] or 0)}"
    )


def main():
    cfg = Config()
    conn = sqlite3.connect(cfg.db_path)
    init_db(conn)
    log_event(
        conn,
        "INFO",
        (
            f"bot started exchange={cfg.exchange} symbol={cfg.symbol} interval={cfg.interval} "
            f"strategies={','.join(ACTIVE_STRATEGIES)} loop={cfg.loop_seconds}s "
            f"atm_monitor={cfg.atm_monitor_seconds}s db={cfg.db_path} log={cfg.log_path}"
        ),
    )
    send_telegram(
        cfg,
        f"❤️ S5 + ATM 機器人啟動\n"
        f"S5 標的: {cfg.symbol} ({cfg.interval})\n"
        f"ATM 標的: {os.getenv('ATM_SYMBOL', 'NCSINASDAQ1002USD-USDT')} (BINGx 亞洲盤)\n"
        f"ATM 追價: 每 {cfg.atm_monitor_seconds:g} 秒檢查 TP1 / TP2 / SL\n"
        f"版本: ATM {ATM_VERSION}\n"
        f"時間: {fmt_ts(now_utc())}"
    )

    while True:
        try:
            tick_started = time.time()
            df = fetch_klines(cfg)
            LOGGER.info(
                "✅ 資料取得成功 exchange=%s symbol=%s bars=%s latest_ts=%s close=%.4f",
                cfg.exchange,
                cfg.symbol,
                len(df),
                df.index[-1].isoformat() if not df.empty else "N/A",
                float(df["close"].iloc[-1]) if not df.empty else 0.0,
            )
            if len(df) < 220:
                LOGGER.warning("insufficient bars=%s, sleeping %ss", len(df), cfg.loop_seconds)
                time.sleep(cfg.loop_seconds)
                continue

            # last row is the current in-progress candle
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

            closed_ts_iso = closed_ts.replace(tzinfo=timezone.utc).isoformat()

            for strategy_id in ACTIVE_STRATEGIES:
                pos = get_open_position(conn, cfg.symbol, strategy_id)
                if pos is not None:
                    side = pos["side"]
                    stop_px = float(pos["stop_price"])
                    tp_px = float(pos["tp_price"])
                    entry_px = float(pos["entry_price"])
                    entry_bar_ts = pos["entry_time_utc"][:16]  # YYYY-MM-DDTHH:MM

                    low = float(df["low"].iloc[closed_idx])
                    high = float(df["high"].iloc[closed_idx])
                    close = float(df["close"].iloc[closed_idx])

                    # Skip SL/TP check on the entry bar itself — price may wick
                    # briefly through SL/TP on the open bar without truly being hit
                    if closed_ts.isoformat()[:16] == entry_bar_ts:
                        LOGGER.debug("skip exit check on entry bar strategy=%s", strategy_id)
                        continue

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

                    if raw_exit is not None:
                        direction = 1 if side == "long" else -1
                        exit_px = apply_slippage(raw_exit, direction, is_entry=False)
                        pnl = close_position(conn, cfg, int(pos["id"]), side, entry_px, exit_px, reason)
                        msg = (
                            f"💰 平倉\n"
                            f"幣種: {cfg.symbol}\n"
                            f"策略: {strategy_name(strategy_id)}\n"
                            f"方向: {'做多' if side == 'long' else '做空'}\n"
                            f"原因: {reason}\n"
                            f"進場: {entry_px:.4f}\n"
                            f"出場: {exit_px:.4f}\n"
                            f"盈虧: {pnl:+.2f} USDT\n"
                            f"時間: {fmt_ts(now_utc())}"
                        )
                        log_event(conn, "TRADE_EXIT", msg)
                        send_telegram(cfg, msg)
                        # record close time for cooldown
                        db_set(conn, f"last_close_utc:{strategy_id}", now_utc().isoformat())

                pos = get_open_position(conn, cfg.symbol, strategy_id)
                last_signal_key = f"last_signal_bar_utc:{strategy_id}"
                last_processed_signal_ts = db_get(conn, last_signal_key, "")

                # 1-hour cooldown after last trade close
                last_close_str = db_get(conn, f"last_close_utc:{strategy_id}", "")
                if last_close_str:
                    elapsed = (now_utc() - datetime.fromisoformat(last_close_str)).total_seconds()
                    if elapsed < 3600:
                        LOGGER.debug("cooldown active strategy=%s remaining=%.0fs", strategy_id, 3600 - elapsed)
                        continue

                if pos is None and last_processed_signal_ts != closed_ts_iso:
                    signals = strategy_signals(df, strategy_id)
                    LOGGER.debug(
                        "generated signals strategy=%s count=%s closed_idx=%s",
                        strategy_id,
                        len(signals),
                        closed_idx,
                    )
                    target = [s for s in signals if int(s["bar"]) == open_idx]
                    if target:
                        s = target[0]
                        direction = int(s["direction"])
                        side = "long" if direction == 1 else "short"
                        entry_raw = float(df["open"].iloc[open_idx])
                        entry_px = apply_slippage(entry_raw, direction, is_entry=True)
                        stop_px = float(s["stop"])
                        tp_px = float(s["tp"])
                        setup_session = str(s.get("setup_session", ""))
                        setup_type = str(s.get("setup_type", ""))

                        if entry_px > 0 and stop_px > 0 and tp_px > 0:
                            open_position(
                                conn=conn,
                                cfg=cfg,
                                strategy_id=strategy_id,
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
                                f"幣種: {cfg.symbol}\n"
                                f"策略: {strategy_name(strategy_id)}\n"
                                f"方向: {'做多' if side == 'long' else '做空'}\n"
                                f"進場價: {entry_px:.4f}\n"
                                f"止損價: {stop_px:.4f}\n"
                                f"止盈價: {tp_px:.4f}\n"
                                f"Setup: {setup_session or '-'} / {setup_type or '-'}\n"
                                f"時間: {fmt_ts(now_utc())}"
                            )
                            log_event(conn, "TRADE_ENTRY", msg)
                            send_telegram(cfg, msg)

                    db_set(conn, last_signal_key, closed_ts_iso)
                else:
                    LOGGER.debug(
                        "no new signal processed strategy=%s flat=%s last_signal=%s",
                        strategy_id,
                        pos is None,
                        last_processed_signal_ts,
                    )

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


# ─────────────────────────────────────────────────────────────────
# ATM Asia Strategy — background thread
# Runs independently from the main crypto loop.
# Uses BINGx NQ-USDT, 1m (Asia KZ) / 5m (Tokyo KZ), TW time.
# ─────────────────────────────────────────────────────────────────
ATM_VERSION = "1.1.1-20260612"

def _atm_thread(cfg: Config) -> None:
    import threading
    try:
        from atm_asia_core import (
            ATMContext, ATMState, fetch_klines as atm_fetch,
            process_candle, should_daily_reset,
            build_range_locked_msg, build_ob_found_msg,
            build_tokyo_range_locked_msg, build_ob_invalidated_msg,
            build_interaction_msg, build_choch_confirmed_msg, build_ob_retest_msg,
            kill_zone_windows, is_trade_day, SYMBOL as ATM_SYMBOL, TW_TZ,
        )
    except ImportError as exc:
        LOGGER.error("[ATM] failed to import atm_asia_core: %s", exc)
        return

    LOGGER.info(
        "[ATM] ✅ loaded  version=%s  symbol=%s  mock=%s",
        ATM_VERSION, ATM_SYMBOL, os.getenv("ATM_USE_MOCK", "0"),
    )

    # ── auto-trade setup ────────────────────────────────────────────
    _auto_trade = cfg.atm_auto_trade and cfg.bingx_api_key and cfg.bingx_api_secret
    if _auto_trade:
        try:
            from bingx_trade import set_leverage
            set_leverage(cfg.bingx_api_key, cfg.bingx_api_secret)
            LOGGER.info("[ATM] Auto-trade ON — leverage set")
            send_telegram(cfg, "🤖 ATM Auto-Trade 啟動 (100x, 20 USDT/單)")
        except Exception as _e:
            LOGGER.error("[ATM] Auto-trade init failed: %s", _e)
            _auto_trade = False
    else:
        LOGGER.info("[ATM] Auto-trade OFF")

    conn = sqlite3.connect(cfg.db_path, timeout=30)
    init_db(conn)
    ctx      = ATMContext()
    history  = []
    seen_ts  = set()
    prev_state = ATMState.IDLE
    prev_tokyo_locked = False
    last_signal_key = ""
    weekend_paused = False
    first_run = True   # suppress notifications on startup to avoid replaying old state
    loop_sec = int(os.getenv("ATM_LOOP_SECONDS", "60"))

    while True:
        try:
            now_tw   = datetime.now(TW_TZ)
            if not is_trade_day(now_tw):
                if not weekend_paused:
                    LOGGER.info("[ATM] weekend pause active, resetting intraday state")
                    ctx.reset()
                    history.clear()
                    seen_ts.clear()
                    prev_state = ATMState.IDLE
                    prev_tokyo_locked = False
                    weekend_paused = True
                time.sleep(loop_sec)
                continue
            if weekend_paused:
                LOGGER.info("[ATM] weekday session resumed")
                weekend_paused = False
            windows  = kill_zone_windows(now_tw)
            t        = now_tw.time()

            interval = "1m"

            if should_daily_reset(ctx, now_tw):
                LOGGER.info("[ATM] daily reset")
                ctx.reset()
                history.clear()
                seen_ts.clear()
                prev_state = ATMState.IDLE
                prev_tokyo_locked = False
                first_run = True

            # 14:00 TW session timeout — give up if no signal has fired yet
            if (
                now_tw.hour == 14
                and now_tw.minute == 0
                and ctx.state not in {ATMState.IDLE, ATMState.SIGNAL_FIRED}
            ):
                LOGGER.info("[ATM] 14:00 session timeout  state=%s — resetting", ctx.state)
                ctx.reset()
                history.clear()
                seen_ts.clear()
                prev_state = ATMState.IDLE
                prev_tokyo_locked = False
                first_run = True

            candles = atm_fetch(ATM_SYMBOL, interval, limit=120)
            for c in candles[:-1]:
                if c.ts in seen_ts:
                    continue
                seen_ts.add(c.ts)
                history.append(c)

                signal = process_candle(c, ctx, history)

                # Tokyo range lock notification (state doesn't change, track separately)
                if ctx.tokyo_range_locked and not prev_tokyo_locked:
                    prev_tokyo_locked = True
                    if not first_run:
                        send_telegram(cfg, build_tokyo_range_locked_msg(ctx))

                # state-change notifications (range lock only — others come from process_candle)
                if ctx.state != prev_state:
                    LOGGER.info("[ATM] state %s → %s", prev_state, ctx.state)
                    if not first_run:
                        if ctx.state == ATMState.ASIA_RANGE_LOCKED and prev_state in {
                            ATMState.IDLE,
                            ATMState.ASIA_RANGE_FORMING,
                        }:
                            send_telegram(cfg, build_range_locked_msg(ctx))
                    prev_state = ctx.state

                if signal and not first_run:
                    # ── informational notifications (no DB write) ──
                    _NOTIFY_TYPES = {"OB_INVALIDATED", "INTERACTION_DETECTED", "CHOCH_CONFIRMED", "OB_RETEST"}
                    if signal.get("notification_type") in _NOTIFY_TYPES:
                        send_telegram(cfg, signal["telegram_message"])
                        continue

                    # ── trade signal ────────────────────────────────
                    signal_key = str(signal.get("signal_key") or signal.get("signal_time") or "")
                    if signal_key and signal_key == last_signal_key:
                        LOGGER.info("[ATM] duplicate signal skipped key=%s", signal_key)
                        continue
                    inserted = insert_atm_signal(conn, ATM_SYMBOL, signal)
                    if not inserted:
                        LOGGER.info("[ATM] signal already recorded in DB key=%s", signal_key)
                        last_signal_key = signal_key
                        continue
                    LOGGER.info(
                        "[ATM] SIGNAL %s  entry=%.2f  SL=%.2f  TP1=%.2f  TP2=%.2f",
                        signal["direction"], signal["entry"],
                        signal["sl"], signal["tp1"], signal["tp2"],
                    )
                    log_event(
                        conn,
                        "INFO",
                        (
                            f"[ATM] signal {signal['direction']} key={signal_key} "
                            f"entry={float(signal['entry']):.2f} sl={float(signal['sl']):.2f} "
                            f"tp1={float(signal['tp1']):.2f} tp2={float(signal['tp2']):.2f}"
                        ),
                    )
                    send_telegram(cfg, signal["telegram_message"])
                    last_signal_key = signal_key

                    # ── auto-trade ──────────────────────────────────
                    if _auto_trade:
                        try:
                            from bingx_trade import (
                                place_atm_trade,
                                format_trade_notification,
                                format_trade_skipped,
                                format_trade_error,
                            )
                            trade_results, trade_err = place_atm_trade(
                                bias   = signal["direction"],
                                entry  = float(signal["entry"]),
                                sl     = float(signal["sl"]),
                                tp1    = float(signal["tp1"]),
                                tp2    = float(signal["tp2"]),
                                api_key= cfg.bingx_api_key,
                                secret = cfg.bingx_api_secret,
                            )
                            if trade_results:
                                send_telegram(cfg, format_trade_notification(trade_results, trade_err))
                            else:
                                send_telegram(cfg, format_trade_skipped(trade_err or "unknown"))
                        except Exception as _te:
                            LOGGER.exception("[ATM] Auto-trade error: %s", _te)
                            send_telegram(cfg, format_trade_error(str(_te)))

            # After first batch: catch-up notification if bot started mid-session
            if first_run:
                if ctx.state in {ATMState.WAITING_RETEST, ATMState.WAITING_WICK}:
                    send_telegram(cfg, build_ob_found_msg(ctx) + "\n_[startup catch-up]_")
                elif ctx.state in {ATMState.WAITING_CHOCH, ATMState.WAITING_BREAKOUT_CONFIRM}:
                    send_telegram(cfg, build_range_locked_msg(ctx) + f"\n_[startup catch-up: 等待CHoCH ({ctx.state})]_")
            first_run = False  # after first batch, enable notifications

        except Exception as exc:
            LOGGER.exception("[ATM] loop error: %s", exc)
            send_telegram(cfg, f"⚠️ ATM 錯誤\n`{type(exc).__name__}: {exc}`")

        time.sleep(loop_sec)


def _atm_monitor_thread(cfg: Config) -> None:
    from atm_asia_core import SYMBOL as ATM_SYMBOL, TW_TZ, is_trade_day

    conn = sqlite3.connect(cfg.db_path, timeout=30)
    init_db(conn)
    last_error_message = ""
    last_error_logged_at = 0.0

    while True:
        try:
            now_tw = datetime.now(TW_TZ)
            summary_key = "atm_daily_summary_sent_date_tw"
            today_tw = now_tw.strftime("%Y-%m-%d")
            summary_due = (
                now_tw.hour > cfg.atm_daily_summary_hour
                or (
                    now_tw.hour == cfg.atm_daily_summary_hour
                    and now_tw.minute >= cfg.atm_daily_summary_minute
                )
            )
            if summary_due and db_get(conn, summary_key, "") != today_tw:
                summary_msg = build_atm_summary_message(conn, now_tw)
                send_telegram(cfg, summary_msg)
                log_event(conn, "INFO", f"[ATM] nightly summary sent date={today_tw}")
                db_set(conn, summary_key, today_tw)

            if not is_trade_day(now_tw):
                last_error_message = ""
                time.sleep(cfg.atm_monitor_seconds)
                continue

            active_rows = get_active_atm_signals(conn)
            if not active_rows:
                time.sleep(cfg.atm_monitor_seconds)
                continue

            price = fetch_atm_live_price(ATM_SYMBOL, cfg.atm_monitor_interval)
            checked_at = now_utc()
            last_error_message = ""

            for row in active_rows:
                progress = evaluate_atm_signal_progress(row, price, checked_at)
                updates = dict(progress["updates"])
                notifications = list(progress["notifications"])
                if not updates:
                    continue
                updates["last_price"] = float(price)
                updates["last_price_time_utc"] = checked_at.isoformat()
                update_atm_signal_row(conn, int(row["id"]), updates)
                for level in notifications:
                    msg = build_atm_progress_message(row, level, price, updates)
                    log_event(
                        conn,
                        "INFO",
                        (
                            f"[ATM] {level} signal_id={row['id']} price={price:.2f} "
                            f"final={updates.get('final_outcome') or row['final_outcome'] or ''}"
                        ),
                    )
                    send_telegram(cfg, msg)

        except Exception as exc:
            message = f"{type(exc).__name__}: {exc}"
            now_mono = time.monotonic()
            if message != last_error_message or now_mono - last_error_logged_at >= 60:
                LOGGER.warning("[ATM-MONITOR] %s", message)
                last_error_message = message
                last_error_logged_at = now_mono

        time.sleep(cfg.atm_monitor_seconds)


if __name__ == "__main__":
    import threading
    cfg_main = Config()
    atm_t = threading.Thread(target=_atm_thread, args=(cfg_main,), daemon=True, name="atm-asia")
    atm_t.start()
    atm_monitor_t = threading.Thread(
        target=_atm_monitor_thread,
        args=(cfg_main,),
        daemon=True,
        name="atm-monitor",
    )
    atm_monitor_t.start()
    LOGGER.info("[MAIN] ATM thread started  thread=%s", atm_t.name)
    LOGGER.info("[MAIN] ATM monitor thread started  thread=%s interval=%ss", atm_monitor_t.name, cfg_main.atm_monitor_seconds)
    main()
