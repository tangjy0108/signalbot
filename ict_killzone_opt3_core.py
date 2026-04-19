"""
Python port of BTC_ICT_Killzone_opt3.pine for live paper monitoring.

The Pine strategy is evaluated on closed candles. Signals returned here use
``bar = confirm_bar + 1`` so the live bot can enter on the next candle open.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd


SESSION_TZ = "America/New_York"
SESSION_LONDON = "London"
SESSION_NY = "New York"


@dataclass(frozen=True)
class KillzoneParams:
    allow_weekend_trades: bool = False
    enable_london_reversal: bool = True
    enable_ny_reversal: bool = True
    enable_ny_continuation: bool = True
    force_flat_after_ny_window: bool = True
    bias_mode: str = "EMA Only"
    direction_mode: str = "Both"
    require_fvg_for_ny_long: bool = True
    rr_target: float = 2.0
    atr_length: int = 14
    displacement_atr_mult: float = 0.5
    sweep_buffer_atr_mult: float = 0.10
    stop_buffer_atr_mult: float = 0.20
    structure_lookback: int = 3
    confirm_bars: int = 10
    bias_fast_len: int = 20
    bias_slow_len: int = 50


DEFAULT_PARAMS = KillzoneParams()


def _in_session(minute_of_day: int, start_minute: int, end_minute: int) -> bool:
    if start_minute < end_minute:
        return start_minute <= minute_of_day < end_minute
    return minute_of_day >= start_minute or minute_of_day < end_minute


def _et_parts(ts: pd.Timestamp) -> tuple[int, int, int, int]:
    et = ts.tz_convert(SESSION_TZ)
    minute_of_day = et.hour * 60 + et.minute
    # Python Monday=0, Pine Monday=2. Use Python's scale internally.
    return et.hour, et.minute, minute_of_day, et.weekday()


def get_killzone_session(ts: pd.Timestamp, allow_weekend_trades: bool = False) -> str:
    _, _, minute_of_day, weekday = _et_parts(ts)
    trade_day = allow_weekend_trades or weekday <= 4
    if _in_session(minute_of_day, 20 * 60, 0):
        return "Asia"
    if trade_day and _in_session(minute_of_day, 2 * 60, 5 * 60):
        return SESSION_LONDON
    if trade_day and _in_session(minute_of_day, 8 * 60 + 30, 11 * 60):
        return SESSION_NY
    return "Off-Hours"


def should_force_flat_after_ny(ts: pd.Timestamp, params: KillzoneParams = DEFAULT_PARAMS) -> bool:
    if not params.force_flat_after_ny_window:
        return False
    _, _, minute_of_day, _ = _et_parts(ts)
    in_ny_window = _in_session(minute_of_day, 8 * 60 + 30, 11 * 60)
    return not in_ny_window and minute_of_day >= 11 * 60


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False).mean()


def _h1_ema_on_15m(df: pd.DataFrame, period: int) -> pd.Series:
    hourly_bucket = df.index.floor("60min")
    hourly_close = df["close"].groupby(hourly_bucket).last()
    hourly_ema = hourly_close.ewm(span=period, adjust=False).mean()
    # The hourly close is first known on the 15m bar that opens at HH:45.
    availability_index = hourly_ema.index + pd.Timedelta(minutes=45)
    available = pd.Series(hourly_ema.values, index=availability_index)
    return available.reindex(df.index, method="ffill")


def _daily_open(df: pd.DataFrame) -> pd.Series:
    day_bucket = df.index.floor("1D")
    opens = df["open"].groupby(day_bucket).first()
    return opens.reindex(day_bucket).set_axis(df.index)


def killzone_opt3_signals(
    df: pd.DataFrame,
    params: KillzoneParams = DEFAULT_PARAMS,
) -> list[dict[str, Any]]:
    if df.empty:
        return []

    df = df.copy().sort_index()
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    open_arr = df["open"].to_numpy(dtype=float)
    high_arr = df["high"].to_numpy(dtype=float)
    low_arr = df["low"].to_numpy(dtype=float)
    close_arr = df["close"].to_numpy(dtype=float)
    atr_arr = compute_atr(df, params.atr_length).to_numpy(dtype=float)
    fast_ema = _h1_ema_on_15m(df, params.bias_fast_len).to_numpy(dtype=float)
    slow_ema = _h1_ema_on_15m(df, params.bias_slow_len).to_numpy(dtype=float)
    daily_open = _daily_open(df).to_numpy(dtype=float)

    n = len(df)
    allow_longs = params.direction_mode != "Short Only"
    allow_shorts = params.direction_mode != "Long Only"

    asia_high = np.nan
    asia_low = np.nan
    or_high = np.nan
    or_low = np.nan

    wait_bull_confirm = False
    wait_bear_confirm = False
    bull_sweep_extreme = np.nan
    bear_sweep_extreme = np.nan
    bull_sweep_bar: int | None = None
    bear_sweep_bar: int | None = None
    bull_setup_session = ""
    bear_setup_session = ""
    bull_setup_type = ""
    bear_setup_type = ""

    prev_in_asia = False
    prev_in_london = False
    prev_in_ny_window = False

    signals: list[dict[str, Any]] = []

    # Leave the final row alone because Binance returns the in-progress candle.
    for i in range(n - 1):
        ts = df.index[i]
        _, _, minute_of_day, weekday = _et_parts(ts)
        trade_day = params.allow_weekend_trades or weekday <= 4
        in_asia = _in_session(minute_of_day, 20 * 60, 0)
        in_london = trade_day and _in_session(minute_of_day, 2 * 60, 5 * 60)
        in_ny_window = trade_day and _in_session(minute_of_day, 8 * 60 + 30, 11 * 60)
        in_opening_range = trade_day and _in_session(minute_of_day, 9 * 60 + 30, 10 * 60)
        after_opening_range = in_ny_window and minute_of_day >= 10 * 60

        asia_start = in_asia and not prev_in_asia
        london_start = in_london and not prev_in_london
        ny_window_start = in_ny_window and not prev_in_ny_window

        if asia_start:
            asia_high = high_arr[i]
            asia_low = low_arr[i]
        elif in_asia:
            asia_high = high_arr[i] if np.isnan(asia_high) else max(asia_high, high_arr[i])
            asia_low = low_arr[i] if np.isnan(asia_low) else min(asia_low, low_arr[i])

        if ny_window_start:
            or_high = np.nan
            or_low = np.nan

        if in_opening_range:
            or_high = high_arr[i] if np.isnan(or_high) else max(or_high, high_arr[i])
            or_low = low_arr[i] if np.isnan(or_low) else min(or_low, low_arr[i])

        if london_start or ny_window_start:
            wait_bull_confirm = False
            wait_bear_confirm = False
            bull_sweep_extreme = np.nan
            bear_sweep_extreme = np.nan
            bull_sweep_bar = None
            bear_sweep_bar = None
            bull_setup_session = ""
            bear_setup_session = ""
            bull_setup_type = ""
            bear_setup_type = ""

        atr = atr_arr[i]
        if not np.isfinite(atr) or atr <= 0:
            prev_in_asia = in_asia
            prev_in_london = in_london
            prev_in_ny_window = in_ny_window
            continue

        sweep_buffer = atr * params.sweep_buffer_atr_mult
        stop_buffer = atr * params.stop_buffer_atr_mult
        body_size = abs(close_arr[i] - open_arr[i])

        if params.bias_mode == "No Bias Filter":
            bull_bias = True
            bear_bias = True
        elif params.bias_mode == "EMA Only":
            bull_bias = fast_ema[i] > slow_ema[i]
            bear_bias = fast_ema[i] < slow_ema[i]
        else:
            bull_bias = close_arr[i] > daily_open[i] and fast_ema[i] > slow_ema[i]
            bear_bias = close_arr[i] < daily_open[i] and fast_ema[i] < slow_ema[i]

        if i > params.structure_lookback:
            prev_highest = np.nanmax(high_arr[i - params.structure_lookback:i])
            prev_lowest = np.nanmin(low_arr[i - params.structure_lookback:i])
        else:
            prev_highest = np.nan
            prev_lowest = np.nan

        bull_mss = np.isfinite(prev_highest) and close_arr[i] > prev_highest
        bear_mss = np.isfinite(prev_lowest) and close_arr[i] < prev_lowest
        bull_displacement = close_arr[i] > open_arr[i] and body_size >= atr * params.displacement_atr_mult
        bear_displacement = close_arr[i] < open_arr[i] and body_size >= atr * params.displacement_atr_mult
        bull_fvg = i > 2 and low_arr[i] > high_arr[i - 2]
        bear_fvg = i > 2 and high_arr[i] < low_arr[i - 2]
        bull_confirm = bull_displacement and bull_mss
        bear_confirm = bear_displacement and bear_mss

        if allow_longs and params.enable_london_reversal and in_london and np.isfinite(asia_low) and bull_bias:
            if low_arr[i] < asia_low - sweep_buffer:
                wait_bull_confirm = True
                wait_bear_confirm = False
                bull_setup_session = SESSION_LONDON
                bull_setup_type = "London Reversal"
                bear_setup_session = ""
                bear_setup_type = ""
                bull_sweep_extreme = low_arr[i] if np.isnan(bull_sweep_extreme) else min(bull_sweep_extreme, low_arr[i])
                bull_sweep_bar = i

        if allow_shorts and params.enable_london_reversal and in_london and np.isfinite(asia_high) and bear_bias:
            if high_arr[i] > asia_high + sweep_buffer:
                wait_bear_confirm = True
                wait_bull_confirm = False
                bear_setup_session = SESSION_LONDON
                bear_setup_type = "London Reversal"
                bull_setup_session = ""
                bull_setup_type = ""
                bear_sweep_extreme = high_arr[i] if np.isnan(bear_sweep_extreme) else max(bear_sweep_extreme, high_arr[i])
                bear_sweep_bar = i

        if allow_longs and params.enable_ny_reversal and after_opening_range and np.isfinite(or_low) and bull_bias:
            if low_arr[i] < or_low - sweep_buffer and close_arr[i] > or_low:
                wait_bull_confirm = True
                wait_bear_confirm = False
                bull_setup_session = SESSION_NY
                bull_setup_type = "NY OR Reversal"
                bear_setup_session = ""
                bear_setup_type = ""
                bull_sweep_extreme = low_arr[i] if np.isnan(bull_sweep_extreme) else min(bull_sweep_extreme, low_arr[i])
                bull_sweep_bar = i

        if allow_shorts and params.enable_ny_reversal and after_opening_range and np.isfinite(or_high) and bear_bias:
            if high_arr[i] > or_high + sweep_buffer and close_arr[i] < or_high:
                wait_bear_confirm = True
                wait_bull_confirm = False
                bear_setup_session = SESSION_NY
                bear_setup_type = "NY OR Reversal"
                bull_setup_session = ""
                bull_setup_type = ""
                bear_sweep_extreme = high_arr[i] if np.isnan(bear_sweep_extreme) else max(bear_sweep_extreme, high_arr[i])
                bear_sweep_bar = i

        if wait_bull_confirm and bull_sweep_bar is not None and i - bull_sweep_bar > params.confirm_bars:
            wait_bull_confirm = False
            bull_sweep_extreme = np.nan
            bull_sweep_bar = None
            bull_setup_session = ""
            bull_setup_type = ""

        if wait_bear_confirm and bear_sweep_bar is not None and i - bear_sweep_bar > params.confirm_bars:
            wait_bear_confirm = False
            bear_sweep_extreme = np.nan
            bear_sweep_bar = None
            bear_setup_session = ""
            bear_setup_type = ""

        emitted = False
        if wait_bull_confirm and bull_confirm:
            ny_long_ok = bull_setup_session != SESSION_NY or not params.require_fvg_for_ny_long or bull_fvg
            long_entry = close_arr[i]
            long_stop = bull_sweep_extreme - stop_buffer
            if ny_long_ok and np.isfinite(long_stop) and long_entry > long_stop:
                signals.append(
                    {
                        "bar": i + 1,
                        "direction": 1,
                        "entry": float(long_entry),
                        "stop": float(long_stop),
                        "tp": float(long_entry + (long_entry - long_stop) * params.rr_target),
                        "setup_session": bull_setup_session,
                        "setup_type": bull_setup_type,
                    }
                )
                emitted = True
            wait_bull_confirm = False
            bull_sweep_extreme = np.nan
            bull_sweep_bar = None
            bull_setup_session = ""
            bull_setup_type = ""

        if not emitted and wait_bear_confirm and bear_confirm:
            short_entry = close_arr[i]
            short_stop = bear_sweep_extreme + stop_buffer
            if np.isfinite(short_stop) and short_entry < short_stop:
                signals.append(
                    {
                        "bar": i + 1,
                        "direction": -1,
                        "entry": float(short_entry),
                        "stop": float(short_stop),
                        "tp": float(short_entry - (short_stop - short_entry) * params.rr_target),
                        "setup_session": bear_setup_session,
                        "setup_type": bear_setup_type,
                    }
                )
                emitted = True
            wait_bear_confirm = False
            bear_sweep_extreme = np.nan
            bear_sweep_bar = None
            bear_setup_session = ""
            bear_setup_type = ""

        if not emitted and params.enable_ny_continuation and after_opening_range:
            if (
                allow_longs
                and bull_bias
                and np.isfinite(or_high)
                and close_arr[i] > or_high
                and i > 1
                and close_arr[i - 1] <= or_high
                and bull_confirm
                and (not params.require_fvg_for_ny_long or bull_fvg)
            ):
                long_entry = close_arr[i]
                long_stop = min(or_low, low_arr[i - 1], low_arr[i - 2]) - stop_buffer
                if np.isfinite(long_stop) and long_entry > long_stop:
                    signals.append(
                        {
                            "bar": i + 1,
                            "direction": 1,
                            "entry": float(long_entry),
                            "stop": float(long_stop),
                            "tp": float(long_entry + (long_entry - long_stop) * params.rr_target),
                            "setup_session": SESSION_NY,
                            "setup_type": "NY OR Continuation",
                        }
                    )
                    emitted = True

            if (
                not emitted
                and allow_shorts
                and bear_bias
                and np.isfinite(or_low)
                and close_arr[i] < or_low
                and i > 1
                and close_arr[i - 1] >= or_low
                and bear_confirm
            ):
                short_entry = close_arr[i]
                short_stop = max(or_high, high_arr[i - 1], high_arr[i - 2]) + stop_buffer
                if np.isfinite(short_stop) and short_entry < short_stop:
                    signals.append(
                        {
                            "bar": i + 1,
                            "direction": -1,
                            "entry": float(short_entry),
                            "stop": float(short_stop),
                            "tp": float(short_entry - (short_stop - short_entry) * params.rr_target),
                            "setup_session": SESSION_NY,
                            "setup_type": "NY OR Continuation",
                        }
                    )

        prev_in_asia = in_asia
        prev_in_london = in_london
        prev_in_ny_window = in_ny_window

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str]] = set()
    for signal in signals:
        key = (int(signal["bar"]), int(signal["direction"]), str(signal.get("setup_type", "")))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(signal)
    return deduped
