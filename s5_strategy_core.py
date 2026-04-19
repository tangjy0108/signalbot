"""
Standalone S5 core logic extracted from research.py.
This file exists so live_s5_24h_bot.py can run without importing research.py.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([h - l, (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.ewm(span=period, adjust=False).mean()


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(span=period, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(span=period, adjust=False).mean()
    rs = gain / (loss + 1e-9)
    return 100 - 100 / (1 + rs)


def find_swing_highs_lows(df: pd.DataFrame, lookback: int = 10):
    highs = df["high"].values
    lows = df["low"].values
    n = len(df)
    swing_high = np.full(n, np.nan)
    swing_low = np.full(n, np.nan)
    for i in range(lookback, n - lookback):
        window_h = highs[max(0, i - lookback): i + lookback + 1]
        window_l = lows[max(0, i - lookback): i + lookback + 1]
        if highs[i] == window_h.max():
            swing_high[i] = highs[i]
        if lows[i] == window_l.min():
            swing_low[i] = lows[i]
    return swing_high, swing_low


def combined_signals(df: pd.DataFrame, params: dict):
    lookback = params.get("lookback", 15)
    fvg_window = params.get("fvg_window", 30)
    rsi_period = params.get("rsi_period", 14)
    rsi_ob = params.get("rsi_ob", 70)
    rsi_os = params.get("rsi_os", 30)
    atr_mult_sl = params.get("atr_mult_sl", 1.5)
    rr_ratio = params.get("rr_ratio", 2.0)

    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    atr = compute_atr(df).values
    rsi = compute_rsi(df["close"], rsi_period).values
    swing_h, swing_l = find_swing_highs_lows(df, lookback)

    last_sh = np.nan
    last_sl_val = np.nan
    bos_bull = np.zeros(n, dtype=bool)
    bos_bear = np.zeros(n, dtype=bool)
    for i in range(lookback, n):
        if not np.isnan(swing_h[i]):
            last_sh = swing_h[i]
        if not np.isnan(swing_l[i]):
            last_sl_val = swing_l[i]
        if not np.isnan(last_sh) and close[i] > last_sh:
            bos_bull[i] = True
        if not np.isnan(last_sl_val) and close[i] < last_sl_val:
            bos_bear[i] = True

    bull_fvg_active = np.zeros(n, dtype=bool)
    bear_fvg_active = np.zeros(n, dtype=bool)
    for i in range(2, n):
        if high[i - 2] < low[i]:
            end = min(i + fvg_window, n)
            bull_fvg_active[i:end] = True
        if low[i - 2] > high[i]:
            end = min(i + fvg_window, n)
            bear_fvg_active[i:end] = True

    signals = []
    for i in range(lookback + 3, n - 1):
        if bos_bull[i] and bull_fvg_active[i] and rsi[i] < rsi_ob:
            sl = close[i] - atr_mult_sl * atr[i]
            risk = close[i] - sl
            if risk > 0:
                signals.append(
                    {"bar": i + 1, "direction": 1, "entry": close[i], "stop": sl, "tp": close[i] + rr_ratio * risk}
                )
        elif bos_bear[i] and bear_fvg_active[i] and rsi[i] > rsi_os:
            sl = close[i] + atr_mult_sl * atr[i]
            risk = sl - close[i]
            if risk > 0:
                signals.append(
                    {"bar": i + 1, "direction": -1, "entry": close[i], "stop": sl, "tp": close[i] - rr_ratio * risk}
                )

    seen = set()
    deduped = []
    for s in signals:
        if s["bar"] not in seen:
            deduped.append(s)
            seen.add(s["bar"])
    return deduped
