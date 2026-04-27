"""
Research module - provides signal generation and market state analysis
"""
from s5_strategy_core import combined_signals as s5_combined_signals
import numpy as np
import pandas as pd


def combined_signals(df: pd.DataFrame, params: dict):
    """Wrapper around S5 strategy signals"""
    return s5_combined_signals(df, params)


def compute_market_state(df: pd.DataFrame):
    """
    Compute current market state (trend direction, volatility regime)
    Returns: (market_state_array, trend_strength, volatility)
    """
    close = df['close'].values
    n = len(df)
    
    # Simple trend: compare 20-bar MA vs 50-bar MA
    ma20 = pd.Series(close).rolling(20).mean().values if len(df) >= 20 else np.full(n, np.nan)
    ma50 = pd.Series(close).rolling(50).mean().values if len(df) >= 50 else np.full(n, np.nan)
    
    market_state = np.array(['neutral'] * n, dtype=object)
    trend_strength = np.zeros(n)
    
    for i in range(n):
        if np.isnan(ma20[i]) or np.isnan(ma50[i]):
            market_state[i] = 'neutral'
            trend_strength[i] = 0.0
        elif ma20[i] > ma50[i]:
            market_state[i] = 'uptrend'
            trend_strength[i] = (ma20[i] - ma50[i]) / ma50[i] * 100
        else:
            market_state[i] = 'downtrend'
            trend_strength[i] = (ma50[i] - ma20[i]) / ma50[i] * 100
    
    # Volatility: simple ATR-based measure
    volatility = pd.Series(df['high'].values - df['low'].values).rolling(14).mean().values
    if volatility is None:
        volatility = np.full(n, 0.0)
    
    return market_state, trend_strength, volatility
