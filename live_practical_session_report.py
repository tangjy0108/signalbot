import math
from dataclasses import dataclass
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from research import combined_signals, compute_market_state

PARAMS = {
    'lookback': 10,
    'fvg_window': 20,
    'rsi_period': 10,
    'rsi_ob': 65,
    'rsi_os': 35,
    'atr_mult_sl': 1.0,
    'rr_ratio': 1.5,
}

ASSETS = {
    'BTC': 'btc_15min.csv',
    'ETH': 'eth_15min.csv',
    'ADA': 'ada_15min.csv',
}

# More live-like assumptions
INITIAL_CAPITAL = 100_000.0
NOTIONAL_PCT = 0.20       # 20% equity notional per trade, no leverage
FEE_PER_SIDE = 0.0004     # 4 bps each side
SLIPPAGE_PER_SIDE = 0.0002  # 2 bps each side
MAX_HOLD_BARS = 96        # 24h max hold

REPORT_PATH = 'live_practical_session_report.md'
SUMMARY_CSV = 'live_practical_session_summary.csv'


@dataclass
class Trade:
    asset: str
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    direction: int
    session: str
    market_state: str
    notional: float
    ret_pct: float
    pnl: float
    hold_bars: int
    reason: str
    entry_price: float
    exit_price: float
    stop_price: float
    tp_price: float


def utc_session(hour: int) -> str:
    if 0 <= hour <= 7:
        return 'asia'
    if 8 <= hour <= 12:
        return 'london'
    if 13 <= hour <= 16:
        return 'overlap'
    if 17 <= hour <= 21:
        return 'ny'
    return 'late'


def apply_slippage(price: float, direction: int, is_entry: bool) -> float:
    # long: pay up on entry, sell lower on exit
    # short: sell lower on entry, buy higher on exit
    if direction == 1 and is_entry:
        return price * (1 + SLIPPAGE_PER_SIDE)
    if direction == 1 and not is_entry:
        return price * (1 - SLIPPAGE_PER_SIDE)
    if direction == -1 and is_entry:
        return price * (1 - SLIPPAGE_PER_SIDE)
    return price * (1 + SLIPPAGE_PER_SIDE)


def run_live_like_backtest(asset: str, df: pd.DataFrame) -> Tuple[List[Trade], Dict[str, float], pd.DataFrame]:
    df = df.copy()
    ms, _, _ = compute_market_state(df)
    signals = combined_signals(df, PARAMS)

    sig_map = {int(s['bar']): s for s in signals if 0 <= int(s['bar']) < len(df)}

    equity = INITIAL_CAPITAL
    equity_curve = [equity]
    trades: List[Trade] = []

    position = 0
    entry_idx = -1
    entry_px = 0.0
    stop_px = 0.0
    tp_px = 0.0
    shares = 0.0
    entry_state = 'unknown'
    entry_session = 'unknown'

    open_arr = df['open'].values
    high_arr = df['high'].values
    low_arr = df['low'].values
    close_arr = df['close'].values
    idx = df.index

    for i in range(1, len(df)):
        # manage open trade
        if position != 0:
            hold = i - entry_idx
            exit_reason = None
            raw_exit = None

            if hold >= MAX_HOLD_BARS:
                raw_exit = close_arr[i]
                exit_reason = 'timeout'
            else:
                if position == 1:
                    hit_sl = low_arr[i] <= stop_px
                    hit_tp = high_arr[i] >= tp_px
                    if hit_sl and hit_tp:
                        raw_exit = stop_px
                        exit_reason = 'both_hit_stop_first'
                    elif hit_sl:
                        raw_exit = stop_px
                        exit_reason = 'stop'
                    elif hit_tp:
                        raw_exit = tp_px
                        exit_reason = 'tp'
                else:
                    hit_sl = high_arr[i] >= stop_px
                    hit_tp = low_arr[i] <= tp_px
                    if hit_sl and hit_tp:
                        raw_exit = stop_px
                        exit_reason = 'both_hit_stop_first'
                    elif hit_sl:
                        raw_exit = stop_px
                        exit_reason = 'stop'
                    elif hit_tp:
                        raw_exit = tp_px
                        exit_reason = 'tp'

            if raw_exit is not None:
                exit_px = apply_slippage(raw_exit, position, is_entry=False)
                if position == 1:
                    gross_ret = (exit_px - entry_px) / entry_px
                else:
                    gross_ret = (entry_px - exit_px) / entry_px

                net_ret = gross_ret - 2 * FEE_PER_SIDE
                pnl = shares * entry_px * net_ret
                equity += pnl

                trades.append(Trade(
                    asset=asset,
                    entry_time=idx[entry_idx],
                    exit_time=idx[i],
                    direction=position,
                    session=entry_session,
                    market_state=entry_state,
                    notional=shares * entry_px,
                    ret_pct=net_ret,
                    pnl=pnl,
                    hold_bars=hold,
                    reason=exit_reason,
                    entry_price=float(entry_px),
                    exit_price=float(exit_px),
                    stop_price=float(stop_px),
                    tp_price=float(tp_px),
                ))

                position = 0
                entry_idx = -1
                entry_px = stop_px = tp_px = shares = 0.0

        # new entry only if flat
        if position == 0 and i in sig_map:
            s = sig_map[i]
            direction = int(s['direction'])
            raw_entry = float(open_arr[i])
            if raw_entry <= 0:
                equity_curve.append(equity)
                continue

            entry = apply_slippage(raw_entry, direction, is_entry=True)
            sl = float(s['stop'])
            tp = float(s['tp'])
            if entry <= 0 or sl <= 0 or tp <= 0:
                equity_curve.append(equity)
                continue

            # fixed notional per trade
            notional = equity * NOTIONAL_PCT
            if notional < 10:
                equity_curve.append(equity)
                continue

            shares = notional / entry
            position = direction
            entry_idx = i
            entry_px = entry
            stop_px = sl
            tp_px = tp
            entry_state = str(ms.iloc[i]) if hasattr(ms, 'iloc') else str(ms[i])
            entry_session = utc_session(int(idx[i].hour))

        equity_curve.append(equity)

    # close at last bar
    if position != 0:
        final_exit = apply_slippage(float(close_arr[-1]), position, is_entry=False)
        gross_ret = (final_exit - entry_px) / entry_px if position == 1 else (entry_px - final_exit) / entry_px
        net_ret = gross_ret - 2 * FEE_PER_SIDE
        pnl = shares * entry_px * net_ret
        equity += pnl
        trades.append(Trade(
            asset=asset,
            entry_time=idx[entry_idx],
            exit_time=idx[-1],
            direction=position,
            session=entry_session,
            market_state=entry_state,
            notional=shares * entry_px,
            ret_pct=net_ret,
            pnl=pnl,
            hold_bars=int(len(df) - 1 - entry_idx),
            reason='end',
            entry_price=float(entry_px),
            exit_price=float(final_exit),
            stop_price=float(stop_px),
            tp_price=float(tp_px),
        ))

    tdf = pd.DataFrame([t.__dict__ for t in trades]) if trades else pd.DataFrame()

    if tdf.empty:
        return trades, {
            'n_trades': 0,
            'win_rate': 0.0,
            'profit_factor': 0.0,
            'avg_ret_pct': 0.0,
            'total_return': 0.0,
            'max_drawdown': 0.0,
            'avg_trades_per_day': 0.0,
        }, tdf

    tdf['side'] = np.where(tdf['direction'] == 1, 'long', 'short')
    tdf['entry_day'] = pd.to_datetime(tdf['entry_time']).dt.date

    wins = tdf[tdf['pnl'] > 0]['pnl'].sum()
    losses = tdf[tdf['pnl'] <= 0]['pnl'].sum()
    pf = float(wins / abs(losses)) if losses != 0 else np.inf

    # reconstruct approximate equity curve from trades
    eq = INITIAL_CAPITAL * (1 + (NOTIONAL_PCT * tdf['ret_pct'])).cumprod()
    rolling_max = eq.cummax()
    mdd = float(((eq - rolling_max) / (rolling_max + 1e-12)).min())

    daily_counts = tdf.groupby('entry_day').size()
    stats = {
        'n_trades': int(len(tdf)),
        'win_rate': float((tdf['pnl'] > 0).mean()),
        'profit_factor': pf,
        'avg_ret_pct': float(tdf['ret_pct'].mean()),
        'total_return': float(eq.iloc[-1] / INITIAL_CAPITAL - 1),
        'max_drawdown': mdd,
        'avg_trades_per_day': float(daily_counts.mean()),
        'median_trades_per_day': float(daily_counts.median()),
        'p90_trades_per_day': float(daily_counts.quantile(0.9)),
        'start': str(df.index.min()),
        'end': str(df.index.max()),
        'bars': int(len(df)),
    }

    return trades, stats, tdf


def main():
    all_rows = []
    report_sections = []

    for asset, path in ASSETS.items():
        df = pd.read_csv(path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').sort_index()

        _, stats, tdf = run_live_like_backtest(asset, df)

        row = {'asset': asset, **stats}
        all_rows.append(row)

        if not tdf.empty:
            session_side = tdf.groupby(['session', 'side']).agg(
                trades=('pnl', 'count'),
                win_rate=('pnl', lambda s: (s > 0).mean()),
                avg_ret_pct=('ret_pct', 'mean'),
                total_pnl=('pnl', 'sum'),
            ).sort_values('total_pnl', ascending=False)

            daily = tdf.groupby('entry_day').size().rename('trades')
            daily_desc = daily.describe(percentiles=[0.5, 0.9]).to_frame('value')
        else:
            session_side = pd.DataFrame()
            daily_desc = pd.DataFrame()

        report_sections.append((asset, stats, session_side, daily_desc))

    summary_df = pd.DataFrame(all_rows)
    summary_df.to_csv(SUMMARY_CSV, index=False)

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write('# Live-Practical Session/Side Report (BTC/ETH/ADA)\n\n')
        f.write('## Assumptions (closer to live)\n')
        f.write(f'- Initial capital: {INITIAL_CAPITAL}\n')
        f.write(f'- Fixed notional per trade: {NOTIONAL_PCT:.0%} of current equity\n')
        f.write(f'- Fee per side: {FEE_PER_SIDE:.04%}\n')
        f.write(f'- Slippage per side: {SLIPPAGE_PER_SIDE:.04%}\n')
        f.write(f'- Max hold: {MAX_HOLD_BARS} bars ({MAX_HOLD_BARS/4:.1f}h)\n')
        f.write('- Signal source: research.py / combined_signals with S5 params\n\n')

        f.write('## Cross-asset headline\n')
        f.write(summary_df.to_markdown(index=False))
        f.write('\n\n')

        for asset, stats, session_side, daily_desc in report_sections:
            f.write(f'## {asset}\n')
            f.write(f"- Window: {stats['start']} -> {stats['end']}\n")
            f.write(f"- Trades: {stats['n_trades']}\n")
            f.write(f"- Avg trades/day: {stats['avg_trades_per_day']:.2f} (median {stats['median_trades_per_day']:.2f}, p90 {stats['p90_trades_per_day']:.2f})\n\n")

            f.write(f'### {asset} session × side\n')
            if session_side.empty:
                f.write('- no trades\n\n')
            else:
                f.write(session_side.to_markdown())
                f.write('\n\n')

            f.write(f'### {asset} daily trades distribution\n')
            if daily_desc.empty:
                f.write('- no trades\n\n')
            else:
                f.write(daily_desc.to_markdown())
                f.write('\n\n')

        f.write('## Practical usage checklist\n')
        f.write('- Prefer sessions/side cells with stable positive avg_ret_pct and enough trades.\n')
        f.write('- Disable cells with persistent negative avg_ret_pct.\n')
        f.write('- Start paper trading for 2-4 weeks before live capital.\n')
        f.write('- Keep per-trade notional <= 20% equity and set hard daily loss cap.\n')

    print('report', REPORT_PATH)
    print('summary', SUMMARY_CSV)
    print(summary_df)


if __name__ == '__main__':
    main()
