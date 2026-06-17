#!/usr/bin/env python3
"""
ATM Strategy Backtest — compare BASE vs 5 optimization variants.

Variants:
  BASE  current logic, unchanged
  OPT1  dynamic invalidation level in WAITING_CHOCH
  OPT2  CHoCH body/range ratio >= 0.5 (displacement strength filter)
  OPT3  OB minimum body size >= 10 pts
  OPT4  OB must have a Fair Value Gap in the 8 candles after it
  OPT5  after SIGNAL_FIRED, no re-interaction same session
  OPT6  SWEEP TP1 = entry + 2R (fixed), TP2 = ref_high (original big target)
  OPT7  SWEEP TP1 = entry + 3R (fixed), TP2 = ref_high (original big target)

Usage:
  python atm_backtest.py              # last 60 days
  python atm_backtest.py --days 90
  python atm_backtest.py --days 30 --ob-min-body 15
"""
import argparse
import sys
import time as time_mod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

import requests

TW_TZ    = ZoneInfo("Asia/Taipei")
NY_TZ    = ZoneInfo("America/New_York")
SYMBOL   = "NCSINASDAQ1002USD-USDT"
BINGX    = "https://open-api.bingx.com/openApi/swap/v2/quote/klines"
TICK     = 0.25

# ─────────────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class Candle:
    ts: datetime
    open: float; high: float; low: float; close: float

    @property
    def is_bullish(self):  return self.close > self.open
    @property
    def body_size(self):   return abs(self.close - self.open)
    @property
    def body_ratio(self):
        span = self.high - self.low
        return self.body_size / span if span else 0.0
    @property
    def mid(self):         return (self.high + self.low) / 2.0

@dataclass
class Signal:
    ts:          datetime
    bias:        str          # 'LONG' | 'SHORT'
    interaction: str          # 'SWEEP' | 'BREAKOUT'
    entry:       float
    sl:          float
    tp1:         float
    tp2:         float
    rr1:         float
    rr2:         float

@dataclass
class Opts:
    dynamic_invalid:   bool  = False
    choch_body_filter: bool  = False
    ob_min_body:       float = 0.0
    require_fvg:       bool  = False
    no_reinteract:     bool  = False
    breakout_only:     bool  = False  # skip all SWEEP interactions
    sweep_tp1_r:       float = 0.0    # OPT6/7: fixed-R TP1 for SWEEP (0=use ref_high)
    # COMBSB: per-interaction overrides (-1 = not set, use global field)
    sweep_ob_min_body:    float = -1.0  # SWEEP-specific ob_min_body
    bo_dynamic_invalid:   bool  = False # BREAKOUT-specific dynamic_invalid
    bo_choch_body_filter: bool  = False # BREAKOUT-specific choch_body_filter

    def eff_dynamic(self, interaction: str) -> bool:
        if interaction == 'BREAKOUT':
            return self.dynamic_invalid or self.bo_dynamic_invalid
        return self.dynamic_invalid

    def eff_choch_filter(self, interaction: str) -> bool:
        if interaction == 'BREAKOUT':
            return self.choch_body_filter or self.bo_choch_body_filter
        return self.choch_body_filter

    def eff_ob_min_body(self, interaction: str) -> float:
        if interaction == 'SWEEP' and self.sweep_ob_min_body >= 0:
            return self.sweep_ob_min_body
        return self.ob_min_body

# ─────────────────────────────────────────────────────────────────────────────
# BingX data fetch with pagination
# ─────────────────────────────────────────────────────────────────────────────
def fetch_range(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[Candle]:
    """Paginate backwards from end_ms → start_ms (BingX returns newest-first)."""
    rows      = []
    cur_end   = end_ms
    prev_oldest = None

    while True:
        try:
            r = requests.get(BINGX, params={
                "symbol": symbol, "interval": interval,
                "limit": 1000, "endTime": cur_end,
            }, timeout=20)
            r.raise_for_status()
            payload = r.json()
        except Exception as e:
            print(f"  ⚠ fetch error ({interval}): {e}", file=sys.stderr)
            break
        if payload.get("code") != 0:
            print(f"  ⚠ API error code={payload.get('code')}", file=sys.stderr)
            break
        batch = payload.get("data") or []
        if not batch:
            print(f"  empty batch — no more data")
            break

        oldest = min(int(x["time"]) for x in batch)
        newest = max(int(x["time"]) for x in batch)

        rows.extend(batch)

        oldest_dt = datetime.fromtimestamp(oldest / 1000, tz=TW_TZ).strftime('%Y-%m-%d %H:%M')
        newest_dt = datetime.fromtimestamp(newest / 1000, tz=TW_TZ).strftime('%Y-%m-%d %H:%M')
        print(f"  [{interval}] page {len(rows)//1000:>3}  {oldest_dt} → {newest_dt}  ({len(batch)} bars)")

        # Safety: if oldest didn't move, BingX has no more history — stop
        if oldest == prev_oldest:
            print(f"  oldest unchanged — reached limit of available history")
            break
        prev_oldest = oldest

        if oldest <= start_ms:
            print(f"  reached start date — done")
            break

        cur_end = oldest - 1        # next page ends just before oldest seen
        time_mod.sleep(0.35)

    # deduplicate, filter to [start_ms, end_ms], sort ascending
    seen = set()
    out  = []
    for x in sorted(rows, key=lambda r: int(r["time"])):
        t = int(x["time"])
        if t in seen or t < start_ms or t > end_ms:
            continue
        seen.add(t)
        out.append(Candle(
            ts    = datetime.fromtimestamp(t / 1000, tz=TW_TZ),
            open  = float(x["open"]),
            high  = float(x["high"]),
            low   = float(x["low"]),
            close = float(x["close"]),
        ))
    return out

# ─────────────────────────────────────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────────────────────────────────────
def mod(t) -> int:
    return t.hour * 60 + t.minute

def windows(dt: datetime) -> dict:
    ny = dt.astimezone(NY_TZ)
    ut = dt.astimezone(ZoneInfo("UTC"))
    off = ny.hour - ut.hour
    if off >  12: off -= 24
    if off < -12: off += 24
    w = off == -5  # winter
    return {
        "as": (7 if w else 6) * 60,   # asia start
        "ae": (8 if w else 7) * 60,   # asia end
        "ts": (10 if w else 9) * 60,  # tokyo start
        "te": (11 if w else 10) * 60, # tokyo end
        "us": (22 * 60 + 30 if w else 21 * 60 + 30),
    }

def is_weekend(dt: datetime) -> bool:
    return dt.weekday() >= 5

# ─────────────────────────────────────────────────────────────────────────────
# ATM helpers
# ─────────────────────────────────────────────────────────────────────────────
def find_ob(candles: list[Candle], bias: str, up_to: int,
            lookback: int = 30, min_body: float = 0.0) -> Optional[dict]:
    start = max(0, up_to - lookback)
    for c in reversed(candles[start : up_to + 1]):
        if bias == 'LONG'  and not c.is_bullish and c.body_size >= min_body:
            return {"high": c.high, "low": c.low, "mid": c.mid, "ts": c.ts}
        if bias == 'SHORT' and     c.is_bullish and c.body_size >= min_body:
            return {"high": c.high, "low": c.low, "mid": c.mid, "ts": c.ts}
    return None

def has_fvg(candles: list[Candle], after: int, bias: str, window: int = 8) -> bool:
    end = min(len(candles), after + window + 1)
    for i in range(after + 2, end):
        if bias == 'LONG'  and candles[i].low  > candles[i-2].high: return True
        if bias == 'SHORT' and candles[i].high < candles[i-2].low:  return True
    return False

def ob_invalid(c: Candle, ob: dict, bias: str) -> bool:
    return c.close < ob["low"] if bias == 'LONG' else c.close > ob["high"]

def in_zone(c: Candle, ob: dict) -> bool:
    return c.low <= ob["high"] and c.high >= ob["low"]

def wick_reject(c: Candle, ob: dict, bias: str) -> bool:
    if c.body_size == 0:
        return False
    if bias == 'LONG':
        lw = min(c.open, c.close) - c.low
        return c.low <= ob["mid"] and c.close > ob["low"] and lw > c.body_size * 0.5
    uw = c.high - max(c.open, c.close)
    return c.high >= ob["mid"] and c.close < ob["high"] and uw > c.body_size * 0.5

def make_signal(c: Candle, bias: str, interaction: str,
                ob: dict, ref_high: float, ref_low: float,
                opts: 'Opts | None' = None) -> Signal:
    entry = c.close
    if bias == 'LONG':
        sl   = ob["low"]  - TICK
        risk = entry - sl
        if opts and opts.sweep_tp1_r > 0 and interaction == 'SWEEP':
            tp1 = entry + opts.sweep_tp1_r * risk   # fixed-R partial exit
            tp2 = ref_high                           # original big target
        else:
            tp1  = ref_high
            tp2r = entry + 2.0 * risk
            tp2  = tp2r if tp2r > tp1 else tp1 + risk
    else:
        sl   = ob["high"] + TICK
        risk = sl - entry
        if opts and opts.sweep_tp1_r > 0 and interaction == 'SWEEP':
            tp1 = entry - opts.sweep_tp1_r * risk   # fixed-R partial exit
            tp2 = ref_low                            # original big target
        else:
            tp1  = ref_low
            tp2r = entry - 2.0 * risk
            tp2  = tp2r if tp2r < tp1 else tp1 - risk
    rr1 = abs(tp1 - entry) / risk if risk else 0
    rr2 = abs(tp2 - entry) / risk if risk else 0
    return Signal(ts=c.ts, bias=bias, interaction=interaction,
                  entry=entry, sl=sl, tp1=tp1, tp2=tp2, rr1=rr1, rr2=rr2)

# ─────────────────────────────────────────────────────────────────────────────
# Day simulation (one variant)
# ─────────────────────────────────────────────────────────────────────────────
def sim_day(day_1m: list[Candle], day_5m: list[Candle], opts: Opts) -> list[Signal]:
    if not day_1m or is_weekend(day_1m[0].ts):
        return []

    w = windows(day_1m[0].ts)
    idx_1m = {id(c): i for i, c in enumerate(day_1m)}

    asia = [c for c in day_1m if w["as"] <= mod(c.ts.time()) < w["ae"]]
    if not asia:
        return []
    asia_h = max(c.high for c in asia)
    asia_l = min(c.low  for c in asia)

    tokyo = [c for c in day_1m if w["ts"] <= mod(c.ts.time()) < w["te"]]
    tok_h = max((c.high for c in tokyo), default=None)
    tok_l = min((c.low  for c in tokyo), default=None)
    tok_locked = bool(tok_h and tok_l and (tok_h > asia_h or tok_l < asia_l))
    ref_h = tok_h if tok_locked else asia_h
    ref_l = tok_l if tok_locked else asia_l

    post_asia = [c for c in day_1m
                 if w["ae"] <= mod(c.ts.time()) < w["us"]]

    state      = 'RANGE_LOCKED'
    bias       = None
    interaction = None
    ob         = None
    inter_h    = None   # interaction candle high
    inter_l    = None   # interaction candle low
    dyn_l      = None   # OPT1: running min low in WAITING_CHOCH
    dyn_h      = None   # OPT1: running max high in WAITING_CHOCH
    signals: list[Signal] = []

    for candle in post_asia:
        m          = mod(candle.ts.time())
        in_tok     = w["ts"] <= m < w["te"]
        post_tok   = m >= w["te"]

        # Per-candle ref range
        c_ref_h = ref_h
        c_ref_l = ref_l

        # ── RANGE_LOCKED ──────────────────────────────────────────
        if state == 'RANGE_LOCKED':
            if in_tok:
                continue
            if candle.high > c_ref_h:
                bias        = 'SHORT' if candle.close <= c_ref_h else 'LONG'
                interaction = 'SWEEP' if candle.close <= c_ref_h else 'BREAKOUT'
            elif candle.low < c_ref_l:
                bias        = 'LONG'  if candle.close >= c_ref_l else 'SHORT'
                interaction = 'SWEEP' if candle.close >= c_ref_l else 'BREAKOUT'
            else:
                continue
            # OPT BKOUT: skip sweep interactions entirely
            if opts.breakout_only and interaction == 'SWEEP':
                bias = None; continue
            inter_h = candle.high
            inter_l = candle.low
            dyn_l   = candle.low
            dyn_h   = candle.high
            state   = 'WAITING_CHOCH' if interaction == 'SWEEP' else 'WAITING_CONFIRM'

        # ── WAITING_BREAKOUT_CONFIRM (1 candle) ───────────────────
        elif state == 'WAITING_CONFIRM':
            recovered = (bias == 'LONG'  and candle.close < c_ref_h) or \
                        (bias == 'SHORT' and candle.close > c_ref_l)
            if recovered:
                bias        = 'SHORT' if bias == 'LONG' else 'LONG'
                interaction = 'SWEEP'
                # OPT BKOUT: recovery turns a BREAKOUT into SWEEP — skip it
                if opts.breakout_only:
                    state = 'RANGE_LOCKED'; bias = None; ob = None
                    continue
            else:
                interaction = 'BREAKOUT'
            inter_h = candle.high
            inter_l = candle.low
            dyn_l   = candle.low
            dyn_h   = candle.high
            state   = 'WAITING_CHOCH'

        # ── WAITING_CHOCH ─────────────────────────────────────────
        elif state == 'WAITING_CHOCH':
            # OPT1: dynamic invalidation (track running min/max)
            if opts.eff_dynamic(interaction):
                dyn_l = min(dyn_l, candle.low)
                dyn_h = max(dyn_h, candle.high)
                inv_l, inv_h = dyn_l, dyn_h
            else:
                inv_l, inv_h = inter_l, inter_h

            invalidated = (bias == 'LONG'  and candle.close < inv_l) or \
                          (bias == 'SHORT' and candle.close > inv_h)
            if invalidated:
                state = 'RANGE_LOCKED'; bias = None; ob = None
                continue

            choch = (bias == 'LONG'  and candle.close > inter_h) or \
                    (bias == 'SHORT' and candle.close < inter_l)
            if not choch:
                continue

            # OPT2: require strong displacement body
            if opts.eff_choch_filter(interaction) and candle.body_ratio < 0.5:
                continue

            # Find OB: post-Tokyo → try 5m, else 1m
            ob_min = opts.eff_ob_min_body(interaction)
            if post_tok and day_5m:
                cts = candle.ts.timestamp()
                j5 = max((i for i, c in enumerate(day_5m)
                           if c.ts.timestamp() <= cts), default=0)
                ob = find_ob(day_5m, bias, j5, min_body=ob_min)
                ob_src = day_5m
            else:
                j1 = idx_1m[id(candle)]
                ob = find_ob(day_1m, bias, j1, min_body=ob_min)
                ob_src = day_1m

            # OPT4: require FVG after the OB candle
            if ob and opts.require_fvg:
                ob_pos = next(
                    (i for i, c in enumerate(ob_src) if c.ts == ob["ts"]), None
                )
                if ob_pos is None or not has_fvg(ob_src, ob_pos, bias):
                    ob = None

            if ob:
                state = 'WAITING_RETEST'
            else:
                state = 'RANGE_LOCKED'; bias = None

        # ── WAITING_RETEST ────────────────────────────────────────
        elif state == 'WAITING_RETEST':
            if ob_invalid(candle, ob, bias):
                state = 'RANGE_LOCKED'; ob = None; bias = None
                continue
            if in_zone(candle, ob):
                state = 'WAITING_WICK'

        # ── WAITING_WICK ──────────────────────────────────────────
        elif state == 'WAITING_WICK':
            if ob_invalid(candle, ob, bias):
                state = 'RANGE_LOCKED'; ob = None; bias = None
                continue
            if wick_reject(candle, ob, bias):
                signals.append(make_signal(candle, bias, interaction, ob, c_ref_h, c_ref_l, opts))
                # OPT5: no re-interaction after first signal
                if opts.no_reinteract:
                    break
                state = 'RANGE_LOCKED'; ob = None; bias = None

    return signals

# ─────────────────────────────────────────────────────────────────────────────
# Outcome simulation
# ─────────────────────────────────────────────────────────────────────────────
def sim_outcome(sig: Signal, future: list[Candle]) -> str:
    tp1_hit = False
    for c in future:
        if sig.bias == 'LONG':
            bull = c.is_bullish
            if bull:
                if not tp1_hit and c.high >= sig.tp1: tp1_hit = True
                if tp1_hit and c.high >= sig.tp2:     return 'TP2'
                if c.low <= sig.sl:                   return 'TP1' if tp1_hit else 'SL'
            else:
                if c.low <= sig.sl:                   return 'TP1' if tp1_hit else 'SL'
                if not tp1_hit and c.high >= sig.tp1: tp1_hit = True
                if tp1_hit and c.high >= sig.tp2:     return 'TP2'
        else:  # SHORT
            bull = c.is_bullish
            if not bull:
                if not tp1_hit and c.low  <= sig.tp1: tp1_hit = True
                if tp1_hit and c.low <= sig.tp2:      return 'TP2'
                if c.high >= sig.sl:                  return 'TP1' if tp1_hit else 'SL'
            else:
                if c.high >= sig.sl:                  return 'TP1' if tp1_hit else 'SL'
                if not tp1_hit and c.low <= sig.tp1:  tp1_hit = True
                if tp1_hit and c.low <= sig.tp2:      return 'TP2'
    return 'TP1' if tp1_hit else 'OPEN'

# ─────────────────────────────────────────────────────────────────────────────
# Stats
# ─────────────────────────────────────────────────────────────────────────────
def calc_stats(trades: list[tuple]) -> dict:
    """trades: list of (Signal, outcome_str)"""
    closed = [(s, o) for s, o in trades if o != 'OPEN']
    wins   = [(s, o) for s, o in closed if o in ('TP1', 'TP2')]
    tp2    = sum(1 for _, o in closed if o == 'TP2')
    tp1    = sum(1 for _, o in closed if o == 'TP1')
    sl     = sum(1 for _, o in closed if o == 'SL')

    gross_p = gross_l = net = 0.0
    for sig, outcome in closed:
        if outcome == 'TP2':
            pnl = 0.75 * sig.rr1 + 0.25 * sig.rr2
        elif outcome == 'TP1':
            pnl = 0.75 * sig.rr1 - 0.25
        else:
            pnl = -1.0
        net += pnl
        if pnl > 0: gross_p += pnl
        else:       gross_l += abs(pnl)

    pf   = gross_p / gross_l if gross_l else float('inf')
    wr   = len(wins) / len(closed) * 100 if closed else 0.0
    avgr = net / len(closed) if closed else 0.0
    return dict(total=len(trades), closed=len(closed), wr=wr,
                tp2=tp2, tp1=tp1, sl=sl, pf=pf, avgr=avgr)

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
VARIANT_DEFS = {
    'BASE':   Opts(),
    'OPT1':   Opts(dynamic_invalid=True),
    'OPT2':   Opts(choch_body_filter=True),
    'OPT3':   None,   # ob_min_body set from args
    'OPT4':   Opts(require_fvg=True),
    'OPT5':   Opts(no_reinteract=True),
    'COMB13': None,   # OPT1 + OPT3, set from args
    'BKOUT':  Opts(), # BREAKOUT-only (SWEEP disabled in sim_day via flag)
    'OPT6':     Opts(sweep_tp1_r=2.0),  # SWEEP TP1=2R, TP2=ref_high
    'OPT7':     Opts(sweep_tp1_r=3.0),  # SWEEP TP1=3R, TP2=ref_high
    'COMB23':   None,  # OPT2 + OPT3 applied to all, set from args
    'COMBSB31': None,  # SWEEP=OPT3, BREAKOUT=OPT1, set from args
    'COMBSB32': None,  # SWEEP=OPT3, BREAKOUT=OPT2, set from args
}

VARIANT_DESC = {
    'BASE':     'current logic',
    'OPT1':     'dynamic CHoCH invalidation level',
    'OPT2':     'CHoCH body/range >= 50%',
    'OPT3':     'OB min body filter',
    'OPT4':     'OB must have FVG after it',
    'OPT5':     'no re-interaction after signal',
    'COMB13':   'OPT1 + OPT3 combined',
    'BKOUT':    'BREAKOUT signals only (no SWEEP)',
    'OPT6':     'SWEEP TP1=2R fixed, TP2=ref_high',
    'OPT7':     'SWEEP TP1=3R fixed, TP2=ref_high',
    'COMB23':   'OPT2 + OPT3 (CHoCH body + OB body, all signals)',
    'COMBSB31': 'SWEEP=OPT3 (ob_min_body), BREAKOUT=OPT1 (dynamic invalid)',
    'COMBSB32': 'SWEEP=OPT3 (ob_min_body), BREAKOUT=OPT2 (CHoCH body filter)',
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--days',         type=int,   default=60,
                    help='historical days to backtest (default 60)')
    ap.add_argument('--ob-min-body',  type=float, default=10.0,
                    help='OPT3 min OB body in points (default 10)')
    ap.add_argument('--fvg-window',   type=int,   default=8,
                    help='OPT4 FVG lookahead window (default 8 candles)')
    ap.add_argument('--variants',     type=str,   default='',
                    help='comma-separated variants to run, e.g. BASE,OPT3,COMBSB32 (default: all)')
    args = ap.parse_args()

    mb = args.ob_min_body
    VARIANT_DEFS['OPT3']     = Opts(ob_min_body=mb)
    VARIANT_DESC['OPT3']     = f'OB min body >= {mb} pts'
    VARIANT_DEFS['COMB13']   = Opts(dynamic_invalid=True, ob_min_body=mb)
    VARIANT_DESC['COMB13']   = f'OPT1 + OPT3 (dynamic invalid + ob_min_body={mb})'
    VARIANT_DEFS['BKOUT']    = Opts(breakout_only=True)
    VARIANT_DESC['BKOUT']    = 'BREAKOUT signals only (no SWEEP)'
    VARIANT_DEFS['COMB23']   = Opts(choch_body_filter=True, ob_min_body=mb)
    VARIANT_DESC['COMB23']   = f'OPT2 + OPT3 (CHoCH body + ob_min_body={mb})'
    VARIANT_DEFS['COMBSB31'] = Opts(sweep_ob_min_body=mb, bo_dynamic_invalid=True)
    VARIANT_DESC['COMBSB31'] = f'SWEEP=OPT3(ob>={mb}), BREAKOUT=OPT1(dynamic invalid)'
    VARIANT_DEFS['COMBSB32'] = Opts(sweep_ob_min_body=mb, bo_choch_body_filter=True)
    VARIANT_DESC['COMBSB32'] = f'SWEEP=OPT3(ob>={mb}), BREAKOUT=OPT2(CHoCH body)'

    now      = datetime.now(tz=TW_TZ)
    start    = now - timedelta(days=args.days)
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(now.timestamp()   * 1000)

    print(f"Backtest: {args.days} days  ({start.strftime('%Y-%m-%d')} → {now.strftime('%Y-%m-%d')})")
    print(f"Symbol  : {SYMBOL}")
    print()

    print("Fetching 1m klines…")
    k1m = fetch_range(SYMBOL, '1m', start_ms, end_ms)
    if k1m:
        print(f"  {len(k1m)} candles  ({k1m[0].ts.strftime('%Y-%m-%d')} → {k1m[-1].ts.strftime('%Y-%m-%d')})")
    else:
        print("  0 candles — no data available"); sys.exit(1)

    print("Fetching 5m klines…")
    k5m = fetch_range(SYMBOL, '5m', start_ms, end_ms)
    if k5m:
        print(f"  {len(k5m)} candles  ({k5m[0].ts.strftime('%Y-%m-%d')} → {k5m[-1].ts.strftime('%Y-%m-%d')})")
    else:
        print("  0 5m candles — will use 1m only")
    print()

    # Group by TW date
    def by_date(candles):
        d = {}
        for c in candles:
            d.setdefault(c.ts.strftime('%Y-%m-%d'), []).append(c)
        return d

    days1 = by_date(k1m)
    days5 = by_date(k5m)

    # Filter to requested variants only
    if args.variants:
        requested = [v.strip().upper() for v in args.variants.split(',')]
        unknown = [v for v in requested if v not in VARIANT_DEFS]
        if unknown:
            print(f"Unknown variants: {unknown}. Available: {list(VARIANT_DEFS)}")
            sys.exit(1)
        active_variants = {v: VARIANT_DEFS[v] for v in requested}
    else:
        active_variants = VARIANT_DEFS

    # Run simulation for all variants
    # variant → list of (Signal, outcome)
    results: dict[str, list[tuple]] = {v: [] for v in active_variants}

    for date_key in sorted(days1):
        d1 = days1[date_key]
        d5 = days5.get(date_key, [])
        for vname, opts in active_variants.items():
            sigs = sim_day(d1, d5, opts)
            for sig in sigs:
                results[vname].append((sig, None))   # outcome filled below

    # Fill outcomes using flat future candles
    ts_to_idx = {c.ts: i for i, c in enumerate(k1m)}
    for vname in active_variants:
        updated = []
        for sig, _ in results[vname]:
            i = ts_to_idx.get(sig.ts)
            future = k1m[i+1 : i+201] if i is not None else []
            outcome = sim_outcome(sig, future)
            updated.append((sig, outcome))
        results[vname] = updated

    # Print table
    print(f"{'Variant':<7} {'Trades':>6} {'Closed':>6} {'Win%':>6} "
          f"{'TP2':>4} {'TP1':>4} {'SL':>4} {'PF':>6} {'AvgR':>6}  Description")
    print("─" * 100)
    for vname, opts in active_variants.items():
        st = calc_stats(results[vname])
        pf = f"{st['pf']:.2f}" if st['pf'] != float('inf') else "  ∞"
        print(
            f"{vname:<9} {st['total']:>6} {st['closed']:>6} {st['wr']:>5.1f}% "
            f"{st['tp2']:>4} {st['tp1']:>4} {st['sl']:>4} {pf:>6} {st['avgr']:>+6.2f}R"
            f"  {VARIANT_DESC[vname]}"
        )

    print()
    # Breakdown by interaction type per variant
    print("── By interaction type (closed trades only) ──")
    print(f"{'Variant':<9} {'Type':<9} {'Closed':>6} {'Win%':>6} {'PF':>6} {'AvgR':>6}")
    print("─" * 50)
    for vname in active_variants:
        for itype in ('SWEEP', 'BREAKOUT'):
            subset = [(s, o) for s, o in results[vname]
                      if s.interaction == itype and o != 'OPEN']
            if not subset:
                continue
            st = calc_stats(subset)
            pf = f"{st['pf']:.2f}" if st['pf'] != float('inf') else "  ∞"
            print(f"{vname:<9} {itype:<9} {st['closed']:>6} {st['wr']:>5.1f}% {pf:>6} {st['avgr']:>+6.2f}R")

    # ── TP geometry analysis (BASE only — explains SWEEP vs BREAKOUT asymmetry) ──
    if 'BASE' in active_variants:
        print()
        print("── TP geometry — BASE (why SWEEP ≠ BREAKOUT) ──")
        print(f"{'Type':<9} {'n':>4}  {'AvgRisk':>8}  {'AvgTP1dist':>10}  {'AvgTP2dist':>10}  {'TP1/Risk':>8}")
        print("─" * 60)
        for itype in ('SWEEP', 'BREAKOUT'):
            sigs = [s for s, _ in results['BASE'] if s.interaction == itype]
            if not sigs:
                continue
            risks    = [abs(s.entry - s.sl)          for s in sigs]
            tp1_dist = [abs(s.tp1  - s.entry)        for s in sigs]
            tp2_dist = [abs(s.tp2  - s.entry)        for s in sigs]
            rr1s     = [s.rr1                         for s in sigs]
            avg_r    = sum(risks)    / len(risks)
            avg_tp1  = sum(tp1_dist) / len(tp1_dist)
            avg_tp2  = sum(tp2_dist) / len(tp2_dist)
            avg_rr1  = sum(rr1s)    / len(rr1s)
            print(f"{itype:<9} {len(sigs):>4}  {avg_r:>8.1f}  {avg_tp1:>10.1f}  {avg_tp2:>10.1f}  {avg_rr1:>8.2f}R")

    # OPEN count (not yet resolved in the 200-candle window)
    print()
    opens = {v: sum(1 for _, o in results[v] if o == 'OPEN') for v in active_variants}
    if any(opens.values()):
        print("Note — unresolved signals (still OPEN after 200 candles):")
        for v, n in opens.items():
            if n:
                print(f"  {v}: {n}")


if __name__ == '__main__':
    main()
