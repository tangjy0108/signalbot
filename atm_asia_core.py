"""
ATM Model — Asia Session Strategy
Based on Adamtrade ATM Model + ICT SMC concepts
Exchange: BINGx | Symbol: NCSINASDAQ1002USD-USDT (env: ATM_SYMBOL)
Timeframe: 1m (Asia Kill Zone 06:00-07:00 TW), 5m (Tokyo Kill Zone 09:00-10:00 TW)
Winter time (US DST off, Nov~Mar): all windows shift +1h automatically
"""

import os
import logging
import requests
import numpy as np
from datetime import datetime, time, timedelta
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
from zoneinfo import ZoneInfo

log = logging.getLogger("atm_asia")

# ─────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────
TW_TZ      = ZoneInfo("Asia/Taipei")
SYMBOL     = os.getenv("ATM_SYMBOL", "NCSINASDAQ1002USD-USDT")
BINGX_BASE = "https://open-api.bingx.com"
USE_MOCK   = os.getenv("ATM_USE_MOCK", "0") == "1"   # set "1" to use mock data for testing

_SUMMER_WINDOWS = {
    "asia_start": time(6, 0), "asia_end": time(7, 0),
    "tokyo_start": time(9, 0), "tokyo_end": time(10, 0),
    "us_start": time(21, 30),
}
_WINTER_WINDOWS = {
    "asia_start": time(7, 0), "asia_end": time(8, 0),
    "tokyo_start": time(10, 0), "tokyo_end": time(11, 0),
    "us_start": time(22, 30),
}


def _is_us_winter(dt: datetime) -> bool:
    m = dt.month
    return m <= 3 or m >= 11


def kill_zone_windows(dt: datetime) -> dict:
    return _WINTER_WINDOWS if _is_us_winter(dt) else _SUMMER_WINDOWS


def is_trade_day(dt: datetime) -> bool:
    return dt.astimezone(TW_TZ).weekday() <= 4


# ─────────────────────────────────────────────────────────────────
# Data model
# ─────────────────────────────────────────────────────────────────
class ATMState(str, Enum):
    IDLE              = "IDLE"
    ASIA_RANGE_FORMING = "ASIA_RANGE_FORMING"
    ASIA_RANGE_LOCKED  = "ASIA_RANGE_LOCKED"
    WAITING_RETEST    = "WAITING_RETEST"
    WAITING_WICK      = "WAITING_WICK"
    SIGNAL_FIRED      = "SIGNAL_FIRED"


class Bias(str, Enum):
    LONG  = "LONG"
    SHORT = "SHORT"


class InteractionType(str, Enum):
    SWEEP    = "SWEEP"      # wick through, body back → reversal
    BREAKOUT = "BREAKOUT"   # body close through → continuation


@dataclass
class Candle:
    ts:     datetime
    open:   float
    high:   float
    low:    float
    close:  float
    volume: float = 0.0

    @property
    def body_high(self) -> float: return max(self.open, self.close)
    @property
    def body_low(self) -> float:  return min(self.open, self.close)
    @property
    def is_bullish(self) -> bool: return self.close >= self.open
    @property
    def upper_wick(self) -> float: return self.high - self.body_high
    @property
    def lower_wick(self) -> float: return self.body_low - self.low
    @property
    def body_size(self) -> float:  return abs(self.close - self.open)


@dataclass
class OrderBlock:
    high:        float
    low:         float
    bias:        Bias
    source_time: datetime
    valid:       bool = True

    @property
    def mid(self) -> float: return (self.high + self.low) / 2


@dataclass
class ATMContext:
    state:            ATMState = ATMState.IDLE
    asia_high:        float = 0.0
    asia_low:         float = float("inf")
    asia_range_locked: bool = False
    tokyo_high:       float = 0.0
    tokyo_low:        float = float("inf")
    tokyo_range_locked: bool = False
    ref_high:         float = 0.0    # active range high when setup was triggered
    ref_low:          float = 0.0    # active range low when setup was triggered
    interaction:      Optional[InteractionType] = None
    bias:             Optional[Bias] = None
    ob:               Optional[OrderBlock] = None
    displaced:        bool = False             # True once price moves away from OB after interaction
    entry:            Optional[float] = None
    sl:               Optional[float] = None
    tp1:              Optional[float] = None   # Range High/Low target
    tp2:              Optional[float] = None   # 1:2 R:R target
    signal_sent:      bool = False
    last_interaction_side: Optional[str] = None
    interaction_rearmed: bool = False
    us_expired:       bool = False   # True after US session opens; cleared by daily reset
    checklist: dict = field(default_factory=lambda: {
        "time_filter":   False,
        "asia_range":    False,
        "interaction":   False,
        "ob_found":      False,
        "retest":        False,
        "wick_rejection": False,
    })

    def reset(self):
        self.__init__()


# ─────────────────────────────────────────────────────────────────
# BINGx data fetcher  (live or mock)
# ─────────────────────────────────────────────────────────────────
def fetch_klines(symbol: str, interval: str, limit: int = 120) -> List[Candle]:
    if USE_MOCK:
        return _mock_klines(interval)
    url = f"{BINGX_BASE}/openApi/swap/v2/quote/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(url, params=params, timeout=10)
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") != 0:
        raise RuntimeError(f"BINGx error code={payload.get('code')} msg={payload.get('msg')}")
    rows = payload.get("data", [])
    candles = sorted([
        Candle(
            ts=datetime.fromtimestamp(int(row["time"]) / 1000, tz=TW_TZ),
            open=float(row["open"]), high=float(row["high"]),
            low=float(row["low"]),   close=float(row["close"]),
            volume=float(row.get("volume", 0)),
        )
        for row in rows if isinstance(row, dict)
    ], key=lambda c: c.ts)
    log.info("✅ [ATM] fetch OK symbol=%s interval=%s bars=%s latest_close=%.2f",
             symbol, interval, len(candles), candles[-1].close if candles else 0)
    return candles


# ─────────────────────────────────────────────────────────────────
# Mock data — LONG setup (Sweep of Asia Low)
#
# Scenario:
#   06:00-06:59  Asia Range forms strictly between H=21430  L=21385
#                (all 60 candles stay within this range, no wick outside)
#   07:01        Sweep below 21385: wick→21368, body closes at 21392 → LONG bias
#   07:02        Last RED candle before displacement → this becomes the OB (21375-21395)
#   07:03        Displacement UP begins (bullish engulf)
#   07:04        Continuation candle, FVG forms above 21395
#   07:05~07:07  Pullback begins, price drifts back toward OB
#   07:08        Price enters OB zone (low touches 21390)
#   07:09        WICK REJECTION: wick goes to 21370 (< ob.low 21375), body closes 21400 → SIGNAL LONG
#   07:10        Price shoots up to confirm
# ─────────────────────────────────────────────────────────────────
def _mock_klines(interval: str) -> List[Candle]:
    now  = datetime.now(TW_TZ)
    base = now.replace(hour=6, minute=0, second=0, microsecond=0)
    step = timedelta(minutes=1 if interval == "1m" else 5)

    # Asia Range candles: contained strictly within 21385-21430, no wick outside
    asia_candles = []
    for i in range(60):
        mid  = 21405 + np.sin(i * 0.3) * 10    # oscillate around 21405
        span = 8 + abs(np.sin(i * 0.7)) * 5
        o = round(mid + np.sin(i) * 3, 2)
        h = round(min(mid + span, 21428), 2)    # hard cap below 21430
        l = round(max(mid - span, 21387), 2)    # hard cap above 21385
        c = round(mid - np.sin(i * 0.5) * 2, 2)
        asia_candles.append((o, h, l, c))

    # Post-KZ candles (07:00 onward)
    post_candles = [
        (21410, 21420, 21395, 21405),   # 07:00 — quiet, no interaction
        (21405, 21412, 21368, 21392),   # 07:01 — SWEEP LOW: wick→21368 < 21385, body 21392 > 21385
        (21392, 21398, 21375, 21382),   # 07:02 — red candle → this is the OB (21375-21398)
        (21382, 21428, 21378, 21422),   # 07:03 — bullish displacement (engulf, body > OB.high)
        (21422, 21442, 21415, 21438),   # 07:04 — continuation, FVG between 07:02.high(21398) & 07:04.low(21415)
        (21438, 21445, 21425, 21430),   # 07:05 — slight pullback
        (21430, 21435, 21410, 21415),   # 07:06 — deeper pullback
        (21415, 21420, 21395, 21400),   # 07:07 — approaching OB zone
        (21400, 21405, 21390, 21398),   # 07:08 — enters OB zone (low=21390, OB.low=21375)
        (21398, 21402, 21370, 21400),   # 07:09 — WICK REJECTION: wick→21370 < OB.low 21375, body 21400 > OB.low
        (21400, 21455, 21398, 21450),   # 07:10 — price shoots up
    ]

    raw = asia_candles + post_candles
    return [
        Candle(ts=base + step * i, open=float(o), high=float(h), low=float(l), close=float(c))
        for i, (o, h, l, c) in enumerate(raw)
    ]


# ─────────────────────────────────────────────────────────────────
# Detection logic
# ─────────────────────────────────────────────────────────────────
def detect_interaction(
    candle: Candle, asia_high: float, asia_low: float
) -> Optional[Tuple[InteractionType, Bias]]:
    """
    ATM rule: use CLOSE PRICE (收盤價) to determine Sweep vs Breakout.
    Sweep    = wick crosses level, close stays back → reversal bias
    Breakout = close confirms beyond level → continuation bias
    """
    if candle.high > asia_high:
        if candle.close <= asia_high:
            return InteractionType.SWEEP, Bias.SHORT
        return InteractionType.BREAKOUT, Bias.LONG
    if candle.low < asia_low:
        if candle.close >= asia_low:
            return InteractionType.SWEEP, Bias.LONG
        return InteractionType.BREAKOUT, Bias.SHORT
    return None


def interaction_side(candle: Candle, asia_high: float, asia_low: float) -> Optional[str]:
    above = candle.high > asia_high
    below = candle.low < asia_low
    if above and below:
        return "both"
    if above:
        return "above"
    if below:
        return "below"
    return None


def is_inside_asia_range(candle: Candle, asia_high: float, asia_low: float) -> bool:
    return candle.high <= asia_high and candle.low >= asia_low


def find_ob(candles: List[Candle], bias: Bias, lookback: int = 10) -> Optional[OrderBlock]:
    """
    Finds the last opposite-color candle before the displacement.
    LONG  → last bearish (red) candle in lookback window
    SHORT → last bullish (green) candle in lookback window
    """
    for c in reversed(candles[-lookback:]):
        if bias == Bias.LONG and not c.is_bullish:
            return OrderBlock(high=c.high, low=c.low, bias=bias, source_time=c.ts)
        if bias == Bias.SHORT and c.is_bullish:
            return OrderBlock(high=c.high, low=c.low, bias=bias, source_time=c.ts)
    return None


def is_in_ob_zone(candle: Candle, ob: OrderBlock) -> bool:
    return candle.low <= ob.high and candle.high >= ob.low


def ob_invalidated(candle: Candle, ob: OrderBlock) -> bool:
    """OB fails when close price confirms through it (strict CHoCH rule: 收盤價判斷)."""
    if ob.bias == Bias.LONG:
        return candle.close < ob.low
    return candle.close > ob.high


def detect_wick_rejection(candle: Candle, ob: OrderBlock) -> bool:
    """
    ATM rule: wick goes deep into OB (at least to OB midpoint),
    but body closes back on the entry side (body must NOT close outside OB).
    Wick must also be at least 50% of body size to confirm rejection strength.
    """
    if ob.bias == Bias.LONG:
        wick_deep   = candle.low <= ob.mid     # wick reaches at least OB midpoint
        body_holds  = candle.close > ob.low    # body stays above OB bottom
        wick_strong = candle.body_size > 0 and candle.lower_wick > candle.body_size * 0.5
        return wick_deep and body_holds and wick_strong
    else:
        wick_deep   = candle.high >= ob.mid    # wick reaches at least OB midpoint
        body_holds  = candle.close < ob.high   # body stays below OB top
        wick_strong = candle.body_size > 0 and candle.upper_wick > candle.body_size * 0.5
        return wick_deep and body_holds and wick_strong


def _reset_setup_progress(ctx: ATMContext) -> None:
    ctx.interaction = None
    ctx.bias = None
    ctx.ob = None
    ctx.displaced = False
    ctx.entry = None
    ctx.sl = None
    ctx.tp1 = None
    ctx.tp2 = None
    ctx.ref_high = 0.0
    ctx.ref_low = 0.0
    ctx.signal_sent = False
    ctx.checklist["interaction"] = False
    ctx.checklist["ob_found"] = False
    ctx.checklist["retest"] = False
    ctx.checklist["wick_rejection"] = False


def _start_interaction_setup(
    candle: Candle,
    ctx: ATMContext,
    history: List[Candle],
    interaction: InteractionType,
    bias: Bias,
    side: str,
) -> None:
    _reset_setup_progress(ctx)
    # Capture active reference range at setup time (Tokyo if locked, else Asia)
    ctx.ref_high = ctx.tokyo_high if ctx.tokyo_range_locked else ctx.asia_high
    ctx.ref_low  = ctx.tokyo_low  if ctx.tokyo_range_locked else ctx.asia_low
    ctx.interaction = interaction
    ctx.bias = bias
    ctx.last_interaction_side = side
    ctx.interaction_rearmed = False
    ctx.checklist["interaction"] = True
    log.info(f"[ATM] {ctx.interaction.value} → {ctx.bias.value} (ref H={ctx.ref_high:.2f} L={ctx.ref_low:.2f})")
    ob = find_ob(history, ctx.bias)
    if ob:
        ctx.ob = ob
        ctx.checklist["ob_found"] = True
        ctx.state = ATMState.WAITING_RETEST
        log.info(f"[ATM] OB  H={ob.high:.2f}  L={ob.low:.2f}")
    else:
        ctx.state = ATMState.ASIA_RANGE_LOCKED
        log.warning("[ATM] interaction detected but no OB found")


# ─────────────────────────────────────────────────────────────────
# Main state machine — call this on every closed candle
# ─────────────────────────────────────────────────────────────────
def process_candle(
    candle: Candle,
    ctx: ATMContext,
    history: List[Candle],
) -> Optional[dict]:
    """
    Feed each closed candle.  Returns a signal dict when entry fires, else None.
    `history` should be the list of all candles seen so far (for OB lookback).
    """
    now_tw  = candle.ts.astimezone(TW_TZ)
    if not is_trade_day(now_tw):
        return None
    windows = kill_zone_windows(now_tw)
    t       = now_tw.time()

    # ── US session expiry: stop monitoring at NY open ────────────
    if ctx.us_expired:
        return None
    if t >= windows["us_start"]:
        notification = None
        if ctx.state in (ATMState.WAITING_RETEST, ATMState.WAITING_WICK):
            notification = _build_us_expired_msg(ctx, candle)
        _reset_setup_progress(ctx)
        ctx.state      = ATMState.IDLE
        ctx.us_expired = True
        log.info("[ATM] US session opened — monitoring ended for today")
        return notification

    in_asia_kz  = windows["asia_start"]  <= t < windows["asia_end"]
    in_tokyo_kz = windows["tokyo_start"] <= t < windows["tokyo_end"]
    post_asia   = t >= windows["asia_end"]
    post_tokyo  = t >= windows["tokyo_end"]

    # ── Phase 1: collect Asia range ──────────────────────────────
    if in_asia_kz:
        ctx.state     = ATMState.ASIA_RANGE_FORMING
        ctx.asia_high = max(ctx.asia_high, candle.high)
        ctx.asia_low  = min(ctx.asia_low,  candle.low)
        ctx.checklist["time_filter"] = True
        return None

    # ── Lock Asia range at Kill Zone close ───────────────────────
    if not ctx.asia_range_locked and post_asia and ctx.asia_high > 0:
        ctx.asia_range_locked = True
        ctx.state             = ATMState.ASIA_RANGE_LOCKED
        ctx.checklist["asia_range"] = True
        log.info(f"[ATM] Asia Range locked  H={ctx.asia_high:.2f}  L={ctx.asia_low:.2f}")

    if not ctx.asia_range_locked:
        return None

    # ── Phase 1.5: collect Tokyo range (only if no active setup yet) ─
    if in_tokyo_kz and not ctx.tokyo_range_locked:
        ctx.tokyo_high = max(ctx.tokyo_high, candle.high)
        ctx.tokyo_low  = min(ctx.tokyo_low,  candle.low)
        if ctx.state == ATMState.ASIA_RANGE_LOCKED:
            return None   # wait for Tokyo KZ to close before detecting interaction

    # ── Lock Tokyo range after Tokyo KZ closes ───────────────────
    if post_tokyo and not ctx.tokyo_range_locked and ctx.tokyo_high > 0:
        tokyo_extends = ctx.tokyo_high > ctx.asia_high or ctx.tokyo_low < ctx.asia_low
        if tokyo_extends:
            ctx.tokyo_range_locked = True
            log.info(f"[ATM] Tokyo Range locked  H={ctx.tokyo_high:.2f}  L={ctx.tokyo_low:.2f}")
        else:
            # Tokyo KZ stayed inside Asia range — keep Asia as reference
            log.info(f"[ATM] Tokyo Range inside Asia — using Asia  TH={ctx.tokyo_high:.2f} TL={ctx.tokyo_low:.2f} AH={ctx.asia_high:.2f} AL={ctx.asia_low:.2f}")

    # ── Determine active reference range ─────────────────────────
    ref_high = ctx.tokyo_high if ctx.tokyo_range_locked else ctx.asia_high
    ref_low  = ctx.tokyo_low  if ctx.tokyo_range_locked else ctx.asia_low

    # ── Phase 2: watch for interaction ───────────────────────────
    if ctx.state == ATMState.ASIA_RANGE_LOCKED:
        result = detect_interaction(candle, ref_high, ref_low)
        if result:
            side = interaction_side(candle, ref_high, ref_low)
            if side:
                interaction, bias = result
                _start_interaction_setup(candle, ctx, history, interaction, bias, side)
        return None

    # ── Phase 3: wait for displacement then retest ───────────────
    if ctx.state == ATMState.WAITING_RETEST:
        if ob_invalidated(candle, ctx.ob):
            log.warning("[ATM] OB invalidated — trying Reversal OB")
            old_bias = ctx.bias
            old_ob   = ctx.ob
            rev_bias = Bias.SHORT if ctx.bias == Bias.LONG else Bias.LONG
            rev_ob   = find_ob(history, rev_bias, lookback=5)
            if rev_ob:
                ctx.bias      = rev_bias
                ctx.ob        = rev_ob
                ctx.displaced = False
                log.info(f"[ATM] Reversal OB  H={rev_ob.high:.2f}  L={rev_ob.low:.2f}")
            else:
                _reset_setup_progress(ctx)
                ctx.state = ATMState.ASIA_RANGE_LOCKED  # keep watching like soup does
                log.info("[ATM] No reversal OB — back to ASIA_RANGE_LOCKED")
            return build_ob_invalidated_msg(old_bias, old_ob, candle, "等待回踩中", rev_ob)

        # Step A: wait for displacement away from OB before accepting a retest
        if not ctx.displaced:
            if ctx.bias == Bias.LONG and candle.close > ctx.ob.high:
                ctx.displaced = True
                log.info(f"[ATM] Displacement confirmed (close={candle.close:.2f} > OB.high={ctx.ob.high:.2f})")
            elif ctx.bias == Bias.SHORT and candle.close < ctx.ob.low:
                ctx.displaced = True
                log.info(f"[ATM] Displacement confirmed (close={candle.close:.2f} < OB.low={ctx.ob.low:.2f})")
            if not ctx.displaced:
                return None
            # just confirmed displacement — fall through to check OB zone on same candle

        # Step B: price returns to OB zone after displacement
        if is_in_ob_zone(candle, ctx.ob):
            ctx.state = ATMState.WAITING_WICK
            ctx.checklist["retest"] = True
            log.info(f"[ATM] Price retested OB zone @ {candle.close:.2f}")
            # fall through — check wick rejection on same candle
        else:
            return None

    # ── Phase 4: wick rejection → fire signal ───────────────────
    if ctx.state == ATMState.WAITING_WICK:
        if ob_invalidated(candle, ctx.ob):
            log.warning("[ATM] OB invalidated during wick wait — back to ASIA_RANGE_LOCKED")
            old_bias = ctx.bias
            old_ob   = ctx.ob
            _reset_setup_progress(ctx)
            ctx.state = ATMState.ASIA_RANGE_LOCKED  # keep watching like soup does
            return build_ob_invalidated_msg(old_bias, old_ob, candle, "等待影線拒絕中")

        if detect_wick_rejection(candle, ctx.ob):
            ctx.checklist["wick_rejection"] = True
            ctx.state = ATMState.SIGNAL_FIRED
            _calculate_levels(ctx, candle)
            signal = _build_signal(ctx, candle)
            ctx.signal_sent = True
            log.info(f"[ATM] SIGNAL {ctx.bias.value}  entry={ctx.entry:.2f}  SL={ctx.sl:.2f}  TP1={ctx.tp1:.2f}  TP2={ctx.tp2:.2f}")
            return signal

    # ── Phase 5: signal fired — watch for re-breakout ────────────
    if ctx.state == ATMState.SIGNAL_FIRED:
        if is_inside_asia_range(candle, ref_high, ref_low):
            ctx.interaction_rearmed = True
            return None

        side = interaction_side(candle, ref_high, ref_low)
        if side not in {"above", "below"}:
            return None

        re_result = detect_interaction(candle, ref_high, ref_low)
        if re_result and ctx.interaction_rearmed:
            new_interaction, new_bias = re_result
            log.info(
                f"[ATM] New interaction after signal → {new_interaction.value} {new_bias.value}"
            )
            _start_interaction_setup(candle, ctx, history, new_interaction, new_bias, side)

    return None


def _calculate_levels(ctx: ATMContext, candle: Candle):
    """
    SL  = OB boundary (OB.low for LONG, OB.high for SHORT)
          ATM rule: OB drawn including wicks — SL sits just outside the OB box
    TP1 = Asia High (LONG) / Asia Low (SHORT)  — natural liquidity target
    TP2 = 1:2 R:R from entry
    """
    ob = ctx.ob
    tick = 0.25  # one NQ tick buffer beyond OB boundary

    if ctx.bias == Bias.LONG:
        ctx.entry = candle.close
        ctx.sl    = ob.low - tick
        ctx.tp1   = ctx.ref_high
        risk      = ctx.entry - ctx.sl
        tp2_rr    = ctx.entry + 2.0 * risk
        # TP2 must always be further than TP1; fall back to TP1 + 1R when 1:2 R:R falls short
        ctx.tp2   = tp2_rr if tp2_rr > ctx.tp1 else ctx.tp1 + risk
    else:
        ctx.entry = candle.close
        ctx.sl    = ob.high + tick
        ctx.tp1   = ctx.ref_low
        risk      = ctx.sl - ctx.entry
        tp2_rr    = ctx.entry - 2.0 * risk
        # TP2 must always be further than TP1; fall back to TP1 - 1R when 1:2 R:R falls short
        ctx.tp2   = tp2_rr if tp2_rr < ctx.tp1 else ctx.tp1 - risk


def _build_signal(ctx: ATMContext, candle: Candle) -> dict:
    cl         = ctx.checklist
    direction  = "📈 LONG" if ctx.bias == Bias.LONG else "📉 SHORT"
    risk       = abs(ctx.entry - ctx.sl)
    rr1        = abs(ctx.tp1 - ctx.entry) / risk if risk else 0
    rr2        = abs(ctx.tp2 - ctx.entry) / risk if risk else 0
    sl_label   = f"OB 低點 {ctx.ob.low:.2f}" if ctx.bias == Bias.LONG else f"OB 高點 {ctx.ob.high:.2f}"
    range_name = "Tokyo" if ctx.tokyo_range_locked else "Asia"
    tp1_label  = (
        f"{range_name} High {ctx.ref_high:.0f}" if ctx.bias == Bias.LONG
        else f"{range_name} Low {ctx.ref_low:.0f}"
    )

    checklist_lines = "\n".join([
        f"{'✅' if cl['time_filter']    else '⬜'} Kill Zone 時段確認",
        f"{'✅' if cl['asia_range']     else '⬜'} Asia Range 鎖定  H:{ctx.asia_high:.0f} / L:{ctx.asia_low:.0f}",
        f"{'✅' if cl['interaction']    else '⬜'} {ctx.interaction.value} 偵測 → {ctx.bias.value}",
        f"{'✅' if cl['ob_found']       else '⬜'} OB 確認  {ctx.ob.low:.0f} – {ctx.ob.high:.0f}",
        f"{'✅' if cl['retest']         else '⬜'} 回踩 OB",
        f"{'✅' if cl['wick_rejection'] else '⬜'} Wick Rejection 確認",
    ])

    session_name = "東京盤" if ctx.tokyo_range_locked else "亞洲盤"
    msg = (
        f"⚡ *ATM {session_name}訊號* — {direction}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{checklist_lines}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"*開單資訊*\n"
        f"進場:  `{ctx.entry:.2f}`\n"
        f"止損:  `{ctx.sl:.2f}`  _({sl_label})_\n"
        f"TP1:   `{ctx.tp1:.2f}`  _({tp1_label}  R:R 1:{rr1:.1f})_\n"
        f"TP2:   `{ctx.tp2:.2f}`  _(1:2 R:R)_\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"_訊號時間: {candle.ts.astimezone(TW_TZ).strftime('%H:%M')}_"
    )

    return {
        "strategy":    "ATM_ASIA",
        "direction":   ctx.bias.value,
        "interaction": ctx.interaction.value,
        "signal_key":  (
            f"{ctx.interaction.value}|{ctx.bias.value}|"
            f"{ctx.ob.source_time.isoformat()}|{candle.ts.isoformat()}"
        ),
        "entry":       ctx.entry,
        "sl":          ctx.sl,
        "tp1":         ctx.tp1,
        "tp2":         ctx.tp2,
        "rr1":         round(rr1, 2),
        "rr2":         round(rr2, 2),
        "asia_high":   ctx.asia_high,
        "asia_low":    ctx.asia_low,
        "ob_high":     ctx.ob.high,
        "ob_low":      ctx.ob.low,
        "signal_time": candle.ts.isoformat(),
        "telegram_message": msg,
    }


# ─────────────────────────────────────────────────────────────────
# Intermediate-stage Telegram messages (called by runner on state change)
# ─────────────────────────────────────────────────────────────────
def build_range_locked_msg(ctx: ATMContext) -> str:
    """Sent once when Asia Range is locked at 07:00."""
    cl = ctx.checklist
    lines = "\n".join([
        f"{'✅' if cl['time_filter'] else '⬜'} Kill Zone 時段確認",
        f"{'✅' if cl['asia_range']  else '⬜'} Asia Range 鎖定  H:{ctx.asia_high:.0f} / L:{ctx.asia_low:.0f}",
        f"⬜ 等待互動 (Sweep / Breakout)...",
        f"⬜ OB",
        f"⬜ 回踩",
        f"⬜ Wick Rejection",
    ])
    return (
        f"🌏 *ATM — Asia Range 鎖定*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{lines}"
    )


def build_ob_found_msg(ctx: ATMContext) -> str:
    """Sent once when interaction + OB are identified."""
    direction  = "📈 LONG" if ctx.bias == Bias.LONG else "📉 SHORT"
    range_name = "Tokyo" if ctx.tokyo_range_locked else "Asia"
    ref_h = ctx.ref_high if ctx.ref_high > 0 else ctx.asia_high
    ref_l = ctx.ref_low  if ctx.ref_low  > 0 else ctx.asia_low
    cl = ctx.checklist
    lines = "\n".join([
        f"{'✅' if cl['time_filter']  else '⬜'} Kill Zone 時段確認",
        f"{'✅' if cl['asia_range']   else '⬜'} {range_name} Range  H:{ref_h:.0f} / L:{ref_l:.0f}",
        f"{'✅' if cl['interaction']  else '⬜'} {ctx.interaction.value} → {ctx.bias.value}",
        f"{'✅' if cl['ob_found']     else '⬜'} OB 確認  {ctx.ob.low:.0f} – {ctx.ob.high:.0f}",
        f"⬜ 等待回踩 OB...",
        f"⬜ Wick Rejection",
    ])
    return (
        f"🔍 *ATM — OB 確認* {direction}\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{lines}"
    )


def build_tokyo_range_locked_msg(ctx: ATMContext) -> str:
    """Sent once when Tokyo Range is locked."""
    cl = ctx.checklist
    lines = "\n".join([
        f"{'✅' if cl['time_filter'] else '⬜'} Kill Zone 時段確認",
        f"✅ Asia Range  H:{ctx.asia_high:.0f} / L:{ctx.asia_low:.0f}",
        f"✅ Tokyo Range 鎖定  H:{ctx.tokyo_high:.0f} / L:{ctx.tokyo_low:.0f}",
        f"⬜ 等待互動 (Sweep / Breakout)...",
        f"⬜ OB",
        f"⬜ 回踩",
        f"⬜ Wick Rejection",
    ])
    return (
        f"🗼 *ATM — Tokyo Range 鎖定*\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"{lines}"
    )


def _build_us_expired_msg(ctx: "ATMContext", candle: "Candle") -> dict:
    time_str = candle.ts.astimezone(TW_TZ).strftime("%H:%M")
    phase = "等待回踩中" if ctx.state == ATMState.WAITING_RETEST else "等待影線拒絕中"
    ob = ctx.ob
    msg = (
        f"⏰ *ATM 監控結束 — 美盤開始*\n"
        f"方向: {ctx.bias.value}  OB: `{ob.low:.0f}–{ob.high:.0f}`\n"
        f"_(在 {phase} 時到期)  {time_str}_"
    )
    return {"notification_type": "OB_INVALIDATED", "telegram_message": msg}


def build_ob_invalidated_msg(
    old_bias: "Bias",
    old_ob:   "OrderBlock",
    candle:   "Candle",
    phase:    str,
    rev_ob:   "Optional[OrderBlock]" = None,
) -> dict:
    """Return a notification dict when an OB is invalidated."""
    bias_str = old_bias.value
    time_str = candle.ts.astimezone(TW_TZ).strftime("%H:%M")
    close    = candle.close

    if rev_ob:
        rev_bias_str = "SHORT" if old_bias == Bias.LONG else "LONG"
        msg = (
            f"↩️ *OB 失效 → 轉換方向*\n"
            f"原方向: {bias_str}  OB: `{old_ob.low:.0f}–{old_ob.high:.0f}`\n"
            f"突破收盤: `{close:.2f}`\n"
            f"新方向: {rev_bias_str}  新OB: `{rev_ob.low:.2f}–{rev_ob.high:.2f}`\n"
            f"_({phase})  {time_str}_"
        )
    else:
        msg = (
            f"❌ *OB 失效 → 重設*\n"
            f"原方向: {bias_str}  OB: `{old_ob.low:.0f}–{old_ob.high:.0f}`\n"
            f"突破收盤: `{close:.2f}`\n"
            f"_({phase})  {time_str}_"
        )
    return {"notification_type": "OB_INVALIDATED", "telegram_message": msg}


# ─────────────────────────────────────────────────────────────────
# Daily reset — call at 05:55 TW (summer) / 06:55 TW (winter)
# ─────────────────────────────────────────────────────────────────
def should_daily_reset(ctx: ATMContext, now_tw: datetime) -> bool:
    """Reset 5 minutes before Asia Kill Zone opens — unconditional, every day."""
    windows      = kill_zone_windows(now_tw)
    reset_hour   = windows["asia_start"].hour
    reset_minute = windows["asia_start"].minute - 5
    if reset_minute < 0:
        reset_hour  -= 1
        reset_minute += 60
    return now_tw.hour == reset_hour and now_tw.minute == reset_minute
