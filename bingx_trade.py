"""
BingX perpetual futures auto-trader for ATM Asia signals.

Specs:
  - Leverage  : 100x (set once on startup)
  - Margin    : 20 USDT  →  notional = 2000 USDT
  - Entry     : LIMIT GTC
  - SL        : STOP_MARKET + closePosition=true  (survives bot crash)
  - TP1 (75%) : TAKE_PROFIT_MARKET
  - TP2 (25%) : TAKE_PROFIT_MARKET
  - Max open  : 2 positions (guard against stacking)

BingX requires HEDGE MODE on the account for positionSide to work.
Enable it in BingX → Trade Settings → Position Mode → Hedge Mode.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import math
import time
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ── constants ──────────────────────────────────────────────────────────────
BINGX_BASE      = "https://open-api.bingx.com"
ATM_SYMBOL      = "NCSINASDAQ1002USD-USDT"
LEVERAGE        = 100
MARGIN_USDT     = 20.0          # USDT margin per trade
MAX_POSITIONS   = 2
TP1_RATIO       = 0.75
TP2_RATIO       = 0.25
QTY_STEP        = 0.001         # NQ minimum qty step on BingX; verify via /quote/contracts
QTY_MIN         = 0.001         # minimum order quantity


# ── internal helpers ───────────────────────────────────────────────────────

def _sign(params: dict, secret: str) -> str:
    qs = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def _req(method: str, path: str, params: dict, api_key: str, secret: str) -> dict:
    p = dict(params)
    p["timestamp"] = int(time.time() * 1000)
    p["signature"] = _sign(p, secret)
    headers = {"X-BX-APIKEY": api_key}
    url = BINGX_BASE + path
    r = getattr(requests, method.lower())(url, params=p, headers=headers, timeout=10)
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"BingX [{data.get('code')}] {data.get('msg')}")
    return data


def _floor_step(value: float, step: float) -> float:
    """Round down to nearest step (avoids floating-point drift)."""
    return math.floor(value / step + 1e-9) * step


def _order(side: str, pos_side: str, order_type: str,
           quantity: Optional[float], price: Optional[float],
           stop_price: Optional[float], close_position: bool,
           api_key: str, secret: str) -> str:
    """Place one order, return orderId string."""
    p: dict = {
        "symbol":       ATM_SYMBOL,
        "side":         side,
        "positionSide": pos_side,
        "type":         order_type,
        "timeInForce":  "GTC",
    }
    if quantity is not None:
        p["quantity"] = quantity
    if price is not None:
        p["price"] = price
    if stop_price is not None:
        p["stopPrice"] = stop_price
    if close_position:
        p["closePosition"] = "true"
    data = _req("POST", "/openApi/swap/v2/trade/order", p, api_key, secret)
    order_id = str(data["data"]["order"]["orderId"])
    log.info("[TRADE] %s %s %s qty=%s price=%s stop=%s → id=%s",
             order_type, side, pos_side, quantity, price, stop_price, order_id)
    return order_id


# ── public API ─────────────────────────────────────────────────────────────

def set_leverage(api_key: str, secret: str, leverage: int = LEVERAGE) -> None:
    """Set leverage for both LONG and SHORT sides. Call once on startup."""
    for side in ("LONG", "SHORT"):
        _req("POST", "/openApi/swap/v2/trade/leverage", {
            "symbol": ATM_SYMBOL, "side": side, "leverage": leverage,
        }, api_key, secret)
    log.info("[TRADE] Leverage set to %dx on %s", leverage, ATM_SYMBOL)


def count_open_positions(api_key: str, secret: str) -> int:
    """Return number of open positions for ATM_SYMBOL."""
    data = _req("GET", "/openApi/swap/v2/user/positions",
                {"symbol": ATM_SYMBOL}, api_key, secret)
    positions = data.get("data", {}).get("positions", [])
    return sum(1 for p in positions if abs(float(p.get("positionAmt", 0))) > 0)


def place_atm_trade(
    bias: str,       # "LONG" | "SHORT"
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    api_key: str,
    secret: str,
) -> tuple[Optional[dict], Optional[str]]:
    """
    Place a full ATM trade set:
      1. Limit entry order
      2. STOP_MARKET SL with closePosition=true  (persists after bot crash)
      3. TAKE_PROFIT_MARKET TP1 for 75% of position
      4. TAKE_PROFIT_MARKET TP2 for remaining 25%

    Returns (order_ids_dict, error_message).
    error_message is None on full success, a string describing partial or full failure.
    """
    # ── guard: max concurrent positions ────────────────────────────────────
    try:
        n_open = count_open_positions(api_key, secret)
    except Exception as e:
        return None, f"Cannot fetch positions: {e}"

    if n_open >= MAX_POSITIONS:
        msg = f"Max {MAX_POSITIONS} positions already open ({n_open}) — skipping"
        log.warning("[TRADE] %s", msg)
        return None, msg

    # ── quantity calculation ────────────────────────────────────────────────
    notional  = MARGIN_USDT * LEVERAGE          # 2000 USDT
    qty_total = _floor_step(notional / entry, QTY_STEP)
    if qty_total < QTY_MIN:
        return None, f"Qty too small: {qty_total} (min {QTY_MIN})"

    qty_tp1 = _floor_step(qty_total * TP1_RATIO, QTY_STEP)
    qty_tp2 = _floor_step(qty_total - qty_tp1, QTY_STEP)   # remainder

    if qty_tp2 < QTY_MIN:
        # edge case: if TP2 rounds to 0, put everything on TP1
        qty_tp1 = qty_total
        qty_tp2 = None

    side       = "BUY"  if bias == "LONG" else "SELL"
    close_side = "SELL" if bias == "LONG" else "BUY"
    pos_side   = bias   # "LONG" | "SHORT"

    results: dict = {
        "bias": bias, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
        "qty_total": qty_total, "qty_tp1": qty_tp1, "qty_tp2": qty_tp2,
    }
    errors: list[str] = []

    # 1. Entry limit order
    try:
        results["entry_order_id"] = _order(
            side, pos_side, "LIMIT",
            quantity=qty_total, price=entry,
            stop_price=None, close_position=False,
            api_key=api_key, secret=secret,
        )
    except Exception as e:
        # Entry failed → abort everything
        return None, f"Entry order failed: {e}"

    # 2. Stop Loss — closePosition=true closes whatever remains after TP1 hit
    try:
        results["sl_order_id"] = _order(
            close_side, pos_side, "STOP_MARKET",
            quantity=None, price=None,
            stop_price=sl, close_position=True,
            api_key=api_key, secret=secret,
        )
    except Exception as e:
        errors.append(f"SL failed: {e}")

    # 3. TP1 (75%)
    try:
        results["tp1_order_id"] = _order(
            close_side, pos_side, "TAKE_PROFIT_MARKET",
            quantity=qty_tp1, price=None,
            stop_price=tp1, close_position=False,
            api_key=api_key, secret=secret,
        )
    except Exception as e:
        errors.append(f"TP1 failed: {e}")

    # 4. TP2 (25%)
    if qty_tp2 is not None:
        try:
            results["tp2_order_id"] = _order(
                close_side, pos_side, "TAKE_PROFIT_MARKET",
                quantity=qty_tp2, price=None,
                stop_price=tp2, close_position=False,
                api_key=api_key, secret=secret,
            )
        except Exception as e:
            errors.append(f"TP2 failed: {e}")

    err_str = "; ".join(errors) if errors else None
    if err_str:
        log.warning("[TRADE] Partial order failure: %s", err_str)
    return results, err_str


def format_trade_notification(results: dict, err: Optional[str]) -> str:
    """Build a Telegram message summarising what was placed."""
    bias  = results.get("bias", "?")
    emoji = "📈" if bias == "LONG" else "📉"
    lines = [
        f"{emoji} <b>Auto Trade Placed — {bias}</b>",
        "",
        f"📍 Entry  : <code>{results['entry']:.2f}</code>  (qty {results['qty_total']})",
        f"🛡️ SL     : <code>{results['sl']:.2f}</code>  (closePosition)",
        f"🎯 TP1 75%: <code>{results['tp1']:.2f}</code>  (qty {results['qty_tp1']})",
    ]
    if results.get("qty_tp2"):
        lines.append(f"🎯 TP2 25%: <code>{results['tp2']:.2f}</code>  (qty {results['qty_tp2']})")
    if err:
        lines += ["", f"⚠️ <i>{err}</i>"]
    return "\n".join(lines)
