#!/usr/bin/env bash
# Patch live_s5_24h_bot.py to add Bitget exchange support.
# Usage: bash patch_add_bitget.sh
set -euo pipefail

FILE="$(dirname "$0")/live_s5_24h_bot.py"

if [ ! -f "$FILE" ]; then
  echo "ERROR: $FILE not found"
  exit 1
fi

python3 - "$FILE" <<'PYEOF'
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    src = f.read()

# ── 1. Add BITGET_CANDLES_URL constant ────────────────────────────────────────
OLD = 'KUCOIN_CANDLES_URL = "https://api.kucoin.com/api/v1/market/candles"\nLOGGER'
NEW = ('KUCOIN_CANDLES_URL = "https://api.kucoin.com/api/v1/market/candles"\n'
       'BITGET_CANDLES_URL = "https://api.bitget.com/api/v2/spot/market/candles"\n'
       'LOGGER')
if OLD in src:
    src = src.replace(OLD, NEW)
    print("[1/5] Added BITGET_CANDLES_URL")
else:
    print("[1/5] SKIP – BITGET_CANDLES_URL already present or pattern mismatch")

# ── 2. Expand SUPPORTED_EXCHANGES ────────────────────────────────────────────
OLD = 'SUPPORTED_EXCHANGES = {"binance", "kucoin"}'
NEW = 'SUPPORTED_EXCHANGES = {"binance", "kucoin", "bitget"}'
if OLD in src:
    src = src.replace(OLD, NEW)
    print("[2/5] Updated SUPPORTED_EXCHANGES")
else:
    print("[2/5] SKIP – already contains bitget or pattern mismatch")

# ── 3. Add BITGET_INTERVAL_MAP after KUCOIN_INTERVAL_MAP ─────────────────────
MARKER = '    "1w": "1week",\n}\n'
BITGET_MAP = '''\
BITGET_INTERVAL_MAP = {
    "1m": "1min",
    "3m": "3min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "12h": "12h",
    "1d": "1day",
    "1w": "1week",
}
'''
if MARKER in src and "BITGET_INTERVAL_MAP" not in src:
    src = src.replace(MARKER, MARKER + "\n" + BITGET_MAP, 1)
    print("[3/5] Added BITGET_INTERVAL_MAP")
else:
    print("[3/5] SKIP – already present or marker mismatch")

# ── 4. Add bitget helper functions + fetch_klines_bitget ─────────────────────
BITGET_FUNCS = '''

def to_bitget_symbol(symbol: str) -> str:
    return symbol.replace("-", "").upper()


def to_bitget_interval(interval: str) -> str:
    key = interval.strip().lower()
    if key not in BITGET_INTERVAL_MAP:
        supported = ", ".join(sorted(BITGET_INTERVAL_MAP.keys()))
        raise ValueError(f"Unsupported interval for Bitget: {interval}. supported={supported}")
    return BITGET_INTERVAL_MAP[key]


def fetch_klines_bitget(symbol: str, interval: str, limit: int, verify: bool | str = True) -> pd.DataFrame:
    bitget_symbol = to_bitget_symbol(symbol)
    bitget_interval = to_bitget_interval(interval)
    r = requests.get(
        BITGET_CANDLES_URL,
        params={"symbol": bitget_symbol, "granularity": bitget_interval, "limit": max(1, min(limit, 1000))},
        verify=verify,
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") != "00000":
        raise ValueError(f"Bitget API error symbol={bitget_symbol} code={payload.get('code')} msg={payload.get('msg')}")

    raw = payload.get("data", [])
    df = pd.DataFrame(raw, columns=["open_time", "open", "high", "low", "close", "volume", "quote_volume"])
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df[["timestamp", "open", "high", "low", "close", "volume"]].dropna()
    return df.set_index("timestamp").sort_index()

'''
ANCHOR = "def fetch_klines_binance("
if "def fetch_klines_bitget(" not in src and ANCHOR in src:
    src = src.replace(ANCHOR, BITGET_FUNCS + ANCHOR, 1)
    print("[4/5] Added fetch_klines_bitget and helpers")
else:
    print("[4/5] SKIP – already present or anchor mismatch")

# ── 5. Wire bitget into fetch_klines dispatcher ───────────────────────────────
OLD_DISPATCH = ('def fetch_klines(exchange: str, symbol: str, interval: str, limit: int, verify: bool | str = True) -> pd.DataFrame:\n'
                '    if exchange == "kucoin":\n'
                '        return fetch_klines_kucoin(symbol, interval, limit, verify=verify)\n'
                '    return fetch_klines_binance(symbol, interval, limit, verify=verify)')
NEW_DISPATCH = ('def fetch_klines(exchange: str, symbol: str, interval: str, limit: int, verify: bool | str = True) -> pd.DataFrame:\n'
                '    if exchange == "kucoin":\n'
                '        return fetch_klines_kucoin(symbol, interval, limit, verify=verify)\n'
                '    if exchange == "bitget":\n'
                '        return fetch_klines_bitget(symbol, interval, limit, verify=verify)\n'
                '    return fetch_klines_binance(symbol, interval, limit, verify=verify)')
if OLD_DISPATCH in src:
    src = src.replace(OLD_DISPATCH, NEW_DISPATCH)
    print("[5/5] Updated fetch_klines dispatcher")
elif 'exchange == "bitget"' in src:
    print("[5/5] SKIP – dispatcher already has bitget")
else:
    print("[5/5] SKIP – dispatcher pattern mismatch (check manually)")

with open(path, "w", encoding="utf-8") as f:
    f.write(src)

print("\nDone. Run: python3 -u live_s5_24h_bot.py")
PYEOF
