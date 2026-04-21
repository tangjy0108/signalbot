from __future__ import annotations

import argparse
import json
import math
import time
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests


SPOT_API = "https://api.binance.com/api/v3"
FUTURES_API = "https://fapi.binance.com/fapi/v1"
FUTURES_DATA_API = "https://fapi.binance.com/futures/data"
DEFAULT_BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CACHE_DIR = DEFAULT_BASE_DIR / "cache"
DEFAULT_OUTPUT_DIR = DEFAULT_BASE_DIR / "output"
REQUEST_TIMEOUT = 30
FEE_PER_SIDE = 0.0004
MS_PER_HOUR = 60 * 60 * 1000

KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "trade_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


@dataclass
class StudyConfig:
    days: int
    symbol_limit: int
    top_n: int
    breakout_hours: int
    breakout_threshold: float
    min_quote_volume: float
    refresh: bool
    cache_dir: Path
    output_dir: Path
    ssl_verify: bool | str
    spot_api: str
    futures_api: str
    futures_data_api: str


def parse_args() -> StudyConfig:
    parser = argparse.ArgumentParser(
        description="Study Binance top gainers and pre-breakout factor behavior.",
    )
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days. OI history is limited, so 30 is a safe default.")
    parser.add_argument("--symbol-limit", type=int, default=80, help="Number of liquid USDT perpetual symbols to analyze.")
    parser.add_argument("--top-n", type=int, default=25, help="Top N gainers per hour to keep as candidates.")
    parser.add_argument("--breakout-hours", type=int, default=8, help="Forward window used to define the breakout label.")
    parser.add_argument("--breakout-threshold", type=float, default=0.08, help="Forward return threshold required to count as a breakout.")
    parser.add_argument("--min-quote-volume", type=float, default=5_000_000, help="Minimum futures quote volume per hour.")
    parser.add_argument("--refresh", action="store_true", help="Ignore cached CSV files and fetch data again.")
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--spot-api", default=SPOT_API, help="Base URL for Binance spot REST API.")
    parser.add_argument("--futures-api", default=FUTURES_API, help="Base URL for Binance futures REST API.")
    parser.add_argument("--futures-data-api", default=FUTURES_DATA_API, help="Base URL for Binance futures data REST API.")
    parser.add_argument(
        "--ssl-verify",
        default="true",
        help="true, false, or a path to a CA bundle file for HTTPS verification.",
    )
    args = parser.parse_args()
    ssl_verify: bool | str
    ssl_text = str(args.ssl_verify).strip()
    if ssl_text.lower() in {"0", "false", "no", "off"}:
        ssl_verify = False
    elif ssl_text.lower() in {"1", "true", "yes", "on"}:
        ssl_verify = True
    else:
        ssl_verify = ssl_text
    return StudyConfig(
        days=args.days,
        symbol_limit=args.symbol_limit,
        top_n=args.top_n,
        breakout_hours=args.breakout_hours,
        breakout_threshold=args.breakout_threshold,
        min_quote_volume=float(args.min_quote_volume),
        refresh=bool(args.refresh),
        cache_dir=args.cache_dir,
        output_dir=args.output_dir,
        ssl_verify=ssl_verify,
        spot_api=str(args.spot_api).rstrip("/"),
        futures_api=str(args.futures_api).rstrip("/"),
        futures_data_api=str(args.futures_data_api).rstrip("/"),
    )


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def request_json(url: str, params: dict[str, Any] | None = None, verify: bool | str = True) -> Any:
    if verify is False:
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
    response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT, verify=verify)
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        if exc.response is not None and exc.response.status_code == 403 and "binance" in url:
            raise RuntimeError(
                "Binance API returned 403 Forbidden. This usually means the endpoint is blocked from the current network or region. "
                "Try a reachable base URL via --futures-api / --futures-data-api, use a proxy/VPN, or run the script from a network where Binance futures endpoints are accessible."
            ) from exc
        raise RuntimeError(f"HTTP request failed for {url} with status {status}.") from exc
    return response.json()


def get_symbol_universe(cfg: StudyConfig) -> list[str]:
    futures_info = request_json(f"{cfg.futures_api}/exchangeInfo", verify=cfg.ssl_verify)
    spot_info = request_json(f"{cfg.spot_api}/exchangeInfo", verify=cfg.ssl_verify)
    futures_24h = request_json(f"{cfg.futures_api}/ticker/24hr", verify=cfg.ssl_verify)

    spot_symbols = {
        item["symbol"]
        for item in spot_info["symbols"]
        if item.get("status") == "TRADING" and item.get("quoteAsset") == "USDT"
    }

    allowed = []
    for item in futures_info["symbols"]:
        if item.get("contractType") != "PERPETUAL":
            continue
        if item.get("status") != "TRADING":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        symbol = item["symbol"]
        if symbol not in spot_symbols:
            continue
        allowed.append(symbol)

    quote_by_symbol = {}
    for item in futures_24h:
        symbol = item.get("symbol")
        if symbol in allowed:
            quote_by_symbol[symbol] = float(item.get("quoteVolume") or 0.0)

    ranked = sorted(allowed, key=lambda symbol: quote_by_symbol.get(symbol, 0.0), reverse=True)
    return ranked[:symbol_limit]


def fetch_klines(
    base_url: str,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    verify: bool | str,
) -> pd.DataFrame:
    all_rows: list[list[Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        rows = request_json(
            f"{base_url}/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            },
            verify=verify,
        )
        if not rows:
            break
        all_rows.extend(rows)
        last_open_time = int(rows[-1][0])
        cursor = last_open_time + MS_PER_HOUR
        if len(rows) < 1000:
            break
        time.sleep(0.05)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=KLINE_COLUMNS)
    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume",
        "trade_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]
    for column in numeric_columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("open_time").sort_index()
    return df


def fetch_open_interest_hist(base_url: str, symbol: str, start_ms: int, end_ms: int, verify: bool | str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        batch = request_json(
            f"{base_url}/openInterestHist",
            params={
                "symbol": symbol,
                "period": "1h",
                "limit": 500,
                "startTime": cursor,
                "endTime": end_ms,
            },
            verify=verify,
        )
        if not batch:
            break
        rows.extend(batch)
        cursor = int(batch[-1]["timestamp"]) + MS_PER_HOUR
        if len(batch) < 500:
            break
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame(columns=["open_interest", "open_interest_value"])

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["open_interest"] = pd.to_numeric(df["sumOpenInterest"], errors="coerce")
    df["open_interest_value"] = pd.to_numeric(df["sumOpenInterestValue"], errors="coerce")
    df = df[["timestamp", "open_interest", "open_interest_value"]]
    return df.set_index("timestamp").sort_index()


def fetch_funding_rates(base_url: str, symbol: str, start_ms: int, end_ms: int, verify: bool | str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    cursor = start_ms
    while cursor < end_ms:
        batch = request_json(
            f"{base_url}/fundingRate",
            params={
                "symbol": symbol,
                "startTime": cursor,
                "endTime": end_ms,
                "limit": 1000,
            },
            verify=verify,
        )
        if not batch:
            break
        rows.extend(batch)
        cursor = int(batch[-1]["fundingTime"]) + 1
        if len(batch) < 1000:
            break
        time.sleep(0.05)

    if not rows:
        return pd.DataFrame(columns=["funding_rate"])

    df = pd.DataFrame(rows)
    df["fundingTime"] = pd.to_datetime(df["fundingTime"], unit="ms", utc=True)
    df["funding_rate"] = pd.to_numeric(df["fundingRate"], errors="coerce")
    df = df[["fundingTime", "funding_rate"]]
    return df.set_index("fundingTime").sort_index()


def build_symbol_panel(symbol: str, cfg: StudyConfig, start_ms: int, end_ms: int) -> pd.DataFrame:
    ensure_dir(cfg.cache_dir)
    cache_path = cfg.cache_dir / f"{symbol}_{cfg.days}d.csv"
    if cache_path.exists() and not cfg.refresh:
        cached = pd.read_csv(cache_path)
        cached["timestamp"] = pd.to_datetime(cached["timestamp"], utc=True)
        return cached.set_index("timestamp").sort_index()

    futures_klines = fetch_klines(cfg.futures_api, symbol, "1h", start_ms, end_ms, cfg.ssl_verify)
    spot_klines = fetch_klines(cfg.spot_api, symbol, "1h", start_ms, end_ms, cfg.ssl_verify)
    oi = fetch_open_interest_hist(cfg.futures_data_api, symbol, start_ms, end_ms, cfg.ssl_verify)
    funding = fetch_funding_rates(cfg.futures_api, symbol, start_ms, end_ms, cfg.ssl_verify)

    if futures_klines.empty or spot_klines.empty:
        return pd.DataFrame()

    panel = futures_klines[
        [
            "open",
            "high",
            "low",
            "close",
            "quote_volume",
            "taker_buy_quote_volume",
        ]
    ].copy()
    panel = panel.rename(
        columns={
            "open": "futures_open",
            "high": "futures_high",
            "low": "futures_low",
            "close": "futures_close",
            "quote_volume": "futures_quote_volume",
            "taker_buy_quote_volume": "futures_taker_buy_quote_volume",
        }
    )
    panel["spot_close"] = spot_klines["close"].reindex(panel.index)

    if not oi.empty:
        panel = panel.join(oi.reindex(panel.index).ffill())
    else:
        panel["open_interest"] = np.nan
        panel["open_interest_value"] = np.nan

    if not funding.empty:
        panel = panel.join(funding.reindex(panel.index).ffill())
    else:
        panel["funding_rate"] = np.nan

    panel["symbol"] = symbol
    panel["ret_4h"] = panel["futures_close"].pct_change(4)
    panel["oi_change_8h"] = panel["open_interest"].pct_change(8)
    panel["funding_rate"] = panel["funding_rate"].ffill().fillna(0.0)

    taker_sell_quote = (panel["futures_quote_volume"] - panel["futures_taker_buy_quote_volume"]).clip(lower=0.0)
    panel["taker_buy_sell_ratio_8h"] = (
        panel["futures_taker_buy_quote_volume"].rolling(8).sum() / (taker_sell_quote.rolling(8).sum() + 1e-9)
    )
    panel["basis"] = (panel["futures_close"] - panel["spot_close"]) / panel["spot_close"].replace(0.0, np.nan)
    panel["basis_change_8h"] = panel["basis"] - panel["basis"].shift(8)
    panel["volume_surge_24h"] = panel["futures_quote_volume"] / panel["futures_quote_volume"].rolling(24).mean()

    prev_24h_high = panel["futures_close"].shift(1).rolling(24).max()
    future_max_close = panel["futures_close"].shift(-1).iloc[::-1].rolling(cfg.breakout_hours).max().iloc[::-1]
    future_close_horizon = panel["futures_close"].shift(-cfg.breakout_hours)
    panel["forward_max_return"] = future_max_close / panel["futures_close"] - 1.0
    panel["forward_horizon_return"] = future_close_horizon / panel["futures_close"] - 1.0
    panel["label_breakout"] = (
        (panel["forward_max_return"] >= cfg.breakout_threshold)
        & (future_max_close > prev_24h_high)
    )

    panel = panel.dropna(subset=["spot_close"]).copy()
    panel.index.name = "timestamp"
    panel.reset_index().to_csv(cache_path, index=False)
    return panel


def build_candidate_frame(frames: list[pd.DataFrame], cfg: StudyConfig) -> pd.DataFrame:
    panel = pd.concat(frames, ignore_index=False)
    panel = panel.reset_index().rename(columns={"index": "timestamp"})
    panel["rank_4h"] = panel.groupby("timestamp")["ret_4h"].rank(method="first", ascending=False)
    candidate = panel[
        (panel["rank_4h"] <= cfg.top_n)
        & (panel["futures_quote_volume"] >= cfg.min_quote_volume)
    ].copy()

    feature_columns = [
        "oi_change_8h",
        "funding_rate",
        "taker_buy_sell_ratio_8h",
        "basis_change_8h",
        "volume_surge_24h",
        "forward_horizon_return",
    ]
    candidate = candidate.dropna(subset=feature_columns)
    return candidate.sort_values(["timestamp", "rank_4h", "symbol"]).reset_index(drop=True)


def effect_size(series_yes: pd.Series, series_no: pd.Series) -> float:
    diff = float(series_yes.mean() - series_no.mean())
    var_yes = float(series_yes.var(ddof=1)) if len(series_yes) > 1 else 0.0
    var_no = float(series_no.var(ddof=1)) if len(series_no) > 1 else 0.0
    denom = math.sqrt((var_yes + var_no) / 2.0) if (var_yes + var_no) > 0 else 0.0
    return diff / denom if denom > 0 else 0.0


def permutation_p_value(series_yes: pd.Series, series_no: pd.Series, seed: int = 42, rounds: int = 2000) -> float:
    if len(series_yes) < 2 or len(series_no) < 2:
        return 1.0
    observed = abs(float(series_yes.mean() - series_no.mean()))
    combined = np.concatenate([series_yes.to_numpy(), series_no.to_numpy()])
    rng = np.random.default_rng(seed)
    count = 0
    split = len(series_yes)
    for _ in range(rounds):
        rng.shuffle(combined)
        yes_sample = combined[:split]
        no_sample = combined[split:]
        if abs(float(yes_sample.mean() - no_sample.mean())) >= observed:
            count += 1
    return (count + 1) / (rounds + 1)


def build_factor_summary(candidate: pd.DataFrame) -> pd.DataFrame:
    positive = candidate[candidate["label_breakout"]]
    negative = candidate[~candidate["label_breakout"]]
    rows = []
    factors = [
        "oi_change_8h",
        "funding_rate",
        "taker_buy_sell_ratio_8h",
        "basis_change_8h",
        "volume_surge_24h",
    ]
    for factor in factors:
        yes_series = positive[factor].dropna()
        no_series = negative[factor].dropna()
        rows.append(
            {
                "factor": factor,
                "breakout_mean": float(yes_series.mean()) if not yes_series.empty else np.nan,
                "control_mean": float(no_series.mean()) if not no_series.empty else np.nan,
                "breakout_median": float(yes_series.median()) if not yes_series.empty else np.nan,
                "control_median": float(no_series.median()) if not no_series.empty else np.nan,
                "effect_size": effect_size(yes_series, no_series),
                "p_value": permutation_p_value(yes_series, no_series),
            }
        )
    summary = pd.DataFrame(rows)
    return summary.sort_values("effect_size", key=lambda s: s.abs(), ascending=False).reset_index(drop=True)


def select_rule(summary: pd.DataFrame, train_df: pd.DataFrame) -> tuple[list[dict[str, Any]], int]:
    positive = train_df[train_df["label_breakout"]]
    negative = train_df[~train_df["label_breakout"]]
    rule_items: list[dict[str, Any]] = []
    for factor in summary.head(4)["factor"]:
        yes_median = float(positive[factor].median())
        no_median = float(negative[factor].median())
        direction = "gte" if yes_median >= no_median else "lte"
        threshold = (yes_median + no_median) / 2.0
        rule_items.append({"factor": factor, "direction": direction, "threshold": threshold})
    min_signals = max(2, math.ceil(len(rule_items) * 0.75))
    return rule_items, min_signals


def apply_rule(df: pd.DataFrame, rule_items: list[dict[str, Any]], min_signals: int) -> pd.Series:
    passed = []
    for item in rule_items:
        if item["direction"] == "gte":
            passed.append(df[item["factor"]] >= item["threshold"])
        else:
            passed.append(df[item["factor"]] <= item["threshold"])
    if not passed:
        return pd.Series(False, index=df.index)
    score = pd.concat(passed, axis=1).sum(axis=1)
    return score >= min_signals


def classification_metrics(actual: pd.Series, predicted: pd.Series) -> dict[str, float]:
    tp = int(((actual == 1) & (predicted == 1)).sum())
    fp = int(((actual == 0) & (predicted == 1)).sum())
    fn = int(((actual == 1) & (predicted == 0)).sum())
    tn = int(((actual == 0) & (predicted == 0)).sum())
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    accuracy = (tp + tn) / max(tp + fp + fn + tn, 1)
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": precision,
        "recall": recall,
        "accuracy": accuracy,
    }


def backtest_signals(test_df: pd.DataFrame, signal_mask: pd.Series) -> dict[str, float]:
    trades = test_df[signal_mask].copy()
    if trades.empty:
        return {
            "trades": 0,
            "win_rate": 0.0,
            "avg_return": 0.0,
            "median_return": 0.0,
            "profit_factor": 0.0,
            "compound_return": 0.0,
        }

    trades["net_return"] = trades["forward_horizon_return"] - 2 * FEE_PER_SIDE
    wins = trades[trades["net_return"] > 0]["net_return"]
    losses = trades[trades["net_return"] <= 0]["net_return"]
    profit_factor = wins.sum() / abs(losses.sum()) if not losses.empty and abs(losses.sum()) > 0 else float("inf")
    compound = float(np.prod(1.0 + trades["net_return"].to_numpy()) - 1.0)
    return {
        "trades": int(len(trades)),
        "win_rate": float((trades["net_return"] > 0).mean()),
        "avg_return": float(trades["net_return"].mean()),
        "median_return": float(trades["net_return"].median()),
        "profit_factor": float(profit_factor),
        "compound_return": compound,
    }


def format_rule(rule_items: list[dict[str, Any]], min_signals: int) -> str:
    parts = []
    for item in rule_items:
        operator = ">=" if item["direction"] == "gte" else "<="
        parts.append(f"{item['factor']} {operator} {item['threshold']:.6f}")
    return f"Require at least {min_signals} of {len(rule_items)}: " + "; ".join(parts)


def markdown_table(df: pd.DataFrame, float_columns: set[str] | None = None) -> str:
    float_columns = float_columns or set()
    headers = list(df.columns)
    rows = []
    for _, row in df.iterrows():
        formatted = []
        for column in headers:
            value = row[column]
            if pd.isna(value):
                formatted.append("")
            elif column in float_columns:
                formatted.append(f"{float(value):.6f}")
            else:
                formatted.append(str(value))
        rows.append(formatted)

    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def write_report(
    cfg: StudyConfig,
    universe: list[str],
    candidate: pd.DataFrame,
    factor_summary: pd.DataFrame,
    rule_items: list[dict[str, Any]],
    min_signals: int,
    metrics: dict[str, float],
    backtest: dict[str, float],
) -> Path:
    ensure_dir(cfg.output_dir)
    report_path = cfg.output_dir / "binance_breakout_report.md"
    breakout_rate = float(candidate["label_breakout"].mean()) if not candidate.empty else 0.0
    factor_lines = markdown_table(
        factor_summary,
        float_columns={
            "breakout_mean",
            "control_mean",
            "breakout_median",
            "control_median",
            "effect_size",
            "p_value",
        },
    )
    payload = {
        "config": {
            "days": cfg.days,
            "symbol_limit": cfg.symbol_limit,
            "top_n": cfg.top_n,
            "breakout_hours": cfg.breakout_hours,
            "breakout_threshold": cfg.breakout_threshold,
            "min_quote_volume": cfg.min_quote_volume,
        },
        "classification": metrics,
        "backtest": backtest,
        "rule": rule_items,
        "universe": universe,
    }
    (cfg.output_dir / "binance_breakout_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    report = f"""# Binance Top Gainers Breakout Study

## Scope

- Universe size: {len(universe)} USDT perpetual symbols with matching spot pairs
- Lookback: {cfg.days} days
- Candidate set: hourly trailing 4h gainers top {cfg.top_n}
- Breakout label: future {cfg.breakout_hours}h max return >= {cfg.breakout_threshold:.2%} and breaks prior 24h close high
- Net outflow proxy: taker buy/sell quote volume ratio over the prior 8h

## Data Constraints

- Binance public open-interest history is limited, so this study is designed around recent history.
- Funding rate is perpetual-only and sampled at the latest known funding print for each hour.
- This report uses public REST data only; no private APIs or paid datasets.

## Sample Overview

- Candidate rows: {len(candidate)}
- Breakout hit rate: {breakout_rate:.2%}
- Mean hourly futures quote volume floor: {cfg.min_quote_volume:,.0f}

## Factor Summary

{factor_lines}

## Rule Selected

{format_rule(rule_items, min_signals)}

## Classification Metrics

- Precision: {metrics['precision']:.2%}
- Recall: {metrics['recall']:.2%}
- Accuracy: {metrics['accuracy']:.2%}
- TP / FP / FN / TN: {metrics['tp']} / {metrics['fp']} / {metrics['fn']} / {metrics['tn']}

## Horizon Backtest

- Trades: {backtest['trades']}
- Win rate: {backtest['win_rate']:.2%}
- Average net return: {backtest['avg_return']:.2%}
- Median net return: {backtest['median_return']:.2%}
- Profit factor: {backtest['profit_factor']:.3f}
- Compound return: {backtest['compound_return']:.2%}

## Interpretation Notes

- Positive effect size means the factor is higher in breakout candidates than in controls.
- Funding rate can flip sign quickly; treat it as a crowding filter, not a standalone trigger.
- Basis and volume surge are added because exchange net outflow is not directly published by Binance.
"""
    report_path.write_text(report, encoding="utf-8")
    candidate.to_csv(cfg.output_dir / "binance_breakout_candidates.csv", index=False)
    factor_summary.to_csv(cfg.output_dir / "binance_breakout_factor_summary.csv", index=False)
    return report_path


def main() -> None:
    cfg = parse_args()
    ensure_dir(cfg.cache_dir)
    ensure_dir(cfg.output_dir)

    end_ts = pd.Timestamp.now("UTC").floor("1h")
    start_ts = end_ts - pd.Timedelta(days=cfg.days)
    start_ms = int(start_ts.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000)

    if cfg.ssl_verify is False:
        print("WARNING: SSL verification is disabled for HTTPS requests.")
    elif isinstance(cfg.ssl_verify, str):
        print(f"Using custom CA bundle: {cfg.ssl_verify}")

    universe = get_symbol_universe(cfg)
    frames = []
    print(f"Selected {len(universe)} symbols. Fetching panels from {start_ts} to {end_ts}...")
    for idx, symbol in enumerate(universe, start=1):
        print(f"[{idx:03d}/{len(universe):03d}] {symbol}")
        panel = build_symbol_panel(symbol, cfg, start_ms, end_ms)
        if len(panel) >= 72:
            frames.append(panel)

    if not frames:
        raise RuntimeError("No valid symbol panels were built. Check network access or reduce symbol count.")

    candidate = build_candidate_frame(frames, cfg)
    if candidate.empty:
        raise RuntimeError("No candidates matched the rank and liquidity filters. Try lowering the volume floor.")

    split_time = candidate["timestamp"].quantile(0.70)
    train_df = candidate[candidate["timestamp"] <= split_time].copy()
    test_df = candidate[candidate["timestamp"] > split_time].copy()
    if train_df.empty or test_df.empty:
        raise RuntimeError("Not enough samples to split train/test. Increase --days or lower filters.")

    factor_summary = build_factor_summary(train_df)
    rule_items, min_signals = select_rule(factor_summary, train_df)
    predicted = apply_rule(test_df, rule_items, min_signals)
    metrics = classification_metrics(test_df["label_breakout"].astype(int), predicted.astype(int))
    backtest = backtest_signals(test_df, predicted)
    report_path = write_report(cfg, universe, candidate, factor_summary, rule_items, min_signals, metrics, backtest)

    print("\nStudy complete.")
    print(f"Candidates: {len(candidate)} | Train: {len(train_df)} | Test: {len(test_df)}")
    print(f"Report: {report_path}")
    print(f"Precision: {metrics['precision']:.2%} | Recall: {metrics['recall']:.2%} | PF: {backtest['profit_factor']:.3f}")


if __name__ == "__main__":
    main()