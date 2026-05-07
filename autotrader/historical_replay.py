"""Historical replay trainer for AutoBott.

This is the first offline training engine: it replays historical stock bars
through the existing scanner as if each timestamp were live, simulates simple
directional trades, and writes every opportunity/outcome to CSV.

It intentionally does not place orders and does not mutate live runtime state.
Historical options quote replay can be layered in later when a local options
history dataset is available.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytz
import yfinance as yf

import config
from scanner import IntradayScanner

EASTERN = pytz.timezone(config.EASTERN_TZ)


@dataclass(frozen=True)
class ReplayConfig:
    symbols: list[str]
    start: str
    end: str
    interval: str
    scan_every_minutes: int
    horizon_minutes: int
    take_profit_pct: float
    stop_loss_pct: float
    max_signals_per_scan: int
    output: Path
    cache_dir: Path
    daily_lookback_days: int
    min_daily_bars: int
    scan_bars: int = int(getattr(config, "SCAN_INTRADAY_BARS", 60))
    offline: bool = False


REPLAY_RESULT_COLUMNS = [
    "timestamp",
    "symbol",
    "direction",
    "strategy_profile",
    "signal_score",
    "direction_score",
    "rvol",
    "roc",
    "rsi",
    "volatility_score",
    "reason",
    "evaluated",
    "verdict",
    "entry_price",
    "exit_price",
    "exit_time",
    "directional_move_pct",
    "max_favorable_pct",
    "max_adverse_pct",
    "bars_used",
]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return float(default)
    if math.isnan(parsed) or math.isinf(parsed):
        return float(default)
    return parsed


def _normalize_bars(df: pd.DataFrame, *, daily: bool = False) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(col[0]).lower() for col in df.columns]
    else:
        df.columns = [str(col).lower() for col in df.columns]
    df = df.reset_index()
    ts_col = next((c for c in df.columns if c in {"datetime", "date", "timestamp"} or "date" in c), df.columns[0])
    df = df.rename(
        columns={
            ts_col: "timestamp",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
    )
    required = ["timestamp", "open", "high", "low", "close", "volume"]
    if not set(required).issubset(df.columns):
        return pd.DataFrame(columns=required)
    if daily:
        date_text = df["timestamp"].astype(str).str.slice(0, 10)
        parsed_dates = pd.to_datetime(date_text, errors="coerce")
        df["timestamp"] = parsed_dates.apply(
            lambda item: EASTERN.localize(datetime.combine(item.date(), time(12, 0))) if pd.notna(item) else pd.NaT
        )
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert(EASTERN)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"]).sort_values("timestamp")
    return df[required].reset_index(drop=True)


def _parse_cache_range_from_filename(path: Path) -> tuple[date, date] | None:
    """Return the date window encoded in a cache filename, if available."""
    match = re.match(
        r"^(?P<symbol>.+)_(?P<interval>[^_]+)_(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})\.csv$",
        path.name,
    )
    if not match:
        return None
    try:
        return datetime.fromisoformat(match.group("start")).date(), datetime.fromisoformat(match.group("end")).date()
    except ValueError:
        return None


def _iter_cache_files(cache_dir: Path, symbol: str, interval: str):
    pattern = f"{symbol.upper()}_{interval}_*.csv"
    return cache_dir.glob(pattern)


def _find_cached_file(cache_dir: Path, symbol: str, interval: str, start: str, end: str) -> Path | None:
    start_date = datetime.fromisoformat(start).date()
    end_date = datetime.fromisoformat(end).date()
    preferred = _cache_path(cache_dir, symbol, interval, start, end)
    if preferred.exists():
        return preferred
    candidates: list[tuple[tuple[date, date], Path]] = []
    for path in _iter_cache_files(cache_dir, symbol, interval):
        parsed = _parse_cache_range_from_filename(path)
        if not parsed:
            continue
        file_start, file_end = parsed
        if file_start <= start_date and file_end >= end_date:
            candidates.append(((file_start, file_end), path))
    if not candidates:
        return None
    candidates.sort(key=lambda item: ((item[0][1] - item[0][0]).days, item[0][1]))
    return candidates[0][1]


def _cache_path(cache_dir: Path, symbol: str, interval: str, start: str, end: str) -> Path:
    safe = f"{symbol.upper()}_{interval}_{start}_{end}".replace(":", "").replace("/", "-")
    return cache_dir / f"{safe}.csv"


def _load_or_fetch_bars(symbol: str, cfg: ReplayConfig) -> pd.DataFrame:
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = _cache_path(cfg.cache_dir, symbol, cfg.interval, cfg.start, cfg.end)
    path = _find_cached_file(cfg.cache_dir, symbol, cfg.interval, cfg.start, cfg.end)
    if path is not None:
        out = _normalize_bars(pd.read_csv(path))
        if out.empty:
            return out
        start_ts = datetime.fromisoformat(cfg.start).replace(tzinfo=None)
        end_ts = datetime.fromisoformat(cfg.end).replace(tzinfo=None)
        start_at = EASTERN.localize(start_ts)
        end_at = EASTERN.localize(end_ts)
        return out[(out["timestamp"] >= start_at) & (out["timestamp"] < end_at)].reset_index(drop=True)
    if cfg.offline:
        raise FileNotFoundError(f"[historical_replay] Missing cache file for {symbol}: {symbol}_{cfg.interval}_{cfg.start}_{cfg.end}.csv")

    df = yf.download(
        symbol,
        start=cfg.start,
        end=cfg.end,
        interval=cfg.interval,
        auto_adjust=True,
        progress=False,
        prepost=False,
        threads=False,
    )
    out = _normalize_bars(df)
    if not out.empty:
        out.to_csv(cache_path, index=False)
    return out


def _daily_history_start(start: str, lookback_days: int) -> str:
    parsed = datetime.fromisoformat(str(start)).date()
    return (parsed - timedelta(days=max(30, int(lookback_days)))).isoformat()


def _load_or_fetch_daily_bars(symbol: str, cfg: ReplayConfig) -> pd.DataFrame:
    cfg.cache_dir.mkdir(parents=True, exist_ok=True)
    daily_start = _daily_history_start(cfg.start, cfg.daily_lookback_days)
    cache_path = _cache_path(cfg.cache_dir, symbol, "1d", daily_start, cfg.end)
    path = _find_cached_file(cfg.cache_dir, symbol, "1d", daily_start, cfg.end)
    if path is not None:
        out = _normalize_bars(pd.read_csv(path), daily=True)
        if out.empty:
            return out
        return out[(out["timestamp"].dt.date >= datetime.fromisoformat(daily_start).date()) & (out["timestamp"].dt.date < datetime.fromisoformat(cfg.end).date())].reset_index(drop=True)
    if cfg.offline:
        return pd.DataFrame()

    df = yf.download(
        symbol,
        start=daily_start,
        end=cfg.end,
        interval="1d",
        auto_adjust=True,
        progress=False,
        prepost=False,
        threads=False,
    )
    out = _normalize_bars(df, daily=True)
    if not out.empty:
        out.to_csv(cache_path, index=False)
    return out


def _session_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ts = df["timestamp"]
    market_open = time(9, 30)
    market_close = time(16, 0)
    return df[(ts.dt.time >= market_open) & (ts.dt.time <= market_close)].reset_index(drop=True)


class HistoricalReplayDataClient:
    """DataClient-compatible adapter pinned to a replay timestamp."""

    def __init__(
        self,
        bars_by_symbol: dict[str, pd.DataFrame],
        daily_bars_by_symbol: dict[str, pd.DataFrame] | None = None,
    ):
        self.bars_by_symbol = {k.upper(): _session_rows(v) for k, v in bars_by_symbol.items()}
        self.daily_bars_by_symbol = {
            k.upper(): v.sort_values("timestamp").reset_index(drop=True)
            for k, v in (daily_bars_by_symbol or {}).items()
            if v is not None and not v.empty
        }
        self.now_et: datetime | None = None

    def set_time(self, now_et: datetime) -> None:
        self.now_et = now_et.astimezone(EASTERN)

    def _bars_until_now(self, symbol: str) -> pd.DataFrame:
        if self.now_et is None:
            return pd.DataFrame()
        df = self.bars_by_symbol.get(symbol.upper(), pd.DataFrame())
        if df.empty:
            return df
        return df[df["timestamp"] <= self.now_et].reset_index(drop=True)

    def get_latest_stock_price(self, symbol: str) -> float | None:
        df = self._bars_until_now(symbol)
        if df.empty:
            return None
        return float(df.iloc[-1]["close"])

    def get_latest_stock_trade_price(self, symbol: str) -> float | None:
        return self.get_latest_stock_price(symbol)

    def get_latest_stock_quote(self, symbol: str) -> dict[str, float | None]:
        price = self.get_latest_stock_price(symbol)
        if price is None:
            return {"bid": None, "ask": None, "bid_size": None, "ask_size": None}
        spread = max(0.01, price * 0.0005)
        return {
            "bid": round(price - spread / 2, 4),
            "ask": round(price + spread / 2, 4),
            "bid_size": 1000.0,
            "ask_size": 1000.0,
        }

    def get_intraday_bars_since_open(
        self,
        symbol: str,
        now_et: datetime,
        limit: int = 120,
        bar_timeframe: str | None = None,
    ) -> pd.DataFrame:
        self.set_time(now_et)
        df = self._bars_until_now(symbol)
        if df.empty:
            return df
        session_date = now_et.astimezone(EASTERN).date()
        df = df[df["timestamp"].dt.date == session_date]
        return df.tail(limit).reset_index(drop=True)

    def get_intraday_bars_window(
        self,
        symbol: str,
        start_et: datetime,
        end_et: datetime,
        limit: int = 120,
    ) -> pd.DataFrame:
        df = self.bars_by_symbol.get(symbol.upper(), pd.DataFrame())
        if df.empty:
            return df
        if start_et.tzinfo is None:
            start_et = EASTERN.localize(start_et)
        if end_et.tzinfo is None:
            end_et = EASTERN.localize(end_et)
        out = df[(df["timestamp"] >= start_et) & (df["timestamp"] <= end_et)]
        return out.tail(limit).reset_index(drop=True)

    def get_stock_daily_bars(self, symbol: str, limit: int = 30) -> pd.DataFrame:
        daily = self.daily_bars_by_symbol.get(symbol.upper(), pd.DataFrame())
        if not daily.empty:
            out = daily
            if self.now_et is not None:
                session_date = self.now_et.astimezone(EASTERN).date()
                out = out[out["timestamp"].dt.date < session_date]
            return out.tail(limit).reset_index(drop=True)

        df = self._bars_until_now(symbol)
        if df.empty:
            return pd.DataFrame()
        daily = (
            df.set_index("timestamp")
            .resample("1D")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna()
            .reset_index()
        )
        return daily.tail(limit).reset_index(drop=True)

    def get_option_contracts(
        self,
        underlying_symbol: str,
        contract_type: str,
        expiration_date_gte,
        expiration_date_lte,
    ) -> list[dict[str, Any]]:
        price = self.get_latest_stock_price(underlying_symbol)
        if price is None or price <= 0:
            return []
        strike = round(price)
        suffix = "C" if str(contract_type).lower() == "call" else "P"
        exp = str(expiration_date_gte)
        symbol = f"{underlying_symbol.upper()}{exp[2:4]}{exp[5:7]}{exp[8:10]}{suffix}{int(strike * 1000):08d}"
        return [
            {
                "symbol": symbol,
                "option_symbol": symbol,
                "strike_price": strike,
                "expiration_date": exp,
                "status": "active",
                "tradable": True,
                "open_interest": 1000,
                "volume": 100,
                "implied_volatility": None,
                "delta": 0.50 if suffix == "C" else -0.50,
            }
        ]

    def get_option_contract(self, option_symbol: str) -> dict[str, Any]:
        return {"symbol": option_symbol, "status": "active", "tradable": True, "open_interest": 1000, "volume": 100}

    def has_earnings_within_days(self, symbol: str, days: int, now_et: datetime) -> bool:
        return False

    def has_high_impact_news(self, symbol: str, now_et: datetime, lookback_minutes: int, keywords: tuple[str, ...]):
        return False, ""


def _future_window(df: pd.DataFrame, symbol: str, entry_time: datetime, horizon_minutes: int) -> pd.DataFrame:
    if df.empty:
        return df
    end = entry_time + timedelta(minutes=max(1, int(horizon_minutes)))
    return df[(df["timestamp"] > entry_time) & (df["timestamp"] <= end)].reset_index(drop=True)


def _simulate_outcome(
    *,
    bars: pd.DataFrame,
    direction: str,
    entry_time: datetime,
    horizon_minutes: int,
    take_profit_pct: float,
    stop_loss_pct: float,
) -> dict[str, Any]:
    window = _future_window(bars, "", entry_time, horizon_minutes)
    if window.empty:
        return {"evaluated": False, "verdict": "no_future_bars"}

    entry = float(window.iloc[0]["open"] or window.iloc[0]["close"])
    if entry <= 0:
        return {"evaluated": False, "verdict": "invalid_entry"}

    max_fav = 0.0
    max_adv = 0.0
    exit_price = float(window.iloc[-1]["close"])
    exit_time = window.iloc[-1]["timestamp"]
    verdict = "timeout"

    for _, row in window.iterrows():
        high = float(row["high"])
        low = float(row["low"])
        if direction == "call":
            fav = ((high - entry) / entry) * 100.0
            adv = ((low - entry) / entry) * 100.0
            tp_hit = fav >= take_profit_pct
            sl_hit = adv <= -abs(stop_loss_pct)
        else:
            fav = ((entry - low) / entry) * 100.0
            adv = ((entry - high) / entry) * 100.0
            tp_hit = fav >= take_profit_pct
            sl_hit = adv <= -abs(stop_loss_pct)
        max_fav = max(max_fav, fav)
        max_adv = min(max_adv, adv)
        if tp_hit:
            exit_price = entry * (1.0 + take_profit_pct / 100.0) if direction == "call" else entry * (1.0 - take_profit_pct / 100.0)
            exit_time = row["timestamp"]
            verdict = "win"
            break
        if sl_hit:
            exit_price = entry * (1.0 - stop_loss_pct / 100.0) if direction == "call" else entry * (1.0 + stop_loss_pct / 100.0)
            exit_time = row["timestamp"]
            verdict = "loss"
            break

    raw_move = ((exit_price - entry) / entry) * 100.0
    directional_move = raw_move if direction == "call" else -raw_move
    if verdict == "timeout":
        verdict = "win" if directional_move > 0 else "loss" if directional_move < 0 else "flat"

    return {
        "evaluated": True,
        "verdict": verdict,
        "entry_price": round(entry, 4),
        "exit_price": round(exit_price, 4),
        "exit_time": str(exit_time),
        "directional_move_pct": round(directional_move, 4),
        "max_favorable_pct": round(max_fav, 4),
        "max_adverse_pct": round(max_adv, 4),
        "bars_used": len(window),
    }


def _result_columns(rows: list[dict[str, Any]]) -> list[str]:
    columns = list(REPLAY_RESULT_COLUMNS)
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)
    return columns


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = _result_columns(rows)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _summarize(
    rows: list[dict[str, Any]],
    *,
    scan_iterations: int = 0,
    failure_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    evaluated = [r for r in rows if str(r.get("evaluated")) == "True" or r.get("evaluated") is True]
    wins = [r for r in evaluated if r.get("verdict") == "win"]
    losses = [r for r in evaluated if r.get("verdict") == "loss"]
    failure_counts: dict[str, int] = {}
    failure_detail_counts: dict[str, int] = {}
    for row in failure_rows or []:
        reason = str(row.get("reason", "") or "unknown").strip() or "unknown"
        family = reason.split(":", 1)[0].strip() if ":" in reason else reason
        failure_counts[family] = failure_counts.get(family, 0) + 1
        failure_detail_counts[reason] = failure_detail_counts.get(reason, 0) + 1
    top_failures = [
        {"reason": key, "count": count}
        for key, count in sorted(failure_counts.items(), key=lambda item: item[1], reverse=True)[:8]
    ]
    top_failure_details = [
        {"reason": key, "count": count}
        for key, count in sorted(failure_detail_counts.items(), key=lambda item: item[1], reverse=True)[:12]
    ]
    by_profile: dict[str, dict[str, int]] = {}
    for row in evaluated:
        key = str(row.get("strategy_profile") or "unknown")
        by_profile.setdefault(key, {"trades": 0, "wins": 0, "losses": 0})
        by_profile[key]["trades"] += 1
        if row.get("verdict") == "win":
            by_profile[key]["wins"] += 1
        elif row.get("verdict") == "loss":
            by_profile[key]["losses"] += 1
    profile_rows = []
    for key, item in by_profile.items():
        trades = max(1, item["trades"])
        profile_rows.append({**item, "profile": key, "win_rate_pct": round(item["wins"] / trades * 100.0, 2)})
    profile_rows.sort(key=lambda item: (float(item["win_rate_pct"]), int(item["trades"])), reverse=True)
    total = max(1, len(evaluated))
    return {
        "scan_iterations": int(scan_iterations),
        "scan_failures": len(failure_rows or []),
        "top_failures": top_failures,
        "top_failure_details": top_failure_details,
        "opportunities": len(rows),
        "evaluated": len(evaluated),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / total * 100.0, 2),
        "by_profile": profile_rows,
    }


def run_replay(cfg: ReplayConfig) -> dict[str, Any]:
    old_config = {
        "RATE_LIMIT_SLEEP_SECONDS": config.RATE_LIMIT_SLEEP_SECONDS,
        "SCAN_INTRADAY_BARS": config.SCAN_INTRADAY_BARS,
        "SCAN_MIN_DAILY_BARS": getattr(config, "SCAN_MIN_DAILY_BARS", 8),
        "RVOL_AVG_DAILY_BARS": getattr(config, "RVOL_AVG_DAILY_BARS", 8),
        "ATR_PERIOD": getattr(config, "ATR_PERIOD", 7),
        "ATR_MIN_PERIOD": getattr(config, "ATR_MIN_PERIOD", 4),
    }
    min_daily_bars = max(3, int(cfg.min_daily_bars))
    config.RATE_LIMIT_SLEEP_SECONDS = 0.0
    config.SCAN_INTRADAY_BARS = max(1, int(cfg.scan_bars))
    config.SCAN_MIN_DAILY_BARS = min_daily_bars
    config.RVOL_AVG_DAILY_BARS = min_daily_bars
    config.ATR_PERIOD = max(2, min(int(old_config["ATR_PERIOD"] or 7), min_daily_bars - 1))
    config.ATR_MIN_PERIOD = max(2, min(int(old_config["ATR_MIN_PERIOD"] or 4), min_daily_bars - 1))
    try:
        bars_by_symbol = {}
        missing_symbol_rows: list[str] = []
        for symbol in cfg.symbols:
            try:
                bars_by_symbol[symbol] = _load_or_fetch_bars(symbol, cfg)
            except FileNotFoundError:
                missing_symbol_rows.append(symbol)
        if missing_symbol_rows and cfg.offline:
            missing = ", ".join(missing_symbol_rows)
            raise FileNotFoundError(
                "Offline replay requires cached bars for all requested symbols. "
                f"Missing: {missing}. Build cache first by running with network or preloading."
            )
        bars_by_symbol = {s: df for s, df in bars_by_symbol.items() if not df.empty}
        if not bars_by_symbol:
            raise ValueError("No symbols with usable intraday data were loaded for replay.")
        daily_bars_by_symbol = {
            symbol: _load_or_fetch_daily_bars(symbol, cfg)
            for symbol in bars_by_symbol.keys()
        }
        data_client = HistoricalReplayDataClient(bars_by_symbol, daily_bars_by_symbol)
        scanner = IntradayScanner(data_client, emit_summary=False, write_scan_log=False)  # type: ignore[arg-type]

        all_times = sorted(
            {
                ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
                for df in bars_by_symbol.values()
                for ts in df["timestamp"].tolist()
            }
        )
        rows: list[dict[str, Any]] = []
        failure_rows: list[dict[str, Any]] = []
        scan_iterations = 0
        last_scan: datetime | None = None
        for now_et in all_times:
            now_et = now_et.astimezone(EASTERN)
            if now_et.time() < time(9, 35) or now_et.time() > time(15, 15):
                continue
            if last_scan is not None and (now_et - last_scan).total_seconds() < cfg.scan_every_minutes * 60:
                continue
            last_scan = now_et
            data_client.set_time(now_et)
            signals = scanner.run_scan(list(bars_by_symbol.keys()), now_et=now_et)
            scan_iterations += 1
            failure_rows.extend(list(getattr(scanner, "last_failures", []) or []))
            for signal in signals[: cfg.max_signals_per_scan]:
                symbol = str(signal.get("symbol", "") or "").upper()
                direction = str(signal.get("direction", "") or "").lower()
                if direction not in {"call", "put"}:
                    continue
                outcome = _simulate_outcome(
                    bars=bars_by_symbol.get(symbol, pd.DataFrame()),
                    direction=direction,
                    entry_time=now_et,
                    horizon_minutes=cfg.horizon_minutes,
                    take_profit_pct=cfg.take_profit_pct,
                    stop_loss_pct=cfg.stop_loss_pct,
                )
                rows.append(
                    {
                        "timestamp": now_et.isoformat(),
                        "symbol": symbol,
                        "direction": direction,
                        "strategy_profile": signal.get("strategy_profile", ""),
                        "signal_score": signal.get("signal_score", ""),
                        "direction_score": signal.get("direction_score", ""),
                        "rvol": signal.get("rvol", ""),
                        "roc": signal.get("roc", ""),
                        "rsi": signal.get("rsi", ""),
                        "volatility_score": signal.get("volatility_score", ""),
                        "reason": signal.get("reason", ""),
                        **outcome,
                    }
                )
        _write_rows(cfg.output, rows)
        summary = _summarize(rows, scan_iterations=scan_iterations, failure_rows=failure_rows)
        summary_path = cfg.output.with_suffix(".summary.json")
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        return {"output": str(cfg.output), "summary_path": str(summary_path), "summary": summary}
    finally:
        for key, value in old_config.items():
            setattr(config, key, value)


def _parse_args() -> ReplayConfig:
    parser = argparse.ArgumentParser(description="Replay historical bars through AutoBott scanner.")
    parser.add_argument("--symbols", default=",".join(config.CORE_TICKERS), help="Comma-separated symbols.")
    parser.add_argument(
        "--offline",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Load bars only from cache files. Use --offline/--no-offline to control. Default: --offline.",
    )
    parser.add_argument("--start", required=True, help="YYYY-MM-DD start date.")
    parser.add_argument("--end", required=True, help="YYYY-MM-DD end date.")
    parser.add_argument("--interval", default="5m", help="yfinance interval, e.g. 1m, 5m, 15m, 1d.")
    parser.add_argument(
        "--scan-bars",
        type=int,
        default=int(getattr(config, "SCAN_INTRADAY_BARS", 60)),
        help="Number of intraday bars passed to each scanner snapshot.",
    )
    parser.add_argument("--scan-every-minutes", type=int, default=5)
    parser.add_argument("--horizon-minutes", type=int, default=45)
    parser.add_argument("--take-profit-pct", type=float, default=0.35, help="Underlying directional move percent.")
    parser.add_argument("--stop-loss-pct", type=float, default=0.20, help="Underlying adverse move percent.")
    parser.add_argument("--max-signals-per-scan", type=int, default=2)
    parser.add_argument("--output", default=str(Path(config.DATA_DIR) / "historical_replay_results.csv"))
    parser.add_argument("--cache-dir", default=str(Path(config.DATA_DIR) / "historical_cache"))
    parser.add_argument("--daily-lookback-days", type=int, default=90, help="Calendar days of daily bars to backfill for scanner context.")
    parser.add_argument("--min-daily-bars", type=int, default=int(getattr(config, "SCAN_MIN_DAILY_BARS", 8)), help="Minimum prior daily bars required by the replay scanner.")
    args = parser.parse_args()
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    return ReplayConfig(
        symbols=symbols,
        start=str(args.start),
        end=str(args.end),
        interval=str(args.interval),
        scan_bars=max(1, int(args.scan_bars)),
        scan_every_minutes=max(1, int(args.scan_every_minutes)),
        horizon_minutes=max(1, int(args.horizon_minutes)),
        take_profit_pct=float(args.take_profit_pct),
        stop_loss_pct=float(args.stop_loss_pct),
        max_signals_per_scan=max(1, int(args.max_signals_per_scan)),
        output=Path(args.output),
        cache_dir=Path(args.cache_dir),
        daily_lookback_days=max(30, int(args.daily_lookback_days)),
        min_daily_bars=max(3, int(args.min_daily_bars)),
        offline=bool(args.offline),
    )


if __name__ == "__main__":
    result = run_replay(_parse_args())
    print(json.dumps(result, indent=2))
