"""Trade analytics and execution quality review for trades.csv.

Run directly:  python -m autotrader.review
            or python autotrader/review.py

Export examples:
    python autotrader/review.py --format json
    python autotrader/review.py --format json --output autotrader/trade_report.json
    python autotrader/review.py --export-csv-dir autotrader/reports

Sections produced:
  1. Overall summary (paper PnL vs conservative PnL, expectancy, win-rate)
  2. Paper vs. Conservative PnL gap distribution
  3. By exit reason
  4. By signal-score bucket
  5. By ticker
  6. By contract spread quartile
  7. By entry hour (ET)
  8. Execution quality (entry/exit fill latency, slippage, retry rates)
    9. Joint tradeability interactions (ticker x hour, spread x score, stop-loss clusters)
    10. Stop-loss geometry diagnostics
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# Allow running as a standalone script from the repo root or inside autotrader/
_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

import config  # noqa: E402
from logger import TradeLogger  # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_SEP = "=" * 72
_SUBSEP = "-" * 72
_MIN_COMBO_TRADES = 3


def _hdr(title: str) -> None:
    print(f"\n{_SEP}")
    print(f"  {title}")
    print(_SEP)


def _pct(v: float) -> str:
    """Format a fraction (0.12 → +12.0%) or NaN gracefully."""
    if pd.isna(v):
        return "  n/a"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.1f}%"


def _usd(v: float) -> str:
    if pd.isna(v):
        return "  n/a"
    sign = "+" if v >= 0 else ""
    return f"{sign}${v:.2f}"


def _safe_float(value: object) -> float | None:
    try:
        if value is None or pd.isna(value):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _json_ready(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Interval):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if value is None or pd.isna(value):
        return None
    return value


def _load(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        print(f"No trades file found at {path}")
        return None
    df = pd.read_csv(path)
    for column in TradeLogger.columns:
        if column not in df.columns:
            df[column] = pd.NA
    if df.empty:
        return df

    # numeric coercions
    num_cols = [
        "pnl_pct",
        "paper_reported_pnl_usd",
        "paper_reported_pnl_pct",
        "conservative_executable_pnl_usd",
        "conservative_executable_pnl_pct",
        "realized_pnl_usd",
        "signal_score",
        "direction_score",
        "rvol",
        "rsi",
        "roc",
        "iv_rank",
        "contract_spread_pct",
        "hold_seconds",
        "entry_bid_submit",
        "entry_ask_submit",
        "entry_midpoint_submit",
        "entry_filled_price",
        "entry_spread_pct",
        "entry_fill_slippage_vs_ask_pct",
        "entry_fill_seconds",
        "entry_attempts",
        "exit_bid_submit",
        "exit_ask_submit",
        "exit_midpoint_submit",
        "exit_filled_price",
        "exit_spread_pct",
        "exit_fill_slippage_vs_bid_pct",
        "exit_fill_seconds",
        "exit_attempts",
        "max_favorable_excursion_pct",
        "max_adverse_excursion_pct",
        "qty",
        "entry_price",
        "exit_price",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # parse timestamps
    for col in ("entry_time", "exit_time", "timestamp"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # entry hour ET (no tz conversion — data is already ET from Alpaca US market)
    if "entry_time" in df.columns:
        df["entry_hour"] = df["entry_time"].dt.hour
    elif "timestamp" in df.columns:
        df["entry_hour"] = df["timestamp"].dt.hour
    else:
        df["entry_hour"] = pd.NA

    # conservative pnl win flag
    df["con_win"] = df["conservative_executable_pnl_usd"].apply(
        lambda x: True if (not pd.isna(x) and x > 0) else (False if not pd.isna(x) else pd.NA)
    )
    df["paper_win"] = df["paper_reported_pnl_usd"].apply(
        lambda x: True if (not pd.isna(x) and x > 0) else (False if not pd.isna(x) else pd.NA)
    )

    return df


def _win_rate(series: pd.Series) -> float:
    valid = series.dropna()
    if len(valid) == 0:
        return float("nan")
    return float((valid > 0).sum() / len(valid))


def _expectancy(pnl_usd: pd.Series) -> float:
    """Avg PnL per trade in USD."""
    valid = pnl_usd.dropna()
    return float(valid.mean()) if len(valid) else float("nan")


def _group_summary_rows(df: pd.DataFrame, group_col: str) -> list[dict[str, object]]:
    if group_col not in df.columns or df[group_col].isna().all():
        return []

    rows: list[dict[str, object]] = []
    for name, g in df.groupby(group_col, observed=True):
        n = len(g)
        paper_wr = _win_rate(g["paper_reported_pnl_usd"])
        con_wr = _win_rate(g["conservative_executable_pnl_usd"])
        paper_exp = _expectancy(g["paper_reported_pnl_usd"])
        con_exp = _expectancy(g["conservative_executable_pnl_usd"])
        gap = (
            (paper_exp - con_exp)
            if (not pd.isna(paper_exp) and not pd.isna(con_exp))
            else float("nan")
        )
        rows.append(
            {
                group_col: str(name),
                "n": int(n),
                "paper_win_rate": paper_wr,
                "conservative_win_rate": con_wr,
                "paper_expectancy_usd": paper_exp,
                "conservative_expectancy_usd": con_exp,
                "paper_minus_conservative_gap_usd": gap,
            }
        )

    rows.sort(
        key=lambda row: (
            float("inf")
            if _safe_float(row.get("conservative_expectancy_usd")) is None
            else float(row["conservative_expectancy_usd"])
        )
    )
    return rows


def _rows_to_table(rows: list[dict[str, object]], index_col: str) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).set_index(index_col)
    render = df.copy()
    for col in render.columns:
        if "win_rate" in col:
            render[col] = render[col].apply(_pct)
        elif col.endswith("_usd"):
            render[col] = render[col].apply(_usd)
    rename_map = {
        "paper_win_rate": "paper_wr%",
        "conservative_win_rate": "con_wr%",
        "paper_expectancy_usd": "paper_exp",
        "conservative_expectancy_usd": "con_exp",
        "paper_minus_conservative_gap_usd": "gap(p-c)",
    }
    return render.rename(columns=rename_map)


def _group_summary(df: pd.DataFrame, group_col: str, title: str) -> None:
    _hdr(title)
    rows = _group_summary_rows(df, group_col)
    if group_col not in df.columns:
        print(f"  Column '{group_col}' not in data — skipping.")
        return
    if not rows:
        print(f"  No data for '{group_col}' — skipping.")
        return
    summary = _rows_to_table(rows, group_col)
    print(summary.to_string())


def _build_overall(df: pd.DataFrame) -> dict[str, object]:
    n = len(df)
    paper_wr = _win_rate(df["paper_reported_pnl_usd"])
    con_wr = _win_rate(df["conservative_executable_pnl_usd"])
    paper_exp = _expectancy(df["paper_reported_pnl_usd"])
    con_exp = _expectancy(df["conservative_executable_pnl_usd"])
    realized_exp = _expectancy(df.get("realized_pnl_usd", pd.Series(dtype=float)))

    hold_med = df["hold_seconds"].median() if "hold_seconds" in df.columns else float("nan")
    hold_avg = df["hold_seconds"].mean() if "hold_seconds" in df.columns else float("nan")

    con_valid = df["conservative_executable_pnl_usd"].dropna()
    con_wins = con_valid[con_valid > 0]
    con_losses = con_valid[con_valid <= 0]
    avg_con_win = con_wins.mean() if not con_wins.empty else float("nan")
    avg_con_loss = con_losses.mean() if not con_losses.empty else float("nan")

    mfe_avg = df["max_favorable_excursion_pct"].mean() if "max_favorable_excursion_pct" in df.columns else float("nan")
    mae_avg = df["max_adverse_excursion_pct"].mean() if "max_adverse_excursion_pct" in df.columns else float("nan")

    return {
        "total_closed_trades": int(n),
        "paper_win_rate": paper_wr,
        "conservative_win_rate": con_wr,
        "paper_expectancy_usd": paper_exp,
        "conservative_expectancy_usd": con_exp,
        "realized_fill_expectancy_usd": realized_exp,
        "paper_overstatement_avg_usd": (
            paper_exp - con_exp if not pd.isna(paper_exp) and not pd.isna(con_exp) else float("nan")
        ),
        "avg_hold_seconds": hold_avg,
        "median_hold_seconds": hold_med,
        "avg_conservative_win_usd": avg_con_win,
        "avg_conservative_loss_usd": avg_con_loss,
        "avg_mfe_pct": mfe_avg,
        "avg_mae_pct": mae_avg,
    }


def _build_gap(df: pd.DataFrame) -> dict[str, object]:
    both = df[["paper_reported_pnl_usd", "conservative_executable_pnl_usd"]].dropna()
    if both.empty:
        return {"trades_with_both_values": 0, "deciles_usd": {}}

    gap_series = both["paper_reported_pnl_usd"] - both["conservative_executable_pnl_usd"]
    deciles = gap_series.quantile([0.1, 0.25, 0.5, 0.75, 0.9])
    return {
        "trades_with_both_values": int(len(both)),
        "mean_gap_usd": float(gap_series.mean()),
        "median_gap_usd": float(gap_series.median()),
        "max_gap_usd": float(gap_series.max()),
        "min_gap_usd": float(gap_series.min()),
        "paper_gt_conservative_rate": float((gap_series > 0).mean()),
        "paper_lt_conservative_rate": float((gap_series < 0).mean()),
        "deciles_usd": {f"p{int(q * 100)}": float(val) for q, val in deciles.items()},
    }


def _build_signal_score_rows(df: pd.DataFrame) -> list[dict[str, object]]:
    if "signal_score" not in df.columns or df["signal_score"].isna().all():
        return []
    bucketed = df.copy()
    bins = [0, 3, 5, 7, float("inf")]
    labels = ["[0-3)", "[3-5)", "[5-7)", "[7+)"]
    bucketed["score_bucket"] = pd.cut(bucketed["signal_score"], bins=bins, labels=labels, right=False)
    return _group_summary_rows(bucketed, "score_bucket")


def _build_spread_rows(df: pd.DataFrame) -> list[dict[str, object]]:
    if "contract_spread_pct" not in df.columns or df["contract_spread_pct"].isna().all():
        return []
    bucketed = df.copy()
    try:
        bucketed["spread_quartile"] = pd.qcut(
            bucketed["contract_spread_pct"], q=4, labels=["Q1(tight)", "Q2", "Q3", "Q4(wide)"], duplicates="drop"
        )
    except ValueError:
        return []
    return _group_summary_rows(bucketed, "spread_quartile")


def _build_hour_rows(df: pd.DataFrame) -> list[dict[str, object]]:
    if "entry_hour" not in df.columns or df["entry_hour"].isna().all():
        return []
    rows = []
    for hour, g in df.groupby("entry_hour", observed=True):
        rows.append(
            {
                "hour_et": f"{int(hour):02d}:00",
                "n": int(len(g)),
                "conservative_win_rate": _win_rate(g["conservative_executable_pnl_usd"]),
                "paper_expectancy_usd": _expectancy(g["paper_reported_pnl_usd"]),
                "conservative_expectancy_usd": _expectancy(g["conservative_executable_pnl_usd"]),
            }
        )
    rows.sort(
        key=lambda row: (
            float("inf")
            if _safe_float(row.get("conservative_expectancy_usd")) is None
            else float(row["conservative_expectancy_usd"])
        )
    )
    return rows


def _build_execution(df: pd.DataFrame) -> dict[str, object]:
    def _stat_dict(series: pd.Series) -> dict[str, object]:
        valid = series.dropna()
        if valid.empty:
            return {"n": 0, "mean": None, "median": None, "p95": None}
        return {
            "n": int(len(valid)),
            "mean": float(valid.mean()),
            "median": float(valid.median()),
            "p95": float(valid.quantile(0.95)),
        }

    result: dict[str, object] = {
        "entry": {
            "fill_seconds": _stat_dict(df.get("entry_fill_seconds", pd.Series(dtype=float))),
            "attempts": _stat_dict(df.get("entry_attempts", pd.Series(dtype=float))),
            "slippage_vs_ask_pct": _stat_dict(df.get("entry_fill_slippage_vs_ask_pct", pd.Series(dtype=float))),
            "spread_pct": _stat_dict(df.get("entry_spread_pct", pd.Series(dtype=float))),
        },
        "exit": {
            "fill_seconds": _stat_dict(df.get("exit_fill_seconds", pd.Series(dtype=float))),
            "attempts": _stat_dict(df.get("exit_attempts", pd.Series(dtype=float))),
            "slippage_vs_bid_pct": _stat_dict(df.get("exit_fill_slippage_vs_bid_pct", pd.Series(dtype=float))),
            "spread_pct": _stat_dict(df.get("exit_spread_pct", pd.Series(dtype=float))),
        },
        "entry_retry_rate": None,
        "exit_retry_rate": None,
        "exit_fill_seconds_by_reason": [],
    }

    if "entry_attempts" in df.columns and len(df):
        retry = df[df["entry_attempts"].fillna(1) > 1]
        result["entry_retry_rate"] = float(len(retry) / len(df))

    if "exit_attempts" in df.columns and len(df):
        retry = df[df["exit_attempts"].fillna(1) > 1]
        result["exit_retry_rate"] = float(len(retry) / len(df))

    if "exit_reason" in df.columns and "exit_fill_seconds" in df.columns:
        grouped = (
            df.groupby("exit_reason", observed=True)["exit_fill_seconds"]
            .agg(["count", "mean", "median"])
            .rename(columns={"count": "n", "mean": "avg_sec", "median": "med_sec"})
            .sort_values("avg_sec", ascending=False)
        )
        result["exit_fill_seconds_by_reason"] = [
            {
                "exit_reason": str(index),
                "n": int(row["n"]),
                "avg_sec": _safe_float(row["avg_sec"]),
                "med_sec": _safe_float(row["med_sec"]),
            }
            for index, row in grouped.iterrows()
        ]

    return result


def _bucket_signal_score(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    bins = [0, 3, 5, 7, float("inf")]
    labels = ["[0-3)", "[3-5)", "[5-7)", "[7+)"]
    out["score_bucket"] = pd.cut(out["signal_score"], bins=bins, labels=labels, right=False)
    return out


def _bucket_spread_quartile(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["spread_quartile"] = pd.qcut(
        out["contract_spread_pct"], q=4, labels=["Q1(tight)", "Q2", "Q3", "Q4(wide)"], duplicates="drop"
    )
    return out


def _composite_summary_rows(
    df: pd.DataFrame,
    group_cols: list[str],
    *,
    min_trades: int = _MIN_COMBO_TRADES,
) -> list[dict[str, object]]:
    if df.empty:
        return []
    for col in group_cols:
        if col not in df.columns:
            return []

    grouped_rows: list[dict[str, object]] = []
    grouped = df.groupby(group_cols, observed=True)
    for keys, g in grouped:
        n = len(g)
        if n < min_trades:
            continue
        if not isinstance(keys, tuple):
            keys = (keys,)
        row: dict[str, object] = {
            "n": int(n),
            "conservative_win_rate": _win_rate(g["conservative_executable_pnl_usd"]),
            "paper_expectancy_usd": _expectancy(g["paper_reported_pnl_usd"]),
            "conservative_expectancy_usd": _expectancy(g["conservative_executable_pnl_usd"]),
            "paper_minus_conservative_gap_usd": (
                _expectancy(g["paper_reported_pnl_usd"]) - _expectancy(g["conservative_executable_pnl_usd"])
            ),
            "avg_mae_pct": _safe_float(g["max_adverse_excursion_pct"].mean()),
            "avg_mfe_pct": _safe_float(g["max_favorable_excursion_pct"].mean()),
            "stop_loss_rate": float((g["exit_reason"].fillna("").astype(str).str.lower() == "stop_loss").mean())
            if "exit_reason" in g.columns
            else None,
        }
        for idx, col in enumerate(group_cols):
            row[col] = str(keys[idx])
        grouped_rows.append(row)

    grouped_rows.sort(
        key=lambda row: (
            float("inf") if _safe_float(row.get("conservative_expectancy_usd")) is None else float(row["conservative_expectancy_usd"]),
            -int(row.get("n", 0) or 0),
        )
    )
    return grouped_rows


def _build_joint_tradeability(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return {
            "ticker_x_hour": [],
            "spread_quartile_x_score_bucket": [],
            "stop_loss_x_ticker": [],
            "stop_loss_x_hour": [],
            "mae_x_spread_x_ticker": [],
        }

    ticker_hour = _composite_summary_rows(df, ["ticker", "entry_hour"], min_trades=_MIN_COMBO_TRADES)

    spread_score: list[dict[str, object]] = []
    if "contract_spread_pct" in df.columns and "signal_score" in df.columns:
        try:
            spread_df = _bucket_spread_quartile(df)
            spread_df = _bucket_signal_score(spread_df)
            spread_score = _composite_summary_rows(
                spread_df,
                ["spread_quartile", "score_bucket"],
                min_trades=_MIN_COMBO_TRADES,
            )
        except ValueError:
            spread_score = []

    stop_loss_df = df[df.get("exit_reason", pd.Series(dtype=object)).fillna("").astype(str).str.lower() == "stop_loss"].copy()
    stop_loss_x_ticker = _composite_summary_rows(stop_loss_df, ["ticker"], min_trades=1)
    stop_loss_x_hour = _composite_summary_rows(stop_loss_df, ["entry_hour"], min_trades=1)

    mae_spread_ticker: list[dict[str, object]] = []
    if "contract_spread_pct" in df.columns:
        try:
            mae_df = _bucket_spread_quartile(df)
            mae_spread_ticker = _composite_summary_rows(
                mae_df,
                ["ticker", "spread_quartile"],
                min_trades=_MIN_COMBO_TRADES,
            )
        except ValueError:
            mae_spread_ticker = []

    return {
        "ticker_x_hour": ticker_hour,
        "spread_quartile_x_score_bucket": spread_score,
        "stop_loss_x_ticker": stop_loss_x_ticker,
        "stop_loss_x_hour": stop_loss_x_hour,
        "mae_x_spread_x_ticker": mae_spread_ticker,
    }


def _build_stop_loss_geometry(df: pd.DataFrame) -> dict[str, object]:
    if df.empty:
        return {
            "stop_loss_trade_count": 0,
            "stop_loss_rate": None,
            "mae_pct_at_stop_loss": {"mean": None, "median": None, "p90": None},
            "estimated_mae_usd_1c": {"mean": None, "median": None, "p90": None},
            "comment": "No closed trades yet.",
        }

    stop_loss_mask = df.get("exit_reason", pd.Series(dtype=object)).fillna("").astype(str).str.lower() == "stop_loss"
    stop_loss_df = df[stop_loss_mask].copy()
    if stop_loss_df.empty:
        return {
            "stop_loss_trade_count": 0,
            "stop_loss_rate": 0.0,
            "mae_pct_at_stop_loss": {"mean": None, "median": None, "p90": None},
            "estimated_mae_usd_1c": {"mean": None, "median": None, "p90": None},
            "comment": "No stop-loss exits in sample.",
        }

    mae_pct = pd.to_numeric(stop_loss_df.get("max_adverse_excursion_pct"), errors="coerce").abs()
    entry_px = pd.to_numeric(stop_loss_df.get("entry_price"), errors="coerce")
    qty = pd.to_numeric(stop_loss_df.get("qty"), errors="coerce").fillna(1).clip(lower=1)
    est_mae_usd = (mae_pct / 100.0) * entry_px * (qty * 100.0)

    def _moments(series: pd.Series) -> dict[str, object]:
        valid = pd.to_numeric(series, errors="coerce").dropna()
        if valid.empty:
            return {"mean": None, "median": None, "p90": None}
        return {
            "mean": float(valid.mean()),
            "median": float(valid.median()),
            "p90": float(valid.quantile(0.90)),
        }

    stop_loss_rate = float(len(stop_loss_df) / len(df)) if len(df) else None
    return {
        "stop_loss_trade_count": int(len(stop_loss_df)),
        "stop_loss_rate": stop_loss_rate,
        "mae_pct_at_stop_loss": _moments(mae_pct),
        "estimated_mae_usd_1c": _moments(est_mae_usd),
        "configured_stop_loss_usd": float(getattr(config, "STOP_LOSS_USD", 10.0)),
        "comment": "Compare estimated MAE-at-stop-loss moments against configured STOP_LOSS_USD to assess stop tightness.",
    }


def _build_report(df: pd.DataFrame, csv_path: Path) -> dict[str, object]:
    return {
        "metadata": {
            "source_csv": str(csv_path),
            "closed_trade_count": int(len(df)),
            "columns": list(df.columns),
        },
        "overall": _build_overall(df),
        "paper_vs_conservative_gap": _build_gap(df),
        "by_exit_reason": _group_summary_rows(df, "exit_reason"),
        "by_signal_score_bucket": _build_signal_score_rows(df),
        "by_ticker": _group_summary_rows(df, "ticker"),
        "by_contract_spread_quartile": _build_spread_rows(df),
        "by_entry_hour_et": _build_hour_rows(df),
        "execution_quality": _build_execution(df),
        "joint_tradeability": _build_joint_tradeability(df),
        "stop_loss_geometry": _build_stop_loss_geometry(df),
    }


def _write_json_report(report: dict[str, object], output_path: Path | None) -> None:
    payload = json.dumps(_json_ready(report), indent=2, sort_keys=False)
    if output_path is None:
        print(payload)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(payload + "\n", encoding="utf-8")
    print(f"Wrote JSON report to {output_path}")


def _write_csv_exports(report: dict[str, object], export_dir: Path) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)

    list_sections = {
        "by_exit_reason": "exit_reason.csv",
        "by_signal_score_bucket": "signal_score_bucket.csv",
        "by_ticker": "ticker.csv",
        "by_contract_spread_quartile": "contract_spread_quartile.csv",
        "by_entry_hour_et": "entry_hour_et.csv",
        "execution_exit_fill_seconds_by_reason": "execution_exit_fill_seconds_by_reason.csv",
        "joint_ticker_x_hour": "joint_ticker_x_hour.csv",
        "joint_spread_quartile_x_score_bucket": "joint_spread_quartile_x_score_bucket.csv",
        "joint_stop_loss_x_ticker": "joint_stop_loss_x_ticker.csv",
        "joint_stop_loss_x_hour": "joint_stop_loss_x_hour.csv",
        "joint_mae_x_spread_x_ticker": "joint_mae_x_spread_x_ticker.csv",
    }

    section_values = dict(report)
    section_values["execution_exit_fill_seconds_by_reason"] = (
        ((report.get("execution_quality") or {}).get("exit_fill_seconds_by_reason"))
        if isinstance(report.get("execution_quality"), dict)
        else []
    )
    joint = report.get("joint_tradeability") or {}
    section_values["joint_ticker_x_hour"] = joint.get("ticker_x_hour", []) if isinstance(joint, dict) else []
    section_values["joint_spread_quartile_x_score_bucket"] = joint.get("spread_quartile_x_score_bucket", []) if isinstance(joint, dict) else []
    section_values["joint_stop_loss_x_ticker"] = joint.get("stop_loss_x_ticker", []) if isinstance(joint, dict) else []
    section_values["joint_stop_loss_x_hour"] = joint.get("stop_loss_x_hour", []) if isinstance(joint, dict) else []
    section_values["joint_mae_x_spread_x_ticker"] = joint.get("mae_x_spread_x_ticker", []) if isinstance(joint, dict) else []

    for section, filename in list_sections.items():
        rows = section_values.get(section) or []
        out_path = export_dir / filename
        pd.DataFrame(rows).to_csv(out_path, index=False)

    summary_path = export_dir / "overall.json"
    summary_path.write_text(
        json.dumps(_json_ready(report.get("overall") or {}), indent=2) + "\n",
        encoding="utf-8",
    )
    print(f"Wrote CSV exports to {export_dir}")


def _render_composite_table(rows: list[dict[str, object]], index_cols: list[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    tbl = pd.DataFrame(rows)
    for col in ("conservative_win_rate", "stop_loss_rate"):
        if col in tbl.columns:
            tbl[col] = tbl[col].apply(lambda v: _pct(float(v)) if _safe_float(v) is not None else "  n/a")
    for col in ("paper_expectancy_usd", "conservative_expectancy_usd", "paper_minus_conservative_gap_usd"):
        if col in tbl.columns:
            tbl[col] = tbl[col].apply(lambda v: _usd(float(v)) if _safe_float(v) is not None else "  n/a")
    if "avg_mae_pct" in tbl.columns:
        tbl["avg_mae_pct"] = tbl["avg_mae_pct"].apply(lambda v: _pct(float(v) / 100.0) if _safe_float(v) is not None else "  n/a")
    if "avg_mfe_pct" in tbl.columns:
        tbl["avg_mfe_pct"] = tbl["avg_mfe_pct"].apply(lambda v: _pct(float(v) / 100.0) if _safe_float(v) is not None else "  n/a")
    return tbl.set_index(index_cols) if index_cols else tbl


# ---------------------------------------------------------------------------
# section 1 — overall summary
# ---------------------------------------------------------------------------

def _section_overall(df: pd.DataFrame) -> None:
    _hdr("1. OVERALL SUMMARY")
    overall = _build_overall(df)

    print(f"  Total closed trades      : {overall['total_closed_trades']}")
    print(f"  Paper win rate           : {_pct(float(overall['paper_win_rate']))}")
    print(f"  Conservative win rate    : {_pct(float(overall['conservative_win_rate']))}")
    print(f"  Paper expectancy / trade : {_usd(float(overall['paper_expectancy_usd']))}")
    print(f"  Conservative expectancy  : {_usd(float(overall['conservative_expectancy_usd']))}")
    print(f"  Realized fill expectancy : {_usd(float(overall['realized_fill_expectancy_usd']))}")
    print(f"  Paper overstatement avg  : {_usd(float(overall['paper_overstatement_avg_usd']))}")
    avg_hold_seconds = _safe_float(overall.get("avg_hold_seconds"))
    median_hold_seconds = _safe_float(overall.get("median_hold_seconds"))
    print(f"  Avg hold (sec)           : {avg_hold_seconds:.0f}" if avg_hold_seconds is not None else "  Avg hold (sec) : n/a")
    print(f"  Median hold (sec)        : {median_hold_seconds:.0f}" if median_hold_seconds is not None else "  Median hold (sec) : n/a")
    print(f"  Avg conservative win     : {_usd(float(overall['avg_conservative_win_usd']))}")
    print(f"  Avg conservative loss    : {_usd(float(overall['avg_conservative_loss_usd']))}")
    print(f"  Avg MFE                  : {_pct(float(overall['avg_mfe_pct']))}")
    print(f"  Avg MAE                  : {_pct(float(overall['avg_mae_pct']))}")


# ---------------------------------------------------------------------------
# section 2 — paper vs conservative gap
# ---------------------------------------------------------------------------

def _section_gap(df: pd.DataFrame) -> None:
    _hdr("2. PAPER vs. CONSERVATIVE PnL GAP")

    gap = _build_gap(df)
    if int(gap.get("trades_with_both_values", 0) or 0) == 0:
        print("  No rows with both paper and conservative PnL.")
        return

    print(f"  Trades with both values  : {gap['trades_with_both_values']}")
    print(f"  Mean gap (paper - con)   : {_usd(float(gap['mean_gap_usd']))}")
    print(f"  Median gap               : {_usd(float(gap['median_gap_usd']))}")
    print(f"  Max gap                  : {_usd(float(gap['max_gap_usd']))}")
    print(f"  Min gap                  : {_usd(float(gap['min_gap_usd']))}")
    print(f"  % where paper > con      : {float(gap['paper_gt_conservative_rate']) * 100:.1f}%")
    print(f"  % where paper < con      : {float(gap['paper_lt_conservative_rate']) * 100:.1f}%")

    # decile breakdown
    print("\n  Gap deciles (USD, paper minus conservative):")
    for label, value in (gap.get("deciles_usd") or {}).items():
        print(f"    {label:>4}: {_usd(float(value))}")


# ---------------------------------------------------------------------------
# section 3 — by exit reason
# ---------------------------------------------------------------------------

def _section_exit_reason(df: pd.DataFrame) -> None:
    _group_summary(df, "exit_reason", "3. BY EXIT REASON")


# ---------------------------------------------------------------------------
# section 4 — by signal-score bucket
# ---------------------------------------------------------------------------

def _section_signal_score(df: pd.DataFrame) -> None:
    _hdr("4. BY SIGNAL-SCORE BUCKET")

    if "signal_score" not in df.columns or df["signal_score"].isna().all():
        print("  No signal_score data.")
        return

    bins = [0, 3, 5, 7, float("inf")]
    labels = ["[0-3)", "[3-5)", "[5-7)", "[7+)"]
    df = df.copy()
    df["score_bucket"] = pd.cut(df["signal_score"], bins=bins, labels=labels, right=False)
    _group_summary(df, "score_bucket", "4. BY SIGNAL-SCORE BUCKET")


# ---------------------------------------------------------------------------
# section 5 — by ticker
# ---------------------------------------------------------------------------

def _section_ticker(df: pd.DataFrame) -> None:
    _group_summary(df, "ticker", "5. BY TICKER")


# ---------------------------------------------------------------------------
# section 6 — by contract spread quartile
# ---------------------------------------------------------------------------

def _section_spread(df: pd.DataFrame) -> None:
    _hdr("6. BY CONTRACT SPREAD QUARTILE")

    if "contract_spread_pct" not in df.columns or df["contract_spread_pct"].isna().all():
        print("  No contract_spread_pct data.")
        return

    df = df.copy()
    try:
        df["spread_quartile"] = pd.qcut(
            df["contract_spread_pct"], q=4, labels=["Q1(tight)", "Q2", "Q3", "Q4(wide)"], duplicates="drop"
        )
    except ValueError:
        print("  Not enough distinct spread values for quartile split.")
        return
    _group_summary(df, "spread_quartile", "6. BY CONTRACT SPREAD QUARTILE")


# ---------------------------------------------------------------------------
# section 7 — by entry hour ET
# ---------------------------------------------------------------------------

def _section_hour(df: pd.DataFrame) -> None:
    _hdr("7. BY ENTRY HOUR (market-local ET)")

    rows = _build_hour_rows(df)
    if not rows:
        print("  No entry_hour data.")
        return

    tbl = pd.DataFrame(rows).set_index("hour_et")
    tbl["conservative_win_rate"] = tbl["conservative_win_rate"].apply(_pct)
    tbl["paper_expectancy_usd"] = tbl["paper_expectancy_usd"].apply(_usd)
    tbl["conservative_expectancy_usd"] = tbl["conservative_expectancy_usd"].apply(_usd)
    tbl = tbl.rename(
        columns={
            "conservative_win_rate": "con_win%",
            "paper_expectancy_usd": "paper_exp",
            "conservative_expectancy_usd": "con_exp",
        }
    )
    print(tbl.to_string())


# ---------------------------------------------------------------------------
# section 8 — execution quality
# ---------------------------------------------------------------------------

def _section_execution(df: pd.DataFrame) -> None:
    _hdr("8. EXECUTION QUALITY")

    execution = _build_execution(df)

    def _stat(payload: dict[str, object], label: str, fmt: str = ".3f") -> None:
        if int(payload.get("n", 0) or 0) == 0:
            print(f"  {label:<42}: n/a")
            return
        mean_val = float(payload.get("mean") or 0.0)
        med_val = float(payload.get("median") or 0.0)
        p95_val = float(payload.get("p95") or 0.0)
        n_val = int(payload.get("n", 0) or 0)
        print(
            f"  {label:<42}: mean={mean_val:{fmt}}  med={med_val:{fmt}}  p95={p95_val:{fmt}}  n={n_val}"
        )

    print("\n  --- Entry ---")
    entry = dict(execution.get("entry") or {})
    _stat(dict(entry.get("fill_seconds") or {}), "entry_fill_seconds")
    _stat(dict(entry.get("attempts") or {}), "entry_attempts", ".1f")
    _stat(dict(entry.get("slippage_vs_ask_pct") or {}), "entry_slippage_vs_ask_pct")
    _stat(dict(entry.get("spread_pct") or {}), "entry_spread_pct")

    print("\n  --- Exit ---")
    exit_payload = dict(execution.get("exit") or {})
    _stat(dict(exit_payload.get("fill_seconds") or {}), "exit_fill_seconds")
    _stat(dict(exit_payload.get("attempts") or {}), "exit_attempts", ".1f")
    _stat(dict(exit_payload.get("slippage_vs_bid_pct") or {}), "exit_slippage_vs_bid_pct")
    _stat(dict(exit_payload.get("spread_pct") or {}), "exit_spread_pct")

    entry_retry_rate = _safe_float(execution.get("entry_retry_rate"))
    if entry_retry_rate is not None:
        retry_n = int(round(entry_retry_rate * len(df)))
        print(f"\n  Entry retry rate (>1 attempt)   : {entry_retry_rate * 100:.1f}%  ({retry_n}/{len(df)})")

    exit_retry_rate = _safe_float(execution.get("exit_retry_rate"))
    if exit_retry_rate is not None:
        retry_n = int(round(exit_retry_rate * len(df)))
        print(f"  Exit retry rate (>1 attempt)    : {exit_retry_rate * 100:.1f}%  ({retry_n}/{len(df)})")

    by_reason = list(execution.get("exit_fill_seconds_by_reason") or [])
    if by_reason:
        print("\n  Exit fill seconds by exit_reason:")
        tbl = pd.DataFrame(by_reason).set_index("exit_reason")
        if not tbl.empty:
            tbl["avg_sec"] = pd.to_numeric(tbl["avg_sec"], errors="coerce").round(1)
            tbl["med_sec"] = pd.to_numeric(tbl["med_sec"], errors="coerce").round(1)
        print(tbl.to_string())


def _section_joint_tradeability(df: pd.DataFrame) -> None:
    _hdr("9. JOINT TRADEABILITY INTERACTIONS")
    joint = _build_joint_tradeability(df)

    blocks = [
        ("ticker x hour (worst conservative expectancy first)", joint.get("ticker_x_hour", []), ["ticker", "entry_hour"]),
        ("spread quartile x signal-score bucket", joint.get("spread_quartile_x_score_bucket", []), ["spread_quartile", "score_bucket"]),
        ("stop-loss exits x ticker", joint.get("stop_loss_x_ticker", []), ["ticker"]),
        ("stop-loss exits x hour", joint.get("stop_loss_x_hour", []), ["entry_hour"]),
        ("ticker x spread regime (MAE-sensitive)", joint.get("mae_x_spread_x_ticker", []), ["ticker", "spread_quartile"]),
    ]

    for label, rows, index_cols in blocks:
        print(f"\n  {label}:")
        tbl = _render_composite_table(list(rows or []), index_cols)
        if tbl.empty:
            print("    n/a (insufficient sample)")
        else:
            print(tbl.to_string())


def _section_stop_loss_geometry(df: pd.DataFrame) -> None:
    _hdr("10. STOP-LOSS GEOMETRY DIAGNOSTICS")
    stop = _build_stop_loss_geometry(df)

    print(f"  Stop-loss exits                 : {int(stop.get('stop_loss_trade_count', 0) or 0)}")
    stop_rate = _safe_float(stop.get("stop_loss_rate"))
    if stop_rate is None:
        print("  Stop-loss rate                  : n/a")
    else:
        print(f"  Stop-loss rate                  : {stop_rate * 100:.1f}%")

    cfg_stop = _safe_float(stop.get("configured_stop_loss_usd"))
    if cfg_stop is not None:
        print(f"  Configured STOP_LOSS_USD        : ${cfg_stop:.2f}")

    mae_pct = stop.get("mae_pct_at_stop_loss") or {}
    est_mae = stop.get("estimated_mae_usd_1c") or {}
    print("\n  MAE percent at stop-loss exits:")
    print(f"    mean={_pct((float(mae_pct.get('mean')) / 100.0)) if _safe_float(mae_pct.get('mean')) is not None else 'n/a'}")
    print(f"    med ={_pct((float(mae_pct.get('median')) / 100.0)) if _safe_float(mae_pct.get('median')) is not None else 'n/a'}")
    print(f"    p90 ={_pct((float(mae_pct.get('p90')) / 100.0)) if _safe_float(mae_pct.get('p90')) is not None else 'n/a'}")

    print("\n  Estimated MAE in USD at stop-loss exits:")
    print(f"    mean={_usd(float(est_mae.get('mean'))) if _safe_float(est_mae.get('mean')) is not None else 'n/a'}")
    print(f"    med ={_usd(float(est_mae.get('median'))) if _safe_float(est_mae.get('median')) is not None else 'n/a'}")
    print(f"    p90 ={_usd(float(est_mae.get('p90'))) if _safe_float(est_mae.get('p90')) is not None else 'n/a'}")

    print(f"\n  Note: {stop.get('comment', '')}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trade analytics and execution quality review")
    parser.add_argument("--csv", dest="csv_path", default=None, help="Override trades CSV path")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Render report as terminal text or structured JSON",
    )
    parser.add_argument("--output", default=None, help="Write JSON output to this file instead of stdout")
    parser.add_argument(
        "--export-csv-dir",
        default=None,
        help="Optional directory for machine-readable CSV breakdown exports",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main(csv_path: Optional[Path] = None, *, output_format: str = "text", output_path: Optional[Path] = None, export_csv_dir: Optional[Path] = None) -> None:
    path = csv_path or config.TRADES_CSV_PATH
    df = _load(path)
    if df is None:
        return

    report = _build_report(df, path)

    if output_format == "json":
        _write_json_report(report, output_path)
        if export_csv_dir is not None:
            _write_csv_exports(report, export_csv_dir)
        return

    if df.empty:
        print(f"\nTrade Analytics  —  0 closed trades  —  {path}")
        if export_csv_dir is not None:
            _write_csv_exports(report, export_csv_dir)
        print("\ntrades.csv exists but contains no closed trades yet.")
        print(f"\n{_SEP}")
        print("  End of report")
        print(_SEP)
        return

    print(f"\nTrade Analytics  —  {len(df)} closed trades  —  {path}")

    _section_overall(df)
    _section_gap(df)
    _section_exit_reason(df)
    _section_signal_score(df)
    _section_ticker(df)
    _section_spread(df)
    _section_hour(df)
    _section_execution(df)
    _section_joint_tradeability(df)
    _section_stop_loss_geometry(df)

    if export_csv_dir is not None:
        _write_csv_exports(report, export_csv_dir)

    print(f"\n{_SEP}")
    print("  End of report")
    print(_SEP)


if __name__ == "__main__":
    args = _parse_args()
    csv_path = Path(args.csv_path) if args.csv_path else None
    output_path = Path(args.output) if args.output else None
    export_csv_dir = Path(args.export_csv_dir) if args.export_csv_dir else None
    main(csv_path, output_format=args.format, output_path=output_path, export_csv_dir=export_csv_dir)
