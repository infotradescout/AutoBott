"""Trade analytics and execution quality review for trades.csv.

Run directly:  python -m autotrader.review
            or python autotrader/review.py

Sections produced:
  1. Overall summary (paper PnL vs conservative PnL, expectancy, win-rate)
  2. Paper vs. Conservative PnL gap distribution
  3. By exit reason
  4. By signal-score bucket
  5. By ticker
  6. By contract spread quartile
  7. By entry hour (ET)
  8. Execution quality (entry/exit fill latency, slippage, retry rates)
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

# Allow running as a standalone script from the repo root or inside autotrader/
_here = Path(__file__).parent
if str(_here) not in sys.path:
    sys.path.insert(0, str(_here))

import config  # noqa: E402


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

_SEP = "=" * 72
_SUBSEP = "-" * 72


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


def _load(path: Path) -> Optional[pd.DataFrame]:
    if not path.exists():
        print(f"No trades file found at {path}")
        return None
    df = pd.read_csv(path)
    if df.empty:
        print("trades.csv exists but contains no rows.")
        return None

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


def _group_summary(df: pd.DataFrame, group_col: str, title: str) -> None:
    _hdr(title)
    if group_col not in df.columns:
        print(f"  Column '{group_col}' not in data — skipping.")
        return
    if df[group_col].isna().all():
        print(f"  No data for '{group_col}' — skipping.")
        return

    rows = []
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
                group_col: name,
                "n": n,
                "paper_wr%": _pct(paper_wr),
                "con_wr%": _pct(con_wr),
                "paper_exp": _usd(paper_exp),
                "con_exp": _usd(con_exp),
                "gap(p-c)": _usd(gap),
            }
        )

    summary = pd.DataFrame(rows).set_index(group_col)
    # sort by conservative expectancy ascending (worst first)
    summary["_sort_key"] = pd.to_numeric(
        summary["con_exp"].str.replace(r"[$+]", "", regex=True), errors="coerce"
    )
    summary = summary.sort_values("_sort_key").drop(columns=["_sort_key"])
    print(summary.to_string())


# ---------------------------------------------------------------------------
# section 1 — overall summary
# ---------------------------------------------------------------------------

def _section_overall(df: pd.DataFrame) -> None:
    _hdr("1. OVERALL SUMMARY")

    n = len(df)
    paper_wr = _win_rate(df["paper_reported_pnl_usd"])
    con_wr = _win_rate(df["conservative_executable_pnl_usd"])
    paper_exp = _expectancy(df["paper_reported_pnl_usd"])
    con_exp = _expectancy(df["conservative_executable_pnl_usd"])
    realized_exp = _expectancy(df.get("realized_pnl_usd", pd.Series(dtype=float)))

    hold_med = df["hold_seconds"].median() if "hold_seconds" in df.columns else float("nan")
    hold_avg = df["hold_seconds"].mean() if "hold_seconds" in df.columns else float("nan")

    print(f"  Total closed trades      : {n}")
    print(f"  Paper win rate           : {_pct(paper_wr)}")
    print(f"  Conservative win rate    : {_pct(con_wr)}")
    print(f"  Paper expectancy / trade : {_usd(paper_exp)}")
    print(f"  Conservative expectancy  : {_usd(con_exp)}")
    print(f"  Realized fill expectancy : {_usd(realized_exp)}")
    print(
        f"  Paper overstatement avg  : {_usd(paper_exp - con_exp if not pd.isna(paper_exp) and not pd.isna(con_exp) else float('nan'))}"
    )
    print(f"  Avg hold (sec)           : {hold_avg:.0f}" if not pd.isna(hold_avg) else "  Avg hold (sec) : n/a")
    print(f"  Median hold (sec)        : {hold_med:.0f}" if not pd.isna(hold_med) else "  Median hold (sec) : n/a")

    # separate wins/losses by conservative PnL
    con_valid = df["conservative_executable_pnl_usd"].dropna()
    con_wins = con_valid[con_valid > 0]
    con_losses = con_valid[con_valid <= 0]
    avg_con_win = con_wins.mean() if not con_wins.empty else float("nan")
    avg_con_loss = con_losses.mean() if not con_losses.empty else float("nan")
    print(f"  Avg conservative win     : {_usd(avg_con_win)}")
    print(f"  Avg conservative loss    : {_usd(avg_con_loss)}")

    mfe_avg = df["max_favorable_excursion_pct"].mean() if "max_favorable_excursion_pct" in df.columns else float("nan")
    mae_avg = df["max_adverse_excursion_pct"].mean() if "max_adverse_excursion_pct" in df.columns else float("nan")
    print(f"  Avg MFE                  : {_pct(mfe_avg)}")
    print(f"  Avg MAE                  : {_pct(mae_avg)}")


# ---------------------------------------------------------------------------
# section 2 — paper vs conservative gap
# ---------------------------------------------------------------------------

def _section_gap(df: pd.DataFrame) -> None:
    _hdr("2. PAPER vs. CONSERVATIVE PnL GAP")

    both = df[["paper_reported_pnl_usd", "conservative_executable_pnl_usd"]].dropna()
    if both.empty:
        print("  No rows with both paper and conservative PnL.")
        return

    gap_series = both["paper_reported_pnl_usd"] - both["conservative_executable_pnl_usd"]
    print(f"  Trades with both values  : {len(both)}")
    print(f"  Mean gap (paper - con)   : {_usd(gap_series.mean())}")
    print(f"  Median gap               : {_usd(gap_series.median())}")
    print(f"  Max gap                  : {_usd(gap_series.max())}")
    print(f"  Min gap                  : {_usd(gap_series.min())}")
    print(f"  % where paper > con      : {(gap_series > 0).mean() * 100:.1f}%")
    print(f"  % where paper < con      : {(gap_series < 0).mean() * 100:.1f}%")

    # decile breakdown
    print("\n  Gap deciles (USD, paper minus conservative):")
    deciles = gap_series.quantile([0.1, 0.25, 0.5, 0.75, 0.9])
    for q, val in deciles.items():
        print(f"    p{int(q * 100):>3}: {_usd(val)}")


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

    if "entry_hour" not in df.columns or df["entry_hour"].isna().all():
        print("  No entry_hour data.")
        return

    rows = []
    for hour, g in df.groupby("entry_hour", observed=True):
        n = len(g)
        con_exp = _expectancy(g["conservative_executable_pnl_usd"])
        paper_exp = _expectancy(g["paper_reported_pnl_usd"])
        con_wr = _win_rate(g["conservative_executable_pnl_usd"])
        rows.append(
            {
                "hour_ET": f"{int(hour):02d}:00",
                "n": n,
                "con_win%": _pct(con_wr),
                "paper_exp": _usd(paper_exp),
                "con_exp": _usd(con_exp),
            }
        )

    tbl = pd.DataFrame(rows).set_index("hour_ET")
    # sort by conservative expectancy
    tbl["_sort"] = pd.to_numeric(
        tbl["con_exp"].str.replace(r"[$+]", "", regex=True), errors="coerce"
    )
    tbl = tbl.sort_values("_sort").drop(columns=["_sort"])
    print(tbl.to_string())


# ---------------------------------------------------------------------------
# section 8 — execution quality
# ---------------------------------------------------------------------------

def _section_execution(df: pd.DataFrame) -> None:
    _hdr("8. EXECUTION QUALITY")

    def _stat(series: pd.Series, label: str, fmt: str = ".3f") -> None:
        valid = series.dropna()
        if valid.empty:
            print(f"  {label:<42}: n/a")
            return
        mean_val = valid.mean()
        med_val = valid.median()
        p95_val = valid.quantile(0.95)
        print(
            f"  {label:<42}: mean={mean_val:{fmt}}  med={med_val:{fmt}}  p95={p95_val:{fmt}}  n={len(valid)}"
        )

    print("\n  --- Entry ---")
    _stat(df.get("entry_fill_seconds", pd.Series(dtype=float)), "entry_fill_seconds")
    _stat(df.get("entry_attempts", pd.Series(dtype=float)), "entry_attempts", ".1f")
    _stat(df.get("entry_fill_slippage_vs_ask_pct", pd.Series(dtype=float)), "entry_slippage_vs_ask_pct")
    _stat(df.get("entry_spread_pct", pd.Series(dtype=float)), "entry_spread_pct")

    print("\n  --- Exit ---")
    _stat(df.get("exit_fill_seconds", pd.Series(dtype=float)), "exit_fill_seconds")
    _stat(df.get("exit_attempts", pd.Series(dtype=float)), "exit_attempts", ".1f")
    _stat(df.get("exit_fill_slippage_vs_bid_pct", pd.Series(dtype=float)), "exit_slippage_vs_bid_pct")
    _stat(df.get("exit_spread_pct", pd.Series(dtype=float)), "exit_spread_pct")

    # multi-attempt entry stats
    if "entry_attempts" in df.columns:
        retry = df[df["entry_attempts"].fillna(1) > 1]
        print(f"\n  Entry retry rate (>1 attempt)   : {len(retry) / len(df) * 100:.1f}%  ({len(retry)}/{len(df)})")

    if "exit_attempts" in df.columns:
        retry = df[df["exit_attempts"].fillna(1) > 1]
        print(f"  Exit retry rate (>1 attempt)    : {len(retry) / len(df) * 100:.1f}%  ({len(retry)}/{len(df)})")

    # by exit_reason execution latency breakdown
    if "exit_reason" in df.columns and "exit_fill_seconds" in df.columns:
        print("\n  Exit fill seconds by exit_reason:")
        tbl = (
            df.groupby("exit_reason", observed=True)["exit_fill_seconds"]
            .agg(["count", "mean", "median"])
            .rename(columns={"count": "n", "mean": "avg_sec", "median": "med_sec"})
            .sort_values("avg_sec", ascending=False)
        )
        tbl["avg_sec"] = tbl["avg_sec"].round(1)
        tbl["med_sec"] = tbl["med_sec"].round(1)
        print(tbl.to_string())


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main(csv_path: Optional[Path] = None) -> None:
    path = csv_path or config.TRADES_CSV_PATH
    df = _load(path)
    if df is None:
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

    print(f"\n{_SEP}")
    print("  End of report")
    print(_SEP)


if __name__ == "__main__":
    main()
