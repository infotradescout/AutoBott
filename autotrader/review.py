"""Weekly performance review helper for trades.csv."""

from __future__ import annotations

import pandas as pd

import config


def main():
    path = config.TRADES_CSV_PATH
    if not path.exists():
        print(f"No trades file found at {path}")
        return

    df = pd.read_csv(path)
    if df.empty:
        print("No trades logged yet.")
        return

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["pnl_pct"] = pd.to_numeric(df["pnl_pct"], errors="coerce")
    df = df.dropna(subset=["timestamp", "pnl_pct"])
    if df.empty:
        print("No valid closed trades with pnl_pct found.")
        return

    wins = df[df["pnl_pct"] > 0]
    losses = df[df["pnl_pct"] <= 0]
    win_rate = (len(wins) / len(df)) * 100 if len(df) else 0
    avg_win = wins["pnl_pct"].mean() if not wins.empty else 0
    avg_loss = losses["pnl_pct"].mean() if not losses.empty else 0

    print("Weekly Trade Review")
    print(f"Total closed trades: {len(df)}")
    print(f"Win rate: {win_rate:.1f}%")
    print(f"Average win: {avg_win:.2%}")
    print(f"Average loss: {avg_loss:.2%}")

    df["hour_et"] = df["timestamp"].dt.hour
    by_ticker = df.groupby("ticker")["pnl_pct"].agg(["count", "mean"]).sort_values("mean")
    by_hour = df.groupby("hour_et")["pnl_pct"].agg(["count", "mean"]).sort_values("mean")

    print("\nTickers by average pnl (worst first):")
    print(by_ticker.to_string())
    print("\nHours by average pnl (worst first):")
    print(by_hour.to_string())


if __name__ == "__main__":
    main()
