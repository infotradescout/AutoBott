# Codex Build Prompt — Intraday Scanner Module (Add-on to Autotrader)

## Purpose
Add a `scanner.py` module to the existing autotrader project.
The scanner finds tickers worth trading options on by detecting conditions
that cause real intraday price movement — which is what makes options profitable.
It runs in two phases:
1. **Morning scan** at 9:35 AM ET — builds today's watchlist
2. **Intraday re-scan** every loop — confirms signals are still valid before entering

---

## Watchlist Source (Both Fixed + Dynamic)

### Fixed Core List (config.py addition)
```python
CORE_TICKERS = ["SPY", "QQQ", "AAPL", "TSLA", "NVDA", "MSFT", "AMZN"]
```

### Dynamic — Top Movers (Morning Scan)
At 9:35 AM ET, fetch today's biggest movers using Alpaca's market data API:
- Endpoint: `GET https://data.alpaca.markets/v1beta1/screener/stocks/movers`
- Parameters: `top=20`, `market_type=stocks`
- Returns top gainers and top losers by % change

Combine: `SCAN_UNIVERSE = deduplicate(CORE_TICKERS + top_gainers[:10] + top_losers[:10])`

Filter out any ticker where:
- Share price < $10 (cheap stocks = bad options liquidity)
- Share price > $800 (options too expensive per contract)
- No options available (`options_enabled` attribute check via Assets API)

---

## Scanner Criteria — What Makes Options Worth Buying

For a ticker to pass the scan, it must meet ALL of the following:

### 1. Relative Volume (RVOL) ≥ 1.5x
**Most important signal.** High volume = something is happening. Low volume = fake move.
- Fetch today's cumulative volume so far (from intraday bars)
- Fetch 20-day average daily volume (from historical bars)
- Scale average to current time of day: `avg_volume_so_far = avg_daily_volume * (minutes_since_open / 390)`
- `RVOL = today_volume / avg_volume_so_far`
- Pass if `RVOL >= 1.5`

### 2. ATR Filter — Stock Must Move Enough
Options need the underlying to move to be profitable.
- Calculate 14-day ATR (Average True Range) on daily bars
- ATR as % of price: `atr_pct = ATR / current_price * 100`
- Pass if `atr_pct >= 1.5%` (stock moves at least 1.5% on average)
- This filters out slow, boring stocks where options bleed theta

### 3. Price vs VWAP — Determines Direction
VWAP = Volume Weighted Average Price. The most reliable intraday directional indicator.
- Calculate VWAP from intraday 5-min bars since market open:
  `VWAP = sum(typical_price * volume) / sum(volume)`
  where `typical_price = (high + low + close) / 3`
- Bullish if price > VWAP AND last 3 bars all closed above VWAP
- Bearish if price < VWAP AND last 3 bars all closed below VWAP
- Neutral (skip) if price is within 0.1% of VWAP (too close to call)

### 4. Momentum — Price Rate of Change
Confirms the move has energy behind it.
- Calculate 10-bar Rate of Change (ROC) on 5-min closes:
  `ROC = (close_now - close_10_bars_ago) / close_10_bars_ago * 100`
- Bullish: ROC > +0.3%
- Bearish: ROC < -0.3%
- Must match the VWAP direction

### 5. EMA Trend Alignment
Quick trend confirmation — direction must be clear.
- 9 EMA and 21 EMA on 5-min bars
- Bullish: 9 EMA > 21 EMA, and both trending upward (current > 3 bars ago)
- Bearish: 9 EMA < 21 EMA, and both trending downward
- Must match VWAP direction

### 6. RSI — Avoid Exhausted Moves
Don't buy calls when already overbought, don't buy puts when already oversold.
- RSI 14 on 5-min bars
- For CALL: RSI must be between 50–72 (momentum but not exhausted)
- For PUT: RSI must be between 28–50 (momentum but not exhausted)

---

## Scanner Output

`scan_ticker(symbol) -> dict | None`

Returns `None` if ticker fails any filter.

Returns a dict if it passes:
```python
{
  "symbol": "AAPL",
  "direction": "call",       # or "put"
  "rvol": 2.3,
  "atr_pct": 2.1,
  "rsi": 58.4,
  "roc": 0.52,
  "vwap": 198.45,
  "price": 199.20,
  "reason": "RVOL 2.3x | Above VWAP | EMA bullish | ROC +0.52%"
}
```

---

## scanner.py Structure

```python
def build_watchlist() -> list[str]:
    """Morning scan — combine core tickers + top movers, filter for options eligibility."""
    ...

def calculate_vwap(bars_df) -> float:
    """Calculate VWAP from intraday 5-min bars DataFrame."""
    ...

def calculate_rvol(symbol, today_volume) -> float:
    """Compare today's volume pace vs 20-day average."""
    ...

def calculate_atr(symbol, period=14) -> float:
    """14-day ATR on daily bars."""
    ...

def calculate_rsi(closes, period=14) -> float:
    """RSI on a pandas Series of closes."""
    ...

def calculate_roc(closes, period=10) -> float:
    """Rate of change over N bars."""
    ...

def scan_ticker(symbol, bars_df, daily_bars_df, today_volume) -> dict | None:
    """
    Run all filters on a ticker. Return signal dict or None.
    bars_df: 5-min intraday bars (at least 30 bars)
    daily_bars_df: daily bars (at least 20 days)
    today_volume: cumulative volume so far today
    """
    ...

def run_scan(watchlist: list[str]) -> list[dict]:
    """
    Scan all tickers in watchlist.
    Returns list of passing tickers sorted by RVOL descending.
    """
    ...
```

---

## Integration into main.py

Replace the existing ticker loop with scanner output:

```python
# 9:35 AM — build watchlist once
watchlist = build_watchlist()

# Every loop:
signals = run_scan(watchlist)   # list of dicts, sorted by RVOL

for signal in signals:
    symbol    = signal["symbol"]
    direction = signal["direction"]   # "call" or "put"

    if already_have_position(symbol):
        continue
    if positions_at_max():
        break
    if past_no_new_trades_time():
        break

    # Find ATM option and place order (existing logic)
    option_symbol = find_atm_option(symbol, direction)
    place_entry_order(option_symbol, direction)
    log_trade(signal)
```

---

## Console Output Format

Print a scan summary every loop:
```
[09:45 ET] SCAN RESULTS — 4 of 18 tickers passed
  ✓ NVDA  | CALL | RVOL 3.1x | RSI 61 | ROC +0.8% | Above VWAP
  ✓ TSLA  | PUT  | RVOL 2.4x | RSI 44 | ROC -0.5% | Below VWAP
  ✓ AAPL  | CALL | RVOL 1.9x | RSI 57 | ROC +0.4% | Above VWAP
  ✓ SPY   | PUT  | RVOL 1.6x | RSI 46 | ROC -0.3% | Below VWAP
  ✗ MSFT  | failed: RVOL 0.9x (too low)
  ✗ AMZN  | failed: RSI 74 (overbought for call)
```

---

## Dependencies (no new ones needed)
All calculations use `pandas` and `numpy` — already in requirements.txt.
Watchlist data comes from Alpaca's screener endpoint and existing bar fetching in `data.py`.

---

## Notes for Codex
- VWAP must be recalculated fresh each loop from bars since open — do not cache it
- The movers endpoint may require the paid Alpaca data plan — add a try/except fallback
  that just uses CORE_TICKERS if the endpoint returns a 403
- All scan results should also be logged to `scan_log.csv` with timestamp and reason
- Tickers that pass the scan but don't get traded (max positions hit) should still be logged
