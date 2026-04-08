"""Live read-only dashboard for the Alpaca options autotrader."""

from __future__ import annotations

import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pytz
import requests
from flask import Flask, jsonify, render_template_string

import config
from env_config import get_required_env, load_runtime_env

load_runtime_env()

API_KEY = get_required_env("ALPACA_API_KEY")
SECRET_KEY = get_required_env("ALPACA_SECRET_KEY")
PAPER = bool(config.PAPER)
BASE_URL = "https://paper-api.alpaca.markets" if PAPER else "https://api.alpaca.markets"
HEADERS = {"APCA-API-KEY-ID": API_KEY or "", "APCA-API-SECRET-KEY": SECRET_KEY or ""}

TRADES_CSV = Path(config.TRADES_CSV_PATH)
SCAN_LOG_CSV = Path(__file__).resolve().parent / "scan_log.csv"
EASTERN = pytz.timezone(config.EASTERN_TZ)

app = Flask(__name__)


def _now_et() -> datetime:
    return datetime.now(EASTERN)


def _extract_underlying(symbol: str) -> str:
    match = re.match(r"^([A-Z.]+)\d{6}[CP]\d{8}$", symbol or "")
    return match.group(1) if match else ""


def _extract_direction(symbol: str) -> str:
    match = re.match(r"^[A-Z.]+\d{6}([CP])\d{8}$", symbol or "")
    if not match:
        return ""
    return "CALL" if match.group(1) == "C" else "PUT"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _read_csv_rows(path: Path, limit: int, reverse: bool = True) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if reverse:
        rows = list(reversed(rows))
    return rows[:limit]


def _parse_ts(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = EASTERN.localize(dt)
            return dt.astimezone(EASTERN)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = EASTERN.localize(dt)
        return dt.astimezone(EASTERN)
    except ValueError:
        return None


def _today_trade_rows() -> list[dict[str, str]]:
    today = _now_et().date()
    rows = _read_csv_rows(TRADES_CSV, limit=5000, reverse=False)
    out: list[dict[str, str]] = []
    for row in rows:
        dt = _parse_ts(row.get("timestamp", ""))
        if dt and dt.date() == today:
            out.append(row)
    return out


def _daily_loss_and_streak(today_rows: list[dict[str, str]]) -> tuple[float, int]:
    daily_loss = 0.0
    for row in today_rows:
        pnl_pct = _safe_float(row.get("pnl_pct"), 0.0)
        if pnl_pct < 0:
            entry = _safe_float(row.get("entry_price"), 0.0)
            qty = int(_safe_float(row.get("qty"), 0.0))
            premium = entry * qty * 100
            daily_loss += abs(premium * pnl_pct)

    streak = 0
    for row in reversed(today_rows):
        pnl_pct = _safe_float(row.get("pnl_pct"), 0.0)
        if pnl_pct < 0:
            streak += 1
        else:
            break
    return daily_loss, streak


def _progress_color(pct: float) -> str:
    if pct < 50:
        return "#00c853"
    if pct <= 80:
        return "#ffb300"
    return "#ff1744"


@app.get("/api/account")
def api_account():
    try:
        resp = requests.get(f"{BASE_URL}/v2/account", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        return jsonify(
            {
                "equity": body.get("equity", "0"),
                "buying_power": body.get("buying_power", "0"),
                "cash": body.get("cash", "0"),
                "portfolio_value": body.get("portfolio_value", body.get("equity", "0")),
                "status": body.get("status", "UNKNOWN"),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/positions")
def api_positions():
    try:
        resp = requests.get(f"{BASE_URL}/v2/positions", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        rows = []
        for pos in resp.json():
            asset_class = str(pos.get("asset_class", "")).lower()
            if asset_class not in ("us_option", "option"):
                continue
            symbol = pos.get("symbol", "")
            rows.append(
                {
                    "symbol": symbol,
                    "underlying": _extract_underlying(symbol),
                    "direction": _extract_direction(symbol),
                    "qty": int(_safe_float(pos.get("qty"), 0)),
                    "entry_price": _safe_float(pos.get("avg_entry_price"), 0.0),
                    "current_price": _safe_float(pos.get("current_price"), 0.0),
                    "market_value": _safe_float(pos.get("market_value"), 0.0),
                    "unrealized_pl": _safe_float(pos.get("unrealized_pl"), 0.0),
                    "unrealized_plpc": round(_safe_float(pos.get("unrealized_plpc"), 0.0) * 100, 2),
                }
            )
        return jsonify(rows)
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/trades")
def api_trades():
    try:
        return jsonify(_read_csv_rows(TRADES_CSV, limit=50, reverse=True))
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/scanlog")
def api_scanlog():
    try:
        rows = _read_csv_rows(SCAN_LOG_CSV, limit=200, reverse=True)
        passed = [r for r in rows if str(r.get("result", "")).lower() == "pass"]
        return jsonify(passed[:30])
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/api/status")
def api_status():
    try:
        clock = requests.get(f"{BASE_URL}/v2/clock", headers=HEADERS, timeout=10)
        clock.raise_for_status()
        clock_body = clock.json()

        today_rows = _today_trade_rows()
        wins = 0
        losses = 0
        total_plpc = 0.0
        for row in today_rows:
            plpc = _safe_float(row.get("pnl_pct"), 0.0)
            total_plpc += plpc
            if plpc > 0:
                wins += 1
            elif plpc < 0:
                losses += 1

        return jsonify(
            {
                "market_open": bool(clock_body.get("is_open", False)),
                "last_updated": _now_et().strftime("%Y-%m-%d %H:%M:%S ET"),
                "trades_today": len(today_rows),
                "wins_today": wins,
                "losses_today": losses,
                "daily_pnl_pct": round(total_plpc, 4),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@app.get("/")
def home():
    return render_template_string(
        """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Alpaca Options Dashboard</title>
  <style>
    :root {
      --bg: #0d1117;
      --card: #161b22;
      --text: #e6edf3;
      --muted: #8b949e;
      --green: #00c853;
      --red: #ff1744;
      --yellow: #ffb300;
      --border: #30363d;
    }
    body { margin: 0; background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 16px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
    .title { font-size: 22px; font-weight: 700; }
    .paper { color: #111; background: var(--yellow); padding: 4px 8px; border-radius: 8px; font-size: 12px; font-weight: 700; }
    .muted { color: var(--muted); }
    .grid4 { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 12px; }
    .label { font-size: 12px; color: var(--muted); margin-bottom: 6px; }
    .num { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 22px; }
    .section { margin-top: 12px; }
    .section h3 { margin: 0 0 8px 0; font-size: 14px; color: var(--muted); letter-spacing: 0.4px; }
    .bar-wrap { margin-bottom: 10px; }
    .bar-line { display:flex; justify-content:space-between; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }
    .bar { height: 10px; background: #0b1016; border: 1px solid var(--border); border-radius: 999px; overflow: hidden; margin-top: 4px; }
    .fill { height: 100%; width: 0%; background: var(--green); transition: width .2s; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; }
    th, td { border-bottom: 1px solid var(--border); padding: 8px 6px; text-align: left; }
    th { color: var(--muted); font-weight: 600; }
    .badge { font-size: 11px; padding: 2px 7px; border-radius: 999px; border: 1px solid var(--border); }
    .b-green { color: var(--green); border-color: rgba(0,200,83,0.4); }
    .b-red { color: var(--red); border-color: rgba(255,23,68,0.4); }
    .b-gray { color: #aaa; border-color: #555; }
    .pnl-pos { color: var(--green); }
    .pnl-neg { color: var(--red); }
    .pnl-zero { color: #888; }
    @media (max-width: 900px) { .grid4 { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div>
        <div class="title">Alpaca Options Autotrader Dashboard</div>
        <div class="muted">Last updated: <span id="last-updated">--</span> | Auto-refresh: 30s</div>
      </div>
      <div class="paper">{{ "PAPER MODE" if paper else "LIVE MODE" }}</div>
    </div>

    <div class="grid4">
      <div class="card"><div class="label">Equity</div><div id="equity" class="num">--</div></div>
      <div class="card"><div class="label">Buying Power</div><div id="buying-power" class="num">--</div></div>
      <div class="card"><div class="label">Today P&L</div><div id="daily-pnl" class="num">--</div></div>
      <div class="card"><div class="label">Market Status</div><div id="market-status" class="num">--</div></div>
    </div>

    <div class="card section">
      <h3>CIRCUIT BREAKERS</h3>
      <div class="bar-wrap">
        <div class="bar-line"><span id="daily-loss-text">Daily Loss: --</span><span id="daily-loss-pct">--</span></div>
        <div class="bar"><div id="daily-loss-bar" class="fill"></div></div>
      </div>
      <div class="bar-wrap">
        <div class="bar-line"><span id="streak-text">Consec. Losses: --</span><span id="streak-pct">--</span></div>
        <div class="bar"><div id="streak-bar" class="fill"></div></div>
      </div>
    </div>

    <div class="card section">
      <h3>OPEN POSITIONS</h3>
      <div id="positions-wrap" class="muted">Loading...</div>
    </div>

    <div class="card section">
      <h3>RECENT TRADES (last 10)</h3>
      <div id="trades-wrap" class="muted">Loading...</div>
    </div>

    <div class="card section">
      <h3>SCANNER - Last Passing Signals</h3>
      <div id="scan-wrap" class="muted">Loading...</div>
    </div>
  </div>

  <script>
    const DAILY_LOSS_LIMIT = {{ daily_loss_limit }};
    const CONSEC_LOSS_LIMIT = {{ consec_limit }};

    function fmtMoney(v) {
      const n = Number(v);
      if (Number.isNaN(n)) return "--";
      return n.toLocaleString(undefined, {style:"currency", currency:"USD", maximumFractionDigits:2});
    }
    function pctClass(v) {
      if (v > 0) return "pnl-pos";
      if (v < 0) return "pnl-neg";
      return "pnl-zero";
    }
    function asPct(v, digits = 2) {
      const n = Number(v);
      if (Number.isNaN(n)) return "--";
      return `${n >= 0 ? "+" : ""}${n.toFixed(digits)}%`;
    }
    function reasonBadge(reason) {
      if (reason === "profit_target") return `<span class="badge b-green">${reason}</span>`;
      if (reason === "stop_loss") return `<span class="badge b-red">${reason}</span>`;
      return `<span class="badge b-gray">${reason || "manual"}</span>`;
    }
    function barColor(p) {
      if (p < 50) return "#00c853";
      if (p <= 80) return "#ffb300";
      return "#ff1744";
    }

    function renderPositions(data) {
      const el = document.getElementById("positions-wrap");
      if (!Array.isArray(data)) { el.textContent = "—"; return; }
      if (data.length === 0) { el.textContent = "No open positions"; return; }
      let rows = data.map(p => `
        <tr>
          <td>${p.underlying || "-"}</td>
          <td><span class="badge ${p.direction === "CALL" ? "b-green" : "b-red"}">${p.direction || "-"}</span></td>
          <td>${p.qty ?? "-"}</td>
          <td>${fmtMoney(p.entry_price)}</td>
          <td>${fmtMoney(p.current_price)}</td>
          <td class="${pctClass(Number(p.unrealized_plpc || 0))}">${asPct(Number(p.unrealized_plpc || 0), 1)}</td>
        </tr>`).join("");
      el.innerHTML = `
        <table>
          <thead><tr><th>Symbol</th><th>Dir</th><th>Qty</th><th>Entry</th><th>Now</th><th>P&L %</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    }

    function renderTrades(data) {
      const el = document.getElementById("trades-wrap");
      if (!Array.isArray(data)) { el.textContent = "—"; return; }
      const slice = data.slice(0, 10);
      if (slice.length === 0) { el.textContent = "No trades yet"; return; }
      const rows = slice.map(t => {
        const pl = Number(t.pnl_pct || 0) * 100;
        return `<tr>
          <td>${t.timestamp || "-"}</td>
          <td>${t.ticker || "-"}</td>
          <td><span class="badge ${(t.direction || "").toLowerCase() === "call" ? "b-green" : "b-red"}">${(t.direction || "-").toUpperCase()}</span></td>
          <td>${fmtMoney(t.entry_price)}</td>
          <td>${fmtMoney(t.exit_price)}</td>
          <td class="${pctClass(pl)}">${asPct(pl, 2)}</td>
          <td>${reasonBadge(t.exit_reason || "")}</td>
        </tr>`;
      }).join("");
      el.innerHTML = `<table><thead><tr><th>Time</th><th>Ticker</th><th>Dir</th><th>Entry</th><th>Exit</th><th>P&L %</th><th>Reason</th></tr></thead><tbody>${rows}</tbody></table>`;
    }

    function renderScan(data) {
      const el = document.getElementById("scan-wrap");
      if (!Array.isArray(data)) { el.textContent = "—"; return; }
      const slice = data.slice(0, 10);
      if (slice.length === 0) { el.textContent = "No passing scan signals yet"; return; }
      const rows = slice.map(s => `
        <tr>
          <td>${s.timestamp || "-"}</td>
          <td>${s.symbol || "-"}</td>
          <td><span class="badge ${String(s.direction || "").toLowerCase() === "call" ? "b-green" : "b-red"}">${String(s.direction || "-").toUpperCase()}</span></td>
          <td>${s.rvol || "-"}</td>
          <td>${s.rsi || "-"}</td>
          <td>${s.iv_rank || "-"}</td>
          <td>${s.reason || "-"}</td>
        </tr>`).join("");
      el.innerHTML = `<table><thead><tr><th>Time</th><th>Symbol</th><th>Dir</th><th>RVOL</th><th>RSI</th><th>IVR %</th><th>Reason</th></tr></thead><tbody>${rows}</tbody></table>`;
    }

    function computeCircuitBreakers(trades) {
      const today = new Date().toLocaleDateString("en-CA");
      const todayRows = (Array.isArray(trades) ? trades : []).filter(t => String(t.timestamp || "").startsWith(today));
      let dailyLoss = 0;
      for (const t of todayRows) {
        const pnlPct = Number(t.pnl_pct || 0);
        if (pnlPct < 0) {
          const entry = Number(t.entry_price || 0);
          const qty = Number(t.qty || 0);
          const premium = entry * qty * 100;
          dailyLoss += Math.abs(premium * pnlPct);
        }
      }
      let streak = 0;
      for (let i = 0; i < todayRows.length; i++) {
        const pnlPct = Number(todayRows[i].pnl_pct || 0);
        if (pnlPct < 0) streak += 1; else break;
      }
      return { dailyLoss, streak };
    }

    async function fetchJson(url) {
      try {
        const res = await fetch(url);
        const data = await res.json();
        if (!res.ok) return { error: data.error || "request failed" };
        return data;
      } catch {
        return { error: "request failed" };
      }
    }

    async function refresh() {
      const [account, positions, trades, scanlog, status] = await Promise.all([
        fetchJson("/api/account"),
        fetchJson("/api/positions"),
        fetchJson("/api/trades"),
        fetchJson("/api/scanlog"),
        fetchJson("/api/status"),
      ]);

      document.getElementById("last-updated").textContent = new Date().toLocaleTimeString();
      document.getElementById("equity").textContent = account.error ? "—" : fmtMoney(account.equity);
      document.getElementById("buying-power").textContent = account.error ? "—" : fmtMoney(account.buying_power);

      const dailyPct = status.error ? 0 : Number(status.daily_pnl_pct || 0) * 100;
      const pnlEl = document.getElementById("daily-pnl");
      pnlEl.className = `num ${pctClass(dailyPct)}`;
      pnlEl.textContent = status.error ? "—" : asPct(dailyPct, 2);

      const market = status.error ? "—" : (status.market_open ? "OPEN ●" : "CLOSED ○");
      document.getElementById("market-status").textContent = market;
      document.getElementById("market-status").className = `num ${status.error ? "" : (status.market_open ? "pnl-pos" : "pnl-neg")}`;

      renderPositions(positions.error ? [] : positions);
      renderTrades(trades.error ? [] : trades);
      renderScan(scanlog.error ? [] : scanlog);

      const cb = computeCircuitBreakers(trades.error ? [] : trades);
      const lossPct = DAILY_LOSS_LIMIT > 0 ? Math.min(100, (cb.dailyLoss / DAILY_LOSS_LIMIT) * 100) : 0;
      const streakPct = CONSEC_LOSS_LIMIT > 0 ? Math.min(100, (cb.streak / CONSEC_LOSS_LIMIT) * 100) : 0;

      document.getElementById("daily-loss-text").textContent = `Daily Loss: ${fmtMoney(cb.dailyLoss)} / ${fmtMoney(DAILY_LOSS_LIMIT)}`;
      document.getElementById("daily-loss-pct").textContent = `${lossPct.toFixed(0)}%`;
      const dbar = document.getElementById("daily-loss-bar");
      dbar.style.width = `${lossPct}%`;
      dbar.style.background = barColor(lossPct);

      document.getElementById("streak-text").textContent = `Consec. Losses: ${cb.streak} / ${CONSEC_LOSS_LIMIT}`;
      document.getElementById("streak-pct").textContent = `${streakPct.toFixed(0)}%`;
      const sbar = document.getElementById("streak-bar");
      sbar.style.width = `${streakPct}%`;
      sbar.style.background = barColor(streakPct);
    }

    refresh();
    setInterval(refresh, 30000);
  </script>
</body>
</html>
        """,
        paper=PAPER,
        daily_loss_limit=float(config.DAILY_LOSS_LIMIT_USD),
        consec_limit=int(config.CONSECUTIVE_LOSS_LIMIT),
    )


if __name__ == "__main__":
    print("Dashboard running at http://localhost:5000")
    app.run(host="0.0.0.0", port=5000, debug=False)
