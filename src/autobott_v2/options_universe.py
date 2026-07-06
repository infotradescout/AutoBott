from __future__ import annotations

TOP_OPTIONS_UNDERLYINGS_100 = [
    "SPY",
    "QQQ",
    "IWM",
    "DIA",
    "TLT",
    "HYG",
    "XLF",
    "XLE",
    "XLK",
    "SMH",
    "ARKK",
    "TSLA",
    "NVDA",
    "AAPL",
    "AMD",
    "AMZN",
    "MSFT",
    "META",
    "GOOGL",
    "GOOG",
    "NFLX",
    "AVGO",
    "MU",
    "INTC",
    "ORCL",
    "CRM",
    "ADBE",
    "SHOP",
    "SQ",
    "PYPL",
    "COIN",
    "MSTR",
    "PLTR",
    "AI",
    "SNOW",
    "UBER",
    "ABNB",
    "RBLX",
    "ROKU",
    "SOFI",
    "AFRM",
    "UPST",
    "RIVN",
    "LCID",
    "F",
    "GM",
    "NIO",
    "XPEV",
    "LI",
    "BABA",
    "JD",
    "PDD",
    "BIDU",
    "JPM",
    "BAC",
    "WFC",
    "C",
    "GS",
    "MS",
    "V",
    "MA",
    "AXP",
    "XOM",
    "CVX",
    "OXY",
    "SLB",
    "HAL",
    "CCL",
    "DAL",
    "AAL",
    "UAL",
    "BA",
    "LUV",
    "DIS",
    "PARA",
    "WBD",
    "T",
    "VZ",
    "WMT",
    "TGT",
    "COST",
    "NKE",
    "SBUX",
    "MCD",
    "KO",
    "PEP",
    "PFE",
    "MRNA",
    "LLY",
    "UNH",
    "JNJ",
    "ABBV",
    "CVS",
    "GILD",
    "MRK",
    "CAT",
    "DE",
    "GE",
    "RTX",
    "LMT",
]


def resolve_symbol_universe(symbols: list[str]) -> list[str]:
    resolved: list[str] = []
    for symbol in symbols:
        upper = symbol.strip().upper()
        if not upper:
            continue
        if upper in {"TOP_OPTIONS_100", "TOP100", "OPTIONS_TOP_100"}:
            resolved.extend(TOP_OPTIONS_UNDERLYINGS_100)
        else:
            resolved.append(upper)
    return _dedupe(resolved)


def _dedupe(symbols: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for symbol in symbols:
        if symbol in seen:
            continue
        seen.add(symbol)
        result.append(symbol)
    return result
