"""
briefing_generator.py - Generate daily briefing via the Anthropic API
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from zoneinfo import ZoneInfo

import anthropic
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-20250514"
ET = ZoneInfo("America/New_York")


# ── Plain-English display names — NO series IDs visible to readers ─────────────

FRED_DISPLAY: Dict[str, str] = {
    "CPI":                  "Consumer Price Index (CPI)",
    "Core_CPI":             "Core CPI (ex-food & energy)",
    "PCE":                  "PCE Price Index",
    "Core_PCE":             "Core PCE (ex-food & energy)",
    "PPI":                  "Producer Price Index (PPI)",
    "Real_GDP_Growth":      "Real GDP Growth (annualized)",
    "Real_GDP":             "Real GDP",
    "Unemployment":         "Unemployment Rate",
    "Jobless_Claims":       "Initial Jobless Claims",
    "Continued_Claims":     "Continued Claims",
    "JOLTS":                "Job Openings (JOLTS)",
    "Retail_Sales":         "Retail Sales (ex-food services)",
    "M2":                   "M2 Money Supply",
    "Housing_Starts":       "Housing Starts",
    "Building_Permits":     "Building Permits",
    "Consumer_Sentiment":   "Consumer Sentiment (UMich)",
    "Industrial_Production":"Industrial Production",
    "Capacity_Utilization": "Capacity Utilization",
    "Fed_Funds_Rate":       "Fed Funds Rate",
    "10Y_Yield":            "10-Year Treasury Yield",
    "2Y_Yield":             "2-Year Treasury Yield",
    "Yield_Curve_10Y2Y":    "Yield Curve (10Y minus 2Y)",
    "IG_Credit_Spread":     "Investment Grade Credit Spread",
    "HY_Credit_Spread":     "High Yield Credit Spread",
    "ISM_Manufacturing_PMI":"Manufacturing Employment Index",
    "Existing_Home_Sales":  "Existing Home Sales",
}

# Series IDs kept separately — only used in Data Freshness table (small-print)
FRED_SERIES_ID: Dict[str, str] = {
    "CPI": "CPIAUCSL", "Core_CPI": "CPILFESL", "PCE": "PCEPI",
    "Core_PCE": "PCEPILFE", "PPI": "PPIACO",
    "Real_GDP_Growth": "A191RL1Q225SBEA", "Real_GDP": "GDPC1",
    "Unemployment": "UNRATE", "Jobless_Claims": "ICSA", "Continued_Claims": "CCSA",
    "JOLTS": "JTSJOL", "Retail_Sales": "RSXFS", "M2": "M2SL",
    "Housing_Starts": "HOUST", "Building_Permits": "PERMIT",
    "Consumer_Sentiment": "UMCSENT", "Industrial_Production": "INDPRO",
    "Capacity_Utilization": "TCU", "Fed_Funds_Rate": "FEDFUNDS",
    "10Y_Yield": "DGS10", "2Y_Yield": "DGS2", "Yield_Curve_10Y2Y": "T10Y2Y",
    "IG_Credit_Spread": "BAMLC0A0CM", "HY_Credit_Spread": "BAMLH0A0HYM2",
    "ISM_Manufacturing_PMI": "MANEMP", "Existing_Home_Sales": "EXHOSLUSM495S",
}

MARKET_DISPLAY: Dict[str, str] = {
    "S&P 500":      "S&P 500",
    "NASDAQ":       "NASDAQ Composite",
    "Dow Jones":    "Dow Jones Industrial Average",
    "VIX":          "VIX Volatility Index",
    "DXY":          "US Dollar Index (DXY)",
    "10Y Treasury": "10-Year Treasury Yield",
    "2Y Treasury":  "2-Year Treasury Yield",
    "WTI Oil":      "WTI Crude Oil",
    "Brent Oil":    "Brent Crude Oil",
    "Copper":       "Copper",
    "Gold":         "Gold",
}


# ── Timestamp helpers ──────────────────────────────────────────────────────────

def _now_et() -> datetime:
    return datetime.now(ET)


def _ordinal_suffix(day: int) -> str:
    """Return 'st', 'nd', 'rd', or 'th' for a given day-of-month integer."""
    if 11 <= day <= 13:   # 11th, 12th, 13th are exceptions
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")


def _fmt_date(date_str: str) -> str:
    """Convert 'YYYY-MM-DD' → 'April 13th, 2026'. Passes non-date strings through unchanged."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        day = dt.day
        return dt.strftime(f"%B {day}{_ordinal_suffix(day)}, %Y")
    except Exception:
        return date_str


def _fmt_et(dt: datetime) -> str:
    """Format a datetime as 'April 13th, 2026 at 1:52 PM ET'."""
    day = dt.day
    return dt.strftime(f"%B {day}{_ordinal_suffix(day)}, %Y at %-I:%M %p ET")


FRED_WHAT_IT_MEANS: Dict[str, str] = {
    "CPI":                  "Headline consumer inflation — Fed watches closely",
    "Core_CPI":             "Inflation ex-food & energy — Fed's preferred short-term gauge",
    "PCE":                  "Consumer spending deflator — broader than CPI",
    "Core_PCE":             "Fed's primary inflation target (2% goal)",
    "PPI":                  "Pipeline inflation — leads consumer prices by months",
    "Real_GDP_Growth":      "Economy's growth rate — positive = expansion",
    "Real_GDP":             "Total economic output in 2017 dollars",
    "Unemployment":         "Share of workforce without jobs — below 4% = strong labor",
    "Jobless_Claims":       "Weekly new unemployment filings — leading labor indicator",
    "Continued_Claims":     "People still receiving unemployment — tracks labor slack",
    "JOLTS":                "Unfilled job openings — signals employer demand for workers",
    "Retail_Sales":         "Consumer spending on goods — key demand barometer",
    "M2":                   "Money supply — rapid expansion can signal future inflation",
    "Housing_Starts":       "New home construction — leads building activity by months",
    "Building_Permits":     "Forward-looking housing indicator — permits precede starts",
    "Consumer_Sentiment":   "Consumer confidence — below 70 signals caution on spending",
    "Industrial_Production":"Factory & mine output — measures manufacturing health",
    "Capacity_Utilization": "% of factory capacity in use — above 80% = inflationary pressure",
    "Fed_Funds_Rate":       "Central bank benchmark rate — drives all borrowing costs",
    "10Y_Yield":            "Long-term rate — benchmark for mortgages & corporate bonds",
    "2Y_Yield":             "Short-term rate — closely tracks Fed policy expectations",
    "Yield_Curve_10Y2Y":    "10Y minus 2Y — negative (inverted) = recession signal",
    "IG_Credit_Spread":     "Investment grade risk premium — widening = market stress",
    "HY_Credit_Spread":     "High yield risk premium — widening = credit stress / risk-off",
    "ISM_Manufacturing_PMI":"Manufacturing jobs — proxy for factory sector health",
    "Existing_Home_Sales":  "Resales of homes — reflects housing demand & affordability",
}


def _freshness_note(key: str, date_str: str) -> str:
    """Return formatted date with '(latest available)' note if obs is > 6 weeks old."""
    from datetime import timedelta
    try:
        obs_dt = datetime.strptime(date_str, "%Y-%m-%d")
        cutoff = datetime.now() - timedelta(weeks=6)
        formatted = _fmt_date(date_str)
        if obs_dt < cutoff:
            if key in ("Real_GDP_Growth", "Real_GDP"):
                month = obs_dt.month
                quarter = (month - 1) // 3 + 1
                return f"{formatted} (Q{quarter} {obs_dt.year} — latest available, released quarterly)"
            return f"{formatted} (latest available)"
        return formatted
    except Exception:
        return _fmt_date(date_str)


def _market_date(market_data: Dict[str, Any]) -> str:
    for d in market_data.values():
        if d.get("as_of"):
            return d["as_of"]
    return datetime.now(ET).strftime("%Y-%m-%d")


# ── Traffic light signals ──────────────────────────────────────────────────────

def _mkt_signal(name: str, price: Optional[float], chg: Optional[float]) -> str:
    """Traffic light for market price/index data points."""
    c = chg or 0.0
    v = price or 0.0
    key = name.lower()

    if any(x in key for x in ["s&p", "nasdaq", "dow"]):
        return "🟢" if c > 0.3 else "🔴" if c < -0.3 else "🟡"
    if "vix" in key:
        # Rising VIX = fear = red; falling = green; level also matters
        if v > 30: return "🔴"
        if v > 20 and c > 0: return "🔴"
        if v < 15 or c < -5: return "🟢"
        return "🟡"
    if any(x in key for x in ["wti", "brent", "oil"]):
        # Sharp oil surge = bad for economy
        return "🔴" if c > 3 else "🟢" if c < -3 else "🟡"
    if "copper" in key:
        # Copper up = growth signal
        return "🟢" if c > 1 else "🔴" if c < -1 else "🟡"
    if "gold" in key:
        # Gold strong surge = fear signal, not good
        return "🔴" if c > 2 else "🟡"
    if "dollar" in key or "dxy" in key:
        # Moderate dollar strength = neutral; sharp up = headwind
        return "🔴" if c > 1 else "🟢" if c < -1 else "🟡"
    if "treasury" in key or "yield" in key:
        # Rising yields = tighter conditions = negative
        return "🔴" if c > 0.05 else "🟢" if c < -0.05 else "🟡"
    return "🟡"


def _fred_signal(key: str, value: Optional[float], yoy: Optional[float],
                 chg: Optional[float]) -> str:
    """Traffic light for FRED macro indicators."""
    v = value or 0.0
    y = yoy or 0.0
    c = chg or 0.0

    if key in ("CPI", "Core_CPI"):
        return "🟢" if y < 2.0 else "🔴" if y > 3.0 else "🟡"
    if key in ("PCE", "Core_PCE"):
        # Fed target is 2%; above 2.5% = problematic
        return "🟢" if y < 2.0 else "🔴" if y > 2.5 else "🟡"
    if key == "PPI":
        return "🟢" if y < 2.0 else "🔴" if y > 4.0 else "🟡"
    if key == "Real_GDP_Growth":
        return "🟢" if v > 2.5 else "🔴" if v < 0 else "🟡"
    if key == "Real_GDP":
        return "🟡"   # level not directionally meaningful without change
    if key == "Unemployment":
        return "🟢" if v < 4.0 else "🔴" if v > 5.0 else "🟡"
    if key == "Jobless_Claims":
        # Low claims = good; rising trend = bad
        if v < 200_000: return "🟢"
        if v > 300_000: return "🔴"
        return "🟢" if c < 0 else "🔴" if c > 15_000 else "🟡"
    if key == "Continued_Claims":
        return "🟢" if c < 0 else "🔴" if c > 50_000 else "🟡"
    if key == "JOLTS":
        return "🟢" if v > 8_000 else "🔴" if v < 6_000 else "🟡"
    if key == "Retail_Sales":
        return "🟢" if y > 3 else "🔴" if y < 0 else "🟡"
    if key == "Consumer_Sentiment":
        return "🟢" if v > 80 else "🔴" if v < 60 else "🟡"
    if key == "Industrial_Production":
        return "🟢" if c > 0 else "🔴" if c < -0.5 else "🟡"
    if key == "Capacity_Utilization":
        return "🟢" if v > 80 else "🔴" if v < 74 else "🟡"
    if key == "Fed_Funds_Rate":
        # High rate = restrictive = headwind for equities
        return "🔴" if v > 4.5 else "🟡" if v > 3.0 else "🟢"
    if key in ("10Y_Yield", "2Y_Yield"):
        # Rising yields = headwind; level matters for mortgages/valuations
        return "🔴" if (v > 4.5 and c and c > 0) else "🟡"
    if key == "Yield_Curve_10Y2Y":
        # Positive = healthy; inverted = recession signal
        return "🟢" if v > 0.75 else "🔴" if v < 0 else "🟡"
    if key == "IG_Credit_Spread":
        # Lower = tighter = good; FRED stores as %, multiply *100 for bp
        bp = v * 100
        return "🟢" if bp < 80 else "🔴" if bp > 150 else "🟡"
    if key == "HY_Credit_Spread":
        bp = v * 100
        return "🟢" if bp < 300 else "🔴" if bp > 500 else "🟡"
    if key in ("Housing_Starts", "Building_Permits"):
        return "🟢" if c > 0 else "🔴" if c < -100 else "🟡"
    if key == "Existing_Home_Sales":
        return "🟢" if c > 0 else "🔴" if c < -200_000 else "🟡"
    if key == "M2":
        return "🟡"
    if key == "ISM_Manufacturing_PMI":
        return "🟢" if v > 50 else "🔴" if v < 45 else "🟡"
    return "🟡"


def _eia_signal(name: str, chg: Optional[float]) -> str:
    """Traffic light for EIA energy data."""
    c = chg or 0.0
    if "crude" in name and "production" not in name:
        # Crude stock build = bearish oil → 🟡 (more supply)
        return "🟡" if abs(c) < 2_000 else "🟢" if c > 2_000 else "🔴"
    if "gasoline" in name or "distillate" in name:
        # Product draws = demand strong = 🟢 for refining margins
        return "🟢" if c < 0 else "🔴" if c > 2_000 else "🟡"
    if "production" in name:
        return "🟢" if c >= 0 else "🔴"
    return "🟡"


# ── Value formatting — no raw index values, always human-readable units ────────

def _fmt_fred(key: str, value: Optional[float], yoy: Optional[float],
               chg: Optional[float]) -> str:
    """Return a fully formatted, human-readable value string with units."""
    if value is None:
        return "N/A"
    v = value

    # Inflation — show YoY rate, not index level
    if key in ("CPI", "Core_CPI", "PCE", "Core_PCE", "PPI"):
        if yoy is not None:
            return f"{yoy:+.1f}% YoY"
        return f"{v:.1f} (index)"

    if key == "Real_GDP_Growth":
        return f"{v:.1f}% annualized"
    if key == "Real_GDP":
        return f"${v/1_000:.1f} trillion"

    if key == "Unemployment":
        return f"{v:.1f}%"
    if key == "Jobless_Claims":
        return f"{v/1_000:.0f}K claims"
    if key == "Continued_Claims":
        return f"{v/1_000_000:.2f}M claims"
    if key == "JOLTS":
        return f"{v/1_000:.2f}M openings"

    # Retail sales: RSXFS is in millions of dollars
    if key == "Retail_Sales":
        if yoy is not None:
            return f"${v/1_000:.1f}B ({yoy:+.1f}% YoY)"
        return f"${v/1_000:.1f}B"

    if key == "M2":
        return f"${v/1_000:.1f} trillion"
    if key == "Housing_Starts":
        return f"{v/1_000:.2f}M units (ann.)"
    if key == "Building_Permits":
        return f"{v/1_000:.2f}M units (ann.)"
    if key == "Consumer_Sentiment":
        if v < 60:
            sentiment_label = "weak"
        elif v < 80:
            sentiment_label = "moderate"
        else:
            sentiment_label = "strong"
        return f"{v:.1f} ({sentiment_label} — scale of 0–100)"
    if key == "Industrial_Production":
        if chg is not None:
            direction = "expanding" if chg > 0 else "contracting"
            return f"{chg:+.2f}% MoM ({direction})"
        return f"index {v:.1f}"
    if key == "Capacity_Utilization":
        threshold_note = "below 80% healthy threshold" if v < 80 else "above 80% healthy threshold"
        return f"{v:.1f}% ({threshold_note})"
    if key == "Fed_Funds_Rate":
        return f"{v:.2f}%"
    if key in ("10Y_Yield", "2Y_Yield"):
        return f"{v:.2f}%"
    if key == "Yield_Curve_10Y2Y":
        bp = v * 100
        return f"{bp:+.0f} bp"
    if key == "IG_Credit_Spread":
        bp = v * 100
        return f"{bp:.0f} bp"
    if key == "HY_Credit_Spread":
        bp = v * 100
        return f"{bp:.0f} bp"
    if key == "Existing_Home_Sales":
        return f"{v/1_000_000:.2f}M units"
    if key == "ISM_Manufacturing_PMI":
        return f"{v:.1f}"

    # Fallback — 1 decimal, no scientific notation
    return f"{v:.1f}"


def _fmt_mkt_price(name: str, price: Optional[float]) -> str:
    """Format a market price with appropriate units."""
    if price is None:
        return "N/A"
    key = name.lower()
    if any(x in key for x in ["oil", "wti", "brent"]):
        return f"${price:.1f}/barrel"
    if "gold" in key:
        return f"${price:,.0f}/oz"
    if "copper" in key:
        return f"${price:.2f}/lb"
    if "vix" in key:
        return f"{price:.1f}"
    if "yield" in key or "treasury" in key:
        return f"{price:.2f}%"
    if "dollar" in key or "dxy" in key:
        return f"{price:.1f}"
    # Equity indices — no decimal needed at these levels
    if any(x in key for x in ["s&p", "nasdaq", "dow"]):
        return f"{price:,.0f}"
    return f"{price:.2f}"


# ── Data summary builders ──────────────────────────────────────────────────────

def _summarize_market(market_data: Dict[str, Any]) -> str:
    lines = []
    for raw_name, d in market_data.items():
        display = MARKET_DISPLAY.get(raw_name, raw_name)
        price = d.get("price")
        chg = d.get("change_pct")
        as_of = _fmt_date(d.get("as_of", ""))
        sig = _mkt_signal(display, price, chg)
        price_str = _fmt_mkt_price(display, price)
        chg_str = f"({chg:+.1f}%)" if chg is not None else ""
        lines.append(
            f"{sig} **{display}**: {price_str} {chg_str} "
            f"_(yfinance · {as_of} close)_"
        )
    return "\n".join(lines)


def _summarize_sector_table(sector_data: Dict[str, Any]) -> str:
    lines = [
        "| Signal | Sector | Daily | Weekly | MTD | YTD | YTD Rank | Above 50-Day MA | Above 200-Day MA |",
        "|--------|--------|-------|--------|-----|-----|----------|-----------------|------------------|",
    ]
    sorted_sectors = sorted(
        [(s, d) for s, d in sector_data.items() if s != "SPY"],
        key=lambda x: x[1].get("ytd_rank", 99),
    )
    for sector, d in sorted_sectors:
        ytd = d.get("ytd") or 0.0
        sig = "🟢" if ytd > 2 else "🔴" if ytd < -2 else "🟡"
        ma50 = "✓" if d.get("above_50ma") else ("✗" if d.get("above_50ma") is False else "—")
        ma200 = "✓" if d.get("above_200ma") else ("✗" if d.get("above_200ma") is False else "—")
        lines.append(
            f"| {sig} | {sector} | {d.get('daily','—')}% | {d.get('weekly','—')}% | "
            f"{d.get('mtd','—')}% | {d.get('ytd','—')}% | "
            f"#{d.get('ytd_rank','—')} | {ma50} | {ma200} |"
        )
    if "SPY" in sector_data:
        spy = sector_data["SPY"]
        ytd = spy.get("ytd") or 0.0
        sig = "🟢" if ytd > 2 else "🔴" if ytd < -2 else "🟡"
        ma50 = "✓" if spy.get("above_50ma") else ("✗" if spy.get("above_50ma") is False else "—")
        ma200 = "✓" if spy.get("above_200ma") else ("✗" if spy.get("above_200ma") is False else "—")
        lines.append(
            f"| {sig} | **SPY (Benchmark)** | {spy.get('daily','—')}% | {spy.get('weekly','—')}% | "
            f"{spy.get('mtd','—')}% | {spy.get('ytd','—')}% | — | {ma50} | {ma200} |"
        )
    return "\n".join(lines)


def _summarize_fred(fred_data: Dict[str, Any]) -> str:
    ORDERED = [
        "CPI", "Core_CPI", "PCE", "Core_PCE", "PPI",
        "Real_GDP_Growth", "Unemployment", "Jobless_Claims", "Continued_Claims",
        "JOLTS", "Retail_Sales", "Consumer_Sentiment",
        "Industrial_Production", "Capacity_Utilization",
        "Fed_Funds_Rate", "10Y_Yield", "2Y_Yield", "Yield_Curve_10Y2Y",
        "IG_Credit_Spread", "HY_Credit_Spread",
        "Housing_Starts", "Building_Permits", "Existing_Home_Sales",
        "M2", "ISM_Manufacturing_PMI",
    ]
    lines = []
    for key in ORDERED:
        if key not in fred_data:
            continue
        d = fred_data[key]
        val = d.get("value")
        obs = _fmt_date(d.get("date", ""))
        chg = d.get("change")
        yoy = d.get("yoy_pct")
        display = FRED_DISPLAY.get(key, key)
        sig = _fred_signal(key, val, yoy, chg)
        fmt_val = _fmt_fred(key, val, yoy, chg)
        lines.append(f"{sig} **{display}**: {fmt_val} _(FRED · obs. {obs})_")
    return "\n".join(lines)


def _summarize_eia(eia_data: Dict[str, Any]) -> str:
    if not eia_data:
        return "EIA data unavailable."
    # EIA API returns petroleum data in thousand barrels (or Mbbl/day for production)
    # Divide by 1_000 to convert to millions of barrels / million bbl/day
    EIA_LABELS = {
        "crude_oil_stocks":       ("US Crude Oil Stocks",       "M barrels", 1_000),
        "total_petroleum_stocks": ("Total Petroleum Stocks",    "M barrels", 1_000),
        "crude_production":       ("US Crude Oil Production",   "M barrels per day", 1_000),
        "gasoline_stocks":        ("Gasoline Stocks",           "M barrels", 1_000),
        "distillate_stocks":      ("Distillate Fuel Stocks",    "M barrels", 1_000),
        "natgas_storage":         ("Natural Gas Storage",       "Bcf",       1),
    }
    lines = []
    for name, d in eia_data.items():
        val = d.get("value")
        period = _fmt_date(d.get("period", ""))
        chg = d.get("change")
        label, unit, divisor = EIA_LABELS.get(name, (name, "", 1))
        sig = _eia_signal(name, chg)
        if val is not None:
            val_fmt = f"{val/divisor:.1f} {unit}"
        else:
            val_fmt = "N/A"
        chg_str = ""
        if chg is not None:
            chg_unit = unit
            chg_str = f" (WoW: {chg/divisor:+.1f} {chg_unit})"
        lines.append(f"{sig} **{label}**: {val_fmt}{chg_str} _(EIA · week of {period})_")
    return "\n".join(lines)


def _summarize_rotation(rotation: Dict[str, Any]) -> str:
    if not rotation:
        return ""
    sig = rotation.get("rotation_signal", "")
    emoji = "🟢" if "Cyclical" in sig and "ON" in sig else "🔴" if "Defensive" in sig else "🟡"
    return (
        f"{emoji} **Rotation Signal: {sig}** | "
        f"Cyclicals avg YTD: {rotation.get('cyclical_avg_ytd')}% | "
        f"Defensives avg YTD: {rotation.get('defensive_avg_ytd')}% | "
        f"Spread: {rotation.get('cyclical_vs_defensive_spread')}%\n"
        f"{rotation.get('explanation','')}\n"
        f"**Leaders (YTD):** {', '.join(rotation.get('leaders',[]))}\n"
        f"**Laggards (YTD):** {', '.join(rotation.get('laggards',[]))}"
    )


def _summarize_calendar(events: List[Dict]) -> str:
    if not events:
        return "No major events found."
    lines = []
    for e in events[:15]:
        fc = e.get("forecast", "")
        prev = e.get("previous", "")
        imp = e.get("importance", "")
        fc_str = f" | Forecast: {fc}" if fc else ""
        prev_str = f" | Prior: {prev}" if prev else ""
        lines.append(
            f"- **{_fmt_date(e.get('date',''))}** — {e.get('event','')} {imp}{fc_str}{prev_str} "
            f"_(Investing.com)_"
        )
    return "\n".join(lines)


def _summarize_headlines(news: List[Dict], nyt: List[Dict]) -> str:
    lines = []
    for h in news[:8]:
        title = h.get("title", "")
        source = h.get("source", "")
        desc = h.get("description", "")
        pub = _fmt_date(h.get("published_at", "")[:10])
        if title:
            lines.append(f"- **{source}** ({pub}): {title} _(NewsAPI)_")
            if desc:
                lines.append(f"  _{desc[:180]}_")
    for h in nyt[:6]:
        title = h.get("title", "")
        abstract = h.get("abstract", "")
        pub = _fmt_date(h.get("published_at", "")[:10])
        if title and not any(title in ln for ln in lines):
            lines.append(f"- **NYT** ({pub}): {title} _(NYT API)_")
            if abstract:
                lines.append(f"  _{abstract[:180]}_")
    return "\n".join(lines) if lines else "No headlines available."


# ── Programmatic blocks ────────────────────────────────────────────────────────

def _build_meta_block(data: Dict[str, Any], generated_at: datetime) -> str:
    market_date = _market_date(data.get("market_data", {}))
    fred_data = data.get("fred_data", {})
    fred_dates = [d.get("date", "") for d in fred_data.values() if d.get("date")]
    latest_fred = _fmt_date(max(fred_dates)) if fred_dates else "unavailable"

    return (
        f"> **Generated:** {_fmt_et(generated_at)}  \n"
        f"> **Market prices:** {_fmt_date(market_date)} close (yfinance)  \n"
        f"> **Macro data:** most recent available FRED release (latest obs: {latest_fred})  \n"
        f"> **News:** past 24–48 hours (NewsAPI + NYT API)  \n"
        f"> **Economic calendar:** next 7 days (Investing.com)\n"
    )


def _build_freshness_section(data: Dict[str, Any], generated_at: datetime) -> str:
    """Full data provenance tables — no FRED series IDs visible; kept in footer."""
    market_data = data.get("market_data", {})
    fred_data   = data.get("fred_data", {})
    eia_data    = data.get("eia_data", {})
    news        = data.get("news_headlines", [])
    nyt         = data.get("nyt_headlines", [])
    calendar    = data.get("economic_calendar", [])
    sector_data = data.get("sector_performance", {})

    market_date = _fmt_date(_market_date(market_data))
    spy = sector_data.get("SPY", {})
    sector_as_of = _fmt_date(spy.get("as_of", "")) if isinstance(spy, dict) and spy.get("as_of") else market_date

    eia_periods = [d.get("period", "") for d in eia_data.values() if d.get("period")]
    eia_latest  = max(eia_periods) if eia_periods else "unavailable"

    news_dates  = [h.get("published_at", "")[:10] for h in (news + nyt) if h.get("published_at")]
    news_latest = _fmt_date(max(news_dates)) if news_dates else "unavailable"

    # ── Build value strings for each instrument group ────────────────────────
    def _mv(name: str) -> Optional[Dict]:
        return market_data.get(name)

    def _price_chg(name: str) -> str:
        d = _mv(name)
        if not d:
            return "N/A"
        p = d.get("price")
        c = d.get("change_pct")
        p_str = _fmt_mkt_price(MARKET_DISPLAY.get(name, name), p)
        c_str = f" ({c:+.2f}%)" if c is not None else ""
        return f"{p_str}{c_str}"

    # Row 1: Indices
    sp5 = _price_chg("S&P 500")
    nq  = _price_chg("NASDAQ")
    dj  = _price_chg("Dow Jones")
    indices_val = f"S&P 500: {sp5} | NASDAQ: {nq} | Dow: {dj}"

    # Row 2: Sector ETFs — top 2 leaders and top 2 laggards by YTD
    sorted_sectors = sorted(
        [(s, d) for s, d in sector_data.items() if s != "SPY" and isinstance(d, dict)],
        key=lambda x: x[1].get("ytd") or -999,
        reverse=True,
    )
    def _etf_str(sector: str, d: Dict) -> str:
        ticker = d.get("ticker", "")
        ytd = d.get("ytd")
        ytd_str = f"{ytd:+.2f}% YTD" if ytd is not None else "N/A"
        return f"{sector} ({ticker}): {ytd_str}"

    leaders  = [_etf_str(s, d) for s, d in sorted_sectors[:2]]
    laggards = [_etf_str(s, d) for s, d in sorted_sectors[-2:]]
    sectors_val = " | ".join(leaders) + " ··· " + " | ".join(laggards)

    # Row 3: VIX, DXY, Treasury yields
    vix_d = _mv("VIX")
    dxy_d = _mv("DXY")
    t10_d = _mv("10Y Treasury")
    t2_d  = _mv("2Y Treasury")
    vix_str = f"VIX: {vix_d['price']:.2f}" if vix_d and vix_d.get("price") else "VIX: N/A"
    dxy_str = f"US Dollar Index: {dxy_d['price']:.2f}" if dxy_d and dxy_d.get("price") else "DXY: N/A"
    t10_str = f"10Y Yield: {t10_d['price']:.2f}%" if t10_d and t10_d.get("price") else "10Y: N/A"
    t2_str  = f"2Y Yield: {t2_d['price']:.2f}%" if t2_d and t2_d.get("price") else "2Y: N/A"
    rates_val = f"{vix_str} | {dxy_str} | {t10_str} | {t2_str}"

    # Row 4: Commodities
    wti_d    = _mv("WTI Oil")
    brent_d  = _mv("Brent Oil")
    copper_d = _mv("Copper")
    gold_d   = _mv("Gold")
    wti_str    = f"WTI: {_fmt_mkt_price('WTI Oil', wti_d['price'])}" if wti_d and wti_d.get("price") else "WTI: N/A"
    brent_str  = f"Brent: {_fmt_mkt_price('Brent Oil', brent_d['price'])}" if brent_d and brent_d.get("price") else "Brent: N/A"
    copper_str = f"Copper: {_fmt_mkt_price('Copper', copper_d['price'])}" if copper_d and copper_d.get("price") else "Copper: N/A"
    gold_str   = f"Gold: {_fmt_mkt_price('Gold', gold_d['price'])}" if gold_d and gold_d.get("price") else "Gold: N/A"
    commodities_val = f"{wti_str} | {brent_str} | {copper_str} | {gold_str}"

    lines = [
        "", "---", "",
        "## Data Freshness & Source Transparency",
        "",
        "### Market Prices",
        "| Instrument Group | Source | As-Of Date | Values |",
        "|---|---|---|---|",
        f"| Indices (S&P 500, NASDAQ, Dow) | yfinance | {market_date} close | {indices_val} |",
        f"| Sector ETFs | yfinance | {sector_as_of} close | {sectors_val} |",
        f"| VIX Volatility Index, US Dollar Index, Treasury Yields | yfinance | {market_date} close | {rates_val} |",
        f"| WTI Crude Oil, Brent Crude, Copper, Gold | yfinance | {market_date} close | {commodities_val} |",
        "",
        "### Macroeconomic Indicators (FRED)",
        "Observation date = the time period the data covers. "
        "Monthly data typically lags 2–6 weeks; quarterly data (GDP) lags 1–3 months.",
        "",
        "| Indicator | Observation Date | Value | What It Means |",
        "|-----------|-----------------|-------|---------------|",
    ]

    ORDERED = [
        "CPI", "Core_CPI", "PCE", "Core_PCE", "PPI",
        "Real_GDP_Growth", "Real_GDP", "Unemployment", "Jobless_Claims",
        "Continued_Claims", "JOLTS", "Retail_Sales", "Consumer_Sentiment",
        "Industrial_Production", "Capacity_Utilization",
        "Fed_Funds_Rate", "10Y_Yield", "2Y_Yield", "Yield_Curve_10Y2Y",
        "IG_Credit_Spread", "HY_Credit_Spread",
        "Housing_Starts", "Building_Permits", "Existing_Home_Sales", "M2",
        "ISM_Manufacturing_PMI",
    ]
    for key in ORDERED:
        if key not in fred_data:
            continue
        d = fred_data[key]
        display = FRED_DISPLAY.get(key, key)
        raw_date = d.get("date", "")
        obs = _freshness_note(key, raw_date) if raw_date else "—"
        fmt_val = _fmt_fred(key, d.get("value"), d.get("yoy_pct"), d.get("change"))
        what = FRED_WHAT_IT_MEANS.get(key, "—")
        lines.append(f"| {display} | {obs} | {fmt_val} | {what} |")

    lines += [
        "",
        "### EIA Energy Data",
        "| Series | Week Of | Value |",
        "|--------|---------|-------|",
    ]
    EIA_LABELS = {
        "crude_oil_stocks":       "US Crude Oil Stocks",
        "total_petroleum_stocks": "Total Petroleum Stocks",
        "crude_production":       "US Crude Oil Production",
        "gasoline_stocks":        "Gasoline Stocks",
        "distillate_stocks":      "Distillate Fuel Stocks",
        "natgas_storage":         "Natural Gas Storage",
    }
    EIA_DIVISORS = {
        "crude_oil_stocks":       (1_000, "M barrels"),
        "total_petroleum_stocks": (1_000, "M barrels"),
        "crude_production":       (1_000, "M barrels per day"),
        "gasoline_stocks":        (1_000, "M barrels"),
        "distillate_stocks":      (1_000, "M barrels"),
        "natgas_storage":         (1,     "Bcf"),
    }
    for name, d in eia_data.items():
        label = EIA_LABELS.get(name, name)
        period = _fmt_date(d.get("period", "")) if d.get("period") else "—"
        val = d.get("value")
        div, unit = EIA_DIVISORS.get(name, (1, ""))
        val_str = f"{val/div:.1f} {unit}" if val is not None else "—"
        lines.append(f"| {label} | {period} | {val_str} |")

    lines += [
        "",
        "### News & Calendar",
        "| Source | Coverage | Count |",
        "|--------|----------|-------|",
        f"| NewsAPI | Financial headlines (as of {news_latest}) | {len(news)} articles |",
        f"| NYT API | Business & Economy (as of {news_latest}) | {len(nyt)} articles |",
        f"| Investing.com | Economic calendar (next 7 days) | {len(calendar)} events |",
        "",
        f"*Briefing generated {_fmt_et(generated_at)} using {MODEL}*",
        "",
    ]
    return "\n".join(lines)


def _build_disclaimer() -> str:
    return (
        "---\n\n"
        "*Note: Macro indicators (GDP, CPI, PCE, etc.) reflect the most recently published "
        "government release and are inherently lagged by 2–6 weeks. Quarterly indicators (GDP) "
        "may be 1–3 months old. Market prices reflect the most recent completed trading session. "
        "All timestamps are ET. This briefing is for informational purposes only — not financial advice.*\n"
    )


# ── Claude prompt ──────────────────────────────────────────────────────────────

def build_prompt(data: Dict[str, Any]) -> str:
    date_str_raw   = data.get("date", datetime.now().strftime("%Y-%m-%d"))
    date_str       = _fmt_date(date_str_raw)   # "April 13th, 2026"
    market_summary = _summarize_market(data.get("market_data", {}))
    sector_table   = _summarize_sector_table(data.get("sector_performance", {}))
    fred_summary   = _summarize_fred(data.get("fred_data", {}))
    eia_summary    = _summarize_eia(data.get("eia_data", {}))
    cal_summary    = _summarize_calendar(data.get("economic_calendar", []))
    news_summary   = _summarize_headlines(data.get("news_headlines", []), data.get("nyt_headlines", []))
    rotation_sum   = _summarize_rotation(data.get("sector_rotation", {}))
    breadth_sum    = data.get("breadth", {})
    breadth_str    = (
        "\n".join(
            f"- **{s}**: {d.get('pct_above_50ma','—')}% above 50-day MA | "
            f"{d.get('pct_above_200ma','—')}% above 200-day MA (n={d.get('stocks_sampled','?')})"
            for s, d in breadth_sum.items()
        ) if breadth_sum else "Breadth data unavailable."
    )

    return f"""You are a senior macro and equity markets analyst writing a daily briefing for a sophisticated investor. Today is {date_str}.

The data below already contains traffic light indicators (🟢 🟡 🔴) and source tags in italics. Your job is to synthesize it into a clean, executive-quality briefing.

## FORMATTING RULES — FOLLOW EXACTLY
1. Use 🟢 🟡 🔴 at the START of every bullet point and every data citation you write in the narrative. Match the logic: 🟢 = good for markets, 🔴 = bad for markets, 🟡 = neutral or mixed.
2. NEVER use FRED series codes (like CPIAUCSL, BAMLC0A0CM, UNRATE, etc.) anywhere in the output. Use only the plain-English names provided.
3. Round all numbers to 1 decimal place. Always include units (%, $/barrel, K claims, trillion, bp). No raw index levels unless they are the primary metric (e.g. VIX level is meaningful; CPI index level is not).
4. Replace all jargon with plain English: "IG OAS" → "Investment Grade Credit Spread", "HY OAS" → "High Yield Credit Spread", "DXY" → "US Dollar Index", "VIX" → "VIX Volatility Index", etc.
5. Keep "YoY" and "MoM" abbreviations — they are universally understood.
6. Retain source tags in italics exactly as shown in the data (e.g. _(yfinance · April 10th, 2026 close)_, _(FRED · obs. April 1st, 2026)_). All dates in source tags must use Month Dayth, Year format — never YYYY-MM-DD.
7. Do NOT invent or estimate any number not present in the data below.
8. Do NOT include a Data Freshness section — that is added separately.
9. Bold all key numbers in the narrative — specifically: all percentage changes (e.g. **+2.3%**), all primary metric values (e.g. VIX: **21.2**, CPI: **+3.1% YoY**), and traffic light numbers. Do NOT bold labels, dates, source names, or explanatory text.

---

## INPUT DATA

### Indices & Commodities
{market_summary}

### Sector ETF Performance (YTD ranked, yfinance)
{sector_table}

### Sector Rotation Signal
{rotation_sum}

### Breadth — % Stocks Above Moving Averages
{breadth_str}

### Macroeconomic Indicators (FRED)
{fred_summary}

### Energy Inventory Data (EIA)
{eia_summary}

### Top Headlines
{news_summary}

### Economic Calendar — Next 7 Days
{cal_summary}

---

## OUTPUT — Write exactly this structure, starting with the H1 line:

# Daily Market Briefing — {date_str}

## Executive Summary
[FIRST and MOST PROMINENT section. Write exactly 5 bullet points. Each bullet = one clear takeaway a senior executive needs to know. Plain English, no jargon. Start each bullet with 🟢 🟡 or 🔴. Be specific with numbers and units. These 5 bullets should tell the complete story of today's market.]

## Macro Overview
[3 sentences. Most important macro driver right now and why. Include traffic lights and source tags on key numbers.]

## Key Indicator Moves
[Bullet list. Each bullet starts with 🟢/🟡/🔴. Include source tag on every cited number. Focus on inflation, growth, employment, credit, Fed signals. Flag anomalies and surprises.]

## Sector Rotation & Market Structure
[Paragraph with traffic light at the start. Leaders, laggards, rotation signal, breadth read, notable divergences. Source tags on numbers.]

## Sector-Specific Signals
[4 bullets, one per sector below. Start each with 🟢/🟡/🔴.]
- 🟢/🟡/🔴 **Financials:** yield curve + Investment Grade/High Yield Credit Spread read
- 🟢/🟡/🔴 **Energy:** EIA inventory data + WTI/Brent read
- 🟢/🟡/🔴 **Real Estate:** 10-year yield vs sector performance
- 🟢/🟡/🔴 **Technology:** relevant signals from available data

## Watch List: Anomalies & Divergences
[2–4 bullets. Each starts with 🟢/🟡/🔴. Specific numbers required. Source tags required.]

## Top 5 Headlines
[5 bullets. Format: 🟢/🟡/🔴 **Source (date):** Headline. One sentence on why it matters.]

## This Week's Data Calendar
[Bullet list from the calendar data. Format: **Date** — Release name | Forecast vs Prior | Why it matters. Source: _(Investing.com)_]
"""


# ── Main entry point ───────────────────────────────────────────────────────────

def generate_briefing(data: Dict[str, Any]) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    generated_at = _now_et()
    client = anthropic.Anthropic(api_key=api_key, max_retries=5)
    prompt = build_prompt(data)

    logger.info(f"Calling Anthropic API (model={MODEL})")
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
        system=(
            "You are a senior macro and equity markets analyst. "
            "Write concise, data-driven briefings with traffic light indicators on every "
            "data point. Plain English only — no technical codes. Specific numbers with units. "
            "No filler. No invented figures."
        ),
    )

    claude_text = message.content[0].text
    logger.info(
        f"Briefing generated: {len(claude_text)} chars | "
        f"tokens: {message.usage.input_tokens} in / {message.usage.output_tokens} out"
    )

    # Inject metadata block right after the H1 line
    meta_block = _build_meta_block(data, generated_at)
    lines = claude_text.split("\n")
    h1_idx = next((i for i, ln in enumerate(lines) if ln.startswith("# ")), None)
    if h1_idx is not None:
        lines.insert(h1_idx + 1, "\n" + meta_block)
        claude_text = "\n".join(lines)
    else:
        claude_text = meta_block + "\n\n" + claude_text

    # Append data freshness tables + disclaimer at the bottom
    return claude_text + _build_freshness_section(data, generated_at) + _build_disclaimer()
