"""
data_fetcher.py - Market data collection from yfinance, FRED, EIA, NewsAPI, NYT
"""

import os
import ssl
import certifi
import logging
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from fredapi import Fred
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()
logger = logging.getLogger(__name__)

# ── SSL fix for Python 3.14 on macOS (no system certs bundled) ────────────────
# Point stdlib ssl and requests/urllib3 at certifi's trusted CA bundle.
os.environ.setdefault("SSL_CERT_FILE", certifi.where())
os.environ.setdefault("REQUESTS_CA_BUNDLE", certifi.where())
# Also patch the default ssl context so fredapi (which uses urllib) gets certs.
_orig_create_default_https_context = ssl.create_default_context
def _patched_ssl_context(*args, **kwargs):
    ctx = _orig_create_default_https_context(*args, **kwargs)
    ctx.load_verify_locations(certifi.where())
    return ctx
ssl.create_default_context = _patched_ssl_context

# ── Constants ──────────────────────────────────────────────────────────────────

SECTOR_ETFS: Dict[str, str] = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Energy": "XLE",
    "Industrials": "XLI",
    "Materials": "XLB",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}

INDICES: Dict[str, str] = {
    "S&P 500": "^GSPC",
    "NASDAQ": "^IXIC",
    "Dow Jones": "^DJI",
    "VIX": "^VIX",
    "DXY": "DX-Y.NYB",
    "10Y Treasury": "^TNX",
    "2Y Treasury": "^IRX",   # 13-week T-bill proxy for short rates
    "WTI Oil": "CL=F",
    "Brent Oil": "BZ=F",
    "Copper": "HG=F",
    "Gold": "GC=F",
}

# FRED series IDs → friendly names
FRED_SERIES: Dict[str, str] = {
    "CPI": "CPIAUCSL",
    "Core_CPI": "CPILFESL",
    "PCE": "PCEPI",
    "Core_PCE": "PCEPILFE",
    "PPI": "PPIACO",
    "Real_GDP_Growth": "A191RL1Q225SBEA",
    "Real_GDP": "GDPC1",
    "Unemployment": "UNRATE",
    "Jobless_Claims": "ICSA",
    "Continued_Claims": "CCSA",
    "JOLTS": "JTSJOL",
    "Retail_Sales": "RSXFS",
    "M2": "M2SL",
    "Housing_Starts": "HOUST",
    "Building_Permits": "PERMIT",
    "Consumer_Sentiment": "UMCSENT",
    "Industrial_Production": "INDPRO",
    "Capacity_Utilization": "TCU",
    "Fed_Funds_Rate": "FEDFUNDS",
    "10Y_Yield": "DGS10",
    "2Y_Yield": "DGS2",
    "Yield_Curve_10Y2Y": "T10Y2Y",
    "IG_Credit_Spread": "BAMLC0A0CM",
    "HY_Credit_Spread": "BAMLH0A0HYM2",
    "ISM_Manufacturing_PMI": "MANEMP",     # Manufacturing employment proxy; ISM PMI removed from FRED
    "Existing_Home_Sales": "EXHOSLUSM495S",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _safe_float(value) -> Optional[float]:
    try:
        v = float(value)
        return round(v, 4) if not np.isnan(v) else None
    except Exception:
        return None


def _pct_change(new_val: float, old_val: float) -> Optional[float]:
    if old_val and old_val != 0:
        return round((new_val / old_val - 1) * 100, 2)
    return None


# ── Market Data (yfinance) ─────────────────────────────────────────────────────

def fetch_market_data() -> Dict[str, Any]:
    """Fetch index and commodity prices, plus % changes."""
    today = datetime.now().date()
    start = today - timedelta(days=10)  # enough for 1-day change calculation
    logger.info("Fetching market/index data from yfinance")

    tickers = list(INDICES.values())
    raw = yf.download(tickers, start=str(start), progress=False, auto_adjust=True)

    if raw.empty:
        logger.warning("yfinance returned empty data for indices")
        return {}

    close = raw["Close"] if "Close" in raw.columns else raw

    result: Dict[str, Any] = {}
    for name, ticker in INDICES.items():
        try:
            series = close[ticker].dropna()
            if len(series) < 2:
                continue
            latest = series.iloc[-1]
            prev = series.iloc[-2]
            result[name] = {
                "ticker": ticker,
                "price": _safe_float(latest),
                "change_pct": _pct_change(latest, prev),
                "as_of": series.index[-1].strftime("%Y-%m-%d"),
            }
        except Exception as e:
            logger.warning(f"Error fetching {name} ({ticker}): {e}")

    logger.info(f"Fetched {len(result)} index/commodity series")
    return result


def fetch_sector_performance() -> Dict[str, Any]:
    """
    For each sector ETF: daily, weekly, MTD, YTD returns vs SPY.
    Also compute whether the ETF is above its 50-day and 200-day MA.
    """
    today = datetime.now().date()
    ytd_start = today.replace(month=1, day=1) - timedelta(days=5)  # small buffer
    all_tickers = list(SECTOR_ETFS.values()) + ["SPY"]

    logger.info("Fetching sector ETF data from yfinance")
    raw = yf.download(all_tickers, start=str(ytd_start), progress=False, auto_adjust=True)

    if raw.empty:
        logger.warning("yfinance returned empty sector data")
        return {}

    close = raw["Close"] if "Close" in raw.columns else raw

    week_ago = today - timedelta(days=7)
    mtd_start = today.replace(day=1)

    def calc_returns(series: pd.Series) -> Dict[str, Optional[float]]:
        series = series.dropna()
        if len(series) < 2:
            return {}

        latest = series.iloc[-1]
        prev = series.iloc[-2]

        # Weekly: first trading day >= 7 days ago
        wk = series[series.index >= pd.Timestamp(week_ago)]
        weekly_base = wk.iloc[0] if not wk.empty else series.iloc[0]

        # MTD
        mtd = series[series.index >= pd.Timestamp(mtd_start)]
        mtd_base = mtd.iloc[0] if not mtd.empty else series.iloc[0]

        # YTD: use first available value
        ytd_base = series.iloc[0]

        # MAs
        ma50 = series.tail(50).mean() if len(series) >= 50 else None
        ma200 = series.tail(200).mean() if len(series) >= 200 else None

        return {
            "daily": _pct_change(latest, prev),
            "weekly": _pct_change(latest, float(weekly_base)),
            "mtd": _pct_change(latest, float(mtd_base)),
            "ytd": _pct_change(latest, float(ytd_base)),
            "current_price": _safe_float(latest),
            "above_50ma": bool(latest > ma50) if ma50 else None,
            "above_200ma": bool(latest > ma200) if ma200 else None,
            "ma50": _safe_float(ma50),
            "ma200": _safe_float(ma200),
        }

    sectors: Dict[str, Any] = {}
    spy_rets = calc_returns(close["SPY"])

    for sector, ticker in SECTOR_ETFS.items():
        try:
            rets = calc_returns(close[ticker])
            if not rets:
                continue
            # Relative performance vs SPY
            rets["vs_spy_daily"] = (
                round(rets["daily"] - spy_rets.get("daily", 0), 2)
                if rets.get("daily") is not None and spy_rets.get("daily") is not None
                else None
            )
            rets["vs_spy_ytd"] = (
                round(rets["ytd"] - spy_rets.get("ytd", 0), 2)
                if rets.get("ytd") is not None and spy_rets.get("ytd") is not None
                else None
            )
            rets["ticker"] = ticker
            sectors[sector] = rets
        except Exception as e:
            logger.warning(f"Sector error {sector}: {e}")

    sectors["SPY"] = {**spy_rets, "ticker": "SPY"}

    # Rank sectors by YTD momentum
    ranked = sorted(
        [(s, d.get("ytd") or -999) for s, d in sectors.items() if s != "SPY"],
        key=lambda x: x[1],
        reverse=True,
    )
    for rank, (sector, _) in enumerate(ranked, 1):
        sectors[sector]["ytd_rank"] = rank

    logger.info(f"Fetched performance for {len(sectors)-1} sector ETFs")
    return sectors


def fetch_sp500_ma_breadth() -> Dict[str, Any]:
    """
    Fetch S&P 500 constituents from Wikipedia, compute % above 50d/200d MA
    by sector. Returns a dict keyed by GICS sector name.
    """
    logger.info("Fetching S&P 500 constituents for breadth calculation")
    breadth: Dict[str, Any] = {}
    try:
        tables = pd.read_html(
            "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
            storage_options={"verify": certifi.where()},
        )
        constituents = tables[0][["Symbol", "GICS Sector"]].copy()
        constituents["Symbol"] = constituents["Symbol"].str.replace(".", "-", regex=False)

        # Group by sector, limit to manageable batch size per sector
        grouped = constituents.groupby("GICS Sector")

        today = datetime.now().date()
        start = today - timedelta(days=220)  # need 200 trading days

        for sector_name, group in grouped:
            tickers = group["Symbol"].tolist()[:50]  # cap at 50 per sector for speed
            try:
                raw = yf.download(
                    tickers, start=str(start), progress=False, auto_adjust=True, threads=True
                )
                if raw.empty:
                    continue

                prices = raw["Close"] if "Close" in raw.columns else raw
                above_50 = above_200 = total = 0
                for tk in tickers:
                    if tk not in prices.columns:
                        continue
                    s = prices[tk].dropna()
                    if len(s) < 50:
                        continue
                    total += 1
                    latest = s.iloc[-1]
                    if len(s) >= 50 and latest > s.tail(50).mean():
                        above_50 += 1
                    if len(s) >= 200 and latest > s.tail(200).mean():
                        above_200 += 1

                if total > 0:
                    breadth[sector_name] = {
                        "pct_above_50ma": round(above_50 / total * 100, 1),
                        "pct_above_200ma": round(above_200 / total * 100, 1),
                        "stocks_sampled": total,
                    }
            except Exception as e:
                logger.warning(f"Breadth error for {sector_name}: {e}")

    except Exception as e:
        logger.warning(f"Could not compute breadth data: {e}")

    logger.info(f"Breadth computed for {len(breadth)} sectors")
    return breadth


# ── FRED Macroeconomic Data ────────────────────────────────────────────────────

def fetch_fred_data() -> Dict[str, Any]:
    """Fetch latest values for all tracked FRED series."""
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        logger.error("FRED_API_KEY not set")
        return {}

    fred = Fred(api_key=api_key)
    results: Dict[str, Any] = {}
    lookback = datetime.now() - timedelta(days=730)  # 2 years for context

    for name, series_id in FRED_SERIES.items():
        try:
            data = fred.get_series(series_id, observation_start=lookback.strftime("%Y-%m-%d"))
            if data is None or data.empty:
                continue
            data = data.dropna()
            if data.empty:
                continue

            latest_val = _safe_float(data.iloc[-1])
            latest_date = data.index[-1].strftime("%Y-%m-%d")
            prev_val = _safe_float(data.iloc[-2]) if len(data) >= 2 else None
            year_ago_idx = data.index.searchsorted(data.index[-1] - pd.DateOffset(years=1))
            year_ago_val = _safe_float(data.iloc[year_ago_idx]) if year_ago_idx < len(data) else None

            results[name] = {
                "series_id": series_id,
                "value": latest_val,
                "date": latest_date,
                "prev_value": prev_val,
                "change": round(latest_val - prev_val, 4) if latest_val and prev_val else None,
                "yoy_pct": _pct_change(latest_val, year_ago_val) if latest_val and year_ago_val else None,
            }
        except Exception as e:
            logger.warning(f"FRED series {name} ({series_id}) error: {e}")

    logger.info(f"Fetched {len(results)} FRED series")
    return results


# ── EIA Energy Data ────────────────────────────────────────────────────────────

def _eia_fetch(series_id: str, api_key: str, limit: int = 4) -> Optional[Dict]:
    """Generic EIA v2 weekly data fetch."""
    url = "https://api.eia.gov/v2/petroleum/sum/sndw/data/"
    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": limit,
        "offset": 0,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json().get("response", {}).get("data", [])
        if data:
            latest = data[0]
            prev = data[1] if len(data) > 1 else None
            return {
                "value": _safe_float(latest.get("value")),
                "period": latest.get("period"),
                "prev_value": _safe_float(prev.get("value")) if prev else None,
                "change": (
                    round(_safe_float(latest.get("value")) - _safe_float(prev.get("value")), 2)
                    if prev and latest.get("value") and prev.get("value")
                    else None
                ),
            }
    except Exception as e:
        logger.warning(f"EIA fetch error ({series_id}): {e}")
    return None


def _eia_ng_fetch(series_id: str, api_key: str, limit: int = 4) -> Optional[Dict]:
    """EIA natural gas storage fetch."""
    url = "https://api.eia.gov/v2/natural-gas/stor/wkly/data/"
    params = {
        "api_key": api_key,
        "frequency": "weekly",
        "data[0]": "value",
        "facets[series][]": series_id,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": limit,
        "offset": 0,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data = r.json().get("response", {}).get("data", [])
        if data:
            latest = data[0]
            prev = data[1] if len(data) > 1 else None
            return {
                "value": _safe_float(latest.get("value")),
                "period": latest.get("period"),
                "units": "Bcf",
                "change": (
                    round(_safe_float(latest.get("value")) - _safe_float(prev.get("value")), 2)
                    if prev and latest.get("value") and prev.get("value")
                    else None
                ),
            }
    except Exception as e:
        logger.warning(f"EIA NG fetch error ({series_id}): {e}")
    return None


def fetch_eia_data() -> Dict[str, Any]:
    """Fetch EIA weekly petroleum inventory and natural gas storage."""
    api_key = os.getenv("EIA_API_KEY")
    if not api_key:
        logger.warning("EIA_API_KEY not set — skipping EIA data")
        return {}

    logger.info("Fetching EIA energy data")
    results: Dict[str, Any] = {}

    eia_petroleum_series = {
        "crude_oil_stocks": "WCRSTUS1",           # US crude stocks (1000 bbl)
        "total_petroleum_stocks": "WTTSTUS1",     # Total petroleum stocks
        "crude_production": "WCRFPUS2",           # Weekly crude production (kb/d)
        "gasoline_stocks": "WGTSTUS1",            # Gasoline stocks
        "distillate_stocks": "WDISTUS1",          # Distillate fuel stocks
    }

    for name, sid in eia_petroleum_series.items():
        val = _eia_fetch(sid, api_key)
        if val:
            results[name] = val

    # Natural gas storage
    ng = _eia_ng_fetch("NW2_EPG0_SWO_R48_BCF", api_key)
    if ng:
        results["natgas_storage"] = ng

    logger.info(f"Fetched {len(results)} EIA series")
    return results


# ── NewsAPI Headlines ──────────────────────────────────────────────────────────

MACRO_KEYWORDS = (
    "Fed OR Federal Reserve OR inflation OR recession OR GDP OR FOMC OR "
    "jobs OR yields OR earnings OR CPI OR interest rate OR Treasury OR "
    "tariff OR trade war OR market OR economy OR unemployment OR S&P 500 "
    "OR stock market OR hedge fund OR Wall Street OR bond OR debt OR dollar"
)


def fetch_news_headlines() -> List[Dict[str, str]]:
    """Fetch top macro/market headlines from NewsAPI."""
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        logger.warning("NEWS_API_KEY not set — skipping NewsAPI")
        return []

    # Free NewsAPI plan: only top-headlines endpoint is reliably available.
    # Use top-headlines with business category + a broad everything search as fallback.
    headlines = []
    try:
        url = "https://newsapi.org/v2/top-headlines"
        params = {
            "category": "business",
            "language": "en",
            "pageSize": 20,
            "apiKey": api_key,
            "country": "us",
        }
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        articles = r.json().get("articles", [])
        for a in articles[:15]:
            headlines.append({
                "title": a.get("title", ""),
                "source": a.get("source", {}).get("name", ""),
                "url": a.get("url", ""),
                "description": a.get("description", ""),
                "published_at": a.get("publishedAt", ""),
            })
        logger.info(f"Fetched {len(headlines)} NewsAPI top-headlines")
    except Exception as e:
        logger.warning(f"NewsAPI top-headlines error: {e}")

    # Also try everything endpoint (works on paid plans; gracefully empty on free)
    from_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    url = "https://newsapi.org/v2/everything"
    params = {
        "q": MACRO_KEYWORDS,
        "language": "en",
        "sortBy": "relevancy",
        "pageSize": 10,
        "from": from_date,
        "apiKey": api_key,
    }

    try:
        r = requests.get(url, params=params, timeout=15)
        r.raise_for_status()
        articles = r.json().get("articles", [])
        for a in articles[:10]:
            title = a.get("title", "")
            if title and not any(h.get("title") == title for h in headlines):
                headlines.append({
                    "title": title,
                    "source": a.get("source", {}).get("name", ""),
                    "url": a.get("url", ""),
                    "description": a.get("description", ""),
                    "published_at": a.get("publishedAt", ""),
                })
    except Exception as e:
        logger.warning(f"NewsAPI everything error: {e}")

    logger.info(f"Fetched {len(headlines)} total NewsAPI headlines")
    return headlines


# ── NYT Headlines ──────────────────────────────────────────────────────────────

def fetch_nyt_headlines() -> List[Dict[str, str]]:
    """Fetch top business/finance headlines from NYT APIs."""
    api_key = os.getenv("NYT_API_KEY")
    if not api_key:
        logger.warning("NYT_API_KEY not set — skipping NYT headlines")
        return []

    headlines: List[Dict[str, str]] = []

    # 1. Top Stories: Business section
    try:
        url = f"https://api.nytimes.com/svc/topstories/v2/business.json?api-key={api_key}"
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        for a in r.json().get("results", [])[:8]:
            headlines.append({
                "title": a.get("title", ""),
                "source": "NYT Top Stories",
                "url": a.get("url", ""),
                "abstract": a.get("abstract", ""),
                "published_at": a.get("published_date", ""),
            })
    except Exception as e:
        logger.warning(f"NYT Top Stories error: {e}")

    # 2. Times Wire: Business/Finance
    try:
        url = (
            f"https://api.nytimes.com/svc/news/v3/content/all/business.json"
            f"?api-key={api_key}&limit=8"
        )
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        for a in r.json().get("results", [])[:8]:
            if not any(h["title"] == a.get("title") for h in headlines):
                headlines.append({
                    "title": a.get("title", ""),
                    "source": "NYT Times Wire",
                    "url": a.get("url", ""),
                    "abstract": a.get("abstract", ""),
                    "published_at": a.get("published_date", ""),
                })
    except Exception as e:
        logger.warning(f"NYT Times Wire error: {e}")

    # 3. Article Search: key macro terms
    try:
        macro_terms = "inflation Federal Reserve GDP recession tariffs earnings"
        yesterday = (datetime.now() - timedelta(days=2)).strftime("%Y%m%d")
        today_str = datetime.now().strftime("%Y%m%d")
        r = requests.get(
            "https://api.nytimes.com/svc/search/v2/articlesearch.json",
            params={
                "q": macro_terms,
                "begin_date": yesterday,
                "end_date": today_str,
                "sort": "relevance",
                "api-key": api_key,
            },
            timeout=15,
        )
        r.raise_for_status()
        response_body = r.json().get("response") or {}
        docs = response_body.get("docs") or []
        for a in docs[:5]:
            headline_obj = a.get("headline") or {}
            title = headline_obj.get("main", "")
            if title and not any(h.get("title") == title for h in headlines):
                headlines.append({
                    "title": title,
                    "source": "NYT Article Search",
                    "url": a.get("web_url", ""),
                    "abstract": a.get("abstract", "") or a.get("snippet", ""),
                    "published_at": a.get("pub_date", ""),
                })
    except Exception as e:
        logger.warning(f"NYT Article Search error: {e}")

    logger.info(f"Fetched {len(headlines)} NYT headlines")
    return headlines[:15]


# ── Economic Calendar (Investing.com) ─────────────────────────────────────────

def fetch_economic_calendar() -> List[Dict[str, str]]:
    """
    Scrape Investing.com economic calendar for major US releases
    in the next 7 days. Falls back to FRED release schedule on failure.
    """
    logger.info("Fetching economic calendar from Investing.com")
    events: List[Dict[str, str]] = []

    try:
        today = datetime.now()
        end = today + timedelta(days=7)

        url = "https://www.investing.com/economic-calendar/Service/getCalendarFilteredData"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.investing.com/economic-calendar/",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://www.investing.com",
        }
        form_data = {
            "country[]": "5",  # USA = 5
            "importance[]": ["2", "3"],  # medium + high importance
            "dateFrom": today.strftime("%Y-%m-%d"),
            "dateTo": end.strftime("%Y-%m-%d"),
            "timeZone": "55",  # Eastern time zone
            "timeFilter": "timeRemain",
            "currentTab": "custom",
            "submitFilters": "1",
            "limit_from": "0",
        }

        r = requests.post(url, headers=headers, data=form_data, timeout=20)
        r.raise_for_status()
        html_content = r.json().get("data", "")

        soup = BeautifulSoup(html_content, "html.parser")
        rows = soup.find_all("tr", class_=lambda c: c and "js-event-item" in c)

        for row in rows[:30]:
            try:
                # Date/time
                dt_col = row.find("td", class_="left time")
                date_attr = row.get("data-event-datetime", "")

                # Event name
                name_col = row.find("td", class_="left event")
                name = name_col.get_text(strip=True) if name_col else ""

                # Importance (star icons)
                imp_col = row.find("td", class_="left textNum sentiment noWrap")
                importance = ""
                if imp_col:
                    filled = imp_col.find_all("i", class_="grayFullBullishIcon")
                    importance = "★" * len(filled) if filled else ""

                # Consensus / previous / actual
                actual_col = row.find("td", id=lambda i: i and "eventActual" in str(i))
                forecast_col = row.find("td", id=lambda i: i and "eventForecast" in str(i))
                prev_col = row.find("td", id=lambda i: i and "eventPrevious" in str(i))

                actual = actual_col.get_text(strip=True) if actual_col else ""
                forecast = forecast_col.get_text(strip=True) if forecast_col else ""
                previous = prev_col.get_text(strip=True) if prev_col else ""

                if name:
                    events.append({
                        "date": date_attr[:10] if date_attr else "",
                        "time": date_attr[11:16] if len(date_attr) > 10 else "",
                        "event": name,
                        "importance": importance,
                        "forecast": forecast,
                        "previous": previous,
                        "actual": actual,
                        "country": "US",
                    })
            except Exception:
                continue

        logger.info(f"Scraped {len(events)} calendar events from Investing.com")

    except Exception as e:
        logger.warning(f"Investing.com calendar scrape failed: {e} — using fallback")
        events = _get_calendar_fallback()

    return events


def _get_calendar_fallback() -> List[Dict[str, str]]:
    """
    Fallback: return known major scheduled US releases for the next 7 days
    by checking FRED's release dates via their public API.
    """
    logger.info("Using FRED release calendar as fallback")
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        return _static_calendar_events()

    events = []
    today = datetime.now().date()
    end = today + timedelta(days=7)

    # FRED releases of interest
    release_ids = {
        10: "Consumer Price Index",
        21: "GDP",
        50: "Employment Situation (NFP)",
        46: "Initial Jobless Claims",
        175: "Personal Income and Outlays (PCE)",
        99: "Retail Sales",
        404: "ISM Manufacturing PMI",
        184: "Housing Starts",
    }

    for release_id, release_name in release_ids.items():
        try:
            url = (
                f"https://api.stlouisfed.org/fred/release/dates"
                f"?release_id={release_id}&api_key={api_key}"
                f"&realtime_start={today}&realtime_end={end}&file_type=json"
            )
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            dates = r.json().get("release_dates", [])
            for d in dates:
                release_date = d.get("date", "")
                if release_date:
                    events.append({
                        "date": release_date,
                        "time": "08:30",
                        "event": release_name,
                        "importance": "★★★",
                        "forecast": "",
                        "previous": "",
                        "actual": "",
                        "country": "US",
                    })
        except Exception as e:
            logger.debug(f"FRED release {release_id} error: {e}")

    return sorted(events, key=lambda x: x["date"])


def _static_calendar_events() -> List[Dict[str, str]]:
    """Last-resort: return placeholder message."""
    return [
        {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "event": "Economic calendar unavailable — check investing.com/economic-calendar",
            "importance": "",
            "forecast": "",
            "previous": "",
            "actual": "",
            "country": "US",
        }
    ]


# ── Sector Rotation Analysis ───────────────────────────────────────────────────

def analyze_sector_rotation(sector_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Determine if market is rotating cyclical or defensive.
    Classify each sector, compare performance.
    """
    CYCLICAL = {"Technology", "Financials", "Consumer Discretionary", "Industrials",
                "Materials", "Communication Services", "Energy"}
    DEFENSIVE = {"Healthcare", "Consumer Staples", "Utilities", "Real Estate"}

    cyclical_ytd = [
        sector_data[s].get("ytd", 0) or 0
        for s in CYCLICAL if s in sector_data
    ]
    defensive_ytd = [
        sector_data[s].get("ytd", 0) or 0
        for s in DEFENSIVE if s in sector_data
    ]

    avg_cyc = np.mean(cyclical_ytd) if cyclical_ytd else 0
    avg_def = np.mean(defensive_ytd) if defensive_ytd else 0
    spread = round(avg_cyc - avg_def, 2)

    if spread > 3:
        rotation = "Risk-ON / Cyclical"
        signal = (
            "Cyclicals are outperforming defensives, suggesting investors are "
            "comfortable with risk. Typically seen in early/mid expansion phases."
        )
    elif spread < -3:
        rotation = "Risk-OFF / Defensive"
        signal = (
            "Defensives are outperforming cyclicals, indicating a flight to safety. "
            "Often seen before or during economic slowdowns."
        )
    else:
        rotation = "Neutral / Mixed"
        signal = (
            "No clear rotation signal. Cyclical and defensive sectors are "
            "performing similarly — market may be in a transitional phase."
        )

    # Leaders and laggards (by YTD)
    ranked = sorted(
        [(s, d.get("ytd") or -999) for s, d in sector_data.items() if s != "SPY"],
        key=lambda x: x[1],
        reverse=True,
    )

    return {
        "rotation_signal": rotation,
        "cyclical_avg_ytd": round(avg_cyc, 2),
        "defensive_avg_ytd": round(avg_def, 2),
        "cyclical_vs_defensive_spread": spread,
        "explanation": signal,
        "leaders": [s for s, _ in ranked[:3]],
        "laggards": [s for s, _ in ranked[-3:]],
        "full_ranking": [{"sector": s, "ytd": v} for s, v in ranked],
    }


# ── Main Aggregator ────────────────────────────────────────────────────────────

def collect_all_data() -> Dict[str, Any]:
    """
    Run all data collection steps and return a single consolidated dict.
    """
    logger.info("=== Starting full data collection ===")
    today_str = datetime.now().strftime("%Y-%m-%d")

    market_data = fetch_market_data()
    sector_data = fetch_sector_performance()
    fred_data = fetch_fred_data()
    eia_data = fetch_eia_data()
    news_headlines = fetch_news_headlines()
    nyt_headlines = fetch_nyt_headlines()
    calendar_events = fetch_economic_calendar()
    rotation_analysis = analyze_sector_rotation({k: v for k, v in sector_data.items() if k != "SPY"})

    # Breadth — this is slow; run it but allow skip on timeout
    try:
        breadth_data = fetch_sp500_ma_breadth()
    except Exception as e:
        logger.warning(f"Breadth calculation skipped: {e}")
        breadth_data = {}

    payload = {
        "date": today_str,
        "generated_at": datetime.now().isoformat(),
        "market_data": market_data,
        "sector_performance": sector_data,
        "sector_rotation": rotation_analysis,
        "breadth": breadth_data,
        "fred_data": fred_data,
        "eia_data": eia_data,
        "news_headlines": news_headlines,
        "nyt_headlines": nyt_headlines,
        "economic_calendar": calendar_events,
    }

    logger.info("=== Data collection complete ===")
    return payload
