"""
main.py - Market Information daily briefing orchestrator

Run:
    python src/main.py
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Allow running from project root or src/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data_fetcher import collect_all_data
from src.briefing_generator import generate_briefing
from src.email_sender import send_briefing_email
from src.generate_dashboard import build_dashboard

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

# ── Paths ──────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = PROJECT_ROOT / "reports"
DATA_DIR = PROJECT_ROOT / "data"

REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ── Main Pipeline ──────────────────────────────────────────────────────────────

def run_pipeline(
    send_email: bool = True,
    save_files: bool = True,
    build_pages: bool = True,
    date_override: str = None,
) -> dict:
    """
    Full pipeline:
    1. Collect all market data
    2. Generate briefing via Anthropic API
    3. Save markdown and raw JSON
    4. Email the briefing
    5. Build GitHub Pages dashboard
    """
    today = date_override or datetime.now().strftime("%Y-%m-%d")
    logger.info(f"=== Market Information Pipeline | {today} ===")

    # ── Step 1: Data collection ──
    logger.info("Step 1/5: Collecting market data")
    data_path = DATA_DIR / f"{today}.json"
    # Resume from saved raw data if it exists (avoids re-downloading on API retry)
    if data_path.exists() and save_files:
        logger.info(f"Found existing raw data at {data_path} — loading for retry")
        import json as _json
        with open(data_path) as f:
            data = _json.load(f)
    else:
        try:
            data = collect_all_data()
        except Exception as e:
            logger.error(f"Data collection failed: {e}", exc_info=True)
            raise
        # Save raw data immediately so API retries don't re-collect
        if save_files:
            data_path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
            logger.info(f"Saved raw data: {data_path}")

    # ── Step 2: Generate briefing ──
    logger.info("Step 2/5: Generating briefing via Anthropic API")
    try:
        briefing = generate_briefing(data)
    except Exception as e:
        logger.error(f"Briefing generation failed: {e}", exc_info=True)
        raise

    # ── Step 3: Save markdown report ──
    if save_files:
        logger.info("Step 3/5: Saving markdown report")
        report_path = REPORTS_DIR / f"{today}.md"

        report_path.write_text(briefing, encoding="utf-8")
        logger.info(f"Saved report: {report_path}")
    else:
        logger.info("Step 3/5: Skipping file save (--no-save flag)")

    # ── Step 4: Send email ──
    if send_email:
        logger.info("Step 4/5: Sending email")
        success = send_briefing_email(briefing, date_str=today)
        if not success:
            logger.warning("Email delivery failed — check GMAIL_USER / GMAIL_APP_PASSWORD")
    else:
        logger.info("Step 4/5: Skipping email (--no-email flag)")

    # ── Step 5: Build dashboard ──
    if build_pages:
        logger.info("Step 5/5: Building GitHub Pages dashboard")
        try:
            build_dashboard()
        except Exception as e:
            logger.warning(f"Dashboard build failed (non-critical): {e}", exc_info=True)
    else:
        logger.info("Step 5/5: Skipping dashboard build (--no-dashboard flag)")

    logger.info("=== Pipeline complete ===")

    # Print briefing to stdout for CI logs
    print("\n" + "=" * 80)
    print(briefing)
    print("=" * 80 + "\n")

    return {"date": today, "briefing": briefing, "data": data}


# ── CLI Entry Point ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Market Information: Daily macro & sector briefing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/main.py                         # Full pipeline
  python src/main.py --no-email             # Skip email, still saves & builds dashboard
  python src/main.py --no-email --no-save   # Briefing to stdout only
  python src/main.py --dashboard-only       # Rebuild dashboard from existing reports
        """,
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="Skip sending the email"
    )
    parser.add_argument(
        "--no-save", action="store_true",
        help="Skip saving report/data files"
    )
    parser.add_argument(
        "--no-dashboard", action="store_true",
        help="Skip building the GitHub Pages dashboard"
    )
    parser.add_argument(
        "--dashboard-only", action="store_true",
        help="Only rebuild the dashboard from existing reports (no data fetch)"
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Override the report date (YYYY-MM-DD). Default: today."
    )

    args = parser.parse_args()

    if args.dashboard_only:
        logger.info("Dashboard-only mode: rebuilding from existing reports")
        build_dashboard()
        return

    run_pipeline(
        send_email=not args.no_email,
        save_files=not args.no_save,
        build_pages=not args.no_dashboard,
        date_override=args.date,
    )


if __name__ == "__main__":
    main()
