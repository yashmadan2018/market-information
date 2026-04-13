"""
generate_dashboard.py - Build the GitHub Pages HTML dashboard
"""

import os
import re
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"
DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"


def _find_all_reports() -> List[Path]:
    """Return all markdown report files sorted newest-first."""
    if not REPORTS_DIR.exists():
        return []
    files = sorted(REPORTS_DIR.glob("*.md"), reverse=True)
    return [f for f in files if re.match(r"\d{4}-\d{2}-\d{2}\.md", f.name)]


def _markdown_to_html(text: str) -> str:
    """Convert markdown to HTML using the `markdown` library."""
    try:
        import markdown
        extensions = ["tables", "fenced_code", "nl2br", "sane_lists"]
        return markdown.markdown(text, extensions=extensions)
    except ImportError:
        # Minimal fallback
        import html as html_mod
        text = html_mod.escape(text)
        text = re.sub(r"^# (.+)$", r"<h1>\1</h1>", text, flags=re.MULTILINE)
        text = re.sub(r"^## (.+)$", r"<h2>\1</h2>", text, flags=re.MULTILINE)
        text = re.sub(r"^### (.+)$", r"<h3>\1</h3>", text, flags=re.MULTILINE)
        text = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", text)
        text = re.sub(r"_(.+?)_", r"<em>\1</em>", text)
        text = re.sub(r"^- (.+)$", r"<li>\1</li>", text, flags=re.MULTILINE)
        text = re.sub(r"(<li>.*?</li>\n?)+", lambda m: f"<ul>{m.group(0)}</ul>", text, flags=re.DOTALL)
        return text


def _read_report(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _extract_first_section(markdown_text: str, max_chars: int = 500) -> str:
    """Pull the first meaningful paragraph for archive card previews."""
    lines = markdown_text.split("\n")
    content_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            content_lines.append(stripped)
        if len(" ".join(content_lines)) >= max_chars:
            break
    preview = " ".join(content_lines)[:max_chars]
    return preview + "…" if len(preview) == max_chars else preview


CSS = """
:root {
  --navy: #0f2544;
  --navy-light: #1a3a6b;
  --accent: #1a6fc4;
  --accent-light: #e8f0fb;
  --text: #1a1a1a;
  --muted: #6b7280;
  --border: #e0e4e8;
  --bg: #f5f7fa;
  --card: #ffffff;
}

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 15px;
  line-height: 1.7;
  color: var(--text);
  background: var(--bg);
}

/* ── Top navigation ── */
nav {
  background: linear-gradient(135deg, var(--navy) 0%, var(--navy-light) 100%);
  color: white;
  padding: 0 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: 60px;
  position: sticky;
  top: 0;
  z-index: 100;
  box-shadow: 0 2px 8px rgba(0,0,0,0.15);
}

nav .brand {
  font-size: 17px;
  font-weight: 700;
  letter-spacing: -0.3px;
  display: flex;
  align-items: center;
  gap: 10px;
}

nav .brand span { opacity: 0.7; font-weight: 400; font-size: 13px; }

nav .nav-links a {
  color: rgba(255,255,255,0.8);
  text-decoration: none;
  margin-left: 20px;
  font-size: 14px;
  transition: color 0.2s;
}
nav .nav-links a:hover { color: white; }

/* ── Layout ── */
.page-container {
  max-width: 1100px;
  margin: 0 auto;
  padding: 32px 24px 80px;
}

/* ── Hero latest briefing ── */
.latest-card {
  background: var(--card);
  border-radius: 12px;
  box-shadow: 0 2px 16px rgba(0,0,0,0.07);
  overflow: hidden;
  margin-bottom: 40px;
}

.latest-card .card-header {
  background: linear-gradient(135deg, var(--navy) 0%, var(--navy-light) 100%);
  color: white;
  padding: 24px 32px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.latest-card .card-header h2 {
  font-size: 18px;
  font-weight: 700;
}

.latest-card .card-header .badge {
  background: rgba(255,255,255,0.2);
  color: white;
  padding: 4px 12px;
  border-radius: 20px;
  font-size: 12px;
  font-weight: 600;
}

.briefing-body {
  padding: 32px;
  overflow-x: auto;
}

/* ── Briefing typography ── */
.briefing-body h1 {
  font-size: 22px;
  color: var(--navy);
  border-bottom: 2px solid var(--border);
  padding-bottom: 10px;
  margin-bottom: 20px;
  margin-top: 32px;
}
.briefing-body h1:first-child { margin-top: 0; }

.briefing-body h2 {
  font-size: 17px;
  color: var(--navy);
  border-bottom: 1px solid var(--border);
  padding-bottom: 7px;
  margin-top: 28px;
  margin-bottom: 14px;
}

.briefing-body h3 {
  font-size: 15px;
  color: var(--navy-light);
  margin-top: 20px;
  margin-bottom: 8px;
}

.briefing-body p {
  margin-bottom: 12px;
  color: #2c2c2c;
}

.briefing-body ul, .briefing-body ol {
  padding-left: 22px;
  margin-bottom: 14px;
}

.briefing-body li { margin-bottom: 7px; }

.briefing-body strong { color: var(--navy); }

.briefing-body table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
  margin: 16px 0;
}

.briefing-body th {
  background: var(--navy);
  color: white;
  padding: 9px 12px;
  text-align: left;
  font-weight: 600;
}

.briefing-body td {
  padding: 8px 12px;
  border-bottom: 1px solid var(--border);
}

.briefing-body tr:nth-child(even) td { background: #f9fafb; }
.briefing-body tr:hover td { background: var(--accent-light); }

.briefing-body code {
  background: #f0f4f8;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 13px;
}

.briefing-body blockquote {
  border-left: 3px solid var(--accent);
  padding-left: 16px;
  color: var(--muted);
  margin: 12px 0;
}

/* ── Archive section ── */
.section-title {
  font-size: 18px;
  font-weight: 700;
  color: var(--navy);
  margin-bottom: 20px;
  display: flex;
  align-items: center;
  gap: 10px;
}

.archive-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 16px;
}

.archive-card {
  background: var(--card);
  border-radius: 10px;
  padding: 20px;
  box-shadow: 0 1px 6px rgba(0,0,0,0.06);
  border: 1px solid var(--border);
  transition: box-shadow 0.2s, border-color 0.2s;
  text-decoration: none;
  color: inherit;
  display: block;
}

.archive-card:hover {
  box-shadow: 0 4px 16px rgba(0,0,0,0.10);
  border-color: var(--accent);
}

.archive-card .date {
  font-size: 13px;
  font-weight: 700;
  color: var(--accent);
  margin-bottom: 8px;
}

.archive-card .preview {
  font-size: 13px;
  color: var(--muted);
  line-height: 1.55;
  display: -webkit-box;
  -webkit-line-clamp: 3;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

.archive-card.active {
  border-color: var(--navy);
  background: var(--accent-light);
}

/* ── Meta bar ── */
.meta-bar {
  display: flex;
  align-items: center;
  gap: 16px;
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 20px;
  margin-bottom: 32px;
  font-size: 13px;
  color: var(--muted);
  flex-wrap: wrap;
}

.meta-bar .meta-item { display: flex; align-items: center; gap: 6px; }
.meta-bar .meta-item strong { color: var(--text); }

/* ── Responsive ── */
@media (max-width: 640px) {
  nav { padding: 0 16px; }
  .page-container { padding: 20px 16px 60px; }
  .briefing-body { padding: 20px; }
  .latest-card .card-header { padding: 18px 20px; }
  .archive-grid { grid-template-columns: 1fr; }
}
"""


def generate_report_page(report_path: Path, all_reports: List[Path]) -> str:
    """Generate a single report's dedicated HTML page."""
    date_str = report_path.stem
    markdown_text = _read_report(report_path)
    body_html = _markdown_to_html(markdown_text)

    # Build archive sidebar links
    archive_links = ""
    for rp in all_reports[:30]:
        d = rp.stem
        active = " active" if d == date_str else ""
        archive_links += (
            f'<a href="{d}.html" class="archive-card{active}">'
            f'<div class="date">{d}</div>'
            f'</a>\n'
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Market Briefing — {date_str}</title>
<meta name="description" content="Daily macro and sector market briefing for {date_str}"/>
<style>{CSS}</style>
</head>
<body>
<nav>
  <div class="brand">
    📈 Market Information <span>Daily Briefing</span>
  </div>
  <div class="nav-links">
    <a href="index.html">Latest</a>
    <a href="archive.html">Archive</a>
  </div>
</nav>
<div class="page-container">
  <div class="latest-card">
    <div class="card-header">
      <h2>Market Briefing — {date_str}</h2>
      <span class="badge">Archived</span>
    </div>
    <div class="briefing-body">
      {body_html}
    </div>
  </div>
  <div class="section-title">Archive</div>
  <div class="archive-grid">
    {archive_links}
  </div>
</div>
</body>
</html>"""


def generate_index_page(latest_report: Path, all_reports: List[Path]) -> str:
    """Generate the main index.html showing the latest briefing."""
    date_str = latest_report.stem
    markdown_text = _read_report(latest_report)
    body_html = _markdown_to_html(markdown_text)
    generated_at = datetime.now().strftime("%B %d, %Y at %I:%M %p ET")
    report_count = len(all_reports)

    # Archive cards (skip the latest, it's already shown)
    archive_cards = ""
    for rp in all_reports[1:7]:  # Show 6 most recent after latest
        d = rp.stem
        preview = _extract_first_section(_read_report(rp), max_chars=150)
        archive_cards += f"""
<a href="{d}.html" class="archive-card">
  <div class="date">{d}</div>
  <div class="preview">{preview}</div>
</a>"""

    view_all = (
        f'<a href="archive.html" style="font-size:13px;color:var(--accent);text-decoration:none;">'
        f'View all {report_count} reports →</a>'
        if report_count > 7 else ""
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Market Information — Daily Briefing</title>
<meta name="description" content="Automated daily macro and sector market briefing powered by AI"/>
<style>{CSS}</style>
</head>
<body>
<nav>
  <div class="brand">
    📈 Market Information <span>Daily Briefing</span>
  </div>
  <div class="nav-links">
    <a href="index.html">Latest</a>
    <a href="archive.html">Archive</a>
  </div>
</nav>
<div class="page-container">

  <div class="meta-bar">
    <div class="meta-item">📅 <strong>Latest:</strong> {date_str}</div>
    <div class="meta-item">🕐 <strong>Updated:</strong> {generated_at}</div>
    <div class="meta-item">📁 <strong>Reports in archive:</strong> {report_count}</div>
    <div class="meta-item">🤖 <strong>Powered by:</strong> Claude AI + live market data</div>
  </div>

  <div class="latest-card">
    <div class="card-header">
      <h2>Today's Briefing — {date_str}</h2>
      <span class="badge">Latest</span>
    </div>
    <div class="briefing-body">
      {body_html}
    </div>
  </div>

  {"<div class='section-title'>Recent Briefings " + view_all + "</div><div class='archive-grid'>" + archive_cards + "</div>" if archive_cards else ""}

</div>
</body>
</html>"""


def generate_archive_page(all_reports: List[Path]) -> str:
    """Generate archive.html with cards for all briefings."""
    cards = ""
    for rp in all_reports:
        d = rp.stem
        preview = _extract_first_section(_read_report(rp), max_chars=200)
        cards += f"""
<a href="{d}.html" class="archive-card">
  <div class="date">{d}</div>
  <div class="preview">{preview}</div>
</a>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Archive — Market Information</title>
<style>{CSS}</style>
</head>
<body>
<nav>
  <div class="brand">
    📈 Market Information <span>Daily Briefing</span>
  </div>
  <div class="nav-links">
    <a href="index.html">Latest</a>
    <a href="archive.html">Archive</a>
  </div>
</nav>
<div class="page-container">
  <div class="section-title">All Briefings ({len(all_reports)} total)</div>
  <div class="archive-grid">
    {cards}
  </div>
</div>
</body>
</html>"""


def build_dashboard() -> None:
    """
    Build the full GitHub Pages site:
    - docs/index.html  (latest briefing)
    - docs/archive.html (all reports)
    - docs/YYYY-MM-DD.html (individual report pages)
    """
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    all_reports = _find_all_reports()
    if not all_reports:
        logger.warning("No reports found in reports/ — dashboard not generated")
        return

    logger.info(f"Building dashboard with {len(all_reports)} reports")

    # Index page (latest)
    latest = all_reports[0]
    index_html = generate_index_page(latest, all_reports)
    (DOCS_DIR / "index.html").write_text(index_html, encoding="utf-8")
    logger.info("Generated docs/index.html")

    # Archive page
    archive_html = generate_archive_page(all_reports)
    (DOCS_DIR / "archive.html").write_text(archive_html, encoding="utf-8")
    logger.info("Generated docs/archive.html")

    # Individual report pages
    for report_path in all_reports:
        page_html = generate_report_page(report_path, all_reports)
        out_path = DOCS_DIR / f"{report_path.stem}.html"
        out_path.write_text(page_html, encoding="utf-8")

    logger.info(f"Generated {len(all_reports)} individual report pages")
    logger.info("Dashboard build complete")
