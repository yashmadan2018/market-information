"""
email_sender.py - Send the daily briefing via Gmail SMTP
"""

import os
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

RECIPIENT_EMAILS = ["yashmadan2018@gmail.com", "vkilikitas@gmail.com", "micnic89@hotmail.com"]


def _markdown_to_html(markdown_text: str) -> str:
    """
    Convert a subset of Markdown to HTML for the email body.
    Uses the 'markdown' package if available, otherwise does basic conversion.
    """
    try:
        import markdown as md
        html_body = md.markdown(
            markdown_text,
            extensions=["tables", "fenced_code", "nl2br", "sane_lists"],
        )
        return html_body
    except ImportError:
        pass

    # Minimal fallback conversion
    import re
    html = markdown_text

    # Headers
    html = re.sub(r"^# (.+)$", r"<h1>\1</h1>", html, flags=re.MULTILINE)
    html = re.sub(r"^## (.+)$", r"<h2>\1</h2>", html, flags=re.MULTILINE)
    html = re.sub(r"^### (.+)$", r"<h3>\1</h3>", html, flags=re.MULTILINE)

    # Bold
    html = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", html)

    # Italic
    html = re.sub(r"_(.+?)_", r"<em>\1</em>", html)

    # Bullet points — wrap consecutive lines starting with '- '
    html = re.sub(r"^- (.+)$", r"<li>\1</li>", html, flags=re.MULTILINE)

    # Wrap <li> groups in <ul>
    html = re.sub(r"(<li>.*?</li>\n?)+", lambda m: f"<ul>{m.group(0)}</ul>", html, flags=re.DOTALL)

    # Table rows (basic)
    html = re.sub(r"^\|(.+)\|$", lambda m: "<tr>" + "".join(
        f"<td>{c.strip()}</td>" for c in m.group(1).split("|")
    ) + "</tr>", html, flags=re.MULTILINE)
    html = re.sub(r"<tr>(<td>[-:\s|]+</td>)+</tr>", "", html)  # Remove separator rows
    html = re.sub(r"(<tr>.*?</tr>\n?)+", lambda m: f"<table border='1' cellpadding='4' style='border-collapse:collapse'>{m.group(0)}</table>", html, flags=re.DOTALL)

    # Newlines to paragraphs
    paragraphs = []
    for line in html.split("\n\n"):
        line = line.strip()
        if line and not line.startswith("<"):
            line = f"<p>{line}</p>"
        paragraphs.append(line)
    html = "\n".join(paragraphs)

    return html


def _build_html_email(briefing_markdown: str, date_str: str) -> str:
    """Wrap the briefing in a clean HTML email template."""
    body_html = _markdown_to_html(briefing_markdown)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Daily Market Briefing — {_ordinal_date(date_str)}</title>
<style>
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto,
                 Oxygen, Ubuntu, sans-serif;
    font-size: 15px;
    line-height: 1.65;
    color: #1a1a1a;
    background: #f5f5f5;
    margin: 0;
    padding: 0;
  }}
  .container {{
    max-width: 760px;
    margin: 24px auto;
    background: #ffffff;
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
  }}
  .header {{
    background: linear-gradient(135deg, #0f2544 0%, #1a3a6b 100%);
    color: white;
    padding: 28px 36px;
  }}
  .header h1 {{
    margin: 0 0 6px 0;
    font-size: 22px;
    font-weight: 700;
    letter-spacing: -0.3px;
    color: #ffffff !important;
  }}
  .header .subtitle {{
    font-size: 13px;
    opacity: 0.75;
    margin: 0;
  }}
  .content {{
    padding: 28px 36px;
  }}
  h1, h2, h3 {{
    color: #0f2544;
    margin-top: 28px;
    margin-bottom: 10px;
  }}
  h1 {{ font-size: 20px; border-bottom: 2px solid #e8ecf0; padding-bottom: 8px; }}
  h2 {{ font-size: 17px; border-bottom: 1px solid #e8ecf0; padding-bottom: 6px; }}
  h3 {{ font-size: 15px; }}
  ul {{
    padding-left: 20px;
    margin: 8px 0;
  }}
  li {{ margin-bottom: 6px; }}
  table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    margin: 12px 0;
  }}
  th, td {{
    padding: 8px 10px;
    border: 1px solid #e0e4e8;
    text-align: right;
  }}
  th {{
    background: #f0f4f8;
    text-align: left;
    font-weight: 600;
  }}
  tr:nth-child(even) {{ background: #f9fafb; }}
  strong {{ color: #0f2544; }}
  .footer {{
    background: #f0f4f8;
    padding: 16px 36px;
    font-size: 12px;
    color: #666;
    border-top: 1px solid #e0e4e8;
  }}
  a {{ color: #1a6fc4; text-decoration: none; }}
  code {{
    background: #f0f4f8;
    padding: 2px 5px;
    border-radius: 3px;
    font-size: 13px;
  }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h1>Market Information Briefing</h1>
    <p class="subtitle">Daily macro &amp; sector briefing | {_ordinal_date(date_str)} | Powered by Claude AI</p>
  </div>
  <div class="content">
    {body_html}
  </div>
  <div class="footer">
    Generated automatically by the Market Information system using live data from
    yfinance, FRED, EIA, NewsAPI, and NYT. This is an automated briefing — not
    financial advice. Data may be delayed or subject to revision.
  </div>
</div>
</body>
</html>"""


def _ordinal_date(date_str: str) -> str:
    """Convert '2026-04-13' → 'April 13th, 2026'."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        day = dt.day
        # 11th, 12th, 13th are exceptions to the normal suffix rule
        suffix = "th" if 11 <= day <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
        return dt.strftime(f"%B {day}{suffix}, %Y")
    except Exception:
        return date_str


def send_briefing_email(
    briefing_markdown: str,
    date_str: Optional[str] = None,
    recipients: list = None,
) -> bool:
    """
    Send the markdown briefing as a formatted HTML email via Gmail SMTP.

    Returns True on success, False on failure.
    """
    gmail_user = os.getenv("GMAIL_USER")
    gmail_password = os.getenv("GMAIL_APP_PASSWORD")

    if not gmail_user or not gmail_password:
        logger.error("GMAIL_USER or GMAIL_APP_PASSWORD not set — cannot send email")
        return False

    if recipients is None:
        recipients = RECIPIENT_EMAILS

    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    subject = f"Market Information — {_ordinal_date(date_str)}"

    # Build message
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"Market Information <{gmail_user}>"
    msg["To"] = ", ".join(recipients)

    # Plain text fallback
    plain_text = briefing_markdown
    msg.attach(MIMEText(plain_text, "plain", "utf-8"))

    # HTML version
    html_content = _build_html_email(briefing_markdown, date_str)
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    try:
        logger.info(f"Connecting to Gmail SMTP as {gmail_user}")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_password)
            server.sendmail(gmail_user, recipients, msg.as_string())
        logger.info(f"Briefing email sent successfully to {', '.join(recipients)}")
        return True
    except smtplib.SMTPAuthenticationError:
        logger.error(
            "Gmail SMTP authentication failed. "
            "Ensure you are using a Gmail App Password, not your account password. "
            "Enable 2FA and generate an App Password at: "
            "https://myaccount.google.com/apppasswords"
        )
        return False
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending email: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected email error: {e}")
        return False
