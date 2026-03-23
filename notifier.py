"""
notifier.py

Sends a plain text email notification when leads need manual review.
Only fires when there are needs_review leads — silent otherwise.

Uses Zoho SMTP (same credentials as cold email pipeline).
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import date
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SMTP_HOST = "smtppro.zoho.com"
SMTP_PORT = 465


def send_review_notification(
    needs_review: list[dict],
    run_summary: dict,
    hunter_credits: dict | None = None,
) -> bool:
    """
    Send a notification email summarizing leads that need manual review.

    Args:
        needs_review: List of lead dicts with status='needs_review'
        run_summary:  Dict from sheet_writer.write_leads() with insert counts

    Returns:
        True if email sent successfully, False otherwise.
    """
    # Always send — even if no needs_review leads (daily summary)

    smtp_user    = os.getenv("ZOHO_SMTP_USER")
    smtp_pass    = os.getenv("ZOHO_SMTP_PASSWORD")
    notify_email = os.getenv("NOTIFICATION_EMAIL")
    sheet_id     = os.getenv("GOOGLE_SHEET_ID")

    if not all([smtp_user, smtp_pass, notify_email]):
        logger.error("Missing SMTP credentials — cannot send notification")
        return False

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}" if sheet_id else "your Google Sheet"

    review_count = len(needs_review)
    if review_count:
        subject = f"Lead Discovery: {review_count} lead(s) need review — {date.today().isoformat()}"
    else:
        subject = f"Lead Discovery: run complete — {date.today().isoformat()}"
    body    = _build_body(needs_review, run_summary, sheet_url, hunter_credits)

    try:
        msg = MIMEMultipart()
        msg["From"]    = smtp_user
        msg["To"]      = notify_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, notify_email, msg.as_string())

        logger.info("Notification sent to %s (%d needs_review leads)", notify_email, len(needs_review))
        return True

    except Exception as e:
        logger.error("Failed to send notification: %s", e)
        return False


def _build_body(
    needs_review: list[dict],
    run_summary: dict,
    sheet_url: str,
    hunter_credits: dict | None = None,
) -> str:
    """Build the plain text email body."""

    hunter_line = ""
    if hunter_credits:
        used      = hunter_credits.get("used", "?")
        available = hunter_credits.get("available", "?")
        remaining = hunter_credits.get("remaining", "?")
        hunter_line = f"  Hunter credits:        {used} used / {available} total ({remaining} remaining)"

    lines = [
        f"Lead Discovery Run — {date.today().isoformat()}",
        "=" * 45,
        "",
        "RUN SUMMARY",
        f"  Total leads processed: {run_summary.get('total', 0)}",
        f"  Inserted to sheet:     {run_summary.get('inserted', 0)}",
        f"  Skipped (duplicate):   {run_summary.get('skipped_duplicate', 0)}",
        f"  Ready to send:         {run_summary.get('inserted', 0) - len(needs_review)}",
        f"  Needs review:          {len(needs_review)}",
    ]
    if hunter_line:
        lines.append(hunter_line)
    lines.append("")

    if needs_review:
        lines += [
            "LEADS NEEDING REVIEW",
            "-" * 45,
        ]

    for i, lead in enumerate(needs_review, start=1):
        name    = f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()
        title   = lead.get("title",        "unknown title")
        company = lead.get("company",      "unknown company")
        domain  = lead.get("domain",       "unknown domain")
        source  = lead.get("lead_source",  lead.get("source", "unknown"))
        rl      = lead.get("role_level",   "?")
        rc      = lead.get("role_context", "?")
        conf    = lead.get("confidence",   "?")

        lines += [
            f"{i}. {name}",
            f"   Title:        {title}",
            f"   Company:      {company} ({domain})",
            f"   Source:       {source}",
            f"   role_level:   {rl}",
            f"   role_context: {rc}",
            f"   confidence:   {conf}",
            "",
        ]

    if needs_review:
        lines += [
            "-" * 45,
            "To review: open the sheet, find rows with status='needs_review',",
            "update role_level, role_context, and industry as needed,",
            "then change status to 'ready_to_send' to queue for outreach.",
            "",
        ]

    lines += [
        f"Sheet: {sheet_url}",
        "",
        "— Lead Discovery Pipeline",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Smoke test — python3 notifier.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    test_leads = [
        {
            "first_name":   "Jane",
            "last_name":    "Doe",
            "title":        "Operations Manager",
            "company":      "Acme Corp",
            "domain":       "acme.com",
            "role_level":   "hr_leader",
            "role_context": "needs_review",
            "confidence":   "low",
            "source":       "hackernews",
            "lead_source":  "hackernews",
        },
        {
            "first_name":   "John",
            "last_name":    "Smith",
            "title":        "Managing Partner",
            "company":      "Smith Legal",
            "domain":       "smithlegal.com",
            "role_level":   "hr_leader",
            "role_context": "leadership teams",
            "confidence":   "low",
            "source":       "scraper",
            "lead_source":  "scraper",
        },
    ]

    test_summary = {
        "total":             5,
        "inserted":          3,
        "skipped_duplicate": 2,
    }

    # Print what the email would look like without sending
    sheet_url    = f"https://docs.google.com/spreadsheets/d/{os.getenv('GOOGLE_SHEET_ID', 'YOUR_SHEET_ID')}"
    review_count = len(test_leads)
    if review_count:
        subject = f"Lead Discovery: {review_count} lead(s) need review — {date.today().isoformat()}"
    else:
        subject = f"Lead Discovery: run complete — {date.today().isoformat()}"

    body = _build_body(test_leads, test_summary, sheet_url)

    print(f"SUBJECT: {subject}")
    print("=" * 55)
    print(body)
    print("\n--- To send a real email, call send_review_notification() with real leads ---")
    print("\n--- To test a clean run, set test_leads = [] above ---")