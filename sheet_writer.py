"""
sheet_writer.py

Writes discovered leads to the Google Sheet used by the cold email pipeline.
Handles deduplication before inserting.

Dedup key: domain + first_name + last_name (case-insensitive)

Column order matches the cold email pipeline's leads tab exactly:
    first_name, last_name, company, domain, industry, role_level, role_context,
    title, email, verification_result, personalization, personalization_nudge,
    subject_line, cta, status, message_id, date_sent, fu1_target, fu1_sent,
    fu2_target, fu2_sent, nudge_target, nudge_sent, reply_status, notes
"""

import os
import logging
from datetime import date

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Google Sheets API scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Leads tab column order — must match cold email pipeline exactly
LEADS_COLUMNS = [
    "first_name",
    "last_name",
    "company",
    "domain",
    "industry",
    "role_level",
    "role_context",
    "title",
    "email",
    "verification_result",
    "personalization",
    "personalization_nudge",
    "subject_line",
    "cta",
    "status",
    "message_id",
    "date_sent",
    "fu1_target",
    "fu1_sent",
    "fu2_target",
    "fu2_sent",
    "nudge_target",
    "nudge_sent",
    "reply_status",
    "notes",
]

# Columns we populate — rest are left blank for cold email pipeline
WRITABLE_COLUMNS = {
    "first_name", "last_name", "company", "domain", "industry",
    "role_level", "role_context", "title", "status",
}


def _get_sheet_client() -> gspread.Client:
    """Authenticate and return a gspread client.

    Handles two cases:
    - Local dev: GOOGLE_SERVICE_ACCOUNT_JSON is a file path
    - Cloud Run: GOOGLE_SERVICE_ACCOUNT_JSON is the JSON content as a string
    """
    import json as _json
    sa_value = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")

    # Detect if value is JSON content or a file path
    if sa_value.strip().startswith("{"):
        # It's JSON content directly (Cloud Run / Secret Manager)
        sa_info = _json.loads(sa_value)
        creds   = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    else:
        # It's a file path (local dev)
        creds = Credentials.from_service_account_file(sa_value, scopes=SCOPES)

    return gspread.authorize(creds)


def _get_leads_worksheet(client: gspread.Client) -> gspread.Worksheet:
    """Open the leads worksheet from the configured Google Sheet."""
    sheet_id  = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("GOOGLE_SHEET_ID not set in environment")
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.worksheet("leads")


def _build_dedup_set(worksheet: gspread.Worksheet) -> set[str]:
    """
    Build a set of existing dedup keys from the sheet.
    Key format: "{domain}|{first_name}|{last_name}" (all lowercase)
    """
    records = worksheet.get_all_records()
    keys = set()
    for row in records:
        domain     = str(row.get("domain",     "")).lower().strip()
        first_name = str(row.get("first_name", "")).lower().strip()
        last_name  = str(row.get("last_name",  "")).lower().strip()
        if domain or first_name:
            keys.add(f"{domain}|{first_name}|{last_name}")
    logger.info("Loaded %d existing leads for dedup check", len(keys))
    return keys


def _lead_to_row(lead: dict) -> list:
    """
    Convert a lead dict to a row list matching LEADS_COLUMNS order.
    Columns not in WRITABLE_COLUMNS are left blank.
    """
    row = []
    for col in LEADS_COLUMNS:
        if col in WRITABLE_COLUMNS:
            row.append(lead.get(col, ""))
        else:
            row.append("")
    return row


def _dedup_key(lead: dict) -> str:
    """Build the dedup key for a lead dict."""
    domain     = str(lead.get("domain",     "")).lower().strip()
    first_name = str(lead.get("first_name", "")).lower().strip()
    last_name  = str(lead.get("last_name",  "")).lower().strip()
    return f"{domain}|{first_name}|{last_name}"


def write_leads(
    leads: list[dict],
    dry_run: bool = False,
) -> dict:
    """
    Write leads to the Google Sheet, skipping duplicates.

    Args:
        leads:   List of lead dicts from confidence_gate.py
        dry_run: If True, log what would be written but don't actually write

    Returns:
        Summary dict with counts:
            inserted, skipped_duplicate, skipped_missing_name, total
    """
    summary = {
        "inserted":             0,
        "skipped_duplicate":    0,
        "skipped_missing_name": 0,
        "total":                len(leads),
    }

    if not leads:
        logger.info("No leads to write")
        return summary

    client    = _get_sheet_client()
    worksheet = _get_leads_worksheet(client)
    existing  = _build_dedup_set(worksheet)

    rows_to_insert = []
    new_keys       = set()  # track keys added this run to avoid within-batch dupes

    for lead in leads:
        # Must have at minimum a first name to be useful
        if not lead.get("first_name", "").strip():
            logger.debug("Skipping lead with no first name: %s", lead)
            summary["skipped_missing_name"] += 1
            continue

        key = _dedup_key(lead)

        # Check against existing sheet rows and within this batch
        if key in existing or key in new_keys:
            logger.debug("Duplicate skipped: %s", key)
            summary["skipped_duplicate"] += 1
            continue

        new_keys.add(key)
        rows_to_insert.append(_lead_to_row(lead))

        logger.info(
            "Queued: %s %s — %s (%s)",
            lead.get("first_name"), lead.get("last_name"),
            lead.get("title"), lead.get("domain"),
        )

    if dry_run:
        logger.info("DRY RUN — would insert %d rows", len(rows_to_insert))
        summary["inserted"] = len(rows_to_insert)
        return summary

    if rows_to_insert:
        worksheet.append_rows(rows_to_insert, value_input_option="USER_ENTERED")
        summary["inserted"] = len(rows_to_insert)
        logger.info("Inserted %d new leads into sheet", len(rows_to_insert))
    else:
        logger.info("No new leads to insert after dedup")

    return summary


# ---------------------------------------------------------------------------
# Smoke test — python3 sheet_writer.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Test with a fake lead — dry_run=True so nothing actually gets written
    test_leads = [
        {
            "first_name":   "Lori",
            "last_name":    "Joint",
            "company":      "MBA USA",
            "domain":       "mbausa.org",
            "industry":     "consulting",
            "role_level":   "ceo_founder",
            "role_context": "founders and CEOs",
            "title":        "President & CEO",
            "status":       "ready_to_send",
            "source":       "hackernews",
        },
        {
            "first_name":   "Jane",
            "last_name":    "Doe",
            "company":      "Tech Co",
            "domain":       "techco.com",
            "industry":     "software",
            "role_level":   "hr_leader",
            "role_context": "leadership teams",
            "title":        "VP of People",
            "status":       "needs_review",
            "source":       "scraper",
        },
    ]

    print("Running sheet_writer smoke test (dry_run=True)...\n")
    summary = write_leads(test_leads, dry_run=True)

    print(f"\nSummary:")
    print(f"  Total leads:        {summary['total']}")
    print(f"  Would insert:       {summary['inserted']}")
    print(f"  Skipped duplicate:  {summary['skipped_duplicate']}")
    print(f"  Skipped no name:    {summary['skipped_missing_name']}")
    print("\nDry run complete — nothing written to sheet.")
    print("\nTo test a real write, call write_leads(leads, dry_run=False)")