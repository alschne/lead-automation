"""
backfill_industry.py

One-time script to populate the industry column for existing leads
that were inserted without industry data.

Reads all rows from the leads sheet where industry is blank,
infers industry from company name + domain using Gemini,
and writes it back.

Run once:
    python3 backfill_industry.py
"""

import os
import logging
import time
import google.generativeai as genai
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
from industry_normalizer import normalize_industry

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Column index (0-based) for industry in the leads sheet
# first_name, last_name, company, domain, industry = index 4
INDUSTRY_COL_INDEX = 4  # 0-based
INDUSTRY_COL_LETTER = "E"  # for gspread range notation


def _get_worksheet() -> gspread.Worksheet:
    sa_path     = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    sheet_id    = os.getenv("GOOGLE_SHEET_ID")
    creds       = Credentials.from_service_account_file(sa_path, scopes=SCOPES)
    client      = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.worksheet("leads")


def _infer_industry(company: str, domain: str, gemini_client) -> str:
    """Infer industry from company name and domain using Gemini."""
    if not company and not domain:
        return ""
    try:
        prompt = (
            f"What industry is this company in?\n"
            f"Company: {company}\n"
            f"Domain: {domain}\n\n"
            f"Reply with ONLY a short lowercase label (1-4 words) "
            f"that a human would say in conversation. "
            f"Examples: 'software', 'it services', 'legal services', "
            f"'cybersecurity', 'consulting', 'marketing', 'fintech'. "
            f"No explanation, no punctuation, just the label."
        )
        model    = gemini_client.GenerativeModel("gemini-flash-latest")
        response = model.generate_content(prompt)
        result   = response.text.strip().lower()
        if result and len(result.split()) <= 4 and len(result) < 40:
            return result
        return ""
    except Exception as e:
        logger.warning("Gemini inference failed for %s: %s", company, e)
        return ""


def backfill():
    # Initialize Gemini
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not set — cannot infer industries")
        return
    genai.configure(api_key=api_key)

    # Load sheet
    logger.info("Loading leads sheet...")
    worksheet = _get_worksheet()
    all_rows  = worksheet.get_all_values()

    if not all_rows:
        logger.info("Sheet is empty")
        return

    # First row is headers
    headers = all_rows[0]
    rows    = all_rows[1:]

    # Find column indices
    try:
        company_idx  = headers.index("company")
        domain_idx   = headers.index("domain")
        industry_idx = headers.index("industry")
    except ValueError as e:
        logger.error("Column not found in sheet: %s", e)
        return

    # Find rows with blank industry
    to_update = []
    for i, row in enumerate(rows, start=2):  # start=2 because row 1 is headers
        # Pad row if shorter than expected
        while len(row) <= max(company_idx, domain_idx, industry_idx):
            row.append("")
        industry = row[industry_idx].strip()
        if not industry:
            company = row[company_idx].strip()
            domain  = row[domain_idx].strip()
            if company or domain:
                to_update.append((i, company, domain))

    logger.info("Found %d leads with missing industry", len(to_update))

    if not to_update:
        logger.info("Nothing to backfill")
        return

    # Infer and update each row
    updated = 0
    for sheet_row, company, domain in to_update:
        logger.info("Inferring industry for %s (%s)...", company, domain)
        industry = _infer_industry(company, domain, genai)

        if not industry:
            logger.warning("Could not infer industry for %s — skipping", company)
            continue

        # Run through normalizer for consistent labels
        industry = normalize_industry(industry)

        # Update the cell directly
        cell = f"{INDUSTRY_COL_LETTER}{sheet_row}"
        worksheet.update(cell, [[industry]])
        logger.info("  Row %d: %s → '%s'", sheet_row, company, industry)
        updated += 1

        # Rate limit — Gemini free tier allows 5 requests/minute
        time.sleep(13)

    logger.info("Backfill complete: %d/%d rows updated", updated, len(to_update))


if __name__ == "__main__":
    backfill()