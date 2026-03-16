"""
backfill_industry.py

One-time script to populate the industry column for existing leads
that were inserted without industry data.

Uses batched Gemini calls — all companies in one prompt, one API call.

Run once:
    python3 backfill_industry.py

Delete this file once backfill is complete.
"""

import os
import json
import logging
import re
import time
import gspread
import google.generativeai as genai
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

INDUSTRY_COL_LETTER = "E"
BATCH_SIZE = 30


def _get_worksheet() -> gspread.Worksheet:
    import json as _json
    sa_value = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")

    if sa_value.strip().startswith("{"):
        sa_info = _json.loads(sa_value)
        creds   = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(sa_value, scopes=SCOPES)

    client      = gspread.authorize(creds)
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.worksheet("leads")


def _batch_infer_industries(
    companies: list[tuple[int, str, str]],
    gemini_model,
) -> dict[int, str]:
    """
    Infer industries for a batch of companies in a single Gemini call.

    Args:
        companies: List of (sheet_row, company_name, domain) tuples
        gemini_model: Initialized Gemini model instance

    Returns:
        Dict mapping sheet_row -> industry label
    """
    if not companies:
        return {}

    company_lines = "\n".join(
        f"{i+1}. Company: {company}, Domain: {domain}"
        for i, (_, company, domain) in enumerate(companies)
    )

    prompt = f"""For each company below, determine what industry they are in.

{company_lines}

Reply ONLY with a JSON array of objects in this exact format, one per company, in the same order:
[
  {{"index": 1, "industry": "software"}},
  {{"index": 2, "industry": "fintech"}}
]

Rules for industry labels:
- Lowercase only
- 1-4 words maximum
- Plain English a human would say in conversation
- Examples: "software", "it services", "legal services", "cybersecurity", "consulting", "marketing", "fintech", "hardware manufacturing", "education"
- No explanation, no markdown, no code fences — just the raw JSON array"""

    try:
        response = gemini_model.generate_content(prompt)
        raw = response.text.strip()
        raw = re.sub(r'^```json\s*|^```\s*|```$', '', raw, flags=re.MULTILINE).strip()
        results = json.loads(raw)

        output = {}
        for item in results:
            idx      = item.get("index", 0) - 1
            industry = item.get("industry", "").strip().lower()
            if 0 <= idx < len(companies):
                sheet_row  = companies[idx][0]
                normalized = normalize_industry(industry)
                output[sheet_row] = normalized if normalized else industry
        return output

    except json.JSONDecodeError as e:
        logger.error("Failed to parse Gemini JSON: %s", e)
        return {}
    except Exception as e:
        logger.error("Gemini batch call failed: %s", e)
        return {}


def backfill():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not set")
        return

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-flash-latest")

    logger.info("Loading leads sheet...")
    try:
        worksheet = _get_worksheet()
        all_rows  = worksheet.get_all_values()
    except Exception as e:
        logger.error("Failed to load sheet: %s", e)
        return

    if not all_rows:
        logger.info("Sheet is empty")
        return

    headers = all_rows[0]
    rows    = all_rows[1:]

    try:
        company_idx  = headers.index("company")
        domain_idx   = headers.index("domain")
        industry_idx = headers.index("industry")
    except ValueError as e:
        logger.error("Column not found: %s", e)
        return

    to_update: list[tuple[int, str, str]] = []
    for i, row in enumerate(rows, start=2):
        while len(row) <= max(company_idx, domain_idx, industry_idx):
            row.append("")
        if not row[industry_idx].strip():
            company = row[company_idx].strip()
            domain  = row[domain_idx].strip()
            if company or domain:
                to_update.append((i, company, domain))

    logger.info("Found %d leads with missing industry", len(to_update))
    if not to_update:
        logger.info("Nothing to backfill")
        return

    batches = [to_update[i:i+BATCH_SIZE] for i in range(0, len(to_update), BATCH_SIZE)]
    logger.info("Processing %d companies in %d batch(es)", len(to_update), len(batches))

    total_updated = 0
    for batch_num, batch in enumerate(batches, start=1):
        logger.info("Batch %d/%d (%d companies)...", batch_num, len(batches), len(batch))
        for _, company, domain in batch:
            logger.info("  - %s (%s)", company, domain)

        results = _batch_infer_industries(batch, model)

        if not results:
            logger.warning("Batch %d returned no results", batch_num)
            continue

        for sheet_row, industry in results.items():
            cell = f"{INDUSTRY_COL_LETTER}{sheet_row}"
            try:
                worksheet.update(range_name=cell, values=[[industry]])
                company = next((c for r, c, d in batch if r == sheet_row), "unknown")
                logger.info("  Row %d: %s -> '%s'", sheet_row, company, industry)
                total_updated += 1
                time.sleep(0.3)
            except Exception as e:
                logger.warning("Failed to write row %d: %s", sheet_row, e)

        if batch_num < len(batches):
            logger.info("Waiting 5s before next batch...")
            time.sleep(5)

    logger.info("Backfill complete: %d/%d rows updated", total_updated, len(to_update))


if __name__ == "__main__":
    backfill()