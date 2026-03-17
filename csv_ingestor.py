"""
csv_ingestor.py

Ingests lead CSVs exported from Hunter.io and Apollo.io.

Workflow:
    1. Check Google Drive folders for new CSV files
    2. Parse and clean each file
    3. Run title classifier + industry normalizer on each row
    4. Write qualified leads to Google Sheet (dedup handled by sheet_writer)
    5. Delete processed CSV from Drive folder

Google Drive folder structure:
    Lead Imports/
        hunter/     ← drop Hunter CSV exports here
        apollo/     ← drop Apollo CSV exports here

Setup:
    - Create a folder called "Lead Imports" in Google Drive
    - Inside it create "hunter" and "apollo" subfolders
    - Share those folders with your service account email
    - Add HUNTER_IMPORT_FOLDER_ID and APOLLO_IMPORT_FOLDER_ID to .env
      (get folder IDs from the URL when you open each folder in Drive)
"""

import os
import io
import csv
import logging
import re
from dotenv import load_dotenv

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from title_classifier import classify_title, should_extract_lead
from industry_normalizer import normalize_industry
from confidence_gate import assign_status
from sheet_writer import write_leads

load_dotenv()

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Minimum company size — skip leads from tiny companies
MIN_EMPLOYEES = 30
MAX_EMPLOYEES = 200


# ---------------------------------------------------------------------------
# Google Drive helpers
# ---------------------------------------------------------------------------

def _get_drive_service():
    """Build and return a Google Drive API service client."""
    import json as _json
    sa_value = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json")
    if sa_value.strip().startswith("{"):
        sa_info = _json.loads(sa_value)
        creds   = Credentials.from_service_account_info(sa_info, scopes=SCOPES)
    else:
        creds = Credentials.from_service_account_file(sa_value, scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def _list_csv_files(service, folder_id: str) -> list[dict]:
    """List all CSV files in a Drive folder."""
    try:
        results = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false",
            fields="files(id, name)",
        ).execute()
        return results.get("files", [])
    except Exception as e:
        logger.warning("Failed to list files in folder %s: %s", folder_id, e)
        return []


def _download_csv(service, file_id: str) -> str | None:
    """Download a CSV file from Drive and return its content as a string."""
    try:
        request  = service.files().get_media(fileId=file_id)
        buffer   = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buffer.getvalue().decode("utf-8-sig")  # utf-8-sig strips BOM
    except Exception as e:
        logger.warning("Failed to download file %s: %s", file_id, e)
        return None


def _delete_file(service, file_id: str, file_name: str):
    """Delete a file from Drive after successful processing."""
    try:
        service.files().delete(fileId=file_id).execute()
        logger.info("Deleted processed file: %s", file_name)
    except Exception as e:
        logger.warning("Failed to delete file %s: %s", file_name, e)


# ---------------------------------------------------------------------------
# Domain cleaning
# ---------------------------------------------------------------------------

def _clean_domain(raw: str) -> str:
    """Strip protocol, www, and trailing slashes from a URL to get clean domain."""
    if not raw:
        return ""
    domain = raw.strip().lower()
    domain = re.sub(r"^https?://", "", domain)
    domain = re.sub(r"^www\.", "", domain)
    domain = domain.rstrip("/").split("/")[0]  # take only the domain part
    return domain


def _parse_employee_count(raw: str) -> int | None:
    """Parse employee count from strings like '51-200', '200', '1,234'."""
    if not raw:
        return None
    raw = raw.replace(",", "").strip()
    # Handle ranges like "51-200" or "51 - 200"
    match = re.search(r"(\d+)\s*[-–]\s*(\d+)", raw)
    if match:
        return int(match.group(2))  # use upper bound of range
    match = re.search(r"\d+", raw)
    if match:
        return int(match.group())
    return None


def _size_ok(employee_str: str) -> bool:
    """Returns True if company size is within our target range or unknown."""
    if not employee_str or not employee_str.strip():
        return True  # unknown size — let it through
    count = _parse_employee_count(employee_str)
    if count is None:
        return True
    return MIN_EMPLOYEES <= count <= MAX_EMPLOYEES


# ---------------------------------------------------------------------------
# Apollo CSV parser
# ---------------------------------------------------------------------------

def _parse_apollo_row(row: dict) -> dict | None:
    """Parse one row from an Apollo CSV export into a lead dict."""
    first_name = row.get("First Name", "").strip()
    last_name  = row.get("Last Name",  "").strip()
    title      = row.get("Title",      "").strip()
    company    = row.get("Company Name", "").strip()
    website    = row.get("Website",    "").strip()
    email      = row.get("Email",      "").strip()
    email_status = row.get("Email Status", "").strip().lower()
    employees  = row.get("# Employees", "").strip()
    industry   = row.get("Industry",   "").strip()

    if not first_name or not title:
        return None

    if not should_extract_lead(title):
        logger.debug("Apollo title filtered: %s", title)
        return None

    if not _size_ok(employees):
        logger.debug("Apollo company size out of range: %s (%s)", company, employees)
        return None

    domain = _clean_domain(website)

    # Map Apollo email status to our verification_result
    if email_status in ("verified", "valid"):
        verification_result = "valid"
    elif email and email_status not in ("invalid", "bounced"):
        verification_result = "unverified"
    else:
        verification_result = ""
        email = ""  # don't use invalid emails

    classification = classify_title(title)
    normalized_industry = normalize_industry(industry) if industry else ""

    return {
        "first_name":          first_name,
        "last_name":           last_name,
        "title":               title,
        "company":             company,
        "domain":              domain,
        "email":               email,
        "verification_result": verification_result,
        "industry":            normalized_industry,
        "source":              "apollo_csv",
        "role_level":          classification["role_level"],
        "role_context":        classification["role_context"],
        "confidence":          classification["confidence"],
        "needs_review":        classification["needs_review"],
    }


# ---------------------------------------------------------------------------
# Hunter CSV parser
# ---------------------------------------------------------------------------

def _parse_hunter_row(row: dict) -> dict | None:
    """Parse one row from a Hunter CSV export into a lead dict."""
    first_name   = row.get("First name",   "").strip()
    last_name    = row.get("Last name",    "").strip()
    title        = row.get("Job title",    "").strip()
    company      = row.get("Company",      "").strip()
    website      = row.get("Website",      "").strip()
    email        = row.get("Email address","").strip()
    verification = row.get("Verification status", "").strip().lower()
    employees    = row.get("Company size", "").strip()
    industry     = row.get("Industry",     "").strip()

    if not first_name or not title:
        return None

    if not should_extract_lead(title):
        logger.debug("Hunter title filtered: %s", title)
        return None

    if not _size_ok(employees):
        logger.debug("Hunter company size out of range: %s (%s)", company, employees)
        return None

    domain = _clean_domain(website)

    verification_result = "valid" if verification == "valid" else (
        "unverified" if email else ""
    )

    classification = classify_title(title)
    normalized_industry = normalize_industry(industry) if industry else ""

    return {
        "first_name":          first_name,
        "last_name":           last_name,
        "title":               title,
        "company":             company,
        "domain":              domain,
        "email":               email,
        "verification_result": verification_result,
        "industry":            normalized_industry,
        "source":              "hunter_csv",
        "role_level":          classification["role_level"],
        "role_context":        classification["role_context"],
        "confidence":          classification["confidence"],
        "needs_review":        classification["needs_review"],
    }


# ---------------------------------------------------------------------------
# CSV processing
# ---------------------------------------------------------------------------

def _parse_csv_content(content: str, source: str) -> list[dict]:
    """Parse CSV content string into a list of lead dicts."""
    leads  = []
    reader = csv.DictReader(io.StringIO(content))

    parser = _parse_apollo_row if source == "apollo" else _parse_hunter_row

    for row in reader:
        lead = parser(row)
        if lead:
            lead = assign_status(lead)
            leads.append(lead)

    return leads


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def ingest_csvs(dry_run: bool = False) -> dict:
    """
    Check Drive folders for new CSVs, parse them, write to sheet, delete files.

    Returns summary dict with counts per source.
    """
    hunter_folder_id = os.getenv("HUNTER_IMPORT_FOLDER_ID")
    apollo_folder_id = os.getenv("APOLLO_IMPORT_FOLDER_ID")

    if not hunter_folder_id and not apollo_folder_id:
        logger.warning(
            "Neither HUNTER_IMPORT_FOLDER_ID nor APOLLO_IMPORT_FOLDER_ID set — "
            "skipping CSV ingestor"
        )
        return {}

    service = _get_drive_service()
    summary = {}

    for source, folder_id in [("hunter", hunter_folder_id), ("apollo", apollo_folder_id)]:
        if not folder_id:
            continue

        files = _list_csv_files(service, folder_id)
        if not files:
            logger.info("No CSV files found in %s folder", source)
            continue

        logger.info("Found %d CSV file(s) in %s folder", len(files), source)
        all_leads = []

        for file in files:
            logger.info("Processing %s: %s", source, file["name"])
            content = _download_csv(service, file["id"])
            if not content:
                continue

            leads = _parse_csv_content(content, source)
            logger.info("  Parsed %d qualifying leads from %s", len(leads), file["name"])
            all_leads.extend(leads)

            # Delete file after successful parse
            if not dry_run:
                _delete_file(service, file["id"], file["name"])

        if all_leads:
            result = write_leads(all_leads, dry_run=dry_run)
            summary[source] = result
            logger.info(
                "%s CSV ingest: %d inserted, %d duplicates",
                source, result["inserted"], result["skipped_duplicate"],
            )

    return summary


# ---------------------------------------------------------------------------
# Smoke test — python3 csv_ingestor.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    print("CSV Ingestor smoke test\n")
    print("Checking for CSV files in Drive folders...")
    print("(Set HUNTER_IMPORT_FOLDER_ID and APOLLO_IMPORT_FOLDER_ID in .env first)\n")

    result = ingest_csvs(dry_run=False)  # set to True to preview without writing or deleting

    if not result:
        print("No CSV files found or folder IDs not set.")
        print("\nSetup steps:")
        print("  1. Create 'Lead Imports/hunter' and 'Lead Imports/apollo' folders in Google Drive")
        print("  2. Share both folders with your service account email")
        print("  3. Get folder IDs from the URL: drive.google.com/drive/folders/FOLDER_ID_HERE")
        print("  4. Add to .env:")
        print("     HUNTER_IMPORT_FOLDER_ID=your_hunter_folder_id")
        print("     APOLLO_IMPORT_FOLDER_ID=your_apollo_folder_id")
    else:
        for source, summary in result.items():
            print(f"{source}: would insert {summary['inserted']}, skip {summary['skipped_duplicate']}")