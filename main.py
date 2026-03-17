"""
main.py

Orchestration for the lead discovery pipeline.

Flow:
    1. Discover companies via HackerNews Who's Hiring thread
    2. For each company domain, scrape team page for best lead
    3. Normalize industry via Gemini fallback if needed
    4. Run confidence gate to assign status
    5. Write all leads to Google Sheet (dedup handled in sheet_writer)
    6. Send daily notification email (always sends)

Run locally:
    python3 main.py

Run on Cloud Run:
    Triggered by Cloud Scheduler at 0 21 * * * (9pm Mountain daily)
"""

import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Logging — console only (Cloud Run captures stdout into Cloud Logging)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("main")

# Suppress noisy third-party loggers
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("gspread").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Imports (after logging setup)
# ---------------------------------------------------------------------------

from hackernews_discovery  import discover_companies as hn_discover
from csv_ingestor          import ingest_csvs
from team_page_scraper     import scrape_team_page
from hunter_enrichment     import enrich_failed_domains
from industry_normalizer   import normalize_industry
from confidence_gate       import assign_status, gate_leads
from sheet_writer          import write_leads
from notifier              import send_review_notification


# ---------------------------------------------------------------------------
# Gemini client (optional — used for industry normalization fallback)
# ---------------------------------------------------------------------------

def _get_gemini_client():
    """Initialize Gemini client if API key is available."""
    try:
        import google.generativeai as genai
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            logger.warning("GEMINI_API_KEY not set — industry normalization will use rules only")
            return None
        genai.configure(api_key=api_key)
        return genai
    except ImportError:
        logger.warning("google-generativeai not installed — Gemini fallback unavailable")
        return None


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def _batch_infer_industries(
    companies: list[tuple[str, str]],
    gemini_client,
) -> list[str]:
    """
    Infer industries for a list of (company, domain) tuples in one Gemini call.
    Returns a list of industry strings in the same order as input.
    Falls back to empty strings if Gemini is unavailable.
    """
    import json
    import re
    from industry_normalizer import normalize_industry

    if not gemini_client or not companies:
        return [""] * len(companies)

    company_lines = "\n".join(
        f"{i+1}. Company: {company}, Domain: {domain}"
        for i, (company, domain) in enumerate(companies)
    )

    prompt = (
        f"For each company below, determine what industry they are in.\n\n"
        f"{company_lines}\n\n"
        f"Reply ONLY with a JSON array of objects in this exact format, one per company, in the same order:\n"
        f'[{{"index": 1, "industry": "software"}}, {{"index": 2, "industry": "fintech"}}]\n\n'
        f"Rules: lowercase only, 1-4 words, plain English. "
        f"Examples: software, it services, legal services, cybersecurity, consulting, marketing, fintech. "
        f"No explanation, no markdown, no code fences. Just the raw JSON array."
    )

    try:
        model    = gemini_client.GenerativeModel("gemini-flash-latest")
        response = model.generate_content(prompt)
        raw      = response.text.strip()
        raw      = re.sub(r"```json|```", "", raw).strip()
        results  = json.loads(raw)

        output = [""] * len(companies)
        for item in results:
            idx      = item.get("index", 0) - 1
            industry = item.get("industry", "").strip().lower()
            if 0 <= idx < len(companies):
                normalized  = normalize_industry(industry)
                output[idx] = normalized if normalized else industry
        return output

    except Exception as e:
        logger.warning("Batch industry inference failed: %s", e)
        return [""] * len(companies)
def run_pipeline() -> dict:
    """
    Run the full lead discovery pipeline.

    Returns a summary dict with pipeline stats.
    """
    logger.info("=" * 55)
    logger.info("Lead Discovery Pipeline starting")
    logger.info("=" * 55)

    # ------------------------------------------------------------------
    # Step 0: Ingest any CSV files dropped in Google Drive folders
    # ------------------------------------------------------------------
    logger.info("Step 0: Checking for CSV imports (Hunter/Apollo)...")
    try:
        csv_summary = ingest_csvs()
        for source, result in csv_summary.items():
            logger.info(
                "CSV ingest (%s): %d inserted, %d duplicates skipped",
                source, result.get("inserted", 0), result.get("skipped_duplicate", 0),
            )
    except Exception as e:
        logger.warning("CSV ingest failed: %s — continuing pipeline", e)

    gemini = _get_gemini_client()
    stats  = {
        "companies_discovered": 0,
        "domains_scraped":      0,
        "leads_found":          0,
        "leads_inserted":       0,
        "leads_duplicate":      0,
        "leads_needs_review":   0,
        "leads_ready":          0,
        "errors":               0,
    }

    # ------------------------------------------------------------------
    # Step 1: Discover companies
    # ------------------------------------------------------------------
    logger.info("Step 1: Discovering companies via HackerNews...")
    try:
        companies = hn_discover()
        stats["companies_discovered"] = len(companies)
        logger.info("Discovered %d companies", len(companies))
    except Exception as e:
        logger.error("HackerNews discovery failed: %s", e)
        stats["errors"] += 1
        companies = []

    if not companies:
        logger.warning("No companies discovered — aborting pipeline")
        _send_notification([], {
            "total": 0, "inserted": 0,
            "skipped_duplicate": 0, "skipped_missing_name": 0,
        })
        return stats

    # ------------------------------------------------------------------
    # Step 2: Scrape team pages + enrich leads (concurrent)
    # ------------------------------------------------------------------
    logger.info("Step 2: Scraping team pages for %d companies...", len(companies))

    from concurrent.futures import ThreadPoolExecutor, as_completed

    MAX_WORKERS = 5  # concurrent domains — safe for Playwright + network

    def _process_company(args):
        """Process a single company — scrape, enrich, classify."""
        i, company = args
        domain = company.get("domain")
        if not domain:
            return None, "no_domain"

        logger.info("[%d/%d] Scraping %s...", i, len(companies), domain)
        try:
            lead = scrape_team_page(domain)
        except Exception as e:
            logger.warning("Scrape failed for %s: %s", domain, e)
            return None, "error"

        if not lead:
            return None, "no_lead"

        # Enrich
        lead["company"]    = lead.get("company") or company.get("company", "")
        lead["source"]     = company.get("source", "unknown")
        lead["source_url"] = company.get("source_url", "")

        raw_industry = company.get("industry", "")
        if raw_industry:
            lead["industry"] = normalize_industry(raw_industry, gemini)
        else:
            lead["industry"] = ""  # filled in batch step after scraping

        lead = assign_status(lead)
        return lead, "ok"

    all_leads      = []
    ready_leads    = []
    review_leads   = []
    failed_domains: list[tuple[str, str]] = []  # (domain, company) where scraper found nothing

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_process_company, (i, company)): company
            for i, company in enumerate(companies, start=1)
        }
        for future in as_completed(futures):
            stats["domains_scraped"] += 1
            try:
                lead, status = future.result()
            except Exception as e:
                logger.warning("Unexpected error in worker: %s", e)
                stats["errors"] += 1
                continue

            if status == "error":
                stats["errors"] += 1
            if not lead:
                # Track for Hunter enrichment fallback
                company_name = futures[future].get("company", "") if future in futures else ""
                domain_val = futures[future].get("domain", "") if future in futures else ""
                if domain_val:
                    failed_domains.append((domain_val, company_name))
                continue

            stats["leads_found"] += 1
            all_leads.append(lead)

            if lead["status"] == "ready_to_send":
                ready_leads.append(lead)
                logger.info(
                    "  READY: %s %s — %s (%s)",
                    lead["first_name"], lead["last_name"],
                    lead["title"], lead["role_level"],
                )
            else:
                review_leads.append(lead)
                logger.info(
                    "  REVIEW: %s %s — %s (confidence=%s)",
                    lead["first_name"], lead["last_name"],
                    lead["title"], lead["confidence"],
                )

    stats["leads_ready"]        = len(ready_leads)
    stats["leads_needs_review"] = len(review_leads)

    logger.info(
        "Scraping complete: %d leads found (%d ready, %d needs review)",
        len(all_leads), len(ready_leads), len(review_leads),
    )

    # ------------------------------------------------------------------
    # Step 2b: Hunter enrichment for domains where scraper failed
    # ------------------------------------------------------------------
    if failed_domains:
        # Filter out domains already in the sheet — don't spend Hunter credits on them
        from sheet_writer import _get_sheet_client, _get_leads_worksheet
        try:
            client    = _get_sheet_client()
            worksheet = _get_leads_worksheet(client)
            existing_records = worksheet.get_all_records()
            existing_domains = {
                str(r.get("domain", "")).lower().strip()
                for r in existing_records
                if r.get("domain")
            }
            fresh_failed = [
                (d, c) for d, c in failed_domains
                if d.lower().strip() not in existing_domains
            ]
            logger.info(
                "Step 2b: Hunter enrichment for %d domains (%d skipped — already in sheet)...",
                len(fresh_failed), len(failed_domains) - len(fresh_failed),
            )
            failed_domains = fresh_failed
        except Exception as e:
            logger.warning("Could not pre-filter Hunter domains against sheet: %s — proceeding anyway", e)
            fresh_failed = failed_domains

        hunter_leads = enrich_failed_domains(failed_domains)
        for lead in hunter_leads:
            lead["source"]     = "hunter"
            lead["lead_source"] = "hunter"
            lead = assign_status(lead)
            all_leads.append(lead)
            if lead["status"] == "ready_to_send":
                ready_leads.append(lead)
            else:
                review_leads.append(lead)
            stats["leads_found"] += 1
        logger.info("Hunter enrichment added %d leads", len(hunter_leads))

    # ------------------------------------------------------------------
    # Step 2c: Batch infer industries for leads missing it
    # ------------------------------------------------------------------
    needs_industry = [l for l in all_leads if not l.get("industry")]
    if needs_industry and gemini:
        logger.info("Step 2c: Inferring industry for %d leads...", len(needs_industry))
        company_domain_pairs = [
            (l.get("company", ""), l.get("domain", ""))
            for l in needs_industry
        ]
        industries = _batch_infer_industries(company_domain_pairs, gemini)
        for lead, industry in zip(needs_industry, industries):
            lead["industry"] = industry
        logger.info("Industry inference complete")
    elif needs_industry:
        logger.warning("Gemini unavailable — industry will be blank for %d leads", len(needs_industry))

    # ------------------------------------------------------------------
    # Step 3: Write to Google Sheet
    # ------------------------------------------------------------------
    logger.info("Step 3: Writing %d leads to Google Sheet...", len(all_leads))

    write_summary = {
        "total": 0, "inserted": 0,
        "skipped_duplicate": 0, "skipped_missing_name": 0,
    }

    if all_leads:
        try:
            write_summary = write_leads(all_leads)
            stats["leads_inserted"]  = write_summary["inserted"]
            stats["leads_duplicate"] = write_summary["skipped_duplicate"]
            logger.info(
                "Sheet write complete: %d inserted, %d duplicates skipped",
                write_summary["inserted"], write_summary["skipped_duplicate"],
            )
        except Exception as e:
            logger.error("Sheet write failed: %s", e)
            stats["errors"] += 1
    else:
        logger.info("No leads to write")

    # ------------------------------------------------------------------
    # Step 4: Send notification email
    # ------------------------------------------------------------------
    logger.info("Step 4: Sending notification email...")
    _send_notification(review_leads, write_summary)

    # ------------------------------------------------------------------
    # Final summary
    # ------------------------------------------------------------------
    logger.info("=" * 55)
    logger.info("Pipeline complete")
    logger.info("  Companies discovered: %d", stats["companies_discovered"])
    logger.info("  Domains scraped:      %d", stats["domains_scraped"])
    logger.info("  Leads found:          %d", stats["leads_found"])
    logger.info("  Leads inserted:       %d", stats["leads_inserted"])
    logger.info("  Duplicates skipped:   %d", stats["leads_duplicate"])
    logger.info("  Ready to send:        %d", stats["leads_ready"])
    logger.info("  Needs review:         %d", stats["leads_needs_review"])
    logger.info("  Errors:               %d", stats["errors"])
    logger.info("=" * 55)

    return stats


def _send_notification(review_leads: list[dict], write_summary: dict):
    """Send daily notification email — always fires."""
    try:
        sent = send_review_notification(review_leads, write_summary)
        if sent:
            logger.info("Notification email sent")
        else:
            logger.warning("Notification email failed to send")
    except Exception as e:
        logger.error("Notification error: %s", e)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    stats = run_pipeline()
    # Exit with error code if pipeline had errors — useful for Cloud Run monitoring
    sys.exit(1 if stats["errors"] > 0 and stats["leads_found"] == 0 else 0)