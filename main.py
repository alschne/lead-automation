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

from hackernews_discovery import discover_companies as hn_discover
from team_page_scraper    import scrape_team_page
from industry_normalizer  import normalize_industry
from confidence_gate      import assign_status, gate_leads
from sheet_writer         import write_leads
from notifier             import send_review_notification


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

def _infer_industry(company: str, domain: str, gemini_client) -> str:
    """
    Infer industry from company name and domain using Gemini.
    Falls back to empty string if Gemini is unavailable.
    """
    if not gemini_client:
        return ""
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
        logger.debug("Industry inference failed for %s: %s", company, e)
        return ""


def run_pipeline() -> dict:
    """
    Run the full lead discovery pipeline.

    Returns a summary dict with pipeline stats.
    """
    logger.info("=" * 55)
    logger.info("Lead Discovery Pipeline starting")
    logger.info("=" * 55)

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
            lead["industry"] = _infer_industry(
                lead.get("company", ""),
                lead.get("domain",  ""),
                gemini,
            )

        lead = assign_status(lead)
        return lead, "ok"

    all_leads    = []
    ready_leads  = []
    review_leads = []

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