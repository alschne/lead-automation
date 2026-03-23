"""
hunter_enrichment.py

Uses Hunter.io Domain Search API to find decision-makers at company domains
where the team page scraper returned nothing.

Free tier: 25 searches/month, up to 10 contacts per domain.
We spend searches only on domains where the scraper already failed — maximizing
the value of each credit.

Each lead dict returned:
    {
        "first_name":          str,
        "last_name":           str,
        "title":               str,
        "email":               str,
        "verification_result": str,   # "valid" | "unverified"
        "company":             str,
        "domain":              str,
        "source":              "hunter",
        "source_url":          str,
    }
"""

import os
import logging
import requests
from dotenv import load_dotenv

from title_classifier import classify_title, should_extract_lead

load_dotenv()

logger = logging.getLogger(__name__)

HUNTER_API_BASE = "https://api.hunter.io/v2"
REQUEST_TIMEOUT = 10

# Seniority rank for picking best lead per domain (lower = better)
_SENIORITY_RANK = {
    "executive":  0,
    "director":   1,
    "manager":    2,
    "senior":     3,
    "junior":     4,
    "":           5,
}

# Hunter department values that suggest HR/People roles
_HR_DEPARTMENTS = {
    "human_resources", "hr", "people", "talent", "recruiting",
}


def _get_api_key() -> str | None:
    return os.getenv("HUNTER_API_KEY")


def _check_remaining_credits() -> int | None:
    """
    Check how many search credits remain this month.
    Returns remaining credits or None on failure.
    Free — does not consume a credit.
    """
    api_key = _get_api_key()
    if not api_key:
        return None
    try:
        resp = requests.get(
            f"{HUNTER_API_BASE}/account",
            params={"api_key": api_key},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        requests_obj = data.get("data", {}).get("requests", {})
        searches      = requests_obj.get("searches", {})
        remaining     = searches.get("available", 0) - searches.get("used", 0)
        logger.info("Hunter credits remaining: %d", remaining)
        return remaining
    except Exception as e:
        logger.warning("Failed to check Hunter credits: %s", e)
        return None


def get_credit_usage() -> dict | None:
    """
    Get Hunter credit usage details for the notification email.
    Returns dict with used, available, remaining or None on failure.
    Free — does not consume a credit.
    """
    api_key = _get_api_key()
    if not api_key:
        return None
    try:
        resp = requests.get(
            f"{HUNTER_API_BASE}/account",
            params={"api_key": api_key},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data          = resp.json()
        requests_obj  = data.get("data", {}).get("requests", {})
        searches      = requests_obj.get("searches", {})
        used          = searches.get("used", 0)
        available     = searches.get("available", 0)
        return {
            "used":      used,
            "available": available,
            "remaining": available - used,
        }
    except Exception as e:
        logger.warning("Failed to fetch Hunter credit usage: %s", e)
        return None


def domain_search(domain: str) -> list[dict]:
    """
    Search Hunter for all known contacts at a domain.
    Returns a list of raw contact dicts from Hunter's API.
    Returns empty list if no results or API error.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.warning("HUNTER_API_KEY not set — skipping Hunter enrichment")
        return []

    try:
        resp = requests.get(
            f"{HUNTER_API_BASE}/domain-search",
            params={
                "domain":  domain,
                "api_key": api_key,
                "limit":   10,   # free tier max
                "type":    "personal",  # skip generic role emails
            },
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code == 429:
            logger.warning("Hunter rate limit hit for %s", domain)
            return []

        if resp.status_code == 400:
            errors = resp.json().get("errors", [])
            logger.warning("Hunter API error for %s: %s", domain, errors)
            return []

        resp.raise_for_status()
        data     = resp.json()
        emails   = data.get("data", {}).get("emails", [])
        logger.info("Hunter: found %d contacts for %s", len(emails), domain)
        return emails

    except Exception as e:
        logger.warning("Hunter domain search failed for %s: %s", domain, e)
        return []


def _score_contact(contact: dict) -> tuple:
    """
    Score a contact for lead quality. Lower = better.
    Priority: HR/People department > seniority > confidence.
    """
    title      = contact.get("position", "") or ""
    seniority  = contact.get("seniority", "") or ""
    department = contact.get("department", "") or ""
    confidence = contact.get("confidence", 0) or 0

    # Prefer HR/People department
    is_hr_dept = department.lower() in _HR_DEPARTMENTS

    # Check title against our classifier
    passes_filter = should_extract_lead(title) if title else False

    return (
        0 if is_hr_dept else 1,                         # HR dept first
        0 if passes_filter else 1,                       # passes title filter
        _SENIORITY_RANK.get(seniority.lower(), 5),       # seniority rank
        100 - confidence,                                 # higher confidence = lower score
    )


def enrich_domain(domain: str, company: str = "") -> dict | None:
    """
    Find the best lead for a domain using Hunter's Domain Search API.

    Runs title classifier on each result and returns the highest-quality
    lead that passes the should_extract_lead() filter.

    Returns a lead dict or None if nothing useful found.
    Consumes 1 Hunter search credit.
    """
    contacts = domain_search(domain)
    if not contacts:
        return None

    # Score and sort all contacts
    scored = sorted(contacts, key=_score_contact)

    for contact in scored:
        title = contact.get("position", "") or ""
        if not title:
            continue

        # Run through our title pre-filter
        if not should_extract_lead(title):
            logger.debug("Hunter contact filtered out: %s at %s", title, domain)
            continue

        # Classify title
        classification = classify_title(title)

        first_name = contact.get("first_name", "") or ""
        last_name  = contact.get("last_name",  "") or ""
        email      = contact.get("value",      "") or ""
        confidence = contact.get("confidence", 0)

        if not first_name:
            continue

        # Determine verification result
        verification = contact.get("verification", {}) or {}
        ver_status   = verification.get("status", "")
        if ver_status == "valid":
            verification_result = "valid"
        elif confidence >= 80:
            verification_result = "valid"
        else:
            verification_result = "unverified"

        lead = {
            "first_name":          first_name,
            "last_name":           last_name,
            "title":               title,
            "email":               email,
            "verification_result": verification_result,
            "company":             company or domain.split(".")[0].replace("-", " ").title(),
            "domain":              domain,
            "source":              "hunter",
            "source_url":          f"https://hunter.io/domain-search/{domain}",
            "role_level":          classification["role_level"],
            "role_context":        classification["role_context"],
            "confidence":          classification["confidence"],
            "needs_review":        classification["needs_review"],
        }

        logger.info(
            "Hunter lead: %s %s — %s (%s) confidence=%d%%",
            first_name, last_name, title, domain, confidence,
        )
        return lead

    logger.info("Hunter: no qualifying leads found for %s", domain)
    return None


def enrich_failed_domains(
    failed_domains: list[tuple[str, str]],
    max_searches: int | None = None,
) -> list[dict]:
    """
    Run Hunter enrichment on domains where the scraper returned nothing.

    Args:
        failed_domains: List of (domain, company_name) tuples
        max_searches:   Cap on Hunter API calls. None = use all available credits.

    Returns:
        List of lead dicts found via Hunter.
    """
    if not failed_domains:
        return []

    api_key = _get_api_key()
    if not api_key:
        logger.warning("HUNTER_API_KEY not set — skipping Hunter enrichment")
        return []

    # Check available credits
    remaining = _check_remaining_credits()
    if remaining is not None and remaining <= 0:
        logger.warning("No Hunter search credits remaining this month")
        return []

    # Apply cap
    if max_searches is not None:
        limit = max_searches
    elif remaining is not None:
        limit = remaining
    else:
        limit = 25  # conservative default

    domains_to_search = failed_domains[:limit]
    logger.info(
        "Hunter enrichment: searching %d domains (limit=%d, credits_remaining=%s)",
        len(domains_to_search), limit, remaining,
    )

    leads = []
    for domain, company in domains_to_search:
        lead = enrich_domain(domain, company)
        if lead:
            leads.append(lead)

    logger.info("Hunter enrichment complete: %d leads found from %d domains",
                len(leads), len(domains_to_search))
    return leads


# ---------------------------------------------------------------------------
# Smoke test — python3 hunter_enrichment.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    api_key = _get_api_key()
    if not api_key:
        print("ERROR: HUNTER_API_KEY not set in .env")
        sys.exit(1)

    # Check credits first
    remaining = _check_remaining_credits()
    print(f"Hunter credits remaining: {remaining}")

    if remaining is not None and remaining <= 0:
        print("No credits remaining — cannot run smoke test")
        sys.exit(0)

    # Test with one domain — uses 1 credit
    test_domain  = "mbausa.org"
    test_company = "MBA USA"

    print(f"\nSearching Hunter for {test_domain}...")
    lead = enrich_domain(test_domain, test_company)

    if lead:
        print(f"\nFound lead:")
        print(f"  Name:       {lead['first_name']} {lead['last_name']}")
        print(f"  Title:      {lead['title']}")
        print(f"  Email:      {lead['email']}")
        print(f"  role_level: {lead['role_level']}")
        print(f"  confidence: {lead['confidence']}")
    else:
        print("No qualifying lead found for this domain")
        print("(This is normal if Hunter has no data for this domain)")