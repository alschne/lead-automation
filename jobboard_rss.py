"""
jobboard_rss.py

Discovers companies actively hiring by scraping public job board listings
across multiple ATS platforms.

We are NOT filtering by job title — any company actively hiring is a signal
they are growing and may need compensation analytics help.

Supported platforms:
    - Greenhouse (boards.greenhouse.io)
    - Lever (jobs.lever.co)
    - Ashby (jobs.ashbyhq.com)
    - Workable (apply.workable.com)
    - BambooHR (hire.tss.bamboohr.com)
    - Recruitee (recruitee.com)
    - Teamtailor (careers.teamtailor.com)
    - Breezy HR (breezy.hr)
    - JazzHR (app.jazz.hr)

Each company dict:
    {
        "company":    str,
        "domain":     str | None,
        "source":     "jobboard",
        "source_url": str,
        "ats":        str,   # which ATS platform
    }
"""

import re
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 10
REQUEST_DELAY   = 1.0
USER_AGENT      = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}

# ---------------------------------------------------------------------------
# Domain extraction helpers
# ---------------------------------------------------------------------------

# Domains that are ATS platforms themselves — not company domains
ATS_DOMAINS = {
    "greenhouse.io", "lever.co", "ashbyhq.com", "workable.com",
    "bamboohr.com", "recruitee.com", "teamtailor.com", "breezy.hr",
    "jazz.hr", "pinpointhq.com", "rippling.com", "dover.com",
    "myworkdayjobs.com", "smartrecruiters.com", "icims.com",
}

SKIP_DOMAINS = {
    "linkedin.com", "twitter.com", "x.com", "facebook.com",
    "instagram.com", "youtube.com", "github.com",
    "google.com", "notion.so", "notion.site",
    "netlify.app", "vercel.app", "webflow.io",
}


def _clean_domain(url: str) -> str | None:
    """Extract and clean a domain from a URL string."""
    if not url:
        return None
    try:
        if not url.startswith("http"):
            url = "https://" + url
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        domain = re.sub(r'^www\.', '', domain)
        if not domain or "." not in domain:
            return None
        if domain in ATS_DOMAINS or domain in SKIP_DOMAINS:
            return None
        # Strip subdomains
        parts = domain.split(".")
        if len(parts) > 2:
            if parts[-2] in {"com", "co", "org", "net", "gov"} and len(parts[-1]) == 2:
                domain = ".".join(parts[-3:])
            else:
                domain = ".".join(parts[-2:])
        return domain
    except Exception:
        return None


def _make_company(name: str, domain: str | None, source_url: str, ats: str) -> dict:
    """Build a company dict."""
    if not domain and name:
        domain = None  # enrichment layer will try to find it
    return {
        "company":    name.strip(),
        "domain":     domain,
        "source":     "jobboard",
        "source_url": source_url,
        "ats":        ats,
    }


# ---------------------------------------------------------------------------
# Platform scrapers
# ---------------------------------------------------------------------------

def _scrape_greenhouse(max_companies: int = 50) -> list[dict]:
    """
    Greenhouse publishes a public company list at boards.greenhouse.io.
    Each company has a subdomain: boards.greenhouse.io/{company_token}
    We use their sitemap to discover active companies.
    """
    companies = []
    url = "https://boards.greenhouse.io/sitemap.xml"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("Greenhouse sitemap returned %s", resp.status_code)
            return []
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.text for loc in soup.find_all("loc")]
        # Filter to company board URLs (not individual job pages)
        board_urls = [l for l in locs if l.count("/") == 3 and "greenhouse.io" in l]
        logger.info("Greenhouse: found %d company boards", len(board_urls))
        for board_url in board_urls[:max_companies]:
            token = board_url.rstrip("/").split("/")[-1]
            company_name = token.replace("-", " ").title()
            companies.append(_make_company(company_name, None, board_url, "greenhouse"))
        return companies
    except Exception as e:
        logger.warning("Greenhouse scrape failed: %s", e)
        return []


def _scrape_lever(max_companies: int = 50) -> list[dict]:
    """
    Lever job pages follow: jobs.lever.co/{company_token}
    Use their sitemap to discover active companies.
    """
    companies = []
    url = "https://jobs.lever.co/sitemap.xml"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("Lever sitemap returned %s", resp.status_code)
            return []
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.text for loc in soup.find_all("loc")]
        # Company-level URLs have exactly one path segment
        board_urls = [l for l in locs if l.count("/") == 3 and "lever.co" in l]
        seen = set()
        for board_url in board_urls:
            token = board_url.rstrip("/").split("/")[-1]
            if token in seen:
                continue
            seen.add(token)
            company_name = token.replace("-", " ").title()
            companies.append(_make_company(company_name, None, board_url, "lever"))
            if len(companies) >= max_companies:
                break
        logger.info("Lever: found %d companies", len(companies))
        return companies
    except Exception as e:
        logger.warning("Lever scrape failed: %s", e)
        return []


def _scrape_ashby(max_companies: int = 50) -> list[dict]:
    """
    Ashby job pages: jobs.ashbyhq.com/{company_token}
    """
    companies = []
    url = "https://jobs.ashbyhq.com/sitemap.xml"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("Ashby sitemap returned %s", resp.status_code)
            return []
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.text for loc in soup.find_all("loc")]
        board_urls = [l for l in locs if l.count("/") == 3 and "ashbyhq.com" in l]
        seen = set()
        for board_url in board_urls:
            token = board_url.rstrip("/").split("/")[-1]
            if token in seen:
                continue
            seen.add(token)
            company_name = token.replace("-", " ").title()
            companies.append(_make_company(company_name, None, board_url, "ashby"))
            if len(companies) >= max_companies:
                break
        logger.info("Ashby: found %d companies", len(companies))
        return companies
    except Exception as e:
        logger.warning("Ashby scrape failed: %s", e)
        return []


def _scrape_workable(max_companies: int = 50) -> list[dict]:
    """
    Workable job pages: apply.workable.com/{company_token}
    """
    companies = []
    url = "https://apply.workable.com/sitemap.xml"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("Workable sitemap returned %s", resp.status_code)
            return []
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.text for loc in soup.find_all("loc")]
        board_urls = [l for l in locs if l.count("/") == 3 and "workable.com" in l]
        seen = set()
        for board_url in board_urls:
            token = board_url.rstrip("/").split("/")[-1]
            if token in seen:
                continue
            seen.add(token)
            company_name = token.replace("-", " ").title()
            companies.append(_make_company(company_name, None, board_url, "workable"))
            if len(companies) >= max_companies:
                break
        logger.info("Workable: found %d companies", len(companies))
        return companies
    except Exception as e:
        logger.warning("Workable scrape failed: %s", e)
        return []


def _scrape_breezy(max_companies: int = 50) -> list[dict]:
    """
    Breezy HR: {company}.breezy.hr
    Use their sitemap.
    """
    companies = []
    url = "https://breezy.hr/sitemap.xml"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("Breezy sitemap returned %s", resp.status_code)
            return []
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.text for loc in soup.find_all("loc")]
        seen = set()
        for loc in locs:
            parsed = urlparse(loc)
            subdomain = parsed.netloc.split(".")[0]
            if subdomain in {"www", "breezy", "app"} or subdomain in seen:
                continue
            seen.add(subdomain)
            company_name = subdomain.replace("-", " ").title()
            companies.append(_make_company(company_name, None, loc, "breezy"))
            if len(companies) >= max_companies:
                break
        logger.info("Breezy: found %d companies", len(companies))
        return companies
    except Exception as e:
        logger.warning("Breezy scrape failed: %s", e)
        return []


def _scrape_jazzhr(max_companies: int = 50) -> list[dict]:
    """
    JazzHR: app.jazz.hr/apply/{company_token}
    """
    companies = []
    url = "https://app.jazz.hr/sitemap.xml"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            logger.warning("JazzHR sitemap returned %s", resp.status_code)
            return []
        soup = BeautifulSoup(resp.text, "xml")
        locs = [loc.text for loc in soup.find_all("loc")]
        seen = set()
        for loc in locs:
            parts = loc.rstrip("/").split("/")
            if len(parts) >= 5:
                token = parts[4]
                if token in seen:
                    continue
                seen.add(token)
                company_name = token.replace("-", " ").title()
                companies.append(_make_company(company_name, None, loc, "jazzhr"))
                if len(companies) >= max_companies:
                    break
        logger.info("JazzHR: found %d companies", len(companies))
        return companies
    except Exception as e:
        logger.warning("JazzHR scrape failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

# Scraper registry
SCRAPERS = {
    "greenhouse": _scrape_greenhouse,
    "lever":      _scrape_lever,
    "ashby":      _scrape_ashby,
    "workable":   _scrape_workable,
    "breezy":     _scrape_breezy,
    "jazzhr":     _scrape_jazzhr,
}


def discover_companies(
    platforms: list[str] | None = None,
    max_per_platform: int = 50,
) -> list[dict]:
    """
    Run job board discovery across all (or specified) platforms.

    Args:
        platforms:        List of platform keys to run. None = all platforms.
        max_per_platform: Max companies to pull per platform.

    Returns:
        Deduplicated list of company dicts.
    """
    platforms = platforms or list(SCRAPERS.keys())
    all_companies = []
    seen_companies: set[str] = set()  # dedup by company name token

    for platform in platforms:
        scraper = SCRAPERS.get(platform)
        if not scraper:
            logger.warning("Unknown platform: %s", platform)
            continue
        logger.info("Running %s scraper...", platform)
        try:
            companies = scraper(max_per_platform)
            for company in companies:
                key = company["company"].lower().replace(" ", "")
                if key not in seen_companies:
                    seen_companies.add(key)
                    all_companies.append(company)
        except Exception as e:
            logger.warning("%s scraper error: %s", platform, e)
        time.sleep(REQUEST_DELAY)

    logger.info("Job board discovery: %d total unique companies", len(all_companies))
    return all_companies


# ---------------------------------------------------------------------------
# Smoke test — python3 jobboard_rss.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    print("Running job board discovery (max 20 per platform)...\n")
    companies = discover_companies(max_per_platform=20)

    # Group by ATS
    by_ats: dict[str, list] = {}
    for c in companies:
        by_ats.setdefault(c["ats"], []).append(c)

    for ats, comps in by_ats.items():
        print(f"\n{ats.upper()} ({len(comps)} companies)")
        print("-" * 50)
        for c in comps[:10]:
            domain = c["domain"] or "(no domain yet)"
            print(f"  {c['company']:<35} {domain}")

    print(f"\nTotal: {len(companies)} companies across {len(by_ats)} platforms")
