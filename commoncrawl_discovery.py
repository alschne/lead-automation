"""
commoncrawl_discovery.py

Uses CommonCrawl's CDX Index API to find team/leadership page URLs
for domains where our team page scraper already struck out.

Role: Enrichment helper, not a discovery engine.

When the scraper tries all 14 standard paths and finds nothing, this module
queries CommonCrawl for all URLs it has indexed for that domain, then checks
if any of them look like team/people/leadership pages. If found, it returns
the URL so the scraper can try that specific path.

CDX API usage:
    One targeted query per domain — fast, no rate limiting issues.
    Format: *.domain.com — returns all indexed URLs for that domain.

Free, no auth required.
"""

import re
import json
import time
import logging
import requests
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

CDX_COLLINFO_URL  = "https://index.commoncrawl.org/collinfo.json"
REQUEST_TIMEOUT   = 15
REQUEST_DELAY     = 5.0    # polite delay — CDX server is rate limited, be patient
URLS_PER_DOMAIN   = 50     # max URLs to fetch per domain lookup

# Required per CommonCrawl FAQ — RFC 9110 compliant User-Agent
CDX_USER_AGENT = "LeadAutomationBot/1.0 (respectful single-threaded crawler; contact: allieroth@alineanalytics.com)"

# Path keywords that suggest a team/people/leadership page
TEAM_PATH_SIGNALS = [
    "team", "leadership", "people", "staff", "about",
    "our-team", "meet-the-team", "directory", "executives",
    "management", "founders", "crew", "who-we-are",
]

# Paths to ignore — not useful for lead extraction
IGNORE_PATHS = {
    "/", "/robots.txt", "/sitemap.xml", "/feed", "/rss",
    "/favicon.ico", "/wp-login.php",
}

# Cache the crawl ID so we don't fetch it on every call
_crawl_id_cache: str | None = None


def _get_latest_crawl_id() -> str | None:
    """Fetch and cache the most recent CommonCrawl crawl ID."""
    global _crawl_id_cache
    if _crawl_id_cache:
        return _crawl_id_cache
    try:
        resp = requests.get(CDX_COLLINFO_URL, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        collections = resp.json()
        if not collections:
            return None
        _crawl_id_cache = collections[0].get("id", "")
        logger.info("CommonCrawl crawl ID: %s", _crawl_id_cache)
        return _crawl_id_cache
    except Exception as e:
        logger.warning("Failed to fetch CommonCrawl crawl ID: %s", e)
        return None


def _looks_like_team_page(url: str) -> bool:
    """
    Returns True if a URL path segment exactly matches a team/people signal word.
    Requires the signal to be a complete path segment — not buried in a longer word.

    /about/          → True  (segment: 'about')
    /leadership/     → True  (segment: 'leadership')
    /our-team/       → True  (segment: 'our-team')
    /events/leadership-panel/  → False  (segment is 'leadership-panel', not 'leadership')
    /blog/building-a-team/     → False  (segment is 'building-a-team')
    """
    try:
        path = urlparse(url).path.lower().strip("/")
        if not path:
            return False

        # Split into path segments only (by /)
        segments = [s for s in path.split("/") if s]

        for segment in segments:
            # Check if this segment exactly matches or is exactly a signal word
            # Allow hyphens: our-team, meet-the-team
            clean = segment.strip("-_")
            if clean in TEAM_PATH_SIGNALS:
                return True
            # Also check hyphenated combos that start with a signal
            # e.g. "our-team" → split by hyphen → ["our", "team"] → "team" matches
            parts = re.split(r"[-_]", clean)
            for part in parts:
                if part in TEAM_PATH_SIGNALS:
                    # Only count it if the segment is SHORT (2-3 words max)
                    # Avoids matching 'team' in long blog-post-about-building-a-team
                    if len(parts) <= 3:
                        return True

        return False
    except Exception:
        return False


def find_team_page_url(domain: str, crawl_id: str | None = None) -> list[str]:
    """
    Query CommonCrawl for all indexed URLs on a domain and find candidate
    team/leadership page URLs, sorted by likelihood (shortest path first).

    Args:
        domain:   Company domain e.g. "reedmfgco.com"
        crawl_id: Specific crawl to use. None = latest.

    Returns:
        List of candidate URL strings, sorted best first. Empty list if none found.
    """
    if not crawl_id:
        crawl_id = _get_latest_crawl_id()
    if not crawl_id:
        logger.warning("No CommonCrawl crawl ID available")
        return None

    cdx_url = f"https://index.commoncrawl.org/{crawl_id}-index"
    params  = {
        "url":    f"*.{domain}",
        "output": "json",
        "limit":  URLS_PER_DOMAIN,
        "filter": "statuscode:200",
        "fl":     "url",
        "collapse": "urlkey",   # deduplicate similar URLs
    }

    try:
        resp = requests.get(cdx_url, params=params, timeout=REQUEST_TIMEOUT,
                              headers={"User-Agent": CDX_USER_AGENT})

        if resp.status_code == 429 or "SlowDown" in resp.text:
            logger.warning("CommonCrawl rate limit for %s — skipping", domain)
            return []

        if resp.status_code == 400:
            logger.debug("CommonCrawl bad request for %s — skipping", domain)
            return []

        if resp.status_code == 404 or "No Captures" in resp.text:
            logger.debug("CommonCrawl has no data for %s", domain)
            return []

        if resp.status_code not in (200, 204):
            logger.warning("CDX returned %s for %s", resp.status_code, domain)
            return []

        # Parse URLs from response
        candidate_urls = []
        for line in resp.text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                url  = data.get("url", "")
                if url and _looks_like_team_page(url):
                    candidate_urls.append(url)
            except Exception:
                continue

        if not candidate_urls:
            logger.debug("No team page URLs found in CommonCrawl for %s", domain)
            return None

        # Sort by path depth — prefer shorter paths (closer to root = more likely to be a team page)
        def path_score(url: str) -> int:
            path = urlparse(url).path
            return len([s for s in path.split("/") if s])

        sorted_urls = sorted(candidate_urls, key=path_score)
        logger.info(
            "CommonCrawl found %d candidate team URLs for %s: %s",
            len(sorted_urls), domain,
            [urlparse(u).path for u in sorted_urls[:3]],
        )
        return sorted_urls  # return all candidates, caller tries each

    except Exception as e:
        logger.warning("CommonCrawl lookup failed for %s: %s", domain, e)
        return []


def enrich_failed_domains(
    failed_domains: list[tuple[str, str]],
    crawl_id: str | None = None,
) -> list[tuple[str, list[str]]]:
    """
    For each domain where the scraper failed, ask CommonCrawl if it knows
    any team page URLs we haven't tried yet.

    Args:
        failed_domains: List of (domain, company_name) tuples
        crawl_id:       Specific crawl to use. None = latest.

    Returns:
        List of (domain, [candidate_urls]) tuples where CommonCrawl found URLs.
        Caller should try each URL in order until a lead is found.
    """
    if not failed_domains:
        return []

    results = []
    for domain, company in failed_domains:
        urls = find_team_page_url(domain, crawl_id)
        if urls:
            results.append((domain, urls))
        time.sleep(REQUEST_DELAY)

    logger.info(
        "CommonCrawl enrichment: %d/%d domains had candidate team page URLs",
        len(results), len(failed_domains),
    )
    return results


# ---------------------------------------------------------------------------
# Smoke test — python3 commoncrawl_discovery.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    print("CommonCrawl enrichment smoke test")
    print("Testing domain lookups on known domains...\n")

    # Test domains — mix of ones that should and shouldn't have team pages
    test_domains = [
        ("mbausa.org",          "MBA USA"),
        ("reedmfgco.com",       "Reed Manufacturing"),
        ("gruntwork.io",        "Gruntwork"),
        ("thewarrencompany.com","The Warren Company"),
        ("sitewire.com",        "Sitewire"),
    ]

    crawl_id = _get_latest_crawl_id()
    print(f"Using crawl: {crawl_id}\n")
    print(f"{'Domain':<30} {'Result'}")
    print("-" * 80)

    for domain, company in test_domains:
        urls = find_team_page_url(domain, crawl_id)
        if urls:
            result = f"{len(urls)} URL(s): {urlparse(urls[0]).path}"
            if len(urls) > 1:
                result += f", {urlparse(urls[1]).path}"
                if len(urls) > 2:
                    result += f" (+{len(urls)-2} more)"
        else:
            result = "No team page found in CommonCrawl"
        print(f"  {domain:<28} {result}")
        time.sleep(REQUEST_DELAY)