"""
team_page_scraper.py

Scrapes company team/leadership pages to extract name + title pairs.

Pipeline:
    1. Check robots.txt — skip domain if disallowed
    2. Try common team page URL patterns via requests + BeautifulSoup
    3. If page looks JS-rendered (empty body), fall back to Playwright
    4. Parse name + title pairs from HTML
    5. Pre-filter titles via should_extract_lead()
    6. Return list of raw lead dicts

Each lead dict:
    {
        "first_name": str,
        "last_name":  str,
        "title":      str,
        "company":    str,
        "domain":     str,
        "source_url": str,
    }
"""

import re
import time
import logging
import urllib.robotparser
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 10          # seconds
REQUEST_DELAY   = 1.5         # seconds between requests to same domain
USER_AGENT      = (
    "Mozilla/5.0 (compatible; LeadDiscoveryBot/1.0; "
    "respectful scraper; contact: your@email.com)"
)
HEADERS = {"User-Agent": USER_AGENT}

# Ordered list of URL path patterns to try
TEAM_PAGE_PATHS = [
    "/team",
    "/about/team",
    "/leadership",
    "/about/leadership",
    "/about-us/team",
    "/about-us/leadership",
    "/people",
    "/about/people",
    "/about",
    "/company/team",
    "/company/leadership",
    "/our-team",
    "/meet-the-team",
    "/staff",
]

# Minimum text content to consider a page non-empty (chars)
MIN_CONTENT_LENGTH = 500

# ---------------------------------------------------------------------------
# Title pre-filter — runs before classify_title()
# Only titles with at least one seniority OR HR/people signal pass
# ---------------------------------------------------------------------------

SENIORITY_SIGNALS = [
    r"\bceo\b", r"\bfounder\b", r"\bowner\b", r"\bpresident\b",
    r"\bchief\b", r"\bvice\s+president\b", r"\bvp\b",
    r"\bdirector\b", r"\bmanaging\s+director\b",
    r"\bpartner\b", r"\bprincipal\b",
    r"\bhead\s+of\b", r"\blead\b", r"\bmanager\b",
]

HR_SIGNALS = [
    r"\b(hr|h\.r\.)\b", r"\bhrbp\b", r"\bhuman\s+resources\b",
    r"\bpeople\b", r"\btalent\b", r"\bcompensation\b",
    r"\btotal\s+rewards\b", r"\brecruiting\b", r"\brecruitment\b",
    r"\bbenefits\b", r"\bworkforce\b", r"\bdiversity\b",
    r"\bdei\b", r"\binclusion\b",
]

ALL_SIGNALS = SENIORITY_SIGNALS + HR_SIGNALS


def should_extract_lead(title: str) -> bool:
    """
    Returns True if the title contains at least one seniority or HR/people signal.
    Titles with no signal (Software Engineer, Account Executive, etc.) return False.
    """
    normalized = title.lower().strip()
    for pattern in ALL_SIGNALS:
        if re.search(pattern, normalized):
            return True
    return False


# ---------------------------------------------------------------------------
# robots.txt check
# ---------------------------------------------------------------------------

def _is_allowed(domain: str, path: str) -> bool:
    """
    Returns True if our user agent is allowed to fetch the given path.
    Fails open (returns True) if robots.txt is unreachable.
    """
    robots_url = f"https://{domain}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
        allowed = rp.can_fetch(USER_AGENT, f"https://{domain}{path}")
        if not allowed:
            logger.info("robots.txt disallows %s%s — skipping", domain, path)
        return allowed
    except Exception as e:
        logger.debug("robots.txt fetch failed for %s: %s — failing open", domain, e)
        return True  # fail open: if we can't read it, proceed


# ---------------------------------------------------------------------------
# Page fetching
# ---------------------------------------------------------------------------

def _fetch_with_requests(url: str) -> tuple[str | None, int | None]:
    """
    Fetch a URL with requests.
    Returns (html, status_code) or (None, status_code) on failure.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        logger.info("HTTP %s for %s", resp.status_code, url)
        if resp.status_code == 200:
            return resp.text, 200
        return None, resp.status_code
    except requests.exceptions.ConnectionError as e:
        logger.warning("ConnectionError for %s: %s", url, e)
        return None, None
    except requests.exceptions.Timeout:
        logger.warning("Timeout for %s", url)
        return None, None
    except Exception as e:
        logger.warning("Unexpected fetch error for %s (%s): %s", url, type(e).__name__, e)
        return None, None


def _looks_js_rendered(html: str) -> bool:
    """
    Heuristic: if the body text is very short after stripping tags,
    the page is likely JS-rendered and needs Playwright.
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    return len(text) < MIN_CONTENT_LENGTH


def _fetch_with_playwright(url: str) -> str | None:
    """
    Fetch a page using Playwright (real browser — bypasses Cloudflare challenges).
    Returns HTML string or None on failure.
    """
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()
            page.goto(url, timeout=REQUEST_TIMEOUT * 2000, wait_until="domcontentloaded")
            # Give JS time to render
            page.wait_for_timeout(2000)
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        logger.warning("Playwright fetch failed for %s: %s", url, e)
        return None


# Cloudflare bot protection indicators
_CF_BLOCK_PHRASES = ["cf-mitigated", "Just a moment", "Enable JavaScript and cookies"]

def _is_cf_blocked(html: str) -> bool:
    return any(phrase in html for phrase in _CF_BLOCK_PHRASES)


def _fetch_page(url: str) -> str | None:
    """
    Try requests first.
    Fall back to Playwright if:
      - non-200 status (e.g. Cloudflare 403 challenge)
      - page looks JS-rendered (empty body)
      - response contains Cloudflare block phrases
    """
    html, status = _fetch_with_requests(url)

    needs_playwright = (
        html is None or
        _looks_js_rendered(html) or
        _is_cf_blocked(html or "")
    )

    if needs_playwright:
        reason = "non-200" if html is None else "JS-rendered or CF-blocked"
        logger.info("Trying Playwright for %s (%s)", url, reason)
        pw_html = _fetch_with_playwright(url)
        if pw_html and not _is_cf_blocked(pw_html):
            return pw_html
        logger.warning("Playwright also failed or still CF-blocked for %s", url)
        return None

    return html


# ---------------------------------------------------------------------------
# Name + title extraction
# ---------------------------------------------------------------------------

def _parse_leads(html: str, domain: str, source_url: str) -> list[dict]:
    """
    Extract name + title pairs from a team page.

    Strategy: look for elements where a name and title appear close together
    in the DOM. Common patterns:
        <h3>Jane Doe</h3><p>VP of People</p>
        <div class="name">Jane Doe</div><div class="title">VP of People</div>
        <p class="team-name">Jane Doe</p><p class="team-role">VP of People</p>

    We use a proximity heuristic: scan all text nodes, pair consecutive
    short strings where the first looks like a name and second looks like a title.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove nav, footer, scripts, styles — noise reduction
    for tag in soup(["nav", "footer", "script", "style", "header"]):
        tag.decompose()

    leads = []

    # Strategy 1: find elements with name-like class attributes
    leads.extend(_strategy_class_hints(soup, domain, source_url))

    # Strategy 2: proximity pairing of short text nodes
    if not leads:
        leads.extend(_strategy_proximity(soup, domain, source_url))

    # Deduplicate within this page by (first_name, last_name)
    seen = set()
    unique = []
    for lead in leads:
        key = (lead["first_name"].lower(), lead["last_name"].lower())
        if key not in seen:
            seen.add(key)
            unique.append(lead)

    logger.info("Extracted %d leads from %s", len(unique), source_url)
    return unique


def _looks_like_name(text: str) -> bool:
    """
    Heuristic: a name is 2–4 words, each capitalized, no digits,
    reasonable total length. Rejects strings with punctuation that
    indicates non-name content (colons, ampersands used as headings, etc.)
    """
    text = text.strip()
    if not text or len(text) > 60 or len(text) < 4:
        return False
    # Reject heading-like strings with colons or multiple special chars
    if ":" in text:
        return False
    if text.count("&") > 0 and not any(w.lower() in ("&",) for w in text.split()):
        # Allow "Co-Founder & CEO" style but reject "Topics & Subtopics: ..."
        # We already rejected colons above; this catches standalone & in headings
        pass
    words = text.split()
    if not (2 <= len(words) <= 4):
        return False
    if any(char.isdigit() for char in text):
        return False
    # All words must start with a capital letter (allows Mc, O'Brien etc.)
    if not all(w[0].isupper() for w in words if w and w[0].isalpha()):
        return False
    # Reject if any word is all-caps regardless of length (HR, CEO, IT, etc. in nav items)
    # Exception: single-letter initials like "J." are fine
    if any(w.isupper() and len(w) > 1 and not w.endswith(".") for w in words):
        return False
    # Reject if & appears — nav items like "HR & Legal" use & as a separator
    # Real names rarely contain & (exception: "O'Brien & Associates" type firms handled elsewhere)
    if "&" in text:
        return False
    return True


def _parse_name(text: str) -> tuple[str, str]:
    """
    Split a full name string into (first_name, last_name).
    Handles suffixes like Jr., Sr., III, etc.
    """
    suffixes = {"jr", "sr", "ii", "iii", "iv", "phd", "md", "esq"}
    parts = text.strip().split()
    # Strip trailing suffixes
    while parts and parts[-1].lower().rstrip(".") in suffixes:
        parts.pop()
    if len(parts) == 0:
        return text.strip(), ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _make_lead(name: str, title: str, domain: str, source_url: str) -> dict | None:
    """
    Build a lead dict if the title passes the pre-filter.
    Returns None if the title is not worth extracting.
    """
    title = title.strip()
    if not should_extract_lead(title):
        logger.debug("title pre-filter rejected: %r", title)
        return None
    first, last = _parse_name(name)
    if not first:
        return None
    # Derive company name from domain (best guess — enriched later)
    company = domain.split(".")[0].replace("-", " ").title()
    return {
        "first_name": first,
        "last_name":  last,
        "title":      title,
        "company":    company,
        "domain":     domain,
        "source_url": source_url,
    }


def _strategy_class_hints(soup: BeautifulSoup, domain: str, source_url: str) -> list[dict]:
    """
    Look for elements whose class names suggest they hold a person's name or title.
    """
    NAME_HINTS  = ["name", "person", "member", "employee", "staff", "bio-name", "team-name"]
    TITLE_HINTS = ["title", "role", "position", "job", "bio-title", "team-role", "designation"]

    leads = []

    # Find all elements with name-hint classes
    for name_el in soup.find_all(True):
        el_classes = " ".join(name_el.get("class", [])).lower()
        if not any(hint in el_classes for hint in NAME_HINTS):
            continue
        name_text = name_el.get_text(strip=True)
        if not _looks_like_name(name_text):
            continue

        # Look for a sibling or nearby element with a title-hint class
        title_text = None
        # Check next siblings
        for sibling in name_el.find_next_siblings():
            sib_classes = " ".join(sibling.get("class", [])).lower()
            if any(hint in sib_classes for hint in TITLE_HINTS):
                title_text = sibling.get_text(strip=True)
                break
        # Check parent's children
        if not title_text and name_el.parent:
            for child in name_el.parent.find_all(True):
                child_classes = " ".join(child.get("class", [])).lower()
                if any(hint in child_classes for hint in TITLE_HINTS):
                    candidate = child.get_text(strip=True)
                    if candidate != name_text:
                        title_text = candidate
                        break

        if title_text:
            lead = _make_lead(name_text, title_text, domain, source_url)
            if lead:
                leads.append(lead)

    return leads


def _strategy_proximity(soup: BeautifulSoup, domain: str, source_url: str) -> list[dict]:
    """
    Fallback: scan all short text strings in document order.
    Pair consecutive strings where first looks like a name and second like a title.
    """
    leads = []

    # Collect all leaf text nodes of reasonable length
    candidates = []
    for el in soup.find_all(True):
        if el.find():  # skip non-leaf elements
            continue
        text = el.get_text(strip=True)
        if 3 < len(text) < 80:
            candidates.append(text)

    # Sliding window: name candidate followed by title candidate
    i = 0
    while i < len(candidates) - 1:
        maybe_name  = candidates[i]
        maybe_title = candidates[i + 1]
        if _looks_like_name(maybe_name) and should_extract_lead(maybe_title):
            lead = _make_lead(maybe_name, maybe_title, domain, source_url)
            if lead:
                leads.append(lead)
            i += 2  # advance past both
        else:
            i += 1

    return leads


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

# Seniority sort order — lower number = higher priority
_ROLE_LEVEL_RANK = {"ceo_founder": 0, "hr_leader": 1, "needs_review": 2}
_ROLE_CONTEXT_RANK = {
    "founders and CEOs":      0,
    "leadership teams":       1,
    "HR and people leaders":  2,
    "HR teams":               3,
    "needs_review":           4,
}


def pick_best_lead(leads: list[dict]) -> dict | None:
    """
    Given a list of leads from a single domain, return the single best one.
    Runs title_classifier inline to get role_level + role_context for sorting.
    Priority: ceo_founder > hr_leader, then leadership teams > HR and people leaders > HR teams.
    """
    if not leads:
        return None
    if len(leads) == 1:
        lead = leads[0]
        from title_classifier import classify_title
        result = classify_title(lead["title"])
        lead.update(result)
        return lead

    from title_classifier import classify_title

    def sort_key(lead: dict) -> tuple:
        result = classify_title(lead["title"])
        lead.update(result)
        return (
            _ROLE_LEVEL_RANK.get(result["role_level"], 99),
            _ROLE_CONTEXT_RANK.get(result["role_context"], 99),
        )

    return sorted(leads, key=sort_key)[0]


def scrape_team_page(domain: str) -> dict | None:
    """
    Attempt to scrape leadership/team page for a given domain.

    Returns the single best lead dict, or None if nothing found.
    """
    found_leads = []
    tried_urls  = []
    seen_content_hashes: set[int] = set()  # detect redirect-to-homepage loops

    for path in TEAM_PAGE_PATHS:
        # robots.txt check
        if not _is_allowed(domain, path):
            continue

        url = f"https://{domain}{path}"
        tried_urls.append(url)

        html = _fetch_page(url)
        if not html:
            time.sleep(REQUEST_DELAY)
            continue

        # Detect same-content redirect loops (all paths returning homepage)
        content_hash = hash(html[:2000])
        if content_hash in seen_content_hashes:
            logger.info("Duplicate content detected at %s — skipping (redirect loop)", url)
            time.sleep(REQUEST_DELAY)
            continue
        seen_content_hashes.add(content_hash)

        leads = _parse_leads(html, domain, url)
        if leads:
            found_leads.extend(leads)
            logger.info("Found %d leads at %s", len(leads), url)
            break  # stop trying paths once we find a working page

        time.sleep(REQUEST_DELAY)

    if not found_leads:
        logger.warning("No leads found for %s (tried %d paths)", domain, len(tried_urls))
        return None

    best = pick_best_lead(found_leads)
    logger.info("Best lead for %s: %s %s — %s", domain,
                best["first_name"], best["last_name"], best["title"])
    return best


# ---------------------------------------------------------------------------
# Stress test — python3 team_page_scraper.py
# Diagnostic mode — python3 team_page_scraper.py --diagnose <domain>
# ---------------------------------------------------------------------------

def diagnose_domain(domain: str):
    """
    Diagnostic mode: for each path, report exactly what happened.
    Reveals whether failure is robots.txt, 404, JS-render, or parse failure.
    """
    print(f"\n{'='*60}")
    print(f"DIAGNOSING: {domain}")
    print('='*60)

    seen_content_hashes: set[int] = set()

    for path in TEAM_PAGE_PATHS:
        url = f"https://{domain}{path}"

        allowed = _is_allowed(domain, path)
        if not allowed:
            print(f"  BLOCKED   {path}  (robots.txt)")
            continue

        html, http_status = _fetch_with_requests(url)
        if html is None:
            print(f"  HTTP {http_status or '???'}  {path}  — trying Playwright...")
            html = _fetch_with_playwright(url)
            if not html or _is_cf_blocked(html):
                print(f"  FAILED    {path}  (Playwright also blocked or failed)")
                time.sleep(REQUEST_DELAY)
                continue
            print(f"  PW-OK     {path}  (Playwright succeeded)")

        # Skip duplicate content (redirect loops)
        content_hash = hash(html[:2000])
        if content_hash in seen_content_hashes:
            print(f"  DUPLICATE {path}  (same content as previous path — skipping)")
            continue
        seen_content_hashes.add(content_hash)

        js_rendered = _looks_js_rendered(html)
        soup = BeautifulSoup(html, "html.parser")
        text_len = len(soup.get_text(strip=True))
        leads = _parse_leads(html, domain, url)
        status = "JS-RENDER" if js_rendered else "FETCHED  "
        print(f"  {status}  {path:<35} text={text_len:>6} chars  leads={len(leads)}")
        for lead in leads[:3]:
            print(f"             → {lead['first_name']} {lead['last_name']} — {lead['title']}")

        time.sleep(REQUEST_DELAY)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    test_domains = [
        "comodo.com",
        "digitalcommerce360.com",
        "cta.tech",
        "reedmfgco.com",
        "thewarrencompany.com",
        "lakeerierubber.com",
        "mbausa.org",
    ]

    if "--diagnose" in sys.argv:
        idx = sys.argv.index("--diagnose")
        target = sys.argv[idx + 1] if len(sys.argv) > idx + 1 else test_domains[0]
        diagnose_domain(target)
    else:
        all_results = {}
        for domain in test_domains:
            print(f"\n{'='*60}")
            print(f"Scraping: {domain}")
            print('='*60)
            lead = scrape_team_page(domain)
            all_results[domain] = lead
            if lead:
                print(f"  {lead['first_name']} {lead['last_name']} — {lead['title']}")
                print(f"  role_level: {lead.get('role_level', 'unclassified')}  role_context: {lead.get('role_context', 'unclassified')}  confidence: {lead.get('confidence', '?')}")
            else:
                print("  No lead extracted.")

        print(f"\n{'='*60}")
        print("SUMMARY")
        print('='*60)
        for domain, lead in all_results.items():
            result = f"{lead['first_name']} {lead['last_name']} — {lead['title']}" if lead else "no lead"
            print(f"  {domain:<35} {result}")