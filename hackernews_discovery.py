"""
hackernews_discovery.py

Discovers companies from HackerNews "Who is Hiring?" monthly threads.
Uses the official HackerNews API — no auth required, fully legal, fully free.

Pipeline:
    1. Find the latest "Who is Hiring?" thread via HN Algolia search API
    2. Fetch all top-level comments (job postings)
    3. Extract company name + domain from each posting
    4. Filter by company size signals (where detectable)
    5. Return list of company dicts for enrichment layer

Each company dict:
    {
        "company":    str,
        "domain":     str | None,
        "source":     "hackernews",
        "source_url": str,
    }
"""

import re
import logging
import requests
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 10
HN_API_BASE     = "https://hacker-news.firebaseio.com/v0"
HN_ALGOLIA_BASE = "https://hn.algolia.com/api/v1"

# Signals that suggest a company is too large (>200 employees)
LARGE_COMPANY_SIGNALS = [
    "fortune 500", "publicly traded", "nasdaq", "nyse",
    "10,000", "50,000", "100,000", "we are a global",
]

# Industries to skip entirely — not our ICP
SKIP_INDUSTRY_SIGNALS = [
    "pediatric", "hospital", "clinic", "medical practice",
    "dental", "pharmacy", "health system", "patient care",
]

# Signals that suggest a good size fit (roughly 30-200 employees)
SIZE_FIT_SIGNALS = [
    "small team", "early stage", "series a", "series b",
    "seed stage", "seed funded", "startup", "we're a team of",
    "team of", "person team", "people team",
]


# ---------------------------------------------------------------------------
# Find latest "Who is Hiring?" thread
# ---------------------------------------------------------------------------

def get_latest_hiring_thread() -> dict | None:
    """
    Find the most recent "Ask HN: Who is hiring?" post.

    Uses the official 'whoishiring' HN account's submission history —
    more reliable than Algolia search which has indexing lag.

    Returns a dict with 'id', 'title', 'url' or None on failure.
    """
    try:
        # Fetch whoishiring user's submitted posts (newest first)
        resp = requests.get(
            f"{HN_API_BASE}/user/whoishiring.json",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        user_data = resp.json()
        submitted = user_data.get("submitted", [])

        if not submitted:
            logger.warning("No submissions found for whoishiring user")
            return None

        # Check recent posts for "Who is hiring?" (not "Who wants to be hired?")
        for post_id in submitted[:10]:
            resp = requests.get(
                f"{HN_API_BASE}/item/{post_id}.json",
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            item = resp.json()
            title = item.get("title", "")
            if "who is hiring" in title.lower():
                logger.info("Found hiring thread: %s (id=%s)", title, post_id)
                return {
                    "id":    str(post_id),
                    "title": title,
                    "url":   f"https://news.ycombinator.com/item?id={post_id}",
                }

        logger.warning("No 'Who is hiring?' thread found in recent whoishiring posts")
        return None

    except Exception as e:
        logger.warning("Failed to fetch HN hiring thread: %s", e)
        return None


# ---------------------------------------------------------------------------
# Fetch comments from thread
# ---------------------------------------------------------------------------

def get_thread_comments(thread_id: str) -> list[str]:
    """
    Fetch all top-level comment texts from a HN thread.
    Returns list of comment text strings.
    """
    try:
        resp = requests.get(
            f"{HN_API_BASE}/item/{thread_id}.json",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        thread = resp.json()
    except Exception as e:
        logger.warning("Failed to fetch thread %s: %s", thread_id, e)
        return []

    kid_ids = thread.get("kids", [])
    logger.info("Fetching %d comments from thread %s", len(kid_ids), thread_id)

    comments = []
    for kid_id in kid_ids:
        try:
            resp = requests.get(
                f"{HN_API_BASE}/item/{kid_id}.json",
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            item = resp.json()
            if item and not item.get("deleted") and not item.get("dead"):
                text = item.get("text", "")
                if text:
                    comments.append(text)
        except Exception as e:
            logger.debug("Failed to fetch comment %s: %s", kid_id, e)
            continue

    logger.info("Fetched %d valid comments", len(comments))
    return comments


# ---------------------------------------------------------------------------
# Extract company + domain from comment text
# ---------------------------------------------------------------------------

# Common URL pattern
_URL_RE = re.compile(
    r'https?://(?:www\.)?([a-zA-Z0-9\-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?)'
    r'(?:/[^\s<>"]*)?',
    re.IGNORECASE,
)

# HN job posts often start with "Company Name | ..." or "Company Name (url) |"
_COMPANY_HEADER_RE = re.compile(
    r'^([A-Z][^\|<\n]{2,60}?)\s*[\|(<]',
    re.MULTILINE,
)

# Strip HTML tags
_HTML_TAG_RE = re.compile(r'<[^>]+>')


def _decode_html(text: str) -> str:
    """Decode HTML entities including HN's encoded forward slashes in URLs."""
    text = re.sub(r'&#x2F;', '/', text)
    text = re.sub(r'&#x27;', "'", text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&#62;', '>', text)
    text = re.sub(r'&#60;', '<', text)
    return text


def _strip_html(text: str) -> str:
    text = _decode_html(text)
    text = _HTML_TAG_RE.sub(" ", text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _extract_domain_from_url(url: str) -> str | None:
    """Extract clean domain from a URL string."""
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.lower()
        domain = re.sub(r'^www\.', '', netloc)

        # Skip careers/jobs subdomains early (careers.twitter.com, jobs.cisco.com, etc.)
        subdomain = netloc.split(".")[0]
        skip_subdomains = {"careers", "jobs", "apply", "boards", "corp", "hire", "recruiting"}
        if subdomain in skip_subdomains:
            return None

        # Skip URLs with jobs/careers in the path — return root domain instead
        # e.g. fetlife.com/jobs/head_of_engineering → fetlife.com (handled below)
        path = parsed.path.lower()
        job_path_signals = ["/jobs/", "/careers/", "/job/", "/apply/", "/openings/"]
        if any(sig in path for sig in job_path_signals):
            # Still extract the root domain — just don't use this URL for company name
            pass  # domain extraction continues, company name falls back to domain-derived

        if "." not in domain:
            return None

        # Strip non-www subdomains (engineering.khanacademy.org → khanacademy.org)
        # Keep two-part domains (gruntwork.io, scruff.com) and
        # country-code second-level domains (codeheroes.com.au)
        parts = domain.split(".")
        if len(parts) > 2:
            if parts[-2] in {"com", "co", "org", "net", "gov"} and len(parts[-1]) == 2:
                domain = ".".join(parts[-3:])
            else:
                domain = ".".join(parts[-2:])

        # Skip known job boards, shortlinks, hosting platforms, news sites
        # Check AFTER subdomain stripping so succeed.notion.site → notion.site gets caught
        skip_domains = {
            "github.com", "linkedin.com", "twitter.com", "x.com",
            "greenhouse.io", "lever.co", "jobs.ashbyhq.com",
            "workable.com", "indeed.com", "glassdoor.com",
            "youtube.com", "youtu.be", "docs.google.com", "notion.so",
            "tinyurl.com", "bit.ly", "goo.gl", "grnh.se",
            "smartrecruiters.com", "angel.co", "wellfound.com",
            "wsj.com", "techcrunch.com", "reuters.com", "bloomberg.com",
            "crunchbase.com", "pitchbook.com", "ashbyhq.com", "google.com", "forms.gle",
            "notion.site", "netlify.app", "vercel.app", "webflow.io",
            "fillout.com", "typeform.com", "recruitee.com", "bamboohr.com",
        }
        if domain in skip_domains:
            return None

        return domain
    except Exception:
        return None


def _should_skip(text: str) -> bool:
    """Returns True if the posting is too large or in an excluded industry."""
    text_lower = text.lower()
    if any(signal in text_lower for signal in LARGE_COMPANY_SIGNALS):
        return True
    if any(signal in text_lower for signal in SKIP_INDUSTRY_SIGNALS):
        return True
    return False


def parse_company_from_comment(text: str, source_url: str) -> dict | None:
    """
    Extract company name and domain from a single HN job posting comment.

    HN job posts follow this common format:
        Company Name | Role | Location | Type
        <p>Description with https://company.com link

    Returns a company dict or None if extraction fails.
    """
    # Decode HTML entities first (HN encodes / as &#x2F; in URLs)
    decoded = _decode_html(text)
    clean   = _strip_html(text)

    # Skip if company seems too large
    if _should_skip(clean):
        logger.debug("Skipping large company posting")
        return None

    # Extract domain from URLs in decoded text
    # Collect all valid domains, prefer the company homepage over job board links
    # HN posts often have job board URLs first, company URL last
    domain = None
    all_domains = []
    for url_match in _URL_RE.finditer(decoded):
        full_url  = url_match.group(0)
        candidate = _extract_domain_from_url(full_url)
        if candidate:
            all_domains.append(candidate)

    if all_domains:
        # Prefer domains that look like company homepages:
        # deprioritize anything with job-like path segments
        job_path_signals = ["job", "career", "position", "opening", "role", "apply"]
        homepage_candidates = []
        for url_match in _URL_RE.finditer(decoded):
            full_url = url_match.group(0)
            candidate = _extract_domain_from_url(full_url)
            if not candidate:
                continue
            path = urlparse(full_url).path.lower()
            if not any(sig in path for sig in job_path_signals):
                homepage_candidates.append(candidate)

        domain = homepage_candidates[0] if homepage_candidates else all_domains[0]

    if not domain:
        logger.debug("No usable domain found in comment")
        return None

    # Extract company name
    # Strategy 1: pipe-delimited header (most common HN format)
    # "Company Name | Role | ..."
    company = None
    pipe_match = re.match(r'^([^|<\n]{2,60}?)\s*\|', clean)
    if pipe_match:
        candidate = pipe_match.group(1).strip()
        # Strip parenthetical URLs e.g. "Proton VPN ( https://... )"
        candidate = re.sub(r'\s*\(\s*https?://[^)]+\)', '', candidate).strip()
        # Strip trailing punctuation
        candidate = candidate.rstrip('.,;:')
        if 1 <= len(candidate.split()) <= 8 and len(candidate) <= 60:
            # Reject if it looks like a job title not a company name
            title_signals = ["engineer", "developer", "manager", "head of",
                           "director", "designer", "analyst", "seeking",
                           "infrastructure", "architect", "scientist", "researcher"]
            if not any(sig in candidate.lower() for sig in title_signals):
                company = candidate

    # Strategy 2: original header regex fallback
    if not company:
        header_match = _COMPANY_HEADER_RE.search(clean)
        if header_match:
            candidate = header_match.group(1).strip()
            candidate = re.sub(r'\s*\(\s*https?://[^)]+\)', '', candidate).strip()
            if len(candidate.split()) <= 6 and len(candidate) <= 60:
                title_signals = ["engineer", "developer", "manager", "head of",
                                 "director", "designer", "analyst", "seeking",
                                 "infrastructure", "architect", "scientist", "researcher"]
                if not any(sig in candidate.lower() for sig in title_signals):
                    company = candidate

    # Strategy 3: derive from domain
    if not company:
        company = domain.split(".")[0].replace("-", " ").title()

    return {
        "company":    company,
        "domain":     domain,
        "source":     "hackernews",
        "source_url": source_url,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def discover_companies(max_comments: int = 200) -> list[dict]:
    """
    Run the full HackerNews discovery pipeline.

    Args:
        max_comments: Maximum number of comments to process per thread.
                      HN threads can have 400-600 comments — cap for speed.

    Returns:
        List of company dicts with domain, company name, and source info.
        Deduped by domain.
    """
    thread = get_latest_hiring_thread()
    if not thread:
        logger.error("Could not find HN hiring thread — aborting")
        return []

    comments = get_thread_comments(thread["id"])
    if not comments:
        logger.warning("No comments fetched from thread %s", thread["id"])
        return []

    # Cap at max_comments
    comments = comments[:max_comments]

    companies = []
    seen_domains: set[str] = set()

    for comment in comments:
        company = parse_company_from_comment(comment, thread["url"])
        if not company:
            continue
        domain = company["domain"]
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        companies.append(company)

    logger.info("HackerNews discovery: found %d unique companies", len(companies))
    return companies


# ---------------------------------------------------------------------------
# Smoke test — python3 hackernews_discovery.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    print("Fetching latest HN 'Who is Hiring?' thread...")
    companies = discover_companies(max_comments=50)  # small cap for smoke test

    print(f"\nFound {len(companies)} companies\n")
    print(f"{'Company':<35} {'Domain'}")
    print("-" * 70)
    for c in companies:
        print(f"  {c['company']:<33} {c['domain']}")

    print(f"\nSource: {companies[0]['source_url'] if companies else 'n/a'}")