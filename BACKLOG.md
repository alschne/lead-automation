# Backlog

Planned improvements in rough priority order.

## High priority

### Hunter.io enrichment
Use Hunter's free API (25 searches/month) to find verified email addresses and people at discovered domains before falling back to team page scraping. Hunter returns name + title + email in one call — higher quality than scraping.

Module: `hunter_enrichment.py`
Flow: domain → Hunter API → lead with email → skip scraper for that domain

### CommonCrawl discovery
Query CommonCrawl's CDX index for company domains at scale. Only free source with enough volume to support daily runs. Complex to build but high ceiling.

Module: `commoncrawl_discovery.py`
Approach: CDX API query for pages matching team/leadership URL patterns

### Switch to daily schedule
Currently weekly because HackerNews is the only source. Once CommonCrawl or job boards are added, switch Cloud Scheduler from `0 21 * * 6` to `0 21 * * *`.

## Medium priority

### Company size filtering
Add a signal-based size filter — scrape LinkedIn or Crunchbase (carefully) to verify 30-200 employee range before enrichment. Currently large companies occasionally slip through.

### Job board discovery
Greenhouse, Lever, Ashby, Workable sitemaps are dead. Research current public endpoints or seed lists to find active companies on each ATS platform.

### Google Custom Search fallback
Google deprecated "Search entire web" for new Programmable Search Engines in January 2026. Revisit if Google restores this capability or an alternative emerges. Would help find non-standard team page URLs like `/en/contact-reed/company-directory`.

## Low priority

### Apollo CSV ingestor
Build an ingestor for Apollo.io CSV exports. Apollo free trial gives 25 high-quality contacts. Even as a one-time boost, worth having an automated ingestor ready.

Module: `apollo_csv_ingestor.py`

### Multi-contact per domain
Currently takes only the best lead per domain. Once sending volume is higher and reputation is established, consider adding 2-3 contacts per company for higher conversion probability.

### Lead source analytics
Add a `lead_source` tracking column to the sheet and build a simple weekly report showing which source produces the most leads, most replies, and most booked calls.

### Playwright performance
Playwright is the slowest part of the pipeline (~60% of runtime). Investigate using a headless browser pool or cached session to reduce startup overhead per domain.
