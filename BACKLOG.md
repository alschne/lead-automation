# Backlog

Planned improvements in rough priority order.

## High priority

### CommonCrawl discovery
Query CommonCrawl's CDX index for company domains at scale. Only remaining free source with enough volume to significantly increase weekly lead count. Complex to build but high ceiling — could yield hundreds of additional domains per run.

Module: `commoncrawl_discovery.py`
Approach: CDX API query for URLs matching team/leadership page patterns, extract root domains, feed into enrichment chain

### Switch to daily schedule
Currently weekly because HackerNews is the only automated discovery source. Once CommonCrawl is added, switch Cloud Scheduler from `0 21 * * 6` to `0 21 * * *` for daily runs.
Update: `gcloud scheduler jobs update http lead-automation-weekly --schedule="0 21 * * *"`

## Medium priority

### Company size filtering
Add signal-based size filter to verify 30-200 employee range before enrichment. Currently large companies occasionally slip through, especially from CSV imports.

### Job board discovery
Greenhouse, Lever, Ashby, Workable sitemaps are dead as of 2025. Research current public endpoints or seed lists to find active companies on each ATS platform. May require maintaining a curated company token list.

### Snov.io / Skrapp.io enrichment
Both have free tiers (50-100 credits/month) but **API access requires paid plans** on both platforms as of 2026. Only viable as manual CSV exports — same workflow as Apollo/Hunter. Build CSV parsers for each if you decide to use their UIs.

### Apollo API enrichment
Apollo's People Search API (`/api/v1/mixed_people/api_search`) requires a paid plan — not accessible on free tier as of 2026. If you upgrade Apollo, the `apollo_discovery.py` module is already written and ready to use. People Match endpoint (email enrichment) uses 75 credits/month on free tier.

## Low priority

### Multi-contact per domain
Currently takes only the best lead per domain. Once sending volume is higher and reputation is established, consider adding 2-3 contacts per company for higher conversion probability.

### Lead source analytics
Track which source produces the most replies and booked calls. Add a simple monthly report.

### Google Custom Search fallback
Google deprecated "Search entire web" for new Programmable Search Engines in January 2026. Revisit if Google restores this capability. Would help find non-standard team page URLs.

### Playwright performance
Playwright is the slowest part of the pipeline. Investigate browser pooling or cached sessions to reduce per-domain startup overhead.

### Automated Hunter/Apollo exports
Both platforms have paid API tiers that would allow fully automated monthly exports. Currently requires manual UI work at the start of each month. Worth the cost if you scale beyond free tier limits.

## Completed
- ✅ HackerNews Who's Hiring discovery
- ✅ Team page scraper (requests + Playwright fallback)
- ✅ Title classifier (deterministic rules + Gemini fallback)
- ✅ Industry normalizer (rule-based + Gemini batch inference)
- ✅ Confidence gate (ready_to_send vs needs_review)
- ✅ Sheet writer with dedup
- ✅ Daily notification email
- ✅ Hunter API enrichment (25 searches/month, fallback for scraper failures)
- ✅ CSV ingestor for Hunter and Apollo manual exports via Google Drive
- ✅ Cloud Run + Cloud Scheduler deployment (Saturday 9pm Mountain)