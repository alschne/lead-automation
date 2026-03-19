# Backlog

Planned improvements in rough priority order.

## High priority

### Switch to daily schedule
Currently weekly because HackerNews is the only automated discovery source.
Once a second discovery source is added, switch Cloud Scheduler:
```bash
gcloud scheduler jobs update http lead-automation-weekly \
  --schedule="0 21 * * *" \
  --location=us-central1
```

### Additional discovery sources
All known free options have been exhausted or have paywalled APIs:
- **CommonCrawl as discovery:** CDX API rate limits make broad TLD queries unreliable.
  The Columnar Index via AWS Athena would work but requires AWS account.
- **Apollo API:** People Search requires paid plan ($49+/month)
- **Job board sitemaps:** Greenhouse, Lever, Ashby all removed public sitemaps
- **Google CSE:** Deprecated "Search entire web" for new engines January 2026
- **Snov.io / Skrapp.io:** Free tiers are UI-only, no API access

Best path to more discovery volume: increase manual Apollo/Hunter exports
or upgrade to a paid API tier.

## Medium priority

### Company size filtering
Add signal-based size filter to verify 30-200 employee range. Currently
large companies occasionally slip through from CSV imports and HackerNews.

### CommonCrawl as discovery (revisit)
If CommonCrawl's CDX API becomes more reliable or AWS Athena access becomes
available, the broad discovery approach could yield hundreds of domains/week.
The `commoncrawl_discovery.py` module would need a new `discover_companies()`
function using TLD queries with regex path filtering.

### Apollo API enrichment
If you upgrade Apollo, the `apollo_discovery.py` module is already written.
People Search endpoint gives unlimited free discovery once API access is unlocked.

## Low priority

### Multi-contact per domain
Currently takes only the best lead per domain. Once sending volume is higher,
consider 2-3 contacts per company for higher conversion probability.

### Lead source analytics
Track which source produces the most replies and booked calls.

### Playwright performance
Investigate browser pooling to reduce per-domain startup overhead.

### Automated Hunter/Apollo exports
Both platforms have paid API tiers for fully automated monthly exports.

## Completed
- ✅ HackerNews Who's Hiring discovery
- ✅ Team page scraper (requests + Playwright fallback)
- ✅ CommonCrawl CDX enrichment (finds non-standard team page URLs)
- ✅ Title classifier (deterministic rules + Gemini fallback)
- ✅ Industry normalizer (rule-based + Gemini batch inference)
- ✅ Confidence gate (ready_to_send vs needs_review)
- ✅ Sheet writer with dedup
- ✅ Daily notification email
- ✅ Hunter API enrichment (last resort, 25 searches/month)
- ✅ CSV ingestor for Hunter and Apollo manual exports via Google Drive
- ✅ Cloud Run + Cloud Scheduler deployment (Saturday 9pm Mountain)
- ✅ Enrichment chain: scraper → CommonCrawl → Hunter (credit-efficient ordering)