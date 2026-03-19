# Maintenance Guide

## Routine tasks

### Monthly (1st of each month)
- Go to Hunter.io → Leads → export 50 contacts filtered by your ICP → drop CSV in Drive `Lead Imports/hunter/`
- Go to Apollo.io → People → filter by title + company size → export 75 contacts → drop CSV in Drive `Lead Imports/apollo/`
- Pipeline ingests both automatically on next Saturday run

### Reviewing needs_review leads
When the pipeline sends a notification email with leads needing review:
1. Open the Google Sheet
2. Filter column O (status) for `needs_review`
3. For each row, verify/update: `role_level`, `role_context`, `industry`
4. Change `status` to `ready_to_send` to queue for outreach
5. Delete the row if the lead is not relevant

### Monitoring pipeline runs
Check Cloud Logging after each Saturday run:
```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=lead-automation-pipeline" \
  --limit=100 --format="table(timestamp,textPayload)" --order=asc
```
Key things to check: Errors count, Leads inserted, Hunter credits used, notification email sent.

### Checking Hunter API credits
```bash
curl "https://api.hunter.io/v2/account?api_key=YOUR_KEY" | python3 -m json.tool
```

### CommonCrawl rate limiting
CommonCrawl's CDX API can temporarily block IPs if too many requests are made.
- If you see SlowDown errors: wait 24 hours before retrying
- Never run commoncrawl_discovery.py multiple times in quick succession for testing
- Production runs are safe — one run per week with 5s delays between requests

## Adding new industry labels
When Gemini returns an unrecognized industry label, add it to `INDUSTRY_MAP` in `industry_normalizer.py`:
```python
"your canonical label": [
    "alias one",
    "alias two",
],
```
Then rebuild and redeploy.

## Tuning the title classifier
Add patterns to `ROLE_LEVEL_RULES` or `ROLE_CONTEXT_RULES` in `title_classifier.py`. Rules are checked in order — first match wins. After changes run the smoke test:
```bash
python3 title_classifier.py
```

## Adding new CSV import sources
1. Add a parser function `_parse_SOURCENAME_row(row: dict)` in `csv_ingestor.py`
2. Add a folder ID env var (e.g. `SOURCENAME_IMPORT_FOLDER_ID`)
3. Add the source + folder to the loop in `ingest_csvs()`
4. Create Drive folder, share with service account, add secret to GCP

## Adding new discovery sources
1. Create a new module with a `discover_companies()` function
2. Return list of dicts with: `company`, `domain`, `source`, `source_url`
3. Import and call it in `main.py` Step 1 alongside `hn_discover()`
4. Rebuild and redeploy

## Adding new enrichment sources
1. Create a new module with an `enrich_domain(domain, company)` function
2. Return a lead dict or None
3. Add it to the enrichment chain in `main.py` between Steps 2c and 2d
4. Add API key to Secret Manager and Cloud Run job secrets

## Updating secrets
```bash
echo -n "NEW_VALUE" | gcloud secrets versions add SECRET_NAME --data-file=-
```

## Redeploying after code changes
```bash
gcloud config set project lead-automation-490322

gcloud builds submit \
  --tag=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest

gcloud run jobs update lead-automation-pipeline \
  --image=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest \
  --region=us-central1
```

## Known limitations
- **HackerNews only for automated discovery:** Skews tech/SaaS. Run weekly since thread is monthly.
- **Team page scraper hit rate:** ~35-40% of domains yield a lead.
- **CommonCrawl rate limits:** 24-hour block if too many requests. Never test repeatedly.
- **CommonCrawl coverage gaps:** Not all domains are indexed. Newer sites may be missing.
- **Hunter API credits:** 25/month. Spent only on domains that fail scraper + CommonCrawl.
- **Gemini rate limits:** Free tier ~15 req/min. Industry inference batched to 1 call per run.
- **Apollo/Snov/Skrapp APIs:** All paywalled on free tier. Manual CSV exports only.

## Backlog
See `BACKLOG.md` for planned improvements.