# Maintenance Guide

## Routine tasks

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
Key things to check: Errors count, Leads inserted count, notification email sent.

## Adding new industries to the normalizer
When Gemini returns an unrecognized industry label, add it to `INDUSTRY_MAP` in `industry_normalizer.py`:
```python
"your canonical label": [
    "alias one",
    "alias two",
],
```
Then rebuild and redeploy.

## Tuning the title classifier
Add new patterns to `ROLE_LEVEL_RULES` or `ROLE_CONTEXT_RULES` in `title_classifier.py`. Rules are checked in order — first match wins. After changes, run the smoke test:
```bash
python3 title_classifier.py
```

## Adding new discovery sources
1. Create a new module e.g. `jobboard_rss.py` with a `discover_companies()` function
2. Function must return a list of dicts with keys: `company`, `domain`, `source`, `source_url`
3. Import and call it in `main.py` Step 1 alongside `hn_discover()`
4. Rebuild and redeploy

## Updating secrets
```bash
# Update an existing secret value
echo -n "NEW_VALUE" | gcloud secrets versions add SECRET_NAME --data-file=-
```

## Redeploying after code changes
```bash
gcloud builds submit \
  --tag=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest

gcloud run jobs update lead-automation-pipeline \
  --image=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest \
  --region=us-central1
```

## Known limitations
- **HackerNews only:** Currently the only discovery source. Skews tech/SaaS companies. Run is weekly since the thread is monthly.
- **Team page scraper hit rate:** ~35-40% of domains yield a lead. Sites without public team pages return nothing.
- **Gemini rate limits:** Free tier allows ~15 requests/minute. Industry inference is batched to use 1 call per pipeline run.
- **No company size filter:** We can't reliably detect employee count from scraped pages. Large companies occasionally slip through.
- **Cloudflare-protected domains:** Playwright bypasses most CF challenges but some domains remain inaccessible.

## Backlog
See `BACKLOG.md` for planned improvements.
