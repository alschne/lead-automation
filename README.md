# Lead Automation Pipeline

Automatically discovers companies and extracts decision-maker leads, writing them directly to the cold email pipeline's Google Sheet.

## What it does

Runs every Saturday at 9pm Mountain Time via Google Cloud Run + Cloud Scheduler. It:

1. **Step 0:** Ingests any CSV files dropped in Google Drive (Hunter/Apollo exports)
2. **Step 1:** Discovers companies from HackerNews "Who is Hiring?" monthly thread
3. **Step 2:** Scrapes each company's team/leadership page for decision-makers
4. **Step 2b:** Runs Hunter API enrichment on domains where scraper found nothing
5. **Step 2c:** Infers industry for all leads using batched Gemini AI
6. **Step 3:** Writes qualified leads to Google Sheet (dedup handled automatically)
7. **Step 4:** Sends daily summary email (includes needs_review leads if any)

## Lead flow

```
CSV imports (Hunter/Apollo)          HackerNews
        ↓                                ↓
   CSV ingestor                    Company domains
        ↓                                ↓
  Title classifier             Team page scraper
  Industry normalizer                   ↓
  Confidence gate           Hunter API (fallback)
        ↓                                ↓
        │                     Title classifier
        │                     Industry normalizer
        │                     Confidence gate
        │                                ↓
        └────────────────────────────────┘
                                    ↓
                    Google Sheet (ready_to_send or needs_review)
                                    ↓
                           Notification email
```

## Lead sources

| Source | Type | Leads/month | Automated |
|---|---|---|---|
| Apollo CSV export | Manual import | ~75 | Drop in Drive |
| Hunter CSV export | Manual import | ~50 | Drop in Drive |
| HackerNews scraper | Automated discovery | ~40 | ✅ |
| Hunter API enrichment | Automated enrichment | ~25 | ✅ |

## Target leads

- **Role levels:** CEO, Founder, President, VP, Director, Manager, HR/People leaders
- **Company size:** 30–200 employees (signal-based)
- **Industries:** Broad — any company that may need compensation analytics

## Repository structure

```
lead_automation/
├── main.py                   # Orchestration
├── hackernews_discovery.py   # HackerNews Who's Hiring parser
├── team_page_scraper.py      # requests + Playwright scraper
├── hunter_enrichment.py      # Hunter API domain search fallback
├── csv_ingestor.py           # Hunter/Apollo CSV import from Google Drive
├── title_classifier.py       # Deterministic role_level + role_context rules
├── industry_normalizer.py    # Industry label normalization + Gemini fallback
├── confidence_gate.py        # Assigns ready_to_send vs needs_review status
├── sheet_writer.py           # Dedup check + Google Sheet insert
├── notifier.py               # Zoho SMTP notification emails
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── SETUP.md
├── MAINTENANCE.md
└── BACKLOG.md
```

## Google Sheet columns written

| Column | Value |
|---|---|
| first_name | From scraper / CSV |
| last_name | From scraper / CSV |
| company | From scraper / CSV |
| domain | From discovery |
| industry | Inferred by Gemini |
| role_level | From title_classifier |
| role_context | From title_classifier |
| title | From scraper / CSV |
| email | From Hunter API or CSV (when available) |
| verification_result | From Hunter / CSV verification status |
| status | ready_to_send or needs_review |

All other columns are filled by the cold email pipeline.

## GCP setup

- **Project:** lead-automation-490322
- **Region:** us-central1
- **Artifact Registry:** lead-automation
- **Cloud Run Job:** lead-automation-pipeline
- **Cloud Scheduler:** lead-automation-weekly (`0 21 * * 6` — Saturday 9pm Mountain)