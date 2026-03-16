# Lead Automation Pipeline

Automatically discovers companies and extracts decision-maker leads, writing them directly to the cold email pipeline's Google Sheet.

## What it does

Runs every Saturday at 9pm Mountain Time via Google Cloud Run + Cloud Scheduler. It:

1. Discovers companies from HackerNews "Who is Hiring?" monthly thread
2. Scrapes each company's team/leadership page for decision-makers
3. Classifies each lead by role level and role context
4. Infers industry using Gemini AI
5. Writes qualified leads to the Google Sheet with `status = ready_to_send`
6. Flags low-confidence leads as `needs_review`
7. Sends a daily summary email with any leads needing manual review

## Lead flow

```
HackerNews → company domains
    ↓
Team page scraper → name + title
    ↓
Title classifier → role_level + role_context
    ↓
Industry normalizer → industry label
    ↓
Confidence gate → ready_to_send or needs_review
    ↓
Google Sheet (same sheet as cold email pipeline)
```

## Target leads

- **Role levels:** CEO, Founder, President, VP, Director, Manager, HR/People leaders
- **Company size:** 30–200 employees (signal-based, not hard-filtered)
- **Industries:** Broad — any company with employees that may need compensation analytics help

## Repository structure

```
lead_automation/
├── main.py                   # Orchestration
├── hackernews_discovery.py   # HackerNews Who's Hiring parser
├── team_page_scraper.py      # requests + Playwright scraper
├── title_classifier.py       # Deterministic role_level + role_context rules
├── industry_normalizer.py    # Industry label normalization + Gemini fallback
├── confidence_gate.py        # Assigns ready_to_send vs needs_review status
├── sheet_writer.py           # Dedup check + Google Sheet insert
├── notifier.py               # Zoho SMTP notification emails
├── backfill_industry.py      # One-time script — delete after use
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
├── README.md
├── SETUP.md
└── MAINTENANCE.md
```

## Google Sheet columns written

| Column | Value |
|---|---|
| first_name | From scraper |
| last_name | From scraper |
| company | From scraper / discovery |
| domain | From discovery |
| industry | Inferred by Gemini |
| role_level | From title_classifier |
| role_context | From title_classifier |
| title | From scraper |
| status | ready_to_send or needs_review |

All other columns (email, personalization, send dates, etc.) are filled by the cold email pipeline.

## GCP setup

- **Project:** lead-automation-490322
- **Region:** us-central1
- **Artifact Registry:** lead-automation
- **Cloud Run Job:** lead-automation-pipeline
- **Cloud Scheduler:** lead-automation-weekly (`0 21 * * 6` — Saturday 9pm Mountain)
