# Setup Guide

## Prerequisites

- Python 3.13+
- Google Cloud SDK (`gcloud`)
- A Google Sheet shared with the service account (same sheet as cold email pipeline)

## Local development setup

### 1. Clone the repo

```bash
git clone <your-repo-url>
cd lead_automation
```

### 2. Install dependencies

```bash
pip3 install -r requirements.txt
playwright install chromium
```

### 3. Create service account

1. GCP Console → lead-automation-490322 → IAM & Admin → Service Accounts
2. Create service account: `lead-automation-sheets`
3. Role: Basic → Editor
4. Keys tab → Add Key → JSON → download
5. Move to repo root, rename to `service_account.json`
6. Share your Google Sheet with the service account's `client_email`

### 4. Create Google Drive folders

1. Go to Google Drive
2. Create folder: `Lead Imports`
3. Inside it create two subfolders: `hunter` and `apollo`
4. Share both subfolders with your service account `client_email`
5. Get folder IDs from the URL when you open each folder:
   `drive.google.com/drive/folders/FOLDER_ID_HERE`

### 5. Create `.env`

```bash
cp .env.example .env
```

Fill in all values. Required keys:

```
GOOGLE_SHEET_ID=
GOOGLE_SERVICE_ACCOUNT_JSON=service_account.json
GEMINI_API_KEY=
HUNTER_API_KEY=
HUNTER_IMPORT_FOLDER_ID=
APOLLO_IMPORT_FOLDER_ID=
ZOHO_SMTP_USER=
ZOHO_SMTP_PASSWORD=
NOTIFICATION_EMAIL=
```

### 6. Run locally

```bash
python3 main.py
```

## GCP deployment

### Enable APIs

```bash
gcloud config set project lead-automation-490322

gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  drive.googleapis.com
```

### Create Artifact Registry repo

```bash
gcloud artifacts repositories create lead-automation \
  --repository-format=docker \
  --location=us-central1
```

### Build and push image

```bash
gcloud builds submit \
  --tag=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest
```

### Store secrets

```bash
echo -n "VALUE" | gcloud secrets create GOOGLE_SHEET_ID --data-file=-
echo -n "VALUE" | gcloud secrets create GEMINI_API_KEY --data-file=-
echo -n "VALUE" | gcloud secrets create HUNTER_API_KEY --data-file=-
echo -n "VALUE" | gcloud secrets create HUNTER_IMPORT_FOLDER_ID --data-file=-
echo -n "VALUE" | gcloud secrets create APOLLO_IMPORT_FOLDER_ID --data-file=-
echo -n "VALUE" | gcloud secrets create ZOHO_SMTP_USER --data-file=-
echo -n "VALUE" | gcloud secrets create ZOHO_SMTP_PASSWORD --data-file=-
echo -n "VALUE" | gcloud secrets create NOTIFICATION_EMAIL --data-file=-
gcloud secrets create GOOGLE_SERVICE_ACCOUNT_JSON --data-file=service_account.json
```

### Grant Secret Manager access

```bash
gcloud projects add-iam-policy-binding lead-automation-490322 \
  --member="serviceAccount:857875279961-compute@developer.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Create Cloud Run Job

```bash
gcloud run jobs create lead-automation-pipeline \
  --image=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest \
  --region=us-central1 \
  --task-timeout=1800 \
  --memory=1Gi \
  --cpu=1 \
  --max-retries=0 \
  --set-secrets=GOOGLE_SHEET_ID=GOOGLE_SHEET_ID:latest,GEMINI_API_KEY=GEMINI_API_KEY:latest,HUNTER_API_KEY=HUNTER_API_KEY:latest,HUNTER_IMPORT_FOLDER_ID=HUNTER_IMPORT_FOLDER_ID:latest,APOLLO_IMPORT_FOLDER_ID=APOLLO_IMPORT_FOLDER_ID:latest,ZOHO_SMTP_USER=ZOHO_SMTP_USER:latest,ZOHO_SMTP_PASSWORD=ZOHO_SMTP_PASSWORD:latest,NOTIFICATION_EMAIL=NOTIFICATION_EMAIL:latest,GOOGLE_SERVICE_ACCOUNT_JSON=GOOGLE_SERVICE_ACCOUNT_JSON:latest
```

### Create Cloud Scheduler trigger

```bash
gcloud scheduler jobs create http lead-automation-weekly \
  --location=us-central1 \
  --schedule="0 21 * * 6" \
  --time-zone="America/Boise" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/lead-automation-490322/jobs/lead-automation-pipeline:run" \
  --http-method=POST \
  --oauth-service-account-email="857875279961-compute@developer.gserviceaccount.com"
```

### Test manually

```bash
gcloud run jobs execute lead-automation-pipeline --region=us-central1
```

### View logs

```bash
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=lead-automation-pipeline" \
  --limit=100 \
  --format="table(timestamp,textPayload)" \
  --order=asc
```

## Redeployment (after code changes)

```bash
gcloud builds submit \
  --tag=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest

gcloud run jobs update lead-automation-pipeline \
  --image=us-central1-docker.pkg.dev/lead-automation-490322/lead-automation/lead-automation-pipeline:latest \
  --region=us-central1
```