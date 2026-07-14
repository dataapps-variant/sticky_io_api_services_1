# Sticky.io → BigQuery ETL (Airbyte-free, runs on Cloud Run)

A production-grade pipeline that pulls orders directly from the **sticky.io API**
and loads them into **BigQuery**, with **no Airbyte** anywhere. It runs as a
**Cloud Run Job**: once to back-fill history, then on a schedule for new data.

It was built specifically to fix the problems we found in the old setup:

| Problem in the old pipeline | How this fixes it |
|---|---|
| `utm_info` (and `device_category`) was **dropped** before it reached BigQuery | Nothing is ever dropped — the **entire order** is stored as JSON, so `device_category` and every other field is always available |
| `custom_fields` and `order_customer_types` were **dropped** | Same — kept in full |
| `Date_of_Sale` came from `DATE(time_stamp)` on **UTC** values, causing off-by-one-day errors near midnight | The ETL computes `date_of_sale` in your **reporting timezone**, so the date is correct |
| Confusion about "time of sale" | `time_stamp` (raw) **and** a parsed `time_of_sale` timestamp are both stored |
| A rigid 189-column schema — any new sticky.io field was lost | Adding a field is now a one-line change in a **view**, never a pipeline change |
| Depended on Airbyte | Removed. Cloud Run talks to sticky.io and writes to BigQuery directly |

---

## 1. How it works (plain English)

1. **Find orders.** It asks sticky.io `order_find` for all order IDs created in a
   date window. Sticky.io only returns 50,000 IDs at a time, so it automatically
   walks forward in time to collect them all (this logic is carried over from the
   original code, unchanged in spirit).
2. **Fetch full orders.** It calls `order_view` in batches of 500 IDs, several at
   once, behind a **rate limiter of 15 requests/minute** (sticky.io's limit).
3. **Keep everything.** Each order is stored *verbatim* as JSON in a `raw_order`
   column, plus a few helper columns (`order_id`, `time_stamp`, `time_of_sale`,
   `date_of_sale`, ...).
4. **Load safely.** Rows go into a staging table, then a `MERGE` upserts them into
   the app's main table keyed on `order_id`. Re-running is safe (idempotent) — it
   never creates duplicates.
5. **Status changes.** It also pulls `order_find_updated` (chargebacks, refunds,
   voids, RMAs, ...) so those keep up to date on older orders.

Each app gets its **own tables**: `orders_<app>` and `orders_<app>_staging`.

### Two modes
- **`backfill`** — loads history **month by month** from `BACKFILL_START`
  (default `2025-11-01`) up to **today** (computed live, never hard-coded).
  Every finished month is checkpointed, so if it stops you just run it again and
  it **resumes**.
- **`incremental`** — for scheduled runs afterward. Pulls new **created** orders
  since the newest `time_stamp` already loaded, plus a rolling **45-day** window of
  **status changes**.

---

## 2. What's in this repo

```
sticky-etl/
├── README.md                     ← this file
├── Dockerfile                    ← builds the Cloud Run Job container
├── requirements.txt
├── .env.example                  ← sample settings for local testing
├── config/
│   └── apps.yaml                 ← your list of apps (REPLACES the old Airbyte yaml)
├── schema/
│   └── orders_schema.json        ← the BigQuery table schema, for reference
├── sql/
│   ├── create_tables.sql         ← reference DDL (auto-created by the ETL)
│   └── orders_flat_view.sql      ← the view that surfaces device_category etc.
└── src/
    ├── main.py                   ← entrypoint (reads MODE and runs)
    ├── config.py                 ← env vars, apps.yaml, Secret Manager
    ├── sticky_client.py          ← sticky.io API: pagination + rate limiting
    ├── transform.py              ← order → BigQuery row (keeps all fields)
    ├── bq_loader.py              ← staging load, MERGE, watermark, checkpoints
    ├── pipeline.py               ← one window, end to end
    ├── backfill.py               ← month-by-month backfill
    ├── incremental.py            ← watermark-based incremental
    └── logging_setup.py          ← structured logs for Cloud Logging
```

### What replaces `sticky_io_xp.yaml`
That file was the **Airbyte connector** definition (a URL + a fixed 189-column
schema). Since we removed Airbyte, it's **retired**. Its job is now split between:
- **`config/apps.yaml`** — which apps to pull and their settings, and
- **`schema/orders_schema.json`** — the BigQuery table shape.

You do not need `sticky_io_xp.yaml` anymore.

---

## 3. One-time security cleanup (do this first)

The old repo had a Google service-account **private key pasted into the code**.
Treat it as leaked and delete it:
**Google Cloud Console → IAM & Admin → Service Accounts → `sticky-maintainer` →
Keys → delete the old key.** This ETL never uses a key file — it uses the Cloud
Run Job's own identity — so removing it breaks nothing here.

---

## 4. Setup and run (step by step)

You'll do everything from **Cloud Shell** — a free terminal that runs *inside your
browser*, no installs. Open it with the **`>_`** icon at the top-right of the
[Google Cloud Console](https://console.cloud.google.com).

Throughout, replace the ALL-CAPS placeholders with your values:
- `PROJECT` = your Google Cloud project ID
- `REGION` = e.g. `europe-west1` (match your BigQuery location)
- `DATASET` = the BigQuery dataset to write to, e.g. `Sticky_ETL`
- `APP` = an app name from `config/apps.yaml`, e.g. `pdfdotnet`

### 4.1 Get the code into Cloud Shell
Push this folder to a GitHub repo, then in Cloud Shell:
```bash
git clone https://github.com/<you>/sticky-etl.git
cd sticky-etl
```
(Or upload the zip via Cloud Shell's **⋮ → Upload**, then `unzip`.)

### 4.2 Turn on the services
```bash
gcloud config set project PROJECT
gcloud services enable run.googleapis.com cloudbuild.googleapis.com \
  artifactregistry.googleapis.com bigquery.googleapis.com \
  secretmanager.googleapis.com cloudscheduler.googleapis.com
```

### 4.3 Store your sticky.io login in Secret Manager
The secret value is just `username:password` for that app.
```bash
printf 'YOUR_STICKY_USERNAME:YOUR_STICKY_PASSWORD' | \
  gcloud secrets create sticky-cred-APP --data-file=-
```
The secret name (`sticky-cred-APP`) must match `cred_secret` in `config/apps.yaml`.

### 4.4 Edit `config/apps.yaml`
Make sure your app is listed with the right `company` (subdomain) and
`cred_secret`. Confirm the timezone — see the note in §6.

### 4.5 Deploy the two jobs (backfill + incremental)
Both jobs use the same code; they only differ by `MODE`.
```bash
# BACKFILL job
gcloud run jobs deploy sticky-backfill \
  --source . --region REGION \
  --task-timeout=86400 --max-retries=1 --memory=2Gi \
  --set-env-vars=MODE=backfill,BQ_PROJECT=PROJECT,BQ_DATASET=DATASET,BQ_LOCATION=REGION,STICKY_COMPANY=APP,BACKFILL_START=2025-11-01

# INCREMENTAL job
gcloud run jobs deploy sticky-incremental \
  --source . --region REGION \
  --task-timeout=3600 --max-retries=1 --memory=2Gi \
  --set-env-vars=MODE=incremental,BQ_PROJECT=PROJECT,BQ_DATASET=DATASET,BQ_LOCATION=REGION,STICKY_COMPANY=APP
```
> `--task-timeout=86400` gives the backfill up to 24 hours per run — important,
> because at 15 requests/min a big month can take a while. It resumes if it stops.

### 4.6 Give the jobs permission to use BigQuery + the secret
Cloud Run Jobs run as your project's **compute service account** by default. Grant
it what it needs:
```bash
PROJECT_NUMBER=$(gcloud projects describe PROJECT --format='value(projectNumber)')
SA="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding PROJECT \
  --member="serviceAccount:$SA" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding PROJECT \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser"
gcloud secrets add-iam-policy-binding sticky-cred-APP \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor"
```

### 4.7 Test small first (recommended)
Before the full backfill, prove one small window works. Temporarily point the
backfill at a recent start date and run it:
```bash
gcloud run jobs update sticky-backfill --region REGION \
  --update-env-vars=BACKFILL_START=2026-07-01
gcloud run jobs execute sticky-backfill --region REGION --wait
```
Check the logs (Console → Cloud Run → Jobs → sticky-backfill → Logs), then verify
in BigQuery:
```sql
SELECT COUNT(*) AS orders,
       MIN(time_stamp) AS earliest, MAX(time_stamp) AS latest
FROM `PROJECT.DATASET.orders_APP`;
```
Also confirm the previously-missing field is now present:
```sql
SELECT order_id,
       JSON_VALUE(raw_order,'$.utm_info.device_category') AS device_category
FROM `PROJECT.DATASET.orders_APP`
WHERE JSON_VALUE(raw_order,'$.utm_info.device_category') IS NOT NULL
LIMIT 20;
```

### 4.8 Run the real backfill
Set the start back to November and run:
```bash
gcloud run jobs update sticky-backfill --region REGION \
  --update-env-vars=BACKFILL_START=2025-11-01
gcloud run jobs execute sticky-backfill --region REGION --wait
```
If it ever stops early, just run the `execute` line again — it resumes at the
first unfinished month.

### 4.9 Create the friendly view
Open `sql/orders_flat_view.sql`, replace `<PROJECT>`, `<DATASET>`, `<app>`, and run
it in BigQuery. You now have `orders_APP_flat` with clean columns, correct
`Date_Of_Sale`, and the recovered `Device_Category`, `UTM_*`, `Custom_Fields_Json`,
and `Order_Customer_Types_Json`.

### 4.10 Schedule the incremental job
Have Cloud Scheduler run the incremental job daily (e.g. 06:00):
```bash
gcloud scheduler jobs create http sticky-incremental-daily \
  --location=REGION --schedule="0 6 * * *" --time-zone="America/New_York" \
  --uri="https://REGION-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT/jobs/sticky-incremental:run" \
  --http-method=POST \
  --oauth-service-account-email="$SA"
```

---

## 5. Adding another app later
1. Add the login: `printf 'user:pass' | gcloud secrets create sticky-cred-NEWAPP --data-file=-`
2. Add a block to `config/apps.yaml` (name, company, cred_secret).
3. Grant secret access (the §4.6 secret line, for the new secret).
4. Redeploy the jobs (§4.5) and run backfill with `STICKY_COMPANY=NEWAPP`.

Each app writes to its own `orders_<name>` table, exactly as you wanted.

---

## 6. Timezone note (please verify once)
Sticky.io reports order times in the **account's configured timezone**. The ETL
assumes **America/New_York** by default (what the old code used). On your first
test run, pick one order, open it in the sticky.io Orders report, and compare its
displayed time to `time_stamp`. If they don't line up, change `source_timezone`
for that app in `config/apps.yaml` and re-run. This is the clean, permanent fix
for the time-of-sale confusion we discussed.

---

## 7. Cost & performance notes
- **Rate limit:** ~15 requests/min × 500 orders = ~7,500 orders/min ≈ ~450k
  orders/hour. Backfill time scales with your order volume.
- **BigQuery storage:** storing the full JSON is a little larger, but trivial at
  this scale and worth it — you never lose a field again.
- **Cloud Run Jobs** only cost while running. The daily incremental is cheap.

---

## 8. Quick reference — the tables
- `orders_<app>` — one row per order; `raw_order` holds the complete JSON.
- `orders_<app>_flat` — the view analysts should query.
- `orders_<app>_staging` — internal; used during loads.
- `_etl_state` — internal; backfill checkpoints and run history.
