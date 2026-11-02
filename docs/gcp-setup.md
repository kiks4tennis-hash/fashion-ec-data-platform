# GCP setup

One-time setup for loading the synthetic data into BigQuery. Commands are for Windows
PowerShell; they also work on macOS/Linux with the usual variable syntax.

Expected cost: BigQuery's free tier covers this project (10 GiB storage and 1 TiB of queries
per month). Cloud Storage in `asia-northeast1` is not free-tier eligible, but the data is a
few tens of MB, so the cost is negligible. **Still set a budget alert first (step 2).**

## 1. Install the Google Cloud CLI and sign in

```powershell
winget install Google.CloudSDK
# open a new PowerShell window, then:
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default login
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

`application-default login` stores credentials that the Python clients pick up automatically
(Application Default Credentials). **Do not create or download service-account key files**;
they are the most common way cloud credentials leak. `.gitignore` blocks common key-file
names as a safety net.

## 2. Create a budget alert

In the console: **Billing -> Budgets & alerts -> Create budget**. Scope it to this project,
for example 1,000 JPY per month, with alerts at 50%, 90% and 100%.

A budget only sends notifications; it does not stop spending. Delete resources you no longer
need (see "Cleaning up").

## 3. Enable APIs and create the bucket

```powershell
gcloud services enable bigquery.googleapis.com storage.googleapis.com

# Bucket names are global. The loader's default is <project-id>-fashion-ec.
gcloud storage buckets create gs://YOUR_PROJECT_ID-fashion-ec `
  --location=asia-northeast1 `
  --uniform-bucket-level-access `
  --public-access-prevention
```

The BigQuery dataset (`raw`) is created by the loader. Keep the bucket and the dataset in the
same location (`asia-northeast1`, Tokyo); loading across locations is not allowed.

## 4. Generate, check, load

```powershell
python -m fashion_ec.generate --out data/raw
python -m fashion_ec.load --project YOUR_PROJECT_ID --dry-run   # shows the plan, touches nothing
python -m fashion_ec.load --project YOUR_PROJECT_ID
```

Instead of `--project` you can set `$env:GCP_PROJECT_ID`. Other options: `--bucket`,
`--location`, `--dataset`, `--tables orders returns`.

The loader replaces both the GCS prefix and the BigQuery table on every run, so it is safe to
run repeatedly. After each table it compares BigQuery's row count with the Parquet files and
stops on a mismatch.

## 5. Verify in BigQuery

```sql
SELECT status, COUNT(*) AS orders
FROM `YOUR_PROJECT_ID.raw.orders`
GROUP BY status;
```

Check that `clickstream` is partitioned (table details -> "Partitioned by: DAY on event_ts")
and that `order_ts` shows as `TIMESTAMP`, not `DATETIME`.

## Cleaning up

```powershell
bq rm -r -f -d YOUR_PROJECT_ID:raw
gcloud storage rm -r gs://YOUR_PROJECT_ID-fashion-ec
```

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `Your default credentials were not found` | Run `gcloud auth application-default login`. |
| `403 ... does not have storage.objects.create` | Your account lacks access to the bucket; use the project owner account for this personal project. |
| `404 ... bucket does not exist` | Create the bucket (step 3) or pass `--bucket`. |
| `Cannot read and write in different locations` | The bucket and the dataset must be in the same location (`asia-northeast1`). |
| Row count mismatch error | Do not ignore it: re-run, and compare the file count in `--dry-run` with GCS. |
