# GCP setup

One-time setup for loading the synthetic data into BigQuery. Commands are for Windows
PowerShell; they also work on macOS/Linux with the usual variable syntax.

Expected cost: BigQuery's free tier covers this project (10 GiB storage and 1 TiB of queries
per month). Cloud Storage in `asia-northeast1` is not free-tier eligible, but the data is a
few tens of MB, so the cost is negligible. **Still set a budget alert first (step 2).**

## Pick a mode

| | With a billing account | BigQuery sandbox (no billing account) |
|---|---|---|
| Needs | Billing enabled on the project | Only a Google account and a project |
| Path | local Parquet -> GCS -> BigQuery | local Parquet -> BigQuery |
| Steps to follow | 1, 2, 3, 4, 5 | 1, then [Sandbox mode](#sandbox-mode-no-billing-account) |
| Load command | `fashion_ec.load` | `fashion_ec.load --sandbox` |
| Good for | everything, including Spark on Dataproc | Phases 2-3 (loading, dbt, Airflow) |

If the console says "You cannot create a budget on a closed account", the billing account
attached to your project is closed. Either attach an open billing account or use the sandbox.

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

## Sandbox mode (no billing account)

The BigQuery sandbox lets you use BigQuery without a billing account, so nothing can be
charged and no budget alert is needed. Do step 1 (sign in), skip steps 2 and 3, then:

```powershell
python -m fashion_ec.generate --out data/raw
python -m fashion_ec.load --project YOUR_PROJECT_ID --sandbox --dry-run
python -m fashion_ec.load --project YOUR_PROJECT_ID --sandbox
```

What `--sandbox` changes: Cloud Storage is not used (it needs billing), the files of each
table are merged and loaded directly, and tables are **not** date-partitioned (clustering is
kept). Everything else (explicit schemas, full replacement, row-count check) is the same.

Limits to know about. These come from third-party summaries of the sandbox page, so check
the [official sandbox documentation](https://cloud.google.com/bigquery/docs/sandbox) too:

- **Tables and partitions expire after 60 days.** Re-run the loader to recreate them; this is
  also why partitioning is off: the synthetic data covers past dates, and partitions are said
  to expire by their date.
- **10 GB storage and 1 TB of queries per month.** This project uses well under 1 GB.
- **DML and streaming are reported as unsupported** by third-party summaries. In the
  author's project the DML check below succeeded (2026-11-05), so dbt `incremental` and
  `snapshot` models are possible there; run the check on your own project before relying on it.
- Dataproc (Spark) and Cloud Storage need billing, so Phase 4 needs a billing account.

Check whether DML works in your project (it creates and removes a tiny table):

```powershell
bq query --use_legacy_sql=false --project_id=YOUR_PROJECT_ID "CREATE OR REPLACE TABLE raw.dml_check AS SELECT 1 AS x"
bq query --use_legacy_sql=false --project_id=YOUR_PROJECT_ID "INSERT INTO raw.dml_check (x) VALUES (2)"
bq rm -f -t YOUR_PROJECT_ID:raw.dml_check
```

If the second command fails with a message about DML or billing, DML is not available.

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
| `... does not match the expected schema ... re-run fashion_ec.generate` | `data/raw` was written by an older version of the generator. Regenerate it. |
| `Billing must be enabled` / bucket creation fails | No active billing account: use `--sandbox`. |
| Row count mismatch error | Do not ignore it: re-run, and compare the file count in `--dry-run` with GCS. |
