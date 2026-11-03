# dbt project

The dbt project in [`dbt/`](../dbt) models the raw tables loaded by `fashion_ec.load`.
It uses `dbt-bigquery`, authenticates with Application Default Credentials and contains no
secrets (see [`gcp-setup.md`](gcp-setup.md) for sign-in).

## Run it (Windows PowerShell)

```powershell
python tasks.py install          # installs dbt-core and dbt-bigquery too
$env:GCP_PROJECT_ID = "YOUR_PROJECT_ID"
cd dbt
dbt debug --profiles-dir .       # checks the connection
dbt build --profiles-dir .       # builds the models and runs all tests
```

| Setting | Environment variable | Default |
|---|---|---|
| GCP project | `GCP_PROJECT_ID` | required |
| Raw dataset (source) | `BQ_RAW_DATASET` | `raw` |
| Output dataset prefix | `DBT_DATASET` | `analytics` |
| Location | `BQ_LOCATION` | `asia-northeast1` |

Models are built into `<DBT_DATASET>_staging` (and later `_intermediate`, `_marts`). Use a
different `DBT_DATASET` for experiments. The profile also sets `maximum_bytes_billed` to
1 GB, so a runaway query fails instead of being billed.

Useful commands: `dbt build --select stg_orders+` (a model and everything after it),
`dbt test --select stg_returns`, `dbt ls --resource-type test`.

## What is in the project

```
dbt/
  dbt_project.yml
  profiles.yml                  # connection profile, no secrets
  macros/jst_date.sql           # the one place that converts UTC to Japan time
  models/staging/
    _sources.yml                # the 8 raw tables
    _staging.yml                # model docs and column tests
    stg_*.sql                   # one view per raw table
  tests/
    generic/                    # reusable tests: is_positive, is_non_negative, unique_combination
    assert_*.sql                # business-rule tests across models
```

### Staging conventions

- One model per raw table; renames, light cleanup and nothing else. **No joins.**
- Names carry the unit: `_jpy` for yen, `_units` for counts, `_at` for UTC timestamps and
  `_on_jst` for business dates in Japan time.
- Business dates are always made with `{{ jst_date('column') }}`. Calling `date()` on a UTC
  timestamp puts orders placed between 00:00 and 08:59 JST on the previous day.
- Staging models are views: always current, no storage.

### Tests

63 generic tests (unique, not null, accepted values, relationships between tables, positive
amounts, unique column combinations) and 5 business-rule tests:

| Test | Rule |
|---|---|
| `assert_no_returns_for_cancelled_orders` | A cancelled order never shipped |
| `assert_returns_happen_after_the_order` | A return follows its order |
| `assert_refund_equals_line_amount` | Returns are full refunds |
| `assert_nothing_sold_before_launch` | A SKU is sold on or after its launch date (JST) |
| `assert_every_style_has_one_launch_price` | Price history starts with exactly one launch row |

## Checks that need no BigQuery

`dbt compile` needs a warehouse connection, so CI checks the project without one:

- `tests/test_dbt_models.py` renders every model with its macros, parses it as BigQuery SQL
  and checks every column against the raw schema (`src/fashion_ec/schemas.py`) and the
  upstream models. A misspelled column, an undocumented column, a missing description or
  a model without a uniqueness test fails the build.
- The CI job `dbt-parse` runs `dbt parse --warn-error`, which validates the YAML, Jinja,
  references and deprecations.

These catch mistakes in structure and names. They cannot prove that the data satisfies the
tests; `dbt build` against BigQuery does that.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `Env var required but not provided: 'GCP_PROJECT_ID'` | Set `$env:GCP_PROJECT_ID` in the same PowerShell window. |
| `Your default credentials were not found` | Run `gcloud auth application-default login`. |
| `Not found: Dataset ...:raw` | Load the raw data first: `python -m fashion_ec.load --sandbox`. |
| `Not found: Table ...raw.<table>` after a while | Sandbox tables expire after 60 days; run the loader again. |
| `Access Denied: ... bigquery.jobs.create` | The signed-in account has no access to the project. |
| Dataset location error | The raw dataset and `BQ_LOCATION` must match (`asia-northeast1`). |
