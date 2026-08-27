# collections_platform

A deliberately small, deliberately opinionated Databricks data platform for a
debt-collection business with **two business models and two data models**:

| | **Investing** | **Servicing** |
|---|---|---|
| What it is | We buy NPL portfolios and collect on our **own balance sheet** | We collect **on behalf of a client** |
| Grain | portfolio × as-of month | client × case × as-of month |
| Measures | purchase price, ERC, recovery rate, money multiple | commission rate, commission earned, SLA |
| Does *not* have | client, commission, SLA | purchase price, ERC, money multiple |

Forcing these into one `fct_collections` destroys the grain and produces measures
that are meaningless for half the rows. `tests/test_transforms.py::test_fact_domains_do_not_share_measures`
is a contract test that fails if anyone tries.

---

## Quick start

```bash
# 1. auth
databricks configure                     # or set DATABRICKS_HOST / _TOKEN

# 2. the pepper used for pseudonymisation -- NEVER a literal in code
databricks secrets create-scope collections
databricks secrets put-secret collections pii_pepper

# 3. the CLI builds the wheel itself (see `artifacts:` in databricks.yml),
#    so the build backend just has to be importable
pip install build

# 4. validate, deploy, run
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run seed_synthetic     -t dev   # generate pathological data
databricks bundle run unit_tests         -t dev
databricks bundle run collections_pipeline -t dev
databricks bundle run smoke_test         -t dev
```

Local loop (faster, less faithful):

```bash
pip install -e ".[dev]"
pytest -m "not smoke" -v
```

---

## What each piece is actually demonstrating

| File | The point |
|---|---|
| `pii.py` | HMAC-with-a-pepper, not a bare hash. A PESEL has too small a value space for `sha2()` to be irreversible. |
| `pii.py::assert_no_raw_pii` | A guard called before every gold write. Turns a compliance incident into a failed job. |
| `pii.py::assert_registry_covers` | Catches the *drift × PII* intersection: an originator adds `debtor_mobile` next quarter. |
| `sql/uc_governance.sql` | Column masks and row filters belong in Unity Catalog, not the pipeline — UC enforces on every read path. |
| `ingest.py` | Classifies drift into four kinds and handles each differently. `mergeSchema=true` handles exactly one of them correctly. |
| `dq.py` | Quarantine, not drop. Per-rule severity. NULL-safe conditions. Single-pass metrics. A circuit breaker on the quarantine rate. |
| `features.py` | Two implementations of the same features — the leaky one and the point-in-time one — so the difference is testable. |
| `spark_utils.py` | AQE first, salting only for the cases AQE does not cover (skewed aggregations and windows). |
| `train.py` | Logs the Delta version of the source table, so "which data did this model see" has an answer. |

---

## The tests, ranked by how much they'd matter in production

1. **`test_features_leakage.py`** — build features at T, add a payment after T, rebuild,
   assert nothing changed. Also runs the same test against the known-leaky
   implementation and asserts it *fails*, so the test is proven to have teeth.
2. **`test_pii.py::test_spark_hmac_matches_python_hmac`** — proves the hand-rolled
   Spark HMAC really is HMAC. Hand-rolled crypto without an equivalence test is a liability.
3. **`test_dq.py::test_null_condition_counts_as_failure`** — `NULL > 0` is `NULL`,
   not `False`. Without the `coalesce`, bad rows pass silently.
4. **`test_ingest_drift.py::test_type_change_fails`** — the drift `mergeSchema`
   accepts and shouldn't.
5. **`test_transforms.py::test_commission_uses_rate_valid_at_payment_time`** — the
   SCD2 join. Getting it wrong silently restates historical revenue.

## Pathological data

`synthetic.py` generates data that is wrong on purpose. Every knob maps to a
production failure mode:

| Knob | Failure mode |
|---|---|
| `skew_factor=0.4` | one portfolio holds 40% of cases |
| `null_rate` | NULLs in the join key — breaks naive joins and collapses in `dropDuplicates` |
| `dup_rate` | originator re-sends the same file |
| 1% of balances × 100 | grosze recorded as złoty — a unit error DQ must catch |
| 0.5% `1970-01-01` | epoch-zero leaking through a bad cast |
| 0.2% currency `XXX` | unmapped currency silently breaking FX aggregates |
| payments before `default_date` | late-arriving / backdated corrections |

---

## Things this repo deliberately does NOT do

- **No `mergeSchema=true`.** See `ingest.py`.
- **No partitioning by default.** Under ~1 TB, liquid clustering or plain Delta
  beats manual partitioning; partitioning by day is how you get 10,000 tiny files.
- **No `DoubleType` for money.** `DecimalType(18,2)`, everywhere.
- **No `dropDuplicates()`.** It keeps an arbitrary row, so two runs over the same
  input can differ. `dedupe()` uses an explicit ordering.
- **No `current_date()` inside transform logic.** `as_of_date` is a job parameter,
  which is what makes a backfill reproducible.
- **No DLT.** Not because DLT is wrong — its expectations would replace `dq.py`
  outright — but because plain wheel tasks have no equivalent and this shows what
  DLT is doing for you.

## Known gaps (on purpose — these are the next exercises)

- No incremental/CDC load. Everything is `overwrite`. Adding a `MERGE` with a
  watermark, and a test for late-arriving data, is the obvious next step.
- No Delta Live Tables variant.
- `train.py` is the thinnest possible model — the DE surface is what matters here.
- No FX conversion, despite multi-currency data. Deliberate: it needs a rate
  dimension with an as-of join, which is the same SCD2 pattern as the commission rate.
