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

## Runs on Databricks Free Edition, on serverless

Free Edition is **serverless-only**: no cluster creation, no node types, no
`spark_version`, no Scala, one workspace per account. This bundle is built for
exactly that, which drives most of its non-obvious choices:

| Constraint | What it forces |
|---|---|
| No `new_cluster` anywhere | every task declares an `environment_key`; compute config lives in a job-level `environments:` block, not a cluster spec |
| Serverless wheel tasks ignore `libraries:` | dependencies (the wheel, pytest, chispa) go in `environments[].spec.dependencies` — this is the #1 reason a wheel task deploys fine and then can't import its own package |
| Environment version **5** = Databricks Connect 18 / Spark 4 / Python 3.12 | pinned in one bundle variable and in `pyproject.toml`'s `[tool.databricks.environment]`; the two must move together |
| **ANSI mode is on** (Spark 4 default) | landed strings are cast with `try_cast` in `ingest.conform()`, never with a plain `cast` that would kill the job for one bad row; `tests/conftest.py` turns ANSI on locally so the laptop and the job agree |
| `input_file_name()` unsupported | `_source_file` comes from `_metadata.file_path` |
| No account console: no groups, no service principals | one `platform_user` variable stands in for all of them — `run_as`, job permissions (`CAN_MANAGE`), grants, and the mask predicates, which compare `current_user()` instead of asking about group membership |
| No cluster to attach to | `get_spark()` returns the *existing* session; building one would try to set a static conf on a live Connect session |

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

# 4. create the catalog ONCE. Paste sql/00_bootstrap.sql into a SQL editor tab,
#    or run it via the Statement Execution API. It has no placeholders -- edit
#    the catalog name at the top if you want something other than dev_collections.
#    (At a real employer this half is Terraform's job, not the pipeline repo's.)

# 5. validate, deploy, run
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run bootstrap          -t dev   # schemas, volume, reference tables
databricks bundle run seed_synthetic     -t dev   # pathological data + reference tables
databricks bundle run unit_tests         -t dev
databricks bundle run collections_pipeline -t dev
databricks bundle run apply_governance   -t dev   # masks, row filters, grants
databricks bundle run smoke_test         -t dev
```

Order matters exactly twice: `bootstrap` and `seed_synthetic` come before the
pipeline (nothing to read otherwise), and `apply_governance` comes after it
(`ALTER TABLE ... SET MASK` needs the table to exist, and the pipeline's
`overwrite` replaces it).

### `${catalog}` in a .sql file

Bundle variable interpolation (`${var.catalog}`, `${workspace.file_path}`) is
resolved by the CLI **when it reads the bundle YAML**. It never reaches a file the
YAML merely points at. A `.sql` file full of `${catalog}` pasted into a SQL editor
is just a syntax error.

`sql/uc_governance.sql` is therefore a template rendered by
`collections_platform.sql_runner` — Python substitutes `${catalog}`,
`${bronze_schema}`, `${gold_schema}`, `${ops_schema}` and executes the file
statement by statement. An unknown placeholder raises instead of emitting broken
SQL, so a typo'd `${catalogue}` fails immediately rather than 40 statements later.

Two alternatives, and why they lost:

- **`USE CATALOG <name>;` + two-level names.** Simplest, and right for a file you
  run by hand. Loses because "one line to change per environment" is a line
  someone forgets, and CI can't catch it.
- **`IDENTIFIER(:catalog)` with parameter markers.** Genuinely supported for
  object names in Databricks SQL — but I'm not certain it works in every DDL
  position, and `ALTER TABLE ... SET MASK` is exactly where I'd want to verify
  before depending on it. Test it before you build on it.

`sql/00_bootstrap.sql` deliberately has **no** placeholders: it runs before the
bundle exists, so it has to be paste-able as-is.

### Who creates what, and why the split matters

| Object | Created by | Why there |
|---|---|---|
| metastore, storage credential, **catalog** | Terraform / metastore admin, once | needs metastore-level privilege. If the pipeline's service principal can `CREATE CATALOG`, then a bug — or anyone who can merge a PR — can create or drop catalogs in prod. |
| **schemas, volume** | `resources/schemas.yml` + `bootstrap` job | bundle-owned, so a fresh target works from one `deploy` |
| **fact/dim tables** | the jobs, via `saveAsTable` | schema follows the code that writes them |
| **reference tables** (`portfolios`, `client_contracts`, `forecast_curve`) | `bootstrap` declares, `seed_synthetic` fills | must exist *before* the first run, with explicit types — not inferred from whatever CSV landed first |
| **quarantine tables** | the first ingest run | their shape is "every landed column + `_dq_failures` + audit columns", derived from the ingest contract. Hand-writing that DDL gives you a second definition that drifts, and the append then fails on a schema mismatch. |
| **masks, row filters, grants** | `sql/uc_governance.sql` | enforced by UC on every read path, not by the pipeline |

### Layers

| Layer | What lives there |
|---|---|
| **bronze** | the landing volume — the raw originator files themselves. There is no bronze *table*: a copy of a CSV in Delta with no types and no DQ applied costs storage and buys nothing. |
| **silver** | `cases`, `payments` (typed, deduplicated, DQ-passed, PII policy applied) plus the three reference tables |
| **gold** | `fct_investing_performance`, `fct_servicing_performance`, `propensity_training_set` |
| **ops** | `cases_quarantine`, `payments_quarantine` — rejected rows, with the rules they broke |

One case feed produces **both** fact domains: a case carries an optional
`client_id`. Non-NULL means a third-party placement (Servicing); NULL means a
portfolio we bought (Investing). The Investing build drops placements because
they have no purchase date, and the Servicing build filters `client_id IS NOT
NULL` — `tests/test_transforms.py::test_owned_cases_never_enter_the_servicing_fact`
is the guard.

The pipeline jobs issue **no DDL at all**. That is deliberate: it means the
pipeline service principal never needs `CREATE` on the catalog, so a bug in a
transform cannot drop a schema. Least privilege is much easier to argue for when
the code layout already reflects it.

### The local loop

Two ways to run the suite locally, and `tests/conftest.py` handles both without
a branch in your test code:

```bash
# A. Against serverless, through Databricks Connect (same engine as the jobs).
#    The dependency group in pyproject.toml is managed by:
#      databricks environments setup-local
pytest -m "not smoke" -v

# B. Offline, against a local Spark. No workspace, no network.
pip install -e ".[dev,local-spark]"
pytest -m "not smoke" -v
```

`local-spark` and `databricks-connect` are **mutually exclusive** — both ship a
`pyspark` package, and installing both breaks the import. One per virtualenv.

Either way the suite runs with ANSI mode on and Python 3.12, matching serverless
environment version 5. The third place these same tests run is the `unit_tests`
job, on serverless itself.

---

## What each piece is actually demonstrating

| File | The point |
|---|---|
| `bootstrap.py` | DDL lives outside the pipeline. Every statement idempotent, and `verify()` checks it actually took effect — a `CREATE IF NOT EXISTS` that no-op'd because you were pointed at the wrong catalog is a real failure mode. |
| `pii.py` | HMAC-with-a-pepper, not a bare hash. A PESEL has too small a value space for `sha2()` to be irreversible. |
| `pii.py::assert_no_raw_pii` | A guard called before every gold write. Turns a compliance incident into a failed job. |
| `pii.py::assert_registry_covers` | Catches the *drift × PII* intersection: an originator adds `debtor_mobile` next quarter. |
| `sql/uc_governance.sql` | Column masks and row filters belong in Unity Catalog, not the pipeline — UC enforces on every read path. |
| `ingest.py` | Classifies drift into four kinds and handles each differently. `mergeSchema=true` handles exactly one of them correctly. Reads the landed CSV **as strings, by header** — handing the reader the expected schema makes drift undetectable (the frame then matches the contract by construction) and maps CSV by position, so one inserted column shifts every value. |
| `ingest.conform()` | `try_cast`, one step later, where a failure is visible. Under ANSI (the serverless default) a plain `cast` of one malformed value fails the whole job; `try_cast` costs you one quarantined row instead. |
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
| 10% of placements never contacted | SLA is `PENDING`, not `BREACHED` — collapsing the two overstates breaches |

The generator emits **exactly** the columns in `PORTFOLIO_CASE_RAW`, and
`tests/test_ingest_conform.py::test_generator_emits_exactly_the_contract`
asserts it. If the two drift apart, every ingest run fails the drift check —
a correct outcome, and an expensive way to discover you edited one of the two.

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
  It is also **not wired to a job**: serverless ships `mlflow-skinny`, so
  `mlflow.spark.log_model` would need a declared dependency, and Spark ML on
  serverless is worth verifying before promising it in a pipeline.
- No FX conversion, despite multi-currency data. Deliberate: it needs a rate
  dimension with an as-of join, which is the same SCD2 pattern as the commission rate.
- **No groups, no service principals, anywhere.** This workspace has one
  principal, so `prod` differs from `dev` by the catalog and the landing path
  and nothing else. Three things a paid workspace would change, all of them
  one-liners: `run_as` becomes a service principal (it survives people leaving),
  `permissions` name groups, and the mask/row-filter predicates go back to
  `is_account_group_member(...)`. The `REVOKE ALL PRIVILEGES ON SCHEMA
  silver/ops` pair — the half of the grant story that actually protects
  anything — is commented out for the same reason: with one principal it would
  revoke from the owner.
- `ALTER TABLE ... SET MASK` re-runs are unverified on environment version 5 —
  see the note in `sql/uc_governance.sql`.
