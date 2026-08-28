-- One-time bootstrap. Run BEFORE the first `bundle deploy`.
--
--   databricks sql -f sql/00_bootstrap.sql --warehouse-id <id>
--   (or paste into a SQL editor tab; every statement is idempotent)
--
-- ===========================================================================
-- Why the catalog is created HERE and not by the bundle
-- ===========================================================================
-- Creating a catalog needs CREATE CATALOG on the metastore. If the job service
-- principal that runs your pipeline holds that privilege, then a bug -- or
-- anyone who can merge a PR -- can create or drop catalogs in production.
--
-- The split that survives an audit:
--
--   metastore / catalog / storage credential  -> Terraform or a metastore admin,
--                                                once, outside the pipeline repo
--   schemas, volumes, tables                  -> the bundle (see resources/schemas.yml)
--   masks, row filters, grants                -> sql/uc_governance.sql
--
-- On a personal workspace you are the admin, so running the whole file yourself
-- is fine. Just know which half you would NOT own at a real employer.

-- ---------------------------------------------------------------------------
-- 1. Catalog
-- ---------------------------------------------------------------------------
-- MANAGED LOCATION is optional: without it the catalog inherits the metastore
-- root. Setting it explicitly is what lets you put a catalog holding personal
-- data in its own storage account with its own lifecycle and retention policy --
-- which is exactly the argument you want to be able to make about a catalog
-- holding debtor data.
--
-- On Free Edition, leave it off: custom workspace storage locations are not
-- available there, so the catalog uses the metastore root and that is the only
-- option. Catalog creation itself works.
CREATE CATALOG IF NOT EXISTS dev_collections
  COMMENT 'Debt collection platform - dev. Contains pseudonymised personal data.';

-- MANAGED LOCATION 'abfss://uc-data@<storageaccount>.dfs.core.windows.net/dev_collections'

USE CATALOG dev_collections;

-- ---------------------------------------------------------------------------
-- 2. Schemas
-- ---------------------------------------------------------------------------
-- Duplicated in resources/schemas.yml so the bundle is self-sufficient on a
-- fresh workspace. Both are idempotent; whichever runs second is a no-op.
CREATE SCHEMA IF NOT EXISTS bronze
  COMMENT 'Landed originator data. PII is pseudonymised on write - raw PII is never persisted.';

CREATE SCHEMA IF NOT EXISTS silver
  COMMENT 'Conformed, deduplicated, DQ-checked.';

CREATE SCHEMA IF NOT EXISTS gold
  COMMENT 'Investing and Servicing fact domains. Analyst-facing.';

CREATE SCHEMA IF NOT EXISTS ops
  COMMENT 'Quarantine, DQ metrics, run audit. Not analyst-facing.';

-- ---------------------------------------------------------------------------
-- 3. Volume for inbound files
-- ---------------------------------------------------------------------------
-- MANAGED, because on dev nobody else writes here. In production this is an
-- EXTERNAL volume over the SFTP drop the originators actually deliver to --
-- you do not want a copy step between "the file arrived" and "we can read it".
CREATE VOLUME IF NOT EXISTS bronze.landing
  COMMENT 'Raw portfolio and payment files from originators.';

-- ---------------------------------------------------------------------------
-- 4. Tables the pipeline does NOT create
-- ---------------------------------------------------------------------------
-- Everything the jobs write is created by saveAsTable(). These three are
-- reference/config data that has to exist before the first run, so they are
-- declared here with an explicit schema rather than being inferred from
-- whatever CSV someone uploaded first. `seed_synthetic` fills them on dev.

CREATE TABLE IF NOT EXISTS silver.portfolios (
  portfolio_id      STRING  NOT NULL COMMENT 'Natural key from the acquisition system',
  purchase_date     DATE    NOT NULL,
  purchase_price    DECIMAL(18,2)    COMMENT 'What we paid. Investing only.',
  gross_face_value  DECIMAL(18,2)    COMMENT 'Nominal value of the debt bought',
  seller_name       STRING,
  country_code      STRING
) COMMENT 'Bought NPL portfolios. Investing domain reference data.';

CREATE TABLE IF NOT EXISTS silver.client_contracts (
  client_id         STRING  NOT NULL,
  commission_rate   DECIMAL(9,6) NOT NULL COMMENT '0.15 = 15%',
  sla_target_days   INT              COMMENT 'Days from placement to first contact',
  valid_from        DATE    NOT NULL,
  valid_to          DATE             COMMENT 'NULL = current row'
) COMMENT 'SCD2. The rate that applied ON THE PAYMENT DATE, not today. See transform_servicing.py.';

CREATE TABLE IF NOT EXISTS silver.forecast_curve (
  portfolio_id      STRING  NOT NULL,
  months_on_book    INT     NOT NULL,
  forecast_pct      DECIMAL(9,6) NOT NULL COMMENT 'Cumulative % of face value expected by this month'
) COMMENT 'Underwriting recovery curve. Drives ERC. Investing only.';

-- The quarantine tables (`ops.cases_quarantine`, `ops.payments_quarantine`) are
-- deliberately NOT declared here. Their shape is "every landed column, plus
-- _dq_failures, plus the audit columns", which is derived from the ingest
-- contract -- writing that out by hand gives you a second definition that
-- drifts, and the append then fails on a schema mismatch. The first ingest run
-- creates them.

-- ---------------------------------------------------------------------------
-- 5. Verify
-- ---------------------------------------------------------------------------
-- The catalog's OWN information_schema, not `system.information_schema`: every
-- UC catalog exposes it to anyone who can use the catalog, whereas the system
-- catalog needs a separate grant and is not enabled on every workspace.
SELECT 'schemas' AS object, count(*) AS n
FROM   dev_collections.information_schema.schemata
WHERE  schema_name IN ('bronze','silver','gold','ops')
UNION ALL
SELECT 'volumes', count(*)
FROM   dev_collections.information_schema.volumes;
-- Expect: schemas = 4, volumes = 1
