-- Unity Catalog governance: column masks, row filters, grants.
--
-- ===========================================================================
-- HOW ${catalog} GETS ITS VALUE
-- ===========================================================================
-- It is substituted by `collections_platform.sql_runner.render()`, NOT by the
-- Databricks CLI. Bundle variable interpolation (`${var.catalog}`) applies only
-- to the bundle YAML -- it never reaches a file the YAML points at. A `.sql`
-- file full of `${catalog}` handed straight to a SQL editor is a syntax error.
--
-- Run it:
--     databricks bundle run apply_governance -t dev
--
-- Run it by hand instead (e.g. pasting into a SQL editor): replace the
-- placeholders yourself, or put `USE CATALOG dev_collections;` at the top and
-- delete the `${catalog}.` prefixes.
--
-- Available placeholders: ${catalog}, ${bronze_schema}, ${gold_schema}, ${ops_schema}
-- An unknown placeholder makes the runner fail loudly rather than emit broken SQL.
--
-- ===========================================================================
-- WHY THIS IS NOT DONE IN THE PIPELINE
-- ===========================================================================
-- A pipeline-level mask protects only the rows that pipeline writes. A UC column
-- mask is enforced by the engine on every read path -- SQL warehouse, notebook,
-- Power BI, JDBC. If an analyst runs `SELECT *`, the mask still applies.
--
--   pipeline  -> irreversible transformation (pseudonymise, redact)
--   UC mask   -> reversible, identity-dependent presentation

-- ---------------------------------------------------------------------------
-- 1. Masking functions
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.mask_phone(phone STRING)
RETURN CASE
         WHEN is_account_group_member('collections-ops') THEN phone
         ELSE concat('***', right(regexp_replace(phone, '[^0-9]', ''), 4))
       END;

CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.mask_pseudonym(p STRING)
RETURN CASE
         WHEN is_account_group_member('data-engineers') THEN p
         ELSE NULL   -- analysts get aggregates, not join keys
       END;

-- Applied to bronze.cases, where the pseudonymised/masked columns actually live
-- (apply_pii_policy runs on write to bronze, so raw PII is never persisted
-- anywhere in this catalog).
ALTER TABLE ${catalog}.${bronze_schema}.cases
  ALTER COLUMN debtor_phone_masked SET MASK ${catalog}.${gold_schema}.mask_phone;

ALTER TABLE ${catalog}.${bronze_schema}.cases
  ALTER COLUMN national_id_pseudonym SET MASK ${catalog}.${gold_schema}.mask_pseudonym;

-- The quarantine table gets the same treatment. It is the one people forget,
-- and the one an engineer is most likely to `SELECT *` from while debugging.
ALTER TABLE ${catalog}.${ops_schema}.cases_quarantine
  ALTER COLUMN national_id_pseudonym SET MASK ${catalog}.${gold_schema}.mask_pseudonym;

-- ---------------------------------------------------------------------------
-- 2. Row filter
-- ---------------------------------------------------------------------------
-- An analyst assigned to one client must not see another client's cases.
-- Relevant on the Servicing side, where the data belongs to the client, not us.
CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.client_row_filter(client_id STRING)
RETURN is_account_group_member('servicing-all-clients')
       OR is_account_group_member(concat('client-', lower(client_id)));

ALTER TABLE ${catalog}.${gold_schema}.fct_servicing_performance
  SET ROW FILTER ${catalog}.${gold_schema}.client_row_filter ON (client_id);

-- ---------------------------------------------------------------------------
-- 3. Grants
-- ---------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG ${catalog} TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.${gold_schema} TO `data-analysts`;

-- Analysts get gold and nothing else. bronze holds pseudonym join keys; ops
-- holds rejected rows that never passed DQ -- an analyst querying either will
-- produce a number that is wrong in a way nobody can reproduce.
REVOKE ALL PRIVILEGES ON SCHEMA ${catalog}.${bronze_schema} FROM `data-analysts`;
REVOKE ALL PRIVILEGES ON SCHEMA ${catalog}.${ops_schema}    FROM `data-analysts`;

-- ---------------------------------------------------------------------------
-- 4. Verify the masks actually attached
-- ---------------------------------------------------------------------------
-- An ALTER TABLE that silently did not apply is a real failure mode -- check,
-- do not assume. Worth promoting into the smoke job.
SELECT table_schema, table_name, column_name, mask_name
FROM   system.information_schema.column_masks
WHERE  table_catalog = '${catalog}';
