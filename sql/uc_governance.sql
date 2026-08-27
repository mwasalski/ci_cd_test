-- Unity Catalog governance: column masks + row filters.
--
-- Why this is NOT done in the pipeline:
-- a pipeline-level mask protects only the rows the pipeline writes. A UC column
-- mask is enforced by the engine on every read path -- SQL warehouse, notebook,
-- Power BI, JDBC. If an analyst runs `SELECT *`, the mask still applies.
--
-- Rule of thumb:
--   pipeline  -> irreversible transformation (pseudonymise, redact)
--   UC mask   -> reversible, identity-dependent presentation
--
-- Run once per catalog: databricks sql -f sql/uc_governance.sql

-- ---------------------------------------------------------------------------
-- 1. Masking function. Returns the real value only to the collections-ops group.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ${catalog}.gold.mask_phone(phone STRING)
RETURN CASE
         WHEN is_account_group_member('collections-ops') THEN phone
         ELSE concat('***', right(regexp_replace(phone, '[^0-9]', ''), 4))
       END;

CREATE OR REPLACE FUNCTION ${catalog}.gold.mask_pseudonym(p STRING)
RETURN CASE
         WHEN is_account_group_member('data-engineers') THEN p
         ELSE NULL   -- analysts get aggregates, not join keys
       END;

-- Applied to bronze.cases, which is where the pseudonymised/masked columns
-- actually live (apply_pii_policy runs on write to bronze, so raw PII is never
-- persisted anywhere in this catalog).
ALTER TABLE ${catalog}.bronze.cases
  ALTER COLUMN debtor_phone_masked SET MASK ${catalog}.gold.mask_phone;

ALTER TABLE ${catalog}.bronze.cases
  ALTER COLUMN national_id_pseudonym SET MASK ${catalog}.gold.mask_pseudonym;

-- The quarantine table gets the same treatment. It is the one people forget,
-- and the one an engineer is most likely to `SELECT *` from while debugging.
ALTER TABLE ${catalog}.ops.cases_quarantine
  ALTER COLUMN national_id_pseudonym SET MASK ${catalog}.gold.mask_pseudonym;

-- ---------------------------------------------------------------------------
-- 2. Row filter: an analyst assigned to one client must not see another
--    client's cases. Relevant on the Servicing side, where the data belongs to
--    the client, not to us.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ${catalog}.gold.client_row_filter(client_id STRING)
RETURN is_account_group_member('servicing-all-clients')
       OR is_account_group_member(concat('client-', lower(client_id)));

ALTER TABLE ${catalog}.gold.fct_servicing_performance
  SET ROW FILTER ${catalog}.gold.client_row_filter ON (client_id);

-- ---------------------------------------------------------------------------
-- 3. Grants. Note gold is readable, bronze is not -- raw PII lives in bronze.
-- ---------------------------------------------------------------------------
GRANT USE CATALOG ON CATALOG ${catalog} TO `data-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.gold TO `data-analysts`;

-- Analysts get gold and nothing else. bronze holds pseudonym join keys; ops
-- holds rejected rows that never passed DQ -- an analyst querying either will
-- produce a number that is wrong in a way nobody can reproduce.
REVOKE ALL PRIVILEGES ON SCHEMA ${catalog}.bronze FROM `data-analysts`;
REVOKE ALL PRIVILEGES ON SCHEMA ${catalog}.ops    FROM `data-analysts`;

-- ---------------------------------------------------------------------------
-- 4. Verify the masks are actually attached. Run this in the smoke job too --
--    an ALTER TABLE that silently did not apply is a real failure mode.
-- ---------------------------------------------------------------------------
-- SELECT table_name, column_name, mask_name
-- FROM   system.information_schema.column_masks
-- WHERE  table_catalog = '${catalog}';
