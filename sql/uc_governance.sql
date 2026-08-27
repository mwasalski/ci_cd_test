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
-- Placeholders: ${catalog}, ${silver_schema}, ${gold_schema}, ${ops_schema},
--               ${analyst_principal}, ${ops_group}, ${engineer_group}
-- An unknown placeholder makes the runner fail loudly rather than emit broken SQL.
--
-- ===========================================================================
-- WHY THE PRINCIPALS ARE PARAMETERS
-- ===========================================================================
-- Databricks Free Edition has no account console and therefore no account
-- groups. `GRANT ... TO \`data-analysts\`` fails there with "principal not
-- found", which would make this whole file unrunnable on the one edition it is
-- meant to run on. So the principal is a bundle variable: the deploying user on
-- dev, a group on a real workspace.
--
-- `is_account_group_member('<group that does not exist>')` returns false rather
-- than failing, so on Free Edition the masks stay ON for everyone -- the safe
-- direction to fail, and worth knowing before you conclude "the mask is broken".
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
         WHEN is_account_group_member('${ops_group}') THEN phone
         ELSE concat('***', right(regexp_replace(phone, '[^0-9]', ''), 4))
       END;

CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.mask_pseudonym(p STRING)
RETURN CASE
         WHEN is_account_group_member('${engineer_group}') THEN p
         ELSE NULL   -- analysts get aggregates, not join keys
       END;

-- Applied to silver.cases, where the pseudonymised/masked columns actually live
-- (apply_pii_policy runs on write to silver, so raw PII is never persisted
-- anywhere in this catalog).
--
-- NOTE, and I have not verified this on environment version 5: the pipeline
-- writes silver.cases with mode=overwrite, which replaces the table, and a
-- replaced table does not keep its masks. That is exactly why this job runs
-- AFTER the pipeline rather than once at setup. If a re-run ever fails with
-- "column already has a mask", put an `ALTER TABLE ... ALTER COLUMN ... DROP
-- MASK` in front of each SET -- there is no DROP MASK IF EXISTS.
ALTER TABLE ${catalog}.${silver_schema}.cases
  ALTER COLUMN debtor_phone_masked SET MASK ${catalog}.${gold_schema}.mask_phone;

ALTER TABLE ${catalog}.${silver_schema}.cases
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
GRANT USE CATALOG ON CATALOG ${catalog} TO `${analyst_principal}`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.${gold_schema} TO `${analyst_principal}`;

-- Analysts get gold and nothing else. silver holds pseudonym join keys; ops
-- holds rejected rows that never passed DQ -- an analyst querying either will
-- produce a number that is wrong in a way nobody can reproduce.
--
-- REVOKE only removes explicit grants: it never takes away privileges the
-- principal holds by owning the object, so running this as yourself on Free
-- Edition does not lock you out of your own schemas.
REVOKE ALL PRIVILEGES ON SCHEMA ${catalog}.${silver_schema} FROM `${analyst_principal}`;
REVOKE ALL PRIVILEGES ON SCHEMA ${catalog}.${ops_schema}    FROM `${analyst_principal}`;

-- ---------------------------------------------------------------------------
-- 4. Verify the masks actually attached
-- ---------------------------------------------------------------------------
-- An ALTER TABLE that silently did not apply is a real failure mode -- check,
-- do not assume. The catalog's own information_schema, not `system.`: that one
-- needs a separate grant and is not available on every workspace.
SELECT table_schema, table_name, column_name, mask_name
FROM   ${catalog}.information_schema.column_masks;
