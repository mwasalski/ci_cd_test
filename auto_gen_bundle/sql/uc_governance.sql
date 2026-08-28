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
--               ${platform_user}
-- An unknown placeholder makes the runner fail loudly rather than emit broken SQL.
--
-- ===========================================================================
-- ONE PRINCIPAL, NO GROUPS
-- ===========================================================================
-- Free Edition has no account console, so there are no account groups and no
-- service principals -- just one human. `GRANT ... TO `data-analysts`` fails
-- there with "principal not found", and `is_account_group_member('anything')`
-- can only ever answer false.
--
-- So the predicates ask `current_user()` instead. The masks and the row filter
-- are real, attached to real columns, and enforced by UC on every read path --
-- they are simply transparent to the one user who owns the data. On a workspace
-- with groups you swap one function body:
--
--   current_user() = '${platform_user}'   ->   is_account_group_member('collections-ops')
--
-- and nothing else in this file changes. That substitution is the whole
-- difference between this and a "real" governance setup.
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
         WHEN current_user() = '${platform_user}' THEN phone
         ELSE concat('***', right(regexp_replace(phone, '[^0-9]', ''), 4))
       END;

CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.mask_pseudonym(p STRING)
RETURN CASE
         WHEN current_user() = '${platform_user}' THEN p
         ELSE NULL   -- everyone else gets aggregates, not join keys
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
--
-- With groups this reads `is_account_group_member(concat('client-',
-- lower(client_id)))` -- per-client visibility from one function. Without them
-- the honest version is default-deny: the platform user sees everything, and
-- anyone added to this workspace later sees nothing until the predicate is
-- widened deliberately.
CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.client_row_filter(client_id STRING)
RETURN current_user() = '${platform_user}';

ALTER TABLE ${catalog}.${gold_schema}.fct_servicing_performance
  SET ROW FILTER ${catalog}.${gold_schema}.client_row_filter ON (client_id);

-- ---------------------------------------------------------------------------
-- 3. Grants
-- ---------------------------------------------------------------------------
-- On a workspace with exactly one principal there is nothing to grant: the
-- platform user owns every securable here and holds every privilege by
-- ownership. These two statements are therefore no-ops that succeed -- kept
-- because they document the intended read surface, and because the moment a
-- second principal exists they are the lines you edit.
GRANT USE CATALOG ON CATALOG ${catalog} TO `${platform_user}`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog}.${gold_schema} TO `${platform_user}`;

-- What is deliberately NOT here: `REVOKE ALL PRIVILEGES ON SCHEMA silver/ops
-- FROM <analyst group>`. With groups, that is the important half -- analysts get
-- gold and nothing else, because silver holds pseudonym join keys and ops holds
-- rows that never passed DQ. With one principal it would mean revoking from the
-- owner: at best a no-op, at worst an error, and either way theatre. Add it back
-- the same day you add the second user.

-- ---------------------------------------------------------------------------
-- 4. Verify the masks actually attached
-- ---------------------------------------------------------------------------
-- An ALTER TABLE that silently did not apply is a real failure mode -- check,
-- do not assume. The catalog's own information_schema, not `system.`: that one
-- needs a separate grant and is not available on every workspace.
SELECT table_schema, table_name, column_name, mask_name
FROM   ${catalog}.information_schema.column_masks;
