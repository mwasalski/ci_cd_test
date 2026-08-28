"""SQL template renderer + statement splitter.

Pure Python, no Spark. These are the tests that stop a governance script from
silently running against the wrong catalog.
"""

from __future__ import annotations

import pytest

from collections_platform.sql_runner import (
    UnresolvedPlaceholderError,
    render,
    split_statements,
)


# ---------------------------------------------------------------------------
# render
# ---------------------------------------------------------------------------
def test_substitutes_placeholders():
    out = render("SELECT * FROM ${catalog}.${gold_schema}.t", {"catalog": "dev", "gold_schema": "gold"})
    assert out == "SELECT * FROM dev.gold.t"


def test_substitutes_every_occurrence():
    out = render("${catalog}.a ${catalog}.b", {"catalog": "dev"})
    assert out == "dev.a dev.b"


def test_unresolved_placeholder_raises():
    """A typo'd `${catalogue}` must fail HERE, not produce SQL that runs against
    the wrong object or blows up with a confusing parser error 40 statements in."""
    with pytest.raises(UnresolvedPlaceholderError, match="catalogue"):
        render("SELECT ${catalogue}.t", {"catalog": "dev"})


def test_error_lists_all_missing_names_at_once():
    with pytest.raises(UnresolvedPlaceholderError) as exc:
        render("${a} ${b} ${catalog}", {"catalog": "dev"})
    assert "'a'" in str(exc.value) and "'b'" in str(exc.value)


def test_braces_that_are_not_placeholders_survive():
    """SQL contains braces that must not be touched. This is why the renderer is
    a narrow regex over `${name}` and not str.format()."""
    sql = "SELECT from_json(c, 'STRUCT<a: INT>'), '{\"k\": 1}' FROM ${catalog}.t"
    out = render(sql, {"catalog": "dev"})
    assert '{"k": 1}' in out
    assert "dev.t" in out


def test_empty_variables_still_validates():
    with pytest.raises(UnresolvedPlaceholderError):
        render("${catalog}", {})


# ---------------------------------------------------------------------------
# split_statements
# ---------------------------------------------------------------------------
def test_splits_on_semicolons():
    assert split_statements("SELECT 1; SELECT 2;") == ["SELECT 1", "SELECT 2"]


def test_trailing_statement_without_semicolon_is_kept():
    assert split_statements("SELECT 1; SELECT 2") == ["SELECT 1", "SELECT 2"]


def test_semicolon_inside_a_string_literal_does_not_split():
    """The bug `sql.split(';')` has. A MANAGED LOCATION or a COMMENT can contain
    a semicolon, and splitting there produces two invalid statements."""
    sql = "CREATE SCHEMA s COMMENT 'first; second'; SELECT 1;"
    out = split_statements(sql)
    assert len(out) == 2
    assert "first; second" in out[0]


def test_escaped_quote_inside_string():
    sql = "SELECT 'it''s fine; really'; SELECT 2;"
    out = split_statements(sql)
    assert len(out) == 2
    assert "it''s fine; really" in out[0]


def test_semicolon_inside_backticked_identifier():
    sql = "SELECT `weird;name` FROM t; SELECT 2;"
    assert len(split_statements(sql)) == 2


def test_line_comments_are_stripped():
    sql = "-- a comment with ; a semicolon\nSELECT 1;"
    out = split_statements(sql)
    assert out == ["SELECT 1"]


def test_block_comments_are_stripped():
    sql = "/* multi\n line ; comment */ SELECT 1;"
    assert split_statements(sql) == ["SELECT 1"]


def test_empty_statements_are_dropped():
    """A trailing `;;` or a file ending in a newline must not produce an empty
    statement that spark.sql() would reject."""
    assert split_statements("SELECT 1;;\n\n;") == ["SELECT 1"]


def test_create_function_body_survives():
    """The real shape in uc_governance.sql: a CASE body with no internal
    semicolons, terminated by one."""
    sql = (
        "CREATE OR REPLACE FUNCTION c.g.mask(p STRING)\n"
        "RETURN CASE WHEN is_account_group_member('de') THEN p ELSE NULL END;\n"
        "SELECT 1;"
    )
    out = split_statements(sql)
    assert len(out) == 2
    assert out[0].startswith("CREATE OR REPLACE FUNCTION")
    assert out[0].endswith("END")


def test_render_then_split_round_trip():
    sql = """
    -- governance
    CREATE OR REPLACE FUNCTION ${catalog}.${gold_schema}.f(p STRING)
    RETURN CASE WHEN is_account_group_member('de') THEN p ELSE NULL END;

    ALTER TABLE ${catalog}.${bronze_schema}.cases
      ALTER COLUMN national_id_pseudonym SET MASK ${catalog}.${gold_schema}.f;
    """
    out = split_statements(
        render(sql, {"catalog": "dev", "gold_schema": "gold", "bronze_schema": "bronze"})
    )
    assert len(out) == 2
    assert "dev.gold.f" in out[0]
    assert "dev.bronze.cases" in out[1]
    assert "${" not in " ".join(out)
