"""Render and execute a .sql template.

Why this module exists
----------------------
`${catalog}` in a .sql file is NOT interpolated by anything. Databricks Asset
Bundle variable substitution (`${var.catalog}`, `${bundle.target}`) happens when
the CLI reads the bundle YAML -- it never touches files the YAML merely points at.
So a SQL file full of `${catalog}` is just text with a syntax error in it.

Three ways out, and why this one:

  1. `USE CATALOG <name>;` at the top, two-level names below.
     Simplest. One line to change per environment. Good for a file you run by
     hand; bad for CI, because "one line to change" is a line someone forgets.

  2. `IDENTIFIER(:catalog)` with named parameter markers.
     Genuinely supported for object names in Databricks SQL -- but I am not
     certain it works in every DDL position, and `ALTER TABLE ... SET MASK` is
     exactly the kind of statement where I would want to test before relying on
     it. Verify before you build on it.

  3. Render the template in Python, then execute (this module).
     No engine-feature uncertainty, works identically from a job, a notebook or
     a test, and the renderer itself is unit-testable without a cluster.

(3) also keeps ONE definition of the catalog name: the bundle variable, passed to
the job, passed to this runner. No second place to forget.
"""

from __future__ import annotations

import re
from pathlib import Path

from .observability import log_event

_PLACEHOLDER = re.compile(r"\$\{(\w+)\}")


class UnresolvedPlaceholderError(Exception):
    """A `${name}` in the template had no value supplied."""


def render(template: str, variables: dict[str, str]) -> str:
    """Substitute `${name}` placeholders. Fails loudly on anything unresolved.

    Deliberately NOT str.format() or an f-string: SQL is full of braces
    (`ARRAY<STRUCT<...>>` is fine, but `{` shows up in JSON paths and string
    literals) and format() would either choke on them or silently eat them.
    A narrow regex over `${name}` only is the boring, correct choice.

    Deliberately NOT leaving unresolved placeholders in place: a typo'd
    `${catalogue}` must fail here, not produce SQL that runs against the wrong
    object or fails with a confusing parser error 40 statements later.
    """
    missing: set[str] = set()

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in variables:
            missing.add(name)
            return match.group(0)
        return variables[name]

    rendered = _PLACEHOLDER.sub(replace, template)
    if missing:
        raise UnresolvedPlaceholderError(
            f"No value supplied for {sorted(missing)}. Known: {sorted(variables)}"
        )
    return rendered


def split_statements(sql: str) -> list[str]:
    """Split a script into statements on top-level semicolons.

    `spark.sql()` executes ONE statement per call, so a multi-statement file has
    to be split. The naive `sql.split(";")` breaks the moment a semicolon appears
    inside a string literal -- and this repo has exactly that risk, because a
    MANAGED LOCATION or a comment can contain one.

    Handles: line comments (`--`), block comments, single-quoted strings with
    doubled-quote escapes, and backtick-quoted identifiers.
    """
    statements: list[str] = []
    current: list[str] = []
    i, n = 0, len(sql)
    in_string = in_backtick = in_line_comment = in_block_comment = False

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                current.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        if in_string:
            current.append(ch)
            if ch == "'":
                if nxt == "'":            # '' is an escaped quote, not a terminator
                    current.append(nxt)
                    i += 2
                    continue
                in_string = False
            i += 1
            continue

        if in_backtick:
            current.append(ch)
            if ch == "`":
                in_backtick = False
            i += 1
            continue

        if ch == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue
        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue
        if ch == "'":
            in_string = True
            current.append(ch)
            i += 1
            continue
        if ch == "`":
            in_backtick = True
            current.append(ch)
            i += 1
            continue
        if ch == ";":
            statements.append("".join(current).strip())
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return [s for s in statements if s]


def execute_file(spark, path: str | Path, variables: dict[str, str]) -> list[str]:
    """Render a .sql file and run it statement by statement.

    Returns the statements executed, so a caller or a test can assert on them.
    No transaction wraps this: Databricks DDL is not transactional, so a failure
    halfway leaves earlier statements applied. That is why every statement in
    this repo's SQL is idempotent -- re-running after a fix is the recovery
    procedure, and it has to be safe.
    """
    template = Path(path).read_text(encoding="utf-8")
    statements = split_statements(render(template, variables))

    for idx, statement in enumerate(statements, start=1):
        preview = " ".join(statement.split())[:100]
        try:
            spark.sql(statement)
        except Exception as exc:
            log_event(
                "sql.statement_failed",
                file=str(path),
                index=idx,
                total=len(statements),
                statement=preview,
                error=str(exc)[:300],
            )
            raise
        log_event("sql.statement_ok", file=str(path), index=idx, statement=preview)

    log_event("sql.file_done", file=str(path), statements=len(statements))
    return statements
