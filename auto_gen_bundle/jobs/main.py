"""Dispatcher for the file-based (`py_*`) jobs -- the no-wheel variant.

WHY THIS FILE EXISTS
--------------------
`spark_python_task` runs a .py file as a script. Python then puts *that file's
directory* on sys.path[0], which is not the same thing as installing a package:

    $ python src/collections_platform/entrypoints.py --catalog dev
    ImportError: attempted relative import with no known parent package

`from .config import parse_args` has no parent package to resolve against, and
`import collections_platform` fails too, because what landed on sys.path is the
*inside* of the package rather than `src/`. So before importing anything, this
file puts `src/` on the path itself. That one line is precisely the job a wheel
does for you.

The path is derived from `__file__`, not hardcoded: the bundle syncs this repo
to a different workspace directory per target, and a literal
`/Workspace/Users/.../files/src` would be a fourth place to remember to edit.

WHAT YOU GIVE UP VERSUS THE WHEEL JOBS
--------------------------------------
  * identity -- a wheel is one file with a version; this is "whatever is in the
    workspace directory right now", which anyone can edit in the UI without a PR;
  * dependencies -- `[project.dependencies]` is not installed for you, so every
    library has to be repeated in the job's `environments` block;
  * a checked entry point -- `entry_point: apply-governance` is resolved from
    wheel metadata, whereas a typo in `--entrypoint` here surfaces at run time.

WHAT YOU GET
------------
No build step. Edit, `bundle deploy`, run. That is a real advantage while you are
still shaping a job, and a real liability once other people depend on it.

Usage (this is what the py_* job YAML passes):

    main.py --entrypoint apply-governance --catalog dev_collections ...
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Must happen before importing collections_platform. Keep it above the import,
# and keep the `noqa` -- ruff/isort would otherwise "fix" this file into one
# that cannot run.
_SRC = Path(__file__).resolve().parent.parent / "src"
if not _SRC.is_dir():
    # Without this the failure is `ModuleNotFoundError: collections_platform`,
    # which sends you looking at the code instead of at the deploy.
    raise RuntimeError(
        f"Expected the source tree at {_SRC}, and it is not there. The py_* jobs "
        f"run from the files the bundle syncs, so src/ has to be part of that sync "
        f"-- check that it is not excluded from the bundle."
    )
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from collections_platform import entrypoints  # noqa: E402

# The same names the wheel exposes through [project.scripts] in pyproject.toml.
# Keeping the strings identical means the py_* and wheel jobs stay comparable
# line by line -- and that a task can be switched from one to the other by
# editing YAML only.
ENTRYPOINTS = {
    "bootstrap-catalog": entrypoints.bootstrap_catalog,
    "apply-governance": entrypoints.apply_governance,
    "ingest-portfolio": entrypoints.ingest_portfolio,
    "ingest-payments": entrypoints.ingest_payments,
    "build-investing": entrypoints.build_investing,
    "build-servicing": entrypoints.build_servicing,
    "build-features": entrypoints.build_features,
    "train-propensity": entrypoints.train_propensity,
    "run-tests": entrypoints.run_tests,
    "run-smoke": entrypoints.run_smoke,
    "seed-synthetic": entrypoints.seed_synthetic,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--entrypoint", required=True, choices=sorted(ENTRYPOINTS))
    # Everything else belongs to the entry point, and is forwarded untouched.
    # Parsing it here as well would mean two definitions of every flag.
    ns, rest = parser.parse_known_args(argv)
    ENTRYPOINTS[ns.entrypoint](rest)


if __name__ == "__main__":
    main()
