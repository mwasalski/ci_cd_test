"""The two job variants must stay interchangeable.

`resources/*.job.yml` reaches the code through the wheel's entry points;
`resources/py_*.job.yml` reaches it through jobs/main.py. If those two lists of
names drift apart, the failure lands in a job run -- late, and on serverless,
where the feedback loop is minutes rather than milliseconds.

No Spark here on purpose: this is a wiring contract, not a data test.
"""

from __future__ import annotations

import importlib.util
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_dispatcher():
    spec = importlib.util.spec_from_file_location(
        "jobs_main", REPO_ROOT / "jobs" / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["jobs_main"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def dispatcher():
    return _load_dispatcher()


@pytest.fixture(scope="module")
def console_scripts() -> dict[str, str]:
    with (REPO_ROOT / "pyproject.toml").open("rb") as fh:
        return tomllib.load(fh)["project"]["scripts"]


def test_dispatcher_covers_every_console_script(dispatcher, console_scripts):
    """One name list, two delivery mechanisms."""
    assert set(dispatcher.ENTRYPOINTS) == set(console_scripts)


def test_dispatcher_targets_match_the_console_scripts(dispatcher, console_scripts):
    """Same name AND same function -- a dispatcher that quietly points
    `build-features` at `build_investing` would run, and be wrong."""
    for name, target in console_scripts.items():
        module_path, _, func_name = target.partition(":")
        assert module_path == "collections_platform.entrypoints"
        assert dispatcher.ENTRYPOINTS[name].__name__ == func_name


def _job_files(prefix: str) -> list[Path]:
    return sorted(
        p
        for p in (REPO_ROOT / "resources").glob("*.job.yml")
        if p.name.startswith("py_") is (prefix == "py_")
    )


def test_every_wheel_job_has_a_file_based_twin():
    wheel_jobs = {
        name
        for path in _job_files("")
        for name in yaml.safe_load(path.read_text())["resources"]["jobs"]
    }
    file_jobs = {
        name
        for path in _job_files("py_")
        for name in yaml.safe_load(path.read_text())["resources"]["jobs"]
    }
    # py_collections_pipeline <-> collections_pipeline, and so on.
    assert {n.removeprefix("py_") for n in file_jobs} == wheel_jobs


@pytest.mark.parametrize("path", _job_files("py_"), ids=lambda p: p.name)
def test_file_based_jobs_pass_a_known_entrypoint(path, dispatcher):
    """Catches the typo the wheel jobs cannot make: `entry_point:` is resolved
    from wheel metadata, `--entrypoint=` is just a string until it runs."""
    jobs = yaml.safe_load(path.read_text())["resources"]["jobs"]
    for job in jobs.values():
        for task in job["tasks"]:
            assert "spark_python_task" in task, "py_* jobs must not use the wheel"
            params = task["spark_python_task"]["parameters"]
            requested = [p.removeprefix("--entrypoint=") for p in params
                         if p.startswith("--entrypoint=")]
            assert len(requested) == 1, f"{path.name}: exactly one --entrypoint expected"
            assert requested[0] in dispatcher.ENTRYPOINTS
