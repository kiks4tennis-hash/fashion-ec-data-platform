"""Render dbt models and tests the way dbt would, without dbt and without a warehouse.

Shared by the static checks (`test_dbt_models.py`) and the execution tests
(`test_dbt_execution.py`). Only the small part of dbt's Jinja that this project uses is
supported: `source()`, `ref()`, `var()`, `config()` and the project's own macros.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import jinja2
import yaml

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
MODELS_DIR = DBT_DIR / "models"
TESTS_DIR = DBT_DIR / "tests"


def project_vars() -> dict:
    """The `vars:` block of dbt_project.yml (session_gap_minutes and friends)."""
    return (yaml.safe_load((DBT_DIR / "dbt_project.yml").read_text()) or {}).get("vars", {})


def macro_source() -> str:
    return "\n".join(path.read_text() for path in sorted((DBT_DIR / "macros").glob("*.sql")))


def model_files() -> dict[str, Path]:
    return {path.stem: path for path in sorted(MODELS_DIR.rglob("*.sql"))}


def singular_test_files() -> dict[str, Path]:
    """Business-rule tests: `dbt/tests/*.sql` (not the generic tests in `tests/generic/`)."""
    return {path.stem: path for path in sorted(TESTS_DIR.glob("*.sql"))}


def render_sql(
    path: Path,
    ref: Callable[[str], str],
    source: Callable[[str, str], str],
    extra_vars: dict | None = None,
) -> str:
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    variables = {**project_vars(), **(extra_vars or {})}

    def var(name: str, default=None):
        if name in variables:
            return variables[name]
        if default is None:
            raise KeyError(f"dbt var '{name}' is not defined in dbt_project.yml")
        return default

    env.globals.update(source=source, ref=ref, var=var, config=lambda **kwargs: "")
    return env.from_string(macro_source() + path.read_text()).render().strip()
