"""Static checks of the dbt project, runnable without BigQuery or credentials.

`dbt compile` needs a warehouse connection, so these tests do the important part themselves:
they render each model's Jinja with the project's own macros, parse the SQL as BigQuery SQL
with sqlglot, and check every column against the raw schema (`fashion_ec.schemas`) and the
upstream models. A typo in a column name, an undocumented column or a wrong type fails here,
in CI, long before a query is billed.
"""

import re
from pathlib import Path

import jinja2
import pytest
import sqlglot
import yaml
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema

from fashion_ec.schemas import TABLES

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"
MODELS_DIR = DBT_DIR / "models"
PROJECT = "test-project"
DIALECT = "bigquery"

REF = re.compile(r"ref\(\s*'([\w]+)'\s*\)")


# --- rendering and analysis -------------------------------------------------


def model_files() -> dict[str, Path]:
    return {path.stem: path for path in sorted(MODELS_DIR.rglob("*.sql"))}


def macro_source() -> str:
    return "\n".join(path.read_text() for path in sorted((DBT_DIR / "macros").glob("*.sql")))


def render(path: Path) -> str:
    """Render a model like dbt would, with source() and ref() pointing at fixed names."""
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    env.globals.update(
        source=lambda source, table: f"`{PROJECT}.{source}.{table}`",
        ref=lambda name: f"`{PROJECT}.dbt.{name}`",
        config=lambda **kwargs: "",
    )
    return env.from_string(macro_source() + path.read_text()).render().strip()


def raw_schema() -> dict:
    return {
        PROJECT: {
            "raw": {name: dict(spec.columns) for name, spec in TABLES.items()},
            "dbt": {},
        }
    }


def analyze_models() -> dict[str, dict[str, str]]:
    """Qualify every model in dependency order. Returns {model: {column: BigQuery type}}."""
    files = model_files()
    dependencies = {name: set(REF.findall(path.read_text())) for name, path in files.items()}
    schema_dict = raw_schema()
    schema = MappingSchema(schema_dict, dialect=DIALECT)
    analyzed: dict[str, dict[str, str]] = {}

    pending = dict(dependencies)
    while pending:
        ready = [name for name, deps in pending.items() if deps <= analyzed.keys()]
        assert ready, f"circular or missing model references: {sorted(pending)}"
        for name in ready:
            expression = sqlglot.parse_one(render(files[name]), read=DIALECT)
            expression = qualify(
                expression, schema=schema, dialect=DIALECT, validate_qualify_columns=True
            )
            expression = annotate_types(expression, schema=schema, dialect=DIALECT)
            columns = {
                select.alias_or_name: select.type.sql(DIALECT) for select in expression.selects
            }
            analyzed[name] = columns
            schema.add_table(f"`{PROJECT}.dbt.{name}`", columns, dialect=DIALECT)
            del pending[name]
    return analyzed


@pytest.fixture(scope="module")
def analyzed() -> dict[str, dict[str, str]]:
    return analyze_models()


def documented_models() -> dict[str, dict]:
    documented = {}
    for path in sorted(MODELS_DIR.rglob("*.yml")):
        for model in (yaml.safe_load(path.read_text()) or {}).get("models", []):
            documented[model["name"]] = model
    return documented


MODEL_NAMES = sorted(model_files())


# --- every model is valid SQL over real columns ------------------------------


def test_there_are_models():
    assert len(MODEL_NAMES) >= 8


def test_all_models_render_parse_and_use_existing_columns(analyzed):
    """Fails on an unknown column or table, bad Jinja, or SQL that is not valid BigQuery."""
    assert set(analyzed) == set(MODEL_NAMES)
    assert all(columns for columns in analyzed.values())


@pytest.mark.parametrize("name", MODEL_NAMES)
def test_model_columns_match_its_documentation(analyzed, name):
    documented = documented_models()
    assert name in documented, f"{name} is not described in a schema yml"
    described = [column["name"] for column in documented[name].get("columns", [])]
    assert sorted(described) == sorted(analyzed[name]), (
        f"{name}: documented columns differ from the SQL output"
    )


@pytest.mark.parametrize("name", MODEL_NAMES)
def test_model_is_described_and_has_a_key_test(name):
    model = documented_models()[name]
    assert model.get("description"), f"{name} has no description"
    for column in model["columns"]:
        assert column.get("description"), f"{name}.{column['name']} has no description"

    def test_names(tests):
        return {t if isinstance(t, str) else next(iter(t)) for t in tests or []}

    column_tests = set().union(*(test_names(c.get("data_tests")) for c in model["columns"]))
    model_tests = test_names(model.get("data_tests"))
    assert {"unique", "unique_combination"} & (column_tests | model_tests), (
        f"{name} needs a uniqueness test"
    )


# --- project conventions ----------------------------------------------------


def test_column_names_say_their_type(analyzed):
    """_at is a UTC timestamp, _on_jst a Japan-time date, _jpy and _units whole numbers."""
    expected = {"_at": "TIMESTAMP", "_on_jst": "DATE", "_jpy": "BIGINT", "_units": "BIGINT"}
    for model, columns in analyzed.items():
        for column, bq_type in columns.items():
            for suffix, wanted in expected.items():
                if column.endswith(suffix):
                    assert bq_type in {wanted, "INT64" if wanted == "BIGINT" else wanted}, (
                        f"{model}.{column} is {bq_type}, expected {wanted}"
                    )


def test_models_use_source_and_ref_never_hardcoded_tables():
    for name, path in model_files().items():
        text = path.read_text()
        assert "`" not in text, f"{name} hardcodes a table name"
        assert "source(" in text or "ref(" in text, f"{name} reads from nothing"


def test_japan_time_conversion_goes_through_the_macro():
    for name, path in model_files().items():
        assert not re.search(r"(?<![\w.])date\(", path.read_text()), (
            f"{name} calls date() directly; use jst_date() so the time zone stays in one place"
        )


def test_jst_macro_converts_with_the_tokyo_time_zone():
    sql = render_inline("{{ jst_date('order_ts') }}")
    assert sql == "date(order_ts, 'Asia/Tokyo')"


def render_inline(template: str) -> str:
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    return env.from_string(macro_source() + template).render().strip()


# --- sources match the loaded schema ----------------------------------------


def test_sources_match_the_raw_table_definitions():
    sources = yaml.safe_load((MODELS_DIR / "staging" / "_sources.yml").read_text())["sources"]
    raw = next(source for source in sources if source["name"] == "raw")
    declared = {t["name"]: [c["name"] for c in t["columns"]] for t in raw["tables"]}
    assert declared == {name: spec.column_names for name, spec in TABLES.items()}


def test_every_raw_table_has_a_staging_model(analyzed):
    assert {f"stg_{name}" for name in TABLES} <= set(analyzed)


# --- the checker itself catches mistakes --------------------------------------


def test_checker_rejects_an_unknown_column(tmp_path):
    from sqlglot.errors import OptimizeError

    bad = tmp_path / "stg_bad.sql"
    bad.write_text("select skuu from {{ source('raw', 'products') }}")
    expression = sqlglot.parse_one(render(bad), read=DIALECT)
    with pytest.raises(OptimizeError):
        qualify(
            expression,
            schema=MappingSchema(raw_schema(), dialect=DIALECT),
            dialect=DIALECT,
            validate_qualify_columns=True,
        )
