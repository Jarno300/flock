from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
DBT_MODELS_DIR = ROOT / "dbt" / "models"
ALLOWED_LAYERS = {"raw", "staging", "marts"}


def fail(message: str) -> None:
    print(f"[dbt-structure] ERROR: {message}")
    sys.exit(1)


def validate_layer_directories() -> None:
    if not DBT_MODELS_DIR.exists():
        fail(f"Missing dbt models directory: {DBT_MODELS_DIR}")

    child_dirs = [p for p in DBT_MODELS_DIR.iterdir() if p.is_dir()]
    unknown_dirs = sorted(p.name for p in child_dirs if p.name not in ALLOWED_LAYERS)
    if unknown_dirs:
        fail(
            "Unknown layer directories under dbt/models: "
            f"{', '.join(unknown_dirs)}. Allowed layers: raw, staging, marts."
        )

    missing_dirs = sorted(layer for layer in ALLOWED_LAYERS if not (DBT_MODELS_DIR / layer).exists())
    if missing_dirs:
        fail(f"Missing required layer directories: {', '.join(missing_dirs)}")


def validate_file_locations() -> None:
    sql_files = sorted(DBT_MODELS_DIR.rglob("*.sql"))
    yml_files = sorted(DBT_MODELS_DIR.rglob("*.yml"))

    if not sql_files:
        fail("No SQL models found under dbt/models.")
    if not yml_files:
        fail("No YAML model files found under dbt/models.")

    for path in sql_files + yml_files:
        rel = path.relative_to(DBT_MODELS_DIR)
        layer = rel.parts[0]
        if layer not in ALLOWED_LAYERS:
            fail(f"{rel} is outside allowed layers (raw/staging/marts).")


def validate_raw_layer() -> None:
    raw_dir = DBT_MODELS_DIR / "raw"
    raw_sql = list(raw_dir.rglob("*.sql"))
    if raw_sql:
        fail("Raw layer should only define source YAML, but SQL models were found.")

    sources_file = raw_dir / "sources.yml"
    if not sources_file.exists():
        fail("Expected dbt/models/raw/sources.yml is missing.")


def validate_staging_layer() -> None:
    staging_dir = DBT_MODELS_DIR / "staging"
    bad_names = []
    for path in staging_dir.rglob("*.sql"):
        if not path.stem.startswith("stg_"):
            bad_names.append(path.name)
    if bad_names:
        fail("Staging SQL model names must start with 'stg_': " + ", ".join(sorted(bad_names)))


def validate_marts_layer() -> None:
    marts_dir = DBT_MODELS_DIR / "marts"
    bad_names = []
    for path in marts_dir.rglob("*.sql"):
        if not (path.stem.startswith("dim_") or path.stem.startswith("fct_")):
            bad_names.append(path.name)
    if bad_names:
        fail(
            "Mart SQL model names must start with 'dim_' or 'fct_': "
            + ", ".join(sorted(bad_names))
        )


def main() -> None:
    validate_layer_directories()
    validate_file_locations()
    validate_raw_layer()
    validate_staging_layer()
    validate_marts_layer()
    print("[dbt-structure] OK: folder and naming conventions are valid.")


if __name__ == "__main__":
    main()
