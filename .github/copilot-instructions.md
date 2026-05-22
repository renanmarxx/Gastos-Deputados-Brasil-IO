# Copilot Instructions for this repository

Purpose
- Help Copilot-powered sessions understand how to build, run, and reason about this data engineering pipeline.

Build / Test / Lint commands
- Setup (recommended):
  - python -m pip install --upgrade pip
  - pip install poetry
  - poetry config virtualenvs.in-project true
  - poetry install --no-interaction --no-root

- Run full test suite: poetry run pytest
- Run a single test: poetry run pytest path/to/test_file.py::test_name  (or use -k <expr> to filter)

- Lint / format:
  - Run pre-commit for all files: pre-commit run --all-files
  - Run ruff check: poetry run ruff check .
  - Fix with ruff: poetry run ruff . --fix
  - Format with black: poetry run black .
  - Sort imports: poetry run isort .

- Run the ingestion script (local/manual):
  - Export env vars: BRASIL_IO_TOKEN, AWS_S3_BUCKET, AWS_S3_PREFIX
  - poetry run python scripts/ingest_csv_to_s3.py
  - Note: scripts/config.py contains default fallbacks when env vars are not provided.

High-level architecture (big picture)
- Purpose: periodic/CI-driven ingestion of "gastos-deputados" data from Brasil.io, store CSVs in S3, and prepare data for Databricks Delta (DLT) ingestion.
- Main components:
  - scripts/brasil_io.py: small client to download datasets from Brasil.io (iterator + full CSV download).
  - scripts/ingest_csv_to_s3.py: orchestration script that uses BrasilIO client, writes a local copy to data/, then uploads to S3 with key pattern: {S3_PREFIX}/dt=YYYY-MM-DD/{dataset}_{table}.csv.
  - scripts/config.py: local defaults and environment-variable-backed configuration for the ingestion scripts (BRASIL_IO_TOKEN, S3_BUCKET, S3_PREFIX, DELTA_TABLE).
  - scripts/core and scripts/helpers: Databricks/DLT helpers, Spark settings, and a shared logging implementation (chaos/helpers and scripts/helpers/logging).
  - data_contracts/: placeholder for schema/contract artifacts used by ingestion/validation.
  - .github/workflows/main.yml: CI job that installs dependencies via Poetry and runs scripts/ingest_csv_to_s3.py (CI-driven ingestion). Pre-commit workflow runs linters, Terraform validation, tflint, and checkov.

Key conventions / repository-specific patterns
- Python environment: Poetry + Python 3.12. CI uses poetry install --no-root and an in-project virtualenv (.venv is cached by CI).
- Config pattern: scripts/config.py provides environment-backed defaults. Prefer setting BRASIL_IO_TOKEN, AWS_S3_BUCKET, AWS_S3_PREFIX in the environment for CI/production. Local runs may rely on the defaults in that module.
- Logging: use helpers.logging.logger (centralized logger under scripts/helpers/logging and chaos/helpers/logging).
- S3 upload pattern: uploaded objects are partitioned by date with the prefix dt=YYYY-MM-DD. Tests or downstream jobs assume that partitioning.
- Pre-commit rules: ruff runs with --line-length=120 and auto-fix enabled in the pre-commit config. Pre-commit also runs terraform checks and checkov for IaC validation.
- No dedicated test folder detected: pytest is listed as a dependency but no tests are present in the repository root. When adding tests, follow pytest naming conventions (test_*.py) and prefer small unit tests for helper modules and an integration test for the ingestion script (can be run with recorded network or local mock of BrasilIO/API).

Integration notes pulled from existing files
- GitHub Actions:
  - main.yml: installs Poetry, configures AWS via OIDC, runs scripts/ingest_csv_to_s3.py and uploads resulting CSV to the landing bucket.
  - pre-commit.yml: runs terraform init/validate, tflint, checkov, and pre-commit hooks for code quality.

Files to inspect for runtime behavior
- scripts/ingest_csv_to_s3.py (entrypoint for ingestion)
- scripts/brasil_io.py (API client)
- scripts/config.py (defaults)
- scripts/core/settings.py (Databricks / Spark conventions)
- scripts/helpers/* and chaos/helpers/* (shared utilities and logger)

If you want, add a short note here about environment variables or secrets storage conventions (where CI reads BRASIL_IO_TOKEN, S3 bucket secrets, etc.) and this file will be updated.

Secrets & environment (concrete)
- CI secrets used in workflows:
  - BRASIL_IO_TOKEN: Brasil.io API token (used by .github/workflows/main.yml)
  - LANDING_BUCKET_INGESTION: S3 bucket target for the generated CSV (main.yml uses this secret when uploading)
  - Other AWS access is provided in CI via OIDC role assumption (see .github/workflows/main.yml).

- Local development / example (run ingestion locally):
  - Export required variables and run in one line:
    - BRASIL_IO_TOKEN="<token>" AWS_S3_BUCKET="my-bucket" AWS_S3_PREFIX="my/prefix" poetry run python scripts/ingest_csv_to_s3.py
  - Or set them in your shell:
    - export BRASIL_IO_TOKEN="<token>"
    - export AWS_S3_BUCKET="my-bucket"
    - export AWS_S3_PREFIX="my/prefix"
    - poetry run python scripts/ingest_csv_to_s3.py
  - scripts/config.py contains safe defaults for local experimentation; CI/production should always set explicit secrets.

More run / test examples
- Run full test suite: poetry run pytest
- Run a single test by file: poetry run pytest tests/test_file.py
- Run a single test case: poetry run pytest tests/test_file.py::test_name
- Filter tests by keyword: poetry run pytest -k "partial_name" -q
- Run ingestion script manually (example with inline env vars):
  - BRASIL_IO_TOKEN="token" AWS_S3_BUCKET="bucket" AWS_S3_PREFIX="prefix" poetry run python scripts/ingest_csv_to_s3.py

Databricks & Terraform notes
- Databricks settings: scripts/core/settings.py defines Databricks-related defaults (SECRETS_SCOPE mapping) and Spark ingestion column names. Keep secrets in Databricks Secret Scopes and map scope names per env (dev/prod).
- AWS / Databricks role: scripts/core/settings.py contains ACCOUNT_ROLE placeholder. CI uses AWS OIDC to assume a role; local runs need AWS creds configured (aws cli or environment variables).
- Terraform in CI: pre-commit.yml runs `terraform init -backend=false` and `terraform validate` and configures tflint. Locally, validate with the same commands before opening PRs:
  - terraform init -backend=false
  - terraform validate
  - tflint
- IaC scanning: checkov is run via pre-commit; run it locally with `checkov -d .` if needed.

What changed
- Added concrete secrets/env examples, copy-paste commands to run ingestion locally, single-test examples, and short Databricks/Terraform references so future Copilot sessions can suggest accurate commands.

If you want more: indicate whether to add examples for mocking Brasil.io responses in tests, a sample terraform directory path to validate, or a Databricks runbook (notebook / CLI commands) and those will be added.
