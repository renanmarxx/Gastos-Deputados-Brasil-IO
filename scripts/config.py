"""
Data ingesting settings from Brasil.io to AWS S3 Bucket.

Reads environment variables as standards; provides standards values case they are not defined.
It is possible to customize them directly on this file or export them as environment variables.
"""

import os

# S3 Bucket where data will be stored
# Pattern: reads from AWS_S3_BUCKET environment variable; if it does not exists, use 'renan-marx-data-engineering-projects'
S3_BUCKET: str = os.environ.get("AWS_S3_BUCKET", "renan-marx-data-engineering-projects")

# Prefix (path) inside the bucket to store data
# Pattern: reads from AWS_S3_BUCKET environment variable; if it does not exists, use 'gastos-deputados-brasil-io/landing-bucket-gastos-deputados-brasil-io'
S3_PREFIX: str = os.environ.get(
    "AWS_S3_PREFIX",
    "gastos-deputados-brasil-io/landing-bucket-gastos-deputados-brasil-io",
)

# Brasil.io API Token
BRASIL_IO_TOKEN: str = os.environ.get("BRASIL_IO_TOKEN", "meu-api-token")

# Delta table name where data will be stored
# Pattern: reads from AWS_S3_BUCKET environment variable; if it does not exists, use 'bronze.brasil_io_gastos_deputados'
DELTA_TABLE: str = os.environ.get(
    "BRASIL_IO_TOKEN", "bronze.brasil_io_gastos_deputados"
)

# Usage examples (copy and run on the terminal):
# export AWS_S3_BUCKET="seu-bucket"
# export AWS_S3_PREFIX="dados/gastos"
# export BRASIL_IO_TOKEN="seu_token_brasil_io"
# export DELTA_TABLE="gastos_deputados"
# python scripts/ingest_csv_to_s3.py
# python scripts/ingest_s3_to_delta.py
