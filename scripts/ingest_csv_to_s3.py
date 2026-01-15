import os
import sys
import boto3
from io import BytesIO
import shutil
from datetime import date
from typing import Optional
from brasil_io import BrasilIO
from config import S3_BUCKET, S3_PREFIX, BRASIL_IO_TOKEN


def ingest_brasil_io_to_s3(
    dataset_slug: str,
    table_name: str,
    s3_bucket: str,
    s3_prefix: str,
    brasil_io_token: Optional[str] = None,
) -> None:
    """Download data from Brasil.io and upload to S3.

    Args:
        dataset_slug: dataset slug (e.g., "gastos-deputados").
        table_name: table name (e.g., "cota_parlamentar").
        s3_bucket: S3 bucket name.
        s3_prefix: S3 path prefix (e.g., "data").
        brasil_io_token: Brasil.io API token. If None, reads from BRASIL_IO_TOKEN.

    Raises:
        SystemExit: if Brasil.io token, S3_BUCKET, or AWS credentials are invalid.
    """

    # Validate Brasil.io token
    token = brasil_io_token or os.environ.get("BRASIL_IO_TOKEN")
    if not token or token == "":
        raise SystemExit(
            "ERROR: set BRASIL_IO_TOKEN (environment variable or config.py)"
        )

    # Validate S3 bucket
    if not s3_bucket or s3_bucket == "meu-bucket-exemplo":
        raise SystemExit("ERROR: set AWS_S3_BUCKET with a valid S3 bucket")

    # Download data from Brasil.io into memory
    api = BrasilIO(token)
    response = api.download(dataset_slug, table_name)
    csv_content = response.read()

    # Save local copy for reference
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)
    out_path = os.path.join("data", f"{dataset_slug}_{table_name}.csv")
    with open(out_path, mode="wb") as fobj:
        fobj.write(csv_content)
    print(f"\nFile stored successfully at: {out_path}")

    # Uploading .csv file to the S3 Bucket
    print("\n>>> Starting S3 Bucket upload...")

    try:
        csv_bytes = BytesIO(csv_content)
        csv_bytes.seek(0)

        s3 = boto3.client("s3")
        today = date.today().isoformat()
        key = f"{s3_prefix}/dt={today}/{dataset_slug}_{table_name}.csv"

        s3.upload_fileobj(csv_bytes, s3_bucket, key)
        print(f"Successful upload to the S3 Bucket: s3://{s3_bucket}/{key}")
    except Exception as e:
        print(f"Error uploading to the S3 Bucket: {type(e).__name__}: {e}")
        raise


def main() -> None:
    """Main function: ingest Brasil.io data to S3."""

    DATASET_SLUG = "gastos-deputados"
    TABLE_NAME = "cota_parlamentar"

    ingest_brasil_io_to_s3(
        dataset_slug=DATASET_SLUG,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        brasil_io_token=BRASIL_IO_TOKEN,
    )


if __name__ == "__main__":
    main()
