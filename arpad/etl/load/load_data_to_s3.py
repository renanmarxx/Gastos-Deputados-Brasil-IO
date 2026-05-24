from builtins import Exception, str, type
import boto3
from io import BytesIO
from datetime import date

from arpad.helpers.logging import logger
from arpad.etl.extract.extract_data import ExtractData


class S3LoadData:
    """Class responsible for loading data into AWS S3 bucket.

    This class orchestrates the complete data pipeline: extracting CSV data from
    Brasil.io API, and uploading it to an S3 bucket with date-based partitioning
    (dt=YYYY-MM-DD format).
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_prefix: str,
        dataset_slug: str,
        table_name: str,
        brasil_io_token: str,
        folder: str,
    ) -> None:
        """Initialize the S3LoadData instance with configuration parameters.

        Args:
            s3_bucket (str): AWS S3 bucket name where data will be uploaded.
            s3_prefix (str): Prefix path in S3 (e.g., "data/landing").
            dataset_slug (str): Dataset slug from Brasil.io (e.g., "gastos-deputados").
            table_name (str): Table name from the dataset (e.g., "cota_parlamentar").
            brasil_io_token (str): API token for Brasil.io authentication.
            folder (str): Local folder path for temporary CSV storage.

        Returns:
            None
        """
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.dataset_slug = dataset_slug
        self.table_name = table_name
        self.brasil_io_token = brasil_io_token
        self.folder = folder

    def load_data_into_s3_bucket(self) -> None:
        """Orchestrate the complete data ingestion pipeline to S3.

        Executes the full ETL process:
        1. Extracts CSV data from Brasil.io API
        2. Saves a local copy to the specified folder
        3. Uploads the CSV to S3 with date-based partitioning (s3_prefix/dt=YYYY-MM-DD/)

        The S3 object key follows the pattern:
        {s3_prefix}/dt={YYYY-MM-DD}/{dataset_slug}_{table_name}.csv

        Args:
            None

        Returns:
            None

        Raises:
            SystemExit: If Brasil.io token is invalid or API request fails.
            Exception: If S3 upload fails, includes boto3 client errors.
        """
        # Uploading .csv file to the S3 Bucket
        logger.info("Starting S3 Bucket upload...")

        logger.info(
            "Stancing the ExtractData class to collect the CSV data from Brasil.io API..."
        )
        extract_data = ExtractData()

        logger.info("Calling the method to collect the CSV data from Brasil.io API...")
        csv_content = extract_data.get_csv_file_and_save_locally(
            dataset_slug=self.dataset_slug,
            table_name=self.table_name,
            folder=self.folder,
            brasil_io_token=self.brasil_io_token,
        )

        logger.info("Starting the upload to the S3 Bucket...")
        try:
            csv_bytes = BytesIO(csv_content)
            csv_bytes.seek(0)

            s3 = boto3.client("s3")
            today = date.today().isoformat()
            key = (
                f"{self.s3_prefix}/dt={today}/{self.dataset_slug}_{self.table_name}.csv"
            )

            s3.upload_fileobj(csv_bytes, self.s3_bucket, key)
            logger.info(
                "Successful upload to the S3 Bucket: s3://%s/%s", self.s3_bucket, key
            )

        except Exception as e:
            logger.error(
                "Error uploading to the S3 Bucket: %s: %s",
                type(e).__name__,
                e,
            )
            raise

        return
