from builtins import Exception, bytes, str

from core.settings import BrasilIOConfig, AWSSettings


class ExtractData:
    """Class responsible for extracting data from Brasil.io API and uploading to S3."""

    def __init__(
        self,
        dataset_slug: str,
        table_name: str,
        s3_bucket: str,
        s3_prefix: str,
        brasil_io_token: str,
    ) -> None:
        self.dataset_slug = dataset_slug
        self.table_name = table_name
        self.s3_bucket = s3_bucket or AWSSettings.S3_BUCKET
        self.s3_prefix = s3_prefix or AWSSettings.S3_PREFIX
        self.brasil_io_token = brasil_io_token or BrasilIOConfig.BRASIL_IO_TOKEN

    def extract_csv_from_brasil_io(
        self, brasil_io_token: str, dataset_slug: str, table_name: str
    ) -> bytes:
        """Download data from Brasil.io API and return as bytes.

        Args:
            brasil_io_token: Brasil.io API token.
            dataset_slug: dataset slug (e.g., "gastos-deputados").
            table_name: table name (e.g., "cota_parlamentar").

        Returns:
            CSV content as bytes.

        Raises:
            SystemExit: if Brasil.io token is invalid or API request fails.
        """

        BASE_URL = "https://api.brasil.io/v1/"
        brasil_io_token = BrasilIOConfig.BRASIL_IO_TOKEN
        dataset_slug = BrasilIOConfig.DATASET_SLUG
        table_name = BrasilIOConfig.TABLE_NAME

        if not brasil_io_token or brasil_io_token == "":
            raise Exception(
                "ERROR: No brasil_io_token token provided. Please inform one"
            )

        if not dataset_slug or dataset_slug == "":
            raise Exception("ERROR: No dataset slug provided. Please inform one")

        if not table_name or table_name == "":
            raise Exception("ERROR: No table_name provided. Please inform one")

        a = BASE_URL + f"datasets/{dataset_slug}/data/{table_name}/?format=csv"
        return a
