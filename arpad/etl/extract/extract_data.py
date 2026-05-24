from builtins import Exception, SystemExit, open, str, bytes
import shutil

from arpad.BrasilIO import BrasilIO
from arpad.helpers.logging import logger
import os


class ExtractData:
    """Class responsible for extracting data from Brasil.io API.

    This class provides methods to download CSV data from the Brasil.io API,
    manage local temporary storage, and prepare data for S3 upload.
    """

    def __init__(self) -> None:
        """Initialize the ExtractData instance.

        Returns:
            None
        """
        pass

    def create_data_temporary_folder(
        self, dataset_slug: str, table_name: str, folder: str, brasil_io_token: str
    ) -> None:
        """Create a temporary folder and download CSV file from Brasil.io API.

        Creates a temporary directory, downloads the CSV data from Brasil.io API,
        and saves it locally with the naming convention: {dataset_slug}_{table_name}.csv

        Args:
            dataset_slug (str): Dataset slug from Brasil.io (e.g., "gastos-deputados").
            table_name (str): Table name from the dataset (e.g., "cota_parlamentar").
            folder (str): Path to the temporary folder where the CSV will be stored.
            brasil_io_token (str): API token for Brasil.io authentication.

        Returns:
            None

        Raises:
            SystemExit: If Brasil.io token is invalid or API request fails.
            OSError: If temporary folder creation or file writing fails.
        """
        temp_folder = folder

        logger.info("Creating temporary folder for CSV file...")

        logger.info(
            "Checking if the temporary folder already exists. If, so re-create it.."
        )
        if os.path.exists(temp_folder):
            shutil.rmtree(temp_folder)
        os.makedirs(temp_folder, exist_ok=True)
        logger.info("Temporary folder created: %s", temp_folder)

        logger.info("Saving CSV file to the temporary folder...")
        out_path = os.path.join(temp_folder, f"{dataset_slug}_{table_name}.csv")
        with open(out_path, mode="wb") as fobj:
            fobj.write(
                self.collect_csv_from_brasil_io(
                    brasil_io_token, dataset_slug, table_name
                )
            )
        logger.info("File stored successfully at: %s", out_path)

        return

    def collect_csv_from_brasil_io(
        self, brasil_io_token: str, dataset_slug: str, table_name: str
    ) -> bytes:
        """Download CSV data from Brasil.io API and return as bytes.

        Validates the Brasil.io API token, downloads the specified dataset and table
        as a CSV file, and returns the content as bytes. Uses gzip compression from
        the API and decompresses it automatically.

        Args:
            brasil_io_token (str): API token for Brasil.io authentication.
            dataset_slug (str): Dataset slug from Brasil.io (e.g., "gastos-deputados").
            table_name (str): Table name within the dataset (e.g., "cota_parlamentar").

        Returns:
            bytes: CSV file content as raw bytes.

        Raises:
            SystemExit: If Brasil.io token is not provided or invalid, or if API request fails.
        """

        logger.info("Starting CSV data collections from Brasil.io...")

        token = brasil_io_token
        logger.info("Validating if Brasil.io was properly configured...")
        if not token or token == "":
            raise SystemExit(
                "ERROR: set BRASIL_IO_TOKEN (environment variable or config.py)"
            )

        logger.info("Downloading data from Brasil.io into memory...")
        api = BrasilIO(token)

        logger.info("Getting response from Brasil.io API...")
        try:
            response = api.download(
                dataset_slug, table_name
            )  # TODO: Implement pagination handling if needed or DOS requests handling for large datasets
            if response.status != 200:
                raise SystemExit(
                    f"ERROR: Failed to download data from Brasil.io API. Status code: {response.status}"
                )
        except Exception as e:
            logger.error("Error occurred while downloading data from Brasil.io: %s", e)
            raise SystemExit("ERROR: Failed to download data from Brasil.io")

        logger.info("Getting csv content from the API...")
        csv_content = response.read()

        return csv_content

    def get_csv_file_and_save_locally(
        self, dataset_slug: str, table_name: str, folder: str, brasil_io_token: str
    ) -> None:
        """Download CSV from Brasil.io API and save it to a local folder.

        Fetches CSV data from Brasil.io API and writes it to a local file in the
        specified folder with the naming convention: {dataset_slug}_{table_name}.csv

        Args:
            dataset_slug (str): Dataset slug from Brasil.io (e.g., "gastos-deputados").
            table_name (str): Table name from the dataset (e.g., "cota_parlamentar").
            folder (str): Path to the folder where the CSV file will be saved.
            brasil_io_token (str): API token for Brasil.io authentication.

        Returns:
            None

        Raises:
            SystemExit: If Brasil.io token is invalid or API request fails.
            OSError: If file writing to the local folder fails.
        """
        logger.info("Saving CSV file to the temporary folder...")

        temp_folder = folder

        out_path = os.path.join(temp_folder, f"{dataset_slug}_{table_name}.csv")
        with open(out_path, mode="wb") as fobj:
            fobj.write(
                self.collect_csv_from_brasil_io(
                    brasil_io_token, dataset_slug, table_name
                )
            )
        logger.info("File stored successfully at: %s", out_path)

        return
