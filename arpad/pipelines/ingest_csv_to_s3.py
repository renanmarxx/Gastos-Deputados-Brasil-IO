from arpad.helpers.logging import logger

from arpad.core.settings import BrasilIOConfig, AWSSettings, EnvinronmentSettings
from arpad.etl.load.load_data_to_s3 import S3LoadData


def main():
    logger.info("Starting CSV to S3 ingestion pipeline")

    load_data = S3LoadData(
        s3_bucket=AWSSettings.S3_BUCKET,
        s3_prefix=AWSSettings.S3_PREFIX,
        dataset_slug=BrasilIOConfig.DATASET_SLUG,
        table_name=BrasilIOConfig.TABLE_NAME,
        brasil_io_token=BrasilIOConfig.BRASIL_IO_TOKEN,
        folder=EnvinronmentSettings.CSV_FOLDER,
    )

    load_data.load_data_into_s3_bucket()

    logger.info("CSV to S3 ingestion pipeline completed successfully")


if __name__ == "__main__":
    main()
