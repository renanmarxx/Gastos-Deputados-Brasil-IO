from pyspark.sql import SparkSession

from arpad.core.settings import settings
from arpad.helpers.logging import logger


class DataSource:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def read(self, path: str, file_format: str = settings.spark.RAW_FILE_TYPE):
        match file_format.lower():
            case "csv":
                logger.info("Reading CSV file from path: %s", path)
                return (
                    self._spark.read.format(settings.spark.RAW_FILE_TYPE)
                    .option("header", "true")
                    .load(path)
                )
            case _:
                msg = f"Unsupported file format: {file_format}. Supported formats: {list(settings.spark.OPERATION_VALUE.keys())}"
                logger.error(msg)
                raise NotImplementedError(msg)
