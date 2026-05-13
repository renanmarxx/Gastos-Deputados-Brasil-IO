import json
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from chaos.helpers.logging import logger
from chaos.metadata import settings


class JsonStringParser:
    @staticmethod
    def clean_text_str(text_str: str) -> str:
        """
        Cleans a JSON string by removing unwanted characters and ensuring it is properly formatted.

        Args:
            text_str (str): The input JSON string to be cleaned.

        Returns:
            str: A cleaned and properly formatted JSON string.
        """
        logger.info("Cleaning text string")
        return re.sub(r"\}\{", "}|||{", re.sub(r"[/t/n]*", "", text_str))

    @staticmethod
    def parse_str_to_dict(text_str: str) -> list:
        logger.info(f"Parsing text string to dictionary: {text_str}")
        return list(map(lambda data: json.loads(data), text_str.split("|||")))

    def get(self, text_str: str) -> list:
        text_str = self.clean_text_str(text_str=text_str)
        return self.parse_str_to_dict(text_str=text_str)


class StructFlattener:
    @staticmethod
    def flatten(df, prefix=""):
        while any(isinstance(field.dataType, StructType) for field in df.schema.fields):
            flat_cols = []

            for field in df.schema.fields:
                full_field_name = prefix + field.name if prefix else field.name

                if isinstance(field.dataType, StructType):
                    for subfield in field.dataType.fields:
                        field_name = field_name + "." + subfield.name  # noqa: F841
                        subfield_name = full_field_name + "_" + subfield.name
                        flat_cols.append(col(field.name).alias(subfield_name))
                else:
                    flat_cols.append(col(field.name).alias(full_field_name))

            df = df.select(flat_cols)

        return df


class MultilineJsonReader:
    def __init__(
        self,
        spark: SparkSession,
        text_json_parser: JsonStringParser,
        struct_flattener: StructFlattener,
    ):
        self.spark = (spark,)
        self._sc = self._spark.sparkContext
        self._text_json_parser = text_json_parser
        self._struct_flattener = struct_flattener

    def __load_text_file(self, path: str) -> str:
        return self._spark.read.text(path, wholetext=True).collect()[0]["value"]

    def load(self, path: str, flatten: bool = False):
        text_file_content = self.__load_text_file(path=path)
        text_str = self._text_json_parser.get(text_str=text_file_content)
        df = self._spark.read.json(
            self._sc.parallelize(text_str).map(lambda x: json.dumps(x))
        )
        if flatten:
            return self._struct_flattener.flatten(df)
        return df


class DataSource:
    def __init__(self, spark: SparkSession):
        self._spark = spark

    def read(self, path: str, file_format: str = settings.spark.RAW_FILE_TYPE):
        match file_format.lower():
            case "json":
                logger.info("Reading JSON file")
                return MultilineJsonReader(
                    spark=self._spark,
                    text_json_parser=JsonStringParser(),
                    struct_flattener=StructFlattener(),
                ).load(path=path, flatten=settings.spark.FLATTEN)
            case "parquet":
                logger.info("Reading Parque file")
                return self._spark.read.format(settings.spark.RAW_FILE_TYPE).load(path)
            case "delta":
                logger.info("Reading Delta table")
                return self._spark.table(path)
            case "csv":
                logger.info("Reading CSV file")
                return self._spark.read.format(
                    settings.spark.RAW_FILE_TYPE
                ).load(
                    path
                )  # TODO: check if this will work since settings.py file does not have a csv format

            case _:
                msg = f"Unsupported file format: {file_format}"
                logger.error(msg)
                raise NotImplementedError(msg)
