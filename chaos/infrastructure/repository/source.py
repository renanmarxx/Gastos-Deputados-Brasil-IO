import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

from chaos.metadata import settings
from chaos.helpers.logging import logger


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
                        field_name = field_name + "." + subfield.name
                        subfield_name = full_field_name + "_" + subfield.name
                        flat_cols.append(col(field.name).alias(subfield_name))
                else:
                    flat_cols.append(col(field.name).alias(full_field_name))

            df = df.select(flat_cols)

        return df
