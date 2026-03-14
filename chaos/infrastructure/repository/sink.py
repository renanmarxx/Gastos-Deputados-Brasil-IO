from builtins import Exception, str
from typing import Any

from pandas import DataFrame
from delta.tables import DeltaTable
from pyspark.sql import Dataframe, SparkSession

# PENDING CLASSES AND FUNCTIONS
# PENDING CLASSES AND FUNCTIONS

from chaos.metadata import settings
from chaos.helpers.datalake import DatalakeHelper
from chaos.helpers.logging import logger
from chaos.helpers.contracts import SchemaUtils
from chaos.helpers.transformations import EnhanceTransformations


class DataSink:
    def __init__(
        self,
        spark: SparkSession,
        schema: DataContractSchema,
        catalog_dataset: CatalogDatasetInfo,
    ) -> None:
        self.spark = spark
        self.schema = schema
        self.catalog_dataset = catalog_dataset

    def build_path(self, database: str) -> str:
        schema_environment = SchemaUtils.get_catalog_schema_environment(self.schema)
        base_path = (
            f"{self.catalog_dataset.s3_bucket_name}{{separator}}{schema_environment}"
        )
        match database:
            case "<PENDING>":
                return base_path.format(separator="/")
            case "<PENDING>":
                return base_path.format(separator="/current/")
            case _:
                msg = f"Database should be '<PENDING' or '<PENDING>'. Got {database}."
                logger.error(msg)
                raise Exception(msg)

    def first_load_to_datalake(
        self, df: DataFrame, delta_table_name: str, datalake_path: str
    ) -> None:

        if DatalakeHelper.catalog_table_exists(self.spark, delta_table_name):
            return

        logger.info(f"Creating empty schema for {delta_table_name}")

        empty_df = DatalakeHelper.create_empty_dataframe(self.spark, df.schema)
        partition_cols = SchemaUtils.get_partition_columns(self.schema)
        schema_catalog_name = SchemaUtils.get_catalog_schema_full_name(self.schema)

        DatalakeHelper.write_empty_schema(
            df=empty_df,
            datalake_table=delta_table_name,
            schema_path=datalake_path,
            partition_columns=partition_cols,
        )
        DatalakeHelper.alter_datalake_properties(
            spark=self.spark,
            delta_table_name=delta_table_name,
            catalog_full_name=schema_catalog_name,
            dataset_environment=self.catalog_datset.environment_name,
        )

    def append_data(
        self, df: DataFrame, database: str, namespace: str = "<PENDING>"
    ) -> None:

        datalake_path = self.build_path(database=database)
        versioned_table_name = SchemaUtils.get_versioned_table_name(self.schema)
        delta_table_name = f"{namespace}.{database}.{versioned_table_name}"

        datalake_df = df.drop(
            settings.spark.OPERATION_COLUMN_NAME, settings.spark.ORDER_COLUMN_NAME
        )

        self.first_load_to_datalake(
            df=datalake_df,
            delta_table_name=delta_table_name,
            datalake_path=datalake_path,
        )

        logger.info(f"Writing data to {database.upper()}: {delta_table_name}")
        logger.info(f"{database.upper()} Dataframe schema: {datalake_df.columns}")

        datalake_df.write.format("delta").mode("append").save(datalake_path)

        logger.info(f"Data wrote into {database.upper()} table {delta_table_name}")

    # def merge_data(self, df: DataFrame, database: str, namespace: str = "<PENDING>") -> None:

    # to be continued....
