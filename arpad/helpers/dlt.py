from re import compile
from pyspark.sql.types import StructType
from pyspark.sql import Dataframe, SparkSession

from arpad.helpers.logging import logger
from arpad.core.settings import SparkSettings


class DLTHelper:
    @staticmethod
    def order_files_to_process(input_list: list[str]) -> list[str]:
        """
        Orders the input list of files to process.

        The function sorts the input list in ascending order and checks if there are any full load files.
        If there are full load files, it moves them to the beggining of the list, preserving the order of the remaining files.

        Args:
            input_list (list[str]): A list of file paths to be ordered.

        Returns:
            list: The ordered list of files to process.
        """
        input_list = sorted(input_list)

        # Checking if there are any full load files
        load_index, load_pattern = (
            0,
            compile(r"LOAD\d+\." + f"{SparkSettings.RAW_FILE_TYPE}"),
        )
        for index, value in enumerate(input_list):
            if load_pattern.match(value):
                load_index = index
                return input_list[load_index:] + input_list[:load_index]

        return input_list

    @staticmethod
    def catalog_table_exists(spark: SparkSession, table: str) -> bool:
        """
        Checks if a table exists in the Spark catalog.

        Args:
            spark (SparkSession): The SparkSession object.
            table (str): The name of the table to check for existence.

        Returns:
            bool: True if the table exists, False otherwise.
        """

        logger.info(f"Checking if table {table} exists in the Spark catalog")
        return spark.catalog._jcatalog.tableExists(table)

    @staticmethod
    def create_empty_dataframe(spark: SparkSession, schema: StructType) -> Dataframe:
        """
        Creates an empty DataFrame with the specified schema.

        Args:
            spark (SparkSession): The SparkSession object.
            schema (StructType): The schema for the empty DataFrame.

        Returns:
            DataFrame: An empty DataFrame with the specified schema.
        """
        logger.info("Creating an empty DataFrame with the specified schema")
        return spark.createDataFrame(data=[], schema=schema)

    @staticmethod
    def write_empty_schema(
        df: Dataframe,
        dlt_table_name: str,
        schema_path: str,
        partition_columns: list[str],
    ) -> None:
        """
        Writes an empty DataFrame with the specified schema to a Delta Live Table.

        Args:
            df (DataFrame): The empty DataFrame to write.
            dlt_table_name (str): The name of the Delta Live Table to write to.
            schema_path (str): The path where the schema of the DataFrame is stored.
            partition_columns (list[str]): A list of columns to partition the Delta Live Table by.

        Raises:
            Exception: If the DataFrame is not empty.
        """
        if bool(df.count()):
            raise Exception("DataFrame must be empty")

        logger.info(
            f"Writing an empty DataFrame with the specified schema to the Delta Live Table: {dlt_table_name}"
        )
        df_writer = df.write.format("delta").mode("overwrite")

        if partition_columns:
            logger.info(
                f"Partitioning Delta Live Table by columns: {partition_columns}"
            )
            df_writer = df_writer.partitionBy(partition_columns)

        df_writer.option("path", schema_path).saveAsTable(dlt_table_name)

    @staticmethod
    def join_condition_str(
        primary_keys: list,
        table_a_alias: str = "original",
        table_b_alias: str = "updates",
    ) -> str:
        """
        Generate the join condition string for two tables based on their primary keys.

        Args:
            primary_keys (list): A list of primary key column names to use in the join condition.
            table_a_alias (str): The alias for the first table in the join condition. Default is "original".
            table_b_alias (str): The alias for the second table in the join condition. Default is "updates".

        Returns:
            str: A string representing the join condition for the two tables.

        Example:
            >>> primary_keys = ["id", "name"]
            >>> join_condition_str(primary_keys)
            "original.id = updates.id AND original.name = updates.name"
        """
        logger.info(
            f"Generating join condition string for primary keys: '{table_a_alias}'"
        )
        return " AND ".join(
            [f"{table_a_alias}.{key} = {table_b_alias}.{key}" for key in primary_keys]
        )

    @staticmethod
    def alter_table_properties(
        spark: SparkSession,
        dlt_table_name: str,
        catalog_full_name: str,
        dataset_environment: str,
    ) -> None:
        """
        Alters the properties of a Delta Live Table in the specified database and schema.

        Args:
            spark (SparkSession): The SparkSession object.
            database (str): The name of the database.
            schema (CatalogSchema): The schema object.
            dataset_environment (str): The dataset environment name.
        """
        logger.info(
            f"Run ALTER TABLE PROPERTIES for the Delta Live Table: {dlt_table_name}"
        )
        sql_tblproperties = f"""
            ALTER TABLE {dlt_table_name}
            SET TBLPROPERTIES (
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5',
                'delta.minReaderVersion' = '2',
                'delta.enableChangeDataFeed' = 'true',
                'quality' = 'bronze',
                'environment' = '{dataset_environment}',
                'source' = '{catalog_full_name}'

            )
        """
        spark.sql(sql_tblproperties)
