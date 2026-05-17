class SparkSettings:
    # File type to be read and ingested into the Delta Live Table
    RAW_FILE_TYPE: str = "csv"
    # Name of the column which marks if a record is an insert, update or delete
    OPERATION_COLUMN_NAME: str = "Op"
    # Name of the column that marks if a record was deleted on source database
    DELETE_INDICATOR_COLUMN_NAME: str = "del_indicator"
    # Name of the column that keeps the datetime of when the record was captured by ingestion infrastructure
    MIGRATION_TIMESTAMP_COLUMN_NAME: str = "migrated_timestamp"
    # Name of the column that keeps the datetime of when the record was processed by the pipeline to be ingested on the Delta Lake
    DLT_INGESTION_TIMESTAMP_COLUMN_NAME: str = "dlt_ingested_timestamp"

    # Values on operation column that mark a record as insert, update or delete
    OPERATION_VALUE: dict[str, str] = {
        "insert": "I",
        "update": "U",
        "delete": "D",
    }


class DatabricksSettings:
    SECRETS_SCOPE: dict[str, str] = {
        "dev": "YOUR-DEV-DATABRICKS-SECRET-SCOPE",
        "prod": "YOUR-PROD-DATABRICKS-SECRET-SCOPE",
    }


class AWSSettings:
    DEFAULT_REGION: str = "sa-east-1"
    ACCOUNT_ROLE: str = "databricks/<YOUR-ACCOUNT-ROLE>"  # TODO: Check on the Databricks context how to get the account role name and update here
    ACCOUNT_NUMBER: dict[str, str] = {
        "dev": "YOUR-DEV-AWS-ACCOUNT-NUMBER",
        "prod": "YOUR-PROD-AWS-ACCOUNT-NUMBER",
    }
    DATA_CONTRACTS_BUCKET: dict[
        str, str
    ] = {  # TODO: Implement a Data Contract method to store the .yaml file
        "dev": "YOUR-DEV-DATA-CONTRACTS-BUCKET",
        "prod": "YOUR-PROD-DATA-CONTRACTS-BUCKET",
    }


class Settings:
    aws = AWSSettings()
    spark = SparkSettings()
    databricks = DatabricksSettings()
