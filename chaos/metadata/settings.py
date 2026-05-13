class SparkSettings:
    RAW_FILE_TYPE: str = "csv"

    FLATTEN: bool = False

    OPERATION_VALUE: dict[str, str] = {
        "insert": "I",
        "update": "U",
        "delete": "D",
    }


class DatabricksSettings:
    SECRETS_SCOPE: dict[str, str] = {
        "dev": "dev-secrets",
        "prod": "prod-secrets",
    }  # PENDING


class AWSSettings:
    DEFAULT_REGION: str = "us-east-1"
    ACCOUNT_ROLE: str = "databricks/<PENDING>"
    ACCOUNT_NUMBER: dict[str, str] = {
        "dev": "<PENDING>",
        "prod": "<PENDING>",
    }
    DATA_CONTRACTS_BUCKET: dict[str, str] = {
        "dev": "<PENDING>",
        "prod": "<PENDING>",
    }


class Settings:
    aws = AWSSettings()
    sparkk = SparkSettings()
    databricks = DatabricksSettings()
