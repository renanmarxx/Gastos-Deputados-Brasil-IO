import sys
from inspect import currentframe, getfile
from os.path import abspath, dirname

if (frame := currentframe()) is not None:
    sys.path.append(dirname(dirname(dirname(abspath(getfile(frame))))))

from multiprocessing.pool import ThreadPool

from chaos.helpers.contracts import CatalogDatasetUtils
from chaos.helpers.databricks import DatabricksHelper
from chaos.helpers.enhance import EnhanceTable
from chaos.metadata.settings import AWSSettings

# PENDING CLASSES AND FUNCTIONS
# PENDING CLASSES AND FUNCTIONS



def main(data_contract_name: str, environment: str):

    data_contract = DataContractReader.from_s3_bucket(
        bucket_name=AWSSettings.DATA_CONTRACTS_BUCKET[environment],
        data_contract=data_contract_name,
        aws_utils=DatabricksAWSUtils(),
    )

    raw_dataset = data_contract.datasets.raw.environment_info[environment]
    raw_dataset = CatalogDatasetUtils.adjust_s3_bucket_path(
        raw_dataset, is_enhance=False
    )

    enhance_dataset = data_contract.datasets.enhance.environment_info[environment]
    enhance_dataset = CatalogDatasetUtils.adjust_s3_bucket_path(
        enhance_dataset, is_enhance=True
    )

    enhance_tables = [
        EnhanceTable(
            dbutils=dbutils,
            spark=spark,
            schema=schema,
            raw_catalog_info=raw_dataset,
            enhance_catalog_info=enhance_dataset,
        )
        for schema in data_contract.schemas
    ]

    pool = ThreadPool(10)
    mapped = pool.map_async(lambda enhance_table: enhance_table.run(), enhance_tables)
    mapped.get()


if __name__ == "__main__":
    run_params = DatabricksHelper.get_script_run_parameters()

    data_contracts = run_params.get("data_contracts")
    environment = run_params.get("environment")

    if not isinstance(data_contracts, list) or len(data_contracts) > 1:
        raise Exception("Should pass only one datasets to create tables for.")

    if not isinstance(environment, str):
        raise Exception(f"Environment must be a string. Got {type(environment)}.")

    main(
        data_contract_name=data_contracts[0],
        environment=environment,
    )
