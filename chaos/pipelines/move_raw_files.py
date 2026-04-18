import sys
from inspect import currentframe, getfile
from os.path import abspath, dirname

if (frame := currentframe()) is not None:
    sys.path.append(dirname(dirname(dirname(abspath(getfile(frame))))))

from multiprocessing.pool import ThreadPool

from chaos.helpers.contracts import CatalogDatasetUtils
from chaos.helpers.databricks import DatabricksHelper
from chaos.metadata.settings import AWSSettings

# PENDING CLASSES AND FUNCTIONS
# PENDING CLASSES AND FUNCTIONS


def main(data_contract_names: list[str], environment: str, landing_path: str):
    transfer_map = []

    for data_contract_name in data_contract_names:
        data_contract = DataContractReader.from_s3_bucket(
            bucket_name=AWSSettings.DATA_CONTRACTS_BUCKET[environment],
            data_contract=data_contract_name,
            aws_utils=DatabricksAWSUtils(),
        )

        raw_dataset = data_contract.datasets.raw.environment_info[environment]
        raw_dataset = CatalogDatasetUtils.adjust_s3_bucket_path(raw_dataset, is_enhance=False)

        for schema in data_contract.schemas:
            landing_table_dir = "/".join(
                [
                    landing_path,
                    schema.source_info.schema or schema.source_info.database,
                    schema.source_info.table,
                ]
            )
            if not DatabricksHelper.directory_exists(dbutils=dbutils, dir_path=landing_table_dir):
                continue

            transfer_map.extend(
                [
                    (
                        file_info.path,
                        "/".join(
                            filter(
                                None,
                                [
                                    raw_dataset.s3_bucket_name,
                                    "processing-queue",
                                    schema.source_info.database,
                                    schema.source_info.schema,
                                    schema.source_info.table,
                                    file_info.name,
                                ],
                            )
                        ),
                    )
                    for file_info in dbutils.ls.ls(landing_table_dir)
                ]
            )

    table_move = lambda item: DatabricksHelper.move_file(dbutils, item[0], item[1], False)

    pool = ThreadPool(10)
    mapped = pool.map_async(table_move, transfer_map)
    mapped.get()


if __name__ == "__main__":
    run_params = DatabricksHelper.get_script_run_parameters(["environment", "data_contracts", "landing_path"])

    data_contracts = run_params.get("data_contracts")
    environment = run_params.get("environment")
    landing_path = run_params.get("landing_path")

    if not isinstance(data_contracts, list):
        raise Exception("Exactly one data contract must be provided")

    if not isinstance(environment, str):
        raise Exception(f"Environment must be a string. Got {type(environment)}.")

    if not isinstance(landing_path, str):
        raise Exception(f"Landing path must be a string. Got {type(landing_path)}.")

    main(
        data_contract_names=data_contracts,
        environment=environment,
        landing_path=landing_path,
    )
