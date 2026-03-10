import sys
from os.path import dirname, abspath
from inspect import getfile, currentframe

if (frame := currentframe()) is not None:
    sys.path.append(dirname(dirname(dirname(abspath(getfile(frame))))))

from multiprocessing.pool import ThreadPool

from chaos.helpers.enhance import EnhanceTable
from chaos.metadata.settings import AWSSettings
from chaos.helpers.databricks import DatabricksHelper
from chaos.helpers.contracts import CatalogDatasetUtils


def main(data_contract_name: str, environment: str):

    data_contract = DataContractReader.from_s3_bucket(
        bucket_name=AWSSettings.DATA_CONTRACTS_BUCKET[environment],
        data_contract=data_contract_name,
        aws_utils=DatabricksAWSUtils(),
    )

    raw_dataset = ...


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
