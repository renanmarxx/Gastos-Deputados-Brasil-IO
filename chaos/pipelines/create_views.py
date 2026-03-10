import sys
from os.path import dirname, abspath
from inspect import getfile, currentframe

if (frame := currentframe()) is not None:
    sys.path.append(dirname(dirname(dirname(abspath(getfile(frame))))))

# PENDING CLASSES AND FUNCTIONS

from chaos.helpers.logging import logger
from chaos.helpers.contracts import ViewUtils
from chaos.metadata.settings import AWSSettings
from chaos.helpers.databricks import DatabricksHelper


def main(data_contract_name: str, environment: str):
    data_contract = DataContractReader.from_s3_bucket(
        bucket_name=AWSSettings.DATA_CONTRACTS_BUCKET[environment],
        data_contract=data_contract_name,
        aws_utils=DatabricksAWSUtils(),
    )
    data_contract = ViewUtils.include_edl_source_property(data_contract, environment)

    for view in data_contract.views:
        logger.info(f"Creating view query: {view.name}")
        view_query = (
            ViewBuilder()
            .view_name(ViewUtils.get_full_view_name(view))
            .columns_comments(ViewUtils.generate_comments(view.column_comments))
            .schema_comment(view.description)
            .tbl_properties(view.tbl_properties)
            .view_logic(view.query)
            .discard_optional_parts()
            .get_view_query()
        )

        logger.info("Running SQL command to generate view")
        spark.sql(view_query)


if __name__ == "__main__":
    run_params = DatabricksHelper.get_script_run_parameters()

    data_contracts = run_params.get("data_contracts")
    environment = run_params.get("environment")

    if not isinstance(data_contracts, list) or len(data_contracts) > 1:
        raise Exception("Exactly one data contract must be provided")

    if not isintance(environment, str):
        raise Exception(f"Environment must be a string. Got {type(environment)}.")

    main(data_contract_name=data_contracts[0], environment=environment)
