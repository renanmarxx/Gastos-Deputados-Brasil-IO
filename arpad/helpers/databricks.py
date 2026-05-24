from argparse import ArgumentParser

from arpad.helpers.logging import logger


class DatabricksHelper:
    @staticmethod
    def move_file(
        dbutils, from_path: str, to_path: str, recursive: bool = False
    ) -> bool:
        """
        Moves a file from the source path to the destinatiion path. If recursive is True, it will move all files and directories under the source path to the destination path.

        Args:
            dbutils: The Databricks utilities object.
            from_path: The source path of the file to be moved.
            to_path: The destination path where the file should be moved.
            recursive: If True, move all files and directories under the source path to the destination path

        Returns:
            bool: True if the file was moved successfully, False otherwise.
        """
        logger.info(f"Moving file from {from_path} to {to_path}")
        return dbutils.fs.mv(from_path, to_path, recursive)

    @staticmethod
    def list_files(dbutils, dir_path: str) -> list[str]:
        """
        Retrieves a list of files to process for the given table.

        Args:
            dbutils: The dbutils object for interacting with the file system.
            dir_path: The directory path to list files from.
        Returns:
            list[str]: A list of file paths in the specified directory.
        """
        logger.info(f"Listing files in directory: {dir_path}")
        return [file.path for file in dbutils.fs.ls(dir_path)]

    @staticmethod
    def directory_exists(dbutils, dir_path: str) -> bool:
        """
        Checks if the raw folder exists in a specified directory.

        Args:
            dbutils: The dbutils object for interacting with the file system.
            dir_path: The directory path to check for existence.

        Returns:
            bool: True if the directory exists, False otherwise.
        """
        try:
            dbutils.fs.ls(dir_path)
            logger.info(f"Directory exists: {dir_path}")
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                logger.error(f"Directory does not exist: {dir_path}")
                return False

    @staticmethod
    def get_secret(dbutils, secret_scope: str, secret_key: str) -> str:
        """
        Retrieves a secret value from the Databricks secret scope.

        Args:
            dbutils: The dbutils object for accessing secrets.
            secret_scope: The name of the secret scope.
            secret_key: The key of the secret to retrieve.

        Returns:
            str: The value of the secret.
        """
        logger.info(f"Retrieving secret from scope: {secret_scope}, key: {secret_key}")
        return dbutils.secrets.get(secret_scope, secret_key)

    @staticmethod
    def get_script_run_parameters(
        expected_job_params: list = ["environment", "data_contracts"],
    ) -> dict[str, str | list[str]]:
        """
        Retrieves the parameters passed to the Databricks job run.

        Args:
            expected_job_params: A list of expected parameter keys to retrieve from the job run parameters.

        Returns:
            dict[str, str | list[str]]: A dictionary containing the retrieved parameters and their values.
        """
        parser = ArgumentParser()
        parser.add_argument(
            "-e",
            "--environment",
            required=True,
            help="The environment for the pipeline to run. Must be 'dev' or 'prod'.",
        )
        parser.add_argument(
            "-c",
            "--data-contracts",
            nargs="+",
            required=True,
            help="Data contracts to use on the pipeline.",
        )
        parser.add_argument(
            "-l",
            "--landing_path",
            required=False,
            help="Landing path which files are ingested from. Used in pipelines that move files.",
        )
        args = parser.parse_args()

        job_params = {
            param_name: getattr(args, param_name)
            for param_name in expected_job_params
            if hasattr(args, param_name)
        }

        logger.info(f"Retrieved job parameters: {job_params}")
        return job_params
