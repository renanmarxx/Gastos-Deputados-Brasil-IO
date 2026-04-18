from argparse import ArgumentParser
from builtins import Exception, bool, dict, getattr, hasattr, list, staticmethod, str

from chaos.helpers.logging import logger


class DatabricksHelper:
    @staticmethod
    def move_file(dbutils, from_path: str, to_path: str, recursive: bool = False) -> bool:
        """
        Moves a file form the source path to the destination path.

        Args:
            dbutils: The dbutils object.
            from_path (str): The source path of the file to be moved.
            to_path (str): The destination path where the file should be moved.
            recursive (bool, optional): Whether to move directories recursively. Default is False.

        Returns:
            bool: True if the file was moved successfully, False otherwise.
        """
        logger.info(f"Moving file from {from_path} to {to_path} (recursive={recursive})")
        return dbutils.fs.mv(from_path, to_path, recursive)

    @staticmethod
    def list_files(dbutils, dir_path: str) -> list[str]:
        """
        Retrieves a list of files to process for the given table.

        Args:
            dbutils: The dbutils object for interacting with file system.
            dir_path (str): The path for which to retrieve the files.

        Returns:
            list[str]: An ordened list of file paths to process.
        """
        return [file.path for file in dbutils.fs.ls(dir_path)]

    @staticmethod
    def directory_exists(dbutils, dir_path: str) -> bool:
        """
        Check if a directory exists at the specified path.

        Args:
            dbutils: The dbutils object for interacting with file system.
            dir_path (str): The path of the directory to check.

        Returns:
            bool: True if the directory exists, False otherwise.
        """
        try:
            dbutils.fs.ls(dir_path)
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                logger.error(f"Folder: {dir_path} does not exist.")
                return False

        return True

    @staticmethod
    def get_secret(dbutils, secret_scope: str, secret_key: str) -> str:
        """
        Retrieves a secret value from the specified secret scope and key.

        Args:
            dbutils: The dbutils object for interacting with secrets.
            secret_scope (str): The name of the secret scope.
            secret_key (str): The key of the secret to retrieve.

        Returns:
            str: The value of the retrieved secret.
        """
        return dbutils.secrets.get(scope=secret_scope, key=secret_key)

    @staticmethod
    def get_script_run_parameters(
        expected_job_params: list = ["environment", "data_contracts"]
    ) -> dict[str, str] | list[str]:
        """
        Retrieves the parameters passed to the script when executed as a Databricks job.

        Args:
            expected_job_params (list, optional): A list of expected parameter names.
            Default is ["environment", "data_contracts"].

        Returns:
            dict[str, str] | list[str]: A dictionary of parameter names and their corresponding values
                if all expected parameters are present, otherwise a list of missing parameter names.

        Raises:
            Exception: If any of the expected parameters are not found in the job run.
        """
        parser = ArgumentParser()
        parser.add_argument(
            "-e",
            "--environment",
            required=True,
            help="The environment for which to run the script (e.g., dev, staging, prod).",
        )
        parser.add_argument(
            "-c",
            "--data-contracts",
            nargs="+",
            required=True,
            help="A list of data contracts to process in the pipeline.",
        )
        parser.add_argument(
            "-l",
            "--landing_path",
            required=False,
            help="The landing path where the source files are located.",
        )
        args = parser.parse_args()

        job_params = {
            param_name: getattr(args, param_name) for param_name in expected_job_params if hasattr(args, param_name)
        }

        logger.info(f"Retrieved job parameters: {job_params}")
        return job_params
