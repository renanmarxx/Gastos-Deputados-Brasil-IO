import csv
import gzip
import io
import json
import os
import shutil
from typing import Any, Dict, Iterator, Optional, BinaryIO
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


class BrasilIO:

    BASE_URL = "https://api.brasil.io/v1/"

    def __init__(self, auth_token: str) -> None:
        self.__auth_token = auth_token

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "User-Agent" : "python-urllib/brasilio-client-0.1.0",
        }

    @property
    def api_headers(self) -> Dict[str, str]:
        data = self.headers
        data.update({"Authorization": f"Token {self.__auth_token}"})
        return data

    def api_request(
        self, path: str, query_string: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Request to Brasil.io API and returns a JSON dictionary.

        Args:
            path: API relative path (e.g.: "dataset/.../data/").
            query_string: query parameters (optional).

        Returns:
            Decoding JSON as `dict`.
        """

        url = urljoin(self.BASE_URL, path)
        if query_string:
            url += "?" + urlencode(query_string)
        request = Request(url, headers=self.api_headers)
        response = urlopen(request)
        return json.load(response)

    def data(
        self,
        DATASET_SLUG: str,
        TABLE_NAME: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterator over dataset rows.

        Args:
            DATASET_SLUG: dataset slug (e.g.: "gastos-deputados").
            TABLE_NAME: nome da tabela (e.g.: "cota_parlamentar").
            filters: query aditional filters (opcional).

        Yields:
            Each line returned by the API as `dict`.
        """

        url = f"dataset/{DATASET_SLUG}/{TABLE_NAME}/data/"
        filters = filters or {}
        filters["page"] = 1

        finished = False
        while not finished:
            response = self.api_request(url, filters)
            next_page = response.get("next", None)
            for row in response["results"]:
                yield row
            filters = {}
            url = next_page
            finished = next_page is None

    def download(self, dataset: str, TABLE_NAME: str) -> BinaryIO:
        """
        Downloads the dataset file on .csv.gz format and returns a binary object with its content.

        Args:
            dataset: dataset slug.
            TABLE_NAME: table name.

        Returns:
            Binary (`BinaryIO`) with the file content (`read()`).
        """

        url = f"https://data.brasil.io/dataset/{dataset}/{TABLE_NAME}.csv.gz"
        request = Request(url, headers=self.headers)
        response = urlopen(request)
        return response


if __name__ == "__main__":
    API = BrasilIO("meu-api-token")
    DATASET_SLUG = "gastos-deputados"
    TABLE_NAME = "cota_parlamentar"

    # To download the full file:
    # After downloading, it will store the file on local memory in `data/` folder.

    # Connects to the API:
    response = API.download(DATASET_SLUG, TABLE_NAME)

    # Check if `data/` folder exists, if so cleans the entire directory. Otherwise, it will create a new folder:
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)

    # Defining file path:
    out_path = os.path.join("data", f"{DATASET_SLUG}_{TABLE_NAME}.csv.gz")

    # Defining chunks to store the file to avoid memory overloads:
    chunk_size = 16 * 1024

    # Writing the file in chunks:
    with open(out_path, mode="wb") as fobj:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            fobj.write(chunk)

    print(f"File stored succesfuly at: {out_path}")
