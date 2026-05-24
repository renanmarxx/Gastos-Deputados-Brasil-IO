import json
from typing import Any, BinaryIO, Dict, Iterator, Optional
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen
from builtins import property, str

from arpad.core.settings import BrasilIOConfig


class BrasilIO:
    BASE_URL = BrasilIOConfig.BASE_URL

    def __init__(self, auth_token: str) -> None:
        self.__auth_token = auth_token

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "User-Agent": "python-urllib/brasilio-client-0.1.0",
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
        dataset: str,
        table_name: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Iterator over dataset rows.

        Args:
            dataset: dataset slug (e.g.: "gastos-deputados").
            table_name: nome da tabela (e.g.: "cota_parlamentar").
            filters: query aditional filters (opcional).

        Yields:
            Each line returned by the API as `dict`.
        """

        url = f"dataset/{dataset}/{table_name}/data/"
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

    def download(self, dataset: str, table_name: str) -> BinaryIO:
        """
        Downloads the dataset file on .csv format
        and returns a binary object with its content.

        Args:
            dataset: dataset slug.
            table_name: table name.

        Returns:
            Binary (`BinaryIO`) with the file content (`read()`).
        """

        url = f"{self.BASE_URL}/dataset/{dataset}/{table_name}.csv.gz"
        request = Request(url, headers=self.headers)
        response = urlopen(request)
        return response
