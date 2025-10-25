import csv
import gzip
import io
import json
import os
import shutil
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen


class BrasilIO:

    base_url = "https://api.brasil.io/v1/"

    def __init__(self, auth_token):
        self.__auth_token = auth_token

    @property
    def headers(self):
        return {
            "User-Agent": "python-urllib/brasilio-client-0.1.0",
        }
        
    @property
    def api_headers(self):
        data = self.headers
        data.update({"Authorization": f"Token {self.__auth_token}"})
        return data

    def api_request(self, path, query_string=None):
        url = urljoin(self.base_url, path)
        if query_string:
            url += "?" + urlencode(query_string)
        request = Request(url, headers=self.api_headers)
        response = urlopen(request)
        return json.load(response)

    def data(self, dataset_slug, table_name, filters=None):
        url = f"dataset/{dataset_slug}/{table_name}/data/"
        filters = filters or {}
        filters["page"] = 1

        finished = False
        while not finished:
            response = self.request(url, filters)
            next_page = response.get("next", None)
            for row in response["results"]:
                yield row
            filters = {}
            url = next_page
            finished = next_page is None

    def download(self, dataset, table_name):
        url = f"https://data.brasil.io/dataset/{dataset}/{table_name}.csv.gz"
        request = Request(url, headers=self.headers)
        response = urlopen(request)
        return response


if __name__ == "__main__":
    api = BrasilIO("meu-api-token")
    dataset_slug = "gastos-deputados"
    table_name = "cota_parlamentar"

    # Para baixar o arquivo completo:

    # Após fazer o download, você salvá-lo no disco ou percorrer o arquivo em memória. 
    
    # Para salvá-lo em memória (em streaming, dentro da pasta `data/`):
    response = api.download(dataset_slug, table_name)
    
    # Limpa a pasta "data" - pasta que armazenará os arquivos de ingestão -  por completo (remove e recria)
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)

    # Define o caminho do arquivo de saída
    out_path = os.path.join("data", f"{dataset_slug}_{table_name}.csv.gz")

    # grava em chunks para não carregar todo o arquivo em memória
    chunk_size = 16 * 1024
    
    with open(out_path, mode="wb") as fobj:
        while True:
            chunk = response.read(chunk_size)
            if not chunk:
                break
            fobj.write(chunk)

    # Caso queira percorrer o CSV em memória:
    response = api.download(dataset_slug, table_name)
    fobj = io.TextIOWrapper(gzip.GzipFile(fileobj=response), encoding="utf-8")
    reader = csv.DictReader(fobj)
    #for row in reader:
    #    print(row)
        #pass  # faça algo com `row`

    # Para navegar pela API:
    #filters = {"state": "PR", "is_last": True}
    #data = api.data(dataset_slug, table_name, filters)
    #for row in data:
    #    print(row)
        #pass  # faça algo com `row`