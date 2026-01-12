import os
import sys
import boto3
from io import BytesIO
import shutil
from datetime import date
from typing import Optional
from brasil_io import BrasilIO
from config import S3_BUCKET, S3_PREFIX, BRASIL_IO_TOKEN


def ingest_brasil_io_to_s3(
    dataset: str,
    dataset_slug: str, 
    table_name: str,
    s3_bucket: str,
    s3_prefix: str,
    brasil_io_token: Optional[str] = None,
) -> None:
    """
    Baixa dados da Brasil.io e faz upload para S3.

    Args:
        dataset: slug do dataset (ex: "gastos-deputados").
        table_name: nome da tabela (ex: "cota_parlamentar").
        s3_bucket: nome do bucket S3.
        s3_prefix: prefixo do caminho no S3 (ex: "dados").
        brasil_io_token: token da API Brasil.io. Se None, lê de BRASIL_IO_TOKEN.

    Raises:
        SystemExit: se token da Brasil.io não estiver disponível.
    """

    # Lê token da variável de ambiente se não fornecido
    token = BRASIL_IO_TOKEN or os.environ.get("BRASIL_IO_TOKEN")
    if not token:
        raise SystemExit(
            "ERROR: define the environment variable BRASIL_IO_TOKEN with your Brasil.io"
        )

    # Baixa dados da Brasil.io
    API = BrasilIO(token)
    
    # Connects to the API:
    response = API.download(dataset_slug, table_name)

    # Check if `data/` folder exists, if so cleans the entire directory. Otherwise, it will create a new folder:
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)

    # Defining file path:
    out_path = os.path.join("data", f"{dataset_slug}_{table_name}.csv.gz")

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

    # Converte resposta para bytes em memória
    #csv_bytes = BytesIO(response.read())
    #csv_bytes.seek(0)
#
    ## Faz upload para S3
    #s3 = boto3.client("s3")
    #today = date.today().isoformat()
    #key = f"{s3_prefix}/dt={today}/{dataset}_{table_name}.csv.gz"
#
    #s3.upload_fileobj(csv_bytes, s3_bucket, key)
    #print(f"Upload bem-sucedido: s3://{s3_bucket}/{key}")


def main() -> None:
    """Função principal: ingest de dados Brasil.io para S3."""

    DATASET_SLUG = "gastos-deputados"
    TABLE_NAME = "cota_parlamentar"

    ingest_brasil_io_to_s3(
        dataset=DATASET_SLUG,
        dataset_slug = DATASET_SLUG, 
        table_name = TABLE_NAME,
        s3_bucket = S3_BUCKET,
        s3_prefix = S3_PREFIX,
    )


if __name__ == "__main__":
    main()
