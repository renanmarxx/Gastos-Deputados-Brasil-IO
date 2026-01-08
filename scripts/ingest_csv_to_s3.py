import os
import sys
import boto3
from io import BytesIO
from datetime import date
from typing import Optional
from brasil_io import BrasilIO
from config import S3_BUCKET, S3_PREFIX, BRASIL_IO_TOKEN


def ingest_brasil_io_to_s3(
    dataset: str,
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
    token = brasil_io_token or os.environ.get("BRASIL_IO_TOKEN")
    if not token:
        raise SystemExit(
            "ERRO: defina a variável de ambiente BRASIL_IO_TOKEN com seu token Brasil.io"
        )

    # Baixa dados da Brasil.io
    api = BrasilIO(token)
    response = api.download(dataset, table_name)

    # Converte resposta para bytes em memória
    csv_bytes = BytesIO(response.read())
    csv_bytes.seek(0)

    # Faz upload para S3
    s3 = boto3.client("s3")
    today = date.today().isoformat()
    key = f"{s3_prefix}/dt={today}/{dataset}_{table_name}.csv.gz"

    s3.upload_fileobj(csv_bytes, s3_bucket, key)
    print(f"Upload bem-sucedido: s3://{s3_bucket}/{key}")


def main() -> None:
    """Função principal: ingest de dados Brasil.io para S3."""

    DATASET_SLUG = "gastos-deputados"
    TABLE_NAME = "cota_parlamentar"

    ingest_brasil_io_to_s3(
        dataset=DATASET_SLUG,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
    )


if __name__ == "__main__":
    main()
