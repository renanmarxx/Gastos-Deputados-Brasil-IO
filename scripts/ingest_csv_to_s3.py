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
    dataset_slug: str,
    table_name: str,
    s3_bucket: str,
    s3_prefix: str,
    brasil_io_token: Optional[str] = None,
) -> None:
    """Baixa dados da Brasil.io e faz upload para S3.

    Args:
        dataset_slug: slug do dataset (ex: "gastos-deputados").
        table_name: nome da tabela (ex: "cota_parlamentar").
        s3_bucket: nome do bucket S3.
        s3_prefix: prefixo do caminho no S3 (ex: "dados").
        brasil_io_token: token da API Brasil.io. Se None, lê de BRASIL_IO_TOKEN.

    Raises:
        SystemExit: se token da Brasil.io ou credenciais AWS forem inválidas.
    """

    # Baixa dados da Brasil.io
    api = BrasilIO(token)
    response = api.download(dataset_slug, table_name)

    # Lê a resposta uma única vez em memória
    csv_content = response.read()
    csv_bytes = BytesIO(csv_content)
    csv_bytes.seek(0)

    # Faz upload para S3
    # AVISO: Configure credenciais AWS antes de executar:
    #   export AWS_ACCESS_KEY_ID="sua_chave"
    #   export AWS_SECRET_ACCESS_KEY="sua_chave_secreta"
    #   export AWS_DEFAULT_REGION="us-east-1"
    # Ou configure em ~/.aws/credentials
    try:
        s3 = boto3.client("s3")
        today = date.today().isoformat()
        key = f"{s3_prefix}/dt={today}/{dataset_slug}_{table_name}.csv.gz"
        s3.upload_fileobj(csv_bytes, s3_bucket, key)
        print(f"✓ Upload bem-sucedido: s3://{s3_bucket}/{key}")
    except Exception as e:
        print(f"✗ Erro no upload para S3: {e}")
        raise


def main() -> None:
    """Função principal: ingest de dados Brasil.io para S3."""

    DATASET_SLUG = "gastos-deputados"
    TABLE_NAME = "cota_parlamentar"

    ingest_brasil_io_to_s3(
        dataset_slug=DATASET_SLUG,
        table_name=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
    )


if __name__ == "__main__":
    main()
