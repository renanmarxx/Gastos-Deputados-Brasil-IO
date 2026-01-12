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
        SystemExit: se token da Brasil.io, S3_BUCKET ou credenciais AWS forem inválidas.
    """
    # Validar token Brasil.io
    token = brasil_io_token or os.environ.get("BRASIL_IO_TOKEN")
    if not token or token == "":
        raise SystemExit("ERRO: defina BRASIL_IO_TOKEN (variável de ambiente ou config.py)")
    
    # Validar bucket S3
    if not s3_bucket or s3_bucket == "meu-bucket-exemplo":
        raise SystemExit("ERRO: defina AWS_S3_BUCKET com um bucket S3 válido (não use 'meu-bucket-exemplo')")

    # Baixa dados da Brasil.io em memória
    api = BrasilIO(token)
    response = api.download(dataset_slug, table_name)
    csv_content = response.read()
    
    # Salva cópia local para referência
    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)
    out_path = os.path.join("data", f"{dataset_slug}_{table_name}.csv")
    with open(out_path, mode="wb") as fobj:
        fobj.write(csv_content)
    print(f"\nFile stored succesfuly at: {out_path}")

    # Uploading .csv file to the S3 Bucket
    print("\n>>> Starting S3 Bucket upload...")
    
    try:
        csv_bytes = BytesIO(csv_content)
        csv_bytes.seek(0)
        
        s3 = boto3.client("s3")
        today = date.today().isoformat()
        key = f"{s3_prefix}/dt={today}/{dataset_slug}_{table_name}.csv"
        
        s3.upload_fileobj(csv_bytes, s3_bucket, key)
        print(f"Succesful upload to the S3 Bucket: s3://{s3_bucket}/{key}")
    except Exception as e:
        print(f"Error uploading to the S3 Bucket: {type(e).__name__}: {e}")
        raise


def main() -> None:
    """Função principal: ingest de dados Brasil.io para S3."""

    DATASET_SLUG = "gastos-deputados"
    TABLE_NAME = "cota_parlamentar"

    ingest_brasil_io_to_s3(
        dataset_slug = DATASET_SLUG,
        table_name = TABLE_NAME,
        s3_bucket = S3_BUCKET,
        s3_prefix = S3_PREFIX,
        brasil_io_token = BRASIL_IO_TOKEN,
    )

if __name__ == "__main__":
    main()
