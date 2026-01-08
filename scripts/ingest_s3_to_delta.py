"""Ingestão de CSV compactado (.csv.gz) do S3 para Delta Lake.

Pipeline que:
1. Lê arquivos CSV.gz do S3.
2. Converte para DataFrame Spark.
3. Escreve em formato Delta Lake (tabela otimizada).
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from config import S3_BUCKET, S3_PREFIX, DELTA_TABLE


def ingest_s3_to_delta(
    s3_bucket: str,
    s3_prefix: str,
    delta_table: str,
    spark_session: Optional[SparkSession] = None,
) -> None:
    """Lê CSV.gz do S3 e armazena em Delta Lake.

    Args:
        s3_bucket: nome do bucket S3.
        s3_prefix: prefixo do caminho no S3 (ex: "dados/gastos").
        delta_table: nome da tabela Delta Lake para armazenar.
        spark_session: sessão Spark (se None, usa a ativa).

    Returns:
        None
    """
    if spark_session is None:
        spark_session = SparkSession.builder.getOrCreate()

    # Lê arquivos CSV.gz do S3
    # - formato: "csv"
    # - "inferSchema": detecta tipos automaticamente
    # - "header": primeira linha contém nomes de colunas
    # - "compression": "gzip" para descompactar .gz
    df: DataFrame = (
        spark_session.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("compression", "gzip")
        .load(f"s3a://{s3_bucket}/{s3_prefix}/")
    )

    print(f"DataFrame lido com {df.count()} linhas")

    # Escreve para Delta Lake em modo append
    df.write.format("delta").mode("append").saveAsTable(delta_table)
    print(f"Dados armazenados em tabela Delta: {delta_table}")


def main() -> None:
    """Função principal: ingest S3 → Delta Lake."""
    ingest_s3_to_delta(
        s3_bucket=S3_BUCKET,
        s3_prefix=S3_PREFIX,
        delta_table=DELTA_TABLE,
    )


if __name__ == "__main__":
    main()
