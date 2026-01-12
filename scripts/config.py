"""Configurações para ingestão de dados Brasil.io para S3.

Lê variáveis de ambiente por padrão; fornece valores padrão caso não estejam definidas.
Você pode customizar diretamente neste arquivo ou exportar variáveis de ambiente.
"""

import os

# Bucket S3 onde os dados serão armazenados
# Padrão: lê de variável de ambiente AWS_S3_BUCKET; se não existir, usa 'meu-bucket-exemplo'
S3_BUCKET: str = os.environ.get("AWS_S3_BUCKET", "renan-marx-data-engineering-projects")

# Prefixo (caminho) dentro do bucket para armazenar os dados
# Padrão: lê de variável de ambiente AWS_S3_PREFIX; se não existir, usa 'brasil-io/gastos-deputados'
S3_PREFIX: str = os.environ.get("AWS_S3_PREFIX", "gastos-deputados-brasil-io/landing-bucket-gastos-deputados-brasil-io")

# Token da API Brasil.io (lido também no script; aqui é para referência)
# Você deve definir via variável de ambiente: export BRASIL_IO_TOKEN='seu_token_aqui'
# Recomendação: NUNCA faça commit de tokens no repositório
BRASIL_IO_TOKEN: str = os.environ.get("BRASIL_IO_TOKEN", "meu-api-token")

# Nome da tabela Delta Lake onde os dados serão armazenados
# Padrão: lê de variável de ambiente DELTA_TABLE; se não existir, usa 'gastos_deputados'
DELTA_TABLE: str = os.environ.get("BRASIL_IO_TOKEN", "bronze.brasil_io_gastos_deputados")

# Exemplos de uso (copie e execute no terminal):
# export AWS_S3_BUCKET="seu-bucket"
# export AWS_S3_PREFIX="dados/gastos"
# export BRASIL_IO_TOKEN="seu_token_brasil_io"
# export DELTA_TABLE="gastos_deputados"
# python scripts/ingest_csv_to_s3.py
# python scripts/ingest_s3_to_delta.py
