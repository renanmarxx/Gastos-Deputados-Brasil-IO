POST /api/2.0/workspace/import

#!/usr/bin/env bash
set -euo pipefail

echo "🚀 Deploy Databricks Assets"

# -------------------------
# Validação de variáveis
# -------------------------
required_vars=(
  DATABRICKS_HOST
  DATABRICKS_TOKEN
  DATABRICKS_WORKSPACE_BASE
)

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "❌ Variável $var não definida"
    exit 1
  fi
done

# -------------------------
# Paths no workspace
# -------------------------
SCRIPTS_PATH="${DATABRICKS_WORKSPACE_BASE}/scripts"
NOTEBOOKS_PATH="${DATABRICKS_WORKSPACE_BASE}/databricks"

# -------------------------
# Função helper: mkdir workspace
# -------------------------
workspace_mkdir() {
  local path=$1

  curl -s -X POST "${DATABRICKS_HOST}/api/2.0/workspace/mkdirs" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"path\": \"${path}\"}" > /dev/null
}

# -------------------------
# Função helper: upload arquivo
# -------------------------
workspace_upload() {
  local local_file=$1
  local workspace_path=$2
  local language=$3

  echo "⬆️  Upload ${local_file} → ${workspace_path}"

  curl -s -X POST "${DATABRICKS_HOST}/api/2.0/workspace/import" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -F "path=${workspace_path}" \
    -F "overwrite=true" \
    -F "format=SOURCE" \
    -F "language=${language}" \
    -F "content=@${local_file}"
}

# -------------------------
# Criar diretórios
# -------------------------
echo "📁 Criando diretórios no workspace"
workspace_mkdir "${SCRIPTS_PATH}"
workspace_mkdir "${NOTEBOOKS_PATH}"

# -------------------------
# Upload scripts Python
# -------------------------
echo "📦 Upload scripts Python"
for file in scripts/*.py; do
  filename=$(basename "$file")
  workspace_upload "$file" "${SCRIPTS_PATH}/${filename}" "PYTHON"
done

# -------------------------
# Upload notebook executor
# -------------------------
echo "📓 Upload notebook executor"
workspace_upload \
  "databricks/executor_notebook.py" \
  "${NOTEBOOKS_PATH}/executor_notebook" \
  "PYTHON"

echo "✅ Deploy concluído com sucesso"