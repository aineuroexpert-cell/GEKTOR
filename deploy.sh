#!/usr/bin/env bash
# GEKTOR APEX: ATOMIC BLUE-GREEN DEPLOYMENT PROTOCOL
set -euo pipefail
IFS=$'\n\t'

BASE_DIR="/opt/gektor"
TIMESTAMP=$(date +%Y%m%d%H%M%S)
RELEASE_DIR="${BASE_DIR}/releases/${TIMESTAMP}"
CURRENT_LINK="${BASE_DIR}/current"
SERVICE_NAME="gektor.service"

echo "[GEKTOR] Initiating Atomic Blue-Green Deployment: ${TIMESTAMP}"

# 1. Клонирование и изоляция (без мусора .git)
mkdir -p "${RELEASE_DIR}"
git archive main | tar -x -C "${RELEASE_DIR}"
cd "${RELEASE_DIR}"

# 2. Идемпотентная гидрация через Poetry (тотальный Lock)
echo "[GEKTOR] Hydrating dependencies strictly from poetry.lock..."
python3.11 -m venv venv
source venv/bin/activate
pip install poetry --quiet
poetry install --only main --no-interaction --no-root --quiet

# 3. Хардкорный Pre-flight Audit
echo "[GEKTOR] Running Physical Hardware & Latency Validators..."
python src/preflight_check.py

# 4. Атомарное переключение (Zero-Downtime)
echo "[GEKTOR] Physical checks passed. Atomic symlink switch..."
ln -sfn "${RELEASE_DIR}" "${CURRENT_LINK}"

# 5. Hot-Reload через SIGHUP (Без потери дескрипторов)
echo "[GEKTOR] Triggering Hot-Reload (os.execv)..."
sudo systemctl reload "${SERVICE_NAME}"
sudo systemctl status "${SERVICE_NAME}" --no-pager | grep "Active:"

echo "[APPROVED_EXECUTION] Deployment successful. Alpha Engine online."
