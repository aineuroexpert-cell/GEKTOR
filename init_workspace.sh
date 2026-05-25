#!/usr/bin/env bash
set -eo pipefail

echo "[GEKTOR INIT] Запуск инициализации среды авангардного квант-движка..."

# 1. Проверка системных требований
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
if [ "$PYTHON_VERSION" != "3.11" ] && [ "$PYTHON_VERSION" != "3.12" ]; then
    echo "[CRITICAL] Требуется Python 3.11 или 3.12. Текущая версия: $PYTHON_VERSION"
    exit 1
fi

# 2. Тюнинг сетевого стека ядра (Низкая латентность для WebSocket)
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    echo "[SYS] Оптимизация параметров ядра Linux под Zero-Latency..."
    sudo sysctl -w net.core.rmem_max=16777216 || true
    sudo sysctl -w net.core.wmem_max=16777216 || true
    sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 16777216" || true
    sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 16777216" || true
fi

# 3. Создание изолированного окружения
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate

# 4. Установка фиксированных зависимостей (Уничтожаем дрейф версий)
pip install --upgrade pip setuptools wheel

cat << 'EOF' > requirements.txt
# Квантовое Ядро и Асинхронность
numpy==1.26.4
pydantic==2.6.4
websockets==12.0
uvloop==0.19.0; sys_platform != 'win32'
async_timeout==4.0.3

# Базы данных и Векторы
motor==3.3.2
pymongo==4.6.2
chromadb==0.4.24

# Инфраструктура и Тесты
pytest==8.1.1
pytest-asyncio==0.23.5
loguru==0.7.2
EOF

pip install -r requirements.txt

# 5. Инициализация структуры проекта (Единая точка входа)
mkdir -p src/{application/{services,tools},domain/entities,infrastructure/{cache,database,gektor_l2,llm/security}}
mkdir -p tests/unit

if [ ! -f "pyproject.toml" ]; then
cat << 'EOF' > pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
python_files = ["test_*.py"]

[tool.mypy]
python_version = "3.11"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
EOF
fi

echo "[SUCCESS] Среда GEKTOR синхронизирована. Передаю управление агентам-кодерам."
