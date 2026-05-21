# ╔══════════════════════════════════════════════════════════════╗
# ║  GEKTOR APEX — ATOMIC DEPLOY v3.0 (Windows → Tokyo VPS)   ║
# ║  Детерминированная синхронизация + systemd установка        ║
# ╚══════════════════════════════════════════════════════════════╝
param (
    [string]$SSHAlias = "gektor",
    [string]$TargetDir = "/opt/gektor"
)

$ErrorActionPreference = "Stop"
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║     🚀 GEKTOR APEX — ATOMIC DEPLOY v3.0        ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# ═══════════════════════════════════════════
# ФАЗА 1: РАЗВЕДКА СЕРВЕРА
# ═══════════════════════════════════════════
Write-Host "📡 [1/6] Проверка связи с сервером..." -ForegroundColor Yellow
try {
    $result = ssh $SSHAlias "echo OK && python3.11 --version 2>/dev/null || python3 --version"
    Write-Host "  ✅ Связь установлена: $result" -ForegroundColor Green
} catch {
    Write-Host "  ❌ Сервер недоступен! Проверь SSH конфигурацию." -ForegroundColor Red
    exit 1
}

# ═══════════════════════════════════════════
# ФАЗА 2: ПОДГОТОВКА СЕРВЕРА (idempotent)
# ═══════════════════════════════════════════
Write-Host "`n🔧 [2/6] Подготовка сервера..." -ForegroundColor Yellow

$ServerSetup = @'
set -e

# Установка Python 3.11 + Redis (если не установлены)
if ! command -v python3.11 &> /dev/null; then
    apt update -qq
    apt install -y python3.11 python3.11-venv python3.11-dev build-essential
fi

# Redis для EventBus
if ! command -v redis-server &> /dev/null; then
    apt install -y redis-server
fi
systemctl enable redis-server --now 2>/dev/null || true

# Redis: привязка к Unix Socket (performance) + TCP fallback
if ! grep -q "^port 55555" /etc/redis/redis.conf 2>/dev/null; then
    sed -i 's/^port .*/port 55555/' /etc/redis/redis.conf 2>/dev/null || true
    systemctl restart redis-server 2>/dev/null || true
fi

# Проект
mkdir -p /opt/gektor/artifacts /opt/gektor/secrets /opt/gektor/logs

echo "[OK] Server prep complete"
'@

ssh $SSHAlias $ServerSetup
Write-Host "  ✅ Сервер подготовлен" -ForegroundColor Green

# ═══════════════════════════════════════════
# ФАЗА 3: СИНХРОНИЗАЦИЯ КОДА (scp -r)
# ═══════════════════════════════════════════
Write-Host "`n📦 [3/6] Синхронизация кода..." -ForegroundColor Yellow

# Файлы и директории для деплоя
$DeployItems = @(
    "main.py",
    "pyproject.toml",
    "requirements.txt",
    "start.sh",
    "gektor_control.sh",
    "preflight_check.py",
    "CLAUDE.md"
)

# Копируем основные файлы
foreach ($item in $DeployItems) {
    $localPath = Join-Path $ProjectDir $item
    if (Test-Path $localPath) {
        scp -q "$localPath" "${SSHAlias}:${TargetDir}/$item"
        Write-Host "  → $item" -ForegroundColor DarkGray
    }
}

# Копируем src/ директорию целиком
Write-Host "  → src/ (recursive)..." -ForegroundColor DarkGray
scp -q -r "$ProjectDir\src" "${SSHAlias}:${TargetDir}/"

# Копируем .env (SECRETS — только если нет на сервере)
$envExists = ssh $SSHAlias "test -f ${TargetDir}/.env && echo YES || echo NO"
if ($envExists.Trim() -eq "NO") {
    Write-Host "  → .env (первичная установка)" -ForegroundColor Yellow
    scp -q "$ProjectDir\.env" "${SSHAlias}:${TargetDir}/.env"
    ssh $SSHAlias "chmod 600 ${TargetDir}/.env"
} else {
    Write-Host "  → .env (уже на сервере — пропускаем)" -ForegroundColor DarkGray
}

# secrets/ (alpha weights)
if (Test-Path "$ProjectDir\secrets") {
    scp -q -r "$ProjectDir\secrets" "${SSHAlias}:${TargetDir}/"
    Write-Host "  → secrets/" -ForegroundColor DarkGray
}

Write-Host "  ✅ Код синхронизирован" -ForegroundColor Green

# ═══════════════════════════════════════════
# ФАЗА 4: УСТАНОВКА ЗАВИСИМОСТЕЙ
# ═══════════════════════════════════════════
Write-Host "`n🐍 [4/6] Установка Python-окружения..." -ForegroundColor Yellow

$PythonSetup = @'
set -e
cd /opt/gektor

# Создаём venv если нет
if [ ! -d "venv" ]; then
    python3.11 -m venv venv
    echo "[OK] venv created"
fi

source venv/bin/activate
pip install --upgrade pip -q

# Установка зависимостей из pinned requirements.txt
pip install -r requirements.txt -q 2>&1 | tail -5

# uvloop для x86 Linux (ускорение Event Loop)
pip install uvloop -q 2>/dev/null || true

echo "[OK] Dependencies installed"
python --version
'@

ssh $SSHAlias $PythonSetup
Write-Host "  ✅ Python-окружение установлено" -ForegroundColor Green

# ═══════════════════════════════════════════
# ФАЗА 5: SYSTEMD UNIT (автозапуск)
# ═══════════════════════════════════════════
Write-Host "`n⚙️  [5/6] Настройка systemd unit..." -ForegroundColor Yellow

$SystemdUnit = @'
cat > /etc/systemd/system/gektor.service << 'UNIT_EOF'
[Unit]
Description=GEKTOR Advisory Radar — Mid-Term Anomaly Scanner
After=network-online.target redis-server.service
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/gektor
ExecStart=/opt/gektor/venv/bin/python main.py
ExecReload=/bin/kill -HUP $MAINPID

# Graceful Shutdown
KillSignal=SIGTERM
TimeoutStopSec=15
FinalKillSignal=SIGKILL

# Автоперезапуск при крашах
Restart=on-failure
RestartSec=5
StartLimitBurst=5
StartLimitIntervalSec=300

# Security Hardening
NoNewPrivileges=yes
ProtectSystem=strict
ReadWritePaths=/opt/gektor /dev/shm /tmp
PrivateTmp=yes

# Environment
EnvironmentFile=/opt/gektor/.env

# Resource Limits
LimitNOFILE=65536
LimitMEMLOCK=infinity

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=gektor

[Install]
WantedBy=multi-user.target
UNIT_EOF

systemctl daemon-reload
systemctl enable gektor.service
echo "[OK] Systemd unit installed"
'@

ssh $SSHAlias $SystemdUnit
Write-Host "  ✅ Systemd unit установлен и активирован" -ForegroundColor Green

# ═══════════════════════════════════════════
# ФАЗА 6: ЗАПУСК И ВЕРИФИКАЦИЯ
# ═══════════════════════════════════════════
Write-Host "`n🔥 [6/6] Запуск GEKTOR..." -ForegroundColor Yellow

$LaunchCmd = @'
set -e

# Стерилизация SHM
rm -f /dev/shm/psm_* /dev/shm/gektor_* 2>/dev/null || true

# (Ре)старт сервиса
systemctl restart gektor.service
sleep 3

# Проверка статуса
if systemctl is-active --quiet gektor.service; then
    echo "🟢 GEKTOR ALIVE"
    journalctl -u gektor.service -n 15 --no-pager
else
    echo "🔴 GEKTOR DEAD"
    journalctl -u gektor.service -n 30 --no-pager
    exit 1
fi
'@

ssh $SSHAlias $LaunchCmd

Write-Host ""
Write-Host "╔══════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║     ✅ GEKTOR APEX — ДЕПЛОЙ ЗАВЕРШЁН           ║" -ForegroundColor Green
Write-Host "║     📡 Радар активен на Tokyo VPS               ║" -ForegroundColor Green
Write-Host "║     🔍 journalctl -u gektor -f (мониторинг)    ║" -ForegroundColor Green
Write-Host "╚══════════════════════════════════════════════════╝" -ForegroundColor Green
Write-Host ""
Write-Host "Управление:" -ForegroundColor Cyan
Write-Host "  ssh gektor 'journalctl -u gektor -f'         # Live логи" -ForegroundColor DarkGray
Write-Host "  ssh gektor 'systemctl restart gektor'         # Рестарт" -ForegroundColor DarkGray
Write-Host "  ssh gektor 'bash /opt/gektor/gektor_control.sh status'  # Здоровье" -ForegroundColor DarkGray
