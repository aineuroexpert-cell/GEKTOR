# deploy_sync.ps1 - Детерминированная Git-синхронизация локальной среды с сервером
param (
    [string]$TargetIP = "45.76.212.160",
    [string]$TargetDir = "/opt/gektor",
    [string]$User = "root"
)

Write-Host "🛑 [1/3] Локальная фиксация стейта (Git Push)..." -ForegroundColor Cyan
# Фиксируем актуальные зависимости
pip freeze > requirements.txt

# Отправляем иммутабельный стейт в репозиторий
git add .
git commit -m "Deploy sync: Immutable State Update"
git push origin main
if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Ошибка Git Push. Деплой отменен. Убедись, что нет конфликтов." -ForegroundColor Red
    exit 1
}

Write-Host "📦 [2/3] Синхронизация сервера (Git Pull & Hard Reset)..." -ForegroundColor Yellow
$AND = [char]38 + "" + [char]38
$DeployCmd = "cd $TargetDir " + $AND + " git fetch origin " + $AND + " git reset --hard origin/main " + $AND + " source venv/bin/activate " + $AND + " pip install -r requirements.txt " + $AND + " systemctl restart gektor.service"
ssh ($User + "@" + $TargetIP) $DeployCmd

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Ошибка деплоя на сервере!" -ForegroundColor Red
    exit 1
}

Write-Host "✅ [3/3] ДЕПЛОЙ УСПЕШНО ЗАВЕРШЕН. СЕРВЕР ИДЕНТИЧЕН ЛОКАЛЬНОМУ СТЕЙТУ." -ForegroundColor Green
