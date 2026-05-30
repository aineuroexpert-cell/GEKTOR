# deploy_sync.ps1 - Deterministic Git sync to remote production
param (
    [string]$TargetIP = "45.76.212.160",
    [string]$TargetDir = "/opt/gektor",
    [string]$User = "root"
)

Write-Host "[1/3] Git state commit and push..." -ForegroundColor Cyan
pip freeze > requirements.txt

git add .
git commit -m "Deploy sync: Immutable State Update"
git push origin main
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Git push failed. Deployment aborted." -ForegroundColor Red
    exit 1
}

Write-Host "[2/3] Synching server (Git Pull and Hard Reset)..." -ForegroundColor Yellow
$AND = [char]38 + "" + [char]38
$DeployCmd = "cd $TargetDir " + $AND + " git fetch origin " + $AND + " git reset --hard origin/main " + $AND + " source venv/bin/activate " + $AND + " pip install -r requirements.txt " + $AND + " systemctl restart gektor.service"
ssh ($User + "@" + $TargetIP) $DeployCmd

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Deployment command failed on VPS!" -ForegroundColor Red
    exit 1
}

Write-Host "[3/3] DEPLOYMENT COMPLETE. VPS SYNCED WITH LOCAL STATE." -ForegroundColor Green
