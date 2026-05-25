$TARGET_IP = "45.76.212.160"
$TARGET_DIR = "/opt/gektor"
$USER = "root"
$LOCAL_DIR = "."
$ARCHIVE = "gektor_core.tar.gz"

Write-Host "🛑 [1/4] Terminating active GEKTOR instance..." -ForegroundColor Red
ssh $USER@$TARGET_IP "systemctl stop gektor.service"

Write-Host "📦 [2/4] Compressing core fabric (excluding trash)..." -ForegroundColor Yellow
tar --exclude=".git" --exclude="venv" --exclude="__pycache__" --exclude=".env" --exclude="data_run/logs" --exclude="*.sqlite3-journal" -czf $ARCHIVE -C $LOCAL_DIR .

Write-Host "🚀 [3/4] Transmitting payload to bare-metal..." -ForegroundColor Cyan
scp $ARCHIVE ${USER}@${TARGET_IP}:${TARGET_DIR}/

Write-Host "✅ [4/4] Extracting, rebuilding, and igniting radar..." -ForegroundColor Green
ssh $USER@$TARGET_IP "cd $TARGET_DIR && tar -xzf $ARCHIVE && rm $ARCHIVE && source venv/bin/activate && pip install -r requirements.txt && systemctl daemon-reload && systemctl start gektor.service && systemctl status gektor.service --no-pager"

Remove-Item $ARCHIVE
Write-Host "DEPLOYMENT SECURE." -ForegroundColor Green
