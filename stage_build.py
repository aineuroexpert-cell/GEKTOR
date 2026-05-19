import os
import subprocess
import zipfile
import datetime

def run_compileall():
    print("[1/3] Проверка синтаксиса (compileall)...")
    result = subprocess.run(["python", "-m", "compileall", "."], capture_output=True, text=True)
    if result.returncode != 0:
        print("ОШИБКА: Обнаружены синтаксические ошибки!")
        print(result.stdout)
        print(result.stderr)
        exit(1)
    print("✅ Синтаксис проверен. Ошибок нет.")

def generate_context_snapshot():
    print("[2/3] Генерация иммутабельного снимка контекста...")
    snapshot_path = "context_analysis.txt"
    try:
        result = subprocess.run(["git", "log", "-1"], capture_output=True, text=True)
        git_info = result.stdout if result.returncode == 0 else "Git repository not found or no commits."
    except Exception:
        git_info = "Git not installed or accessible."
        
    with open(snapshot_path, "w", encoding="utf-8") as f:
        f.write(f"GEKTOR HFT - Snapshot Build\n")
        f.write(f"Date: {datetime.datetime.now().isoformat()}\n")
        f.write(f"Git Info:\n{git_info}\n")
    print("✅ Снимок сохранен в context_analysis.txt.")

def pack_archive():
    print("[3/3] Сборка ZIP архива (Исключая мусор и базы данных)...")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_name = f"gektor_build_{timestamp}.zip"
    
    exclude_dirs = {".git", "__pycache__", ".venv", ".mypy_cache", ".pytest_cache", "logs", "artifacts", ".gemini", "_archive_quarantine"}
    exclude_exts = {".db", ".sqlite", ".jsonl", ".log", ".pyc"}
    
    with zipfile.ZipFile(archive_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk("."):
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                if any(file.endswith(ext) for ext in exclude_exts):
                    continue
                if file == archive_name:
                    continue
                
                file_path = os.path.join(root, file)
                zipf.write(file_path, arcname=file_path)
                
    print(f"✅ Успешно запаковано в: {archive_name}")

if __name__ == "__main__":
    print("=== GEKTOR HFT STAGE BUILD ===")
    run_compileall()
    generate_context_snapshot()
    pack_archive()
    print("=== ГОТОВО К WINSCP ===")
