import os
import shutil
import datetime
from src.shared.logger import logger
from src.shared.config import config


class BackupSystem:
    """
    Automated backup for Gerald's memory and configurations.
    """

    def __init__(self):
        self.backup_dir = "backups"
        os.makedirs(self.backup_dir, exist_ok=True)

    def create_backup(self):
        """Backup LanceDB and Config."""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        target = os.path.join(self.backup_dir, f"backup_{timestamp}")

        logger.info(f"Creating system backup: {target}")
        try:
            os.makedirs(target)

            # 1. Backup LanceDB
            lancedb_dir = os.path.dirname(config.memory.vector_store_path)
            if os.path.exists(lancedb_dir):
                shutil.copytree(
                    lancedb_dir, os.path.join(target, "memory"), dirs_exist_ok=True
                )

            # 2. Backup Configs
            shutil.copy2("config.yaml", os.path.join(target, "config.yaml"))
            shutil.copy2("SOUL.md", os.path.join(target, "SOUL.md"))

            logger.info("Backup completed successfully.")
            self._prune_old_backups()
        except Exception as e:
            logger.error(f"Backup failed: {e}")

    def _prune_old_backups(self, keep: int = 5):
        """Keep only the latest backups."""
        backups = sorted(
            [os.path.join(self.backup_dir, d) for d in os.listdir(self.backup_dir)]
        )

        while len(backups) > keep:
            oldest = backups.pop(0)
            if os.path.isdir(oldest):
                shutil.rmtree(oldest)
            else:
                os.remove(oldest)
            logger.info(f"Pruned old backup: {oldest}")
