import sqlite3
import os
from typing import List, Dict
from src.shared.logger import logger


class SessionDatabase:
    """Relational storage for chat history and session states."""

    def __init__(self, db_path: str = "memory/sessions.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    role TEXT,
                    content TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_session ON messages(session_id)"
            )
            conn.commit()

    def save_message(self, session_id: str, role: str, content: str):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT INTO messages (session_id, role, content) VALUES (?, ?, ?)",
                    (session_id, role, content),
                )
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to save message to DB: {e}")

    def load_history(self, session_id: str, limit: int = 15) -> List[Dict]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT role, content FROM messages WHERE session_id = ? ORDER BY timestamp DESC, id DESC LIMIT ?",
                    (session_id, limit),
                )
                rows = cursor.fetchall()
                # Reverse to get chronological order
                return [
                    {"role": row["role"], "content": row["content"]}
                    for row in reversed(rows)
                ]
        except Exception as e:
            logger.error(f"Failed to load history from DB: {e}")
            return []

    def clear_session(self, session_id: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
            conn.commit()
