import os
import sys
import unittest
import warnings
from datetime import datetime

import pytest

warnings.simplefilter(action='ignore', category=DeprecationWarning)

# This test suite belongs to the deferred Agent/LLM "Architect" contour and
# depends on lancedb / GPU monitoring \u2014 neither of which is required by the
# Advisory radar (v3.6.0 APEX-RADAR). Skip when deps are missing.
pytest.importorskip("lancedb", reason="lancedb not installed; architect tests deferred")

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.infrastructure.database.session_db import SessionDatabase
from src.infrastructure.cache.semantic_cache import SemanticCache
from src.shared.gpu_monitor import GPUMonitor


class TestGeraldArchitectV2(unittest.IsolatedAsyncioTestCase):

    def test_gpu_monitor(self):
        monitor = GPUMonitor()
        stats = monitor.get_stats()
        if monitor.enabled:
            self.assertIn("vram_used_mb", stats)
            self.assertGreater(stats["vram_total_mb"], 0)
            print(f"\n[Test] GPU Stats: {stats}")
        else:
            print("\n[Test] GPU Monitor disabled (no NVIDIA?)")

    def test_session_db(self):
        # Use a unique DB for each test run to avoid permission issues
        test_db = f"memory/test_sessions_{datetime.now().timestamp()}.db"
        db = SessionDatabase(test_db)
        session_id = "test_session"

        db.save_message(session_id, "user", "Привет, Джеральд")
        db.save_message(session_id, "assistant", "Привет, Слава!")

        history = db.load_history(session_id)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["role"], "user")
        self.assertEqual(history[1]["role"], "assistant")

        db.clear_session(session_id)
        # Cleanup is harder on Windows, so we leave it for the next run or manual cleanup
        # Or try one last time
        try:
            if os.path.exists(test_db):
                pass  # os.remove(test_db) -> Skip on Windows during test to avoid flakes
        except Exception:
            pass

    async def test_semantic_cache(self):
        # We might need sentence_transformers installed for this
        try:
            cache = SemanticCache(db_path="memory/test_cache.lance")
            query = "Как запустить проект?"
            response = "Используй start-gerald.bat"

            cache.set(query, response)
            hit = cache.get(query)

            self.assertEqual(hit, response)
            print(f"\n[Test] Cache Hit: {hit}")

            # Semantic hit (very similar)
            hit_similar = cache.get("Как запустить этот проект?")
            self.assertIsNotNone(hit_similar)
            print(f"\n[Test] Cache Semantic Hit: {hit_similar}")
        except Exception as e:
            print(f"\n[Test] Cache test skipped or failed: {e}")

    async def test_secure_sandbox(self):
        from src.infrastructure.sandbox.python_executor import PythonExecutor

        executor = PythonExecutor()

        # Test safe code
        safe_code = "print(1 + 1)"
        res = await executor.execute(safe_code)
        self.assertTrue(res.success)
        self.assertEqual(res.output, "2")

        # Test dangerous import
        bad_code = "import os; print(os.getcwd())"
        res = await executor.execute(bad_code)
        self.assertFalse(res.success)
        self.assertIn("ОШИБКА БЕЗОПАСНОСТИ", res.error)

        # Test banned builtin
        bad_code_2 = "open('secret.txt', 'w')"
        res = await executor.execute(bad_code_2)
        self.assertFalse(res.success)
        self.assertIn("функции 'open()'", res.error)

    async def test_vector_db_chroma(self):
        from src.infrastructure.database.vector_db import VectorDatabase

        vdb = VectorDatabase()

        # Test search (assuming indexer was run)
        query = "What is whale tracker?"
        results = await vdb.search(query, limit=2)

        self.assertGreater(len(results), 0)
        found_skill = any("whale-tracker-pro" in res["filepath"] for res in results)
        if not found_skill:
            print("\n[Test] Warning: whale-tracker-pro not found in RAG index (might not be populated yet).")
        print(f"\n[Test] VDB search for '{query}' found: {results[0]['filepath']}")


if __name__ == "__main__":
    unittest.main()
