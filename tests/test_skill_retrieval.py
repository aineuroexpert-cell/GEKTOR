import asyncio
import os
import sys

import pytest

# Skill retrieval depends on the LLM / Vector-DB / Redis stack \u2014 entirely
# outside the Advisory radar contour (v3.6.0 APEX-RADAR). Skip when deps
# are unavailable.
pytest.importorskip("redis", reason="redis not installed; skill retrieval tests deferred")

# Fix path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.infrastructure.database.vector_db import VectorDatabase
from src.infrastructure.llm.reranker import Reranker


@pytest.mark.asyncio
async def test_skill_retrieval():
    print("🔍 Testing Crypto Skills Retrieval...")
    vdb = VectorDatabase()
    reranker = Reranker()

    queries = [
        "Что там киты на Солане делают?",
        "Найди новый гем на pump.fun",
        "Нужно оптимизировать стратегию трейдинга",
    ]

    for query in queries:
        print(f"\nQUERY: {query}")
        results = await vdb.search(query, limit=5)
        top = reranker.rerank(query, results, top_n=2)

        for i, doc in enumerate(top):
            print(f"  [{i+1}] Found in: {doc['filepath']}")
            print(f"      Snippet: {doc['text'][:100]}...")


if __name__ == "__main__":
    asyncio.run(test_skill_retrieval())
