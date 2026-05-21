import asyncio
import os
import sys

import pytest

# ChromaDB is part of the LLM/RAG infrastructure, NOT the Advisory radar.
# Skip cleanly when the package is unavailable.
chromadb = pytest.importorskip("chromadb", reason="chromadb not installed; RAG tests deferred")

# Fix path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


@pytest.mark.asyncio
async def test_chroma_retrieval():
    print("🔍 Testing ChromaDB Skills Retrieval...")

    # Connect directly to Chroma
    client = chromadb.HttpClient(host="localhost", port=8000)
    collection = client.get_collection("gerald-files")

    queries = [
        "What do whales do on Solana?",
        "Find new gem on pump.fun",
        "Optimize trading strategy",
    ]

    for query in queries:
        print(f"\nQUERY: {query}")
        # Chroma search
        results = collection.query(query_texts=[query], n_results=3)

        if results["documents"] and results["documents"][0]:
            for i, (doc, meta) in enumerate(
                zip(results["documents"][0], results["metadatas"][0])
            ):
                print(f"  [{i+1}] Found in: {meta.get('filepath')}")
                print(f"      Snippet: {doc[:150].replace('\\n', ' ')}...")
        else:
            print("  ❌ No results found in ChromaDB.")


if __name__ == "__main__":
    asyncio.run(test_chroma_retrieval())
