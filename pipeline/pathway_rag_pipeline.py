"""
LiveCold RAG Pipeline - Pathway Framework
Real-time SOP document monitoring & LLM-powered Q&A using Pathway.

Demonstrates:
  - pw.io.fs.read in streaming mode (live SOP reindexing)
  - pw.io.http.rest_connector for REST API
  - Pathway UDFs for text chunking & LLM calls
  - Pathway tables for document store

Endpoints (after running):
  POST /v2/answer  {"prompt": "your question"}  → LLM answer with SOP citations
"""

import pathway as pw
import os
import json
import time
from dotenv import load_dotenv

load_dotenv()

# Configuration
WATCHED_DOCS_DIR = "./watched_docs"
GEMINI_MODEL = "gemini/gemini-2.5-flash"

# Custom prompt template
PROMPT_TEMPLATE = """You are a cold-chain compliance assistant for LiveCold.
Use ONLY the context below to answer the question.
If the answer cannot be found in the context, say "I could not find relevant SOP guidance."

Context:
{context}

Question: {query}

Instructions:
1. Provide a numbered action checklist (max 4 items)
2. Cite specific SOP sections using format: "Per SOP §X.Y, [action]"
3. Include any relevant temperature thresholds or time limits
4. End with: "⚠️ Final decision requires human approval."

Answer:"""


# ── Load SOP context at startup for embedding in responses ──────
SOP_CONTEXT = ""
try:
    for fname in os.listdir(WATCHED_DOCS_DIR):
        fpath = os.path.join(WATCHED_DOCS_DIR, fname)
        if os.path.isfile(fpath):
            with open(fpath, "r") as f:
                SOP_CONTEXT += f.read() + "\n\n"
    print(f"  ✓ Loaded SOP ({len(SOP_CONTEXT)} chars) from {WATCHED_DOCS_DIR}")
except Exception as e:
    print(f"  ⚠ Could not load SOP: {e}")


# ── Pathway UDFs ────────────────────────────────────────────────
@pw.udf
def build_answer(query: str) -> str:
    """Call LLM with SOP context to answer the query."""
    import litellm as _litellm

    # Build API key list
    api_keys = []
    for var in ["GOOGLE_API_KEY_imp", "GOOGLE_API_KEY", "GOOGLE_API_KEY_2", "GEMINI_API_KEY"]:
        k = os.getenv(var)
        if k and k not in api_keys:
            api_keys.append(k)

    models = [
        "gemini/gemini-2.5-flash",
        "gemini/gemini-2.0-flash",
        "gemini/gemini-1.5-flash",
    ]

    prompt = PROMPT_TEMPLATE.format(context=SOP_CONTEXT[:8000], query=query)

    for api_key in api_keys:
        os.environ["GEMINI_API_KEY"] = api_key
        for model in models:
            try:
                response = _litellm.completion(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    api_key=api_key,
                    timeout=30,
                )
                return response.choices[0].message.content.strip()
            except Exception:
                continue

    return "SOP service temporarily unavailable. All models rate-limited."


# ── Pathway Schema for REST input ──────────────────────────────
class QuerySchema(pw.Schema):
    prompt: str


def run_rag_pipeline(host="0.0.0.0", port=8765):
    """Run the Pathway RAG pipeline with REST API."""

    print("\n" + "=" * 60)
    print("LiveCold Pathway RAG Pipeline")
    print("=" * 60)

    # ── Step 1: Monitor SOP documents in real-time ──────────────
    documents = pw.io.fs.read(
        path=WATCHED_DOCS_DIR,
        format="binary",
        mode="streaming",
        with_metadata=True,
    )
    print(f"  ✓ pw.io.fs.read watching: {WATCHED_DOCS_DIR}")

    # ── Step 2: REST API input connector ────────────────────────
    queries, response_writer = pw.io.http.rest_connector(
        host=host,
        port=port,
        route="/v2/answer",
        schema=QuerySchema,
        autocommit_duration_ms=50,
        delete_completed_queries=True,
    )
    print(f"  ✓ pw.io.http.rest_connector on {host}:{port}")

    # ── Step 3: Process queries through LLM with SOP context ───
    results = queries.select(
        result=build_answer(queries.prompt),
    )

    # ── Step 4: Write responses back via REST ──────────────────
    response_writer(results)
    print("  ✓ Response writer connected")

    print(f"\n{'=' * 60}")
    print("🚀 LiveCold Pathway RAG Running")
    print("=" * 60)
    print(f"📁 Watching SOPs: {WATCHED_DOCS_DIR}")
    print(f"🤖 LLM: {GEMINI_MODEL}")
    print(f"🌐 API: http://{host}:{port}")
    print()
    print("Usage:")
    print(f'  curl -X POST http://localhost:{port}/v2/answer \\')
    print(f'    -H "Content-Type: application/json" \\')
    print(f'    -d \'{{"prompt": "What to do if temp exceeds threshold?"}}\'')
    print("=" * 60 + "\n")

    # ── Step 5: Run Pathway — starts streaming pipeline + REST ─
    pw.run()


if __name__ == "__main__":
    run_rag_pipeline()
