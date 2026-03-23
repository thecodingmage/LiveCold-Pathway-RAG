# RAG Response Performance Analysis & Recommendations

After reviewing your entire RAG pipeline ([pathway_rag_pipeline.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline.py), [pathway_rag_pipeline_v2.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline_v2.py), [pathway_integrated_full.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_integrated_full.py)), I identified **6 major bottlenecks** and concrete solutions for each.

---

## Bottleneck 1: Disk I/O on Every Single Query (V2)

**File:** [pathway_rag_pipeline_v2.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline_v2.py) — [build_answer()](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline.py#60-95) → [_read_sop_files_from_disk()](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline_v2.py#67-84)

Every query triggers a full re-read of **all** SOP files from disk (lines 102–147). This means `os.listdir()` + `open()` + [read()](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline_v2.py#67-84) for each file on **every single request**.

### ✅ Solution: In-Memory Cache with File-Watcher Invalidation

```python
import hashlib

_sop_cache = {"content": None, "hash": None}

def _read_sop_files_cached() -> str:
    """Read SOP files with content-hash cache — only re-reads if files changed."""
    file_hashes = []
    for fname in sorted(os.listdir(WATCHED_DOCS_DIR)):
        fpath = os.path.join(WATCHED_DOCS_DIR, fname)
        if os.path.isfile(fpath) and not fname.startswith('.'):
            stat = os.stat(fpath)
            file_hashes.append(f"{fname}:{stat.st_mtime}:{stat.st_size}")
    
    current_hash = hashlib.md5("|".join(file_hashes).encode()).hexdigest()
    
    if _sop_cache["hash"] == current_hash and _sop_cache["content"]:
        return _sop_cache["content"]  # Cache hit — no disk I/O
    
    # Cache miss — read from disk
    context = _read_sop_files_from_disk()
    _sop_cache["content"] = context
    _sop_cache["hash"] = current_hash
    return context
```

**Impact:** Eliminates redundant disk reads; typical queries become ~0ms for SOP loading instead of ~5-15ms.

---

## Bottleneck 2: LLM Model Fallback Cascade

**Files:** Both [pathway_rag_pipeline.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline.py) (line 80–92) and [pathway_rag_pipeline_v2.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline_v2.py) (line 133–145)

The [build_answer()](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline.py#60-95) UDF iterates through **all API keys × all models** sequentially, with a 30s timeout each. Worst case: **4 keys × 3 models × 30s = 360 seconds** before returning a failure.

### ✅ Solution A: Reduce Timeout + Fail Fast

```python
# Use a shorter timeout and only retry on clearly transient errors
response = _litellm.completion(
    model=model,
    messages=[{"role": "user", "content": prompt}],
    api_key=api_key,
    timeout=10,  # ← Was 30, reduce to 10
    num_retries=0,  # ← Don't let litellm retry internally
)
```

### ✅ Solution B: Track Working Key/Model (Skip Known-Bad Combos)

```python
_last_working = {"api_key": None, "model": None}

@pw.udf
def build_answer(query: str) -> str:
    # Try last known working combo first
    if _last_working["api_key"] and _last_working["model"]:
        try:
            response = _litellm.completion(
                model=_last_working["model"],
                messages=[{"role": "user", "content": prompt}],
                api_key=_last_working["api_key"],
                timeout=10,
            )
            return response.choices[0].message.content.strip()
        except Exception:
            pass  # Fall through to full cascade
    
    # Full cascade as fallback...
```

**Impact:** Reduces worst-case latency from 360s → ~10-20s; typical happy-path stays at ~2-5s.

---

## Bottleneck 3: No Context Truncation Strategy

**File:** [pathway_rag_pipeline_v2.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline_v2.py) — line 131: `sop_context[:12000]`

The SOP file is 12,397 chars and you're sending 12,000 chars of raw text to the LLM. This is:
- **Slow:** More tokens = slower LLM response
- **Imprecise:** No semantic filtering — irrelevant SOP sections are included

### ✅ Solution: Semantic Chunking + Relevance Filtering

Instead of dumping the entire SOP, chunk it and only send relevant sections:

```python
def _get_relevant_chunks(query: str, sop_text: str, max_chars: int = 4000) -> str:
    """Simple keyword-based relevance filtering."""
    chunks = sop_text.split("\n\n")  # Split by paragraphs
    query_words = set(query.lower().split())
    
    scored = []
    for chunk in chunks:
        if not chunk.strip():
            continue
        chunk_words = set(chunk.lower().split())
        overlap = len(query_words & chunk_words)
        scored.append((overlap, chunk))
    
    scored.sort(key=lambda x: x[0], reverse=True)
    
    result = []
    total = 0
    for _, chunk in scored:
        if total + len(chunk) > max_chars:
            break
        result.append(chunk)
        total += len(chunk)
    
    return "\n\n".join(result)
```

**Impact:** Reduces prompt token count by ~60-70%, leading to faster LLM response (~1-2s faster per query).

---

## Bottleneck 4: `litellm` Import Inside UDF

**Files:** Both pipelines — `import litellm as _litellm` inside [build_answer()](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline.py#60-95)

The `import litellm` statement runs **on every query invocation**. While Python caches modules, the import machinery still has overhead, and litellm itself does initialization work on import.

### ✅ Solution: Import at Module Level

```python
# At the top of the file
import litellm as _litellm

@pw.udf  
def build_answer(query: str) -> str:
    # Remove: import litellm as _litellm
    ...
```

**Impact:** Saves ~5-20ms per query (small but adds up).

---

## Bottleneck 5: `autocommit_duration_ms=50` is Too Aggressive

**File:** [pathway_rag_pipeline_v2.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_rag_pipeline_v2.py) — line 196

A 50ms autocommit means Pathway processes every micro-batch very frequently, creating overhead. For a Q&A endpoint where queries arrive sporadically, this is unnecessary churn.

### ✅ Solution: Increase Autocommit

```python
queries, response_writer = pw.io.http.rest_connector(
    host=host,
    port=port,
    route="/v2/answer",
    schema=QuerySchema,
    autocommit_duration_ms=500,  # ← Was 50, increase to 500
    delete_completed_queries=True,
)
```

> [!NOTE]
> This adds up to 500ms latency before processing starts, but removes significant overhead. Tune to your needs — 200ms is a good middle ground.

**Impact:** Reduces Pathway framework overhead; net latency may actually decrease under load.

---

## Bottleneck 6: Full Pipeline (Integrated) has No LLM Response Caching

**File:** [pathway_integrated_full.py](file:///d:/liveCold4/Pathway-Hackathon/pathway_integrated_full.py)

Identical alert queries (same shipment + same risk + same temp) call the LLM every single time. In a streaming pipeline with sliding windows, many consecutive alerts may produce **identical RAG queries**.

### ✅ Solution: Add LRU Cache for LLM Responses

```python
from functools import lru_cache

@lru_cache(maxsize=128)
def _cached_llm_call(prompt_hash: str, prompt: str) -> str:
    """Cache LLM responses by prompt content hash."""
    response = _litellm.completion(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        timeout=10,
    )
    return response.choices[0].message.content.strip()
```

**Impact:** Eliminates redundant LLM calls for repeated alerts — can save 2-5s per duplicate query.

---

## Summary: Expected Impact

| Bottleneck | Fix Effort | Latency Saved |
|---|---|---|
| Disk I/O on every query | 🟢 Easy | ~5-15ms/query |
| LLM fallback cascade | 🟢 Easy | Up to 300s worst-case |
| No context truncation | 🟡 Medium | ~1-2s/query |
| Import inside UDF | 🟢 Easy | ~5-20ms/query |
| Aggressive autocommit | 🟢 Easy | Framework overhead |
| No LLM caching | 🟡 Medium | 2-5s/duplicate query |

> [!IMPORTANT]
> **Bottlenecks #2 (fallback cascade) and #3 (context truncation) are the biggest wins.** The LLM call itself dominates latency (~2-8s), so reducing prompt size and avoiding unnecessary retries will have the most dramatic effect.

## Would You Like Me to Implement These?

Let me know which fixes you'd like me to apply. I'd recommend starting with:
1. **#2** — Reduce timeout + try last-working combo first
2. **#3** — Add relevance filtering to reduce prompt size
3. **#1** — Add SOP file caching

These three alone should cut typical response time by **40-60%**.
