#!/usr/bin/env python3
"""
Semantic Near-Duplicate Detector (Contracts/Invoices/Docs)
-----------------------------------------------------------
Detects semantically near-duplicate documents (e.g., same contract/invoice scanned twice,
reformatted PDFs, or slight edits) using embeddings + cosine similarity. Optionally, a
GenAI LLM can summarize the differences for high-similarity pairs to aid review.

Features
- Input: a folder of .pdf, .docx, or .txt files
- Text extraction (PyPDF, python-docx, or plain text)
- Normalization + paragraph-aware chunking with overlap
- Embeddings backends:
    1) SBERT (local): sentence-transformers 'all-MiniLM-L6-v2' (fast, no API)
    2) OpenAI (remote): text-embedding-3-large or text-embedding-3-small
- Similarity metrics:
    - Document-level cosine similarity (mean pooled chunk embeddings)
    - Max chunk-to-chunk similarity (catch small-but-identical sections)
- Optional LLM explanations (OpenAI) for pairs above a threshold:
    - Short, structured diff summary (e.g., parties, amounts, dates changed)
- Outputs: CSV report of candidate duplicates + (optional) JSONL explanations

Quickstart
----------
1) Install deps (choose SBERT-only or with OpenAI support):

   # Core
   pip install numpy scipy scikit-learn tqdm pypdf python-docx

   # Local embeddings (recommended default)
   pip install sentence-transformers

   # Optional: OpenAI embeddings / LLM explanations
   pip install openai

2) Run (local SBERT, threshold=0.88):
   python near_duplicate_detection.py --input ./docs --backend sbert --threshold 0.88 --report duplicates.csv

3) Run (OpenAI embeddings + diff explanations):
   export OPENAI_API_KEY=sk-....
   python near_duplicate_detection.py --input ./docs --backend openai --threshold 0.9 \
       --report duplicates.csv --explain --explain-model gpt-4o-mini

Notes
-----
- For large corpora, consider pre-filtering by MinHash/LSH or file hash; you can add
  a size gate (--min_chars) to skip very small files.
- Adjust chunk_size/overlap if your docs are very long or very short.
- The --max-pairs limits how many candidate pairs to explain to control costs.

Author: (you)
License: MIT
"""
from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity

# Optional imports guarded at runtime
# - PyPDF2 / pypdf for PDF text
# - python-docx for docx text
# - sentence_transformers for SBERT
# - openai for OpenAI API


# ---------------
# I/O Utilities
# ---------------

SUPPORTED_EXTS = {'.pdf', '.docx', '.txt'}


def read_text_from_file(path: Path) -> str:
    ext = path.suffix.lower()
    if ext == '.pdf':
        return _read_pdf(path)
    elif ext == '.docx':
        return _read_docx(path)
    elif ext == '.txt':
        return path.read_text(encoding='utf-8', errors='ignore')
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _read_pdf(path: Path) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as e:
        raise ImportError("Missing dependency 'pypdf'. Install with: pip install pypdf") from e

    text_parts = []
    with path.open('rb') as f:
        reader = PdfReader(f)
        for page in reader.pages:
            try:
                text_parts.append(page.extract_text() or "")
            except Exception:
                # Fallback: skip page if extraction fails
                text_parts.append("")
    return "\n".join(text_parts)


def _read_docx(path: Path) -> str:
    try:
        import docx  # python-docx
    except ImportError as e:
        raise ImportError("Missing dependency 'python-docx'. Install with: pip install python-docx") from e

    doc = docx.Document(str(path))
    paras = [p.text for p in doc.paragraphs]
    return "\n".join(paras)


# -------------------
# Text Preprocessing
# -------------------

def normalize_text(s: str) -> str:
    # Light normalization: collapse whitespace, standardize punctuation spacing
    s = s.replace('\r', '\n')
    s = re.sub(r'[ \t]+', ' ', s)
    s = re.sub(r'\n{3,}', '\n\n', s)
    s = s.strip()
    return s


def split_into_paragraphs(text: str) -> List[str]:
    # Split on blank lines; keep paragraphs reasonably sized
    paras = [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip()]
    return paras


def chunk_paragraphs(paragraphs: List[str], chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    """
    Make chunks around `chunk_size` characters, respecting paragraph boundaries,
    with soft overlap between adjacent chunks to improve robustness.
    """
    chunks = []
    cur = []
    cur_len = 0

    for p in paragraphs:
        if cur_len + len(p) + 1 <= chunk_size:
            cur.append(p)
            cur_len += len(p) + 1
        else:
            if cur:
                chunks.append("\n\n".join(cur))
                # overlap: carry tail of previous chunk to next
                if overlap > 0:
                    tail = "\n\n".join(cur)[-overlap:]
                    cur = [tail, p]
                    cur_len = len(tail) + len(p) + 1
                else:
                    cur = [p]
                    cur_len = len(p) + 1
            else:
                # single paragraph longer than chunk_size
                chunks.append(p[:chunk_size])
                if overlap > 0:
                    tail = p[:chunk_size][-overlap:]
                    remainder = p[chunk_size:]
                    cur = [tail, remainder]
                    cur_len = len(tail) + len(remainder) + 1
                else:
                    cur = [p[chunk_size:]]
                    cur_len = len(p) - chunk_size

    if cur:
        chunks.append("\n\n".join(cur))

    # final tidy
    return [c.strip() for c in chunks if c.strip()]


# -------------------
# Embedding Backends
# -------------------

class Embedder:
    def embed(self, texts: List[str]) -> np.ndarray:
        raise NotImplementedError


class SBERTEmbedder(Embedder):
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise ImportError(
                "Missing dependency 'sentence-transformers'. "
                "Install with: pip install sentence-transformers"
            ) from e
        self.model = SentenceTransformer(model_name)

    def embed(self, texts: List[str]) -> np.ndarray:
        return np.asarray(self.model.encode(texts, normalize_embeddings=True, convert_to_numpy=True))


class OpenAIEmbedder(Embedder):
    def __init__(self, model_name: str = "text-embedding-3-large"):
        try:
            import openai  # legacy; we use client below
            from openai import OpenAI
        except ImportError as e:
            raise ImportError(
                "Missing dependency 'openai'. Install with: pip install openai"
            ) from e
        self.model_name = model_name
        self.client = OpenAI()

    def embed(self, texts: List[str]) -> np.ndarray:
        # OpenAI embeddings API (batched for efficiency)
        # Note: set OPENAI_API_KEY in env
        from openai import OpenAI
        client = self.client
        # OpenAI supports up to certain tokens per request; keep batches modest
        batch = 64
        vectors: List[np.ndarray] = []
        for i in range(0, len(texts), batch):
            chunk = texts[i:i + batch]
            resp = client.embeddings.create(model=self.model_name, input=chunk)
            vecs = [np.array(d.embedding, dtype=np.float32) for d in resp.data]
            # normalize to unit length for cosine
            vecs = [v / (np.linalg.norm(v) + 1e-8) for v in vecs]
            vectors.extend(vecs)
        return np.vstack(vectors)


# ------------------------
# Similarity Computations
# ------------------------

def mean_pool(embs: np.ndarray) -> np.ndarray:
    v = embs.mean(axis=0)
    norm = np.linalg.norm(v) + 1e-8
    return v / norm


def pairwise_doc_scores(
    doc_chunks_embs: Dict[str, np.ndarray],
) -> List[Tuple[str, str, float, float]]:
    """
    Returns a list of tuples: (docA, docB, doc_sim, max_chunk_sim)
      - doc_sim: cosine(sim) between mean pooled embeddings
      - max_chunk_sim: maximum chunk-to-chunk cosine similarity between the two docs
    """
    results = []
    docs = sorted(doc_chunks_embs.keys())
    for a_idx in range(len(docs)):
        for b_idx in range(a_idx + 1, len(docs)):
            A = docs[a_idx]
            B = docs[b_idx]
            A_embs = doc_chunks_embs[A]
            B_embs = doc_chunks_embs[B]
            A_mean = mean_pool(A_embs).reshape(1, -1)
            B_mean = mean_pool(B_embs).reshape(1, -1)
            doc_sim = float(cosine_similarity(A_mean, B_mean)[0][0])

            # Chunk-level max sim (compute efficiently in blocks if needed)
            sims = cosine_similarity(A_embs, B_embs)
            max_chunk_sim = float(np.max(sims))

            results.append((A, B, doc_sim, max_chunk_sim))
    return results


# ------------------------
# Optional LLM Explainer
# ------------------------

def explain_differences_openai(
    file_a: str,
    file_b: str,
    text_a: str,
    text_b: str,
    model: str = "gpt-4o-mini",
    max_chars: int = 2500,
) -> Dict:
    """
    Use an LLM to summarize concrete differences. Truncates inputs to max_chars each.
    Returns a python dict (to be written as JSON).
    """
    try:
        from openai import OpenAI
    except ImportError as e:
        raise ImportError(
            "Missing dependency 'openai'. Install with: pip install openai"
        ) from e
    client = OpenAI()

    prompt = f"""
You are a contracts/invoices reviewer. Two documents are semantically near-duplicates.
Identify precise differences in a compact, bullet-style JSON. Focus on:
- Parties, dates, amounts/currency, line items, signatures, jurisdictions, terms changed.

Return JSON with fields:
- "summary": one-sentence overview of key diffs
- "changed_entities": list of {{"field":"...", "old":"...", "new":"..."}}
- "risk_flags": list of short strings (e.g., "amount changed", "date altered")

Doc A (truncated):
{text_a[:max_chars]}

Doc B (truncated):
{text_b[:max_chars]}
"""
    messages = [
        {"role": "system", "content": "You are a precise, compliance-focused document reviewer."},
        {"role": "user", "content": prompt.strip()},
    ]
    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.1,
        response_format={"type": "json_object"},
    )
    try:
        content = resp.choices[0].message.content
        data = json.loads(content)
        return {
            "file_a": file_a,
            "file_b": file_b,
            "model": model,
            "explanation": data,
        }
    except Exception:
        # Fallback: return raw text if JSON parse fails
        return {
            "file_a": file_a,
            "file_b": file_b,
            "model": model,
            "explanation": {"summary": resp.choices[0].message.content},
        }


# ---------------
# Main Pipeline
# ---------------

@dataclass
class DocRecord:
    path: Path
    text: str
    chunks: List[str]
    embeddings: Optional[np.ndarray] = None


def collect_documents(input_dir: Path, min_chars: int = 200) -> List[DocRecord]:
    docs: List[DocRecord] = []
    for p in sorted(input_dir.glob("**/*")):
        if not p.is_file():
            continue
        if p.suffix.lower() not in SUPPORTED_EXTS:
            continue
        try:
            raw = read_text_from_file(p)
        except Exception as e:
            print(f"[WARN] Skipping {p} due to read error: {e}", file=sys.stderr)
            continue
        norm = normalize_text(raw)
        if len(norm) < min_chars:
            continue
        paras = split_into_paragraphs(norm)
        chunks = chunk_paragraphs(paras, chunk_size=1000, overlap=200)
        docs.append(DocRecord(path=p, text=norm, chunks=chunks))
    return docs


def build_embeddings(docs: List[DocRecord], backend: str, emb_model: str) -> None:
    if backend == 'sbert':
        embedder = SBERTEmbedder(emb_model)
    elif backend == 'openai':
        embedder = OpenAIEmbedder(emb_model)
    else:
        raise ValueError("backend must be one of: 'sbert', 'openai'")

    all_chunks: List[str] = []
    offsets: List[Tuple[int, int]] = []  # (start_idx, end_idx) per doc
    idx = 0
    for d in docs:
        n = len(d.chunks)
        all_chunks.extend(d.chunks)
        offsets.append((idx, idx + n))
        idx += n

    embs = embedder.embed(all_chunks)

    for d, (s, e) in zip(docs, offsets):
        d.embeddings = embs[s:e]


def main():
    ap = argparse.ArgumentParser(description="Semantic near-duplicate detector for documents.")
    ap.add_argument("--input", required=True, help="Input directory containing .pdf/.docx/.txt")
    ap.add_argument("--backend", default="sbert", choices=["sbert", "openai"], help="Embedding backend")
    ap.add_argument("--emb-model", default=None, help="Embedding model name (sbert or openai)")
    ap.add_argument("--threshold", type=float, default=0.9, help="Similarity threshold for flagging")
    ap.add_argument("--report", default="duplicates_report.csv", help="CSV output path")
    ap.add_argument("--explain", action="store_true", help="Use LLM to summarize diffs for flagged pairs (OpenAI)")
    ap.add_argument("--explain-model", default="gpt-4o-mini", help="Chat model for explanations")
    ap.add_argument("--jsonl", default=None, help="Optional JSONL output with explanations")
    ap.add_argument("--min-chars", type=int, default=200, help="Skip docs with fewer characters")
    ap.add_argument("--max-pairs", type=int, default=50, help="Cap explanations to control cost")
    args = ap.parse_args()

    input_dir = Path(args.input).expanduser().resolve()
    if not input_dir.exists():
        print(f"[ERROR] Input directory not found: {input_dir}", file=sys.stderr)
        sys.exit(1)

    # Choose defaults for embedding models
    if args.emb_model is None:
        if args.backend == 'sbert':
            args.emb_model = "sentence-transformers/all-MiniLM-L6-v2"
        else:
            args.emb_model = "text-embedding-3-large"

    print(f"[INFO] Collecting documents from: {input_dir}")
    docs = collect_documents(input_dir, min_chars=args.min_chars)
    if len(docs) < 2:
        print("[ERROR] Need at least 2 documents to compare.", file=sys.stderr)
        sys.exit(1)
    print(f"[INFO] Loaded {len(docs)} docs. Building embeddings with backend={args.backend}, model={args.emb_model}")

    build_embeddings(docs, backend=args.backend, emb_model=args.emb_model)

    doc_embs: Dict[str, np.ndarray] = {str(d.path): d.embeddings for d in docs if d.embeddings is not None}
    print("[INFO] Computing pairwise similarities...")
    pairs = pairwise_doc_scores(doc_embs)

    # Write CSV report
    out_csv = Path(args.report).expanduser().resolve()
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    flagged = []
    with out_csv.open('w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(["file_a", "file_b", "doc_sim", "max_chunk_sim", "flagged"])
        for (A, B, doc_sim, max_chunk_sim) in sorted(pairs, key=lambda x: max(x[2], x[3]), reverse=True):
            is_flag = (doc_sim >= args.threshold) or (max_chunk_sim >= args.threshold)
            w.writerow([A, B, f"{doc_sim:.4f}", f"{max_chunk_sim:.4f}", "YES" if is_flag else "NO"])
            if is_flag:
                flagged.append((A, B, doc_sim, max_chunk_sim))

    print(f"[INFO] Wrote report: {out_csv}")
    print(f"[INFO] Flagged {len(flagged)} candidate near-duplicate pairs with threshold >= {args.threshold}")

    # Optional explanations
    if args.explain:
        if args.backend != 'openai':
            print("[WARN] --explain uses OpenAI LLM. Ensure OPENAI_API_KEY is set.", file=sys.stderr)
        out_jsonl_path = None
        if args.jsonl:
            out_jsonl_path = Path(args.jsonl).expanduser().resolve()
            out_jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            jf = out_jsonl_path.open('w', encoding='utf-8')
        else:
            jf = None

        n_done = 0
        for (A, B, _, _) in flagged[:args.max_pairs]:
            text_a = next(d.text for d in docs if str(d.path) == A)
            text_b = next(d.text for d in docs if str(d.path) == B)
            try:
                exp = explain_differences_openai(A, B, text_a, text_b, model=args.explain_model)
            except Exception as e:
                exp = {"file_a": A, "file_b": B, "error": str(e)}
            line = json.dumps(exp, ensure_ascii=False)
            if jf:
                jf.write(line + "\n")
            else:
                print(line)
            n_done += 1
        if jf:
            jf.close()
            print(f"[INFO] Wrote {n_done} explanations to: {out_jsonl_path}")


if __name__ == "__main__":
    main()
