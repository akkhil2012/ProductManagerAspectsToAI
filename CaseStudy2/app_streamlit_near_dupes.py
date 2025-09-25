import io
import os
import json
import tempfile
from pathlib import Path
from typing import List, Tuple, Dict, Optional

import numpy as np
import streamlit as st
from sklearn.metrics.pairwise import cosine_similarity

# Optional imports handled lazily
# - pypdf
# - python-docx
# - sentence-transformers
# - openai / OpenAI

SUPPORTED_EXTS = {'.pdf', '.docx', '.txt'}


def _read_pdf_bytes(data: bytes) -> str:
    try:
        from pypdf import PdfReader
    except ImportError as e:
        st.error("Missing dependency 'pypdf'. Install with: pip install pypdf")
        raise

    reader = PdfReader(io.BytesIO(data))
    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            parts.append("")
    return "\n".join(parts)


def _read_docx_bytes(data: bytes) -> str:
    try:
        import docx
    except ImportError as e:
        st.error("Missing dependency 'python-docx'. Install with: pip install python-docx")
        raise
    d = docx.Document(io.BytesIO(data))
    return "\n".join([p.text for p in d.paragraphs])


def read_text(file_name: str, data: bytes) -> str:
    ext = Path(file_name).suffix.lower()
    if ext == '.pdf':
        return _read_pdf_bytes(data)
    elif ext == '.docx':
        return _read_docx_bytes(data)
    elif ext == '.txt':
        return data.decode('utf-8', errors='ignore')
    else:
        raise ValueError(f"Unsupported extension: {ext}")


def normalize_text(s: str) -> str:
    import re
    s = s.replace('\r', '\n')
    s = re.sub(r'[ \t]+', ' ', s)
    s = re.sub(r'\n{3,}', '\n\n', s)
    return s.strip()


def split_into_paragraphs(text: str) -> List[str]:
    import re
    return [p.strip() for p in re.split(r'\n\s*\n', text) if p.strip()]


def chunk_paragraphs(paragraphs: List[str], chunk_size: int = 1000, overlap: int = 200) -> List[str]:
    chunks = []
    cur = []
    cur_len = 0

    for p in paragraphs:
        if cur_len + len(p) + 1 <= chunk_size:
            cur.append(p)
            cur_len += len(p) + 1
        else:
            if cur:
                joined = "\n\n".join(cur)
                chunks.append(joined)
                if overlap > 0:
                    tail = joined[-overlap:]
                    cur = [tail, p]
                    cur_len = len(tail) + len(p) + 1
                else:
                    cur = [p]
                    cur_len = len(p) + 1
            else:
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

    return [c.strip() for c in chunks if c.strip()]


class Embedder:
    def embed(self, texts: List[str]) -> np.ndarray:
        raise NotImplementedError


class SBERTEmbedder(Embedder):
    def __init__(self, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            st.error("Missing dependency 'sentence-transformers'. Install with: pip install sentence-transformers")
            raise
        self.model = SentenceTransformer(model_name)

    def embed(self, texts: List[str]) -> np.ndarray:
        return np.asarray(self.model.encode(texts, normalize_embeddings=True, convert_to_numpy=True))


class OpenAIEmbedder(Embedder):
    def __init__(self, model_name: str = "text-embedding-3-large", api_key: Optional[str] = None):
        try:
            from openai import OpenAI
        except ImportError:
            st.error("Missing dependency 'openai'. Install with: pip install openai")
            raise
        if not api_key:
            raise ValueError("OpenAI API key not provided.")
        os.environ['OPENAI_API_KEY'] = api_key
        self.model_name = model_name
        self.client = OpenAI()

    def embed(self, texts: List[str]) -> np.ndarray:
        vectors = []
        batch = 64
        for i in range(0, len(texts), batch):
            chunk = texts[i:i + batch]
            resp = self.client.embeddings.create(model=self.model_name, input=chunk)
            vecs = [np.array(d.embedding, dtype=np.float32) for d in resp.data]
            vecs = [v / (np.linalg.norm(v) + 1e-8) for v in vecs]
            vectors.extend(vecs)
        return np.vstack(vectors)


def mean_pool(embs: np.ndarray) -> np.ndarray:
    v = embs.mean(axis=0)
    return v / (np.linalg.norm(v) + 1e-8)


def compute_pairs(doc_embs: Dict[str, np.ndarray]) -> List[Tuple[str, str, float, float]]:
    docs = sorted(doc_embs.keys())
    res = []
    for i in range(len(docs)):
        for j in range(i + 1, len(docs)):
            A, B = docs[i], docs[j]
            A_embs, B_embs = doc_embs[A], doc_embs[B]
            doc_sim = float(cosine_similarity(mean_pool(A_embs).reshape(1, -1),
                                              mean_pool(B_embs).reshape(1, -1))[0][0])
            sims = cosine_similarity(A_embs, B_embs)
            max_chunk_sim = float(np.max(sims))
            res.append((A, B, doc_sim, max_chunk_sim))
    res.sort(key=lambda x: max(x[2], x[3]), reverse=True)
    return res


def explain_differences_openai(file_a: str, file_b: str, text_a: str, text_b: str,
                               model: str = "gpt-4o-mini", api_key: Optional[str] = None, max_chars: int = 2500) -> Dict:
    try:
        from openai import OpenAI
    except ImportError:
        st.error("Missing dependency 'openai'. Install with: pip install openai")
        raise
    if not api_key:
        raise ValueError("OpenAI API key not provided.")
    os.environ['OPENAI_API_KEY'] = api_key
    client = OpenAI()

    prompt = (
        "You are a contracts/invoices reviewer. Two documents are semantically near-duplicates.\n"
        "Identify precise differences in a compact, bullet-style JSON. Focus on:\n"
        "- Parties, dates, amounts/currency, line items, signatures, jurisdictions, terms changed.\n\n"
        "Return JSON with fields:\n"
        '- "summary": one-sentence overview of key diffs\n'
        '- "changed_entities": list of {"field":"...", "old":"...", "new":"..."}\n'
        '- "risk_flags": list of short strings (e.g., "amount changed", "date altered")\n\n'
        "Doc A (truncated):\n"
        f"{text_a[:max_chars]}\n\n"
        "Doc B (truncated):\n"
        f"{text_b[:max_chars]}"
    )

    messages = [
        {"role": "system", "content": "You are a precise, compliance-focused document reviewer."},
        {"role": "user", "content": prompt},
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
    except Exception:
        data = {"summary": resp.choices[0].message.content}
    return {"file_a": file_a, "file_b": file_b, "model": model, "explanation": data}


st.set_page_config(page_title="Semantic Near-Duplicate Detector", layout="wide")
st.title("📑 Semantic Near‑Duplicate Detector (Contracts / Invoices)")

with st.sidebar:
    st.header("Settings")
    backend = st.selectbox("Embedding backend", ["sbert (local)", "openai (API)"], index=0)
    threshold = st.slider("Similarity threshold", 0.5, 0.99, 0.9, 0.01)
    chunk_size = st.number_input("Chunk size (chars)", min_value=400, max_value=4000, value=1000, step=100)
    overlap = st.number_input("Overlap (chars)", min_value=0, max_value=1000, value=200, step=50)
    min_chars = st.number_input("Min chars to include doc", min_value=0, max_value=5000, value=200, step=50)
    do_explain = st.checkbox("LLM diff explanations (costs API calls)")
    max_pairs = st.number_input("Max pairs to explain", min_value=1, max_value=500, value=25, step=1)
    if backend.startswith("openai") or do_explain:
        openai_key = st.text_input("OpenAI API Key", type="password", placeholder="sk-...")
        emb_model_openai = st.text_input("OpenAI Embedding model", value="text-embedding-3-large")
        chat_model_openai = st.text_input("OpenAI Chat model", value="gpt-4o-mini")
    else:
        openai_key = None
        emb_model_openai = "text-embedding-3-large"
        chat_model_openai = "gpt-4o-mini"
    sbert_model = st.text_input("SBERT model", value="sentence-transformers/all-MiniLM-L6-v2")

st.markdown("Upload your **.pdf**, **.docx**, or **.txt** files:")
uploads = st.file_uploader("Choose multiple files", type=["pdf", "docx", "txt"], accept_multiple_files=True)

if uploads:
    records = []
    for f in uploads:
        try:
            raw = read_text(f.name, f.getvalue())
            norm = normalize_text(raw)
            if len(norm) < min_chars:
                continue
            paras = split_into_paragraphs(norm)
            chunks = chunk_paragraphs(paras, chunk_size=chunk_size, overlap=overlap)
            records.append({"name": f.name, "text": norm, "chunks": chunks})
        except Exception as e:
            st.warning(f"Skipping {f.name}: {e}")

    st.info(f"Loaded {len(records)} documents that passed min_chars filter.")
    if len(records) >= 2:
        # Build embeddings
        if backend.startswith("sbert"):
            embedder = SBERTEmbedder(sbert_model)
        else:
            if not openai_key:
                st.error("OpenAI API Key is required for 'openai' backend.")
                st.stop()
            embedder = OpenAIEmbedder(emb_model_openai, api_key=openai_key)

        all_chunks = []
        offsets = []
        idx = 0
        for r in records:
            n = len(r["chunks"])
            all_chunks.extend(r["chunks"])
            offsets.append((idx, idx + n))
            idx += n

        embs = embedder.embed(all_chunks)
        for r, (s, e) in zip(records, offsets):
            r["embs"] = embs[s:e]

        doc_embs = {r["name"]: r["embs"] for r in records}
        pairs = compute_pairs(doc_embs)

        # Build result table
        import pandas as pd
        rows = []
        flagged = []
        for (A, B, doc_sim, max_chunk_sim) in pairs:
            flag = (doc_sim >= threshold) or (max_chunk_sim >= threshold)
            rows.append({"file_a": A, "file_b": B,
                         "doc_sim": round(doc_sim, 4),
                         "max_chunk_sim": round(max_chunk_sim, 4),
                         "flagged": "YES" if flag else "NO"})
            if flag:
                flagged.append((A, B))

        df = pd.DataFrame(rows)
        st.subheader("Results")
        st.dataframe(df, use_container_width=True)

        # Downloads
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        st.download_button("⬇️ Download CSV report", data=csv_bytes, file_name="duplicates_report.csv", mime="text/csv")

        # Explanations
        if do_explain and flagged:
            if not openai_key:
                st.error("OpenAI API Key required for explanations.")
            else:
                st.subheader("LLM Explanations")
                jsonl_lines = []
                count = 0
                for (A, B) in flagged:
                    if count >= max_pairs:
                        break
                    text_a = next(r["text"] for r in records if r["name"] == A)
                    text_b = next(r["text"] for r in records if r["name"] == B)
                    try:
                        exp = explain_differences_openai(A, B, text_a, text_b,
                                                         model=chat_model_openai, api_key=openai_key)
                        jsonl_lines.append(json.dumps(exp, ensure_ascii=False))
                        with st.expander(f"Explanation: {A} ↔ {B}"):
                            st.json(exp)
                        count += 1
                    except Exception as e:
                        st.warning(f"Explanation failed for {A} ↔ {B}: {e}")

                if jsonl_lines:
                    jsonl_bytes = ("\n".join(jsonl_lines)).encode('utf-8')
                    st.download_button("⬇️ Download Explanations (JSONL)", data=jsonl_bytes,
                                       file_name="near_duplicate_explanations.jsonl", mime="application/json")
    else:
        st.warning("Please upload at least two documents.")