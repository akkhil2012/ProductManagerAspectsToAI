# dedupe_llm.py
"""
LLM-powered semantic deduplication for tabular data.

- Reads CSV/JSONL with a text column.
- Uses OpenAI embeddings to find near-duplicates (cosine similarity).
- Clusters items above a similarity threshold and keeps one representative.
- Writes filtered (deduplicated) rows and an optional cluster report.

Usage:
  python dedupe_llm.py --in data.csv --out dedup.csv --text-col text --threshold 0.92

Dependencies:
  pip install openai pandas numpy scikit-learn tqdm
Requires:
  export OPENAI_API_KEY=sk-...
"""

import os
import sys
import argparse
import pandas as pd
import numpy as np
from tqdm import tqdm
from typing import List, Dict, Tuple, Any, Optional

# For nearest neighbor search
from sklearn.neighbors import NearestNeighbors

# OpenAI SDK v1.x
try:
    from openai import OpenAI
except Exception as e:
    print("Please install openai>=1.0.0: pip install openai", file=sys.stderr)
    raise

# ---------------------------
# Utils
# ---------------------------

def normalize_text(s: str) -> str:
    """Lightweight normalization for exact-duplicate collapsing before embeddings."""
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    return " ".join(s.strip().lower().split())

class DSU:
    """Disjoint Set Union (Union-Find) for clustering indices."""
    def __init__(self, n: int):
        self.parent = list(range(n))
        self.rank = [0]*n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[rb] < self.rank[ra]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1

def batched(lst: List[Any], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ---------------------------
# Embedding
# ---------------------------

def embed_texts(texts: List[str], model: str = "text-embedding-3-small", batch_size: int = 100) -> np.ndarray:
    """Return (N, D) embedding matrix for the given texts using OpenAI embeddings."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set")

    client = OpenAI()
    vecs = []
    for batch in tqdm(list(batched(texts, batch_size)), desc="Embedding", unit="batch"):
        resp = client.embeddings.create(model=model, input=batch)
        # Each resp.data[i].embedding is a list[float]
        vecs.extend([d.embedding for d in resp.data])
    return np.array(vecs, dtype=np.float32)

# ---------------------------
# Dedup logic
# ---------------------------

def build_clusters(embeddings: np.ndarray, threshold: float) -> Dict[int, List[int]]:
    """
    Cluster items whose cosine similarity >= threshold.
    We transform similarity threshold to cosine distance radius: dist <= 1 - threshold.
    """
    n = embeddings.shape[0]
    if n == 0:
        return {}

    # Normalize vectors for cosine similarity (optional but helps numerics)
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0.0] = 1.0
    E = embeddings / norms

    radius = 1.0 - float(threshold)
    if radius <= 0 or radius >= 2:
        raise ValueError("Threshold must be between -1 and 1. Typical range: 0.85–0.97")

    # Use NearestNeighbors with cosine metric to find neighbors within radius
    nn = NearestNeighbors(metric="cosine", radius=radius, n_jobs=-1)
    nn.fit(E)
    # For each point, find neighbors within radius
    # returns array of arrays of neighbor indices
    neigh_ind = nn.radius_neighbors(E, radius=radius, return_distance=False)

    # Union-find over neighbor graph
    dsu = DSU(n)
    for i in range(n):
        # connect i with all j in its epsilon-neighborhood
        for j in neigh_ind[i]:
            if i != j:
                dsu.union(i, j)

    # Group by root
    clusters: Dict[int, List[int]] = {}
    for i in range(n):
        r = dsu.find(i)
        clusters.setdefault(r, []).append(i)

    return clusters

def choose_representative(indices: List[int], texts: List[str], strategy: str = "first") -> int:
    """
    Pick one row to keep from a cluster.
    Strategies:
      - "first": keep first occurrence
      - "longest": keep longest text
    """
    if strategy == "longest":
        lengths = [(i, len(texts[i])) for i in indices]
        lengths.sort(key=lambda x: x[1], reverse=True)
        return lengths[0][0]
    return min(indices)  # first by original order

# ---------------------------
# I/O and main
# ---------------------------

def load_df(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".csv", ".tsv"):
        sep = "," if ext == ".csv" else "\t"
        return pd.read_csv(path, sep=sep)
    elif ext in (".jsonl", ".ndjson"):
        return pd.read_json(path, lines=True)
    else:
        # default try CSV
        return pd.read_csv(path)

def save_df(df: pd.DataFrame, path: str):
    ext = os.path.splitext(path)[1].lower()
    if ext in (".csv", ".tsv", ""):
        sep = "," if ext != ".tsv" else "\t"
        df.to_csv(path if ext else (path + ".csv"), index=False, sep=sep)
    elif ext in (".jsonl", ".ndjson"):
        df.to_json(path, orient="records", lines=True, force_ascii=False)
    else:
        # fallback to CSV
        df.to_csv(path + ".csv", index=False)

def main():
    ap = argparse.ArgumentParser(description="Semantic deduplication with LLM embeddings")
    ap.add_argument("--in", dest="in_path", required=True, help="Input CSV/TSV/JSONL file")
    ap.add_argument("--out", dest="out_path", required=True, help="Output file for filtered (deduped) data")
    ap.add_argument("--report", dest="report_path", default=None, help="Optional CSV report mapping kept_row -> cluster members")
    ap.add_argument("--text-col", dest="text_col", required=True, help="Name of the text column to dedupe on")
    ap.add_argument("--threshold", type=float, default=0.92, help="Cosine similarity threshold (0.85–0.97 typical)")
    ap.add_argument("--model", default="text-embedding-3-small", help="Embedding model (e.g., text-embedding-3-small|large)")
    ap.add_argument("--batch-size", type=int, default=100, help="Embedding batch size")
    ap.add_argument("--rep-strategy", choices=["first", "longest"], default="first", help="Representative selection per cluster")
    args = ap.parse_args()

    df = load_df(args.in_path)
    if args.text_col not in df.columns:
        raise ValueError(f"Column `{args.text_col}` not in input. Found: {list(df.columns)}")

    # Drop rows where text is NA/empty
    df = df.copy()
    df["_text_norm"] = df[args.text_col].map(normalize_text)
    df = df[~df["_text_norm"].isna() & (df["_text_norm"].str.len() > 0)].reset_index(drop=True)

    if len(df) == 0:
        print("No non-empty rows found. Nothing to do.")
        save_df(df.drop(columns=["_text_norm"], errors="ignore"), args.out_path)
        return

    # Pre-collapse exact duplicates (string-equal after normalization)
    print("Pre-collapsing exact duplicates...")
    first_idx_by_text: Dict[str, int] = {}
    group_indices: Dict[int, List[int]] = {}
    for i, val in enumerate(df["_text_norm"].tolist()):
        if val not in first_idx_by_text:
            first_idx_by_text[val] = i
        group_indices.setdefault(first_idx_by_text[val], []).append(i)

    # Representatives after exact-collapse
    exact_reps = sorted(group_indices.keys())
    rep_mask = np.zeros(len(df), dtype=bool)
    rep_mask[exact_reps] = True
    df_exact = df.loc[rep_mask].reset_index(drop=False).rename(columns={"index": "_orig_idx"})

    print(f"Exact duplicates collapsed: kept {len(df_exact)} from {len(df)}")

    # Embeddings for the representative set
    texts_rep = df_exact[args.text_col].astype(str).tolist()
    embeddings = embed_texts(texts_rep, model=args.model, batch_size=args.batch_size)

    # Cluster near-duplicates on the representative set
    clusters = build_clusters(embeddings, threshold=args.threshold)

    # Map cluster root -> indices within df_exact
    # Choose representative per cluster (then map back to original df rows)
    kept_rows_exact_idx = []
    cluster_report: List[Tuple[int, List[int]]] = []

    for root, members in clusters.items():
        # Convert local indices (within df_exact) to their original df indices
        local_to_global = [int(df_exact.iloc[m]["_orig_idx"]) for m in members]
        # From these, also gather any exact-duplicate fold-ins
        expanded = []
        for g in local_to_global:
            expanded.extend(group_indices.get(g, [g]))
        expanded = sorted(set(expanded))

        # Choose final representative
        rep_global = choose_representative(expanded, df[args.text_col].astype(str).tolist(), strategy=args.rep_strategy)
        kept_rows_exact_idx.append(rep_global)
        cluster_report.append((rep_global, expanded))

    # Build filtered dataframe (unique representatives)
    kept_rows_exact_idx = sorted(set(kept_rows_exact_idx))
    df_filtered = df.loc[kept_rows_exact_idx].drop(columns=["_text_norm"])
    df_filtered = df_filtered.reset_index(drop=True)

    # Save outputs
    save_df(df_filtered, args.out_path)
    print(f"Wrote filtered (deduplicated) data: {args.out_path}  (rows: {len(df_filtered)})")

    if args.report_path:
        # Expand report to a tidy format
        rows = []
        for rep, members in cluster_report:
            for m in members:
                rows.append({
                    "kept_row_idx": rep,
                    "member_row_idx": m,
                    "kept_text": df.loc[rep, args.text_col],
                    "member_text": df.loc[m, args.text_col]
                })
        rep_df = pd.DataFrame(rows)
        save_df(rep_df, args.report_path)
        print(f"Wrote cluster report: {args.report_path}  (rows: {len(rep_df)})")

if __name__ == "__main__":
    main()
