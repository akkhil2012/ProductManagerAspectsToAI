import io
import json
import re
import hashlib
from datetime import datetime
from collections import Counter
from typing import List, Dict, Tuple, Optional

import pandas as pd
import numpy as np
import streamlit as st
import yaml

# =============================
# Helpers
# =============================

def _hash_series(s: pd.Series) -> str:
    """Stable hash for a series (for lineage/consistency checks)."""
    m = hashlib.sha256()
    # Convert to bytes in a stable way
    for v in s.fillna("__NaN__").astype(str).values:
        m.update(v.encode("utf-8"))
    return m.hexdigest()

def _infer_types(df: pd.DataFrame) -> Dict[str, str]:
    """Crude type inference for display."""
    types = {}
    for c in df.columns:
        col = df[c].dropna().astype(str)
        sample = col.head(50)
        if all(re.fullmatch(r"-?\d+", x) for x in sample):
            types[c] = "integer"
        elif all(re.fullmatch(r"-?\d+(\.\d+)?", x) for x in sample):
            types[c] = "float"
        elif all(re.fullmatch(r"\d{4}-\d{2}-\d{2}", x) or re.fullmatch(r"\d{2}/\d{2}/\d{4}", x) for x in sample):
            types[c] = "date-like"
        else:
            types[c] = "string"
    return types

def _tokenize(name: str) -> set:
    return set(re.split(r"[_\W]+", name.lower())) - {""}

def _jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    return len(a & b) / max(1, len(a | b))

def _value_kl(raw: pd.Series, proc: pd.Series, bins: int = 50) -> float:
    """Approx KL divergence for numeric-like columns (symmetrized)."""
    try:
        r = pd.to_numeric(raw, errors="coerce").dropna()
        p = pd.to_numeric(proc, errors="coerce").dropna()
        if len(r) < 5 or len(p) < 5:
            return np.nan
        hr, edges = np.histogram(r, bins=bins, density=True)
        hp, _ = np.histogram(p, bins=edges, density=True)
        # add epsilon to avoid zeros
        eps = 1e-12
        hr = hr + eps
        hp = hp + eps
        kl_rp = np.sum(hr * np.log(hr / hp))
        kl_pr = np.sum(hp * np.log(hp / hr))
        return float(0.5 * (kl_rp + kl_pr))
    except Exception:
        return np.nan

def detect_pii_series(s: pd.Series) -> List[str]:
    """Detect simple PII patterns present in series."""
    detectors = {
        "email": r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        "phone": r"(\+?\d[\d\-\s]{7,}\d)",
        "credit_card": r"\b(?:\d[ -]*?){13,16}\b",
        "ssn_like": r"\b\d{3}-\d{2}-\d{4}\b",
    }
    present = []
    s_str = s.astype(str).fillna("")
    for label, pat in detectors.items():
        if s_str.str.contains(pat, regex=True).any():
            present.append(label)
    return present

def pii_report(df: pd.DataFrame) -> Dict[str, List[str]]:
    out = {}
    for c in df.columns:
        found = detect_pii_series(df[c])
        if found:
            out[c] = found
    return out

def compare_schemas(raw: pd.DataFrame, proc: pd.DataFrame) -> pd.DataFrame:
    raw_types = _infer_types(raw)
    proc_types = _infer_types(proc)
    cols = sorted(set(raw.columns) | set(proc.columns))
    rows = []
    for c in cols:
        rows.append({
            "column": c,
            "in_raw": c in raw.columns,
            "in_processed": c in proc.columns,
            "raw_type": raw_types.get(c, None),
            "processed_type": proc_types.get(c, None),
            "name_similarity": _jaccard(_tokenize(c if c in raw.columns else ""), _tokenize(c if c in proc.columns else "")),
        })
    return pd.DataFrame(rows)

def key_check(raw: pd.DataFrame, proc: pd.DataFrame, keys: List[str]) -> Dict[str, int]:
    def dup_count(df, keys):
        return int(df.duplicated(subset=keys).sum())
    return {
        "raw_dupe_keys": dup_count(raw, keys),
        "processed_dupe_keys": dup_count(proc, keys),
        "raw_rows": int(len(raw)),
        "processed_rows": int(len(proc)),
    }

def preservation_checks(raw: pd.DataFrame, proc: pd.DataFrame, keys: List[str]) -> Dict[str, Optional[str]]:
    out = {}
    if not keys:
        return out
    # join on keys to compare row-level preservation for a couple of columns
    common_cols = [c for c in raw.columns if c in proc.columns]
    if not common_cols:
        return out
    merged = raw[keys + common_cols].merge(proc[keys + common_cols], on=keys, how="left", suffixes=("_raw", "_proc"))
    # pick a few comparable columns
    comparable = [c for c in common_cols if c not in keys][:5]
    for c in comparable:
        hr = _hash_series(merged[f"{c}_raw"])
        hp = _hash_series(merged[f"{c}_proc"])
        out[f"column_hash_match::{c}"] = "match" if hr == hp else "DIFF"
    return out

def distribution_checks(raw: pd.DataFrame, proc: pd.DataFrame) -> pd.DataFrame:
    common = [c for c in raw.columns if c in proc.columns]
    rows = []
    for c in common:
        kl = _value_kl(raw[c], proc[c])
        rows.append({"column": c, "kl_divergence_sym": kl})
    return pd.DataFrame(rows)

def build_lineage_notes(raw_name: str, proc_name: str, keys: List[str], schema_df: pd.DataFrame, pii_raw: Dict[str, List[str]], pii_proc: Dict[str, List[str]]) -> str:
    points = []
    points.append(f"- **Raw dataset**: `{raw_name}`; **Processed dataset**: `{proc_name}`.")
    if keys:
        points.append(f"- **Primary/business keys**: {', '.join(keys)}.")
    # Schema summary
    added = schema_df[(schema_df["in_processed"]) & (~schema_df["in_raw"])]["column"].tolist()
    dropped = schema_df[(schema_df["in_raw"]) & (~schema_df["in_processed"])]["column"].tolist()
    type_changed = schema_df[(schema_df["raw_type"] != schema_df["processed_type"]) & schema_df["in_raw"] & schema_df["in_processed"]][["column", "raw_type", "processed_type"]]
    if added:
        points.append(f"- **Columns added in processed**: {added}.")
    if dropped:
        points.append(f"- **Columns dropped in processed**: {dropped}.")
    if not type_changed.empty:
        changes = "; ".join(f"{r.column}: {r.raw_type} → {r.processed_type}" for r in type_changed.itertuples(index=False))
        points.append(f"- **Type changes**: {changes}.")

    # PII movement notes
    if pii_raw:
        points.append(f"- **PII detected in raw**: { {k: v for k, v in pii_raw.items()} }")
    if pii_proc:
        points.append(f"- **PII detected in processed**: { {k: v for k, v in pii_proc.items()} }")
    if pii_raw and not pii_proc:
        points.append("- **PII removed during processing** ✅.")
    elif pii_proc and not pii_raw:
        points.append("- **PII introduced during processing** ❗ Review masking rules.")
    elif pii_raw and pii_proc:
        points.append("- **PII persists from raw to processed**. Ensure masking/tokenization/RBAC in curated layer.")

    md = "# Lineage Notes\n" + "\n".join(points)
    return md

def build_policy_yaml(dataset: str, pii_cols: Dict[str, List[str]], retention_hot_days: int, retention_cold_days: int, rbac_roles: List[str]) -> str:
    policy = {
        "dataset": dataset,
        "version": 1,
        "retention": {
            "hot_storage_days": retention_hot_days,
            "cold_storage_days": retention_cold_days
        },
        "governance": {
            "pii_columns": pii_cols,
            "masking": [{"column": c, "policy": "hash_or_tokenize"} for c in pii_cols.keys()],
            "rbac": [{"role": r, "access": "read"} for r in rbac_roles]
        }
    }
    return yaml.safe_dump(policy, sort_keys=False)

# =============================
# UI
# =============================

st.set_page_config(page_title="GenAI-ish Storage Validator", page_icon="🗄️", layout="wide")
st.title("🗄️ Data Storage & Persistence Validator (Raw vs Processed)")

with st.expander("What is this?"):
    st.markdown("""
This app demonstrates a **Data Storage & Persistence** validation step that compares **raw** vs **processed** datasets.
It performs:
- **Schema comparison** (columns added/dropped, type hints)
- **Key preservation** (duplicates, row count checks)
- **Distribution drift** (KL divergence for numeric columns)
- **PII checks** (simple regex-based)
- **Lineage note generation** (markdown summary)
- **Policy YAML** draft (retention, RBAC, masking)

> You can extend this with a real LLM to generate richer lineage summaries or masking policies.
""")

left, right = st.columns(2)

with left:
    st.subheader("Upload RAW dataset (CSV)")
    raw_file = st.file_uploader("raw.csv", type=["csv"], key="raw")
    if raw_file is not None:
        raw_df = pd.read_csv(raw_file)
        st.dataframe(raw_df.head(20))

with right:
    st.subheader("Upload PROCESSED dataset (CSV)")
    proc_file = st.file_uploader("processed.csv", type=["csv"], key="proc")
    if proc_file is not None:
        proc_df = pd.read_csv(proc_file)
        st.dataframe(proc_df.head(20))

st.divider()

if 'raw_df' in locals() and 'proc_df' in locals():
    st.subheader("Configuration")
    all_keys = sorted(list(set(raw_df.columns) & set(proc_df.columns)))
    keys = st.multiselect("Select primary/business key(s) shared by both tables", options=all_keys, default=[all_keys[0]] if all_keys else [])

    colA, colB, colC = st.columns([1,1,1])
    with colA:
        retention_hot = st.number_input("Hot storage retention (days)", min_value=1, max_value=3650, value=90)
    with colB:
        retention_cold = st.number_input("Cold storage retention (days)", min_value=0, max_value=3650, value=365)
    with colC:
        rbac_input = st.text_input("RBAC roles (comma-separated)", value="data_analyst, data_scientist, auditor")

    st.markdown("### 1) Schema Comparison")
    schema_df = compare_schemas(raw_df, proc_df)
    st.dataframe(schema_df)

    st.markdown("### 2) Key & Row Preservation")
    if keys:
        kc = key_check(raw_df, proc_df, keys)
        st.json(kc)
        st.caption("Tip: Duplicates or row count shrink/expansion may indicate deduplication or data loss. Validate business rules.")
    else:
        st.warning("Please select at least one key to run key checks.")

    st.markdown("### 3) Distribution Drift (Numeric-like Columns)")
    drift_df = distribution_checks(raw_df, proc_df)
    st.dataframe(drift_df)

    st.markdown("### 4) PII Detection")
    pii_raw = pii_report(raw_df)
    pii_proc = pii_report(proc_df)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**PII in RAW**")
        st.json(pii_raw or {"PII": "none"})
    with c2:
        st.markdown("**PII in PROCESSED**")
        st.json(pii_proc or {"PII": "none"})

    st.markdown("### 5) Column Preservation Hash Spot-Checks")
    pres = preservation_checks(raw_df, proc_df, keys) if keys else {}
    if pres:
        st.json(pres)
    else:
        st.info("Select keys and ensure both tables share some comparable columns.")

    st.markdown("### 6) Lineage Notes (Markdown)")
    lineage_md = build_lineage_notes(
        raw_name=getattr(raw_file, "name", "raw.csv"),
        proc_name=getattr(proc_file, "name", "processed.csv"),
        keys=keys,
        schema_df=schema_df,
        pii_raw=pii_raw,
        pii_proc=pii_proc
    )
    st.code(lineage_md, language="markdown")

    st.markdown("### 7) Draft Governance Policy (YAML)")
    rbac_roles = [r.strip() for r in rbac_input.split(",") if r.strip()]
    policy_yaml = build_policy_yaml(
        dataset=getattr(proc_file, "name", "processed.csv"),
        pii_cols=pii_proc,
        retention_hot_days=int(retention_hot),
        retention_cold_days=int(retention_cold),
        rbac_roles=rbac_roles
    )
    st.code(policy_yaml, language="yaml")

    # downloads
    st.download_button("Download lineage.md", data=lineage_md, file_name="lineage.md", mime="text/markdown")
    st.download_button("Download policy.yaml", data=policy_yaml, file_name="policy.yaml", mime="text/yaml")

else:
    st.info("Upload both RAW and PROCESSED CSVs to begin.")
