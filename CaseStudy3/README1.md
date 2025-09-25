
# GenAI-ish Data Storage & Persistence Validator (Raw vs Processed)

A ready-to-run **Streamlit** app that demonstrates how to validate what you wrote into storage:
- Compare **raw vs processed** datasets
- Schema diffs, type hints, PII detection
- Key/row preservation checks
- Numeric distribution drift (KL divergence)
- **Lineage notes** generator (Markdown)
- **Governance policy** draft (YAML: retention, RBAC, masking)

## Quickstart

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Then open the local URL Streamlit shows. Upload two CSVs (or use the provided `sample_raw.csv` and `sample_processed.csv`).

## Files
- `streamlit_app.py` — the Streamlit UI & logic
- `sample_raw.csv`, `sample_processed.csv` — toy example data
- `requirements.txt` — minimal deps
- `README.md` — this file

## Extend with a real LLM (optional)
- Replace `build_lineage_notes(...)` with a call to your LLM to:
  - Summarize business rule impacts for each column transformation
  - Recommend retention & RBAC policies per data sensitivity
  - Generate docstrings for your data catalog (e.g., OpenMetadata, DataHub)

## PM Talking Points
- Storage savings via policy-driven tiering and TTL
- Compliance: automatic PII detection → masking → RBAC
- Trust: dual-write validation assures parity and reveals silent data loss
- Performance: steer consumers to curated tables; propose materialized views
