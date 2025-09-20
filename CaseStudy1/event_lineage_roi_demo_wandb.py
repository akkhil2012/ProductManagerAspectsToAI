"""
Event-Driven Data Lineage + GenAI Explanation + ROI Demo
--------------------------------------------------------
- Simulates lineage capture for a banking transaction flowing through Kafka -> Spark -> Ledger DB
- Uses GenAI (if OPENAI_API_KEY is set) to produce an auditor-friendly narrative of the lineage path
  with governance and data-quality notes
- Calculates a simple ROI for adopting the platform

Run:
  python event_lineage_roi_demo.py

Optional env:
  export OPENAI_API_KEY="sk-..."

Dependencies:
  pip install networkx openai
  (The script also runs without openai; it will use a fallback explanation.)
"""
import os
import json
import time
from dataclasses import dataclass, field
from typing import List, Dict, Any
try:
    import networkx as nx
except ImportError:
    raise SystemExit("Please install networkx: pip install networkx")
import wandb
from pathlib import Path

# ------------- Lineage Event Model (minimal, OpenLineage-inspired) -----------------
@dataclass
class Dataset:
    name: str
    system: str
    schema: Dict[str, str] = field(default_factory=dict)
    pii_cols: List[str] = field(default_factory=list)

@dataclass
class ProcessRun:
    run_id: str
    job_name: str
    ts: float
    status: str = "COMPLETED"
    facets: Dict[str, Any] = field(default_factory=dict)

# ------------- Simulate Event-Driven Lineage Capture -------------------------------
def build_sample_lineage_graph():
    G = nx.DiGraph()
    # Datasets
    kafka_topic = Dataset(
        name="topic.payments.txn_events",
        system="kafka",
        schema={"txnId": "string", "accountId": "string", "amount": "decimal", "currency": "string", "email":"string"},
        pii_cols=["email"]
    )
    bronze_tbl = Dataset(
        name="lake.bronze.payments_raw",
        system="object_store",
        schema=kafka_topic.schema,
        pii_cols=["email"]
    )
    enriched_tbl = Dataset(
        name="lake.silver.payments_enriched",
        system="object_store",
        schema={**kafka_topic.schema, "risk_score":"double"},
        pii_cols=["email"]
    )
    ledger_tbl = Dataset(
        name="db.core.settlement_ledger",
        system="postgres",
        schema={"txnId":"string","accountId":"string","amount":"decimal","currency":"string","risk_score":"double","email_masked":"string"},
        pii_cols=[]  # masked at write
    )

    # Add dataset nodes
    for ds in [kafka_topic, bronze_tbl, enriched_tbl, ledger_tbl]:
        G.add_node(ds.name, type="dataset", obj=ds)

    # Processes (runs)
    ingest_run = ProcessRun(run_id="run-001", job_name="ingest_raw_from_kafka", ts=time.time(), facets={
        "policy": {"masking":"none (raw)", "retention":"7d", "access":"restricted"},
        "dq": {"null_checks":"ok","schema_drift":"none"}
    })
    enrich_run = ProcessRun(run_id="run-002", job_name="spark_enrich_risk_scoring", ts=time.time(), facets={
        "policy": {"masking":"none (processing)", "retention":"30d", "access":"restricted"},
        "dq": {"null_checks":"ok","schema_drift":"none"}
    })
    write_run = ProcessRun(run_id="run-003", job_name="write_ledger_with_masking", ts=time.time(), facets={
        "policy": {"masking":"email -> tokenized","retention":"7y","access":"auditor_allowed"},
        "dq": {"null_checks":"ok","schema_drift":"none"}
    })

    # Add process nodes
    for r in [ingest_run, enrich_run, write_run]:
        G.add_node(r.run_id, type="process", obj=r)

    # Edges (lineage)
    # kafka_topic --(ingest_run)--> bronze_tbl
    G.add_edge(kafka_topic.name, ingest_run.run_id, relation="USED")
    G.add_edge(ingest_run.run_id, bronze_tbl.name, relation="WROTE")
    # bronze_tbl --(enrich_run)--> enriched_tbl
    G.add_edge(bronze_tbl.name, enrich_run.run_id, relation="USED")
    G.add_edge(enrich_run.run_id, enriched_tbl.name, relation="WROTE")
    # enriched_tbl --(write_run)--> ledger_tbl
    G.add_edge(enriched_tbl.name, write_run.run_id, relation="USED")
    G.add_edge(write_run.run_id, ledger_tbl.name, relation="WROTE")

    return G

# ------------- Path & Policy Extraction -------------------------------------------
def compute_upstream_path(G, target_dataset: str):
    # Find a path that ends at target_dataset by following reverse edges
    # We pick a simple path for demo purposes.
    # Reverse DFS from target to sources
    path = []
    def dfs(node):
        pred = list(G.predecessors(node))
        if not pred:
            return
        # pick first predecessor for demo determinism
        p = pred[0]
        path.append((p, node))
        dfs(p)
    dfs(target_dataset)
    path.reverse()
    return path  # list of (src -> dst) tuples along graph

def collect_policies_on_path(G, path_edges):
    policies = []
    for src, dst in path_edges:
        if G.nodes[dst].get("type") == "process":
            run: ProcessRun = G.nodes[dst]["obj"]
            policies.append({"run_id": run.run_id, "job_name": run.job_name, "policy": run.facets.get("policy", {}), "dq": run.facets.get("dq", {})})
    return policies

# ------------- GenAI Explanation (with offline fallback) --------------------------
def genai_explain_lineage(path_edges, policies):
    api_key = os.getenv("OPENAI_API_KEY")
    try:
        if api_key:
            from openai import OpenAI
            client = OpenAI(api_key=api_key)
            # Build a compact, structured prompt
            content = {
                "task": "Explain banking data lineage for auditors in clear, factual language.",
                "path_edges": path_edges,
                "policies": policies,
                "notes": [
                    "Call out where masking happens and why PII is protected."
                    #"Mention retention and access policies in simple terms.",
                    #"State that no schema drift was detected in this run."
                ]
            }
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role":"system","content":"You are a compliance-savvy data lineage explainer."},
                    {"role":"user","content": json.dumps(content)}
                ],
                temperature=0.2
            )
            return resp.choices[0].message.content.strip()
    except Exception as e:
        # Fall through to fallback if anything goes wrong
        pass

    # Offline fallback (template-based)
    lines = ["Auditor-Focused Lineage Narrative (Fallback)"]
    lines.append("The settlement_ledger record originates from raw payment events published to Kafka.")
    lines.append("Events were ingested into a bronze table without masking (raw zone, 7-day retention, restricted access).")
    lines.append("A Spark job enriched the data with a risk score (processing zone, 30-day retention, restricted access).")
    lines.append("Finally, a write job applied tokenization to email before loading into the settlement ledger (7-year retention; auditors allowed).")
    lines.append("All data-quality checks passed and no schema drift was detected across runs.")
    return "\n".join(lines)

# ------------- Simple ROI Calculator ---------------------------------------------
@dataclass
class ROIInputs:
    annual_audits: int = 12
    hours_per_audit_before: float = 40.0
    hours_per_audit_after: float = 12.0  # with lineage+evidence export
    hourly_fully_loaded_cost: float = 80.0  # USD
    incidents_per_year: int = 24
    mttr_hours_before: float = 6.0
    mttr_hours_after: float = 3.5
    violations_prevented_per_year: int = 10
    cost_per_violation: float = 5000.0  # fines/rework/brand risk proxy
    platform_cost_per_year: float = 150000.0

def compute_roi(i: ROIInputs):
    audit_hours_saved = i.annual_audits * (i.hours_per_audit_before - i.hours_per_audit_after)
    audit_savings = audit_hours_saved * i.hourly_fully_loaded_cost

    mttr_hours_saved = i.incidents_per_year * (i.mttr_hours_before - i.mttr_hours_after)
    mttr_savings = mttr_hours_saved * i.hourly_fully_loaded_cost

    violation_savings = i.violations_prevented_per_year * i.cost_per_violation

    total_benefit = audit_savings + mttr_savings + violation_savings
    net_benefit = total_benefit - i.platform_cost_per_year
    roi_pct = (net_benefit / i.platform_cost_per_year) * 100.0

    return {
        "audit_hours_saved": audit_hours_saved,
        "audit_savings_usd": round(audit_savings, 2),
        "mttr_hours_saved": mttr_hours_saved,
        "mttr_savings_usd": round(mttr_savings, 2),
        "violation_savings_usd": round(violation_savings, 2),
        "total_annual_benefit_usd": round(total_benefit, 2),
        "platform_cost_usd": i.platform_cost_per_year,
        "net_benefit_usd": round(net_benefit, 2),
        "roi_percent": round(roi_pct, 1)
    }

# ------------- Main Demo ----------------------------------------------------------

def main():
    print("=== Event-Driven Data Lineage + GenAI + ROI Demo (with W&B) ===")
    # Init W&B run
    run = wandb.init(project="event_lineage_eval", job_type="demo", config={"script": "event_lineage_roi_demo.py"})
    stage_meta = {}

    # ---------- Stage 1: Build Lineage Graph ----------
    t0 = time.time()
    G = build_sample_lineage_graph()
    t1 = time.time()
    # Metrics
    num_datasets = len([n for n,d in G.nodes(data=True) if d["type"]=="dataset"])
    num_processes = len([n for n,d in G.nodes(data=True) if d["type"]=="process"])
    num_edges = G.number_of_edges()
    wandb.log({
        "stage": "lineage_graph",
        "lineage/num_datasets": num_datasets,
        "lineage/num_processes": num_processes,
        "lineage/num_edges": num_edges,
        "timing/lineage_sec": t1 - t0
    })
    # Export a compact JSON of nodes/edges as an artifact for auditability
    lineage_json = {
        "nodes": [{"id": n, **({"type": d.get("type")} if isinstance(d, dict) else {})} for n,d in G.nodes(data=True)],
        "edges": [{"src": u, "dst": v, **({"relation": G.edges[u, v].get("relation")} if isinstance(G.edges[u, v], dict) else {})} for u, v in G.edges()]
    }
    out_dir = Path("./artifacts")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "lineage.json"
    with out_path.open("w") as f:
        json.dump(lineage_json, f, indent=2)
    art = wandb.Artifact("lineage_graph", type="lineage")
    art.add_file(str(out_path))
    run.log_artifact(art)

    # ---------- Stage 2: Compute path + policies ----------
    target = "db.core.settlement_ledger"
    path = compute_upstream_path(G, target)
    policies = collect_policies_on_path(G, path)
    path_len = len(path)
    masking_steps = sum(1 for p in policies if isinstance(p.get("policy"), dict) and str(p["policy"].get("masking","none")).lower() not in ["none","none (raw)","none (processing)"])
    wandb.log({
        "stage": "path_and_policies",
        "lineage/path_length": path_len,
        "lineage/masking_steps": masking_steps,
        "lineage/policies_count": len(policies)
    })

    print("Lineage Path (src -> dst):")
    for src, dst in path:
        print(f"  {src} -> {dst}")

    print("Policies along the path:")
    for p in policies:
        print(f"  {p['run_id']} ({p['job_name']}): {p['policy']} | DQ: {p['dq']}")

    # ---------- Stage 3: GenAI Explanation ----------
    t2 = time.time()
    explanation = genai_explain_lineage(path, policies)
    t3 = time.time()
    used_fallback = "Fallback" in explanation
    wandb.log({
        "stage": "genai_explanation",
        "genai/used_fallback": int(used_fallback),
        "genai/explanation_length": len(explanation),
        "timing/genai_sec": t3 - t2
    })
    try:
        wandb.log({"genai/explanation_html": wandb.Html(explanation)})
    except Exception:
        # Fallback if Html is not supported in your environment
        wandb.log({"genai/explanation_text": explanation[:2000]})

    print("GenAI Explanation:")
    print(explanation)

    # ---------- Stage 4: ROI Calculation ----------
    roi_inputs = ROIInputs()
    # Log inputs as config for traceability
    wandb.config.update({
        "roi/annual_audits": roi_inputs.annual_audits,
        "roi/hours_per_audit_before": roi_inputs.hours_per_audit_before,
        "roi/hours_per_audit_after": roi_inputs.hours_per_audit_after,
        "roi/hourly_fully_loaded_cost": roi_inputs.hourly_fully_loaded_cost,
        "roi/incidents_per_year": roi_inputs.incidents_per_year,
        "roi/mttr_hours_before": roi_inputs.mttr_hours_before,
        "roi/mttr_hours_after": roi_inputs.mttr_hours_after,
        "roi/violations_prevented_per_year": roi_inputs.violations_prevented_per_year,
        "roi/cost_per_violation": roi_inputs.cost_per_violation,
        "roi/platform_cost_per_year": roi_inputs.platform_cost_per_year
    }, allow_val_change=True)

    t4 = time.time()
    roi = compute_roi(roi_inputs)
    t5 = time.time()
    wandb.log({f"roi/{k}": v for k, v in roi.items()} | {"timing/roi_sec": t5 - t4})

    print("ROI Calculation:")
    for k, v in roi.items():
        print(f"  {k}: {v}")

    print("Tip: Set OPENAI_API_KEY to upgrade the narrative to a GenAI-generated explanation.")

    wandb.finish()

if __name__ == "__main__":
    main()
