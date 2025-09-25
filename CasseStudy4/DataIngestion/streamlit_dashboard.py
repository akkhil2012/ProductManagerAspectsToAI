import json
import time
from pathlib import Path

import requests
import streamlit as st

st.set_page_config(page_title="GenAI Ingestion Dashboard", page_icon="🧩", layout="wide")
st.title("🧩 GenAI Ingestion — Streamlit Dashboard")

# ----------------------------
# Sidebar: Settings
# ----------------------------
st.sidebar.header("Service Settings")
api_base = st.sidebar.text_input("FastAPI base URL", value="http://localhost:8000")
autorefresh = st.sidebar.checkbox("Auto-refresh jobs", value=True)
interval = st.sidebar.slider("Refresh interval (sec)", min_value=5, max_value=60, value=10)

if autorefresh:
    st.sidebar.caption("Auto-refresh will update job cards below.")
    st_autorefresh(interval=interval * 1000, key="auto-refresh")

# Ensure a job registry in session
if "jobs" not in st.session_state:
    st.session_state["jobs"] = {}  # job_id -> {"status":..., "started":...}

# ----------------------------
# Helpers
# ----------------------------
def api_post(path: str, payload: dict, timeout: int = 60):
    url = f"{api_base.rstrip('/')}{path}"
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        return False, {"error": str(e)}

def api_get(path: str, params: dict | None = None, timeout: int = 30):
    url = f"{api_base.rstrip('/')}{path}"
    try:
        r = requests.get(url, params=params or {}, timeout=timeout)
        r.raise_for_status()
        return True, r.json()
    except Exception as e:
        return False, {"error": str(e)}

def pretty_json(obj) -> str:
    try:
        return json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        return str(obj)

# ----------------------------
# New Run
# ----------------------------
st.header("Start a New Ingestion Run")
c1, c2, c3 = st.columns([1.2,1,1])
with c1:
    config_path = st.text_input("config.yml path", value="/mnt/data/config.yml")
    routes_path = st.text_input("routes.yml path", value="/mnt/data/routes.yml")
    out_dir = st.text_input("Output directory", value="/tmp/artifacts")
with c2:
    llm = st.selectbox("LLM backend", options=["none", "openai"], index=0)
    model = st.text_input("Model (if LLM enabled)", value="gpt-4o-mini")
with c3:
    max_bytes = st.number_input("Max sample bytes", min_value=4096, max_value=2_000_000, value=65536, step=4096)
    st.write("")
    start_btn = st.button("🚀 Start Run", type="primary", use_container_width=True)

if start_btn:
    ok, data = api_post("/run", {
        "config_path": config_path,
        "routes_path": routes_path,
        "out_dir": out_dir,
        "llm": llm,
        "model": model,
        "max_sample_bytes": int(max_bytes)
    })
    if ok:
        st.success(f"Started job: {data.get('job_id')}")
        st.session_state["jobs"][data["job_id"]] = {"status": "running", "started": time.time()}
    else:
        st.error(f"Failed to start job: {data.get('error')}")

st.divider()

# ----------------------------
# File Uploader (optional helper)
# ----------------------------
st.header("Upload Demo Files")
st.caption("Quickly add files to a local folder that your 'local' source points to (e.g., /mnt/data/demo_inputs).")
up_col1, up_col2 = st.columns([2,1])
with up_col1:
    dest_dir = st.text_input("Destination directory", value="/mnt/data/demo_inputs")
with up_col2:
    pass
uploads = st.file_uploader("Drop one or more files", type=None, accept_multiple_files=True)
if uploads and dest_dir:
    dest = Path(dest_dir).expanduser()
    dest.mkdir(parents=True, exist_ok=True)
    saved = []
    for f in uploads:
        outp = dest / f.name
        outp.write_bytes(f.getbuffer())
        saved.append(str(outp))
    st.success(f"Saved {len(saved)} file(s) to {dest}")
    st.code("\n".join(saved), language="bash")

st.divider()

# ----------------------------
# Jobs
# ----------------------------
st.header("Jobs")
if not st.session_state["jobs"]:
    st.info("No jobs yet. Start one above.")
else:
    for job_id in list(st.session_state["jobs"].keys()):
        with st.expander(f"Job {job_id}", expanded=True):
            b1, b2, b3, b4 = st.columns([1,1,1,1])
            with b1:
                if st.button("⟳ Refresh", key=f"refresh_{job_id}"):
                    pass  # autorefresh or manual; nothing needed here
            with b2:
                tail = st.number_input("Log tail (chars)", key=f"tailchars_{job_id}", min_value=1000, max_value=20000, value=5000, step=500)
            with b3:
                mlines = st.number_input("Manifest tail (lines)", key=f"mlines_{job_id}", min_value=10, max_value=1000, value=50, step=10)
            with b4:
                st.write("")

            # Status
            ok_s, status = api_get(f"/jobs/{job_id}")
            if ok_s:
                st.session_state["jobs"][job_id]["status"] = status.get("status", "unknown")
                st.session_state["jobs"][job_id]["manifest_path"] = status.get("manifest_path")
                cA, cB, cC = st.columns([1,1,2])
                with cA:
                    st.metric("Status", status.get("status"))
                    st.caption(f"Started: {status.get('started_at') or '-'}")
                with cB:
                    st.metric("Finished", status.get("finished_at") or "-")
                    if status.get("message"):
                        st.warning(status["message"])
                with cC:
                    st.code(pretty_json(status), language="json")
            else:
                st.error(f"Status error: {status.get('error')}")

            st.markdown("---")

            # Log
            ok_l, log = api_get(f"/jobs/{job_id}/log", {"tail": int(st.session_state[f'tailchars_{job_id}'])})
            if ok_l:
                st.subheader("Log (tail)")
                st.code(log.get("log_tail", ""), language="bash")
            else:
                st.warning(f"Log error: {log.get('error')}")

            # Manifest
            ok_m, mani = api_get(f"/jobs/{job_id}/manifest", {"max_lines": int(st.session_state[f'mlines_{job_id}'])})
            if ok_m:
                st.subheader("Manifest (last lines)")
                lines = mani.get("manifest_tail", [])
                # Render as a small table
                parsed = []
                for ln in lines:
                    try:
                        parsed.append(json.loads(ln))
                    except Exception:
                        parsed.append({"_raw": ln})
                st.dataframe(parsed, use_container_width=True, hide_index=True)
            else:
                st.info(f"Manifest not available yet: {mani.get('error')}")
