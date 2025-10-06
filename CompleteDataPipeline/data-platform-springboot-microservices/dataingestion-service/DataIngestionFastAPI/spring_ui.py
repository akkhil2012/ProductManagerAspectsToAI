# dataingestion-service/DataIngestionFastAPI/spring_ui.py
import time
import requests
import streamlit as st
from classifier import read_text_from_bytes

st.set_page_config(page_title="Spring DataIngestion Controller UI", page_icon="🌱", layout="wide")
st.title("🌱 Spring DataIngestion Controller UI")

with st.sidebar:
    st.subheader("Settings")
    spring_base_url = st.text_input(
        "Spring Base URL",
        value="http://localhost:8081",
        help="Where your Spring Boot app is running",
    )
    base = f"{spring_base_url.rstrip('/')}/api/v1/dataingestion"
    st.caption(f"Target: {base}")

tabs = st.tabs([
    "Create / Read / Update / Delete",
    "Process / Validate / Count",
    "Classify (via Spring)"
])

# Tab 1: CRUD
with tabs[0]:
    st.subheader("Create Record (POST /api/v1/dataingestion)")
    c1, c2 = st.columns(2)
    with c1:
        c_record_id = st.text_input("recordId", key="c_record_id")
        c_status = st.text_input("status", value="NEW", key="c_status")
    with c2:
        c_payload = st.text_area("dataPayload", height=100, key="c_payload")
    if st.button("Create", key="btn_create"):
        try:
            resp = requests.post(
                base,
                json={"recordId": c_record_id, "status": c_status, "dataPayload": c_payload},
                timeout=30
            )
            st.json(resp.json())
        except Exception as e:
            st.error(f"Create failed: {e}")

    st.divider()
    st.subheader("Get All (GET /api/v1/dataingestion)")
    g1, g2, g3 = st.columns(3)
    with g1:
        q_status = st.text_input("status (optional)", key="q_status")
    with g2:
        q_start = st.text_input("startDate yyyy-MM-dd'T'HH:mm:ss (optional)", key="q_start")
    with g3:
        q_end = st.text_input("endDate yyyy-MM-dd'T'HH:mm:ss (optional)", key="q_end")
    if st.button("Fetch All", key="btn_get_all"):
        try:
            params = {}
            if q_status.strip():
                params["status"] = q_status.strip()
            if q_start.strip() and q_end.strip():
                params["startDate"] = q_start.strip()
                params["endDate"] = q_end.strip()
            resp = requests.get(base, params=params, timeout=30)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Fetch all failed: {e}")

    st.divider()
    st.subheader("Get By ID (GET /{id})")
    by_id = st.text_input("id", key="get_id")
    if st.button("Fetch By ID", key="btn_get_id"):
        try:
            resp = requests.get(f"{base}/{by_id}", timeout=30)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Fetch by id failed: {e}")

    st.divider()
    st.subheader("Get By recordId (GET /record/{recordId})")
    by_rid = st.text_input("recordId", key="get_rid")
    if st.button("Fetch By recordId", key="btn_get_rid"):
        try:
            resp = requests.get(f"{base}/record/{by_rid}", timeout=30)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Fetch by recordId failed: {e}")

    st.divider()
    st.subheader("Update (PATCH /{id})")
    u1, u2 = st.columns(2)
    with u1:
        u_id = st.text_input("id", key="u_id")
        u_status = st.text_input("status", key="u_status")
    with u2:
        u_payload = st.text_area("dataPayload", height=100, key="u_payload")
        u_error = st.text_input("errorMessage", key="u_error")
    if st.button("Update", key="btn_update"):
        try:
            body = {"status": u_status, "dataPayload": u_payload, "errorMessage": u_error}
            resp = requests.patch(f"{base}/{u_id}", json=body, timeout=30)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Update failed: {e}")

    st.divider()
    st.subheader("Delete (DELETE /{id})")
    d_id = st.text_input("id", key="d_id")
    if st.button("Delete", key="btn_delete"):
        try:
            resp = requests.delete(f"{base}/{d_id}", timeout=30)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Delete failed: {e}")

# Tab 2: Process / Validate / Count
with tabs[1]:
    st.subheader("Process via Python (POST /process)")
    p1, p2 = st.columns(2)
    with p1:
        p_rid = st.text_input("recordId", key="p_rid")
    with p2:
        p_payload = st.text_area("dataPayload", height=100, key="p_payload")
    if st.button("Process", key="btn_process"):
        try:
            resp = requests.post(f"{base}/process", json={"recordId": p_rid, "dataPayload": p_payload}, timeout=60)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Process failed: {e}")

    st.divider()
    st.subheader("Validate via Python (POST /validate)")
    v1, v2 = st.columns(2)
    with v1:
        v_rid = st.text_input("recordId", key="v_rid")
    with v2:
        v_payload = st.text_area("dataPayload", height=100, key="v_payload")
    if st.button("Validate", key="btn_validate"):
        try:
            resp = requests.post(f"{base}/validate", json={"recordId": v_rid, "dataPayload": v_payload}, timeout=30)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Validate failed: {e}")

    st.divider()
    st.subheader("Count By Status (GET /count?status=)")
    cnt_status = st.text_input("status", value="NEW", key="cnt_status")
    if st.button("Get Count", key="btn_count"):
        try:
            resp = requests.get(f"{base}/count", params={"status": cnt_status}, timeout=30)
            st.json(resp.json())
        except Exception as e:
            st.error(f"Count failed: {e}")

# Tab 3: Classify (via Spring)
with tabs[2]:
    st.subheader("Classify via Spring (POST /classify)")
    st.caption("Upload a file to extract text locally, or paste text directly. The UI calls the Spring endpoint.")
    f_text = ""
    had_file = False

    upload = st.file_uploader("Choose a file (.txt, .md, .csv, .json, .pdf, .docx)", type=["txt","md","csv","json","pdf","docx"], key="cls_upload")
    if upload:
        raw = upload.read()
        try:
            f_text = read_text_from_bytes(upload.name, raw)
            had_file = True
            st.success(f"Loaded `{upload.name}` ({len(f_text)} chars).")
            with st.expander("Preview (first 1,000 chars)"):
                st.code(f_text[:1000])
        except Exception as e:
            st.error(f"Failed to read file: {e}")

    pasted = st.text_area("Or paste text here", height=160, key="cls_pasted")
    if pasted.strip():
        f_text = pasted
        had_file = had_file or False

    if st.button("Classify via Spring", key="btn_classify_spring"):
        if not f_text.strip():
            st.warning("Provide a file or pasted text.")
        else:
            with st.spinner("Classifying..."):
                start = time.time()
                try:
                    resp = requests.post(f"{base}/classify", json={"text": f_text, "had_file": had_file}, timeout=60)
                    result = resp.json()
                    elapsed = time.time() - start
                    st.subheader("Results")
                    st.json(result)
                    st.caption(f"Completed in {elapsed:.2f}s")
                except Exception as e:
                    st.error(f"Spring classify call failed: {e}")