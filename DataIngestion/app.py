
import os
import time
import json
import requests
import streamlit as st
from classifier import combine_results, read_text_from_bytes, OPENAI_MODEL, USE_OPENAI

st.set_page_config(page_title="Desktop Ingestion + PII & Source Classification", page_icon="🔎", layout="wide")
st.title("🔎 Simple Data Ingestion + Classification")
st.caption("Upload a local file (txt/csv/json/pdf/docx) or paste text. Classifies origin (email/chat/local file) and detects PII. Uses OpenAI if available; falls back to heuristics.")

with st.sidebar:
    st.subheader("Settings")
    st.write("**OpenAI**")
    st.write(f"API key detected: {'✅' if USE_OPENAI else '❌'}")
    model_name = st.text_input("OPENAI_MODEL", value=OPENAI_MODEL, key="model_name")
    st.info("Set environment variable OPENAI_API_KEY to enable LLM-powered classification.", icon="ℹ️")
    st.divider()
    st.write("**Optional local API**")
    use_api = st.toggle("Use FastAPI backend (http://127.0.0.1:8000)", value=False)
    st.caption("Run:  `uvicorn api:app --reload`")

tab1, tab2 = st.tabs(["📁 Upload File", "📝 Paste Text"])

with tab1:
    upload = st.file_uploader("Choose a file (.txt, .md, .csv, .json, .pdf, .docx)", type=["txt","md","csv","json","pdf","docx"])
    if upload:
        raw = upload.read()
        text = read_text_from_bytes(upload.name, raw)
        st.success(f"Loaded `{upload.name}` ({len(text)} chars).")
        with st.expander("Preview (first 1,000 chars)"):
            st.code(text[:1000])
        if st.button("Classify Uploaded File"):
            with st.spinner("Classifying..."):
                start = time.time()
                if use_api:
                    try:
                        resp = requests.post("http://127.0.0.1:8000/classify", json={"text": text, "had_file": True}, timeout=30)
                        result = resp.json()
                    except Exception as e:
                        st.error(f"API call failed, falling back to local classify. Error: {e}")
                        result = combine_results(text, had_file=True)
                else:
                    result = combine_results(text, had_file=True)
                elapsed = time.time() - start
            st.subheader("Results")
            st.json(result)
            st.caption(f"Completed in {elapsed:.2f}s")

with tab2:
    pasted = st.text_area("Paste or type your text here", height=220, placeholder="Paste email/chat transcript or arbitrary text...")
    if st.button("Classify Text"):
        if not pasted.strip():
            st.warning("Please paste some text to classify.")
        else:
            with st.spinner("Classifying..."):
                start = time.time()
                if use_api:
                    try:
                        resp = requests.post("http://127.0.0.1:8000/classify", json={"text": pasted, "had_file": False}, timeout=30)
                        result = resp.json()
                    except Exception as e:
                        st.error(f"API call failed, falling back to local classify. Error: {e}")
                        result = combine_results(pasted, had_file=False)
                else:
                    result = combine_results(pasted, had_file=False)
                elapsed = time.time() - start
            st.subheader("Results")
            st.json(result)
            st.caption(f"Completed in {elapsed:.2f}s")

st.markdown("---")
st.write("**Privacy note:** Your text stays on your machine; if `OPENAI_API_KEY` is set, snippets are sent to OpenAI for classification. Remove secrets before uploading.")
