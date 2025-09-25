
# Streamlit Dashboard for GenAI Ingestion

This dashboard lets you:
- Start ingestion+classification runs via the FastAPI service
- Upload demo input files into a local folder
- Monitor job status, view log tails, and inspect the manifest

## Install
```bash
pip install -r /mnt/data/requirements_streamlit.txt
```

## Run
```bash
streamlit run /mnt/data/streamlit_dashboard.py
```

Make sure the FastAPI service is running (default at http://localhost:8000):
```bash
uvicorn fastapi_app:app --host 0.0.0.0 --port 8000 --reload --app-dir /mnt/data
```
