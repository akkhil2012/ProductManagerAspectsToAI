# (recommended) new venv
python3 -m venv .venv && source .venv/bin/activate

# install dependencies
pip install -r requirements.txt

streamlit run app.py

uvicorn api:app --reload

