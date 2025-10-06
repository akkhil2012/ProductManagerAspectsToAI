python3 deduplicateApp.py --in data.csv --out data_output.csv --text-col description --threshold 0.9

In case we see issue with open AI version: try below
python3 -m pip uninstall -y openai && python3 -m pip install -U "openai>=1.40.0"
