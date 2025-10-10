# Streamlit Topology UI

This Streamlit application visualizes the microservice topology for the data platform pipeline.

## Getting Started

1. Create and activate a virtual environment (optional but recommended).
2. Install the dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Run the Streamlit app from the `ui` directory:

   ```bash
   streamlit run app.py
   ```

The app renders an interactive graph where each node corresponds to a microservice and edges
represent the order in which the services are invoked in the pipeline.
