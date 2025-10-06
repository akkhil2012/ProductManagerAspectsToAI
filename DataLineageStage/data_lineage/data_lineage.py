import streamlit as st
import pandas as pd
import openai  # You would need to install openai: pip install openai

# --- Placeholder for LLM API Call ---
# In a real application, you would replace this with a call to an LLM API.
# This function simulates the response for demonstration purposes.
def get_llm_lineage_analysis(api_key, table_schema, selected_attributes, sample_data):
    """
    Constructs a prompt and calls an LLM to infer data lineage.

    Args:
        api_key (str): Your OpenAI API key.
        table_schema (str): A string describing the table's schema.
        selected_attributes (list): The list of attributes for lineage analysis.
        sample_data (str): A string representation of the first few rows of data.

    Returns:
        str: The lineage analysis from the LLM.
    """
    # For demonstration, we'll return a mocked response.
    # To use a real LLM, uncomment the code below and provide your API key.
    
    # --- Start of Real LLM Implementation (Commented Out) ---
    # openai.api_key = api_key
    # prompt = f"""
    # As a data governance expert, analyze the potential data lineage for the specified attributes.
    # Based on the table schema and sample data provided, infer the likely origin and transformations for each attribute.
    # Be concise and clear in your explanation.

    # Table Schema:
    # {table_schema}

    # Sample Data (first 5 rows):
    # {sample_data}

    # Please provide the data lineage for the following attributes: {', '.join(selected_attributes)}
    #
    # Example format for one attribute:
    # - Attribute 'total_sales': Likely derived by multiplying 'quantity_sold' by 'price_per_unit'. It represents the total revenue from a transaction.
    # """
    #
    # try:
    #     response = openai.ChatCompletion.create(
    #         model="gpt-4-turbo",  # Or another model of your choice
    #         messages=[
    #             {"role": "system", "content": "You are a helpful data lineage analysis assistant."},
    #             {"role": "user", "content": prompt}
    #         ],
    #         temperature=0.3
    #     )
    #     return response.choices[0].message['content']
    # except Exception as e:
    #     return f"An error occurred while calling the LLM: {e}"
    # --- End of Real LLM Implementation ---

    # --- Mocked response for demonstration ---
    mock_response = "### LLM-Generated Data Lineage Analysis (Mocked)\n\n"
    for attr in selected_attributes:
        mock_response += f"- **Attribute '{attr}':** This attribute is likely sourced from a transactional database. Based on its name and format, it appears to be a primary key or a foreign key linking to a customer/order table. It may have undergone a data type casting (e.g., from NUMBER to VARCHAR) during the ETL process.\n"
    return mock_response


# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("Interactive Data Lineage with LLM")

st.write("Upload a dataset (CSV) to begin. Select the attributes you want to analyze, and the LLM will infer their data lineage based on the schema and data preview.")

# Use session state to store the dataframe
if 'df' not in st.session_state:
    st.session_state.df = None

uploaded_file = st.file_uploader("Upload your data file", type=["csv"])

if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)
        st.session_state.df = df
    except Exception as e:
        st.error(f"Error loading CSV file: {e}")

if st.session_state.df is not None:
    df = st.session_state.df
    st.success("File uploaded successfully!")
    
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Data Attributes")
        st.write("Select one or more attributes to run lineage analysis.")
        
        all_attributes = df.columns.tolist()
        selected_attributes = st.multiselect(
            label="Select Attributes:",
            options=all_attributes,
            default=all_attributes[:1] if all_attributes else []
        )
        
        # In a real app, you would securely manage the API key
        api_key = st.text_input("Enter your OpenAI API Key", type="password", help="Your key is not stored.")

        run_button = st.button("🚀 Run Lineage Analysis")

    with col2:
        st.subheader("Dataset Preview")
        st.dataframe(df.head())

    if run_button:
        if not selected_attributes:
            st.warning("Please select at least one attribute.")
        elif not api_key:
            # This check is for the real implementation
            st.warning("Please enter your OpenAI API key to run the analysis.")
        else:
            with st.spinner("Calling LLM to analyze data lineage... Please wait."):
                # Prepare context for the LLM
                schema_info = "\n".join([f"- {col} ({dtype})" for col, dtype in df.dtypes.items()])
                sample_data_str = df.head().to_string()
                
                # Get the analysis from the LLM function
                lineage_result = get_llm_lineage_analysis(api_key, schema_info, selected_attributes, sample_data_str)
                
                st.subheader("Lineage Analysis Results")
                st.markdown(lineage_result)

