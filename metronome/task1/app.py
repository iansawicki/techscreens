import streamlit as st
import pandas as pd
from langchain.agents.agent_types import AgentType
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
import openai
from dotenv import load_dotenv
import os
from utils import get_customers, load_and_process_data, get_customer_invoices
import json
from pathlib import Path

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
#st.write(OPENAI_API_KEY)

# Data directories
DATA_DIR = Path("data")
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
# Create directories if they don't exist
# Skip if they do
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PROCESSED_DATA_DIR.mkdir(exist_ok=True)
print(f"Created data directories successfully at {DATA_DIR}/{RAW_DATA_DIR} and {DATA_DIR}/{PROCESSED_DATA_DIR}")


# Streamlit stuff
st.title("Metronome API: Data Explorer")
st.write("Upload a CSV file, and ask questions about your data!")

# Set up your OpenAI API key (you can also use Streamlit's secret management)

st.sidebar.title("Settings")
MODEL = st.sidebar.selectbox("Select a model", ["gpt-4o", "gpt-4o-mini"])

# Set up tabs to define workflow
# Tab 1 raw data loader, 
# Tab 2, EDA
# Tab 3. summary reporter

# Preload customer data for selection tab
customer_list = get_customers()
# Save and reload the customer data data
json_file_raw = RAW_DATA_DIR / "customers_raw.json"
json_file_flat = RAW_DATA_DIR / "customers_flat.json"
csv_file = PROCESSED_DATA_DIR / "customers.csv"
if not csv_file.exists():
    customer_df = load_and_process_data(customer_list, json_file_raw, json_file_flat, csv_file)
else:
    customer_df = pd.read_csv(csv_file)


# Invoices data
json_file_raw_invoices = RAW_DATA_DIR / "invoices_raw.json"
json_file_flat_invoices = RAW_DATA_DIR / "invoices_flat.json"
csv_file_invoices = PROCESSED_DATA_DIR / "invoices.csv"


# Streamlit layout: Multiple tabs to handle overall workflow
tab1, tab2, tab3 = st.tabs(["API-->CSV Exporter", "Summary Report & Review", "AI Assisted Data Explorer",])


with tab1:
    # Defines a schema for loading and saving data: data class name, e.g. customer, the API results, and the CSV file to write
    # Get a list of customers and their corresponding ids from the Metronome API
    st.write("Loading data from API...")
    # Show customer dataframe in table
    # Create a lookup dicionary for the customer ids. The customer can then select a customer name and set the corresponding id
    customer_lookup = {customer.name: customer.id for customer in customer_list}

    selected_customer_name = st.selectbox("Select a customer", list(customer_lookup.keys())) 
    selected_customer_id = customer_lookup[selected_customer_name] # Look up corresponding ID for summary reporting downstream

    st.write("Selected customer name:", selected_customer_name)
    st.write("Customer Unique ID:", selected_customer_id)

    # Create a button to get raw data for invoices, balances, and transactions
    if st.button("Get raw data for summary reporting suite."):
        # Retrieving customer data
        st.write("Getting invoices for selected customer...")
        invoices = get_customer_invoices(selected_customer_id)
        st.write("Invoices retrieved successfully!")
        # Load and process data

        # Save and reload the customer data data
        invoices_df = load_and_process_data(invoices, json_file_raw_invoices, json_file_flat_invoices, csv_file_invoices)

        # Summary of data fetches
        st.write("Data fetches complete!")
        st.write("Invoices data:")
        # Show invoices data in table
        st.write(invoices_df.head())
        # Show data summary EDA stats
        st.write("Data summary:")
        st.write(invoices_df.describe())
        # Total number of invoices fetched
        st.write("Total number of invoices fetched:", len(invoices_df))
        # Total unique invoices
        st.write("Total unique invoices fetched:", len(invoices_df["id"].unique()))


with tab2:
    # Summary report
    # reload invoices data
    invoices_df = pd.read_csv(csv_file_invoices)
    # Add customer name column
    invoices_df["customer_name"] = selected_customer_name
    st.write("Summary report")
    st.write("Customer name:", selected_customer_name)
    st.write("Customer id:", selected_customer_id)
    # Only preserve finalized invoices with a total greater than 0
    filtered_invoices = invoices_df[(invoices_df["total"] > 0) & (invoices_df["status"] == "FINALIZED")]
    # Deduct adjustments from the total
    filtered_invoices["adjusted_totals"] = filtered_invoices["total"] - filtered_invoices["invoice_adjustments_0_total"]

    # OPTIONAL: Group by cols for data slicing and dicing
    groupby_cols = st.multiselect("Select columns to group by", filtered_invoices.columns)
    # Calculate total amount due - subtotal - adjustments = total
    #invoice_totals_df = filtered_invoices.agg({"adjusted_totals": "sum", "total": "sum", "invoice_adjustments_0_total": "sum", "subtotal": "sum"})
    invoice_totals_df = filtered_invoices.groupby(["customer_name"]+groupby_cols).agg({"total": "sum"}).reset_index()
    st.write("Invoice Totals:")
    st.write(invoice_totals_df)

with tab3:

    # Step 1: File Upload
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file:
        # Load CSV to DataFrame
        df = pd.read_csv(uploaded_file)
        st.write("Preview of uploaded data:")
        st.write(df.head())

        # Step 2: Initialize LangChain with OpenAI
        agent = create_pandas_dataframe_agent(
        ChatOpenAI(temperature=0, model=MODEL,api_key=OPENAI_API_KEY),
        df,
        verbose=True,
        agent_type=AgentType.OPENAI_FUNCTIONS,
        allow_dangerous_code=True
    )


        # Step 3: Chat Interface for EDA
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []

        # User input box
        st.write("Ask a question about your data!")
        st.write("Example: How do I calculate the invoice balance?")
        
        user_input = st.text_input("Your question here")

        if user_input.strip():
            # Run the agent with user input
            response = agent.invoke(user_input)
            output = response["output"]
            
            # Save interaction to chat history
            st.session_state.chat_history.append(("User", user_input))
            st.session_state.chat_history.append(("Assistant", output))

        # Display chat history
        for sender, message in st.session_state.chat_history:
            if sender == "User":
                st.write(f"**{sender}:** {message}")
            else:
                st.write(f"*{sender}:* {message}")


