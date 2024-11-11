
# # Task 1 - Querying the Metronome API
#  - Query the Metronome API to retrieve relevant customer data.
#  - Process the retrieved data to generate a summary report (csv).
#  - Include essential customer information such as customer name, customer invoice balance, credit balance, etc. 
#  - Process the report to a single csv

# %%
import pandas as pd
from dotenv import load_dotenv
import os
from utils import get_customers, load_and_process_data, get_customer_invoices, get_credit_balances, models_to_dicts
import json
from pathlib import Path

load_dotenv()

# Data directories
DATA_DIR = Path("data")
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
# Create directories if they don't exist
# Skip if they do
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PROCESSED_DATA_DIR.mkdir(exist_ok=True)

customers_csv = PROCESSED_DATA_DIR / "customer_list.csv"
customer_invoices_csvs = PROCESSED_DATA_DIR / "invoices.csv"
customer_credit_balances_csv = PROCESSED_DATA_DIR / "credit_balances.csv"

# DuckDB name
DB_NAME = "invoicer.db"

# %%
# Preload customer data for selection tab
customer_list = get_customers()
customer_list_dicts = models_to_dicts(customer_list)
# Save customer list to file
with open(RAW_DATA_DIR / "customer_list.json", "w") as f:
    json.dump(customer_list_dicts, f)
# Save to csv   
customer_list_df = pd.DataFrame(customer_list_dicts)
customer_list_df.to_csv(customers_csv, index=False)

# %%
# Get invoices for each customer
all_invoices_all_customers = []
for customer in customer_list:
    invoices = get_customer_invoices(customer.id)
    invoices_dicts = models_to_dicts(invoices)
    # Save invoices to file
    with open(RAW_DATA_DIR / f"{customer.id}_invoices.json", "w") as f:
        json.dump(invoices_dicts, f)
    all_invoices_all_customers.extend(invoices_dicts)
# Save all invoices to single csv
all_invoices_df = pd.DataFrame(all_invoices_all_customers)
all_invoices_df.to_csv(customer_invoices_csvs, index=False)

# %%
# Get credit balances for each customer
customer_ids = [customer.id for customer in customer_list]
credit_balances = get_credit_balances(customer_ids=customer_ids)
credit_balances_dicts = models_to_dicts(credit_balances)
print(len(credit_balances_dicts))
# Save credit balances to file
with open(RAW_DATA_DIR / "credit_balances.json", "w") as f:
    json.dump(credit_balances_dicts, f)

# Save to a single csv
credit_balances_df = pd.DataFrame(credit_balances_dicts)
credit_balances_df.to_csv(customer_credit_balances_csv, index=False)

# %%
import duckdb
con = duckdb.connect(DB_NAME)

# %%
# Load customers into duckdb
con.execute(f"CREATE TABLE customers AS SELECT * FROM read_csv_auto('{customers_csv}')")

# Load invoices into duckdb
con.execute(f"CREATE TABLE invoices AS SELECT * FROM read_csv_auto('{customer_invoices_csvs}')")

# Load credit balances into duckdb
con.execute(f"CREATE TABLE credit_balances AS SELECT * FROM read_csv_auto('{customer_credit_balances_csv}')")


# %%
# Converts PGSQL key-value string to JSON 
def convert_kv_to_json(kv_str: str) -> str:
    # Replace all single quotes with double quotes
    kv_str = kv_str.replace("'", '"')
    return kv_str

# Register the function in DuckDB
#con.remove_function("convert_kv_to_json")
con.create_function("convert_kv_to_json", convert_kv_to_json)



# %% [markdown]
# # Notes
# - I'm not 100% confident that I pulled the exact right fields to calculate invoice or credit balance totals. I had to do some Google searching to determine what the right approach to calculating a balance might be and it turned out, of course, that was precalculated as running balance. 
# - Also it said "current invoice" -- I took that to mean the newest one, so used a rank function to pull that. 
# - Unclear if I should have done the same for credit balance, rank based on some date value. 
# - Finally, I only gave myself so much time. I am sure there are areas to be improved. 

# %%
con.execute("""
            
WITH ranked_invoices AS (
    SELECT customer_id,
           total,
           end_timestamp,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_timestamp DESC) AS rn
    FROM invoices
    WHERE status = 'FINALIZED'
),
            
invoice_totals as (
    SELECT customer_id,
       count(*) as total_invoices,
       CONCAT('$', ROUND(SUM(total/100),2), ' USD') AS total_invoiced
       FROM ranked_invoices
       WHERE rn = 1
       GROUP BY customer_id),
 balance_unpack AS (
    SELECT
        id,
        name,
        customer_id,
        reason,
        JSON(convert_kv_to_json(balance)) AS balance_properties,
        JSON(convert_kv_to_json(deductions)) AS deductions_properties,
        JSON(convert_kv_to_json(grant_amount)) AS grant_amount_properties
    FROM credit_balances
    WHERE balance IS NOT NULL),
            
   balance_adjustments as (select
            id,
            name,
            customer_id,
            reason, 
            deductions_properties[0].amount::double as deductions_amount, 
            deductions_properties[0].running_balance::double as running_balance, 
            balance_properties.including_pending::double as including_pending,
            balance_properties.excluding_pending::double as excluding_pending,
            grant_amount_properties.amount::double as grant_amount
            FROM balance_unpack
            ),
    total_adjustments as (
        SELECT
            customer_id,
            CONCAT('$', ROUND(SUM(running_balance/100),2), ' USD') AS total_balance_credits
        FROM balance_adjustments
        GROUP BY 1
    )
            select c.name,
            i.total_invoiced as current_invoice_balance,
            t.total_balance_credits as credit_balance,
            from customers c
            LEFT JOIN invoice_totals i ON c.id = i.customer_id
            LEFT JOIN total_adjustments t ON c.id = t.customer_id
            ORDER BY c.name
            
            """).fetchdf().to_csv("./submissions/task_1_invoicing_invoicer.csv", index=False)



