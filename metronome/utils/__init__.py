from pydantic import BaseModel, ValidationError
import requests
import pandas as pd
from typing import List, Optional, Union, Dict, Any
from dotenv import load_dotenv
import os
from datetime import datetime
#from uuid import UUID
import json

# Get parent path
PARENT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from dotenv .env file
load_dotenv(PARENT_PATH + "/.env")
API_KEY = os.getenv("API_KEY")
BASE_URL = os.getenv("BASE_URL")

class Customer(BaseModel):
    name: str
    custom_fields: Dict[str, Any]
    external_id: Optional[str]
    ingest_aliases: List[str]
    id: str
    customer_config: Dict[str, Any]

# Found on LineItem Model
class CreditType(BaseModel):
    id: str
    name: str

# Sublines on LineItem Model
class SubLineItem(BaseModel):
    charge_id: str
    name: str
    subtotal: float
    price: float
    quantity: int
    custom_fields: Dict[str, str]


class LineItem(BaseModel):
    total: float
    credit_type: CreditType
    name: str
    product_id: str
    quantity: int
    custom_fields: Dict[str, str]
    sub_line_items: List[SubLineItem]

class InvoiceAdjustment(BaseModel):
    total: float
    credit_type: CreditType

class Invoice(BaseModel):
    id: str
    start_timestamp: str
    end_timestamp: str
    customer_id: str
    customer_custom_fields: Dict[str, str]
    type: str
    credit_type: CreditType
    plan_id: str
    plan_name: str
    plan_custom_fields: Dict[str, str]
    status: str
    total: float
    external_invoice: Optional[str]
    subtotal: float
    line_items: List[LineItem]
    invoice_adjustments: List[InvoiceAdjustment]
    custom_fields: Dict[str, str]
    billable_status: str

# Todo: Implement the API client deal with data wrapper this way
#class ApiResponse(BaseModel):
#    data: List[DataItem]
#    next_page: Optional[str] = None


def _request(endpoint: str, params:dict = {}) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {API_KEY}"}
    full_endpoint = f"{BASE_URL}/{endpoint}"
    print(full_endpoint)
    response = requests.get(f"{full_endpoint}", headers=headers, params=params)
    return response.json()

def get_customers(**params) -> List[Customer]:
        raw_data = _request("customers", params=params)
        print(params)
        try:
            # Extract customers from 'data' key and convert each entry to a Customer model
            return [Customer(**item) for item in raw_data.get("data", [])]
        except ValidationError as e:
            print("Validation error:", e)
            return []
        
def get_customer(customer_id: str) -> Customer:
    raw_data = _request(f"customers/{customer_id}").get("data", {})
    try:
        return Customer(**raw_data)
    except ValidationError as e:
        print("Validation error:", e)
        return None
    

def get_customer_invoices(customer_id: str) -> List[Invoice]:
    raw_data = _request(f"customers/{customer_id}/invoices").get("data", [])
    try:
        #return raw_data
        return [Invoice(**item) for item in raw_data]
    except ValidationError as e:
        print("Validation error:", e)
        return []

def get_balances(**params) -> List[Dict[str, Any]]:
    raw_data = _request("contracts/customerBalances/list", params=params)
    return raw_data
        
# Convert a list of Pydantic models to a list of dictionaries
def models_to_dicts(models: List[BaseModel]) -> List[Dict[str, Any]]:
    return [model.dict() for model in models]

# Flatten nested python dictionaries
def unnest_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(unnest_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(unnest_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}{sep}{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)

def load_and_process_data(api_results,json_file_raw,json_file_flat, csv_file):
    # Convert data models to dictionaries
    data_dicts_list = models_to_dicts(api_results)


    # Write JSON data to JSON file
    with open(json_file_raw, "w") as f:
        json.dump(data_dicts_list, f)

    flat_data = [unnest_dict(d) for d in data_dicts_list]

    # Write flattened JSON data to JSON file
    with open(json_file_flat, "w") as f:
        json.dump(flat_data, f)
    # Convert JSON data to a Pandas DataFrame and write to CSV
    df = pd.DataFrame(flat_data)
    df.to_csv(csv_file, index=False)
    return df


if __name__ == "__main__":
    # Example usage
    # Get a list of customers
    customers = get_customers(limit=1)
    print(customers)
    # Set example customer to first customer in customers list
    eg_customer = customers[0]
    eg_customer_id = eg_customer.id
    customer = get_customer(customer_id=eg_customer_id)
    #print(customer)
    invoices = get_customer_invoices(eg_customer_id)
    #print(invoices)

    # Getting 404s from the balances API
    #balances = get_balances()
    #print(balances)
    
    # Convert a list of Pydantic models to a list of dictionaries
    customer_dicts = models_to_dicts(customers) # Customers
    print("Total records found:", len(customer_dicts))
    flat_customer_dicts = [unnest_dict(c) for c in customer_dicts] 
    print(flat_customer_dicts) 

    invoice_dicts = models_to_dicts(invoices) # Invoices
    flat_invoice_dicts = [unnest_dict(i) for i in invoice_dicts] # Flatten nested dictionaries
    print("Converted invoices and customers to dictionaries")
    
    # Pandas joins and merges
    print("Converting to pandas dataframes")
    customers_df = pd.DataFrame(flat_customer_dicts)
    customers_df.to_csv("customers.csv", index=False)
    invoices_df = pd.DataFrame(flat_invoice_dicts)
    invoices_df.to_csv("invoices.csv", index=False)

    customers_df.id = customers_df.id.astype(str)
    invoices_df.customer_id = invoices_df.customer_id.astype(str)
    # Left join invoices to customers
    summary_df = pd.merge(customers_df, invoices_df, left_on="id", right_on="customer_id", how="left")
    # output to csv
    summary_df.to_csv("output.csv", index=False)