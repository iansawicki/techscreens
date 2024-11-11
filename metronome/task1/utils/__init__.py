from pydantic import BaseModel, ValidationError
import requests
from requests.exceptions import HTTPError, RequestException
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
    quantity: float
    custom_fields: Dict[str, str]


class LineItem(BaseModel):
    total: float
    credit_type: CreditType
    name: str
    product_id: str
    quantity: float
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

# Credit Grant Models
class CreditType(BaseModel):
    id: str
    name: str

class GrantAmount(BaseModel):
    amount: float
    credit_type: CreditType

class PaidAmount(BaseModel):
    amount: float
    credit_type: CreditType

class Balance(BaseModel):
    including_pending: Optional[int] = 0
    excluding_pending: Optional[int] = 0
    effective_at: str

class Deduction(BaseModel):
    amount: float
    reason: str
    running_balance: float
    effective_at: str
    created_by: str
    credit_grant_id: str
    invoice_id: Optional[str]

class CustomFields(BaseModel):
    x_account_id: Optional[str] = None  # Default None

class CreditGrant(BaseModel):
    id: str
    name: str
    customer_id: str
    uniqueness_key: Optional[str] = None  # Default None
    reason: Optional[str]
    effective_at: str
    expires_at: str
    priority: float
    grant_amount: GrantAmount
    paid_amount: PaidAmount
    balance: Balance
    deductions: List[Deduction]
    pending_deductions: List[Deduction] = []
    custom_fields: Optional[CustomFields] = None  # Default None
    credit_grant_type: Optional[str] = None  # Default None



# Todo: Implement the API client deal with data wrapper this way
#class ApiResponse(BaseModel):
#    data: List[DataItem]
#    next_page: Optional[str] = None



# Function to handle HTTP GET requests
def get(endpoint: str, params: dict = {}) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {API_KEY}"}
    full_endpoint = f"{BASE_URL}/{endpoint}"
    
    try:
        response = requests.get(full_endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        
        return response.json()  # Successful response, return JSON
        
    except HTTPError as http_err:
        if response.status_code == 401:
            return {"error": "Unauthorized access, please check your API key."}
        elif response.status_code == 403:
            return {"error": "Forbidden access, you don't have permission."}
        elif response.status_code == 404:
            return {"error": "Endpoint not found."}
        elif response.status_code == 500:
            return {"error": "Internal server error, please try again later."}
        else:
            return {"error": f"HTTP error occurred: {http_err}"}  # Catching all HTTP errors
    except RequestException as req_err:
        return {"error": f"Request error occurred: {req_err}"}  # For any other request issues
    except Exception as err:
        return {"error": f"An unexpected error occurred: {err}"}


# Function to handle HTTP POST requests
def post(endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {API_KEY}"}
    full_endpoint = f"{BASE_URL}/{endpoint}"
    
    try:
        response = requests.post(full_endpoint, headers=headers, json=data)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        
        return response.json()  # Successful response, return JSON
    
    except HTTPError as http_err:
        if response.status_code == 401:
            return {"error": "Unauthorized access, please check your API key."}
        elif response.status_code == 403:
            return {"error": "Forbidden access, you don't have permission."}
        elif response.status_code == 404:
            return {"error": "Endpoint not found."}
        elif response.status_code == 500:
            return {"error": "Internal server error, please try again later."}
        else:
            return {"error": f"HTTP error occurred: {http_err}"}  # Catching all HTTP errors
    except RequestException as req_err:
        return {"error": f"Request error occurred: {req_err}"}  # For any other request issues
    except Exception as err:
        return {"error": f"An unexpected error occurred: {err}"}


def get_customers(**params) -> List[Customer]:
        raw_data = get("customers", params=params)
        print(params)
        try:
            # Extract customers from 'data' key and convert each entry to a Customer model
            return [Customer(**item) for item in raw_data.get("data", [])]
        except ValidationError as e:
            print("Validation error:", e.json())
            return []
        
def get_customer(customer_id: str) -> Customer:
    raw_data = get(f"customers/{customer_id}").get("data", {})
    try:
        return Customer(**raw_data)
    except ValidationError as e:
        print("Validation error:", e.json())
        return None
    

def get_customer_invoices(customer_id: str) -> List[Invoice]:
    raw_data = get(f"customers/{customer_id}/invoices")
    # If the raw_data contains an error, return an empty list
    if "error" in raw_data:
        print("Error fetching invoices:", raw_data["error"])
        return []
    
    print("Fetching invoices for customer:", customer_id)
    try:
        # Return the list of Invoice objects, assuming "data" is the correct key for valid responses
        return [Invoice(**item) for item in raw_data.get("data", [])]
    except ValidationError as e:
        print("Validation error:", e.json())
        return []

def get_credit_balances(**data: Dict[str, Any]) -> List[Balance]:
    raw_data = post("credits/listGrants", data)
    # If the raw_data contains an error, return an empty list
    if "error" in raw_data:
        print("Error fetching balances:", raw_data["error"])
        return []

    print("Fetching balances for {} customers".format(len(data.get("customer_ids", []))))
    try:
        # Parse the raw data into CreditGrant objects and return only the balance part
        credit_grants = [CreditGrant(**item) for item in raw_data.get("data", [])]
        return [grant for grant in credit_grants]
    except ValidationError as e:
        print("Validation error:", e.json())
        return []
        
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
    # customers = get_customers()
    customer_ids = ['004747b8-9124-4060-989a-8d1075af2424',
 '0602ebf7-659e-470a-a536-9fbd413fb42b',
 '12184764-5687-4690-8794-35efc5586e72',
 '15b367c9-04b9-4064-9a58-b589928898fd',
 '1715df37-9b9b-4829-b381-e7febaefb102',
 '20ffd2e6-ff2e-4347-8045-9e744ef8a986',
 '2209a058-dbe1-4e8c-8325-fb8daa1cc987',
 '2294f7f7-19a6-44a2-b9f9-4b6b79134d12',
 '2ac3705a-51a6-4149-8f6a-0113941e94e7',
 '2ae68df2-533e-44bf-9b4d-ac766c7ac3da',
 '334ad07b-7bc1-4e3c-8337-a344837e344f',
 '37154c55-bd42-4b7e-a453-51dd005b35b7',
 '3e233dbd-0280-4def-aaa9-011a3a4ba745',
 '40430943-1fa7-48f4-86ba-0c27ad2386a4',
 '5b9a90c0-75cc-4771-958e-9974a3324dc3',
 '5f770337-056f-4430-813d-f6ace4ff876c',
 '5fb28f87-7884-4ce1-9b37-d3687f2d8cf2',
 '67c54616-a4d5-4717-8c95-4aa3b3e547b2',
 '69e51ea4-2ca3-4bf1-b606-6a8b496d98d5',
 '6c2e6096-2adc-4ced-b660-2211bc59b449']
    credit_balances = get_credit_balances(customer_ids=customer_ids)
    # print the first customer
    print(credit_balances)