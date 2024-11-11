# Metronome Tech Screen 

## Tasks
 - Task 1: Generate a summary report. Process it to a single csv. It should include the following:
    - [x] Invoice Balance
    - [x] Credit Balance - no data returned
    - [ ] Additional Information - what else would someone want?

 - Task 2: Query sample egress dataset for common client asks
    - [x] Number of images generated between March 10th and 25th, 2024
    - [x] Recreate the March invoice for customer A1 Company.
    - [x] Generate a report of billings by Plan for the month of March 2024

## Submissions
All of the csv outputs, with corresponding screenshots, can be found in the `submissions` subfolder found under each task folder, `task1/submissions` and `task2/submissions`.

## Approach
*NOTE*: I did most of the work in notebooks. For task 1, it specifically asked for a script. This is the `invoicer.py` file.

### Task 1
I used the python `requests` module to call the API and pydantic for data validation. I loaded those records into a duckdb table in memory for querying locally.

~I created `unnest_json` function to preprocess the data for pandas. 
You can see the code that calls the API in /utils. The rest of the data ETL code is part of the streamlit app in app.py.~

I decided to use duckdb for this task after using it for the second half of the assignment. Longer-term, that ergonomics of that for these types of queries is simpler and more straightforward than manipulating data. 

- I started the assignment using an older version of the API. I believe all my code is updated to the current spec, but didn't have time to thoroughly review.
- I'm not 100% confident that I pulled the exact right fields to calculate invoice or credit balance totals. I had to do some Google searching to determine what the right approach to calculating a balance might be and it turned out, of course, that was precalculated as running balance. 
- Also it said "current invoice" -- I took that to mean the newest one, so used a rank function to pull that. 
- Unclear if I should have done the same for credit balance, rank based on some date value. 
- Finally, I only gave myself so much time. I am sure there are areas to be improved. 

### Task 2
I hope the SQL queries speak for themselves and am happy to go through them. The biggest challenge was interpreting the schema, since it was new to me. In my first query, for item b in the homework, I examined each schema to find the data I needed. 

The consistent use of `id` made it easy to relate the data items. You can see that below. 

I eventually copied DB table schema INFO into GPT-4 and asked it to infer table relationships for me. This helped validate my assumptions. 


### Table Relationships Map
Here's the schema and relationships description in Markdown format:

---

# Relational Database Schema Diagram and Table Relationships

Based on the given table structures, here is a possible relational database schema diagram and relationship descriptions for the tables:

### Tables and Relationships

1. **Table: `customer`**
   - **Primary Key**: `id`
   - **Relationships**:
     - One-to-many relationship with the **`invoice`** table: each customer (`customer.id`) can have multiple invoices (`invoice.customer_id`).

2. **Table: `invoice`**
   - **Primary Key**: `id`
   - **Foreign Keys**:
     - `customer_id` references `customer.id`, linking each invoice to the corresponding customer.
     - `plan_id` references `plan.id`, associating each invoice with a specific plan.
   - **Relationships**:
     - One-to-many relationship with the **`line_item`** table: each invoice (`invoice.id`) can have multiple line items (`line_item.invoice_id`), representing individual charges within the invoice.

3. **Table: `plan`**
   - **Primary Key**: `id`
   - **Relationships**:
     - One-to-many relationship with the **`invoice`** table: each plan (`plan.id`) can be associated with multiple invoices (`invoice.plan_id`).
     - One-to-many relationship with the **`plan_charge`** table: each plan (`plan.id`) can define multiple charges (`plan_charge.plan_id`) based on products and metrics.

4. **Table: `line_item`**
   - **Primary Key**: `id`
   - **Foreign Keys**:
     - `invoice_id` references `invoice.id`, linking each line item to its parent invoice.
     - `product_id` references `product.id`, associating each line item with a specific product.
   - **Relationships**:
     - One-to-many relationship with the **`sub_line_item`** table: each line item (`line_item.id`) can have multiple sub-line items (`sub_line_item.line_item_id`) that break down charges or items within the line item.

5. **Table: `sub_line_item`**
   - **Primary Key**: `id`
   - **Foreign Keys**:
     - `line_item_id` references `line_item.id`, associating each sub-line item with a specific line item.
     - `charge_id` and `billable_metric_id` relate to specific charges and metrics to provide detailed breakdowns per sub-line item.

6. **Table: `plan_charge`**
   - **Primary Key**: `id`
   - **Foreign Keys**:
     - `plan_id` references `plan.id`, linking each plan charge to a specific plan.
     - `product_id` references `product.id`, associating each charge with a specific product.
     - `billable_metric_id` references `billable_metric.id`, indicating which billable metrics are applicable to each charge within a plan.

7. **Table: `product`**
   - **Primary Key**: `id`
   - **Relationships**:
     - One-to-many relationship with the **`line_item`** table: each product (`product.id`) can be linked to multiple line items through `line_item.product_id`, representing products billed to customers in invoices.

8. **Table: `billable_metric`**
   - **Primary Key**: `id`
   - **Relationships**:
     - One-to-many relationship with the **`plan_charge`** table: each billable metric (`billable_metric.id`) can be used in multiple plan charges through `plan_charge.billable_metric_id`.

