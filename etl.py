import gspread
from google.oauth2.service_account import Credentials
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Table, Column, Integer, String, Date, Numeric, MetaData, text
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import psycopg2

load_dotenv()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_URL = os.getenv("DB_URL")
service_account_file = os.path.join(BASE_DIR, "sheets-credentials.json")
if not service_account_file or not os.path.exists(service_account_file):
    raise FileNotFoundError(f"Service account file not found: {service_account_file}")


scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
client = gspread.authorize(creds)


# Connect to the Google Sheet
sheet_ids = {
    "Deliveries" : os.getenv("Deliveries_SheetID"),
    "Walk-In Store": os.getenv("WalkIn_SheetID"),
    "Servicing" : os.getenv("Servicing_SheetID")
}

# Column mappings
COLUMN_MAPPINGS = {
    "Deliveries": {
        "Delivery Date": "delivery_date",
        "Order ID": "order_id",
        "Customer Name": "customer_name",
        "Delivery Location": "delivery_location",
        "Product": "product",
        "Quantity Delivered": "quantity_delivered",
        "Unit Price": "unit_price",
        "Delivery Fee": "delivery_fee",
        "Total Charge": "total_charge",
        "Payment Method": "payment_method",
    },
    "Walk-In Store": {
        "Receipt Number": "receipt_number",
        "Date": "sale_date",
        "Customer Name": "customer_name",
        "Product": "product",
        "Quantity Sold": "quantity_sold",
        "Price per Unit": "unit_price",
        "Total Amount": "total_amount",
        "Payment Method": "payment_method",
    },
    "Servicing": {
        "Service Date": "service_date",
        "Service Ticket": "service_ticket",
        "Client Name": "client_name",
        "Service Type": "service_type",
        "Service Rate": "service_rate",
        "Payment Method": "payment_method",
    },
}

# Push function
def push_sheet(sheet_id, worksheet_name, table_name, unique_col, mapping):
    print(f"\n--- Pushing sheet: {worksheet_name} ---")
    
    # 1️⃣ Try opening the sheet
    try:
        ws = client.open_by_key(sheet_id).worksheet(worksheet_name)
        print(f"✅ Opened worksheet '{worksheet_name}' successfully.")
    except Exception as e:
        print(f"❌ Failed to open worksheet '{worksheet_name}': {e}")
        return

    # 2️⃣ Fetch rows
    rows = ws.get_all_records()
    if not rows:
        print(f"⚠️ No rows found in '{worksheet_name}'.")
        return
    print(f"ℹ️ Fetched {len(rows)} rows from '{worksheet_name}'.")

    # 3️⃣ Connect to DB
    try:
        engine = create_engine(DB_URL)
        print("✅ Database connection successful.")
    except Exception as e:
        print(f"❌ Failed to connect to DB: {e}")
        return

    # Insert rows with checks
    with engine.begin() as conn:
        inserted = 0
        for i, row in enumerate(rows, 1):
            # Check if all mapping keys exist
            missing_keys = [k for k in mapping if k not in row]
            if missing_keys:
                print(f"⚠️ Row {i} missing keys: {missing_keys}")
                continue

            cleaned_row = {mapping[k]: row[k] for k in mapping}
            cols = ", ".join([f'"{k}"' for k in cleaned_row.keys()])
            placeholders = ", ".join([f":{k}" for k in cleaned_row.keys()])
            stmt = text(f"""
                INSERT INTO {table_name} ({cols})
                VALUES ({placeholders})
                ON CONFLICT ({unique_col}) DO NOTHING
            """)

            try:
                conn.execute(stmt, cleaned_row)
                inserted += 1
            except Exception as e:
                print(f"❌ Failed to insert row {i}: {e}")

        print(f"✅ Inserted {inserted}/{len(rows)} rows into table '{table_name}'.")

# Run etl for each sheet
def run_etl():
    push_sheet(sheet_ids["Deliveries"], "Deliveries", "deliveries", "order_id", COLUMN_MAPPINGS["Deliveries"])
    push_sheet(sheet_ids["Walk-In Store"], "Walk-In Store", "walkinstore", "receipt_number", COLUMN_MAPPINGS["Walk-In Store"])
    push_sheet(sheet_ids["Servicing"], "Servicing", "servicing", "service_ticket", COLUMN_MAPPINGS["Servicing"])

# Check completion
print("Sheets pushed into Postgres")