# Store Management ETL Automation

This project automates the extraction, transformation, and loading (ETL) of data from Google Sheets into a PostgreSQL database for store management purposes. It supports daily scheduled updates using a Python scheduler.

## Features

- Extracts data from multiple Google Sheets (Deliveries, Walk-In Store, Servicing)
- Transforms and maps sheet columns to database schema
- Loads data into PostgreSQL tables, avoiding duplicates
- Scheduled daily ETL runs at 7 PM

## Setup

### 1. Clone the repository

```sh
git clone <your-repo-url>
cd Automation-with-Python
```

### 2. Install dependencies

Create a virtual environment (optional but recommended):

```sh
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

Install required packages:

```sh
pip install -r requirements.txt
pip install python-dotenv psycopg2-binary
```

### 3. Configure environment variables

Edit the `.env` file with your Google Sheets IDs:

```env
Deliveries_SheetID=your_deliveries_sheet_id
WalkIn_SheetID=your_walkin_sheet_id
Servicing_SheetID=your_servicing_sheet_id
```

### 4. Add Google Sheets credentials

Place your `sheets-credentials.json` file in the project root.  
This file should contain your Google service account credentials.

### 5. Set up PostgreSQL

Update the `DB_URL` in [`etl.py`](etl.py) with your PostgreSQL connection string.

## Usage

### Run ETL manually

```sh
python etl.py
```

### Run ETL on a schedule (daily at 7 PM)

```sh
python scheduler.py
```

## File Structure

- [`etl.py`](etl.py): Main ETL logic
- [`scheduler.py`](scheduler.py): Scheduler for daily ETL runs
- [`requirements.txt`](requirements.txt): Python dependencies
- `.env`: Environment variables (sheet IDs, credentials)
- `sheets-credentials.json`: Google Sheets API credentials

## License

MIT License

---

**Note:**  

- Do not commit `.env` or `sheets-credentials.json` to version control.
- Make sure your PostgreSQL tables match the expected schema in [`etl.py`](etl.py).
