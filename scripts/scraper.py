import os
import json
import pandas as pd
from datetime import datetime, date, timedelta
from playwright.sync_api import sync_playwright
import duckdb

# --- Configuration ---
# Paths are now relative to the scraper.py file's location within the 'scripts' folder
COMPANY_JSON_PATH = os.path.join(os.path.dirname(__file__), 'company.json')
EOD_DATA_FOLDER = os.path.join(os.path.dirname(__file__), '..', 'data', 'eod') # Go up, then into data/eod
MAIN_CSV_PATH = os.path.join(EOD_DATA_FOLDER, "nepse.csv")
DUCKDB_DATABASE_PATH = os.path.join(EOD_DATA_FOLDER, "nepse.duckdb")
DUCKDB_TABLE_NAME = "eod_data" # Name of the table inside the DuckDB file
NEPSE_LIVE_URL = "https://nepsealpha.com/live/stocks"

# Ensure data folder exists
os.makedirs(EOD_DATA_FOLDER, exist_ok=True)

# --- Load active equity symbols from company.json ---
def load_active_equity_symbols(json_path=COMPANY_JSON_PATH):
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            companies = json.load(f)
        return [
            c['symbol']
            for c in companies
            if c.get('instrumentType') == 'Equity' and c.get('status') == 'A'
        ]
    except FileNotFoundError:
        print(f"❌ Error: company.json not found at {json_path}. Cannot filter symbols.")
        return []
    except json.JSONDecodeError:
        print(f"❌ Error: Could not decode JSON from {json_path}.")
        return []

# --- Scrape NEPSE live JSON from <pre> tag using Playwright ---
def get_nepse_json_with_playwright(url, run_headless=True):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=run_headless)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        try:
            page.goto(url, wait_until="networkidle")
            pre_text = page.locator("pre").inner_text(timeout=5000)
            return json.loads(pre_text)
        except Exception as e:
            print(f"❌ Error fetching NEPSE data: {e}")
            return None
        finally:
            browser.close()

# --- Function to load CSV to DuckDB ---
def update_duckdb_from_csv(csv_path: str, db_path: str, table_name: str):
    """
    Reads data from a CSV file and loads/replaces it into a DuckDB database file.
    This function is called after the main CSV is updated.
    """
    if not os.path.exists(csv_path):
        print(f"❌ Error: CSV file not found at {csv_path}. Cannot load into DuckDB.")
        return False

    try:
        conn = duckdb.connect(database=db_path) # Connect to the DuckDB file

        # Read the entire CSV into a DuckDB table.
        # This will either create the table or overwrite it if it exists.
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}');")

        print(f"✅ Successfully loaded/updated '{table_name}' table in {db_path} from {csv_path}.")
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Error loading CSV to DuckDB: {e}")
        return False

# --- Main scraper logic ---
def scrape_and_manage_data():
    current_system_date = datetime.now().date()
    is_trading_day_today = current_system_date.weekday() not in [4, 5] # Friday (4) and Saturday (5) are holidays

    message = ""
    df_fresh_eod = None # Will store the newly scraped DataFrame if successful

    print(f"Attempting to scrape NEPSE data from {NEPSE_LIVE_URL}...")
    data = get_nepse_json_with_playwright(NEPSE_LIVE_URL, run_headless=True)
    
    if not data or "stock_live" not in data or not data["stock_live"].get("prices"):
        message = "No fresh live data returned from source or prices not found."
        print(f"⚠️ {message} Proceeding to update DuckDB from existing CSV.")
    else:
        prices = data["stock_live"].get("prices", [])
        as_of_time_str = data["stock_live"].get("asOf")

        if not as_of_time_str:
            message = "Scraped data missing 'asOf' timestamp. Cannot determine date."
            print(f"⚠️ {message} Proceeding to update DuckDB from existing CSV.")
        else:
            scraped_date = pd.to_datetime(as_of_time_str).date()

            for entry in prices:
                entry.pop("stockinfo", None)
                entry["asOf"] = as_of_time_str 

            df_scraped_raw = pd.DataFrame(prices)
            df_scraped_raw["Date"] = pd.to_datetime(df_scraped_raw["asOf"]).dt.strftime('%Y-%m-%d')

            final_columns = ['symbol', 'Date', 'open', 'high', 'low', 'close', 'volume']
            df_fresh_eod = df_scraped_raw[final_columns].rename(columns={
                'symbol': 'Symbol', 'open': 'Open', 'high': 'High', 'low': 'Low',
                'close': 'Close', 'volume': 'Volume'
            })
            df_fresh_eod['Volume'] = pd.to_numeric(df_fresh_eod['Volume'], errors='coerce').fillna(0).astype(int)
            for col in ['Open', 'High', 'Low', 'Close']:
                df_fresh_eod[col] = pd.to_numeric(df_fresh_eod[col], errors='coerce')
            df_fresh_eod = df_fresh_eod.sort_values(by='Symbol')

            # Determine if this scraped data is 'today's' or an older EOD (backfill scenario)
            if scraped_date <= current_system_date and (is_trading_day_today or scraped_date < current_system_date):
                message = f"Scraped EOD data for {scraped_date.isoformat()}."
                if scraped_date < current_system_date:
                    message += " (This is older than today, likely backfilling a missed day or current EOD on non-trading day)."
                print(f"✅ {message}")
            else:
                message = f"Scraped data date {scraped_date.isoformat()} is in the future or invalid. Skipping CSV update from this scrape."
                print(f"⚠️ {message} Proceeding to update DuckDB from existing CSV.")
                df_fresh_eod = None # Invalidate fresh data if it's not usable for update

    # --- CSV Management (only if fresh data was successfully scraped and deemed valid) ---
    df_main_updated_csv = None # DataFrame representing the state of the main CSV after update

    if df_fresh_eod is not None and not df_fresh_eod.empty:
        # Archive today's raw scrape first
        archive_file = os.path.join(EOD_DATA_FOLDER, f"nepse_{df_fresh_eod['Date'].iloc[0]}.csv")
        df_fresh_eod.to_csv(archive_file, index=False, encoding='utf-8-sig')
        print(f"✅ Archived scrape data to {archive_file}")

        # Update main CSV file
        if os.path.exists(MAIN_CSV_PATH):
            df_main_existing = pd.read_csv(MAIN_CSV_PATH)
            # Remove any existing entries for the date(s) we just scraped
            dates_to_replace = df_fresh_eod['Date'].unique()
            df_main_updated_csv = df_main_existing[~df_main_existing['Date'].isin(dates_to_replace)]
            df_main_updated_csv = pd.concat([df_main_updated_csv, df_fresh_eod], ignore_index=True)
        else:
            df_main_updated_csv = df_fresh_eod.copy()

        # Apply symbol filtering and sorting before saving to CSV
        allowed_symbols = load_active_equity_symbols(COMPANY_JSON_PATH)
        if allowed_symbols:
            df_main_updated_csv = df_main_updated_csv[df_main_updated_csv['Symbol'].isin(allowed_symbols)]
        df_main_updated_csv = df_main_updated_csv.sort_values(by=['Symbol', 'Date'], ascending=[True, False])

        df_main_updated_csv.to_csv(MAIN_CSV_PATH, index=False, encoding='utf-8-sig')
        print(f"✅ Updated main CSV file at {MAIN_CSV_PATH} with {len(df_main_updated_csv)} rows.")

        # Delete the temporary archive file
        try:
            if os.path.exists(archive_file):
                os.remove(archive_file)
                print(f"🗑️ Deleted archive file: {archive_file}")
        except Exception as e:
            print(f"⚠️ Error deleting archive file {archive_file}: {e}")
    else:
        print("No fresh data scraped or valid for CSV update. Relying on existing CSV for DuckDB update.")
        if os.path.exists(MAIN_CSV_PATH):
            df_main_updated_csv = pd.read_csv(MAIN_CSV_PATH)
            print(f"Loaded existing CSV with {len(df_main_updated_csv)} rows for DuckDB update.")
        else:
            print(f"❌ Main CSV file not found at {MAIN_CSV_PATH}. Cannot update DuckDB.")
            return None, "No valid data source (fresh scrape or existing CSV) for DuckDB."


    # --- DUCKDB UPDATE FROM CSV (ALWAYS RUNS IF MAIN_CSV_PATH EXISTS) ---
    print("\n--- Starting DuckDB database update from CSV ---")
    if df_main_updated_csv is not None and not df_main_updated_csv.empty:
        duckdb_load_success = update_duckdb_from_csv(MAIN_CSV_PATH, DUCKDB_DATABASE_PATH, DUCKDB_TABLE_NAME)
        if not duckdb_load_success:
            message += "\n⚠️ DuckDB database update failed from CSV."
            print("⚠️ DuckDB database update failed from CSV.")
        else:
            print("--- Finished DuckDB database update from CSV ---\n")
    else:
        message += "\n⚠️ Main CSV is empty or not loaded. Skipping DuckDB update."
        print("⚠️ Main CSV is empty or not loaded. Skipping DuckDB update.")

    return df_main_updated_csv, message


# Example usage (for testing scraper.py directly)
if __name__ == "__main__":
    df, msg = scrape_and_manage_data()
    if df is not None:
        print("\n--- Scraper Run Summary ---")
        print(f"Final Message: {msg}")
        if not df.empty:
            print(f"Total entries in final DataFrame (from updated CSV): {len(df)}")
            print("Sample data (from updated CSV):")
            print(df.head())
        else:
            print("No data in the final DataFrame.")
    else:
        print(f"\n--- Scraper Failed Overall ---")
        print(f"Error: {msg}")