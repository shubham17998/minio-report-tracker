import os
import csv
import gspread
from google.oauth2.service_account import Credentials as GoogleCredentials
from googleapiclient.discovery import build
import time
import json

# --- CONFIGURATION ---
SOURCE_DIR = "minio-report-tracker/csv"
SPREADSHEET_ID = "1uGwQLZ0wIF4nGF98cyoJjZTu9UG30UbTM259X2R1RH4"
CREDENTIALS_FILE = "creds.json"

# --- CONSTANTS ---
START_ROW_INDEX = 2
START_COL = 9  # This is column J
NUM_DATA_COLS = 6
BLOCK_WIDTH = 10  # Updated: 6 data + 1 header + 1 module + 1 date + 1 PO column

# --- FORMATTING STYLES ---
COLORS = {
    "red": {"red": 1.0, "green": 0.0, "blue": 0.0},
    "green": {"red": 0.0, "green": 0.6, "blue": 0.1},
    "light_grey_fill": {"red": 0.85, "green": 0.85, "blue": 0.85}
}
BORDER = {"style": "SOLID", "width": 1}

def col_to_a1(col_idx):
    a1 = ""
    col_idx += 1
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        a1 = chr(65 + remainder) + a1
    return a1

def convert_cell(val):
    if val is None or str(val).strip() == '':
        return None
    try:
        f = float(val)
        return int(f) if f.is_integer() else f
    except (ValueError, TypeError):
        return str(val).strip()

# --- Unchanged helper functions: create_bake_and_delete_requests(), apply_new_conditional_formatting() ---

# (Keep those exactly as-is from your working code; they do not need to change for PO column addition.)

def update_sheet(service, spreadsheet, sheet_name, csv_path):
    try:
        print(f"\n--- Processing: {sheet_name} from {csv_path} ---")
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Worksheet '{sheet_name}' not found. Creating it.")
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="50")

    first_block_rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if any(cell.strip() for cell in row[:10]):
                first_block_rows.append(row[:10])

    if len(first_block_rows) < 2:
        print(f"⚠️ WARNING: CSV '{csv_path}' seems malformed. Skipping.")
        return

    date_label = first_block_rows[1][0].strip()
    csv_headers = [h.strip() for h in first_block_rows[0][2:2 + NUM_DATA_COLS]] + ["PO"]  # Added PO
    csv_data_map = {
        row[1].strip(): [convert_cell(c) for c in row[2:2 + NUM_DATA_COLS]] + [None]  # Added None for PO
        for row in first_block_rows[1:]
        if len(row) > 1 and row[1].strip()
    }
    print(f"  -> Found date '{date_label}' with {len(csv_data_map)} modules in CSV.")

    existing_data = sheet.get_all_values()
    master_module_list = [row[0] for row in existing_data[START_ROW_INDEX:] if row and row[0]] if len(existing_data) > START_ROW_INDEX else []

    new_modules = sorted([m for m in csv_data_map if m not in master_module_list])
    if new_modules:
        print(f"  -> New modules found: {', '.join(new_modules)}. Appending.")
        sheet.append_rows(
            values=[[m] for m in new_modules],
            value_input_option='USER_ENTERED',
            table_range=f"A{len(master_module_list) + START_ROW_INDEX + 1}"
        )
        master_module_list.extend(new_modules)

    aligned_data_block = [csv_data_map.get(m, [None] * (NUM_DATA_COLS + 1)) for m in master_module_list]  # +1 for PO

    sheet_headers = existing_data[0] if existing_data else []
    target_col = sheet_headers.index(date_label) if date_label in sheet_headers else -1

    if target_col == -1:
        print(f"  -> Date '{date_label}' not found. Beginning column insertion process.")
        target_col = START_COL

        if len(sheet_headers) > START_COL and sheet_headers[START_COL]:
            bake_reqs, delete_reqs = create_bake_and_delete_requests(service, SPREADSHEET_ID, sheet.id, sheet.title, START_COL, len(master_module_list))

            if bake_reqs:
                print(f"[DEBUG] STAGE 1: Sending {len(bake_reqs)} requests to bake colors.")
                service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": bake_reqs}).execute()
                time.sleep(3)

            insert_req = {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": START_COL,
                        "endIndex": START_COL + BLOCK_WIDTH
                    },
                    "inheritFromBefore": False
                }
            }
            delete_reqs.append(insert_req)
            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": delete_reqs}).execute()
        else:
            print("  -> No existing data at insertion point. Inserting new columns directly.")
            insert_req = {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": START_COL,
                        "endIndex": START_COL + BLOCK_WIDTH
                    },
                    "inheritFromBefore": False
                }
            }
            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": [insert_req]}).execute()

    else:
        print(f"  -> Date '{date_label}' found. Updating columns in place.")

    update_body = {
        "valueInputOption": "USER_ENTERED",
        "data": [
            {"range": f"'{sheet.title}'!{col_to_a1(target_col)}1", "values": [[date_label]]},
            {"range": f"'{sheet.title}'!{col_to_a1(target_col)}2", "values": [csv_headers]},
            {"range": f"'{sheet.title}'!{col_to_a1(target_col)}{START_ROW_INDEX + 1}", "values": aligned_data_block}
        ]
    }
    print(f"  -> Writing data to sheet '{sheet.title}' starting at column {col_to_a1(target_col)}.")
    service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=update_body).execute()

    apply_new_conditional_formatting(service, SPREADSHEET_ID, sheet.id, target_col, len(master_module_list))

    print(f"✅ Sheet '{sheet_name}' updated successfully.")

def main():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets"
        ]
        creds = GoogleCredentials.from_service_account_file(CREDENTIALS_FILE, scopes=scope)
        service = build("sheets", "v4", credentials=creds)

        from oauth2client.service_account import ServiceAccountCredentials as GSpreadCredentials
        gspread_creds = GSpreadCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        client = gspread.authorize(gspread_creds)
        spreadsheet = client.open_by_key(SPREADSHEET_ID)

        if not os.path.isdir(SOURCE_DIR):
            print(f"❌ ERROR: Source directory '{SOURCE_DIR}' not found.")
            return

        csv_files = sorted([f for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")])
        if not csv_files:
            print("No CSV files found in 'source' directory. Nothing to do.")
            return

        for filename in csv_files:
            sheet_name = os.path.splitext(filename)[0]
            csv_path = os.path.join(SOURCE_DIR, filename)
            update_sheet(service, spreadsheet, sheet_name, csv_path)

    except FileNotFoundError:
        print(f"❌ CRITICAL ERROR: Credentials file '{CREDENTIALS_FILE}' not found.")
    except Exception as e:
        print(f"❌ A critical error occurred in main(): {e}")
        raise

if __name__ == "__main__":
    main()
