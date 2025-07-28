import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime

# Constants
CSV_DIR = "minio-report-tracker/minio-report-tracker/csv"
GSHEET_NAME = "MOSIP Report Tracker"
DATE_CELL = "A1"

# Auth
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)
sheet = client.open(GSHEET_NAME)

# Process each CSV file
for csv_file in os.listdir(CSV_DIR):
    if not csv_file.endswith(".csv"):
        continue

    file_path = os.path.join(CSV_DIR, csv_file)
    worksheet_name = os.path.splitext(csv_file)[0]

    # Create or select worksheet
    try:
        worksheet = sheet.worksheet(worksheet_name)
        sheet.del_worksheet(worksheet)
        worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")

    # Load CSV
    df = pd.read_csv(file_path)
    data = [df.columns.tolist()] + df.fillna("").astype(str).values.tolist()
    
    # Write to sheet
    worksheet.update("A2", data)
    worksheet.update(DATE_CELL, f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
