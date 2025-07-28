import os
import csv
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials as GoogleCredentials
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import time
import json

# --- CONFIGURATION ---
CSV_DIR = "minio-report-tracker/csv"
SPREADSHEET_ID = "1gpV5T5Ol45VqmS8nI6Xk2MXWEeJMiXU1yoFUDMODi6g"
CREDENTIALS_FILE = "creds.json"

# Auth
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID)

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
