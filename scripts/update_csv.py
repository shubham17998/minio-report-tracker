import os
import re
import pandas as pd
from datetime import datetime
import subprocess

# MinIO alias and bucket details
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation/auth"

# Define the output CSV file path
CSV_DIR = "../spreadsheet"
CSV_FILE = os.path.join(CSV_DIR, "reports.csv")

# Ensure the spreadsheet directory exists
os.makedirs(CSV_DIR, exist_ok=True)

# Get the latest file from MinIO
def get_latest_file():
    try:
        result = subprocess.run(
            ["mc", "ls", MINIO_ALIAS, MINIO_BUCKET],
            capture_output=True,
            text=True,
            check=True
        )
        files = result.stdout.splitlines()
        
        # Extract filenames and timestamps
        file_data = []
        for line in files:
            parts = line.split()
            if len(parts) >= 5:
                timestamp = " ".join(parts[:2])  # Date and Time
                filename = parts[-1]
                file_data.append((timestamp, filename))

        if not file_data:
            print("❌ No files found in MinIO bucket.")
            return None

        # Sort by timestamp and get the latest file
        file_data.sort(reverse=True, key=lambda x: x[0])
        return file_data[0][1]

    except subprocess.CalledProcessError as e:
        print(f"❌ Error running mc command: {e}")
        return None

# Extract details from filename
def extract_details(filename):
    match = re.search(r"T-(\d+)_S-(\d+)_F-(\d+)", filename)
    if match:
        t_value, s_value, f_value = match.groups()
        return t_value, s_value, f_value
    return None, None, None

# Main script logic
def update_csv():
    latest_file = get_latest_file()
    if not latest_file:
        return
    
    t_value, s_value, f_value = extract_details(latest_file)
    if not t_value or not s_value or not f_value:
        print(f"❌ Failed to extract details from {latest_file}")
        return

    # Get today's date
    today = datetime.today().strftime("%Y-%m-%d")

    # Load existing CSV or create a new one
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
    else:
        df = pd.DataFrame(columns=["Filename", "T", "S", "F", "Date"])

    # Append new data
    new_data = pd.DataFrame([{
        "Filename": latest_file,
        "T": t_value,
        "S": s_value,
        "F": f_value,
        "Date": today
    }])

    df = pd.concat([df, new_data], ignore_index=True)

    # Save CSV
    df.to_csv(CSV_FILE, index=False)
    print(f"✅ Updated {CSV_FILE} with latest report data.")

if __name__ == "__main__":
    update_csv()
