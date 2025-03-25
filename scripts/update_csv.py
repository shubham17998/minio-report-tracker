import os
import subprocess
import pandas as pd
import re
from datetime import datetime

# MinIO alias and bucket details
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"
MINIO_FOLDER = "auth/"  # Subfolder inside the bucket

# CSV file path
CSV_DIR = "../spreadsheet"
CSV_FILE = os.path.join(CSV_DIR, "reports.csv")

# Ensure the directory exists
os.makedirs(CSV_DIR, exist_ok=True)

# Run mc command to list files
try:
    cmd = f"mc ls {MINIO_ALIAS}/{MINIO_BUCKET}/{MINIO_FOLDER}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
    file_list = result.stdout.strip().split("\n")

    extracted_data = []
    
    # Filename pattern to extract T, P, S, F values
    pattern = re.compile(r"T-(\d+)_S-(\d+)_F-(\d+)_I-\d+_KI-\d+")

    for line in file_list:
        parts = line.split()
        if len(parts) < 5:
            continue  # Skip invalid lines

        filename = parts[-1]
        match = pattern.search(filename)
        
        if match:
            T, S, F = match.groups()
            date = datetime.now().strftime("%Y-%m-%d")  # Today's date
            extracted_data.append([filename, T, "-", S, F, date])
        else:
            print(f"❌ Failed to extract details from {filename}")

    # Convert to DataFrame
    df = pd.DataFrame(extracted_data, columns=["Filename", "T", "P", "S", "F", "Date"])

    # Append or create CSV file
    if os.path.exists(CSV_FILE):
        df.to_csv(CSV_FILE, mode="a", header=False, index=False)
    else:
        df.to_csv(CSV_FILE, index=False)

    print(f"✅ Updated {CSV_FILE} with latest report data.")

except subprocess.CalledProcessError as e:
    print(f"❌ MinIO command failed: {e.stderr}")
except Exception as e:
    print(f"❌ Error: {e}")
