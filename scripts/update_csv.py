import os
import re
import pandas as pd
import subprocess

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get all folders inside the automation bucket
cmd_list_folders = f"mc ls {MINIO_ALIAS}/{MINIO_BUCKET} --json"
output = subprocess.getoutput(cmd_list_folders)
folders = [line.split(' ')[-1].strip('/') for line in output.split('\n') if '"type":"folder"' in line]

# Data storage
report_data = []

for folder in folders:
    # Get the latest report file
    cmd_latest_file = f"mc find {MINIO_ALIAS}/{MINIO_BUCKET}/{folder} --name '*.html' | sort -r | head -1"
    latest_file = subprocess.getoutput(cmd_latest_file).strip()

    if not latest_file:
        print(f"❌ Failed to fetch latest report for {folder}.")
        continue
    
    match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", latest_file)
    
    if match:
        T, P, S, F, I, KI = match.groups()
        report_data.append([folder, T, P, S, F, I, KI])
    else:
        print(f"❌ Failed to extract details from {latest_file}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Module", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with only the latest reports from each folder.")
