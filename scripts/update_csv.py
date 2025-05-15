import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get all folders in the automation bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip() and "key" in json.loads(line)]

report_data = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    # For 'masterdata', fetch top 6 full-report files
    if folder == "masterdata":
        cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -6"
        file_output = subprocess.getoutput(cmd_list_files)
        file_lines = [line.strip() for line in file_output.strip().split("\n") if line.strip()]
    else:
        cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -2"
        file_output = subprocess.getoutput(cmd_list_files)
        file_lines = [line.strip() for line in file_output.strip().split("\n") if line.strip()]

    for line in file_lines:
        try:
            file_info = json.loads(line)
            file_name = file_info["key"]
            mod_time = file_info["mtime"][:10]  # Extract date from mtime (e.g., "2025-05-15T10:23:45Z")

            match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
            if match:
                T, P, S, F, I, KI = match.groups()

                if folder == "masterdata":
                    lang_match = re.search(r'masterdata-([a-z]{3})', file_name)
                    lang = lang_match.group(1) if lang_match else "unknown"
                    module_name = f"{folder}-{lang}"
                else:
                    module_name = folder

                report_data.append([mod_time, module_name, T, P, S, F, I, KI])
            else:
                print(f"❌ Failed to extract details from {file_name}")
        except Exception as e:
            print(f"❌ Error processing line: {line}\n{e}")

# Create DataFrame
df_new = pd.DataFrame(report_data, columns=["Date", "Module", "T", "P", "S", "F", "I", "KI"])

# Load existing data if file exists
if os.path.exists(csv_path):
    df_existing = pd.read_csv(csv_path)
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
else:
    df_combined = df_new

# Keep only the latest 2 dates
df_combined["Date"] = pd.to_datetime(df_combined["Date"])
latest_dates = sorted(df_combined["Date"].unique())[-2:]
df_filtered = df_combined[df_combined["Date"].isin(latest_dates)].sort_values(by=["Date", "Module"])

# Save to CSV
os.makedirs(os.path.dirname(csv_path), exist_ok=True)
df_filtered.to_csv(csv_path, index=False)

print(f"✅ Saved recent 2 days data to {csv_path}")
