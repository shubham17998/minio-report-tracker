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

# Get current date
today = datetime.today().strftime('%Y-%m-%d')

# Get all folders in the automation bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)

folders = []
for line in output.strip().split("\n"):
    try:
        obj = json.loads(line)
        if "key" in obj:
            folders.append(obj["key"].strip("/"))
    except json.JSONDecodeError:
        continue

# Prepare today's report data
report_data = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    if folder == "masterdata":
        # Get top 6 full-report files for masterdata
        cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -6"
        file_output = subprocess.getoutput(cmd_list_files)

        for line in file_output.strip().split("\n"):
            if not line.strip():
                continue
            try:
                file_info = json.loads(line)
                file_name = file_info["key"]
            except json.JSONDecodeError:
                continue

            match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
            lang_match = re.search(r"masterdata-([a-z]+)", file_name)

            if match and lang_match:
                T, P, S, F, I, KI = match.groups()
                lang = lang_match.group(1)
                module = f"masterdata-{lang}"
                report_data.append([today, module, T, P, S, F, I, KI])
            else:
                print(f"❌ Failed to extract details from {file_name}")
    else:
        # Only get latest full-report
        cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -1"
        file_output = subprocess.getoutput(cmd_list_files)

        if not file_output.strip():
            print(f"⚠️ No full-report found in {folder_path}, skipping.")
            continue

        try:
            file_info = json.loads(file_output)
            file_name = file_info["key"]
        except json.JSONDecodeError:
            continue

        match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)

        if match:
            T, P, S, F, I, KI = match.groups()
            report_data.append([today, folder, T, P, S, F, I, KI])
        else:
            print(f"❌ Failed to extract details from {file_name}")

# Create today's DataFrame
df_today = pd.DataFrame(report_data, columns=["Date", "Module", "T", "P", "S", "F", "I", "KI"])

# Create directory if needed
os.makedirs(os.path.dirname(csv_path), exist_ok=True)

# Merge with existing CSV if exists
if os.path.exists(csv_path):
    df_old = pd.read_csv(csv_path)
    df_combined = pd.concat([df_today, df_old], ignore_index=True)
    df_combined.to_csv(csv_path, index=False)
else:
    df_today.to_csv(csv_path, index=False)

print(f"✅ Updated {csv_path} with {len(df_today)} entries for {today}.")
