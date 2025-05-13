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

# Today's date
today = datetime.today().strftime('%Y-%m-%d')

# Get all folders in the automation bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip()]

report_data = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    if folder == "masterdata":
        # Get latest 6 full-report files in masterdata
        cmd_list_files_top6 = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -6"
        file_output_top6 = subprocess.getoutput(cmd_list_files_top6)

        for line in file_output_top6.strip().split("\n"):
            if not line.strip():
                continue
            file_info = json.loads(line)
            file_name = file_info["key"]

            # Extract language part
            lang_match = re.search(r"masterdata-([a-z]+)", file_name)
            lang = lang_match.group(1) if lang_match else "unknown"

            module_name = f"masterdata-{lang}"

            match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
            if match:
                T, P, S, F, I, KI = match.groups()
                report_data.append([module_name, T, P, S, F, I, KI, today])
            else:
                print(f"❌ Failed to extract details from {file_name}")
    else:
        # Get latest full-report file for other folders
        cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -1"
        file_output = subprocess.getoutput(cmd_list_files)

        if not file_output.strip():
            print(f"⚠️ No full-report found in {folder_path}, skipping.")
            continue

        file_info = json.loads(file_output)
        file_name = file_info["key"]

        match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
        if match:
            T, P, S, F, I, KI = match.groups()
            report_data.append([folder, T, P, S, F, I, KI, today])
        else:
            print(f"❌ Failed to extract details from {file_name}")

# Create today's DataFrame
new_df = pd.DataFrame(report_data, columns=["Module", "T", "P", "S", "F", "I", "KI", "Date"])

# Combine with existing CSV if it exists
if os.path.exists(csv_path):
    existing_df = pd.read_csv(csv_path)
    final_df = pd.concat([new_df, existing_df], ignore_index=True)
else:
    final_df = new_df

# Optional: Sort by Date (latest first), Module
final_df.sort_values(by=["Date", "Module"], ascending=[False, True], inplace=True)

# Save back to CSV
os.makedirs(os.path.dirname(csv_path), exist_ok=True)
final_df.to_csv(csv_path, index=False)

print(f"✅ Appended latest data to {csv_path} without losing previous records.")
