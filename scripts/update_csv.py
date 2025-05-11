import os
import re
import json
import subprocess
import pandas as pd

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get all folders in the automation bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip()]

report_data = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    # List full-report files
if folder == "masterdata":
    cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -6"
    file_output = subprocess.getoutput(cmd_list_files)
    file_lines = [line.strip() for line in file_output.strip().split("\n") if line.strip()]
else:
    cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -1"
    file_output = subprocess.getoutput(cmd_list_files)
    file_lines = [file_output.strip()] if file_output.strip() else []

if not file_lines:
    print(f"⚠️ No full-report found in {folder_path}, skipping.")
    continue

for line in file_lines:
    try:
        file_info = json.loads(line)
        file_name = file_info["key"]

        # Extract details from the file name
        match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)

        if match:
            T, P, S, F, I, KI = match.groups()
            report_data.append([folder, T, P, S, F, I, KI])
        else:
            print(f"❌ Failed to extract details from {file_name}")
    except json.JSONDecodeError:
        print(f"❌ Failed to parse JSON line: {line}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Module", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with latest full-report data from all folders.")
