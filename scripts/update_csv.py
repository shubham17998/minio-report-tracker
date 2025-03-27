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

# Extract folder names, ensuring masterdata is included
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip()]

if "masterdata" not in folders:
    folders.append("masterdata")  # Ensure masterdata is processed

report_data = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    # List all full-report HTML files in the folder and get the latest one
    cmd_list_files = f"mc find {MINIO_ALIAS}/{folder_path} --name '*full-report*.html' --exec 'stat --format \"%Y %n\" {{}}' | sort -nr | head -1"
    file_output = subprocess.getoutput(cmd_list_files)

    if not file_output.strip():
        print(f"⚠️ No full-report found in {folder_path}, skipping.")
        continue

    try:
        _, file_name = file_output.split(" ", 1)  # Extract filename from output
    except ValueError:
        print(f"❌ Failed to extract file name from {file_output}")
        continue

    # Extract details from the file name
    match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
    
    if match:
        T, P, S, F, I, KI = match.groups()
        report_data.append([folder, T, P, S, F, I, KI])  # Store folder name
    else:
        print(f"❌ Failed to extract details from {file_name}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Filename", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with latest full-report data from all folders.")
