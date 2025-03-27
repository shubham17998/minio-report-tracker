import os
import re
import json
import pandas as pd
import subprocess

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get list of folders dynamically from MinIO
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}"
output = subprocess.getoutput(cmd_list_folders)
folders = []

# Parse JSON output to extract folder names
for line in output.split("\n"):
    try:
        folder_info = json.loads(line)
        if folder_info.get("type") == "folder":
            folders.append(folder_info.get("key").strip("/"))  # Remove trailing slash
    except json.JSONDecodeError:
        print(f"❌ Failed to parse folder info: {line}")

# Data storage
report_data = []

for folder in folders:
    # List latest HTML file in each folder
    cmd_list_files = f"mc find {MINIO_ALIAS}/{MINIO_BUCKET}/{folder} --name '*.html' --exec 'stat --format \"%Y %n\" {{}}' | sort -nr | head -1"
    output = subprocess.getoutput(cmd_list_files).strip()

    if not output:
        print(f"❌ No reports found for {folder}")
        continue

    try:
        timestamp, file_path = output.split(" ", 1)  # Extract timestamp and filename
        file_name = os.path.basename(file_path)

        # Extract report details from filename
        match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
        if match:
            T, P, S, F, I, KI = match.groups()
            report_data.append([folder, file_name, T, P, S, F, I, KI])
        else:
            print(f"❌ Failed to extract details from {file_name}")
    except ValueError:
        print(f"❌ Failed to extract details from {output}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Module", "Filename", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with only the latest reports from each folder.")
