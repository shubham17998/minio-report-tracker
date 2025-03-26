import os
import re
import pandas as pd
import subprocess

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Fetch all folders inside the automation bucket
cmd_list_folders = f"mc ls {MINIO_ALIAS}/{MINIO_BUCKET} --recursive --folders"
output_folders = subprocess.getoutput(cmd_list_folders)
folders = [line.split()[-1].strip().replace(f"{MINIO_ALIAS}/{MINIO_BUCKET}/", "") for line in output_folders.split("\n") if line.strip()]

report_data = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"
    
    # Fetch latest report for the current folder
    cmd_list_files = f"mc find {MINIO_ALIAS}/{folder_path} --name '*.html' --exec 'stat -c "%Y %n" {}' | sort -nr | head -1"
    output_file = subprocess.getoutput(cmd_list_files).strip()
    
    if output_file:
        file_parts = output_file.split(" ", 1)
        if len(file_parts) < 2:
            continue
        file_name = file_parts[1]

        if "full-report" not in file_name:
            print(f"❌ Skipping {file_name} (not a full-report)")
            continue

        match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
        if match:
            T, P, S, F, I, KI = match.groups()
            report_data.append([folder, T, P, S, F, I, KI])
        else:
            print(f"❌ Failed to extract details from {file_name}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Folder", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with the latest full-report data from each folder.")
