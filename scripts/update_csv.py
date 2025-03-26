import os
import re
import pandas as pd
import subprocess

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation/auth"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# List only `.html` files from MinIO
cmd = f"mc find {MINIO_ALIAS}/{MINIO_BUCKET} --name '*.html'"
output = subprocess.getoutput(cmd)
files = [line.strip() for line in output.split("\n") if line.strip()]

# Data storage
report_data = []

for file_path in files:
    if "full-report" not in file_path:
        print(f"❌ Skipping {file_path} (not a full-report)")
        continue

    # Extract folder name (assumes the structure: myminio/automation/{folder}/...)
    parts = file_path.split("/")
    folder_name = parts[2] if len(parts) > 2 else "unknown"

    # Extract report details
    match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_path)
    
    if match:
        T, P, S, F, I, KI = match.groups()
        report_data.append([folder_name, T, P, S, F, I, KI])
    else:
        print(f"❌ Failed to extract details from {file_path}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Filename", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with latest full-report data.")
