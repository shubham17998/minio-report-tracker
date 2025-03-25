import os
import re
import pandas as pd
import subprocess

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation/auth"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# List files from MinIO
cmd = f"mc ls {MINIO_ALIAS} {MINIO_BUCKET}"
output = subprocess.getoutput(cmd)
files = [line.split()[-1] for line in output.split("\n") if line]

# Data storage
report_data = []

for file_name in files:
    if "full-report" not in file_name:
        print(f"❌ Skipping {file_name} (not a full-report)")
        continue
    
    match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
    
    if match:
        T, P, S, F, I, KI = match.groups()
        report_data.append([file_name, T, P, S, F, I, KI])
    else:
        print(f"❌ Failed to extract details from {file_name}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Filename", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with latest full-report data.")
