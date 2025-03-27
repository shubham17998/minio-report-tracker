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

# Filter only "full-report" files
full_reports = [f for f in files if "full-report" in f]

if not full_reports:
    print("❌ No full-report files found.")
    exit(1)

# Sort files by modification time (latest first)
full_reports.sort(key=lambda f: f.split("/")[-1], reverse=True)
latest_report = full_reports[0]  # Pick the latest report

# Extract folder name
parts = latest_report.split("/")
folder_name = parts[2] if len(parts) > 2 else "unknown"

# Extract report details
match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", latest_report)

if match:
    T, P, S, F, I, KI = match.groups()
    report_data = [[folder_name, T, P, S, F, I, KI]]
else:
    print(f"❌ Failed to extract details from {latest_report}")
    exit(1)

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Filename", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with the latest full-report data.")
