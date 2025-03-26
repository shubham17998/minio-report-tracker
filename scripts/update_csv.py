import os
import re
import pandas as pd
import subprocess
from datetime import datetime

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation/auth"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get today's date in the expected filename format
today_date = datetime.now().strftime("%Y-%m-%d")

# List only `.html` files from MinIO
cmd = f"mc find {MINIO_ALIAS}/{MINIO_BUCKET} --name '*.html'"
output = subprocess.getoutput(cmd)
files = [line.strip() for line in output.split("\n") if line.strip()]

# Filter only "full-report" files
report_files = [f for f in files if "full-report" in f]

# Sort files by extracted date and timestamp
sorted_files = sorted(report_files, key=lambda x: re.search(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})", x).group(1), reverse=True) if report_files else []

# Select today's report if available, otherwise the latest one
latest_report = None
for file_name in sorted_files:
    if today_date in file_name:
        latest_report = file_name
        break
if not latest_report and sorted_files:
    latest_report = sorted_files[0]  # Fallback to the latest file

if not latest_report:
    print("❌ No valid full-report found in MinIO.")
    exit(1)

# Extract details from the latest report filename
match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", latest_report)

if match:
    T, P, S, F, I, KI = match.groups()
    report_data = [[latest_report, T, P, S, F, I, KI]]
else:
    print(f"❌ Failed to extract details from {latest_report}")
    exit(1)

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Filename", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
os.makedirs(os.path.dirname(csv_path), exist_ok=True)
df.to_csv(csv_path, index=False)

print(f"✅ Updated {csv_path} with the latest full-report: {latest_report}")
