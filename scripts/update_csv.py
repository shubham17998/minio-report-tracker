import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "apitestrig"
csv_path = "../spreadsheet/reports.csv"

# Get all folders from bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.splitlines() if line.strip()]
print(f"📁 Folders found: {folders}")

# Function to extract date from filename
def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    return None

# Function to extract metrics
def extract_metrics(folder, filename):
    m = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", filename)
    if not m:
        return None
    T, P, S, F, I, KI = m.groups()
    if folder == "masterdata":
        lang = re.search(r"masterdata-([a-z]{3})", filename)
        mod = f"{folder}-{lang.group(1)}" if lang else folder
    else:
        mod = folder
    return mod, T, P, S, F, I, KI

# Store latest reports for each module by date
latest_report_data_1 = {}
latest_report_data_2 = {}

# Collect and filter reports
for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"
    cmd_list = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report'"
    lines = subprocess.getoutput(cmd_list).splitlines()

    reports = []
    for line in lines:
        info = json.loads(line)
        fn = info["key"]
        dt = extract_date_from_filename(fn)
        if not dt:
            continue
        reports.append((dt, fn))

    # Sort by date descending, keep only latest per date
    reports.sort(reverse=True)
    seen_dates = set()
    selected = []
    for dt, fn in reports:
        dt_str = dt.strftime("%d-%B-%Y")
        if dt_str not in seen_dates:
            selected.append((dt_str, fn))
            seen_dates.add(dt_str)
        if len(selected) == 2:
            break

    # Assign reports
    for idx, (date_str, fn) in enumerate(selected):
        metrics = extract_metrics(folder, fn)
        if metrics:
            row = [date_str] + list(metrics)
            if idx == 0:
                latest_report_data_1[metrics[0]] = row
            else:
                latest_report_data_2[metrics[0]] = row

# Convert to DataFrames
columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]
df1 = pd.DataFrame(latest_report_data_1.values(), columns=columns)
df2 = pd.DataFrame(latest_report_data_2.values(), columns=columns)

# Combine with spacing columns
df1[""] = ""
df1[" "] = ""
df_combined = pd.concat([df1, df2], axis=1)

# Write to CSV
df_combined.to_csv(csv_path, index=False)
print(f"✅ CSV saved with latest reports per date to: {csv_path}")
