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
folders = []
for line in output.splitlines():
    try:
        folders.append(json.loads(line)["key"].strip("/"))
    except json.JSONDecodeError:
        continue

print(f"📁 Folders found: {folders}")

report_data_1 = []
report_data_2 = []

def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    return None

def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")

columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"
    head_n = 6 if folder == "masterdata" else 1

    # Fetch report files
    cmd = (
        f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
        f"| grep 'full-report' | sort -r"
    )
    lines = subprocess.getoutput(cmd).splitlines()

    reports_by_key = {}

    for line in lines:
        try:
            info = json.loads(line)
        except json.JSONDecodeError:
            continue

        fn = info["key"]
        date_obj = extract_date_from_filename(fn)
        if not date_obj:
            continue

        m = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", fn)
        if not m:
            continue
        T, P, S, F, I, KI = m.groups()

        if folder == "masterdata":
            lang = re.search(r"masterdata-([a-z]{3})", fn)
            mod = f"{folder}-{lang.group(1)}" if lang else folder
            key = f"{format_date_str(date_obj)}|{mod}"
        else:
            mod = folder
            key = format_date_str(date_obj)

        if key not in reports_by_key:
            reports_by_key[key] = (date_obj, [format_date_str(date_obj), mod, T, P, S, F, I, KI])

    sorted_reports = sorted(reports_by_key.values(), key=lambda x: x[0], reverse=True)

    if folder == "masterdata":
        report_data_1.extend([x[1] for x in sorted_reports[:6]])
        report_data_2.extend([x[1] for x in sorted_reports[6:12]])
    else:
        if sorted_reports:
            report_data_1.append(sorted_reports[0][1])
        if len(sorted_reports) > 1:
            report_data_2.append(sorted_reports[1][1])

# Convert to DataFrames
df1 = pd.DataFrame(report_data_1, columns=columns)
df2 = pd.DataFrame(report_data_2, columns=columns)

# Add spacing columns
df1[""] = ""
df1[" "] = ""

# Combine both DataFrames side-by-side
df_combined = pd.concat([df1, df2], axis=1)

# Write to CSV
df_combined.to_csv(csv_path, index=False)
print(f"✅ CSV saved to: {csv_path}")
