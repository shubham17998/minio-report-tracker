import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime
from collections import defaultdict

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "apitestrig"
csv_path = "../spreadsheet/reports.csv"

# Get all folders from the bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)

folders = []
for line in output.splitlines():
    try:
        obj = json.loads(line)
        if obj.get("key", "").endswith("/"):
            folders.append(obj["key"].strip("/"))
    except json.JSONDecodeError:
        continue

print(f"📁 Folders found: {folders}")

report_data_by_date = defaultdict(list)

def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d").strftime("%d-%B-%Y")
    return None

# Parse reports and group by unique date
for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"
    cmd = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report'"
    lines = subprocess.getoutput(cmd).splitlines()

    for line in lines:
        try:
            info = json.loads(line)
            fn = info["key"]
        except json.JSONDecodeError:
            continue

        date_val = extract_date_from_filename(fn)
        if not date_val:
            continue

        m = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", fn)
        if not m:
            continue
        T, P, S, F, I, KI = m.groups()

        if folder == "masterdata":
            lang = re.search(r"masterdata-([a-z]{3})", fn)
            mod = f"{folder}-{lang.group(1)}" if lang else folder
        else:
            mod = folder

        report_data_by_date[date_val].append([date_val, mod, T, P, S, F, I, KI])

# Sort and select latest two unique date blocks
sorted_dates = sorted(report_data_by_date.keys(), key=lambda x: datetime.strptime(x, "%d-%B-%Y"), reverse=True)
if len(sorted_dates) < 2:
    print("❌ Not enough unique date entries found.")
    exit()

date_str_1 = sorted_dates[0]
date_str_2 = sorted_dates[1]

columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]
df1 = pd.DataFrame(report_data_by_date[date_str_1], columns=columns)
df2 = pd.DataFrame(report_data_by_date[date_str_2], columns=columns)

# Add empty columns for visual spacing
df1[""] = ""
df1[" "] = ""

# Combine both date blocks side by side
df_combined = pd.concat([df1, df2], axis=1)

# Save final CSV
df_combined.to_csv(csv_path, index=False)
print(f"✅ CSV written successfully to {csv_path}")
