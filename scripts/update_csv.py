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

# Get all folders from bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.splitlines() if line.strip()]
print(f"📁 Folders found: {folders}")

# Helper functions
def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    return None

def format_date(dt):
    return dt.strftime("%d-%B-%Y")

# Storage for reports
data_by_date = defaultdict(list)

# Collect report data
for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"
    head_n = 6 if folder == "masterdata" else 1

    # FIRST PASS
    cmd1 = (
        f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
        f"| grep 'full-report' | sort -r "
    )
    lines1 = subprocess.getoutput(cmd1).splitlines()

    # Parse and group by date
    date_file_map = defaultdict(list)
    for line in lines1:
        info = json.loads(line)
        fn = info["key"]
        date = extract_date_from_filename(fn)
        if not date:
            continue
        date_file_map[date].append(fn)

    # Keep top 3 most recent dates
    for date in sorted(date_file_map.keys(), reverse=True)[:3]:
        for fn in sorted(date_file_map[date], reverse=True):
            m = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", fn)
            if not m:
                continue
            T, P, S, F, I, KI = m.groups()

            if folder == "masterdata":
                lang = re.search(r"masterdata-([a-z]{3})", fn)
                mod = f"{folder}-{lang.group(1)}" if lang else folder
            else:
                mod = folder

            data_by_date[date].append([mod, T, P, S, F, I, KI])

# Create CSV output
columns = ["Module", "T", "P", "S", "F", "I", "KI"]
dfs = []
date_labels = []

for date in sorted(data_by_date.keys(), reverse=True):
    df = pd.DataFrame(data_by_date[date], columns=columns)
    df[" "] = ""
    df["  "] = ""
    dfs.append(df)
    date_labels.append(format_date(date))

# Merge DataFrames side-by-side
df_combined = pd.concat(dfs, axis=1)

# Write with date row and headers
with open(csv_path, "w") as f:
    date_row = []
    for label in date_labels:
        date_row += [label] + [""] * (len(columns) - 1) + ["", ""]
    f.write(",".join(date_row) + "\n")

# Combine column headers for each date block
header_row = []
for _ in date_labels:
    header_row += columns + ["", ""]

with open(csv_path, "a") as f:
    f.write(",".join(header_row) + "\n")

# Append data
df_combined.to_csv(csv_path, mode="a", index=False)
print(f"✅ Saved reports for top 3 recent dates to: {csv_path}")
