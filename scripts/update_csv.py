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

def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d").strftime("%d-%B-%Y")
    return ""

def parse_reports(lines, folder):
    report_rows = []
    for line in lines:
        info = json.loads(line)
        fn = info["key"]

        m = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", fn)
        if not m:
            continue
        T, P, S, F, I, KI = m.groups()

        if folder == "masterdata":
            lang = re.search(r"masterdata-([a-z]{3})", fn)
            mod = f"{folder}-{lang.group(1)}" if lang else folder
        else:
            mod = folder

        date_str = extract_date_from_filename(fn)
        report_rows.append((date_str, [mod, T, P, S, F, I, KI]))
    return report_rows

all_rows = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    cmd_all = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r"
    lines_all = subprocess.getoutput(cmd_all).splitlines()

    if folder == "masterdata":
        # Group by date and collect max 6 per date
        date_grouped = {}
        for line in lines_all:
            info = json.loads(line)
            fn = info["key"]
            report_date = extract_date_from_filename(fn)
            if not report_date:
                continue
            if report_date not in date_grouped:
                date_grouped[report_date] = []
            if len(date_grouped[report_date]) < 6:
                date_grouped[report_date].append(line)

        for date in sorted(date_grouped.keys(), reverse=True)[:3]:
            all_rows.extend(parse_reports(date_grouped[date], folder))
    else:
        # Pick only 1 per recent 3 unique dates
        date_to_report = {}
        for line in lines_all:
            info = json.loads(line)
            fn = info["key"]
            report_date = extract_date_from_filename(fn)
            if report_date and report_date not in date_to_report:
                date_to_report[report_date] = line
            if len(date_to_report) >= 3:
                break
        all_rows.extend(parse_reports(date_to_report.values(), folder))

# Sort all rows by date (descending)
all_rows.sort(reverse=True, key=lambda x: datetime.strptime(x[0], "%d-%B-%Y"))

# Write to CSV
with open(csv_path, "w") as f:
    current_date = ""
    for date, row in all_rows:
        if date != current_date:
            current_date = date
            f.write(f"Date,Module,T,P,S,F,I,KI\n")
        f.write(f"{date},{','.join(row)}\n")

print(f"✅ Final CSV with grouped 3 recent unique dates saved at: {csv_path}")
