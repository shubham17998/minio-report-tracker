import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# ✅ Aliases to check
MINIO_ALIASES = ["qa-java21", "dev3"]

# ✅ Bucket preference order
PREFERRED_BUCKETS = ["apitestrig", "automation"]

columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]

def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    return None

def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")

for MINIO_ALIAS in MINIO_ALIASES:
    # 🔍 Detect available bucket
    available_buckets_output = subprocess.getoutput(f"mc ls --json {MINIO_ALIAS}")
    found_bucket = None
    for line in available_buckets_output.splitlines():
        try:
            bucket_name = json.loads(line)["key"].strip("/")
            if bucket_name in PREFERRED_BUCKETS:
                found_bucket = bucket_name
                break
        except json.JSONDecodeError:
            continue

    if not found_bucket:
        print(f"⚠️ No preferred bucket found for alias {MINIO_ALIAS}, skipping.")
        continue

    MINIO_BUCKET = found_bucket
    print(f"✅ Using bucket '{MINIO_BUCKET}' for alias '{MINIO_ALIAS}'")

    all_data_by_date = {}
    csv_filename = "reports.csv"
    found_identifier = False

    # Get all folders from bucket
    cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
    output = subprocess.getoutput(cmd_list_folders)
    folders = []
    for line in output.splitlines():
        try:
            folders.append(json.loads(line)["key"].strip("/"))
        except json.JSONDecodeError:
            continue

    print(f"📁 Folders found in {MINIO_ALIAS}/{MINIO_BUCKET}: {folders}")

    for folder in folders:
        folder_path = f"{MINIO_BUCKET}/{folder}"
        cmd = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep -E 'full-report|[^-]report_T-' | grep -v 'error-report'"
        lines = subprocess.getoutput(cmd).splitlines()

        for line in lines:
            try:
                info = json.loads(line)
            except json.JSONDecodeError:
                continue

            fn = info["key"]
            date_obj = extract_date_from_filename(fn)
            if not date_obj:
                continue

            m = re.search(r"report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)(?:_I-(\d+))?(?:_KI-(\d+))?", fn)
            if not m:
                continue

            T, P, S, F, I, KI = (m.group(1), m.group(2), m.group(3), m.group(4), m.group(5) or "0", m.group(6) or "0")

            if folder == "masterdata":
                lang = re.search(r"masterdata-([a-z]{3})", fn)
                mod = f"{folder}-{lang.group(1)}" if lang else folder
            else:
                mod = folder

            if not found_identifier:
                match = re.search(r"mosip-api-internal\.([a-z0-9\-]+)-auth", fn)
                if match:
                    csv_filename = f"{match.group(1)}.csv"
                    found_identifier = True

            date_key = format_date_str(date_obj)
            if date_key not in all_data_by_date:
                all_data_by_date[date_key] = []

            if not any(row[1] == mod for row in all_data_by_date[date_key]):
                all_data_by_date[date_key].append([date_key, mod, T, P, S, F, I, KI])

    sorted_dates = sorted(all_data_by_date.keys(), key=lambda x: datetime.strptime(x, "%d-%B-%Y"), reverse=True)
    latest_dates = sorted_dates[:3]

    dfs = []
    for date in latest_dates:
        df = pd.DataFrame(all_data_by_date[date], columns=columns)
        dfs.append(df)

    if not dfs:
        print(f"⚠️ No data found for alias: {MINIO_ALIAS}")
        continue

    max_len = max(len(df) for df in dfs)
    for i in range(len(dfs)):
        dfs[i] = dfs[i].reindex(range(max_len))
        if i < len(dfs) - 1:
            dfs[i][""] = ""
            dfs[i][" "] = ""

    os.makedirs("../spreadsheet", exist_ok=True)
    csv_path = f"../spreadsheet/{csv_filename}"
    final_df = pd.concat(dfs, axis=1)
    final_df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved to: {csv_path}")
