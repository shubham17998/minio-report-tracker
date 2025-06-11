import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# ✅ Multiple MinIO aliases
MINIO_ALIASES = ["qa-java21", "dev3", "collab"]
MINIO_BUCKETS = ["apitestrig", "automation"]
columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]


def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    match_alt = re.search(r"-(\d{13})-report", filename)
    if match_alt:
        return datetime.fromtimestamp(int(match_alt.group(1)) / 1000)
    return None


def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")


for MINIO_ALIAS in MINIO_ALIASES:
    all_data_by_date = {}
    csv_filename = f"{MINIO_ALIAS}.csv"  # use alias directly for file name

    # Try buckets in priority
    bucket_found = False
    for MINIO_BUCKET in MINIO_BUCKETS:
        # Get all folders from bucket
        cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
        output = subprocess.getoutput(cmd_list_folders)
        folders = []
        for line in output.splitlines():
            try:
                folders.append(json.loads(line)["key"].strip("/"))
            except json.JSONDecodeError:
                continue

        if folders:
            bucket_found = True
            print(f"\n📁 Folders found in {MINIO_ALIAS}/{MINIO_BUCKET}: {folders}")
            break  # Use the first bucket that works

    if not bucket_found:
        print(f"⚠️ No valid bucket found for {MINIO_ALIAS}, skipping...\n")
        continue

    # Step 1: Group latest reports by unique date
    for folder in folders:
        folder_path = f"{MINIO_BUCKET}/{folder}"
        cmd = f"mc ls --json {MINIO_ALIAS}/{folder_path}/"
        lines = subprocess.getoutput(cmd).splitlines()

        for line in lines:
            try:
                info = json.loads(line)
            except json.JSONDecodeError:
                continue

            fn = info["key"]
            if not re.search(r"(?<!error-)report_T-", fn) and not "full-report_T-" in fn:
                continue  # Only process full-report or normal report, skip error-report

            date_obj = extract_date_from_filename(fn)
            if not date_obj:
                continue

            m = re.search(r"_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", fn)
            if not m:
                continue
            T, P, S, F, I, KI = m.groups()

            if folder == "masterdata":
                lang = re.search(r"masterdata-([a-z]{3})", fn)
                mod = f"{folder}-{lang.group(1)}" if lang else folder
            else:
                mod = folder

            date_key = format_date_str(date_obj)
            if date_key not in all_data_by_date:
                all_data_by_date[date_key] = []

            # Avoid duplicates per module per date
            if not any(row[1] == mod for row in all_data_by_date[date_key]):
                all_data_by_date[date_key].append([date_key, mod, T, P, S, F, I, KI])

    # Step 2: Pick 2 latest dates
    sorted_dates = sorted(all_data_by_date.keys(), key=lambda x: datetime.strptime(x, "%d-%B-%Y"), reverse=True)
    latest_dates = sorted_dates[:2]

    dfs = []
    for date in latest_dates:
        df = pd.DataFrame(all_data_by_date[date], columns=columns)
        dfs.append(df)

    if not dfs:
        print(f"⚠️ No report data found for alias: {MINIO_ALIAS}")
        continue

    # Step 3: Pad and align side-by-side
    max_len = max(len(df) for df in dfs)
    for i in range(len(dfs)):
        dfs[i] = dfs[i].reindex(range(max_len))
        if i < len(dfs) - 1:
            dfs[i][""] = ""
            dfs[i][" "] = ""

    # Step 4: Merge and save
    os.makedirs("../spreadsheet", exist_ok=True)
    csv_path = f"../spreadsheet/{csv_filename}"
    final_df = pd.concat(dfs, axis=1)
    final_df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved to: {csv_path}")
