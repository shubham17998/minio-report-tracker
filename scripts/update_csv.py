import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# ✅ Multiple MinIO aliases
MINIO_ALIASES = ["qa-java21", "dev3"]
# ✅ One or more possible buckets to try per alias
MINIO_BUCKETS = ["apitestrig", "automation"]

columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]


def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    return None


def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")


for MINIO_ALIAS in MINIO_ALIASES:
    all_data_by_date = {}
    csv_filename = f"{MINIO_ALIAS}.csv"  # Now based on alias
    bucket_found = False

    for bucket in MINIO_BUCKETS:
        # Get all folders from the current bucket
        cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{bucket}/"
        output = subprocess.getoutput(cmd_list_folders)
        folders = []
        for line in output.splitlines():
            try:
                json_obj = json.loads(line)
                if "key" in json_obj:
                    folders.append(json_obj["key"].strip("/"))
            except json.JSONDecodeError:
                continue

        if folders:
            print(f"📁 Folders found in {MINIO_ALIAS}/{bucket}: {folders}")
            bucket_found = True
            break  # ✅ Use the first bucket that has content

    if not bucket_found:
        print(f"⚠️ No valid bucket found for alias: {MINIO_ALIAS}")
        continue

    # Step 1: Group latest reports by unique date
    for folder in folders:
        folder_path = f"{bucket}/{folder}"
        cmd = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'report_T-' | sort -r"
        lines = subprocess.getoutput(cmd).splitlines()

        for line in lines:
            try:
                info = json.loads(line)
            except json.JSONDecodeError:
                continue

            fn = info.get("key", "")
            if not fn or "error-report" in fn:
                continue

            # Match files ending in either full-report_T-... or -report_T-...
            m = re.search(r"(?:full-)?report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)(?:_I-(\d+)_KI-(\d+))?", fn)
            if not m:
                continue

            T, P, S, F = m.group(1), m.group(2), m.group(3), m.group(4)
            I = m.group(5) if m.lastindex >= 5 and m.group(5) else "0"
            KI = m.group(6) if m.lastindex >= 6 and m.group(6) else "0"

            date_obj = extract_date_from_filename(fn)
            if not date_obj:
                continue

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

    # Step 2: Pick 2-3 latest dates
    sorted_dates = sorted(all_data_by_date.keys(), key=lambda x: datetime.strptime(x, "%d-%B-%Y"), reverse=True)
    latest_dates = sorted_dates[:3]

    dfs = []
    for date in latest_dates:
        df = pd.DataFrame(all_data_by_date[date], columns=columns)
        dfs.append(df)

    if not dfs:
        print(f"⚠️ No data found for alias: {MINIO_ALIAS}")
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
