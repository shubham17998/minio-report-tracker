import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# ✅ Multiple MinIO aliases
MINIO_ALIASES = ["qa-java21", "dev3"]
MINIO_BUCKET = "apitestrig"
columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]

def extract_date_from_filename(filename, fallback_ts):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    # Fallback to timestamp (from MinIO metadata)
    return datetime.strptime(fallback_ts.split("T")[0], "%Y-%m-%d")

def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")

for MINIO_ALIAS in MINIO_ALIASES:
    all_data_by_date = {}
    csv_filename = "reports.csv"  # default fallback
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

    print(f"\U0001F4C1 Folders found in {MINIO_ALIAS}: {folders}")

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
            timestamp = info.get("@timestamp", "1970-01-01T00:00:00Z")

            # ✅ Ignore error-report, allow report and full-report
            if "error-report" in fn:
                continue
            if not ("report" in fn or "full-report" in fn):
                continue

            # ✅ Match both formats, with optional I and KI
            m = re.search(r"report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)(?:_I-(\d+))?(?:_KI-(\d+))?", fn)
            if not m:
                continue

            T, P, S, F = m.group(1), m.group(2), m.group(3), m.group(4)
            I = m.group(5) if m.group(5) else "0"
            KI = m.group(6) if m.group(6) else "0"

            date_obj = extract_date_from_filename(fn, timestamp)
            mod = folder

            if folder == "masterdata":
                lang = re.search(r"masterdata-([a-z]{3})", fn)
                mod = f"{folder}-{lang.group(1)}" if lang else folder

            # Extract identifier for CSV file naming
            if not found_identifier:
                match = re.search(r"mosip-api-internal\\.([a-z0-9\-]+)-auth", fn)
                if match:
                    csv_filename = f"{match.group(1)}.csv"
                    found_identifier = True

            date_key = format_date_str(date_obj)
            if date_key not in all_data_by_date:
                all_data_by_date[date_key] = []

            # Avoid duplicates per module per date
            if not any(row[1] == mod for row in all_data_by_date[date_key]):
                all_data_by_date[date_key].append([date_key, mod, T, P, S, F, I, KI])

    # Step 2: Pick 2 latest dates
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
