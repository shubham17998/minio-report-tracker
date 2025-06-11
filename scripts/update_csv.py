import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# ✅ Multiple MinIO aliases and default bucket
MINIO_ALIASES = ["qa-java21", "dev3", "collab", "dev", "dev-int", "dev2", "released"]
MINIO_BUCKETS = ["apitestrig", "automation"]
columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]

def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")

for alias in MINIO_ALIASES:
    csv_filename = f"{alias}.csv"
    all_data_by_date = {}

    bucket_used = None
    folders = []
    for bucket in MINIO_BUCKETS:
        cmd_list_folders = f"mc ls --json {alias}/{bucket}/"
        output = subprocess.getoutput(cmd_list_folders)

        for line in output.splitlines():
            try:
                folders.append(json.loads(line)["key"].strip("/"))
            except (json.JSONDecodeError, KeyError):
                continue

        if folders:
            bucket_used = bucket
            print(f"📁 Folders found in {alias}/{bucket}: {folders}")
            break

    if not bucket_used:
        print(f"⚠️ No valid buckets found for alias {alias}")
        continue

    for folder in folders:
        folder_path = f"{bucket_used}/{folder}"
        cmd = f"mc ls --json {alias}/{folder_path}/ | grep 'report_T-' | sort -r"
        lines = subprocess.getoutput(cmd).splitlines()

        for line in lines:
            try:
                info = json.loads(line)
                fn = info["key"]
                ts = info["lastModified"]
                date_obj = datetime.strptime(ts[:10], "%Y-%m-%d")
            except (json.JSONDecodeError, KeyError, ValueError):
                continue

            # Filter out error-report files
            if "error-report" in fn:
                continue

            # Match both full-report_T and generic -report_T
            if not re.search(r"(full-)?report_T-\d+_P-\d+_S-\d+_F-", fn):
                continue

            m = re.search(r"report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)(?:_I-(\d+))?(?:_KI-(\d+))?", fn)
            if not m:
                continue

            T, P, S, F, I, KI = m.groups()
            I = I or "0"
            KI = KI or "0"

            if folder == "masterdata":
                lang = re.search(r"masterdata-([a-z]{3})", fn)
                mod = f"{folder}-{lang.group(1)}" if lang else folder
            else:
                mod = folder

            date_key = format_date_str(date_obj)
            if date_key not in all_data_by_date:
                all_data_by_date[date_key] = []

            if not any(row[1] == mod for row in all_data_by_date[date_key]):
                all_data_by_date[date_key].append([date_key, mod, T, P, S, F, I, KI])

    # Step 2: Pick latest 3 dates
    sorted_dates = sorted(all_data_by_date.keys(), key=lambda x: datetime.strptime(x, "%d-%B-%Y"), reverse=True)
    latest_dates = sorted_dates[:3]

    dfs = []
    for date in latest_dates:
        df = pd.DataFrame(all_data_by_date[date], columns=columns)
        dfs.append(df)

    if not dfs:
        print(f"⚠️ No report data extracted for alias: {alias}")
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
