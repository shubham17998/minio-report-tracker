import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# ✅ Multiple MinIO aliases and bucket fallback options
MINIO_CONFIG = {
    "qa-java21": ["apitestrig"],
    "dev3": ["apitestrig"],
    "collab": ["automation"]
}
columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]

def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")

for MINIO_ALIAS, BUCKETS in MINIO_CONFIG.items():
    for MINIO_BUCKET in BUCKETS:
        all_data_by_date = {}
        csv_filename = f"{MINIO_ALIAS}.csv"

        # Get all folders from bucket
        cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
        output = subprocess.getoutput(cmd_list_folders)
        folders = []
        for line in output.splitlines():
            try:
                folders.append(json.loads(line)["key"].strip("/"))
            except (json.JSONDecodeError, KeyError):
                continue

        print(f"📁 Folders found in {MINIO_ALIAS}/{MINIO_BUCKET}: {folders}")

        # Step 1: Group latest reports by date
        for folder in folders:
            folder_path = f"{MINIO_BUCKET}/{folder}"
            cmd = f"mc ls --json {MINIO_ALIAS}/{folder_path}/"
            lines = subprocess.getoutput(cmd).splitlines()

            for line in lines:
                try:
                    info = json.loads(line)
                    fn = info["key"]
                except (json.JSONDecodeError, KeyError):
                    continue

                # Ignore unwanted report types
                if not re.search(r"(full-report|report)_T-\d+", fn) or "error-report" in fn:
                    continue

                try:
                    date_obj = datetime.strptime(info["lastModified"][:10], "%Y-%m-%d")
                except Exception as e:
                    print(f"⚠️ Could not parse lastModified for {fn}: {e}")
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

                if not any(row[1] == mod for row in all_data_by_date[date_key]):
                    all_data_by_date[date_key].append([date_key, mod, T, P, S, F, I, KI])

        # Step 2: Pick 3 latest dates
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
