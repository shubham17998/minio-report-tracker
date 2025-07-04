import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# ✅ All MinIO aliases and buckets
MINIO_ALIASES = ["cellbox21"]
MINIO_BUCKETS = ["apitestrig", "automation", "dslreports"]
columns = ["Date", "Module", "T", "P", "S", "F", "I", "KI"]

def format_date_str(dt):
    return dt.strftime("%d-%B-%Y")

for alias in MINIO_ALIASES:
    csv_filename = f"{alias}.csv"
    all_data_by_date = {}

    bucket_used = None
    folders = []

    for bucket in MINIO_BUCKETS:
        folders.clear()

        # ✅ Special logic for 'dslreports' bucket
        if bucket == "dslreports":
            cmd_list_dsl_full = f"mc ls --json {alias}/dslreports/full/"
            output = subprocess.getoutput(cmd_list_dsl_full)

            found_dsl = False
            for line in output.splitlines():
                try:
                    info = json.loads(line)
                    fn = info["key"]
                    ts = info["lastModified"]
                except (json.JSONDecodeError, KeyError):
                    continue

                if not re.search(r"report_T-\d+_P-\d+_S-\d+_F-", fn):
                    continue

                m = re.search(r"report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)", fn)
                if not m:
                    continue

                T, P, S, F = m.groups()
                I, KI = "0", "0"
                date_obj = datetime.strptime(ts[:10], "%Y-%m-%d")
                date_key = format_date_str(date_obj)

                if date_key not in all_data_by_date:
                    all_data_by_date[date_key] = []

                if not any(row[1] == "dsl" for row in all_data_by_date[date_key]):
                    all_data_by_date[date_key].append([date_key, "dsl", T, P, S, F, I, KI])
                    found_dsl = True

            if found_dsl:
                print(f"📁 DSL reports found in {alias}/dslreports/full")
            else:
                print(f"⚠️ No DSL reports found in {alias}/dslreports/full")
            continue  # skip rest of logic for dslreports

        # ✅ Default logic for other buckets (apitestrig, automation)
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
        print(f"⚠️ No valid folders found in any default buckets for alias {alias}")
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

            if "error-report" in fn:
                continue

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

    # ✅ Step 2: Pick latest dates
    sorted_dates = sorted(all_data_by_date.keys(), key=lambda x: datetime.strptime(x, "%d-%B-%Y"), reverse=True)
    latest_dates = sorted_dates[:1]

    dfs = []
    for date in latest_dates:
        df = pd.DataFrame(all_data_by_date[date], columns=columns)
        dfs.append(df)

    if not dfs:
        print(f"⚠️ No report data extracted for alias: {alias}")
        continue

    # ✅ Step 3: Pad and align side-by-side
    max_len = max(len(df) for df in dfs)
    for i in range(len(dfs)):
        dfs[i] = dfs[i].reindex(range(max_len))
        if i < len(dfs) - 1:
            dfs[i][""] = ""
            dfs[i][" "] = ""

    # ✅ Step 4: Merge and save
    os.makedirs("../spreadsheet", exist_ok=True)
    csv_path = f"../spreadsheet/{csv_filename}"
    final_df = pd.concat(dfs, axis=1)
    final_df.to_csv(csv_path, index=False)
    print(f"✅ CSV saved to: {csv_path}")
