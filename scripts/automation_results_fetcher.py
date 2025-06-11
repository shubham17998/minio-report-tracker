import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "apitestrig"

# Output path
csv_path = "../spreadsheet/reports.csv"

# Get all folders
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip()]
print(f"📁 Folders found: {folders}")

report_data_1, report_data_2 = [], []
date_str_1, date_str_2 = "", ""

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"
    head_n = 6 if folder == "masterdata" else 1

    # First report set
    cmd1 = (
        f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -{head_n}"
    )
    lines1 = subprocess.getoutput(cmd1).splitlines()

    # Second report set
    if folder == "masterdata":
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | tail -n +7 | head -6"
        )
    else:
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -2 | tail -1"
        )
    lines2 = subprocess.getoutput(cmd2).splitlines()

    for lines, target_list, date_store in ((lines1, report_data_1, "date_str_1"), (lines2, report_data_2, "date_str_2")):
        for line in lines:
            info = json.loads(line)
            fn = info["key"]

            m = re.search(
                r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", fn
            )
            if not m:
                continue
            T, P, S, F, I, KI = m.groups()

            if folder == "masterdata":
                lang = re.search(r"masterdata-([a-z]{3})", fn)
                mod = f"{folder}-{lang.group(1)}" if lang else folder
            else:
                mod = folder

            target_list.append([mod, T, P, S, F, I, KI])

            # Extract date once for each block
            match_date = re.search(r"(\d{4}-\d{2}-\d{2})", fn)
            if match_date:
                formatted_date = datetime.strptime(match_date.group(1), "%Y-%m-%d").strftime("%d-%B-%Y")
                if date_store == "date_str_1":
                    date_str_1 = formatted_date
                else:
                    date_str_2 = formatted_date

# Create DataFrames
cols = ["Module", "T", "P", "S", "F", "I", "KI"]
df1 = pd.DataFrame(report_data_1, columns=cols)
df2 = pd.DataFrame(report_data_2, columns=cols)

# Add space columns between tables
spacer = [""] * len(df1)
df1[""] = spacer
df1[" "] = spacer

# Combine horizontally
df_final = pd.concat([df1, df2], axis=1)

# Add date row ABOVE the header
date_row = [date_str_1] + [""] * (len(cols) - 1) + ["", ""] + [date_str_2] + [""] * (len(cols) - 1)
df_final.columns = pd.MultiIndex.from_tuples([("", c) for c in df_final.columns])  # preserve headers

# Insert date as first row manually
df_final.columns = df_final.columns.droplevel(0)  # flatten header
df_final = pd.concat([pd.DataFrame([date_row], columns=df_final.columns), df_final], ignore_index=True)

# Save CSV
df_final.to_csv(csv_path, index=False)
print(f"✅ Final report with proper date row saved to {csv_path}")
