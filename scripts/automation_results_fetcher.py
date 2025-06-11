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

# Step 1: Get folders from bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.splitlines() if line.strip()]
print(f"📁 Folders found: {folders}")

report_data_1 = []
report_data_2 = []

date_str_1 = ""
date_str_2 = ""

def extract_date_from_filename(filename):
    match = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match:
        return datetime.strptime(match.group(1), "%Y-%m-%d").strftime("%d-%B-%Y")
    return ""

# Step 2: Process each folder
for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"
    head_n = 6 if folder == "masterdata" else 1

    # FIRST PASS
    cmd1 = (
        f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
        f"| grep 'full-report' | sort -r | head -{head_n}"
    )
    lines1 = subprocess.getoutput(cmd1).splitlines()

    # SECOND PASS
    if folder == "masterdata":
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
            f"| grep 'full-report' | sort -r | tail -n +7 | head -6"
        )
    else:
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
            f"| grep 'full-report' | sort -r | head -2 | tail -1"
        )
    lines2 = subprocess.getoutput(cmd2).splitlines()

    for lines, target_list, date_var in ((lines1, report_data_1, "date_str_1"), (lines2, report_data_2, "date_str_2")):
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

            target_list.append([mod, T, P, S, F, I, KI])

            if not globals()[date_var]:
                date_val = extract_date_from_filename(fn)
                if date_val:
                    globals()[date_var] = date_val

# Step 3: Create dataframes
columns = ["Module", "T", "P", "S", "F", "I", "KI"]
df1 = pd.DataFrame(report_data_1, columns=columns)
df2 = pd.DataFrame(report_data_2, columns=columns)

# Step 4: Add empty columns for spacing
df1[""] = ""
df1[" "] = ""

# Combine both tables side by side
df_combined = pd.concat([df1, df2], axis=1)

# Step 5: Build final output with date row on top
header_row = df_combined.columns.tolist()
date_row = [date_str_1] + [""] * (len(columns) - 1) + ["", ""] + [date_str_2] + [""] * (len(columns) - 1)

# Create final DataFrame with date row + header + data
final_df = pd.DataFrame([date_row], columns=header_row)
final_df = pd.concat([final_df, df_combined], ignore_index=True)

# Step 6: Save
final_df.to_csv(csv_path, index=False)
print(f"✅ Written final CSV with date row above headers to {csv_path}")
