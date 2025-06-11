import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "apitestrig"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get all folders in the automation bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip()]
print(f"📁 Folders found: {folders}")

report_data_1 = []
report_data_2 = []
report_dates_1 = []
report_dates_2 = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    head_n = 6 if folder == "masterdata" else 1
    cmd1 = (
        f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
        f"| grep 'full-report' | sort -r "
        f"| head -{head_n}"
    )
    lines1 = subprocess.getoutput(cmd1).splitlines()

    if folder == "masterdata":
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
            f"| grep 'full-report' | sort -r "
            f"| tail -n +7 | head -6"
        )
    else:
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
            f"| grep 'full-report' | sort -r "
            f"| head -2 | tail -1"
        )
    lines2 = subprocess.getoutput(cmd2).splitlines()

    # Process both report sets
    for lines, target_list, date_list in ((lines1, report_data_1, report_dates_1), (lines2, report_data_2, report_dates_2)):
        for line in lines:
            info = json.loads(line)
            fn = info["key"]

            # Extract date (YYYY-MM-DD → 11-June-2025)
            date_match = re.search(r"(\d{4}-\d{2}-\d{2})", fn)
            if date_match:
                date_str = date_match.group(1)
                formatted_date = datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%B-%Y")
            else:
                formatted_date = ""

            m = re.search(
                r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", fn
            )
            if not m:
                continue
            T, P, S, F, I, KI = m.groups()

            if folder == "masterdata":
                lang_match = re.search(r"masterdata-([a-z]{3})", fn)
                lang = lang_match.group(1) if lang_match else "unk"
                mod = f"{folder}-{lang}"
            else:
                mod = folder

            target_list.append([mod, T, P, S, F, I, KI])
            date_list.append(formatted_date)

# Create DataFrames
cols = ["Module", "T", "P", "S", "F", "I", "KI"]
df1 = pd.DataFrame(report_data_1, columns=cols)
df2 = pd.DataFrame(report_data_2, columns=cols)

# Add spacing columns
df1[""] = ""
df1[" "] = ""
df2.columns = df2.columns  # keep as is

# Merge side by side
df_wide = pd.concat([df1, df2], axis=1, ignore_index=False)

# Create top row with dates (fill only first column of each table block)
date_row = []
date1 = report_dates_1[0] if report_dates_1 else ""
date2 = report_dates_2[0] if report_dates_2 else ""
date_row += [date1] + [""] * (len(cols) - 1)
date_row += ["", ""]  # spacing
date_row += [date2] + [""] * (len(cols) - 1)

# Final DataFrame with date row at the top
df_final = pd.concat([pd.DataFrame([date_row], columns=df_wide.columns), df_wide], ignore_index=True)

# Save
df_final.to_csv(csv_path, index=False)
print(f"✅ Written wide-format CSV with date header to {csv_path}")
