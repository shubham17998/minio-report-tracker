import os
import re
import json
import subprocess
import pandas as pd

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

report_data_1 = []   # first table
report_data_2 = []   # second table

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    # ——— FIRST PASS ———
    head_n = 6 if folder == "masterdata" else 1
    cmd1 = (
        f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
        f"| grep 'full-report' | sort -r "
        f"| head -{head_n}"
    )
    lines1 = subprocess.getoutput(cmd1).splitlines()

    # ——— SECOND PASS ———
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

    # ——— PROCESS BOTH PASSES ———
    for lines, target_list in ((lines1, report_data_1), (lines2, report_data_2)):
        for line in lines:
            info = json.loads(line)
            fn = info["key"]
            m = re.search(
                r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)",
                fn
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

# Create DataFrames
cols = ["Module", "T", "P", "S", "F", "I", "KI"]
df1 = pd.DataFrame(report_data_1, columns=cols)
df2 = pd.DataFrame(report_data_2, columns=cols)

# Add spacing columns
df1[""] = ""
df1[" "] = ""

# Combine side by side
df_wide = pd.concat([df1, df2], axis=1, ignore_index=False)

# Write to CSV
df_wide.to_csv(csv_path, index=False)
print(f"✅ Written wide-format CSV with two tables side-by-side to {csv_path}")
