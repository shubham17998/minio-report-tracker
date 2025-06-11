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
    if folder == "masterdata":
        head_n = 6
    else:
        head_n = 1
    cmd1 = (
        f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
        f"| grep 'full-report' | sort -r "
        f"| head -{head_n}"
    )
    lines1 = subprocess.getoutput(cmd1).splitlines()

    # ——— SECOND PASS ———
    if folder == "masterdata":
        # skip first 6, then take next 6 → 7–12
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
            f"| grep 'full-report' | sort -r "
            f"| tail -n +7 | head -6"
        )
    else:
        # take the 2nd most recent → head 2, then skip first
        cmd2 = (
            f"mc ls --json {MINIO_ALIAS}/{folder_path}/ "
            f"| grep 'full-report' | sort -r "
            f"| head -2 | tail -1"
        )
    lines2 = subprocess.getoutput(cmd2).splitlines()

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
        T,P,S,F,I,KI = m.groups()
        if folder=="masterdata":
            lang = re.search(r"masterdata-([a-z]{3})", fn).group(1)
            mod = f"{folder}-{lang}"
        else:
            mod = folder
        target_list.append([mod, T, P, S, F, I, KI])

# Create DataFrame
cols = ["Module","T","P","S","F","I","KI"]
df1 = pd.DataFrame(report_data_1, columns=cols)
df2 = pd.DataFrame(report_data_2, columns=cols)

df1[""]  = ""
df1[""]  = ""

df_wide = pd.concat([df1, df2], axis=1, ignore_index=False)

df_wide.to_csv(csv_path, index=False)
print(f"✅ Written wide-format CSV with two tables side-by-side to {csv_path}")
