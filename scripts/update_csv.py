import os
import re
import json
import subprocess
from datetime import datetime

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get today's date
today = datetime.now().strftime("%Y-%m-%d")

# Get all folders in the automation bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip()]

report_lines = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    if folder == "masterdata":
        cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -6"
        file_output = subprocess.getoutput(cmd_list_files)
        file_lines = [line.strip() for line in file_output.strip().split("\n") if line.strip()]
    else:
        cmd_list_files = f"mc ls --json {MINIO_ALIAS}/{folder_path}/ | grep 'full-report' | sort -r | head -1"
        file_output = subprocess.getoutput(cmd_list_files)
        file_lines = [file_output.strip()] if file_output.strip() else []

    if not file_lines:
        print(f"⚠️ No full-report found in {folder_path}, skipping.")
        continue

    for line in file_lines:
        try:
            file_info = json.loads(line)
            file_name = file_info["key"]

            match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
            if match:
                T, P, S, F, I, KI = match.groups()

                if folder == "masterdata":
                    lang_match = re.search(r'masterdata-([a-z]{3})', file_name)
                    lang = lang_match.group(1) if lang_match else "unknown"
                    module_name = f"{folder}-{lang}"
                else:
                    module_name = folder

                report_lines.append(f"{module_name},{T},{P},{S},{F},{I},{KI}")
            else:
                print(f"❌ Failed to extract details from {file_name}")
        except json.JSONDecodeError:
            print(f"❌ Failed to parse JSON line: {line}")

# Prepare today's block
new_block = [today, "Module,T,P,S,F,I,KI"] + report_lines + [""]

# Read existing content (if any)
if os.path.exists(csv_path):
    with open(csv_path, "r") as f:
        old_content = f.read().splitlines()
else:
    old_content = []

# Combine and write back
final_content = new_block + old_content

if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

with open(csv_path, "w") as f:
    f.write("\n".join(final_content) + "\n")

print(f"✅ Appended structured section for {today} to {csv_path}")
