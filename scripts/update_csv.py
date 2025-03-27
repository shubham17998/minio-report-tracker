import os
import re
import pandas as pd
import subprocess

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get all folders inside the automation bucket
cmd_list_folders = f"mc ls {MINIO_ALIAS}/{MINIO_BUCKET} --recursive | awk '{{print $5}}' | cut -d'/' -f2 | sort -u"
folder_list_output = subprocess.getoutput(cmd_list_folders)
folders = [folder.strip() for folder in folder_list_output.split("\n") if folder.strip()]

# Data storage
report_data = []

for folder in folders:
    # Find the latest HTML report in each folder
    cmd_list_files = f"mc find {MINIO_ALIAS}/{MINIO_BUCKET}/{folder} --name '*.html' | xargs ls -t | head -1"
    latest_file = subprocess.getoutput(cmd_list_files).strip()

    if not latest_file:
        print(f"❌ No reports found for {folder}")
        continue

    file_name = os.path.basename(latest_file)

    # Extract language code for masterdata reports
    if "masterdata" in file_name:
        lang_match = re.search(r"masterdata-([a-z]{3})", file_name)  # Extracts 'kan', 'hin', etc.
        module_name = f"masterdata-{lang_match.group(1).capitalize()}" if lang_match else "masterdata"
    else:
        module_name = folder  # Other folders use their folder name as module

    match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)

    if match:
        T, P, S, F, I, KI = match.groups()
        report_data.append([module_name, T, P, S, F, I, KI])
    else:
        print(f"❌ Failed to extract details from {file_name}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Module", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with masterdata reports in separate rows.")
