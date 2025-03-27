import os
import re
import json
import subprocess
import pandas as pd
from collections import defaultdict

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"

# CSV file path
csv_path = "../spreadsheet/reports.csv"

# Get all folders in the automation bucket
cmd_list_folders = f"mc ls --json {MINIO_ALIAS}/{MINIO_BUCKET}/"
output = subprocess.getoutput(cmd_list_folders)

# Extract folder names, ensuring masterdata is included
folders = [json.loads(line)["key"].strip("/") for line in output.split("\n") if line.strip()]

if "masterdata" not in folders:
    folders.append("masterdata")  # Ensure masterdata is processed

report_data = []

for folder in folders:
    folder_path = f"{MINIO_BUCKET}/{folder}"

    # List all full-report HTML files
    cmd_list_files = f"mc find {MINIO_ALIAS}/{folder_path} --name '*full-report*.html'"
    file_output = subprocess.getoutput(cmd_list_files).strip().split("\n")

    if not file_output or file_output == [""]:
        print(f"⚠️ No full-report found in {folder_path}, skipping.")
        continue

    if folder == "masterdata":
        # Dictionary to track latest report per language
        latest_reports = defaultdict(lambda: ("", 0))  # { lang: (filename, timestamp) }

        for file_path in file_output:
            # Extract language from filename (assuming it's before the date)
            match_lang = re.search(r"masterdata-(\w+)-\d{4}-\d{2}-\d{2}", file_path)
            if not match_lang:
                print(f"⚠️ Skipping {file_path} (No language found)")
                continue

            lang = match_lang.group(1)  # Extracted language
            timestamp = int(os.path.getmtime(file_path))  # Get last modified time

            # Keep only the latest file per language
            if timestamp > latest_reports[lang][1]:
                latest_reports[lang] = (file_path, timestamp)

        # Process only the latest reports per language
        for lang, (latest_file, _) in latest_reports.items():
            match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", latest_file)
            if match:
                T, P, S, F, I, KI = match.groups()
                report_data.append([f"masterdata-{lang}", T, P, S, F, I, KI])  
            else:
                print(f"❌ Failed to extract details from {latest_file}")

    else:
        # Process other folders as before
        latest_file = max(file_output, key=lambda f: os.path.getmtime(f))  # Get latest file

        match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", latest_file)
        if match:
            T, P, S, F, I, KI = match.groups()
            report_data.append([folder, T, P, S, F, I, KI])  
        else:
            print(f"❌ Failed to extract details from {latest_file}")

# Create DataFrame
df = pd.DataFrame(report_data, columns=["Filename", "T", "P", "S", "F", "I", "KI"])

# Save to CSV
if not os.path.exists(os.path.dirname(csv_path)):
    os.makedirs(os.path.dirname(csv_path))

df.to_csv(csv_path, index=False)
print(f"✅ Updated {csv_path} with latest reports per folder and per language in masterdata.")
