import os
import re
import json
import subprocess
import pandas as pd
from datetime import datetime

# MinIO alias and bucket
MINIO_ALIAS = "myminio"
MINIO_BUCKET = "automation"
csv_path = "../spreadsheet/reports.csv"

def run_mc_ls_json(path):
    cmd = f"mc ls --json {path}"
    raw = subprocess.getoutput(cmd)
    lines = [line.strip() for line in raw.strip().split("\n") if line.strip()]
    return [json.loads(line) for line in lines if line.strip()]

# Step 1: Get all folders
print("🔍 Listing folders...")
all_entries = run_mc_ls_json(f"{MINIO_ALIAS}/{MINIO_BUCKET}/")
folders = [entry["key"].strip("/") for entry in all_entries if entry.get("type") == "folder"]

print(f"📁 Found folders: {folders}")

report_data = []

# Step 2: Collect report data
for folder in folders:
    folder_path = f"{MINIO_ALIAS}/{MINIO_BUCKET}/{folder}/"
    print(f"📦 Checking folder: {folder_path}")

    file_entries = run_mc_ls_json(folder_path)
    full_reports = [entry for entry in file_entries if "full-report" in entry.get("key", "")]

    # Sort by mtime descending and pick latest (or top 6 for masterdata)
    full_reports = sorted(full_reports, key=lambda x: x.get("mtime", ""), reverse=True)

    if folder == "masterdata":
        full_reports = full_reports[:6]
    else:
        full_reports = full_reports[:2]

    if not full_reports:
        print(f"⚠️ No full-report found in {folder_path}, skipping.")
        continue

    for file_info in full_reports:
        file_name = file_info["key"]
        mtime = file_info.get("mtime", "")[:10]  # e.g., '2025-05-15'

        if not mtime:
            print(f"⚠️ Skipping {file_name} due to missing mtime.")
            continue

        match = re.search(r"full-report_T-(\d+)_P-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)", file_name)
        if match:
            T, P, S, F, I, KI = match.groups()

            if folder == "masterdata":
                lang_match = re.search(r'masterdata-([a-z]{3})', file_name)
                lang = lang_match.group(1) if lang_match else "unknown"
                module_name = f"{folder}-{lang}"
            else:
                module_name = folder

            print(f"✅ Found report: {file_name} [Date: {mtime}]")
            report_data.append([mtime, module_name, T, P, S, F, I, KI])
        else:
            print(f"❌ Failed to extract metrics from: {file_name}")

# Step 3: Save to CSV (if data exists)
if not report_data:
    print("🚫 No report data collected. Exiting without writing CSV.")
    exit(0)

df_new = pd.DataFrame(report_data, columns=["Date", "Module", "T", "P", "S", "F", "I", "KI"])

# Load existing data
if os.path.exists(csv_path):
    df_existing = pd.read_csv(csv_path)
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
else:
    df_combined = df_new

# Filter to latest 2 dates
df_combined["Date"] = pd.to_datetime(df_combined["Date"])
latest_dates = sorted(df_combined["Date"].unique())[-2:]
df_filtered = df_combined[df_combined["Date"].isin(latest_dates)]

# Save final CSV
os.makedirs(os.path.dirname(csv_path), exist_ok=True)
df_filtered.to_csv(csv_path, index=False)
print(f"✅ CSV saved with {len(df_filtered)} rows to {csv_path}")
