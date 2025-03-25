import os
import re
import subprocess
import pandas as pd
from datetime import date

# Configuration
mc_alias = "myminio"
bucket = os.environ.get("MINIO_BUCKET", "automation")  # Default to "automation"
prefix = os.environ.get("MINIO_PREFIX", "auth")  # Default to "auth"
csv_path = "../spreadsheet/reports.csv"  # Ensure correct path
today = date.today().isoformat()

# Regex pattern to extract metadata values
filename_pattern = re.compile(r"T-(\d+)_S-(\d+)_F-(\d+)_I-(\d+)_KI-(\d+)")

# Read existing CSV or create a new DataFrame
if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
else:
    df = pd.DataFrame(columns=["Filename", "T", "S", "F", "I", "KI", "Date"])

# Get list of existing filenames to avoid duplicates
existing_files = set(df["Filename"].values)

# List HTML report files in MinIO using `mc find`
try:
    result = subprocess.run(
        ["mc", "find", f"{mc_alias}/{bucket}/{prefix}", "--name", "*.html"],
        capture_output=True,
        text=True,
        check=True
    )

    # Process each file
    for file_path in result.stdout.splitlines():
        filename = file_path.strip().split("/")[-1]  # Extract just the filename

        if filename not in existing_files:
            match = filename_pattern.search(filename)
            if match:
                row = {
                    "Filename": filename,
                    "T": match.group(1),
                    "S": match.group(2),
                    "F": match.group(3),
                    "I": match.group(4),
                    "KI": match.group(5),
                    "Date": today
                }
                df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    # Save the updated CSV
    df.to_csv(csv_path, index=False)
    print(f"✅ Updated {csv_path} with latest report data.")

except subprocess.CalledProcessError as e:
    print(f"❌ Error listing MinIO files: {e.stderr}")
