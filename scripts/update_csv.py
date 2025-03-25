import os
import subprocess
import pandas as pd
from datetime import date

# Config
mc_alias = "myminio"
bucket = os.environ["MINIO_BUCKET"]
prefix = os.environ.get("MINIO_PREFIX", "")

# Ensure the script always finds the correct CSV file path
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # Go up one level
csv_path = os.path.join(base_dir, "spreadsheet", "reports.csv")

today = date.today().isoformat()

# Ensure the spreadsheet directory exists
os.makedirs(os.path.dirname(csv_path), exist_ok=True)

# Read existing CSV or create a new DataFrame
if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
else:
    df = pd.DataFrame(columns=["Filename", "T", "P", "S", "F", "Date"])

# Get list of existing filenames
existing_files = set(df["Filename"].values)

# List objects in MinIO bucket using mc
try:
    result = subprocess.run(
        ["mc", "ls", f"{mc_alias}/{bucket}/{prefix}"],
        capture_output=True,
        text=True,
        check=True
    )

    # Process each line of the output
    for line in result.stdout.splitlines():
        parts = line.split()  # Split based on spaces
        filename = parts[-1]  # The last column contains the filename

        if filename.startswith("T-") and filename.endswith(".csv") and filename not in existing_files:
            meta = filename.replace(".csv", "").split("_")
            data = dict(item.split("-") for item in meta if "-" in item)

            row = {
                "Filename": filename,
                "T": data.get("T", ""),
                "P": data.get("P", ""),
                "S": data.get("S", ""),
                "F": data.get("F", ""),
                "Date": today
            }
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)

    # Save the updated CSV
    df.to_csv(csv_path, index=False)
    print(f"✅ Updated {csv_path} with latest report data.")

except subprocess.CalledProcessError as e:
    print(f"❌ Error listing MinIO files: {e.stderr}")
