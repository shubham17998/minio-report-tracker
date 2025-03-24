import os
import boto3
import pandas as pd
from datetime import date

# Config
endpoint_url = os.environ["MINIO_ENDPOINT"]
access_key = os.environ["MINIO_ACCESS_KEY"]
secret_key = os.environ["MINIO_SECRET_KEY"]
bucket = os.environ["MINIO_BUCKET"]
prefix = os.environ.get("MINIO_PREFIX", "")
csv_path = "spreadsheet/reports.csv"
today = date.today().isoformat()

# Connect to MinIO (S3-compatible)
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)

# Read existing CSV or create new DataFrame
if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
else:
    df = pd.DataFrame(columns=["Filename", "T", "P", "S", "F", "Date"])

# Get list of existing filenames
existing_files = set(df["Filename"].values)

# List objects in MinIO bucket
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

for obj in response.get("Contents", []):
    key = obj["Key"]
    filename = key.split("/")[-1]

    if filename.startswith("T-") and filename.endswith(".csv") and filename not in existing_files:
        parts = filename.replace(".csv", "").split("_")
        data = dict(part.split("-") for part in parts if "-" in part)
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
