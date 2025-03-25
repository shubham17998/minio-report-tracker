import os
import boto3
import pandas as pd
from datetime import date
from botocore.config import Config

# Load environment variables
endpoint_url = os.getenv("MINIO_ENDPOINT")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket = os.getenv("MINIO_BUCKET")
prefix = os.getenv("MINIO_PREFIX", "")
csv_path = "spreadsheet/reports.csv"
today = date.today().isoformat()

# Ensure required environment variables are set
if not all([endpoint_url, access_key, secret_key, bucket]):
    raise ValueError("❌ Missing required MinIO environment variables!")

# Configure Boto3 client for MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

# Test MinIO connection
try:
    s3.head_bucket(Bucket=bucket)
    print(f"✅ Successfully connected to MinIO bucket: {bucket}")
except Exception as e:
    print(f"❌ MinIO connection failed: {e}")
    exit(1)

# Read existing CSV or create new DataFrame
if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
else:
    df = pd.DataFrame(columns=["Filename", "T", "P", "S", "F", "Date"])

# Get list of existing filenames
existing_files = set(df["Filename"].values)

# List objects in MinIO bucket
try:
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

except Exception as e:
    print(f"❌ Error accessing MinIO: {e}")
