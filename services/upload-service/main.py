from fastapi import FastAPI, UploadFile, File
import boto3
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

app = FastAPI(title="Upload Service")


s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

RAW_BUCKET = os.getenv("S3_RAW_BUCKET")


@app.get("/health")
def health():
    return {"status": "OK"}


@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    s3_key = f"raw/{file.filename}"

    s3.upload_fileobj(file.file, RAW_BUCKET, s3_key)
    print(f"Uploaded to S3: {s3_key}")

    print(f"Received file: {file.filename}")
    return {
        "filename": file.filename,
        "s3_key": s3_key,
        "message": "Uploaded to S3!"
    }
