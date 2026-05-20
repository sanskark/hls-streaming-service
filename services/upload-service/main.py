import json

from fastapi import FastAPI, UploadFile, File
import boto3
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

app = FastAPI(title="Upload Service")


s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

RAW_BUCKET = os.getenv("S3_RAW_BUCKET")


@app.get("/health")
def health():
    return {"status": "OK!"}


@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    s3_key = f"raw/{file.filename}"

    # Upload to S3
    s3.upload_fileobj(file.file, RAW_BUCKET, s3_key)
    print(f"Uploaded to S3: {s3_key}")

    # Send Kafka event
    producer.send("video.uploaded", {
        "filename": file.filename,
        "s3_key": s3_key,
        "bucket": RAW_BUCKET
    })
    producer.flush()
    print(f"Kafka event sent for: {file.filename}")

    return {
        "filename": file.filename,
        "s3_key": s3_key,
        "message": "Uploaded and queued for transcoding!"
    }
