import json
import logging

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, UploadFile, File
import boto3
import os
from dotenv import load_dotenv
from kafka import KafkaProducer


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALLOWED_TYPES = {"video/mp4", "video/quicktime", "video/x-matroska", "video/webm"}
MAX_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global producer, s3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    logger.info("Services initialized")
    yield
    # Shutdown
    producer.close()
    logger.info("Services shut down")


RAW_BUCKET = os.getenv("S3_RAW_BUCKET")
app = FastAPI(title="Upload Service", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "OK!"}


@app.post("/upload")
async def upload_video(file: UploadFile = File(...)):
    if file.content_type not in ALLOWED_TYPES:
        raise HTTPException(status_code=400, detail="Invalid file type")
    if file.size > MAX_SIZE_BYTES:
        raise HTTPException(status_code=400, detail="File too large")
    s3_key = f"raw/{file.filename}"

    # Upload to S3
    try:
        s3.upload_fileobj(file.file, RAW_BUCKET, s3_key)
    except Exception as e:
        logger.exception("S3 upload failed")
        raise HTTPException(status_code=500, detail="Failed to upload file to storage")

    # Send Kafka event
    try:
        producer.send(topic="video.uploaded", value={
        "filename": file.filename,
        "s3_key": s3_key,
        "bucket": RAW_BUCKET
        })
        producer.flush()
    except Exception as e:
        logger.exception("Kafka publish failed")
        raise HTTPException(status_code=500, detail="File uploaded but failed to queue for transcoding")

    return {"filename": file.filename, "s3_key": s3_key, "message": "Uploaded and queued for transcoding!"}
