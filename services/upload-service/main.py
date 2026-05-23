import json
import logging
import os
import pathlib
import re
from contextlib import asynccontextmanager

import boto3
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from kafka import KafkaProducer
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ALLOWED_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"}
MAX_SIZE_BYTES = 2 * 1024 * 1024 * 1024

producer, s3 = None, None


def safe_filename(name: str) -> str:
    name = pathlib.PurePosixPath(name).name
    name = re.sub(r"[^\w\-.]", "_", name)
    return name or "upload"


class SizeCheckingWrapper:
    def __init__(self, fileobj, max_bytes: int):
        self._f = fileobj
        self._max = max_bytes
        self._total = 0

    def read(self, size: int = -1) -> bytes:
        chunk = self._f.read(size)
        self._total += len(chunk)
        if self._total > self._max:
            raise IOError("File exceeds maximum allowed size")
        return chunk


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, s3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info("Services initialized")
    yield
    producer.close()
    logger.info("Services shut down")


RAW_BUCKET = os.getenv("S3_RAW_BUCKET")

limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="Upload Service", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/health")
def health():
    return {"status": "OK!"}


@app.post("/upload")
@limiter.limit("5/minute")
async def upload_video(request: Request, file: UploadFile = File(...)):
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file type '{ext}'. Allowed: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    clean_name = safe_filename(file.filename or "upload")
    s3_key = f"raw/{clean_name}"

    try:
        wrapper = SizeCheckingWrapper(file.file, MAX_SIZE_BYTES)
        s3.upload_fileobj(wrapper, RAW_BUCKET, s3_key)
    except IOError:
        raise HTTPException(status_code=413, detail="File too large")
    except Exception:
        logger.exception("S3 upload failed")
        raise HTTPException(status_code=500, detail="Failed to upload file to storage")

    try:
        producer.send(
            topic="video.uploaded",
            value={"filename": clean_name, "s3_key": s3_key, "bucket": RAW_BUCKET},
        )
        producer.flush()
    except Exception:
        logger.exception("Kafka publish failed")
        raise HTTPException(
            status_code=500,
            detail="File uploaded but failed to queue for transcoding",
        )

    return {
        "filename": clean_name,
        "s3_key": s3_key,
        "message": "Uploaded and queued for transcoding!",
    }
