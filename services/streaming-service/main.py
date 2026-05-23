import logging
import os
from contextlib import asynccontextmanager

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

HLS_BUCKET = os.getenv("S3_HLS_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")

s3 = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global s3
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION,
    )
    logger.info("Streaming service started — bucket: %s", HLS_BUCKET)
    yield
    logger.info("Streaming service stopped")


app = FastAPI(title="HLS Streaming Service", lifespan=lifespan)
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/videos")
def list_videos():
    """Return all video IDs that have been transcoded and are ready to stream."""
    try:
        resp = s3.list_objects_v2(
            Bucket=HLS_BUCKET,
            Prefix="hls/",
            Delimiter="/",
        )
    except ClientError:
        logger.exception("Failed to list objects in S3")
        raise HTTPException(status_code=500, detail="Could not retrieve video list")

    prefixes = resp.get("CommonPrefixes", [])
    video_ids = [p["Prefix"].rstrip("/").split("/")[-1] for p in prefixes]
    return {"videos": video_ids}



@app.get("/stream/{video_id}/{filename:path}")
def stream_file(video_id: str, filename: str):
    """
    Proxy HLS content directly from S3.
    Playlists (.m3u8) are not cached; segments (.ts) are cached for 1 hour.
    """
    s3_key = f"hls/{video_id}/{filename}"

    try:
        obj = s3.get_object(Bucket=HLS_BUCKET, Key=s3_key)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in ("NoSuchKey", "404"):
            raise HTTPException(status_code=404, detail=f"Not found: {filename}")
        logger.exception("S3 error fetching key: %s", s3_key)
        raise HTTPException(status_code=502, detail="Failed to retrieve file from storage")

    is_playlist = filename.endswith(".m3u8")
    content_type = (
        "application/vnd.apple.mpegurl" if is_playlist else "video/MP2T"
    )
    cache_control = "no-cache, no-store" if is_playlist else "public, max-age=3600"

    def _iter():
        for chunk in obj["Body"].iter_chunks(chunk_size=64 * 1024):
            yield chunk

    return StreamingResponse(
        content=_iter(),
        media_type=content_type,
        headers={
            "Cache-Control": cache_control,
            "Access-Control-Allow-Origin": "*",
        },
    )


@app.get("/player/{video_id}", response_class=HTMLResponse)
def player(request: Request, video_id: str):
    """Serve the HLS web player for a given video ID."""
    stream_url = f"/stream/{video_id}/master.m3u8"
    return templates.TemplateResponse(
        request=request,
        name="player.html",
        context={
            "video_id": video_id,
            "stream_url": stream_url,
        },
    )
