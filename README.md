# HLS Streaming Service

Upload a video, get back a working stream. That's it.

Built this to learn how video platforms actually work under the hood — turns out it's just FFmpeg, S3, and a message queue glued together.

---

## What it does

1. You upload a `.mp4` (or `.mkv`, `.mov`, `.webm`) via a REST endpoint
2. It lands in S3 and fires a Kafka event
3. A transcoding worker picks it up, runs FFmpeg, and produces HLS segments
4. Those segments get pushed to a second S3 bucket
5. You open a URL and watch it in the browser

No third-party video services. No managed transcoding. Just raw infrastructure.

---

## Stack

| Layer | Tech |
|---|---|
| API | FastAPI + Uvicorn |
| Message queue | Apache Kafka |
| Transcoding | FFmpeg (libx264 + AAC → HLS) |
| Storage | AWS S3 (two buckets — raw uploads + HLS output) |
| Player | hls.js |
| Container | Docker + Docker Compose |

---

## Services

```
upload-service      → accepts video uploads, pushes to S3, fires Kafka event
transcoding-service → consumes Kafka, downloads from S3, runs FFmpeg, uploads HLS
streaming-service   → proxies HLS content from S3, serves the player UI
```

---

## Running locally

**Prerequisites:** Docker Desktop, AWS account, two S3 buckets

1. Clone the repo

2. Create a `.env` file in the root:
```env
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_REGION=us-east-1
S3_RAW_BUCKET=your-raw-bucket-name
S3_HLS_BUCKET=your-hls-bucket-name
```

3. Start everything:
```bash
docker compose up --build
```

Kafka takes ~30 seconds to fully start. The dependent services will wait automatically.

---

## Usage

**Upload a video:**
```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@your-video.mp4"
```

**List available videos:**
```bash
curl http://localhost:8001/videos
```

**Open the player:**
```
http://localhost:8001/player/<video-id>
```

---

## Ports

| Service | Port |
|---|---|
| Upload | 8000 |
| Streaming + Player | 8001 |
| Kafka UI | 8090 |

---

## Notes

- FFmpeg is configured for VOD (`-hls_playlist_type vod`), 6-second segments, CRF 23
- The Kafka consumer timeout is set to 30 minutes to handle large files
- File size limit is 2GB enforced at the byte level (not just `Content-Length`)
- Rate limit on uploads: 5 per minute per IP
- Allowed formats: `.mp4` `.mov` `.mkv` `.webm` `.avi` `.m4v`

---

## Project structure

```
services/
├── upload-service/
│   ├── main.py
│   └── Dockerfile
├── transcoding-service/
│   ├── main.py
│   └── Dockerfile
└── streaming-service/
    ├── main.py
    ├── templates/
    │   └── player.html
    └── Dockerfile
docker-compose.yaml
requirements.txt
```
