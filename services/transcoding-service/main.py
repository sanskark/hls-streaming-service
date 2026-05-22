import json
import os
import subprocess
import tempfile
import uuid

import boto3
from kafka import KafkaConsumer


KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_BUCKET  = os.getenv("S3_RAW_BUCKET")
HLS_BUCKET  = os.getenv("S3_HLS_BUCKET")


s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

consumer = KafkaConsumer(
    'video.uploaded',
    bootstrap_servers=[KAFKA_SERVERS],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    enable_auto_commit=False,
    auto_offset_reset='earliest',
    group_id="transcoding-group",
)


def download_from_s3(bucket, key, local_path):
    print(f"Downloading s3://{bucket}/{key}")
    s3.download_file(bucket, key, local_path)
    print(f"Downloaded to {local_path}")


def transcode_to_hls(input_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    playlist = os.path.join(output_dir, "master.m3u8")

    cmd = [
        "ffmpeg",
        "-i", input_path,
        "-c:v", "libx264",
        "-c:a", "aac",
        "-preset", "medium",
        "-crf", "23",
        "-g", "48",
        "-keyint_min", "48",
        "-sc_threshold", "0",
        "-hls_time", "6",
        "-hls_playlist_type", "vod",
        "-hls_segment_filename", os.path.join(output_dir, "seg_%03d.ts"),
        "-f", "hls",
        playlist,
        "-y"
    ]

    print('Running ffmpeg...')
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )

    for line in process.stdout:
        print(line.strip())

    process.wait()
    if process.returncode != 0:
        raise RuntimeError(f"FFmpeg failed with exit code {process.returncode}")

    print("FFmpeg done")
    return output_dir


def upload_hls_to_s3(local_dir, video_id):
    print(f"Uploading HLS files to S3...")

    for fname in os.listdir(local_dir):
        local_path = os.path.join(local_dir, fname)
        s3_key = f"hls/{video_id}/{fname}"

        content_type = "application/vnd.apple.mpegurl" if fname.endswith(".m3u8") else "video/MP2T"

        s3.upload_file(
            local_path, HLS_BUCKET, s3_key,
            ExtraArgs={"ContentType": content_type}
        )
        print(f"Uploaded: {s3_key}")


def process(msg):
    stem = os.path.splitext(msg["filename"])[0]
    video_id = f"{stem}_{uuid.uuid4().hex[:8]}"
    s3_key = msg["s3_key"]

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.mp4")
        output_dir = os.path.join(tmpdir, "hls")

        # 1. Download from S3
        download_from_s3(RAW_BUCKET, s3_key, input_path)

        # 2. Transcode with FFmpeg
        transcode_to_hls(input_path, output_dir)

        # 3. Upload HLS to S3
        upload_hls_to_s3(output_dir, video_id)

    print(f"Done! HLS available at: hls/{video_id}/master.m3u8")


print("Transcoding service started...")

try:
    for message in consumer:
        msg = message.value
        print(f"Got message: {msg}")
        try:
            process(msg)
            consumer.commit()
            print("Offset committed")
        except Exception as e:
            print(f"Failed: {e}")
finally:
    consumer.close()