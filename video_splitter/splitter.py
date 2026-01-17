import os
import json
import uuid
import time
import argparse
import subprocess
from datetime import datetime, timezone

import boto3
from botocore.client import Config
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio12345")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "chunks")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC_VIDEO_CHUNKS = os.getenv("KAFKA_TOPIC_VIDEO_CHUNKS", "video-chunks")

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def ffprobe_duration_seconds(video_path: str) -> float:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        video_path
    ]
    out = subprocess.check_output(cmd).decode().strip()
    return float(out)

def split_video(input_path: str, out_dir: str, chunk_seconds: int) -> list[str]:
    os.makedirs(out_dir, exist_ok=True)
    # Creates out_dir/chunk_000.mp4, chunk_001.mp4, ...
    out_pattern = os.path.join(out_dir, "chunk_%03d.mp4")
    cmd = [
        "ffmpeg", "-y",
        "-i", input_path,
        "-c", "copy",
        "-f", "segment",
        "-segment_time", str(chunk_seconds),
        "-reset_timestamps", "1",
        out_pattern
    ]
    subprocess.check_call(cmd)
    return sorted(
        os.path.join(out_dir, f) for f in os.listdir(out_dir)
        if f.endswith(".mp4") and f.startswith("chunk_")
    )

def upload_file_minio(local_path: str, object_key: str) -> str:
    s3 = s3_client()
    s3.upload_file(local_path, MINIO_BUCKET, object_key)
    return f"s3://{MINIO_BUCKET}/{object_key}"

def kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True, help="Path to input .mp4")
    p.add_argument("--chunk-seconds", type=int, default=120)
    p.add_argument("--out-dir", default="splitter/out")
    args = p.parse_args()

    source_video_id = str(uuid.uuid4())
    duration = ffprobe_duration_seconds(args.input)
    chunks = split_video(args.input, args.out_dir, args.chunk_seconds)

    prod = kafka_producer()

    for idx, chunk_path in enumerate(chunks):
        chunk_id = str(uuid.uuid4())
        object_key = f"{source_video_id}/chunk_{idx:03d}.mp4"
        uri = upload_file_minio(chunk_path, object_key)

        start_sec = idx * args.chunk_seconds
        end_sec = min((idx + 1) * args.chunk_seconds, duration)

        msg = {
            "event_type": "video_chunk_ready",
            "chunk_id": chunk_id,
            "source_video_id": source_video_id,
            "chunk_index": idx,
            "chunk_start_sec": start_sec,
            "chunk_end_sec": end_sec,
            "chunk_duration_sec": end_sec - start_sec,
            "uri": uri,
            "created_utc": datetime.now(timezone.utc).isoformat(),
        }

        prod.send(KAFKA_TOPIC_VIDEO_CHUNKS, key=chunk_id, value=msg)
        print(f"Published: {chunk_id} -> {uri}")
        time.sleep(0.05)

    prod.flush()
    print("Done.")

if __name__ == "__main__":
    main()
