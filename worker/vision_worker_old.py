import os
import json
import time
import uuid
import random
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

from prometheus_client import (
    start_http_server,
    Counter,
    Histogram,
    Gauge,
)


#CV
# Add these new imports at the top
import cv2
import numpy as np
from ultralytics import YOLO

# ----------------------------
# Configuration loading
# ----------------------------

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
KAFKA_TOPIC_IN = os.getenv("KAFKA_TOPIC_IN", "video-chunks")
KAFKA_TOPIC_OUT = os.getenv("KAFKA_TOPIC_OUT", "traffic-events")
KAFKA_TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "alerts")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "vision-workers")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio12345")

WORKER_ID = os.getenv("WORKER_ID", "worker-1")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

SYNTH_VEHICLES_MIN = int(os.getenv("SYNTH_VEHICLES_MIN", "5"))
SYNTH_VEHICLES_MAX = int(os.getenv("SYNTH_VEHICLES_MAX", "20"))
SPEED_ALERT_THRESHOLD_KMH = float(os.getenv("SPEED_ALERT_THRESHOLD_KMH", "130"))



#-----------------------------
# CV
#-----------------------------

# Load the CPU-optimized YOLOv8 Nano model
# This handles both detection AND tracking
print(f"[{WORKER_ID}] Loading YOLOv8 Nano model...")
model = YOLO('yolov8n.pt')


# ----------------------------
# Prometheus metrics
# ----------------------------

# Counts how many chunk tasks we have started processing.
chunks_started_total = Counter(
    "cv_chunks_started_total",
    "Number of video chunks whose processing has started",
    ["worker_id"],
)

# Counts how many chunk tasks we have finished (success path).
chunks_completed_total = Counter(
    "cv_chunks_completed_total",
    "Number of video chunks successfully processed end-to-end",
    ["worker_id"],
)

# Counts failures. Label the reason for debugging.
chunks_failed_total = Counter(
    "cv_chunks_failed_total",
    "Number of video chunks that failed during processing",
    ["worker_id", "reason"],
)

# Histogram: latency for the CV step per chunk (this is your required latency histogram).
cv_chunk_latency_seconds = Histogram(
    "cv_chunk_processing_seconds",
    "Time spent in the CV processing step for each chunk (seconds)",
    ["worker_id"],
    # Buckets chosen for chunk-level processing: adjust later for real CV performance.
    buckets=(0.5, 1, 2, 5, 10, 20, 40, 60, 120, 240),
)

# Gauge: how many chunks are currently in progress on this worker.
chunks_in_progress = Gauge(
    "cv_chunks_in_progress",
    "Number of chunks currently being processed by this worker",
    ["worker_id"],
)

# Counts how many synthetic vehicle summaries we emitted (later: real tracks).
vehicle_summaries_emitted_total = Counter(
    "cv_vehicle_summaries_emitted_total",
    "Number of vehicle track summary events emitted",
    ["worker_id"],
)

# Counts how many alerts we emitted.
speed_alerts_emitted_total = Counter(
    "cv_speed_alerts_emitted_total",
    "Number of speed alerts emitted",
    ["worker_id"],
)


# ----------------------------
# Data models (simple + explicit)
# ----------------------------
#kafka message
@dataclass(frozen=True)
class ChunkEvent:
    """Represents one 'video_chunk_ready' message."""
    chunk_id: str
    source_video_id: str
    chunk_index: int
    chunk_start_sec: float
    chunk_end_sec: float
    chunk_duration_sec: float
    uri: str
    created_utc: str


def utc_now_iso() -> str:
    """Return current UTC timestamp as ISO string (stable and machine-readable)."""
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# External clients
# ----------------------------

def build_s3_client():
    """Create a boto3 S3 client pointed at MinIO (S3-compatible storage)."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def build_kafka_consumer() -> KafkaConsumer:
    """
    Consume chunk events.
    - group_id enables horizontal scaling (multiple workers share the load).
    - auto_offset_reset='earliest' makes dev/test reproducible.
    """
    return KafkaConsumer(
        KAFKA_TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,  # we commit only after successful processing
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def build_kafka_producer() -> KafkaProducer:
    """Produce results and alerts as JSON messages."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )


# ----------------------------
# Core helpers
# ----------------------------

def parse_chunk_event(raw: dict) -> ChunkEvent:
    """
    Validate/normalize the incoming message.
    Correctness reason:
    - Fail fast if a required field is missing, instead of silently producing wrong analytics.
    """
    required = [
        "chunk_id",
        "source_video_id",
        "chunk_index",
        "chunk_start_sec",
        "chunk_end_sec",
        "chunk_duration_sec",
        "uri",
        "created_utc",
    ]
    for k in required:
        if k not in raw:
            raise ValueError(f"Missing required field '{k}' in chunk event: keys={list(raw.keys())}")

    return ChunkEvent(
        chunk_id=str(raw["chunk_id"]),
        source_video_id=str(raw["source_video_id"]),
        chunk_index=int(raw["chunk_index"]),
        chunk_start_sec=float(raw["chunk_start_sec"]),
        chunk_end_sec=float(raw["chunk_end_sec"]),
        chunk_duration_sec=float(raw["chunk_duration_sec"]),
        uri=str(raw["uri"]),
        created_utc=str(raw["created_utc"]),
    )


def download_from_uri(s3, uri: str, local_path: str) -> None:
    """
    Download the chunk from MinIO based on an s3://bucket/key URI.
    Correctness reason:
    - Workers must retrieve the same bytes that were uploaded; URI pointer is the contract.
    """
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Unsupported URI scheme '{parsed.scheme}'. Expected s3://bucket/key")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    s3.download_file(bucket, key, local_path)


def synthetic_process_chunk(ev: ChunkEvent) -> list[dict]:
    """
    Fake 'CV + tracking + speed estimation' for now.
    Output is a list of vehicle_track_summary events.
    Correctness reason:
    - Lets us validate pipeline (queueing, scaling, metrics, analytics) before CV complexity.
    """
    # Decide how many vehicles we "saw" in this chunk.
    n = random.randint(SYNTH_VEHICLES_MIN, SYNTH_VEHICLES_MAX)

    out: list[dict] = []
    for i in range(n):
        # Synthetic track identity within chunk.
        track_id = f"t{ev.chunk_index}_{i}"

        # Pick a vehicle type (cars dominate).
        vehicle_type = "car" if random.random() < 0.8 else "truck"

        # Random direction.
        direction = "inbound" if random.random() < 0.5 else "outbound"

        # Generate plausible speeds; allow some high speeds for alerts.
        base = random.uniform(60, 120) if vehicle_type == "car" else random.uniform(50, 100)
        max_speed_kmh = base + random.uniform(0, 30)
        avg_speed_kmh = (base + max_speed_kmh) / 2.0

        out.append(
            {
                "event_type": "vehicle_track_summary",
                "event_id": str(uuid.uuid4()),
                "timestamp_utc": utc_now_iso(),
                "chunk_id": ev.chunk_id,
                "source_video_id": ev.source_video_id,
                "chunk_index": ev.chunk_index,
                "track_id": track_id,
                "vehicle_type": vehicle_type,
                "direction": direction,
                "avg_speed_kmh": avg_speed_kmh,
                "max_speed_kmh": max_speed_kmh,
            }
        )

    return out


def build_alert_event(summary: dict) -> dict:
    """Create a separate alert message from a summary (keeps alert stream lightweight)."""
    return {
        "event_type": "speed_alert",
        "event_id": str(uuid.uuid4()),
        "timestamp_utc": utc_now_iso(),
        "direction": summary["direction"],
        "chunk_id": summary["chunk_id"],
        "track_id": summary["track_id"],
        "vehicle_type": summary["vehicle_type"],
        "max_speed_kmh": summary["max_speed_kmh"],
        "threshold_kmh": SPEED_ALERT_THRESHOLD_KMH,
    }


# ----------------------------
# Main worker loop
# ----------------------------

def main() -> None:
    """
    Start metrics server and run an infinite consume-process-produce loop.
    Correctness reason:
    - This is the standard streaming worker pattern used in AKS/Kubernetes.
    """
    # Start Prometheus metrics endpoint on http://localhost:METRICS_PORT/metrics
    start_http_server(METRICS_PORT)
    print(f"[{WORKER_ID}] Metrics server running on :{METRICS_PORT}/metrics")

    s3 = build_s3_client()
    consumer = build_kafka_consumer()
    producer = build_kafka_producer()

    print(f"[{WORKER_ID}] Consuming from topic '{KAFKA_TOPIC_IN}' as group '{KAFKA_CONSUMER_GROUP}'")
    print(f"[{WORKER_ID}] Producing summaries to '{KAFKA_TOPIC_OUT}' and alerts to '{KAFKA_TOPIC_ALERTS}'")

    # Poll messages forever.
    for msg in consumer:
        # msg.key is the Kafka key; msg.value is the JSON dict.
        try:
            ev = parse_chunk_event(msg.value)
            chunks_started_total.labels(worker_id=WORKER_ID).inc()
            chunks_in_progress.labels(worker_id=WORKER_ID).inc()

            # Use a temp file so we don't keep chunks forever on disk.
            with tempfile.TemporaryDirectory() as tmpdir:
                local_mp4 = os.path.join(tmpdir, "chunk.mp4")

                # Download the chunk blob locally (same as worker downloading from Blob Storage).
                download_from_uri(s3, ev.uri, local_mp4)

                # Time ONLY the CV step (later: actual OpenCV pipeline), not Kafka IO.
                start_t = time.perf_counter()

                summaries = synthetic_process_chunk(ev)

                elapsed = time.perf_counter() - start_t
                cv_chunk_latency_seconds.labels(worker_id=WORKER_ID).observe(elapsed)

                # Publish summaries. Key by chunk_id for partition locality.
                for summary in summaries:
                    producer.send(KAFKA_TOPIC_OUT, key=ev.chunk_id, value=summary)
                    vehicle_summaries_emitted_total.labels(worker_id=WORKER_ID).inc()

                    # Emit alert if needed.
                    if float(summary["max_speed_kmh"]) > SPEED_ALERT_THRESHOLD_KMH:
                        alert = build_alert_event(summary)
                        producer.send(KAFKA_TOPIC_ALERTS, key=ev.chunk_id, value=alert)
                        speed_alerts_emitted_total.labels(worker_id=WORKER_ID).inc()

                # Ensure messages hit Kafka before we commit offset.
                producer.flush()

            # Commit the Kafka offset only after full success.
            consumer.commit()
            chunks_completed_total.labels(worker_id=WORKER_ID).inc()

        except Exception as e:
            # Do NOT commit; message will be retried (at-least-once processing).
            chunks_failed_total.labels(worker_id=WORKER_ID, reason=type(e).__name__).inc()
            print(f"[{WORKER_ID}] ERROR processing message: {e}")

            # Small sleep to avoid tight error loops.
            time.sleep(1.0)

        finally:
            chunks_in_progress.labels(worker_id=WORKER_ID).dec()


if __name__ == "__main__":
    main()
