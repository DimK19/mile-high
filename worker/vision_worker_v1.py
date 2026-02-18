import os
import json
import time
import uuid
import tempfile
import cv2
import numpy as np
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
from botocore.client import Config
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from ultralytics import YOLO

from prometheus_client import (
    start_http_server,
    Counter,
    Histogram,
    Gauge,
)

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
SPEED_ALERT_THRESHOLD_KMH = float(os.getenv("SPEED_ALERT_THRESHOLD_KMH", "130.0"))

# Speed Gate Configuration (Pixels & Meters)
LINE_A = 282  # Top Line
LINE_B = 370  # Bottom Line
GATE_DISTANCE_METERS = 17.0


# ----------------------------
# GLOBAL MODEL LOADING
# ----------------------------
print(f"[{WORKER_ID}] Loading YOLOv8 model...")
try:
    # Loads the model copied into the Docker image
    model = YOLO('yolov8s.pt')
    print(f"[{WORKER_ID}] Model loaded successfully.")
except Exception as e:
    print(f"[{WORKER_ID}] CRITICAL WARNING: Model failed to load: {e}")
    # Fallback if file missing
    exit(1)


# ----------------------------
# Prometheus metrics
# ----------------------------

chunks_started_total = Counter(
    "cv_chunks_started_total",
    "Number of video chunks whose processing has started",
    ["worker_id"],
)

chunks_completed_total = Counter(
    "cv_chunks_completed_total",
    "Number of video chunks successfully processed end-to-end",
    ["worker_id"],
)

chunks_failed_total = Counter(
    "cv_chunks_failed_total",
    "Number of video chunks that failed during processing",
    ["worker_id", "reason"],
)

cv_chunk_latency_seconds = Histogram(
    "cv_chunk_processing_seconds",
    "Time spent in the CV processing step for each chunk (seconds)",
    ["worker_id"],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
)

chunks_in_progress = Gauge(
    "cv_chunks_in_progress",
    "Number of chunks currently being processed by this worker",
    ["worker_id"],
)

vehicle_summaries_emitted_total = Counter(
    "cv_vehicle_summaries_emitted_total",
    "Number of vehicle track summary events emitted",
    ["worker_id"],
)

speed_alerts_emitted_total = Counter(
    "cv_speed_alerts_emitted_total",
    "Number of speed alerts emitted",
    ["worker_id"],
)


# ----------------------------
# Data models
# ----------------------------
@dataclass(frozen=True)
class ChunkEvent:
    chunk_id: str
    source_video_id: str
    chunk_index: int
    chunk_start_sec: float
    chunk_end_sec: float
    chunk_duration_sec: float
    uri: str
    created_utc: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# External clients
# ----------------------------

def build_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def build_kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_CONSUMER_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        # --- PROJECT REQUIREMENTS FIX ---
        # 1. Give the worker 15 minutes to process a SINGLE chunk
        max_poll_interval_ms=900000,
        # 2. Force the worker to pick up ONLY ONE chunk at a time
        max_poll_records=1,
        # 3. Keep the connection alive
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
        # --------------------------------
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

def build_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
    )


def parse_chunk_event(raw: dict) -> ChunkEvent:
    required = ["chunk_id", "source_video_id", "chunk_index", "chunk_start_sec", "chunk_end_sec", "chunk_duration_sec", "uri", "created_utc"]
    for k in required:
        if k not in raw:
            raise ValueError(f"Missing required field '{k}' in chunk event")

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
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Unsupported URI scheme '{parsed.scheme}'")
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    s3.download_file(bucket, key, local_path)


# ----------------------------
# REAL COMPUTER VISION LOGIC (Speed Gate)
# ----------------------------

def process_video_chunk_cv(ev: ChunkEvent, video_path: str) -> list[dict]:
    """
    REAL CV PROCESSING:
    1. Opens the video file using OpenCV.
    2. Runs YOLOv8 tracking on frames.
    3. Calculates speed using the Virtual Speed Gate (Lines at Y=417, Y=553).
    4. Aggregates results into summaries.
    """

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video file: {video_path}")

    # Tracking state: {track_id: {"y": float, "timestamp": float}}
    track_last_pos = {}

    # Crossing times: {track_id: {"t_A": float, "t_B": float}}
    gate_crossings = {}

    # Final results: {track_id: {data...}}
    track_summaries = {}

    while True:
        success, frame = cap.read()
        if not success:
            break

        # Get current time in seconds
        current_time = cap.get(cv2.CAP_PROP_POS_MSEC) / 1000.0

        # Run YOLO Tracking (with lower confidence to catch edge cars)
        results = model.track(frame, persist=True, conf=0.1, verbose=False, device=0)

        if results[0].boxes.id is not None:
            boxes = results[0].boxes.xywh.cpu().tolist()
            track_ids = results[0].boxes.id.int().cpu().tolist()
            cls_indices = results[0].boxes.cls.int().cpu().tolist()

            for box, track_id, cls_idx in zip(boxes, track_ids, cls_indices):
                class_name = model.names[cls_idx]

                if class_name not in ['car', 'truck', 'bus', 'motorcycle']:
                    continue

                x, y, w, h = box

                # --- SPEED GATE LOGIC ---
                if track_id in track_last_pos:
                    prev_y = track_last_pos[track_id]["y"]

                    # Check Line A (417) Crossing
                    if (prev_y < LINE_A <= y) or (y < LINE_A <= prev_y):
                        if track_id not in gate_crossings: gate_crossings[track_id] = {}
                        gate_crossings[track_id]["t_A"] = current_time

                    # Check Line B (553) Crossing
                    if (prev_y < LINE_B <= y) or (y < LINE_B <= prev_y):
                        if track_id not in gate_crossings: gate_crossings[track_id] = {}
                        gate_crossings[track_id]["t_B"] = current_time

                    # Calculate Speed if both crossed
                    if track_id in gate_crossings and "t_A" in gate_crossings[track_id] and "t_B" in gate_crossings[track_id]:
                        t1 = gate_crossings[track_id]["t_A"]
                        t2 = gate_crossings[track_id]["t_B"]
                        duration = abs(t2 - t1)

                        if duration > 0.1: # Ignore noise
                            speed_mps = GATE_DISTANCE_METERS / duration
                            speed_kmh = speed_mps * 3.6
                            direction = "inbound" if t1 < t2 else "outbound"

                            # Store Result
                            track_summaries[track_id] = {
                                "track_id": f"{ev.chunk_id}_{track_id}",
                                "vehicle_type": class_name,
                                "direction": direction,
                                "avg_speed_kmh": round(speed_kmh, 2),
                                "max_speed_kmh": round(speed_kmh, 2)
                            }
                            # Clear crossing to prevent duplicates
                            del gate_crossings[track_id]

                # Update history
                track_last_pos[track_id] = {"y": y, "timestamp": current_time}

    cap.release()

    # Convert dictionary to list
    output_events = []
    for t_id, data in track_summaries.items():
        output_events.append({
            "event_type": "vehicle_track_summary",
            "event_id": str(uuid.uuid4()),
            "timestamp_utc": utc_now_iso(),
            "chunk_id": ev.chunk_id,
            "source_video_id": ev.source_video_id,
            "chunk_index": ev.chunk_index,
            "track_id": data["track_id"],
            "vehicle_type": data["vehicle_type"],
            "direction": data["direction"],
            "avg_speed_kmh": data["avg_speed_kmh"],
            "max_speed_kmh": data["max_speed_kmh"],
        })

    print(f"[{WORKER_ID}] Finished chunk {ev.chunk_index}: Detected {len(output_events)} vehicles.")
    return output_events


def build_alert_event(summary: dict) -> dict:
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
    start_http_server(METRICS_PORT)
    print(f"[{WORKER_ID}] Metrics server running on :{METRICS_PORT}/metrics")

    s3 = build_s3_client()
    consumer = build_kafka_consumer()
    producer = build_kafka_producer()

    print(f"[{WORKER_ID}] Consuming from '{KAFKA_TOPIC_IN}'. Ready for Real CV.")

    for msg in consumer:
        try:
            ev = parse_chunk_event(msg.value)
            chunks_started_total.labels(worker_id=WORKER_ID).inc()
            chunks_in_progress.labels(worker_id=WORKER_ID).inc()

            with tempfile.TemporaryDirectory() as tmpdir:
                local_mp4 = os.path.join(tmpdir, "chunk.mp4")
                download_from_uri(s3, ev.uri, local_mp4)

                start_t = time.perf_counter()

                # Run CV Logic
                summaries = process_video_chunk_cv(ev, local_mp4)

                elapsed = time.perf_counter() - start_t
                cv_chunk_latency_seconds.labels(worker_id=WORKER_ID).observe(elapsed)

                for summary in summaries:
                    # 1. Send Analytics Data
                    producer.send(KAFKA_TOPIC_OUT, key=ev.chunk_id, value=summary)
                    vehicle_summaries_emitted_total.labels(worker_id=WORKER_ID).inc()

                    # 2. Send Alert if Speeding
                    if float(summary["max_speed_kmh"]) > SPEED_ALERT_THRESHOLD_KMH:
                        print(f"[{WORKER_ID}] 🚨 ALERT! Vehicle {summary['track_id']} doing {summary['max_speed_kmh']} km/h")
                        alert = build_alert_event(summary)
                        producer.send(KAFKA_TOPIC_ALERTS, key=ev.chunk_id, value=alert)
                        speed_alerts_emitted_total.labels(worker_id=WORKER_ID).inc()

                producer.flush()

            consumer.commit()
            chunks_completed_total.labels(worker_id=WORKER_ID).inc()

        except Exception as e:
            chunks_failed_total.labels(worker_id=WORKER_ID, reason=type(e).__name__).inc()
            print(f"[{WORKER_ID}] ERROR processing message: {e}")
            time.sleep(1.0)

        finally:
            chunks_in_progress.labels(worker_id=WORKER_ID).dec()


if __name__ == "__main__":
    main()
