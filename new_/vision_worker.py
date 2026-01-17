import os
import json
import time
import uuid
import random
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone

# ========== AZURE IMPORTS (NEW) ==========
from azure.storage.blob import BlobServiceClient
from azure.eventhub import EventHubConsumerClient, EventHubProducerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore

from prometheus_client import start_http_server, Counter, Histogram, Gauge
from dotenv import load_dotenv

load_dotenv("azure.env")

# ========== CONFIGURATION ==========
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_CHUNKS = os.getenv("BLOB_CONTAINER_CHUNKS", "chunks")

EVENTHUB_CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING")
EVENTHUB_CHUNKS = os.getenv("EVENTHUB_CHUNKS", "video-chunks")
EVENTHUB_EVENTS = os.getenv("EVENTHUB_EVENTS", "traffic-events")
EVENTHUB_ALERTS = os.getenv("EVENTHUB_ALERTS", "alerts")
EVENTHUB_CONSUMER_GROUP = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")

WORKER_ID = os.getenv("WORKER_ID", "worker-1")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
SPEED_ALERT_THRESHOLD_KMH = float(os.getenv("SPEED_ALERT_THRESHOLD_KMH", "130"))

SYNTH_VEHICLES_MIN = int(os.getenv("SYNTH_VEHICLES_MIN", "5"))
SYNTH_VEHICLES_MAX = int(os.getenv("SYNTH_VEHICLES_MAX", "20"))

# ========== PROMETHEUS METRICS (SAME AS BEFORE) ==========
chunks_started_total = Counter("cv_chunks_started_total", "Chunks started", ["worker_id"])
chunks_completed_total = Counter("cv_chunks_completed_total", "Chunks completed", ["worker_id"])
chunks_failed_total = Counter("cv_chunks_failed_total", "Chunks failed", ["worker_id", "reason"])
cv_chunk_latency_seconds = Histogram(
    "cv_chunk_processing_seconds",
    "CV processing time per chunk",
    ["worker_id"],
    buckets=(0.5, 1, 2, 5, 10, 20, 40, 60, 120, 240)
)
chunks_in_progress = Gauge("cv_chunks_in_progress", "Chunks in progress", ["worker_id"])
vehicle_summaries_emitted_total = Counter("cv_vehicle_summaries_emitted_total", "Vehicle summaries emitted", ["worker_id"])
speed_alerts_emitted_total = Counter("cv_speed_alerts_emitted_total", "Speed alerts emitted", ["worker_id"])

# ========== AZURE CLIENTS ==========
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
producer_events = EventHubProducerClient.from_connection_string(EVENTHUB_CONNECTION_STRING, eventhub_name=EVENTHUB_EVENTS)
producer_alerts = EventHubProducerClient.from_connection_string(EVENTHUB_CONNECTION_STRING, eventhub_name=EVENTHUB_ALERTS)

# ========== HELPER FUNCTIONS ==========
def download_blob(blob_url: str, local_path: str):
    """Download chunk from Azure Blob Storage"""
    # Extract blob name from URL (format: https://account.blob.core.windows.net/container/blob)
    blob_name = blob_url.split(f"/{BLOB_CONTAINER_CHUNKS}/")[-1]
    
    blob_client = blob_service_client.get_blob_client(
        container=BLOB_CONTAINER_CHUNKS,
        blob=blob_name
    )
    
    with open(local_path, "wb") as f:
        f.write(blob_client.download_blob().readall())

def synthetic_process_chunk(chunk_event: dict) -> list[dict]:
    """Same synthetic CV as before"""
    n = random.randint(SYNTH_VEHICLES_MIN, SYNTH_VEHICLES_MAX)
    summaries = []
    
    for i in range(n):
        track_id = f"t{chunk_event['chunk_index']}_{i}"
        vehicle_type = "car" if random.random() < 0.8 else "truck"
        direction = "inbound" if random.random() < 0.5 else "outbound"
        
        base = random.uniform(60, 120) if vehicle_type == "car" else random.uniform(50, 100)
        max_speed_kmh = base + random.uniform(0, 30)
        avg_speed_kmh = (base + max_speed_kmh) / 2.0
        
        summaries.append({
            "event_type": "vehicle_track_summary",
            "event_id": str(uuid.uuid4()),
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "chunk_id": chunk_event["chunk_id"],
            "source_video_id": chunk_event["source_video_id"],
            "chunk_index": chunk_event["chunk_index"],
            "track_id": track_id,
            "vehicle_type": vehicle_type,
            "direction": direction,
            "avg_speed_kmh": avg_speed_kmh,
            "max_speed_kmh": max_speed_kmh,
        })
    
    return summaries

def send_to_eventhub(producer, events: list[dict]):
    """Send batch of events to Event Hub"""
    batch = producer.create_batch()
    for event in events:
        batch.add(event)
    producer.send_batch(batch)

# ========== EVENT HANDLER ==========
def on_event(partition_context, event):
    """Process one chunk event from Event Hub"""
    try:
        # Parse incoming message
        chunk_event = json.loads(event.body_as_str())
        
        chunks_started_total.labels(worker_id=WORKER_ID).inc()
        chunks_in_progress.labels(worker_id=WORKER_ID).inc()
        
        print(f"[{WORKER_ID}] Processing chunk: {chunk_event['chunk_id']}")
        
        # Download chunk from Blob Storage
        with tempfile.TemporaryDirectory() as tmpdir:
            local_mp4 = os.path.join(tmpdir, "chunk.mp4")
            download_blob(chunk_event["uri"], local_mp4)
            
            # CV processing (timed)
            start_t = time.perf_counter()
            summaries = synthetic_process_chunk(chunk_event)
            elapsed = time.perf_counter() - start_t
            
            cv_chunk_latency_seconds.labels(worker_id=WORKER_ID).observe(elapsed)
            
            # Send results to Event Hubs
            events_batch = []
            alerts_batch = []
            
            for summary in summaries:
                events_batch.append(json.dumps(summary).encode('utf-8'))
                vehicle_summaries_emitted_total.labels(worker_id=WORKER_ID).inc()
                
                # Check for speed alerts
                if summary["max_speed_kmh"] > SPEED_ALERT_THRESHOLD_KMH:
                    alert = {
                        "event_type": "speed_alert",
                        "event_id": str(uuid.uuid4()),
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "chunk_id": summary["chunk_id"],
                        "track_id": summary["track_id"],
                        "vehicle_type": summary["vehicle_type"],
                        "direction": summary["direction"],
                        "max_speed_kmh": summary["max_speed_kmh"],
                        "threshold_kmh": SPEED_ALERT_THRESHOLD_KMH,
                    }
                    alerts_batch.append(json.dumps(alert).encode('utf-8'))
                    speed_alerts_emitted_total.labels(worker_id=WORKER_ID).inc()
                    print(f"[{WORKER_ID}] 🚨 ALERT: {summary['track_id']} at {summary['max_speed_kmh']:.1f} km/h")
            
            # Send to Event Hubs
            send_to_eventhub(producer_events, events_batch)
            if alerts_batch:
                send_to_eventhub(producer_alerts, alerts_batch)
        
        # Update checkpoint (like Kafka commit)
        partition_context.update_checkpoint(event)
        
        chunks_completed_total.labels(worker_id=WORKER_ID).inc()
        print(f"[{WORKER_ID}] ✅ Completed chunk: {chunk_event['chunk_id']} ({elapsed:.2f}s)")
        
    except Exception as e:
        chunks_failed_total.labels(worker_id=WORKER_ID, reason=type(e).__name__).inc()
        print(f"[{WORKER_ID}] ❌ ERROR: {e}")
    finally:
        chunks_in_progress.labels(worker_id=WORKER_ID).dec()

# ========== MAIN ==========
def main():
    print(f"[{WORKER_ID}] Starting vision worker...")
    
    # Start Prometheus metrics server
    start_http_server(METRICS_PORT)
    print(f"[{WORKER_ID}] Metrics: http://localhost:{METRICS_PORT}/metrics")
    
    # Create Event Hub consumer
    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        consumer_group=EVENTHUB_CONSUMER_GROUP,
        eventhub_name=EVENTHUB_CHUNKS,
    )
    
    print(f"[{WORKER_ID}] Listening to Event Hub: {EVENTHUB_CHUNKS}")
    print(f"[{WORKER_ID}] Consumer group: {EVENTHUB_CONSUMER_GROUP}")
    
    try:
        # Start consuming (blocks forever)
        with consumer:
            consumer.receive(
                on_event=on_event,
                starting_position="-1",  # From beginning
            )
    except KeyboardInterrupt:
        print(f"[{WORKER_ID}] Shutting down...")
    finally:
        producer_events.close()
        producer_alerts.close()

if __name__ == "__main__":
    main()