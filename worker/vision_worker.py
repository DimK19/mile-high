import os
import json
import time
import uuid
import random
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

from azure.eventhub import EventHubConsumerClient, EventData


# ----------------------------
# Configuration loading
# ----------------------------

SYNTH_VEHICLES_MIN = int(os.getenv("SYNTH_VEHICLES_MIN", "5"))
SYNTH_VEHICLES_MAX = int(os.getenv("SYNTH_VEHICLES_MAX", "20"))
SPEED_ALERT_THRESHOLD_KMH = float(os.getenv("SPEED_ALERT_THRESHOLD_KMH", "130"))


# ----------------------------
# Data models (simple + explicit)
# ----------------------------
#kafka message
@dataclass(frozen=True)
class ChunkEvent:
    """Represents one 'video_chunk_ready' message."""
    sourceVideoID: str
    contentLength: int
    url: str
    eventTime: str


def utc_now_iso() -> str:
    """Return current UTC timestamp as ISO string (stable and machine-readable)."""
    return datetime.now(timezone.utc).isoformat()


# ----------------------------
# External clients
# ----------------------------

def build_EventHub_consumer():
    """
    Consume chunk events.
    - group_id enables horizontal scaling (multiple workers share the load).
    - auto_offset_reset='earliest' makes dev/test reproducible.
    """
    return EventHubConsumerClient.from_connection_string(
        conn_str=os.environ["EVENTHUB_CONNECTION"],
        eventhub_name=os.environ["EVENTHUB_NAME"],
        consumer_group=os.environ["EVENTHUB_CONSUMER_GROUP"]
    )


def build_ΕventΗub_producer():
    return EventHubProducerClient.from_connection_string(
        conn_str=EVENTHUB_CONNECTION_STRING,
        eventhub_name=os.environ['EVENT_TRAFFIC']
    )


# ----------------------------
# Core helpers
# ----------------------------

def parse_chunk_event(raw: dict):
    """
    Validate/normalize the incoming message.
    Correctness reason:
    - Fail fast if a required field is missing, instead of silently producing wrong analytics.

    {"topic":"/subscriptions/98fd285f-5717-4413-a86c-ecdd65488235/resourceGroups/DSP/providers/Microsoft.Storage/storageAccounts/sydneysweeney","subject":"/blobServices/default/containers/vidsegments/blobs/segment5.mp4","eventType":"Microsoft.Storage.BlobCreated","id":"a91d3ead-901e-0046-5be8-871444066c91","data":{"api":"PutBlob","clientRequestId":"5a32b310-f3db-11f0-9234-b8d43fdc6b8b","requestId":"a91d3ead-901e-0046-5be8-871444000000","eTag":"0x8DE55FF48C31F7B","contentType":"video/mp4","contentLength":5173908,"blobType":"BlockBlob","accessTier":"Default","url":"https://sydneysweeney.blob.core.windows.net/vidsegments/segment5.mp4","sequencer":"00000000000000000000000000008FC700000000003e6d11","storageDiagnostics":{"batchId":"f69f7426-3006-003d-00e8-8756d8000000"}},"dataVersion":"","metadataVersion":"1","eventTime":"2026-01-17T19:33:25.8967684Z"}
    """
    return ChunkEvent(
        sourceVideoID = raw["url"].split('/')[-1],
        contentLength=raw["contentLength"],
        url=str(raw["url"]),
        eventTime=str(raw["eventTime"])
    )


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
                "source_video_id": ev.sourceVideoID,
                "track_id": track_id,
                "vehicle_type": vehicle_type,
                "direction": direction,
                "avg_speed_kmh": avg_speed_kmh,
                "max_speed_kmh": max_speed_kmh,
            }
        )

    return out


# ----------------------------
# Main worker loop
# ----------------------------

def main() -> None:
    """
    Start metrics server and run an infinite consume-process-produce loop.
    Correctness reason:
    - This is the standard streaming worker pattern used in AKS/Kubernetes.
    """
    consumer = build_EventHub_consumer()
    producer = build_ΕventΗub_producer()

    def on_event(partition_context, event):
        ev = parse_chunk_event(event.value)

        # Time ONLY the CV step (later: actual OpenCV pipeline), not Kafka IO.
        start_t = time.perf_counter()
        summaries = synthetic_process_chunk(ev)
        elapsed = time.perf_counter() - start_t

        event_data_batch = producer.create_batch()

        # Publish summaries. Key by chunk_id for partition locality.
        for s in summaries:
            event_data_batch.add(EventData(s))

        producer.send_batch(event_data_batch)


    with consumer, producer:
        consumer.receive(
            on_event=on_event,
            starting_position="-1"
        )


if __name__ == "__main__":
    main()
