from azure.eventhub import EventHubConsumerClient, EventHubProducerClient, EventData
import os
import json
import datetime

consumer = EventHubConsumerClient.from_connection_string(
    conn_str=os.environ["EVENTHUB_CONNECTION"],
    eventhub_name=os.environ["EVENTHUB_NAME"],
    consumer_group=os.environ["EVENTHUB_CONSUMER_GROUP"]
)

# This is the Event Hub where we send the results (and where Capture will happen)
producer = EventHubProducerClient.from_connection_string(
    conn_str=os.environ["EVENTHUB_CONNECTION"],
    eventhub_name=os.environ['EVENT_TRAFFIC']
)

def on_event(partition_context, event):
    print("RECEIVED JOB:", event.body_as_str())
    partition_context.update_checkpoint(event)

    # Construct the JSON payload with placeholders as requested
    result_payload = {
        "event_type": "vehicle_track_summary",
        "event_id": 69420,
        "timestamp_utc": datetime.datetime.utcnow().isoformat(),
        "chunk_id": "sydney",
        "source_video_id": "vid_camera_01",
        "chunk_index": 5,
        "track_id": "sweeney",
        "vehicle_type": "sydney sweeney",
        "direction": "sydney sweeney",
        "avg_speed_kmh": 45.2,
        "max_speed_kmh": 52.0
    }

    event_data_batch = producer.create_batch()
    
    # Convert dictionary to JSON string and add to batch
    event_data_batch.add(EventData(json.dumps(result_payload)))
    
    producer.send_batch(event_data_batch)
    print(f"SENT RESULT: {result_payload['event_id']}")

with consumer, producer:
    consumer.receive(on_event=on_event, starting_position="-1")
