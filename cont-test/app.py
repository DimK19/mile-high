from azure.eventhub import EventHubConsumerClient, EventHubProducerClient, EventData
import os
import json
import datetime
import random

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

    num_results = random.randint(10, 12)

    event_data_batch = producer.create_batch()

    for _ in range(num_results):
        is_speeding = random.random() < 0.05

        if is_speeding:
            max_speed = random.uniform(131.0, 180.0)
        else:
            max_speed = random.uniform(60.0, 129.0)

        avg_speed = max_speed

        # 90% car, 10% truck
        vehicle_type = "car" if random.random() < 0.9 else "truck"

        result_payload = {
            "event_type": "vehicle_track_summary",
            "event_id": random.randint(1000, 9999),
            "timestamp_utc": datetime.datetime.utcnow().isoformat(),
            "chunk_id": "sydney",
            "source_video_id": "vid_camera_01",
            "chunk_index": 5,
            "track_id": "sweeney",
            "vehicle_type": vehicle_type,
            "direction": random.choice(["northbound", "southbound"]),
            "avg_speed_kmh": round(avg_speed, 2),
            "max_speed_kmh": round(max_speed, 2)
        }
        
        # Convert dictionary to JSON string and add to batch
        event_data_batch.add(EventData(json.dumps(result_payload)))

    producer.send_batch(event_data_batch)
    print(f"SENT {num_results} RESULTS")

with consumer, producer:
    consumer.receive(on_event=on_event, starting_position="-1")
