from azure.eventhub import EventHubConsumerClient, EventHubProducerClient, EventData
import os

print('Hello')

consumer = EventHubConsumerClient.from_connection_string(
    conn_str=os.environ["EVENTHUB_CONNECTION"],
    eventhub_name=os.environ["EVENTHUB_NAME"],
    consumer_group=os.environ["EVENTHUB_CONSUMER_GROUP"]
)

producer = EventHubProducerClient.from_connection_string(
    conn_str=os.environ["EVENTHUB_CONNECTION"],
    eventhub_name=os.environ['EVENT_TRAFFIC']
)

def on_event(partition_context, event):
    print("MESSAGE:", event.body_as_str())
    partition_context.update_checkpoint(event)
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData("Hello Event Hub from Python!"))
    producer.send_batch(event_data_batch)

with consumer, producer:
    consumer.receive(on_event=on_event, starting_position="-1")
