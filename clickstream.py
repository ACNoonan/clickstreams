import json
import faker
import time
import requests
from confluent_kafka import Producer


# Create a Kafka producer
producer = Producer(bootstrap_servers="localhost:9092")

# Create a Faker clickstream generator
generator = faker.Faker("clickstream")

# Start producing messages
while True:
    # Generate 10 clickstream events
    events = [generator.clickstream_event() for _ in range(10)]

    # Serialize the events to JSON
    events_json = [json.dumps(event) for event in events]

    # Produce the events to Kafka
    for event_json in events_json:
        producer.produce("clickstream", event_json.encode("utf-8"))

    # Sleep for 5 seconds
    time.sleep(5)
