import json
import time
from faker import Faker
from faker_clickstream import ClickstreamProvider
from confluent_kafka import Producer


# Clickstream2: Streams clickstream sessions until force-stop.


# Kafka configuration
conf = {'bootstrap.servers': 'localhost:9092'}  # replace with your Kafka server details

# Create Producer instance
p = Producer(conf)


try:
    while True:
        # Generate clickstream session (1 - 25 records)
        fake = Faker()
        fake.add_provider(ClickstreamProvider)
        clickstream_data = fake.session_clickstream()

        # Convert the clickstream data to JSON and produce it to Kafka
        for data in clickstream_data:
            p.produce('clickstream', json.dumps(data))  # replace 'your_topic' with your Kafka topic

        # Wait for any outstanding messages to be delivered and delivery reports to be received.
        p.flush()

        print("Producing session data...")
        time.sleep(5)
except KeyboardInterrupt:
    print("Loop interrupted. Shutting down gracefully.")


