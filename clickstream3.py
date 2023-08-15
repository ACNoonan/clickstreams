import json
import time
from faker import Faker
import requests
from faker_clickstream import ClickstreamProvider
from confluent_kafka import Producer


# Clickstream3: Streams clickstream sessions; includes Registration HTTP call to Lenses.


# Register External Application
url = "http://127.0.0.1:3030/api/v1/apps/external"

payload = json.dumps({
  "name": "Clickstream-Producer",
  "metadata": {
    "version": "1.0",
    "description": "Testing External App Registration via HTTP & Postman",
    "owner": "Adam",
    "appType": "stream-generator",
    "tags": [
      "box",
      "demo",
      "Adam",
      "SLA:1"
    ],
    "deployment": "k8s"
  },
  "output": [
    {
      "name": "clickstream"
    }
  ]
})

headers = {
  'X-Kafka-Lenses-Token': 'ServiceAccount:Credentials',
  'Content-Type': 'application/json'
}


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



response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
