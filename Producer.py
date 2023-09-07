import json
import time
import logging
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the Kafka topic
kafka_topic = "meetup_events"

# Create a Kafka producer instance
producer = Producer({'bootstrap.servers': 'localhost:9092'})

with open('resources/meetup.json', 'r') as file:
    for line in file:
        try:
            # Parse the JSON message from each line
            meetup_event = json.loads(line.strip())

            # Convert the event to a JSON string
            event_json = json.dumps(meetup_event)

            # Send the JSON message to the Kafka topic
            producer.produce(kafka_topic, event_json)
            logger.info("Sent 1 message to Kafka")

            time.sleep(1)

        except json.JSONDecodeError as e:
            logger.error("Error decoding JSON: %s", e)
        except Exception as e:
            logger.error("Error: %s", e)

producer.flush()
producer.close()
