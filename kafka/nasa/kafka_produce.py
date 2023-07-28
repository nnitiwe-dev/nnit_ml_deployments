import requests
import os
import json
import logging
from kafka import KafkaProducer
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka broker address
kafka_broker = "localhost:9092"
# Kafka topic
kafka_topic = "nasa_events_topic"

# File to store the latest event timestamp
latest_event_file = "latest_event_timestamp.txt"

def fetch_data_from_nasa_api():
    url = "https://eonet.gsfc.nasa.gov/api/v2.1/events?source=InciWeb"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data.get("events", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from NASA API: {e}")
        return []

def flatten_events(events):
    flat_events = []
    for event in events:
        flat_event = {
            "id": event.get("id"),
            "title": event.get("title"),
            "description": event.get("description", ""),
            "link": event.get("link"),
            "categories": [category["title"] for category in event.get("categories", [])],
            "source_url": event.get("sources", [{}])[0].get("url"),
            "latitude": event.get("geometries", [{}])[0].get("coordinates", [])[1],
            "longitude": event.get("geometries", [{}])[0].get("coordinates", [])[0],
            "timestamp": event.get("geometries", [{}])[0].get("date")
        }
        flat_events.append(flat_event)
    return flat_events

def push_to_kafka(events):
    producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    for event in events:
        producer.send(kafka_topic, value=event)
    producer.close()

def get_latest_event_timestamp():
    if os.path.exists(latest_event_file):
        with open(latest_event_file, "r") as file:
            try:
                return int(file.read())
            except ValueError:
                logging.warning("Invalid timestamp found in the latest event file.")
    return "0001-01-15T21:51:00Z"

def update_latest_event_timestamp(timestamp):
    with open(latest_event_file, "w") as file:
        file.write(str(timestamp))

def execute_pipeline():
    # Fetch data from NASA API
    events_data = fetch_data_from_nasa_api()

    # Flatten the nested JSON data
    flat_events_data = flatten_events(events_data)

    # Check if the events data is new based on the latest event timestamp
    latest_event_timestamp = get_latest_event_timestamp()
    new_events_data = [event for event in flat_events_data if datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%SZ") > datetime.strptime(latest_event_timestamp, "%Y-%m-%dT%H:%M:%SZ")]

    if new_events_data:
        # Push the new data through Kafka producer
        push_to_kafka(new_events_data)

        # Update the latest event timestamp
        latest_timestamp = max(event["timestamp"] for event in new_events_data)
        update_latest_event_timestamp(latest_timestamp)

if __name__ == "__main__":
    execute_pipeline()
