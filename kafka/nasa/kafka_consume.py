import logging
from kafka import KafkaConsumer
from elasticsearch.exceptions import RequestError
from elasticsearch_dsl import Document, Date, Keyword, Text,Float,GeoPoint, connections
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka broker address
kafka_broker = "localhost:9092"
# Kafka topic
kafka_topic = "nasa_events_topic"
# Elasticsearch host
elasticsearch_host = "localhost"
# Elasticsearch port
elasticsearch_port = 9200
# Elasticsearch index
elasticsearch_index = "nasa_events_indice_2"


class NASAEvent(Document):
    id = Keyword()
    title = Text()
    description = Text()
    link = Keyword()
    categories = Keyword(multi=True)
    source_url = Keyword()
    latitude = Float()
    longitude = Float()
    timestamp= Date()
    location=GeoPoint(lat_lon=True)

    class Index:
        name = elasticsearch_index

def consume_data_from_kafka(index_):
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_broker,
        group_id='kibana_consumer_task_2',
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )

    try:
        for message in consumer:
            event_data = message.value
            logging.info(f"Received new event data: {event_data}")

            # Index the data on Elasticsearch
            try:
                nasa_event = NASAEvent(
                id=event_data["id"],
                title=event_data["title"],
                description=event_data["description"],
                link=event_data["link"],
                categories=event_data["categories"],
                source_url=event_data["source_url"],
                latitude=event_data["latitude"],
                longitude=event_data["longitude"],
                timestamp=event_data["timestamp"],
                location=dict(lat=event_data["latitude"], lon=event_data["longitude"])
                )
                nasa_event.save(index=index_)
                logging.info("Data indexed successfully on Elasticsearch.")
            except RequestError as e:
                logging.error(f"Error indexing data on Elasticsearch: {e}")

            # Commit the Kafka offset to mark the message as processed
            consumer.commit()

    except KeyboardInterrupt:
        consumer.close()

def execute_pipeline():
    # Set up the Elasticsearch connection with the alias 'default'
    connections.create_connection(alias='default', hosts=[elasticsearch_host])

    # Create the Elasticsearch index if it does not exist
    consume_data_from_kafka(elasticsearch_index)

if __name__ == "__main__":
    execute_pipeline()
