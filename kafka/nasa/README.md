# NASA Data Ingestion Pipeline with Kafka and Elasticsearch

Welcome to the NASA Data Ingestion Pipeline project! This project demonstrates how to build a robust data pipeline that extracts real-time data from NASA's API, streams it through Apache Kafka, and indexes it into Elasticsearch for visualization in Kibana.

## Overview

The NASA Data Ingestion Pipeline is designed to provide real-time insights into natural events captured by NASA's Earth Observatory. The pipeline leverages the power of Kafka, Elasticsearch, and Kibana to efficiently process, store, and visualize the data, enabling users to explore dynamic space events with speed and precision.

## Key Features

- Data extraction from NASA's API using a Kafka producer
- High-throughput data streaming with Apache Kafka
- Real-time indexing of data into Elasticsearch
- Interactive data visualization using Kibana's geospatial capabilities
- Error handling and logging for enhanced reliability

## Getting Started

Follow these steps to set up and run the data pipeline locally:

1. Install Apache Kafka, Elasticsearch, and Kibana on your system.
2. Clone this repository to your local machine.
3. Set up a virtual environment and install required Python packages using `pip install -r requirements.txt`.
4. Configure the NASA API key and Kafka broker settings in `config.py`.
5. Run the Kafka producer script to start ingesting data from the NASA API: `python kafka_produce.py`.
6. Run the Kafka consumer script to consume data and index it into Elasticsearch: `python kafka_consume.py`.

## Requirements

- Python 3.x
- Apache Kafka
- Elasticsearch
- Kibana

## Resources

- [NASA EONET API](https://eonet.gsfc.nasa.gov/docs/v2.1)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Elasticsearch Documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana Documentation](https://www.elastic.co/guide/en/kibana/current/index.html)

