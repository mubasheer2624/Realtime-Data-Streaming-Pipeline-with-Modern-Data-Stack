# Realtime-Data-Streaming-Pipeline-with-Modern-Data-Stack

## Overview
This repository contains an implementation of a realtime data streaming pipeline, leveraging a modern data stack composed of Apache Airflow, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra, all containerized using Docker. The project demonstrates how to build a robust and scalable data pipeline from data ingestion to processing and storage.

The pipeline uses a series of components to manage and process data:
- **Data Source**: Utilizes randomuser.me API to simulate realtime user data ingestion.
- **Apache Airflow**: Orchestrates the pipeline, managing task dependencies and scheduling.
- **Apache Kafka and Zookeeper**: Facilitate robust data streaming capabilities.
- **Apache Spark**: Processes the data in real-time, leveraging distributed computing.
- **Cassandra**: Acts as the final data store, optimized for high-velocity data writes.

## Key Features
- **Data Ingestion**: Automated fetching and initial processing of data using Apache Airflow.
- **Data Streaming**: Real-time data handling using Apache Kafka, managed by Apache Zookeeper.
- **Data Processing**: Distributed data processing using Apache Spark's powerful computing resources.
- **Data Storage**: Efficient and scalable data storage using Cassandra.

## Technologies Used
- Apache Airflow
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- Cassandra
- Docker
- Python

## Learnings
By setting up and using this pipeline, you will gain insights into:
- Setting up a robust data pipeline using Docker and Apache Airflow.
- Real-time data streaming and processing techniques.
- The architectural considerations for building scalable data processing systems.



