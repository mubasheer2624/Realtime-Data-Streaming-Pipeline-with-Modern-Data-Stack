import uuid
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import logging
from kafka import KafkaProducer
import time

# Default arguments for the DAG
dag_args = {
    'owner': 'data_streamer',
    'start_date': datetime(2023, 9, 3, 10, 0)
}

def fetch_user_data():
    """Fetching data from an API that generates random user profiles."""
    response = requests.get("https://randomuser.me/api/")
    user_data = response.json()['results'][0]
    return user_data

def transform_user_data(raw_data):
    """Transforming raw user data into a structured dictionary."""
    user = {
        'id': uuid.uuid4(),
        'first_name': raw_data['name']['first'],
        'last_name': rawur_data['name']['last'],
        'gender': raw_data['gender'],
        'address': f"{raw_data['location']['street']['number']} {raw_data['location']['street']['name']}, "
                   f"{raw_data['location']['city']}, {raw_data['location']['state']}, {raw_data['location']['country']}",
        'post_code': raw_data['location']['postcode'],
        'email': raw_data['email'],
        'username': raw_data['login']['username'],
        'dob': raw_data['dob']['date'],
        'registered_date': raw_data['registered']['date'],
        'phone': raw_data['phone'],
        'picture': raw_data['picture']['medium']
    }
    return user

def send_to_kafka(user_data):
    """Sending transformed data to a Kafka topic for further processing."""
    try:
        producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
        producer.send('users_created', json.dumps(user_data).encode('utf-8'))
        producer.flush()
    except Exception as e:
        logging.error(f"Failed to send data to Kafka due to: {e}")

with DAG('user_data_pipeline', default_args=dag_args, schedule_interval='@daily', catchup=False) as dag:
    fetch_and_process_user_data = PythonOperator(
        task_id='fetch_and_transform_user_data',
        python_callable=lambda: send_to_kafka(transform_user_data(fetch_user_data()))
    )
