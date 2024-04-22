# 2. stream processes using kafka streams

from turtle import up
from kafka import KafkaConsumer, KafkaProducer
from google.cloud import storage
from google.cloud import pubsub_v1
import json
from datetime import datetime
from threading import Thread
import os

# Google Cloud Pub/Sub parameters
project_id = 'your_project_id'
pubsub_subscription = 'your_pubsub_subscription_name'

# Kafka parameters
kafka_bootstrap_servers = 'your_kafka_broker_address:9092'
kafka_input_topic = 'input_topic'
kafka_output_topic = 'output_topic'

# Google Cloud Storage parameters
gcs_bucket_name = 'your_gcs_bucket_name'
gcs_folder_name = 'processed_data'

# Function to consume messages from Pub/Sub and produce them to Kafka input topic
def pubsub_to_kafka():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, pubsub_subscription)
    consumer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)
    
    def callback(message):
        # Publish the message to Kafka input topic
        consumer.send(kafka_input_topic, value=message.data)
        message.ack()
    
    subscriber.subscribe(subscription_path, callback=callback)
    print('Subscribed to Pub/Sub topic and publishing to Kafka input topic...')

# process messages using Kafka Streams by consuming the kafka_input topic
def upload_messages():
    consumer = KafkaConsumer(kafka_input_topic, bootstrap_servers=kafka_bootstrap_servers)
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_name)


    for message in consumer:
        print(message)
        producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

        # Produce the processed message to Kafka output topic
        producer.send(kafka_output_topic, value={'processed_data': message})
        print(f'Processed message sent to Kafka output topic: {message}')
        
        # Upload the processed message to GCS
        file_name = f'{gcs_bucket_name}/{gcs_folder_name}/output_data_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.json'
        print(file_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(json.dumps({'data': message}), content_type='application/json')
        print(f'Processed message uploaded to GCS: {file_name}')

# Main function to run Pub/Sub to Kafka Streams processing
def main():
    # # Start Pub/Sub to Kafka data transfer
    # pubsub_to_kafka_thread = Thread(target=pubsub_to_kafka)
    # pubsub_to_kafka_thread.start()
    
    # Start Kafka Streams processing
    upload_messages()

if __name__ == '__main__':
    main()
