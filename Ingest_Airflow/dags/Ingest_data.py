# Data Integration 

"""
    This pipeline is to show that we will can pull data from postgres and stream the data in 
    real-time to kafka topic and can also be processed to Googl cloud storage. 
"""
import psycopg2
from confluent_kafka import SerializingProducer
from google.cloud import pubsub_v1
import json


def Save_Pubsub_Function():
    try:
        print('connceting to PostgreSQL......')
        # PostgreSQL connection parameters
        postgres_host = 'your_postgres_host'
        postgres_port = 'your_postgres_port'
        postgres_db = 'your_postgres_database'
        postgres_user = 'your_postgres_username'
        postgres_password = 'your_postgres_password'

        # Kafka parameters
        kafka_bootstrap_servers = 'your_kafka_broker_address:9092'
        kafka_topic = 'your_kafka_topic'

        # Google Cloud Pub/Sub parameters
        project_id = 'your_project_id'
        pubsub_topic = 'projects/{}/topics/{}'.format(project_id, 'your_pubsub_topic')

        # Create a PostgreSQL connection
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password
        )
        cur = conn.cursor()

        # Create a Kafka Producer
        # kafka_producer = SerializingProducer({'bootstrap.servers': kafka_bootstrap_servers})

        # Create a Pub/Sub Publisher
        publisher = pubsub_v1.PublisherClient()

        cur.execute("SELECT * FROM your_table")
        rows = cur.fetchall()
        for row in rows:
            # Process the row data
            data = {
                'column1': row[0],
                'column2': row[1],
                # Add more columns as needed
            }
            json_data = json.dumps(data)
            print(json_data) # verify Data in json format before sending to PubSub
            
            # Send the data to Google Cloud Pub/Sub
            Send_Message_Pubsub_Function(json_data)  

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
Save_Pubsub_Function()
    
    

# def send_message_to_kafka(message):
#     kafka_producer.produce(kafka_topic, value=message.encode('utf-8'))
#     kafka_producer.flush()

def Send_Message_Pubsub_Function(message):
    publisher = pubsub_v1.PublisherClient()
    project_id = 'your_project_id'
    pubsub_topic = 'projects/{}/topics/{}'.format(project_id, 'your_pubsub_topic')
    data = message.encode('utf-8')
    future = publisher.publish(pubsub_topic, data=data)
    future.result()
    return future 



    
    

# def fetch_data_from_postgres():
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM your_table")
#     rows = cursor.fetchall()
#     for row in rows:
#         # Process the row data
#         data = {
#             'column1': row[0],
#             'column2': row[1],
#             # Add more columns as needed
#         }
#         json_data = json.dumps(data)
#         # Send the data to Kafka or Pub/Sub
#         # send_message_to_kafka(json_data)
#         send_message_to_pubsub(json_data)  

# if __name__ == "__main__":
#     fetch_data_from_postgres()
