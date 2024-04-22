"""
This Function keeps track record of the data coming in and the send a notification mail upon sucesss of completion 
"""
import datetime
import psycopg2
from confluent_kafka import SerializingProducer
from google.cloud import pubsub_v1
import json
import smtplib
from  email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from airflow.utils.email import send_email
from Ingest_Airflow.dags.Ingest_Data_Airflow import send_email_alert


def Verify_Completed_Data_Function():
    try:
        print('connceting to PostgreSQL......')
        # PostgreSQL connection parameters
        postgres_host = 'your_postgres_host'
        postgres_port = 'your_postgres_port'
        postgres_db = 'your_postgres_database'
        postgres_user = 'your_postgres_username'
        postgres_password = 'your_postgres_password'

        

        # Create a PostgreSQL connection
        conn = psycopg2.connect(
            host=postgres_host,
            port=postgres_port,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password
        )
        cur = conn.cursor()

        # Get current date
        current_date = datetime.datetime.now().date()

        # Query to check completed records for the current date
        query = """
           SELECT COUNT(*) from TABLE; 
            WHERE completion_date = %s
        """
        cur.execute(query, (current_date,))
        completed_records = cur.fetchall()

        # Notify if records are completed for the current date
        if completed_records:
            print(f'{len(completed_records)} records completed on {current_date}.')
            send_email_alert(context={'execution_date': current_date})    
        else:
            print(f'No records completed on {current_date}.')

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
Verify_Completed_Data_Function()

# def send_email_alert(context):
#     email_recipients = ['your@email.com']
#     email_subject = 'Airflow DAG Alert'
#     email_body = 'An error occurred in the data processing workflow.'
    
#     execution_date = context['execution_date']
#     execution_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')
#     email_subject_with_date = f'{email_subject} - {execution_date_str}'
#     send_email(email_recipients, email_subject_with_date, email_body)
    