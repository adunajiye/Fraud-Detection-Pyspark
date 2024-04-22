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
from dateutil import parser
import re
from airflow.utils.email import send_email
from Ingest_Data_Airflow import send_email_alert
from Ingest_data import Save_Pubsub_Function
import pandas as pd 

def is_weekend(tx_datetime):
    # Transform date into weekday (0 is Monday, 6 is Sunday)
    weekday = tx_datetime.weekday()
    # Binary value: 0 if weekday, 1 if weekend
    is_weekend = weekday>= 5
    return int(is_weekend)

def is_night(tx_datetime):
    # Get the hour of the transaction
    tx_hour = tx_datetime.hour
    # Binary value: 1 if hour less than 6, and 0 otherwise
    is_night = tx_hour<=6
    
    return int(is_night)

def Preprocess_Transaction_Function():
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
           SELECT * from TABLE; 
            WHERE completion_date = %s
        """
        cur.execute(query, (current_date,))
        transaction_records = cur.fetchall()
        print(transaction_records)

        """
        Modify the columns and convert the date to datetime
        2. appply the is weekend function to modoify the conditions within the Data
        3. apply same fucntion to is night or overhead
        """
        for row in transaction_records:
            rows = row[0] # This is the datetme column
            # convert to datetime
            rows_query = f""" Select TO_TIMESTAMP({rows}) as Converted_Date from Table; """
            cur.execute(rows_query)
            converted_date = cur.fetchall()
            print(converted_date)

            is_weekend_value = is_weekend(converted_date)
            is_night_value = is_night(converted_date)

            # Update the database with is_weekend and is_night values
            update_query = """
            UPDATE TABLE 
            SET is_weekend = %s, is_night = %s 
            WHERE datetime_column = %s
            """
            cur.execute(update_query, (is_weekend_value, is_night_value, converted_date))
            conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)
    finally:
            if conn is not None:
                conn.close()
                print('Database connection closed.')
Preprocess_Transaction_Function()
































































# def send_email_alert(context):
#     email_recipients = ['your@email.com']
#     email_subject = 'Airflow DAG Alert'
#     email_body = 'An error occurred in the data processing workflow.'
    
#     execution_date = context['execution_date']
#     execution_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')
#     email_subject_with_date = f'{email_subject} - {execution_date_str}'
#     send_email(email_recipients, email_subject_with_date, email_body)
    