from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
import os
from Ingest_data import Save_Pubsub_Function
from Preprocessing_new_transactions import Preprocess_Transaction_Function

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

# def run_python_script():
#     os.system('python /path/to/your/script.py')

def send_email_alert(context):
    email_recipients = ['your@email.com']
    email_subject = 'Airflow DAG Alert'
    email_body = 'An error occurred in the data processing workflow.'

    
    execution_date = context['execution_date']
    execution_date_str = execution_date.strftime('%Y-%m-%d %H:%M:%S')
    email_subject_with_date = f'{email_subject} - {execution_date_str}'
    send_email(email_recipients, email_subject_with_date, email_body)



# Define your DAG.. This code runs every 4 minutes 
with DAG('Ingest_Data_Email_Task', default_args=default_args, schedule_interval='*/4****',
         max_active_runs=1, catchup=False) as dag:

    ingest_data_task = PythonOperator(
    task_id='ingest_data_task',
    python_callable=Save_Pubsub_Function,  
    )
    
    ingested_prepocessing_task = PythonOperator(
    task_id = 'ingested_prepocessing_task',
      python_callable=  Preprocess_Transaction_Function,
    )
    
    ingest_data_notification_task_1 = PythonOperator(
        task_id='ingest_data_task_1',
        python_callable=Preprocess_Transaction_Function,  
        provide_context=True,  
        on_failure_callback=send_email_alert 
    )

    ingested_data_notification_task_2 = PythonOperator(
        task_id = 'ingest_data_task_2',
        python_callable=send_email_alert,
        )

# ingest_data_task >> ingested_prepocesing_task >> ingest_data_notification_task

