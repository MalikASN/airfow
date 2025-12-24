import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from datetime import datetime
import pandas as pd

BUCKET_NAME = "airflow-bucket798"
AWS_CONN_ID = "aws_default"
POSTGRES_CONN_ID = "postgres_default"
SLACK_CONN_ID = "slack_conn" 

def get_and_transform_files(ti):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix='', delimiter='/')
    csv_files = [k for k in keys if k.endswith('.csv')]
    
    if not csv_files:
        print("Aucun fichier CSV trouvé !")
        return

    target_key = csv_files[0]
    local_filename = hook.download_file(key=target_key, bucket_name=BUCKET_NAME)
    
    df = pd.read_csv(local_filename)
    
    if "Order Date" in df.columns:
        df = df.dropna(axis=0, how="any", subset=["Order Date"])
    
    df = df.iloc[:50, :] 
    
    output_filename = "transformed.csv"
    output_path = f"/tmp/{output_filename}"
    df.to_csv(output_path, index=False)

    s3_key_output = f"ecom_output/{output_filename}"
    hook.load_file(
        filename=output_path, 
        key=s3_key_output, 
        bucket_name=BUCKET_NAME, 
        replace=True
    )

    ti.xcom_push(key='s3_key_file', value=s3_key_output)

def pg_loader_method(ti):
    try:
        s3_key = ti.xcom_pull(key='s3_key_file', task_ids="transform_task")
        
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        local_filename = s3_hook.download_file(key=s3_key, bucket_name=BUCKET_NAME)
        
        df = pd.read_csv(local_filename)
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        
        logging.info(f"Importing {len(df)} rows into Postgres...")
        
        df.to_sql(
            name='daily_sales_report',
            con=engine,
            if_exists='replace',    
            index=False                
        )
        logging.info("Import terminé avec succès !")
        ti.xcom_push(key="status", value="success")

    except Exception as e:
        logging.error(f"Erreur SQL : {e}")
        ti.xcom_push(key="status", value="error")
        raise e 

def slack_call(ti):

    status = ti.xcom_pull(key='status', task_ids='postgres_task')
    
    slack_hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    
    if status == "success":
        msg = " *Succès Airflow* : Les données de vente ont été chargées dans Postgres !"
    else:
        msg = " *Alerte Airflow* : Échec du chargement en base de données."
        
    slack_hook.send(text=msg)
    print("Message Slack envoyé.")

with DAG(
    dag_id="hello_airflow_postgres_slack",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    

    """
    sensor qui detecte si un fichier est uploade sur le bucket S3
    """

    s3_listener = S3KeySensor(
        task_id="s3_sensor",
        bucket_key="*.csv",
        bucket_name=BUCKET_NAME,
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True
    )

    """
    Recupere le fichier avec S3Hook puis le pretraite avec pandas puis le revoie sur le bucket S3  + enregistre le chemin sur xcom
    """ 
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=get_and_transform_files,
    ) 


    """
    recupere le chemain depuis xcom puis insert le dataset dans un datawarehouse postgres + insert dans xcom le statut du resultat
    """
    postgres_task = PythonOperator(
        task_id="postgres_task",
        python_callable=pg_loader_method
    )

    """
    notifier en envoyant un message sur un canal slack 
    """

    slack_task = PythonOperator(
        task_id="slack_task",
        python_callable=slack_call,
        trigger_rule="all_done" 
    )

    s3_listener >> transform_task >> postgres_task >> slack_task