from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd

BUCKET_NAME = "airflow-bucket798"
AWS_CONN_ID = "aws_default"

# 👇 On supprime "pusher_le_fichier" qui ne servait à rien.

# 👇 On garde celle-ci, elle est parfaite pour la réception
def recevoir_le_fichier(ti):
    # Elle va chercher le message envoyé par la task 'process_file'
    file_path = ti.xcom_pull(key='file_path', task_ids='process_file')
    
    if not file_path:
        print("Erreur : Aucun chemin de fichier reçu via XCom !")
    else:
        print(f"✅ Message bien reçu ! Le fichier traité est ici : {file_path}")

# 👇 MODIFICATION IMPORTANTE : Ajout de 'ti' dans les arguments
def traiter_le_fichier(ti):
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix='', delimiter='/')
    csv_files = [k for k in keys if k.endswith('.csv')]
    
    if not csv_files:
        print("Aucun fichier CSV trouvé !")
        return

    target_key = csv_files[0]
    local_filename = hook.download_file(key=target_key, bucket_name=BUCKET_NAME)
    
    df = pd.read_csv(local_filename)
    df = df.dropna(axis=0, how='any')
    df = df.iloc[:10, :]
    
    output_path = '/tmp/processed_data.csv'
    df.to_csv(output_path, index=False)
    print(f"Traitement terminé sur : {output_path}")

    # Upload S3
    hook.load_file(
        filename=output_path, 
        key="output/processed_data.csv", 
        bucket_name=BUCKET_NAME, 
        replace=True
    )
    
    # 👇 C'EST ICI QUE LA MAGIE OPÈRE
    # On envoie l'info "J'ai fini, voici où est le fichier" à Airflow
    ti.xcom_push(key='file_path', value=output_path)
    print(f"XCom envoyé : file_path = {output_path}")

with DAG(
    dag_id="hello_airflow_xcom",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    detecter_fichier = S3KeySensor(
        task_id="wait_for_file",
        bucket_name=BUCKET_NAME,
        bucket_key="*.csv", 
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,
        poke_interval=30,
        timeout=600
    )

    traitement = PythonOperator(
        task_id="process_file",
        python_callable=traiter_le_fichier
        # Pas besoin de préciser op_args=['ti'], Airflow le fait tout seul
    )

    receveur = PythonOperator(
        task_id="receive_file", 
        python_callable=recevoir_le_fichier
    )

    detecter_fichier >> traitement >> receveur