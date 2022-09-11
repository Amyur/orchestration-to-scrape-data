from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta
from scripts.web_scraper import extract_information
import csv
import requests
import json


default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

#create the dag
with DAG("data_pipeline", start_date=datetime(2021, 1 ,1), 
    schedule_interval='*/5 * * * *',
    dagrun_timeout=timedelta(seconds=5),
    default_args=default_args, 
    catchup=False) as dag:

    #Check if the website is working
    is_website_available = HttpSensor(
        task_id="is_information_available", 
        http_conn_id="meli_api", 
        endpoint="deportes-fitness/ciclismo/bicicletas/_Desde_51_NoIndex_True", 
        response_check=lambda response: "Bicicleta" in response.text, 
        poke_interval=5,
        timeout=20 
    )

    scraping_data = PythonOperator(
        task_id="scraping_data",
        python_callable=extract_information #se llama a la función creada para extraer la info

    )

    saving_data = BashOperator(
        task_id="saving_data",
        bash_command="""
            hdfs dfs -mkdir -p /bycicles && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/export.json /bycicles && \
            exit 99
        """
    )

    creating_bycicles_data_table = HiveOperator(
        task_id="creating_bycicles_data_table",
        hive_cli_conn_id="hive_coon",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS bycicles_data(
                Titles STRING, 
                Prices STRING, 
                Name STRING, 
                N_titles STRING, 
                N_prices STRING, 
                rin STRING, 
                brand STRING
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )
    
    bycicle_processing = SparkSubmitOperator(
        task_id="bycicle_processing",
        application="/opt/airflow/dags/scripts/spark_processing.py",
        conn_id="spark_conn",
        verbose=False,
        trigger_rule="all_done"
    )

    is_website_available >> scraping_data >> saving_data >> creating_bycicles_data_table >> bycicle_processing
    #saving_data >> creating_bycicles_data_table
    #saving_data >> creating_bycicles_data_table >> bycicle_processing