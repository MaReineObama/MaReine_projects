"""
Example Airflow DAG for Google BigQuery service.
"""
from airflow import models
# models used in BQ for interaction
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryExecuteQueryOperator,
    BigQueryUpdateTableOperator,
    BigQueryDeleteTableOperator,
    BigQueryInsertJobOperator,
    BigQueryExecuteQueryOperator,
    BigQueryCheckOperator
)
from airflow.providers.google.cloud.sensors.bigquery import (
    BigQueryTableExistenceSensor,
)

from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from google.cloud import bigquery
from google.cloud import storage 
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, timezone, date
import json.decoder
import pandas as pd
import requests
import json
import sys
import os
import ast
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

PROJECT_ID = "<project_id"
DESTINATION_DATASET_NAME = "<destination_dataset_name>"
Table_name = "<destination_table_name>" + str(date.today())
API_key = "<Tead_API_key>"
report_id = 8888
bucket_name = "<destination_bucket_name>" 
client = bigquery.Client(PROJECT_ID)
storage_client = storage.Client()



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


def run_id(key, id):
  url_sr = f" https://ads.teads.tv/api/reports/{id}/run"

  headers = {"Authorization": key,
            "Content-Type":"application/json",
            "Accept": "application/json"}

  Body = {
      "filters": {
      "date": {
        "start": (datetime.now() - timedelta(days=1)).astimezone().isoformat(),
        "end": datetime.now().astimezone().isoformat(),
        "timezone": "Etc/GMT+0"
      },
      "demand_source": "connect"
      }
                }


  r_sr = requests.post(url_sr, headers=headers)


  pretty_json_sr = json.loads(r_sr.text)
  return pretty_json_sr


def get_report(key, ti):
    id = ti.xcom_pull(task_ids=['retrieve_run_id'])[0]['id']
    url_get_sr = f"https://ads.teads.tv/api/reports/status/{id}"

    if not ti.xcom_pull(task_ids=['retrieve_run_id']):
        raise ValueError("No value stored in xcoms.")
    
    headers_get = {"Authorization": key}

    r_final_sr = requests.get(url_get_sr, headers=headers_get)
    json_final_sr = json.loads(r_final_sr.text)
    
    while str(json_final_sr['status']) == 'processing':
        try:

            r_final_sr = requests.get(url_get_sr, headers=headers_get)
            json_final_sr = json.loads(r_final_sr.text)


        except json.decoder.JSONDecodeError as e:
            print('error', e)

    
        if str(json_final_sr['status']) == 'finished':
            break
        
        
    return json_final_sr


def get_file(ti):
    # ti = kwargs['ti']
    url_download_sr = ti.xcom_pull(task_ids = ['get_report_link'])[0]['uri']
    f_sr = requests.get(url_download_sr, allow_redirects=True)
    return f_sr.text


def create_table(ti):
    report = ti.xcom_pull(task_ids = ['report_download'])[0]
    columns = []
    for col in report.split('\n')[0].split(','):
        columns.append(col)
    print(columns)

    #data
    data_list = []
    for liste in report.split('\n'):
        data_list.append(liste.split(','))
    data_list[1:]

    report_df = pd.DataFrame(data=data_list[1:], columns=columns)

    #cleansing
    int_col = ["Video starts", "Video completes", "Video first quartile", "Video third quartile", "Video midpoint" , "Clicks", "Impressions"]
    for col in int_col:
        report_df[col] = report_df[col].fillna('0').astype(int)

    report_df['Budget spent'] = report_df['Budget spent'].fillna('0.0').astype(float)

    string_col = [col for col in report_df if report_df.dtypes[col] not in ['float64', 'int64']]
    for col in string_col:
        report_df[col] = report_df[col].apply(lambda x: str(x).strip('\"')).fillna('').astype(str)

    report_df = report_df.rename(columns={'Day':'Date'})
    report_df = report_df[report_df['Date'] != '']
    report_df['Date'] = report_df['Date'].apply(lambda x: str(x).split('T')[0])
    report_df['Date'] = pd.to_datetime(report_df['Date'], utc=False).dt.date
    print(report_df.dtypes)

    report_df.columns = report_df.columns.str.replace(' ', '_')

    print(report_df.columns)

    table_id = f"{PROJECT_ID}.{DESTINATION_DATASET_NAME}.{Table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect= True

        )



    job = client.load_table_from_dataframe(
        report_df, table_id, job_config=job_config

        )

    job.result()



    table = client.get_table(table_id)  

    print(

        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id

            )

        )

def cleaning_data_function():
    query = (
    f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{DESTINATION_DATASET_NAME}.{Table_name}` AS "
    "SELECT CAST(Date AS DATE) AS Date, Seat_name, Country_name, Campaign_name, Line_item_name, Creative_external_integration_code, Budget_spent_currency, Budget_spent, Video_first_quartile, "
    f"Video_third_quartile,Video_midpoint, Video_starts, Video_completes, Clicks, Impressions FROM `{PROJECT_ID}.{DESTINATION_DATASET_NAME}.{Table_name}`"
    )

    job_config = bigquery.QueryJobConfig()
    sql = query
    query_job = client.query(sql, job_config=job_config)
    query_job.result()


def load_to_storage(ti):
    report = ti.xcom_pull(task_ids = ['report_download'])[0]
    bucket_name = "dspdata" 
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'Teads/{Table_name}.csv')
    with blob.open("w") as f:
        f.write(report)


with models.DAG(
        "Teads_Weekly_data_Staging",
        default_args=default_args,
        schedule_interval="45 4 * * Mon",  
        tags=["Staging_React"],
        template_searchpath=['/home/airflow/gcs/dags/sql/staging',
                             '/home/airflow/gcs/dags/sql/warehouse/adobe/creating_inserting_data/emea'
                             #'/home/airflow/gcs/dags/sql/maintenance_files/staging',
                             #'/home/airflow/gcs/dags/sql/maintenance_files/warehouse/adobe/creating_inserting_data/emea'
                             ],
        catchup=False
        ) as dag:


    get_run_id = PythonOperator(
        task_id = 'retrieve_run_id',
        python_callable = run_id,
        op_args = [API_key, report_id],
        do_xcom_push=True
        )



    get_url_report = PythonOperator(
        task_id = 'get_report_link',
        python_callable = get_report,
        op_kwargs = {'key': API_key},
        do_xcom_push=True
        )


    download_report = PythonOperator(
        task_id = 'report_download',
        python_callable = get_file,
        do_xcom_push = True
        )
    
    
    create_table_report = PythonOperator(
        task_id = 'create_teads_table',
        python_callable = create_table
        )
    
    cleaning_data = PythonOperator(
        task_id = 'cleansing_table',
        python_callable = cleaning_data_function
    )

    create_storage_file = PythonOperator(
        task_id = 'load_csv_to_storage',
        python_callable = load_to_storage
    )

get_run_id >> get_url_report >> download_report >> [create_table_report, create_storage_file] >> cleaning_data



