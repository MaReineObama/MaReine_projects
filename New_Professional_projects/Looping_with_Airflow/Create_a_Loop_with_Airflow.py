"""
Example Airflow DAG for Google BigQuery service.


This use case is an example of our to insert data from multiple source table using the Airflow loop.

In this exercise. With insert into the table <destination_table> the data from the source tables named as follows <source_table{iso}_name>'. 
Iso represents the country iso code. Each source table contains campaign data of an european country representing a Nissan's market. (eg: iso = 'de' for the German Market)
"""
from asyncore import write
from genericpath import exists
from pickle import FALSE, TRUE
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
from google.cloud import bigquery
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

PROJECT_ID = "<project_id>"
SOURCE_DATASET_NAME = "<source_dataset>"
DESTINATION_DATASET_NAME_STAGING = "<destination_dataset_stg>"
DESTINATION_DATASET_NAME_DWH = "<destination_dataset_dwh>"
TABLE1 = "<destination_table>"

client = bigquery.Client(PROJECT_ID)

quality_check_country = ['in']
source_tables = client.list_tables(SOURCE_DATASET_NAME)  # Make an API request.
source_tables = list(source_tables)

liste_amio_tables = [table.table_id for table in source_tables if 'amio' in table.table_id and 'campaign' in table.table_id and 'STAGE' not in table.table_id]
liste_amio_table_id = [ids.split('_')[2] for ids in liste_amio_tables]

liste_amio_table_id = set(liste_amio_table_id)
list_of_countries = []

for i in liste_amio_table_id:
    list_of_countries.append(i[6:])


def custom_failure_function(context):
    "Define custom failure notification behavior"
    dag_run = context.get("dag_run")
    task_instances = dag_run.get_task_instances()
    print("These task instances failed:", task_instances)


# Data Manipulation and Creation set for Table1 ===============================>>
location = 'EU'
staging_table_schema = [
    {"name": "date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "trackingcode", "type": "STRING", "mode": "NULLABLE"},
    {"name": "bounces", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "pageviews", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "uniquevisitors", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "visits", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_1", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_2", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_3", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_4", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_5", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_6", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_7", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_8", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_9", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_10", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_11", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_12", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_13", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_14", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_15", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_16", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_17", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_18", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_19", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_20", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cm1006_21", "type": "INTEGER", "mode": "NULLABLE"},
]

dwh_table_schema = [

    {"name": "date", "type": "DATE", "mode":"NULLABLE"},
    {"name": "country", "type": "STRING", "mode":"NULLABLE"},
    {"name": "trackingcode", "type": "STRING", "mode":"NULLABLE"},
    {"name": "uniquevisitors", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "bounces", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "pageviews", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "visits", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "Qualified_Visits", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "KBA", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "Lead", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "K_Configurator_Engagement", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "K_Vehicle_Brochure_Download", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "K_Offer_Engagement", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "K_Compare_tool_Interactions", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "K_Dealer_Search", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "Configurator_Completion", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "K_Dealer_Contact", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "K_Click_to_Dealer_Website", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Contact_Dealer", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Test_Drive_Request", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Book_a_Service", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Reserve_Online", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Request_a_Brochure", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Request_a_Quote", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Keep_Me_Informed", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Request_a_Call_Back", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Offer_Details_Request", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "L_Video_Call_Request", "type": "INTEGER", "mode":"NULLABLE"},
    {"name": "channel_type", "type": "STRING", "mode":"NULLABLE"}
]


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2020, 1, 1),
    'retries':2,
    'retry_delay':timedelta(minutes=3),
    'email': ['marie.obama@nissanunited.com'],
    'email_on_failure': True,
    'on_failure_callback': custom_failure_function,

}

dwh_table = {
    '<dwh_table>': '<staging_table>',
}
yesterday_date = (datetime.now() - timedelta(days=1)).date()

with models.DAG(
        "Adobe_CampaignPerformance_amio_Staging_gravity",
        default_args=default_args,
        schedule_interval="45,15 6,9 * * *",  # Override to match your needs
        tags=["Staging_React"],
        template_searchpath=['/home/airflow/gcs/dags/sql/staging',
                             '/home/airflow/gcs/dags/sql/warehouse/adobe/creating_inserting_data/amio'],
        catchup=False,
) as dag:

    create_staging_table = BigQueryCreateEmptyTableOperator(
        task_id = f"create_table_{TABLE1}",
        dataset_id = DESTINATION_DATASET_NAME_STAGING,
        table_id = TABLE1,
        schema_fields = staging_table_schema,
        exists_ok = True
    )



    check_the_adobecamapignperf_data = BigQueryCheckOperator(
        task_id=f"check_row_data_{TABLE1}",
        sql=f"SELECT count(*) from {PROJECT_ID}.{DESTINATION_DATASET_NAME_STAGING}.{TABLE1} where date='{yesterday_date}'",
        use_legacy_sql=False,

    )

    for iso in list_of_countries:
        ingesting_data = BigQueryExecuteQueryOperator(
            task_id=f'ingesting_the_data_for_{iso}',
            write_disposition='WRITE_APPEND',
            sql= 'query_staging.sql',
            use_legacy_sql=False,
            params={
                'into_table_name': TABLE1,
                'iso_code': iso,
                'from_table_name': f'<project_id>.<source_dataset>.source_table{iso}_name'
            },

        )
        
        create_staging_table >> ingesting_data >> check_the_adobecamapignperf_data

    
    for table_name, staging_table in dwh_table.items():
        create_dwh_table = BigQueryCreateEmptyTableOperator(
        task_id = f"create_table_{table_name}",
        dataset_id = DESTINATION_DATASET_NAME_DWH,
        table_id = table_name,
        schema_fields = dwh_table_schema,
        exists_ok = True
        )

        Staging_to_AdobeWarehouse = BigQueryExecuteQueryOperator(
            task_id=f'inserting_data_into_{table_name}',
            sql=f'{table_name}.sql',
            use_legacy_sql=False,
            params={
                'table_name': table_name,
                'staging_table': staging_table
            },
        )
    check_the_adobecamapignperf_data >> create_dwh_table >> Staging_to_AdobeWarehouse