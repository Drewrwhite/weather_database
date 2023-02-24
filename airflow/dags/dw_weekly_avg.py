from datetime import datetime, date, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from google.cloud.exceptions import NotFound
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

PROJECT_ID = "team-week3"
DATASET_ID = "weather_dw"
DAILY_TABLE_ID = "daily"
WEEKLY_TABLE_ID = "weekly_avg"

SCHEMA = [
    bigquery.SchemaField("location", "STRING", "REQUIRED"),
    bigquery.SchemaField("city", "STRING", "NULLABLE"),
    bigquery.SchemaField("state", "STRING", "NULLABLE"),
    bigquery.SchemaField("lat", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("lon", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("temp_f_avg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("temp_c_avg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("humidity_avg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("barometer_avg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("dewpoint_f_avg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("dewpoint_c_avg", "FLOAT", "NULLABLE"),
    bigquery.SchemaField("modified_at", "TIMESTAMP", "NULLABLE")]


@task
def calculate_weekly_averages():
    client = bigquery.Client()
    query = f"""
    SELECT location, city, state, lat, lon,
        ROUND(AVG(temp_f), 1) AS temp_f_avg,
        ROUND(AVG(temp_c), 1) AS temp_c_avg,
        ROUND(AVG(humidity), 1) AS humidity_avg,
        ROUND(AVG(barometer), 1) AS barometer_avg,
        ROUND(AVG(dewpoint_f), 1) AS dewpoint_f_avg,
        ROUND(AVG(dewpoint_c), 1) AS dewpoint_c_avg,
        CURRENT_TIMESTAMP() AS modified_at
    FROM `{PROJECT_ID}.{DATASET_ID}.{DAILY_TABLE_ID}`
    WHERE DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) <= DATE(last_update)
    GROUP BY location, city, state, lat, lon
    """
    job_config = bigquery.QueryJobConfig(use_query_cache=True)
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    weekly_df = results.to_dataframe(bqstorage_client=None)
    data = weekly_df.to_json(orient='records')
    return data

@task
def write_weekly_avg_to_bq(data):

    df = pd.read_json(data, orient='records')

    client = bigquery.Client()

    try:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = client.get_dataset(dataset_ref)
    except NotFound:
        dataset_ref = client.dataset(DATASET_ID)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)

    table_ref = dataset.table(WEEKLY_TABLE_ID)

    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=SCHEMA)
        table = client.create_table(table)

    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


@dag(
    'weekly_avg',
    description='Calculates weekly averages of weather data in bigquery',
    start_date=days_ago(2),
    schedule_interval='0 0 * * *',
)

def weekly_avg_to_bq():
    # Define tasks
    calculate_weekly_avg_task = calculate_weekly_averages()
    write_weekly_avg_task = write_weekly_avg_to_bq(calculate_weekly_avg_task)
    done = DummyOperator(task_id='done')

    # Define task dependencies
    calculate_weekly_avg_task >> write_weekly_avg_task >> done

dag = weekly_avg_to_bq()


