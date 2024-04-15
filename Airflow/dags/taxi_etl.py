from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from tasks import *
from create_dashboard import create_dashboard

input_dataset_filename =  "green_tripdata_2017-11.csv"
cleaned_dataset_filename = "cleaned_dataset.csv"

# Defining the default_args arg to be passed to the DAG constructor
default_args = {
    "owner": "islam",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 0
}

# Defining our cleaning workflow using conext manager
with DAG(
    dag_id = "NYC_Taxi_data_pipeline",
    description= "Cleaning the NYC Taxt Dataset, loading it into postgres and performing some visualizaitons",
    default_args=default_args,
    schedule_interval='@once'
) as dag:
    extract_data = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_clean,
        op_kwargs={
            "input_filename": input_dataset_filename,
            "output_filename": cleaned_dataset_filename
        }
    )

    extract_gps_locations = PythonOperator(
        task_id = "extract_gps",
        python_callable= extract_additional_resources,
        op_kwargs = {
            "transformed_csv_filename": cleaned_dataset_filename
        }
    )

    integrate_data_sources = PythonOperator(
        task_id= "integration",
        python_callable= integrate_and_load,
        op_kwargs={
            "transformed_csv_filename": cleaned_dataset_filename
        }
    )

    visualize = PythonOperator(
        task_id = "visualization",
        python_callable= create_dashboard,
         op_kwargs={
            "transformed_csv_filename": cleaned_dataset_filename
        }
    )


    extract_data >> extract_gps_locations >> integrate_data_sources >> visualize