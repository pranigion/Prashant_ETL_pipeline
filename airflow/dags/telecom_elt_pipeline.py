
"""
Telecom ELT Pipeline - Airflow DAG
Author: Prashant Pathak
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import os
import socket

default_args = {
    'owner': 'prashant-pathak',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['prashantpathak4144@gmail.com'],
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'telecom_elt_pyspark_pipeline',
    default_args=default_args,
    description='Scalable ELT Pipeline with PySpark',
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['elt', 'pyspark', 'scalable'],
)

CSV_FILE = '/opt/airflow/data/raw/telecom_churn.csv'

def task_ingest_bronze(**context):
    """Bronze Layer: CSV to PostgreSQL"""
    logger = logging.getLogger(__name__)
    logger.info("="*70)
    logger.info("BRONZE LAYER: CSV Ingestion")
    logger.info("="*70)
    
    if not os.path.exists(CSV_FILE):
        raise FileNotFoundError(f"CSV not found: {CSV_FILE}")
    
    df = pd.read_csv(CSV_FILE)
    initial_count = len(df)
    logger.info(f"Read {initial_count:,} rows")
    
    df = df.drop_duplicates()
    logger.info(f"Removed {initial_count - len(df)} duplicates")
    
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        if df[col].isnull().any():
            median_val = df[col].median()
            df[col].fillna(median_val, inplace=True)
    
    categorical_cols = df.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        df[col].fillna('Unknown', inplace=True)
    
    df['ingestion_timestamp'] = datetime.now()
    df['source_file'] = os.path.basename(CSV_FILE)
    df['pipeline_run_id'] = context['run_id']
    
    df.columns = [c.lower() for c in df.columns]
    
    hook = PostgresHook(postgres_conn_id='postgres_staging')
    engine = hook.get_sqlalchemy_engine()
    
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE bronze.raw_telecom_churn")
    
    df.to_sql('raw_telecom_churn', engine, schema='bronze',
              if_exists='append', index=False, chunksize=1000)
    
    logger.info(f"Loaded {len(df):,} rows to bronze.raw_telecom_churn")
    logger.info("="*70)
    
    return {'rows_loaded': len(df)}

def task_validate(**context):
    """Validate Gold layer"""
    logger = logging.getLogger(__name__)
    logger.info("="*70)
    logger.info("VALIDATION")
    logger.info("="*70)
    
    hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    checks = {
        'dim_customer': "SELECT COUNT(*) FROM gold.dim_customer",
        'dim_service': "SELECT COUNT(*) FROM gold.dim_service",
        'dim_contract': "SELECT COUNT(*) FROM gold.dim_contract",
        'fact_churn': "SELECT COUNT(*) FROM gold.fact_customer_churn",
    }
    
    for name, query in checks.items():
        result = hook.get_first(query)
        count = result[0] if result else 0
        logger.info(f"{name}: {count:,} rows")
        assert count > 0, f"{name} is empty!"
    
    logger.info("All checks PASSED!")
    logger.info("="*70)
    return True

bronze_task = PythonOperator(
    task_id='bronze_ingest',
    python_callable=task_ingest_bronze,
    dag=dag,
)


silver_task = SparkSubmitOperator(
    task_id='silver_pyspark',
    application='/opt/airflow/spark/jobs/silver_transformation.py',
    conn_id='spark_default',
    packages='org.postgresql:postgresql:42.7.1',


    executor_cores=1,
    num_executors=1,
    executor_memory= '512m', #'1g', # change as per your needs
    driver_memory='512m', #'1g',     # change as per your needs

    conf={
        'spark.submit.deployMode': 'client',
        'spark.driver.host': socket.gethostbyname(socket.gethostname()),
        'spark.executor.memoryOverhead': '256m',
        'spark.driver.memoryOverhead': '256m',
        'spark.default.parallelism': '1',
        'spark.sql.shuffle.partitions': '1',
    },
    dag=dag,
)

gold_task = SparkSubmitOperator(
    task_id='gold_pyspark',
    application='/opt/airflow/spark/jobs/gold_star_schema.py',
    conn_id='spark_default',
    packages='org.postgresql:postgresql:42.7.1',

    executor_cores=1,
    num_executors=1,
    executor_memory= '512m', #'1g', # change as per your needs
    driver_memory= '512m', #'1g',  # change as per your needs

    conf={
        # 'spark.master': 'spark://spark-master:7077',
        'spark.submit.deployMode': 'client',
        'spark.driver.host': socket.gethostbyname(socket.gethostname()),
        'spark.executor.memoryOverhead': '256m',
        'spark.driver.memoryOverhead': '256m',
        'spark.default.parallelism': '1',
        'spark.sql.shuffle.partitions': '1',
    },
    dag=dag,
)



validate_task = PythonOperator(
    task_id='validate_gold',
    python_callable=task_validate,
    dag=dag,
)

bronze_task >> silver_task >> gold_task >> validate_task