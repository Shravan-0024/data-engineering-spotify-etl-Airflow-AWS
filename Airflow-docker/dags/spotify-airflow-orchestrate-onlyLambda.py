from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta

default_args = {
    'owner': 'sannu',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 19),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag =  DAG(
    'lambda_trigger_dag',
    default_args=default_args,
    description='DAG to trigger Lambda functions and check S3 upload',
    schedule=timedelta(days=1),
    catchup=False) 

# Task to invoke the first Lambda function
trigger_first_lambda = LambdaInvokeFunctionOperator(
    task_id='trigger_extract_lambda',
    function_name='spotify_api_extract',
    aws_conn_id='aws-airlow-conn',
    region_name="us-east-1", 
    dag=dag,
)

# Task to check if data is uploaded to S3
check_s3_upload = S3KeySensor(
    task_id='check_s3_upload',
    bucket_key='s3://spotify-etl-project-sannu/raw_data/to_process/*',
    wildcard_match=True,
    aws_conn_id='aws-airlow-conn',
    timeout=60 * 60,  # wait for up to 1 hour
    poke_interval=60,  # check every 60 seconds
    dag=dag,
)

# Task to invoke the second Lambda function
trigger_second_lambda = LambdaInvokeFunctionOperator(
    task_id='trigger_transformation_lambda',
    function_name='spotify_transformation_load',
    aws_conn_id='aws-airlow-conn',
    region_name="us-east-1",
    dag=dag,
)



trigger_first_lambda >> check_s3_upload >> trigger_second_lambda