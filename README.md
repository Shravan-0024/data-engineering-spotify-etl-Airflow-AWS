# data-engineering-spotify-etl-Airflow-AWS
Built an end-to-end data pipeline to fetch Spotify's Top 50 Indian songs daily using Apache Airflow. Implemented two approaches: one using Airflow(using Airflow Docker Compose) for the entire ETL process with all Python code written directly in DAG tasks, and another using Airflow purely for orchestration by triggering AWS Lambda functions and Glue jobs.

Project Extension:
Extended the project by using AWS Glue Crawler and Amazon Athena instead of Snowflake for analysis.
Glue Crawler: Automatically updated table schema and metadata.
Athena: Used for fast, serverless SQL queries on S3 data.
This helped me understand how Glue and Athena work together for easy and low-cost data analysis.
