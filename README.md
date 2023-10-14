# Chiangmai Data Extraction, Transfomation and Load (ETL) with Apache Airflow
![ETL](https://github.com/wwdyKawinpat/ETL-cnx-report/assets/88372950/db61cb9b-1585-468e-9582-77dc77c7b108)

This project demonstrates how to use Apache Airflow for ingesting Chaingmai Weather data, extracting it, and loading it into a PostgreSQL database.

## Table of Contents
1. [Project Overview](#project-overview)
2. [Project Structure](#project-structure)
3. [Setup and Installation](#setup-and-installation)

## Project Overview

This project focuses on automating the data processing pipeline for ingesting data from Chiangmai, performing necessary extractions and ingest data, transformations, and loading the data into a PostgreSQL database. Utilizing Apache Airflow, a powerful open-source workflow automation tool, the project enables efficient data management and orchestration.

**Main Features**
  - **Automated Data Pipeline:**
  Apache Airflow orchestrates the entire data pipeline, allowing scheduled and automated execution of tasks.
  
  - **Chiangmai Data Ingestion:**
  The project provides functionality to fetch data from various sources in Chiangmai, ensuring comprehensive data collection.

  - **Data Transformation and Cleansing:**
  Transformations and cleansing tasks are implemented to prepare the data for effective storage and analysis.

  - **Data Loading into PostgreSQL:**
  Data is loaded into a PostgreSQL database for structured storage and retrieval.

## Project Structure

Explain the structure of your project. For example:
- `cnx_weather_pipelines.py` : Python file for for data extraction, transformation, and loading into the database.
- `dags/`: Airflow DAGs (Directed Acyclic Graphs) and tasks definition.
- `docker-compose.yml`: Docker Compose configuration for setting up the environment.
- `requirements.txt`: Python package dependencies for the project.

## Setup and Installation

Provide step-by-step instructions on how to set up and install the project. Include any prerequisites, environment setup, and configuration details.

1. Clone airflow docker-compose template repository:
```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.1.4/docker-compose.yaml'
```
2. Setting postgres port in docker-compose.yaml file.
```bash
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - 5432:5432
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always
```
*In this project I set other config as default.*

3. Build docker-compose from docker-compose.ymal.
```bash
docker-compose up
```
*FYI: Airflow will provide `Example DAGs`. If you don't need it set `AIRFLOW__CORE__LOAD_EXAMPLES: 'False`.*

4. When build was finished. We can access Airflow UI by localhost:port. For user to login `USER:airflow` `PASSWORD: airflow`
![image](https://github.com/wwdyKawinpat/ETL-cnx-report/assets/88372950/2d4a2151-5b5c-4396-b6ef-151476e895f7)


5. Create databse and table in postgres using DBeaver.

```bash
CREATE DATABASE report
```

```bash
  CREATE TABLE IF NOT EXISTS cnx_report(
      name VARCHAR (50) NOT NULL,
      weather_main VARCHAR (50) NOT NULL,
      sunrise_date DATE,
      sunrise_time TIMESTAMP ,
      sunset_date DATE,
      sunset_time TIMESTAMP,
      temp numeric NOT NULL,
      temp_feellike numeric NOT NULL,
      temp_max numeric NOT NULL,
      temp_min numeric NOT NULL,
      aqi numeric NOT NULL,
      population numeric NOT null);
```

7. Define DAGs. Python code for all tasks: `cnx_weather_piplines.py`.
```bash
with DAG('cnx-weather-pipeline',
        schedule_interval = '@daily',
        default_args = default_args,
        description = 'Chiangmai weather report',
        catchup = False ) as dag:
    
    t1 = PythonOperator(
        task_id = 'get_weather',
        python_callable=open_weather_extract
    )
    
    t2 = PythonOperator(
        task_id = 'convert_utc',
        python_callable=convert_utc_time
    )
    
    t3 = PythonOperator(
        task_id = 'get_population',
        python_callable=extract_cnx_pop
    )
    
    t4 = PythonOperator(
        task_id = 'get_aqi',
        python_callable=extract_aqi
    )
    
    t5 = PythonOperator(
        task_id='create_dataframe',
        python_callable=to_dataframe
    )
    
    t6 = PythonOperator(
        task_id = 'load_to_postgres',
        python_callable=load_to_db
    )
                
    t1 >> t2 >> t5
    [t3,t4] >> t5
    t5 >> t6
```
DAGs graph view.
![image](https://github.com/wwdyKawinpat/ETL-cnx-report/assets/88372950/8504d37e-2963-4650-90c1-1c652f902d27)

8. Query to check data in Postgres.
   ```bash
   SELECT * FROM cnx_report
   ```
   
<!-- Beginning of auto-generated table -->
   |         timestamp        |    name    |  weather_main | sunrise_date | sunrise_time |  sunset_date |  sunset_time |    temp   | temp_feellike |  temp_max |  temp_min  |  aqi  | population |
   |------------------------- |----------- |-------------- |------------- |------------- |------------- |------------- |---------- |-------------- |---------- |----------- |------ |----------- |
   |	2023-10-14 08:11:09.086 | Chiang Mai |	   Clouds    |  2023-10-14  |	  06:17:17   |	2023-10-14	|    18:02:43	 |   30.269  |     35.99     |	 32.779  | 	 29.189   |	  17	|   1202618  |

<!-- End of auto-generated table -->

The final result in postgresql.
