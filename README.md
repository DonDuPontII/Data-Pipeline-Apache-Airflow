### Purpose
- Sparktify has requested automation and monitoring be integrated into their data warehouse ETL pipelines using Apache Airflow
- Data pipelines are to be dynamic and allow easy backfills
- Data quality tasks are to be established for data warehouse validity 

### Data context
- Two source dataset types (both are JSON) are stored in Amazon S3:
  - Log files refer to user application activity
  - Song files inform what users are listening to 
- Data warehouse lives in Amazon Redishift

### Files contained in the project
- SQL scripts
  - Create_tables.sql
    - Designs schemas for staging, dimension, and fact tables
  - Sql_queries.py
    - A class containing all sql statements to populate the dimension and fact tables
- Customized operators that connect to a Redshift cluster with a Postgres hook
  - Stage_redshift.py
    - Clears data from destination Redshift table if specified
    - Copies data from S3 to Redshift
  - Load_fact.py
    - Truncates data from destination Redshift table if specified
    - Load data into fact table
  - Load_dimension.py
    - Truncates data from destination Redshift table if specified
    - Load data into dimension table
  - Data_quality.py
    - Run data quality check on primary key duplication and if unwanted null values exist 
    - If duplication or unwanted null values exist, raise an error, retry, and eventually fail
- DAG
  - Udac_example_dag.py
    - Defines default arguments for DAG
    - Establishes scheduler to be ran hourly for automation
    - Configures customized operators for each task
    - Maps the task dependencies

### Process execution steps
1. Create an Amazon Redshift cluster
2. Execute create_tables.sql scripts in Amazon Redshift
3. Update the DAG and task parameters to match your AWS account and Redshift cluster 
4. In bash, execute command /opt/airflow/start.sh to start Airflow webserver
5. In Airflow's UI, configure your AWS credentials and connection to Redshift
6. Toggle on the DAG and observe the tasks being executed

