import json
import pwd
import boto3
import requests
import time
from datetime import datetime, timedelta
from airflow.models import Variable
import logging 
import os
import airflow
import yaml
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime 
import json
import boto3
from airflow.utils import dates as af_date_utils
from FDW.sns import utils_sns
from FDW.send_email_common import call_email_api


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '~/airflow')
DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')
PROJECT_FOLDER = os.path.join(DAGS_FOLDER, "FDW/")
DAG_DIR = os.path.abspath(os.path.dirname(__file__))
logging.info(f"Project_folder: {PROJECT_FOLDER} and DAG_DIR: {DAG_DIR}")
config_file = PROJECT_FOLDER + "config.yaml"
config = yaml.load(open(config_file, 'r'), Loader=yaml.SafeLoader)
schedule = Variable.get("FDW_falcon_hourly_schedule")
v_message = 'LDW ETL Job Failed' 

default_args = {
    "owner": "juno",
    "start_date": af_date_utils.days_ago(1),
    'depends_on_past': False,
    'email': [config['email']],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': utils_sns.sns_publish_on_failure_callback
 }    

def call_snowflake():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_ldw_dev')
    with hook.get_conn() as conn:
        c = conn.cursor()
        try:            
                email_recipients = c.execute("select EMAIL,'!There was an error during the LDW ETL process. Please see recid = '||recid||' in the CUR_ETHOS_LDW.CS_PROCESS_LOG table.' from CS_PROCESS_LOG_EMAIL where ERROR_CODE is not null and to_date(current_timestamp()) = to_date(RUN_DATE) limit 1").fetchall()
                if email_recipients is True:
                    res = [item[0] for item in email_recipients]
                    mess = [item[1] for item in email_recipients] 
                    email =str(res[0]).split(',')
                    email_message = str(mess[0]) 
                    call_email_api( v_message , email_message,  email)
                else:
                    pass
        finally:
                c.close()
dag = DAG(
    dag_id="ldw_etl_job_failure_email_test",
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    concurrency=1,
    max_active_runs=3,
    template_searchpath=[DAG_DIR]
)

with dag:
    ldw_email = PythonOperator(
        task_id='ldw_email',
        python_c
