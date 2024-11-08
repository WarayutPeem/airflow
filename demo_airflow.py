from airflow import configuration, DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

from controller.data_control import *
from controller.gcp_control import *
from controller.airflow_control import *
from controller.oracle_control import *
from controller.object_control import *
from controller.system_control import *

import pandas as pd
import datetime as dt

# MAIN PROCESS
# variable default value
type_process = 'incremental'
extenion_file = 'arvo'
chunk_size = 500
date_str = (dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y%m%d")
asatdate = str(dt.datetime.now().date() - dt.timedelta(days=1))
from_date = str(dt.datetime.now().date() - dt.timedelta(days=1))
to_date = str(dt.datetime.now().date())

# variable for gcp
gcs_json = './key/tqm-cdp-beta-d12083c2d017.json'
bucket_name = "test_arvo" # "tqm-cdp-beta-raw"
key = set_up_key('./key/key-beta.json')

object_process = [
    {
        "database_name" : 'OK',
        "list_table" : [
            'receiveitemclear'
            , 'sale'
            , 'saleaction'
            , 'saledata'
            , 'saleaddress'
            , 'salepayment'
            , 'customer'
            , 'customersale'
            , 'renewalnotice'
            , 'mapmembershipcust'
        ]
    },
    {
        "database_name" : 'SALE',
        "list_table" : [
            'leadassign'
            , 'leadcar'
            , 'lead'
            , 'leadaction'
            , 'leaddata'
            , 'leadtrack'
            , 'leadchatclient'
            , 'smsitem'
            , 'tqmappuser'
            , 'tqmappnoti'
            , 'web30tempsale'
            , 'chatcenter'
            , 'lineitem'
            , 'chatsurveyanswer'
            , 'membership'
            , 'membersale'
            , 'consent'
            , 'ecommsale'
        ]
    }
]


with (DAG(
        dag_id="demo_airflow_cdp",
        schedule_interval=None,
        # schedule_interval="45 21 * * *",
        start_date=dt.datetime(2024, 11, 8),
        catchup=True,
        max_active_runs=1,
        tags=['de', 'production', 'daily data', 'cdp', 'incremental data'],
        description='Oracle to GCS, type load incremental',
        render_template_as_native_obj=True,
        default_args={
            # 'pool': 'daily_datalake_pool',
            # 'on_failure_callback': ofm_task_fail_slack_alert,
            'retries': 1,
            'retry_delay': dt.timedelta(minutes=3),
        }
) as dag):
    start_task = DummyOperator(task_id="start_task")
    end_task = DummyOperator(task_id="end_task", trigger_rule='none_failed')

    # get macros value at current datetime of job
    task_gen_ds_main = PythonOperator(
        task_id=f"gen_ds_main",
        python_callable=gen_object_date,
        op_kwargs={
            'airflow_date': '{{ data_interval_end }}',
        }
    )

    task_clear_xcom = PythonOperator(
        task_id=f"clear_xcom",
        python_callable=clear_xcom_dags_id,
        # provide_context = True,
        trigger_rule="none_failed"
    )

    for obj_process in object_process:
        database_name = obj_process["database_name"].lower().strip()

        for table_name in obj_process["list_table"]:
            table_name = table_name.lower().strip()

            with TaskGroup(f'group_{table_name}') as task_group_tables:
                table_task = DummyOperator(task_id=f"table_task_{table_name }")

                table_task

            task_gen_ds_main >> task_group_tables >> task_clear_xcom >> end_task

    start_task >> task_gen_ds_main