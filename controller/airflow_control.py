from airflow import settings
from airflow.models import XCom

import datetime as dt
import pandas as pd


def create_df(database, query):
    df = pd.read_sql(query, database, coerce_float=False)
    df = df.rename(columns={col: col.lower() for col in df.columns})

    return df


def gen_object_date(airflow_date):
    date_action = dt.datetime.strptime(
        airflow_date.add(hours=7).strftime("%Y-%m-%d %H:%M:%S"), '%Y-%m-%d %H:%M:%S'
    )
    print("Date Action :", type(date_action), date_action)

    # create variable of date
    date_str = (date_action - dt.timedelta(days=1)).strftime("%Y%m%d")
    asatdate = str(date_action.date() - dt.timedelta(days=1))
    from_date = str(date_action.date() - dt.timedelta(days=1))
    to_date = str(date_action.date())

    # แสดงผลลัพธ์ของค่าที่ได้
    print("Date String:", date_str)
    print("As At Date:", asatdate)
    print("From Date:", from_date)
    print("To Date:", to_date)

    # return ds_main


## STF Clear Xcom per tasks
def clear_xcom_dags_id(**kwargs):
    task_instance = kwargs['ti']
    dag_id = task_instance.dag_id

    # Get the session and clear all XComs for the specified task instance
    session = settings.Session()

    print("Dag ID:", dag_id)
    session.query(XCom).filter( XCom.dag_id == dag_id ).delete()

    # Commit the changes
    session.commit()

    # Close the session
    session.close()