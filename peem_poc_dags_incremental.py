from controller.data_control import *
from controller.gcp_control import *
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

# # prod
# key_json = 'key-prod.json'
# gcs_json = 'tqm-cdp-prod-750e9ca88402.json'
# bucket_name = "tqm-cdp-prod-raw"

# print('date_str: ', date_str)
# print('asatdate: ', asatdate)
# print('from_date: ', from_date)
# print('to_date: ', to_date)


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

# ### START PROCESS
# get value from environment variables
for obj_process in object_process:
    database_name = obj_process["database_name"].lower().strip()
    
    for table_name in obj_process["list_table"]:
        table_name = table_name.lower().strip()
        # get query by table name
        obj_table = get_object_table_name(database_name, table_name, type_process, from_date, to_date)

        # create file information
        object_value = create_object_value(asatdate, table_name, database_name, extenion_file, type_process)

        # create new folder
        if_exists_folder(obj_table[2])

        # create dataframe from query
        df = create_df(obj_table[0], obj_table[1])

        # clean data
        df = clean_fix_type_data(df, obj_table[2], obj_table[3], key)

        # create file for upload to GCS
        path_file_table = create_file_table_name(df, obj_table[2], object_value[1], object_value[0], extenion_file)

        # Upload file avro to GCS
        upload_to_gcs(gcs_json, bucket_name, object_value[3], path_file_table)

        # Update data on etlchangedata
        update_table_control(obj_table[0], df, from_date, to_date, obj_table[4], chunk_size)

        # clean data for dataframe
        df = pd.DataFrame
