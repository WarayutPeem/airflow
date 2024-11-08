from config.source_data_full import get_object_by_table_full
from config.source_data_incremental import get_object_by_table_incremental

from sqlalchemy import create_engine, text
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import json, avro
import urllib.parse
import pandas as pd

constr_ok_etl ='oracle+cx_oracle://conetl:X1npr0et1@10.0.0.111:1521/OK'
constr_sale_etl = f'oracle+cx_oracle://conetl:{urllib.parse.quote('Pr0s@leet1')}@10.0.0.72:1521/SALE'

def create_object_value(asatdate, table_name, database_name, extenion_file, type_process):
    file_name = f"{table_name}" if table_name != 'sysbytedes' else f"{database_name}_{table_name}"
    path_file = f'./{extenion_file}/{table_name}/ASATDATE={asatdate}'

    destination_blob = ''
    if type_process.lower() == 'full':
        destination_blob = f"{table_name}/{file_name}.{extenion_file}"

    elif type_process.lower() == 'incremental':
        destination_blob = f"{table_name}/ASATDATE={asatdate}/{file_name}.{extenion_file}"

    return file_name, path_file, destination_blob


def create_object_return(database, table_name, obj_table):
    # create variable for process
    list_field = [field.strip() for field in obj_table['select'].replace("\n", "").split(",")]
    where_field = ", ".join([f"'{field.strip()}'" for field in list_field])

    sql = f"""
        select column_name, data_type, NVL(data_scale, 0) AS data_scale, nullable
        from all_tab_columns
        where lower(owner)||'.'||lower(table_name) = '{obj_table['from']}'
            and lower(column_name) in ({where_field})
        order by column_id
    """
    
    list_field_type_date = []
    # create schema
    schema = {
        "type": "record",
        "name": f"{table_name}",
        "fields": []
    }
    
    engine = create_engine(database)
    with engine.connect() as connection:
        transaction = connection.begin()
        
        result = connection.execute(text(sql))
        for row in result:
            column_name = row["column_name"].lower().strip()
            data_type = row["data_type"]
            nullable = row["nullable"]
            data_scale = row["data_scale"]
            
            # create data per row of schema.fields
            field_entry = {"name": column_name}

            # dynamic schema by data type from sysview
            if data_type == 'NUMBER':
                if data_scale == 0:
                    field_entry["type"] = ["null", "long"] if nullable == 'Y' else "long"
                else:
                    decimal_type = {"type": "bytes", "logicalType": "decimal", "precision": 15, "scale": data_scale}
                    field_entry["type"] = ["null", decimal_type] if nullable == 'Y' else decimal_type

            else:
                field_entry["type"] = ["null", "string"] if nullable == 'Y' else "string"
                if data_type == 'DATE':
                    list_field_type_date.append(column_name)

            # add field attribute of schema
            schema["fields"].append(field_entry)
                
        transaction.commit()
            
    # create query
    query = f"SELECT {obj_table['select']} FROM {obj_table['from']}"
    
    if obj_table.get('where'):
        query += f"""
            WHERE {obj_table['where']}
        """
    
    # create field_action
    field_action = obj_table['field_action']
    field_action['field_type_date'] = list_field_type_date if list_field_type_date else {}
    
    # create object_for_update
    object_for_update = obj_table['object_for_update'] if obj_table.get('object_for_update') else {}
    
    return database, query, schema, field_action, object_for_update


def create_file_table_name(df, schema, path_file, file_name, extenion_file):
    # Convert DataFrame to records
    schema_parsed = avro.schema.parse(json.dumps(schema))

    # Write to files avro
    dict_df = df.to_dict(orient="records")
    with open(f'{path_file}/{file_name}.{extenion_file}', 'wb') as f:
        writer = DataFileWriter(f, DatumWriter(), schema_parsed)
        [writer.append(row) for row in dict_df]
        writer.close()

    # clean data after done process
    df = pd.DataFrame()
    dict_df = []

    return f'{path_file}/{file_name}.{extenion_file}'


def create_df(database, query):
    df = pd.read_sql(query, database, coerce_float=False)
    df = df.rename(columns={col: col.lower() for col in df.columns})
    
    return df


def get_object_table_name(database_name, table_name, type_process, from_date, to_date):
    database = ''
    if database_name.lower().strip() == 'ok':
        database = constr_ok_etl

    elif database_name.lower().strip() == 'sale':
        database = constr_sale_etl

    # get object with table name
    if type_process == 'full':
        obj_table = get_object_by_table_full(table_name)

    else:
        obj_table = get_object_by_table_incremental(table_name)

    # create other object from obj_table
    object_return = create_object_return(database, table_name, obj_table)

    if object_return:
        return object_return

    else:
        return None