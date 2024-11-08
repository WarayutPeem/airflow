from sqlalchemy import create_engine, text
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from dotenv import load_dotenv
import os, json, avro
import pandas as pd

load_dotenv()


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


def get_object_table_name(database_name, table_name, from_date, to_date):
    if database_name.lower().strip() == 'ok':
        database = os.getenv('constr_ok_etl')
        
    elif database_name.lower().strip() == 'sale':
        database = os.getenv('constr_sale_etl')
    
    # START Create object table per table
    object_all_table = {
        
        # table_name : supplier
        "supplier": {
            "select": """
                supplierid, suppliercode, suppliername
            """,
            "from": "etl.view_supplier",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : product
        "product": {
            "select": """
                productid, productcode, productname, producttype, productstatus,
                saledatefrom, saledateto, planname, packagename, salebookcode,
                netamount, coveramount, amount, vat, duty, vatrate, dutyrate, 
                coverperiod, coverperiodunit, coverperiodunit_des, productfor, 
                supplierid, productgroup
            """,
            "from": "etl.view_product",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : promotion
        "promotion": {
            "select": """
                supplierid, promotioncode, promotioncodesupplier, promotionname, insurelevel, 
                insurelevel_des, cover, coverv3rd, coverv3rdtime, coverv3rdasset, 
                coverdeduct1, coverdeduct2, coverext, coveraccd, coveraccp, coverpassenger, 
                coveracc2d, coveracc2p, covermedd, coverlegal, premiummain, premiumendose,
                premiumadd, discountage, discountdeduct, discountfleet, discountother,
                discountexp, standardpremium, amount, duty, vat,
                promotionstatus, promotionstatus_des, carusage, cargroup, carbrand,
                carmodel, yearmin, yearmax, promotiontype, startdate, enddate
            """,
            "from": "etl.view_promotion",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : receivecost
        "receivecost": {
            "select": """           
                receivecostcode, receivecostname, chargeto, interest, installment,
                bankcode, bankname, receivecostgroup, receivecostgroup_des
            """,
            "from": "etl.view_receivecost",
            "field_action": {
                "field_encrypt": {}           
            }
        },
        
        # table_name : result
        "result": {
            "select": """
                resultid, resultcode, resultname, resulttype
            """,
            "from": "etl.view_result",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : staff
        "staff": {
            "select": """
                staffid, staffcode, staffname, stafftype, stafftype_des,
                staffstatus, staffstatus_des, departmentid
            """,
            "from": "etl.view_staff",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : department
        "department": {
            "select": """
                departmentid, departmentcode, departmentname, masterid, levelid, companyid, 
                departmentgroup, departmentgroupsub, departmentgroup_mod, departmentgroupsub_mod
            """,
            "from": "etl.view_department",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : departmenttree
        "departmenttree": {
            "select": """
                departmentid, departmentcode, departmentname, levelid,
                subdepartmentid, subdepartmentcode, subdepartmentname, sublevelid
            """,
            "from": "etl.view_departmenttree",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : batchcodeassigninfo
        "batchcodeassigninfo": {
            "select": """
                batchcode, batchgroup, assignmonth, caryear, carmonth, carmonth_type
            """,
            "from": "etl.view_batchcodeassigninfo",
            "field_action": {
                "field_encrypt": {}        
            }
        },
        
        # table_name : chatsurvey
        "chatsurvey": {
            "select": """
                chatsurveyid, surveyname, surveytype, surveystatus,
                title1, question1, title2, question2, title3, question3, title4, question4, title5, question5
            """,
            "from": "etl.view_chatsurvey",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
        # table_name : sysbytedes
        "sysbytedes": {
            "select": """
                tablename, columnname, bytecode, bytedes
            """,
            "from": "etl.view_sysbytedes",
            "field_action": {
                "field_encrypt": {}
            }
        },
        
    }
    
    # get object with table name
    obj_table = object_all_table.get(table_name)
    
    # create other object from obj_table
    object_return = create_object_return(database, table_name, obj_table)
    
    if object_return:
        return object_return
    
    else:
        return None


def create_file_table_name(df, schema, path_file, file_name):
    # Convert DataFrame to records
    schema_parsed = avro.schema.parse(json.dumps(schema))

    # Write to files avro
    dict_df = df.to_dict(orient="records")
    with open(f'{path_file}/{file_name}.avro', 'wb') as f:
        writer = DataFileWriter(f, DatumWriter(), schema_parsed)
        [writer.append(row) for row in dict_df]
        writer.close()
        
    # clean data after done process
    df = pd.DataFrame()
    dict_df = []
    
    return f'{path_file}/{file_name}.avro'


def create_df(database, query):
    df = pd.read_sql(query, database, coerce_float=False)
    df = df.rename(columns={col: col.lower() for col in df.columns})
    
    return df