from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import json, avro, os, shutil
import pandas as pd


# clean file
def clean_file(path_file):
    if os.path.exists(path_file):
        os.remove(path_file)
        print(f"File {path_file} deleted locally.")
    else:
        print(f"The file {path_file} does not exist.")


# clean folder and file
def clean_folder(path_folder):
    if os.path.exists(path_folder) and os.path.isdir(path_folder):
        shutil.rmtree(path_folder)


# create folder for save all file
def if_exists_folder(path_full):
    if path_full:
        if not os.path.exists(path_full):
            os.makedirs(path_full, exist_ok=True)
            os.chmod(path_full, 0o777)

    return path_full


# create file arvo
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