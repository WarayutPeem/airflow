from tink import daead, secret_key_access
from google.cloud import storage
import json, tink, base64
import pandas as pd


def set_up_key(key_json):
    with open(key_json) as fp:
        keyset = json.dumps(json.load(fp))

    keyset_handle = tink.json_proto_keyset_format.parse(keyset, secret_key_access.TOKEN)
    daead.register()
    
    return keyset_handle.primitive(daead.DeterministicAead)


def encrypt(key, secret):
    cipher = key.encrypt_deterministically(bytes(secret, encoding="utf-8"), b"")
    
    return str(base64.b64encode(cipher), encoding="utf-8")


def decrypt(key, encrypted):
    output = key.decrypt_deterministically(base64.b64decode(encrypted), b"")
    
    return str(output, encoding="utf-8")


def encrypt_cols_in_df(df, columns, key):
    df_copy = df.copy()
    for column in columns:
        df_copy[column] = df_copy[column].apply(lambda c: encrypt(key, c) if pd.notnull(c) else None)
        
    return df_copy


def decrypt_cols_in_df(df, columns, key):
    df_copy = df.copy()
    for column in columns:
        df_copy[column] = df_copy[column].apply(lambda c: decrypt(key, c) if pd.notnull(c) else None)
        
    return df_copy


def upload_to_gcs(gcs_json, bucket_name, destination_blob, path_file_table):
    # File detail
    # destination_blob_name = f"{table_name}/ASATDATE={asatdate}/{file_name}.{extenion_file}"
    
    # connect to GCS
    storage_client = storage.Client.from_service_account_json(gcs_json)

    # Get the GCS bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object from the filename
    blob = bucket.blob(destination_blob)

    # Upload the file to GCS
    blob.upload_from_filename(path_file_table, timeout=1800)
    
    # Close the connection
    storage_client.close()

    print(f"File {path_file_table} uploaded to {destination_blob}.")