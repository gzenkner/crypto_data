import pandas as pd
import os
import datetime
import logging
from google.cloud import storage
from cmcDatasetClass import Dataset
import gc
import io


import functions_framework

logger = logging.getLogger('my_logger')

def print_and_log(string):
    logger.info(string)
    print(string)

def replace_dot_with_underscore(df):
    for col in df.columns:
        new_col = col.replace('.', '_')
        df.rename(columns={col: new_col}, inplace=True)

@functions_framework.http
# def hello_http(request):
def hello_http():


    extraction_date = datetime.datetime.now().date()
    file_name = f'coinMarketCapDaily_{extraction_date}.parquet'


    bucket='coin_market_cap'
    local_path = f'/tmp/{file_name}'

    df = blob_string_to_df(bucket, file_name)
    df_processed = etl(df)

    df_to_local(df_processed, local_path)
    local_to_gcs(file_name, local_path, bucket)
    
    try:
        os.remove(file_name)
    except:
        print_and_log('nothing to remove')

        print_and_log('Successfully saved DataFrame to Parquet and wrote to GCS')

    return f'Successfully saved DataFrame to Parquet and wrote to GCS'


def print_and_log(string):
    logger.info(string)
    print(string)

def blob_string_to_df(bucket, file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)

    blob = bucket.blob(file_name)
    data = blob.download_as_string()

    return pd.read_parquet(io.BytesIO(data))

def etl(df):
    dataset = Dataset()
    dataset.parse_header_raw_data(df)
    dataset.get_unique_headers_with_frequency()
    df_new = dataset.joined_arrays_df()
    gc.collect()
    
    return df_new

def df_to_local(df, destination):
    df = df.astype(str)
    replace_dot_with_underscore(df)
    df.to_parquet(destination)
    print_and_log(f'Saved parquet file locally')

def local_to_gcs(destination_blob_name, source_parquet_name, bucket):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_parquet_name)

hello_http()