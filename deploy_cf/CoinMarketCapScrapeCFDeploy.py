from bs4 import BeautifulSoup
import requests
import math
import re
import json
import pandas as pd
import os
import datetime
import logging
from google.cloud import storage
import pandas_gbq

import functions_framework

logger = logging.getLogger('my_logger')

def print_and_log(string):
    logger.info(string)
    print(string)



@functions_framework.http
def hello_http(request):
# def hello_http():

    extraction_date = datetime.datetime.now().date()
    total_pages = get_number_of_active_pages_cmc()
    d = create_dict_from_pages(total_pages)
    date_page_dict = extract_key_values_from_dict(d, total_pages)
    file_name = f'coinMarketCapDaily_{extraction_date}.parquet'
    local_path = f'/tmp/{file_name}'
    record_count = write_dict_to_parquet_on_local(local_path, date_page_dict)

    bucket='coin_market_cap'
    local_to_gcs(file_name, local_path, bucket)

    metadata_to_bigquery(extraction_date, record_count)
    
    try:
      os.remove(file_name)
    except:
      print_and_log('nothing to remove')

    print_and_log('Successfully saved DataFrame to Parquet and wrote to GCS')

    return f'Successfully saved DataFrame to Parquet and wrote to GCS'



def get_number_of_active_pages_cmc():
    base_url = "https://coinmarketcap.com/?page=1"

    response = requests.get(base_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    json_data = json.loads(script_tag.contents[0])
    props = json_data.get("props", {})
    props['initialState'] = json.loads(props['initialState'])

    page = int(props['initialState']['cryptocurrency']['listingLatest']['page'])
    total_items = int(props['initialState']['cryptocurrency']['listingLatest']['totalItems'] )
    page_size = int(props['initialState']['cryptocurrency']['listingLatest']['pageSize'])
    total_pages = math.ceil(total_items / page_size)
    print_and_log(f'total number of pages: {total_pages}')
    return total_pages


def create_dict_from_pages(total_pages):
    """"
    returns a dict where the page is the key"""
    d = {}
    for page in range(1, total_pages+1):
        response = requests.get(f'https://coinmarketcap.com/?page={page}')
        soup = BeautifulSoup(response.content, 'html.parser')
        script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
        json_data = json.loads(script_tag.contents[0])
        props = json_data.get("props", {})
        props['initialState'] = json.loads(props['initialState'])

        d[page] = {
            'headers':props['initialState']['cryptocurrency']['listingLatest']['data'][0]['keysArr'],
            'data':props['initialState']['cryptocurrency']['listingLatest']['data'][1:],
        }
    print_and_log(f' {len(d)}/{total_pages} page dictionaries created')
    return d


def generate_pk(page):
    return str(datetime.datetime.now().date())+'-page-'+str(page)


def extract_key_values_from_dict(d, pages):
    date_page_dict = {}

    for page in range(1, pages + 1): 
        date_page_dict[generate_pk(page)] = [] 

        for idx, coin_data in enumerate(d[page]['data']):   
            try:
                symbol = [i for i in coin_data if isinstance(i, str) and len(str(i)) in [1, 2, 3, 4, 5] and str(i) != 'USD' and i.isupper()][0]
            except IndexError:
                symbol = 'NA'
            
            dates = [s for s in coin_data if isinstance(s, str) and re.match(r'\d{4}-\d{2}-\d{2}T', str(s))]
            dateAdded = dates[0]
            lastUpdated = dates[1]
            quote_USD_lastUpdated = dates[2]
            isActive = [i for i in coin_data if isinstance(i, bool)][0]

            date_page_dict[generate_pk(page)].append({
                'page_idx':idx,
                'sym': symbol,
                'dateAdded': dateAdded,
                'lastUpdated': lastUpdated,
                'quote_USD_lastUpdated': quote_USD_lastUpdated,
                'isActive': isActive,
                'header': d[page]['headers'],
                'raw_data': coin_data
        
            })
    print_and_log('task extract_key_values_from_dict complete')
    return date_page_dict


def write_dict_to_parquet_on_local(destination, date_page_dict):
    l = []
    for key, values in date_page_dict.items():

        for value in values:
            pk = str(key)+'-idx-'+str(value['page_idx'])
            l.append([pk, value['sym'], value['dateAdded'], value['lastUpdated'], value['quote_USD_lastUpdated'], value['isActive'], value['header'], value['raw_data']])

    df = pd.DataFrame(
        columns = ['pk', 'sym', 'dateAdded', 'lastUpdated', 'quote_USD_lastUpdated', 'isActive', 'header', 'raw_data'],
        data = l
    )
    df = df.astype(str)
    df.to_parquet(destination)
    coin_count = len(df)
    print_and_log(f'{coin_count} coins found')
    print_and_log(f'Saved parquet file locally')
    return coin_count


def local_to_gcs(destination_blob_name, source_parquet_name, bucket):
    # GOOGLE_APPLICATION_CREDENTIALS = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    # storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_parquet_name)
    print_and_log('Moved local parquet file to GCS')



def metadata_to_bigquery(extraction_date, record_count):
    PROJECT_ID = os.environ.get('PROJECT_ID')
    df = pd.DataFrame(
        columns = ['extraction_date', 'record_count'],
        data = [[extraction_date, record_count]]
    )
    df = df.astype(str)
    print_and_log(f'Metadata: {df}')
    try:
        pandas_gbq.to_gbq(
            destination_table='coinmarketcap.pipelinesummary',
            project_id=PROJECT_ID,
            if_exists='append',
            dataframe=df
            )
        print_and_log(f'Data written successfully')
    except Exception as e:
        logger.error(f'Error occurred: {e}')

# hello_http()