#!/usr/bin/python
'''
Sript to download gdelt data and save to s3.
'''
from __future__ import print_function
from __future__ import unicode_literals
import os
import io
import sys
import time
import zipfile
import requests
import argparse
import datetime
import pandas as pd
import requests
import lxml.html as lh
import re
from tqdm import tqdm
import pickle
import fastparquet
import datetime
import logging
import psycopg2
import boto
from configs import db_config,aws_config

def get_list_of_urls():
    '''
    Gets a list of gdelt archives in the form of URLs.
    INPUT:
        - NA
    OUTPUT:
        - masterlist_urls: List
                           URLs to obtain zipped CSVs.
    '''
    # Base directory for gdelt version 2 files
    gdelt_2_events_base_url = 'http://data.gdeltproject.org/gdeltv2/'

    # Directory containing all urls of historical data
    masterList_page = requests.get(gdelt_2_events_base_url+'masterfilelist.txt')
    print(type(masterList_page))

    # Requests output as text
    masterList = masterList_page.text
    print(len(masterList))

    # Extract URLs from text with regex
    masterList_urls = re.findall(r'(http|ftp|https)(://)([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:/~+#-]*[\w@?^=%&/~+#-])?',
                                 masterList)
    print(len(masterList_urls))

    # Concatenate regex groups to unified url strings
    masterList_urls = list(map(lambda x: ''.join(x), masterList_urls))
    print(masterList_urls[0])

    return masterList_urls

def download_zip_in_chunks(directory, url):
    '''
    Downloads a zipped file in chunks.
    INPUT:
        - directory: String
                     Directory to write downloaded file.
        - url: String
               URL of the file to download.
    OUTPUT:
        - local_file: String
                      Filepath where contents were written.

    '''
    # Unique name of file at the end of a URL
    base_file = os.path.basename(url)
    print('\n\nDOWNLOADING {}'.format(base_file))

    # Path to save output
    temp_path = directory

    if base_file in os.listdir(temp_path):
        print("FILE EXISTS, CONTINUING...")
    else:
        try:
            local_file = os.path.join(temp_path, base_file)
            # Connect to url
            req = requests.get(url, stream=True)

            # Open local file and write content to it
            with io.open(local_file, 'wb') as fp:
                # Stream data from requests generator in units of 1000
                for chunk in tqdm(req.iter_content(chunk_size=1000)):
                    # Write chunk content if found
                    if chunk:
                        fp.write(chunk)
        # Print errors and corresponding urls
        except Exception as e:
            print("ERROR: {}; {}".format(e, url))

def unzip_file(directory, zipped_file):
    '''
    Unzips files that have been downloaded locally and stores them in a directory.
    INPUT:
        - directory: String
                     Destination to save output.
        - zipped_file: String
                       Filepath to find zipped file for unzipping
    OUTPUT:
        - out_path: String
                    Filepath where content was written
    '''
    print('\n\nUnzipping {}'.format(zipped_file))
    # Try to unzip file
    try:
        # instatiate ZipFile object
        z = zipfile.ZipFile(zipped_file)
        # for each unit in z
        for name in tqdm(z.namelist()):
            # Open file and set output path
            f = z.open(name)
            out_path = os.path.join(directory, name)
            # Open out_path file for writing in utf-8
            with io.open(out_path, 'w', encoding='utf-8') as out_file:
                # Read unzipped content content as utf-8
                content = f.read().decode('utf-8')
                # Write content to output file
                out_file.write(content)

        print('Finished Unzippling {}'.format(zipped_file))
        return out_path

    # Print errors
    except zipfile.BadZipfile:
        print('Bad zip file for {}, passing on...'.format(zipped_file))

def make_gdelt_dataframe(csv_path):
    '''
    Takes a filepath, determines what type of gdelt data it is, and returns
    a pandas DataFrame.
    INPUT:
        - csv_path : string
    OUTPUT:
        - pandas dataframe
    '''
    if re.search(r'gkg', csv_path):
        columns = pickle.load(open('./gkg_columns.pkl', 'rb'))
    elif re.search(r'export', csv_path):
        columns = pickle.load(open('./events_columns.pkl', 'rb'))

    elif re.search(r'mentions', csv_path):
        columns = pickle.load(open('./mentions_columns.pkl', 'rb'))
    else:
        return 'NOT A VALID GDELT FILE'

    df = pd.read_csv(csv_path, sep='\t')
    df.columns = columns

    return df


def csv_to_parquet(file_path, output_path):
    '''
    Takes a csv path and makes a pandas DataFrame out of it so it can be
    compressed as a parqeut file.
    '''
    print('\n\nCONVERTING {} TO PARQUET'.format(file_path))

    df = make_gdelt_dataframe(file_path)
    basename = os.path.basename(file_path)
    output_filename = '{}.gzip.parquet'.format(basename)
    
    if basename in os.listdir(output_path):
        print("FILE EXISTS, CONTINUING...")
        return output_filename
    else:
        output_filepath = os.path.join(output_path, output_filename)
        # df.to_csv(output_filepath)
        fastparquet.write(output_filepath, df, compression='GZIP')
        return output_filepath

def log_filename_to_SQL(connection,file_name):
    #connect to db 
    cursor = connection.cursor()
    cursor.execute("insert into log_filename(file_name,Isdownloaded,Isprocessed) values ('{}',1,0)".format(file_name))
    connection.commit()

def upload_file_to_s3(csv_path):
    print('\n\nUPLOADING {} TO S3'.format(csv_path))
    if re.search(r'gkg', csv_path):
        bucket_name = 'gdelt-streaming'
    elif re.search(r'export', csv_path):
        bucket_name = 'gdelt-streaming-events'
    elif re.search(r'mentions', csv_path):
        bucket_name = 'gdelt-streaming-mentions'
    else:
        bucket_name = 'gdelt-streaming'

    bucket = s3.lookup(bucket_name)

    current_keys = bucket.get_all_keys()
    current_keys = [k.key for k in current_keys]

    if os.path.basename(csv_path) in current_keys:
        print('FILE FOUND IN S3, CONTINUING...')
        return

    else:

        key_name = os.path.basename(csv_path)

        key = bucket.new_key(key_name)

        now = datetime.datetime.now()

        if re.search(r'gkg', csv_path):
            dataset = 'gdelt_gkg'
        elif re.search(r'export', csv_path):
            dataset = 'gdelt_events'
        elif re.search(r'mentions', csv_path):
            dataset = 'gdelt_mentions'
        else:
            dataset = 'other'

        metadata = {'dataset': dataset, 'upload_time': str(now)}

        key.metadata.update(metadata)

        key.set_contents_from_filename(csv_path)

        print('{} UPLOAD SUCCESSFUL'.format(key_name))
        log_filename_to_SQL(connection,key_name)


def get_most_recent_files_to_s3(download_filepath, unzip_filepath,parquet_filepath):
    gdelt_last_15 = requests.get('http://data.gdeltproject.org/gdeltv2/lastupdate.txt').text

    urls = re.findall(r'(?P<url>https?://[^\s]+)', gdelt_last_15)

    for f in tqdm(urls):

        download_zip_in_chunks(download_filepath, f)

        f_unzip_path = unzip_file(unzip_filepath,
                                    os.path.join(download_filepath,
                                                 os.path.basename(f)))

        parquet_path = csv_to_parquet(f_unzip_path, parquet_filepath)
        upload_file_to_s3(parquet_path)
        delete_data_after_s3_upload()

def delete_data_after_s3_upload():
    raw_path = os.path.join(PROJ_ROOT, 'data/raw')
    interim_path = os.path.join(PROJ_ROOT, 'data/interim')
    parquet_path = os.path.join(PROJ_ROOT, 'data/parquet')

    file_paths = [raw_path, interim_path, parquet_path]

    print("REMOVING LEFTOVER FILES")
    for p in tqdm(file_paths):
        for f in os.listdir(p):
            os.remove(os.path.join(p, f))

if __name__ == '__main__':

    PROJ_ROOT = '.'
    download_filepath = os.path.join(PROJ_ROOT, 'data/raw')
    unzip_filepath = os.path.join(PROJ_ROOT, 'data/interim')
    parquet_filepath = os.path.join(PROJ_ROOT, 'data/parquet')
     
    AWS_ACCESS_KEY = aws_config['AWS_ACCESS_KEY']
    AWS_SECRET_ACCESS_KEY = aws_config['AWS_SECRET_ACCESS_KEY']
    connection = psycopg2.connect(host = db_config['host'], database = db_config['database'], user = db_config['username'], password = db_config['password'])
    
    s3 = boto.connect_s3(AWS_ACCESS_KEY, AWS_SECRET_ACCESS_KEY)

    gkg_columns = ['GKGRECORDID', 'DATE', 'SourceCollectionIdentifier',
              'SourceCommonName', 'DocumentIdentifier', 'Counts',
              'V2Counts', 'Themes', 'V2Themes', 'Locations',
              'V2Locations', 'Persons', 'V2Persons', 'Organizations',
              'V2Organizations', 'V2Tone', 'Dates', 'GCAM',
               'SharingImage', 'RelatedImages', 'SocialImageEmbeds',
              'SocialVideoEmbeds', 'Quotations', 'AllNames', 'Amounts',
              'TranslationInfo', 'Extras']

    events_columns = ['GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate',
       'Actor1Code', 'Actor1Name', 'Actor1CountryCode',
       'Actor1KnownGroupCode', 'Actor1EthnicCode', 'Actor1Religion1Code',
       'Actor1Religion2Code', 'Actor1Type1Code', 'Actor1Type2Code',
       'Actor1Type3Code', 'Actor2Code', 'Actor2Name', 'Actor2CountryCode',
       'Actor2KnownGroupCode', 'Actor2EthnicCode', 'Actor2Religion1Code',
       'Actor2Religion2Code', 'Actor2Type1Code', 'Actor2Type2Code',
       'Actor2Type3Code', 'IsRootEvent', 'EventCode', 'EventBaseCode',
       'EventRootCode', 'QuadClass', 'GoldsteinScale', 'NumMentions',
       'NumSources', 'NumArticles', 'AvgTone', 'Actor1Geo_Type',
       'Actor1Geo_FullName', 'Actor1Geo_CountryCode', 'Actor1Geo_ADM1Code',
       'Actor1Geo_ADM2Code',
       'Actor1Geo_Lat', 'Actor1Geo_Long', 'Actor1Geo_FeatureID',
       'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode',
       'Actor2Geo_ADM1Code',
       'Actor2Geo_ADM2Code',
        'Actor2Geo_Lat', 'Actor2Geo_Long',
       'Actor2Geo_FeatureID', 'ActionGeo_Type', 'ActionGeo_FullName',
       'ActionGeo_CountryCode', 'ActionGeo_ADM1Code',
       'ActionGeo_ADM2Code',
        'ActionGeo_Lat',
       'ActionGeo_Long', 'ActionGeo_FeatureID', 'DATEADDED', 'SOURCEURL']


    mentions_columns = ['GLOBALEVENTID', 'EventTimeDate', 'MentionTimeDate',
                           'MentionType', 'MentionSourceName', 'MentionIdentifier',
                           'SentenceID', 'Actor1CharOffset', 'Actor2CharOffset',
                           'ActionCharOffset', 'InRawText', 'Confidence',
                           'MentionDocLen', 'MentionDocTone',
                           'MentionDocTranslationInfo', 'Extras']


    pickle.dump(gkg_columns, open('./gkg_columns.pkl', 'wb'))

    pickle.dump(events_columns, open('./events_columns.pkl', 'wb'))

    pickle.dump(mentions_columns, open('./mentions_columns.pkl', 'wb'))

    get_most_recent_files_to_s3(download_filepath, unzip_filepath,parquet_filepath)
    print('Done!')

