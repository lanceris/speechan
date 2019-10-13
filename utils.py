from datetime import datetime
import os 
from typing import List, Tuple # for type annotations

import numpy as np
import pandas as pd
import yadisk


def _load(meta: dict) -> pd.DataFrame:
    '''This function generates pd.DataFrame with calls data'''
    fext = meta.name.split('.')[-1] #gets file extension

    if fext == 'csv':
        # *.csv can have any delimeter, so we need to find it beforehand
        df = pd.read_csv(meta.file, nrows=0) #loads one row with headers
        if len(df.columns) == 1 and df.columns[0].lower()!= 'filename':
            sep = df.columns[0][len('filename'): df.columns[0].find('date')] # gets delimiter
        else:
            sep=','
        
        df = pd.read_csv(meta.file, sep=sep)
    elif fext[:3] == 'xls': # xls, xlsx, xlsm etc, processing as excel file
        df = pd.read_excel(meta.file)
    else:
        raise Exception('Unknown file type')
    return df


def _get_wav_files(y: yadisk.yadisk.YaDisk) -> List[dict]:
    '''This function gets info of all audio files on yandex disk'''
    wav_meta = y.get_meta('/speechanalytics-connect/').embedded.items
    wav_records = [{'filename': meta.name, 'url': meta.file} for meta in wav_meta if meta.media_type=='audio']
    return pd.DataFrame().from_records(wav_records)


def get_files(token:str, cache:dict, date_from:str = None, date_to:str = None) -> Tuple[List[dict], List[dict]]:
    '''This function generates list of dicts of calls data and maps filenames with their download links'''
    date_from = date_from if date_from else 0
    date_to = date_to if date_to else 9999999999999
    
    y = yadisk.YaDisk(token=token) #connecting to yandex disk
    base_path = '/speechanalytics-connect/meta/'

    folder_meta = y.get_meta(base_path).embedded.items
    wav_df = _get_wav_files(y)

    #removes files from cache that are not present on disk
    [cache.pop(x) for x in set(cache) - set([x.name for x in folder_meta])]

    total_df = pd.DataFrame() # dataframe with calls data
    for meta in folder_meta: # iterates over files in base_dir
        fname = meta.name
        fhash = meta.sha256 # if file has not changed, its hash will be the same
        
        try:
            cached = cache[fname]
            if cached['hash'] == fhash: #file hash and cache's file hash are the same
                df = cached['df']
            else: #hashes are not the same, we need to load file
                df = _load(meta)
                cache[fname]['hash'] = fhash 
                cache[fname]['df'] = df
        except KeyError: #fanme not present in cache
            df = _load(meta)
            cache[fname] = {} 
            cache[fname]['hash'] = fhash
            cache[fname]['df'] = df
        total_df = pd.concat([total_df, df], sort=False)

    cols_to_hash = ['filename', 'date', 'phone_number_client']
    df = total_df
    del(total_df)
    
    #takes hash() from 3 columns - filename, date, client_number
    df['id'] = df.loc[:, cols_to_hash].apply(str, axis=1)
    df['id'] = df['id'].apply(hash).apply(abs).apply(str)

    df.date = pd.to_datetime(df.date)
    df['ts'] = df.date.values.astype(np.int64) // 10 ** 9
    df.drop('date', axis=1, inplace=True) #we need to get timestamp, so removing data
    df.rename(columns={'ts': 'date'}, inplace=True) #rename ts -> date
    df.phone_number_operator = df.phone_number_operator.astype(str)
    df.phone_number_client = df.phone_number_client.astype(str)

    df = df.loc[df.date.between(int(date_from), int(date_to))].sort_values('date') #date filter
    df = df.merge(wav_df, on='filename', how='left') #merge download links for audio files
    calls = df[['id', 'type', 'date', 'duration_answer',
             'status', 'phone_number_client', 
             'phone_number_operator']].to_dict(orient='records')
    df.set_index('id', inplace=True)

    calls_map = df[['filename', 'url']].to_dict(orient='index') #[{id (line hash): {'filename': audio filename, 'url':url to download audio}}]
    return calls, calls_map
    


