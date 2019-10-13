from datetime import datetime
import os

import numpy as np
import pandas as pd
import yadisk

def load(meta):
    fext = meta.name.split('.')[-1] #gets file extension

    if fext == 'csv':
        #У CSV файла может быть любой разделитель, поэтому сначала грузим только 1 строку и ищем разделитель
        df = pd.read_csv(meta.file, nrows=0)
        if len(df.columns) == 1 and df.columns[0].lower()!= 'filename': #2 условие т.к может 
            sep = df.columns[0][len('filename'): df.columns[0].find('date')]
        else:
            sep=','
        
        df = pd.read_csv(meta.file, sep=sep)
    elif fext[:3] == 'xls':
        df = pd.read_excel(meta.file)
    else:
        raise Exception('Неизвестный тип файла')
    return df

def get_wav_files(y):
    wav_meta = y.get_meta('/speechanalytics-connect/').embedded.items
    wav_records = [{'filename': meta.name, 'url': meta.file} for meta in wav_meta if meta.media_type=='audio']
    return pd.DataFrame().from_records(wav_records)

def get_result(token, cache, date_from=None, date_to=None):
    date_from = date_from if date_from else 0
    date_to = date_to if date_to else 9999999999999
    
    y = yadisk.YaDisk(token=token)
    base_path = '/speechanalytics-connect/meta/'
    total_df = pd.DataFrame()
    folder_meta = y.get_meta(base_path).embedded.items
    wav_df = get_wav_files(y)

    #удаляем из кэша файлы, которых не существует
    [cache.pop(x) for x in set(cache) - set([x.name for x in folder_meta])]

    total_df = pd.DataFrame()
    for meta in folder_meta:
        fname = meta.name
        fhash = meta.sha256 # если файл не изменялся, его хэш будет таким же
        
        try:
            cached = cache[fname]
            print(f'Файл {fname} есть в кэше')
            if cached['hash'] == fhash:
                print(f'Хэш совпадает')
                df = cached['df']
            else:
                print(f'Хэш не совпадает, грузим заново')
                df = load(meta)
                cache[fname]['hash'] = fhash
                cache[fname]['df'] = df
        except KeyError: #нет в кэше
            print(f'Файла {fname} нет в кэше')
            df = load(meta)
            cache[fname] = {}
            cache[fname]['hash'] = fhash
            cache[fname]['df'] = df
        print('\n')
        total_df = pd.concat([total_df, df], sort=False)

    cols_to_hash = ['filename', 'date', 'phone_number_client']
    df = total_df
    df['id'] = df.loc[:, cols_to_hash].apply(str, axis=1)
    df['id'] = df['id'].apply(hash).apply(abs).apply(str)
    df.date = pd.to_datetime(df.date)
    df['ts'] = df.date.values.astype(np.int64) // 10 ** 9
    df.drop('date', axis=1, inplace=True)
    df.rename(columns={'ts': 'date'}, inplace=True)
    df.phone_number_operator = df.phone_number_operator.astype(str)
    df.phone_number_client = df.phone_number_client.astype(str)

    df = df.loc[df.date.between(int(date_from), int(date_to))].sort_values('date')
    df = df.merge(wav_df, on='filename', how='left')
    calls = df[['id', 'type', 'date', 'duration_answer',
             'status', 'phone_number_client', 
             'phone_number_operator']].to_dict(orient='records')
    df.set_index('id', inplace=True)

    calls_map = df[['filename', 'url']].to_dict(orient='index')
    return calls, calls_map
    


