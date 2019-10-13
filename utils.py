from datetime import datetime
import os 
from typing import List, Tuple # для type annotations

import numpy as np
import pandas as pd
import yadisk


def _load(meta: dict) -> pd.DataFrame:
    '''Функция загружает в dataframe файл с информацией о звонках'''
    fext = meta.name.split('.')[-1] #получаем расширение файла

    if fext == 'csv':
        #У CSV файла может быть любой разделитель, поэтому сначала грузим только 1 строку и ищем разделитель
        df = pd.read_csv(meta.file, nrows=0)
        if len(df.columns) == 1 and df.columns[0].lower()!= 'filename':
            sep = df.columns[0][len('filename'): df.columns[0].find('date')] # определяем разделитель
        else:
            sep=','
        
        df = pd.read_csv(meta.file, sep=sep)
    elif fext[:3] == 'xls': #может быть xls, xlsx, xlsm, мб еще что-то, грузим как excel
        df = pd.read_excel(meta.file)
    else:
        raise Exception('Неизвестный тип файла')
    return df


def _get_wav_files(y: yadisk.yadisk.YaDisk) -> List[dict]:
    '''Функция получает информацию о wav файлах на диске'''
    wav_meta = y.get_meta('/speechanalytics-connect/').embedded.items
    wav_records = [{'filename': meta.name, 'url': meta.file} for meta in wav_meta if meta.media_type=='audio']
    return pd.DataFrame().from_records(wav_records)


def get_files(token:str, cache:dict, date_from:str = None, date_to:str = None) -> Tuple[List[dict], List[dict]]:
    '''Функция получает список звонков и маппинг названий файлов звонов со ссылками на их скачивание'''
    date_from = date_from if date_from else 0
    date_to = date_to if date_to else 9999999999999
    
    y = yadisk.YaDisk(token=token) #подключения к яндекс диску
    base_path = '/speechanalytics-connect/meta/'

    folder_meta = y.get_meta(base_path).embedded.items
    wav_df = _get_wav_files(y)

    #удаляем из кэша файлы, которых нет на диске
    [cache.pop(x) for x in set(cache) - set([x.name for x in folder_meta])]

    total_df = pd.DataFrame() # df, в который будет записываться инфа о звонках
    for meta in folder_meta: # перебираем каждый файл в base_path
        fname = meta.name
        fhash = meta.sha256 # если файл не изменялся, его хэш будет таким же
        
        try:
            cached = cache[fname]
            # на этом этапе fname есть в хэше
            if cached['hash'] == fhash: #Хэш совпадает
                df = cached['df']
            else: #Хэш не совпадает, грузим файл заново
                df = _load(meta)
                #заполняем хэш
                cache[fname]['hash'] = fhash 
                cache[fname]['df'] = df
        except KeyError: #нет в кэше
            df = _load(meta)
            #заполняем хэш
            cache[fname] = {} 
            cache[fname]['hash'] = fhash
            cache[fname]['df'] = df
        #соединяем total_df и df из одного файла
        total_df = pd.concat([total_df, df], sort=False)

    cols_to_hash = ['filename', 'date', 'phone_number_client']
    df = total_df
    del(total_df)
    
    #берем hash() от 3 столбцов - имя файла, дата звонка, номер клиента
    df['id'] = df.loc[:, cols_to_hash].apply(str, axis=1)
    df['id'] = df['id'].apply(hash).apply(abs).apply(str)

    df.date = pd.to_datetime(df.date)
    df['ts'] = df.date.values.astype(np.int64) // 10 ** 9
    df.drop('date', axis=1, inplace=True) #т.к необходимо получить timestamp, удаляем date
    df.rename(columns={'ts': 'date'}, inplace=True) #и переименовываем ts -> date
    df.phone_number_operator = df.phone_number_operator.astype(str)
    df.phone_number_client = df.phone_number_client.astype(str)

    df = df.loc[df.date.between(int(date_from), int(date_to))].sort_values('date') #фильтр по дате
    df = df.merge(wav_df, on='filename', how='left') #присоединяем ссылки на wav файлы
    calls = df[['id', 'type', 'date', 'duration_answer',
             'status', 'phone_number_client', 
             'phone_number_operator']].to_dict(orient='records')
    df.set_index('id', inplace=True)

    calls_map = df[['filename', 'url']].to_dict(orient='index') #[{id (хэш строки): {'filename': имя wav, 'url':url на скачивание wav}}]
    return calls, calls_map
    


