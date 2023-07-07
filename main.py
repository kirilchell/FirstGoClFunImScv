import os
import json
import logging
import pandas as pd
import numpy as np
import requests
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from google.oauth2 import service_account
import time
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.cloud import storage
from flask import escape

# Установка переменных окружения
os.environ['ONLINER_EMAIL'] = 'Watchshop'
os.environ['ONLINER_PASSWORD'] = 'O2203833'
filename = 'b2bonlinerAerae'

def get_credentials():
    # Создайте клиент Cloud Storage.
    storage_client = storage.Client()

    # Получите объект Blob для файла ключа сервисного аккаунта.
    bucket = storage_client.get_bucket('ia_sam')
    blob = bucket.blob('inner-nuance-389811-03fcc49bfb3f.json')

    # Скачайте JSON файл ключа сервисного аккаунта.
    key_json_string = blob.download_as_text()

    # Загрузите ключ сервисного аккаунта из JSON строки.
    key_dict = json.loads(key_json_string)

    # Создайте учетные данные из ключа сервисного аккаунта.
    credentials = service_account.Credentials.from_service_account_info(key_dict)

    return credentials

def authenticate(session, password, email):
    url = 'https://b2b.onliner.by/login'
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36'} 
    data = {
        'email': email,
        'password': password,
    }
    response = session.post(url, data=data, headers=headers)
    if response.status_code == 200:
        print(f'Успешное соединение c {url}.')
    else:
        print(f'Ошибка при подключении к {url}. Код статуса: {response.status_code}')

def download_file(session, url, local_filename):
    r = session.get(url, stream=True)
    print(f'URL of file being downloaded: {r.url}')
    if r.status_code == 200:
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk: 
                    f.write(chunk)
        return local_filename
    else:
        print(f'Ошибка при скачивании файла. Код статуса: {r.status_code}')



def upload_to_drive(local_file, parent_id, credentials):
    drive_service = build('drive', 'v3', credentials=credentials)
    
    # Search for the file in the drive folder
    file_list = drive_service.files().list(q=f"'{parent_id}' in parents and trashed=false").execute().get('files', [])
    for file in file_list:
        if file['name'] == filename:
            # If file is found, update the existing file
            media = MediaFileUpload(local_file,
                                    mimetype='application/gzip',
                                    resumable=True)
            request = drive_service.files().update(
                fileId=file['id'],
                media_body=media,
            )
            request.execute()
            return

    # If file not found, then create new file and upload
    file_metadata = {
        'name': filename,
        'parents': [parent_id],
        'mimeType': 'application/gzip'
    }
    media = MediaFileUpload(local_file, 
                            mimetype='application/gzip',
                            resumable=True)
    request = drive_service.files().create(body=file_metadata,
                                           media_body=media)
    request.execute()

def process_and_upload_files(credentials, parent_id, local_file_path):
    import pandas as pd
    import os
    from googleapiclient.http import MediaFileUpload
    from googleapiclient.discovery import build

    drive_service = build('drive', 'v3', credentials=credentials)

    # Распаковываем gzip-файл
    try:
        os.system('gunzip -c ' + local_file_path + ' > ' + local_file_path[:-3])
    except Exception as e:
        print("Error unzipping file:", e)

    # Убираем расширение .gz у файла
    csv_file = local_file_path[:-3]

    chunksize = 10000  # размер чанка
    upload_chunk_size = 10  # количество чанков перед загрузкой на Google Drive
    header = None
    temp_csv_files = []  # список для хранения имен временных CSV файлов
    file_identifier = 'aszoijodijsoQE'  # идентификатор для имени файла

    upload_counter = 0  # счетчик для определения момента загрузки

    try:
        # Итерация по чанкам файла
        for chunk_id, chunk in enumerate(pd.read_csv(csv_file, encoding='CP1251', sep=';', chunksize=chunksize)):

            print(f'Processing chunk number: {chunk_id}')  # Выводим в лог номер чанка

            # Обрабатываем заголовок
            if header is None:
                header = chunk.columns.values[:8].tolist() + ['Инфо Магазин']

            # Объединяем все столбцы после восьмого и пропускаем пустые ячейки
            chunk['Инфо Магазин'] = chunk.iloc[:, 8:].apply(
                lambda row: '_'.join(row.dropna().astype(str)), axis=1)
            chunk = chunk[header]  # только 9 первых столбцов с новым столбцом
            
            # Записываем чанк во временный CSV файл
            chunk_file = f'{chunk_id}.csv'
            chunk.to_csv(chunk_file, sep=',', index=False)
            temp_csv_files.append(chunk_file)
            upload_counter += 1
            
            end_of_file = chunk.shape[0] < chunksize  # Проверяем, достигли ли мы конца файла
            
            # Если количество обработанных чанков достигло upload_chunk_size или это последний чанк
            if upload_counter >= upload_chunk_size or end_of_file:
                try:
                    # Объединяем все временные CSV файлы
                    combined_file = f'{file_identifier}_{chunk_id//upload_chunk_size:03d}.csv'
                    with open(combined_file, 'w') as outfile:
                        for fname in temp_csv_files:
                            with open(fname) as infile:
                                for line in infile:
                                    outfile.write(line)
                                    
                    # Загружаем объединенный файл на Google Drive
                    # Проверяем, существует ли файл с таким именем на Google Drive
                    results = drive_service.files().list(q=f"name='{combined_file}' and '{parent_id}' in parents",
                                                        fields="files(id)").execute()
                    items = results.get('files', [])
                    
                    # Если файл существует, обновляем его
                    if items:
                        file_id = items[0]['id']
                        media = MediaFileUpload(combined_file, mimetype='text/csv', resumable=True)
                        request = drive_service.files().update(fileId=file_id, media_body=media).execute()
                    # Иначе создаем новый файл
                    else:
                        file_metadata = {
                            'name': combined_file,
                            'parents': [parent_id]
                        }
                        media = MediaFileUpload(combined_file, mimetype='text/csv')
                        request = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    
                    print('File ID:', request.get('id'))
                    
                    # Удаляем временные файлы
                    for f in temp_csv_files:
                        os.remove(f)
                    os.remove(combined_file)
                    
                    # Сбрасываем счетчик и очищаем список имен временных файлов
                    upload_counter = 0
                    temp_csv_files = []
                    
                except Exception as e:
                    print("Error processing and uploading chunk:", e)

    except Exception as e:
        print("Error reading CSV file:", e)

    return None



def main(event, context):
    import base64
    print("""This Function was triggered by messageId {} published at {} to {}
    """.format(context.event_id, context.timestamp, context.resource["name"]))

    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        print("Message Data: {}".format(message))
    else:
        print("No data!")

    if message == 'ne_onliner_file_generat':
        email = os.getenv('ONLINER_EMAIL')
        password = os.getenv('ONLINER_PASSWORD')

        url_onliner_file = 'https://b2b.onliner.by/catalog_prices'
        timestamp = time.strftime('%Y%m%d-%H%M%S')
        data_file_path = f'{filename}.csv.gz'
        session = requests.Session()

        try:
            authenticate(session, password, email)
            download_file(session, url_onliner_file, data_file_path)

            credentials = get_credentials()

            parent_id = "1vTrm1w6YsGbMv4AVLr-GdYdGdbGHooCw"
            upload_to_drive(data_file_path, parent_id, credentials)

            process_and_upload_files(credentials, parent_id, data_file_path) # перемещено перед удалением файла

            if os.path.isfile(data_file_path):
                os.remove(data_file_path)
            else:
                return 'Ошибка: %s файл не найден' % escape(data_file_path)
        except requests.RequestException as e:
            return 'Ошибка при выполнении запроса: %s.' % escape(e)

        except IOError as e:
            return 'Ошибка при записи файла: %s.' % escape(e)

        except Exception as e:
            return 'Произошла непредвиденная ошибка: %s.' % escape(e)

        return 'Файл успешно загружен.'


