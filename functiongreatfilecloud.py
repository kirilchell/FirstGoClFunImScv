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
import gspread
from google.auth.transport.requests import Request
import datetime
from googleapiclient.errors import HttpError
import gc as garbage_collector
import chardet
import itertools

num_files = 12
# Set up logging
logging.basicConfig(level=logging.INFO)
drive_disk = 'https://drive.google.com/drive/folders/1vTrm1w6YsGbMv4AVLr-GdYdGdbGHooCw'
parent_folder_id = '1vTrm1w6YsGbMv4AVLr-GdYdGdbGHooCw'  # id папки в Google Drive
# Установка переменных окружения
os.environ['ONLINER_EMAIL'] = 'Watchshop'
os.environ['ONLINER_PASSWORD'] = 'O2203833'
filename = 'b2bonlinerAerae'
chunksize = 200000

def main(event, context):
    email = os.getenv('ONLINER_EMAIL')
    password = os.getenv('ONLINER_PASSWORD')

    url_onliner_file = 'https://b2b.onliner.by/catalog_prices'
    timestamp = time.strftime('%Y%m%d-%H%M%S')
    data_file_path = f'{filename}.csv.gz'
    session = requests.Session()

    try:
        authenticate(session, password, email)
        download_file(session, url_onliner_file, data_file_path)

        key_filenames = ['inner-nuance-389811-05efdb1df532.json', 'inner-nuance-389811-13fe8ddc7b28.json', 'inner-nuance-389811-ba11f61d8d98.json', 'inner-nuance-389811-f8ca746ad03f.json']
        credentials_list = [get_credentials(key_filename) for key_filename in key_filenames]
        
        file_objects, service_drive = create_and_move_files(filename, credentials_list[0], parent_folder_id, num_files)
        
        upload_files(local_file_path, chunksize, file_objects, service_drive, credentials_list) # используется новая функция обработки и загрузки

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

def get_credentials(key_filename):
    # Создайте клиент Cloud Storage.
    storage_client = storage.Client()

    # Получите объект Blob для файла ключа сервисного аккаунта.
    bucket = storage_client.get_bucket('ia_sam')
    blob = bucket.blob(key_filename)

    # Скачайте JSON файл ключа сервисного аккаунта.
    key_json_string = blob.download_as_text()

    # Загрузите ключ сервисного аккаунта из JSON строки.
    key_dict = json.loads(key_json_string)

    # Создайте учетные данные из ключа сервисного аккаунта.
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive.file']
    credentials = service_account.Credentials.from_service_account_info(
        key_dict, scopes=SCOPES)

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

def create_and_move_files(filename, credentials, parent_folder_id, num_files):
    try:
        gc = gspread.authorize(credentials)
        print("Authorized successfully.")
    except Exception as e:
        print("Error authorizing: ", e)
        return None

    try:
        files = gc.list_spreadsheet_files()
        print("Files retrieved successfully.")
    except Exception as e:
        print("Error retrieving files: ", e)
        return None

    # Создание списка файлов и их имен
    file_names = [f"{filename}_{i}" for i in range(num_files)]
    file_objects = []

    # Создание или открытие файлов
    for name in file_names:
        try:
            file_exists = any(file['name'] == name for file in files)
            if not file_exists:
                file_objects.append(gc.create(name))
            else:
                file_objects.append(gc.open(name))
            print(f"File {name} handled successfully.")
        except Exception as e:
            print(f"Error handling file {name}: ", e)
            return None

    # Перемещение файлов в требуемую папку
    try:
        service_drive = build('drive', 'v3', credentials=credentials)
        print("Drive service built successfully.")
    except Exception as e:
        print("Error building drive service: ", e)
        return None

    for file in file_objects:
        try:
            file_id = file.id
            file_info = service_drive.files().get(fileId=file_id, fields='parents').execute()
            current_parents = set(file_info.get('parents', []))
            if parent_folder_id not in current_parents:
                previous_parents = ",".join(current_parents)
                service_drive.files().update(
                    fileId=file_id,
                    addParents=parent_folder_id,
                    removeParents=previous_parents,
                    fields='id, parents').execute()
            print(f"File {file_id} moved successfully.")
        except Exception as e:
            print(f"Error moving file {file_id}: ", e)
            return None

    return file_objects, service_drive

def process_last_modified_file(file_objects, service_drive):

  
    # Получение последнего измененного файла
    last_modified_file = min(
        file_objects,
        key=lambda file: datetime.datetime.fromisoformat(
            service_drive.files().get(fileId=file.id, fields='modifiedTime')
            .execute()['modifiedTime'].rstrip('Z')
        )
    )

    # Ваш код для работы с последним измененным файлом
    try:
        # Create a new sheet, delete others, and rename the new one
        new_sheet = last_modified_file.add_worksheet(title=None, rows="1", cols="9")

        # Get worksheets in the spreadsheet and delete all except the newly created one
        sheets = last_modified_file.worksheets()
        for sheet in sheets:
            if sheet.id != new_sheet.id:
                last_modified_file.del_worksheet(sheet)

        # Rename the newly created sheet to 'transit'
        new_sheet.update_title("transit")

        # Resize the new sheet
        new_sheet.resize(rows=100, cols=9)
    except Exception as e:
        print(f"Error while manipulating worksheets: {e}")
        return

    return last_modified_file

def detect_encoding(file_path, num_bytes=10000):
    with open(file_path, 'rb') as f:
        rawdata = f.read(num_bytes)
    result = chardet.detect(rawdata)
    return result['encoding']
from google.cloud import storage
from google.cloud import pubsub_v1

# Эти параметры нужно задать в начале кода или взять из окружения
BUCKET_NAME = 'your-bucket-name'
PROJECT_ID = 'your-project-id'
TOPIC_ID = 'your-topic-id'

def upload_file_to_gcs(file_path, destination_blob_name):
    """Uploads a file to the bucket."""
    bucket = storage.Client().bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(file_path)

    print(
        "File {} uploaded to {}.".format(
            file_path, destination_blob_name
        )
    )

def publish_messages_to_pubsub(file_path, service_account, table_id):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    message = f"{file_path},{service_account},{table_id}"
    future = publisher.publish(topic_path, message.encode("utf-8"))
    print(future.result())

def upload_files(local_file_path, chunksize, file_objects, service_drive, credentials_list): 
    try: 
        logging.info("Unzipping file...") 
        os.system('gunzip -c ' + local_file_path + ' > ' + local_file_path[:-3]) 
        logging.info("File unzipped.") 

        csv_file = local_file_path[:-3]

        logging.info("Reading and processing CSV file...") 
        encoding = detect_encoding(csv_file) 
        logging.info(f"Detected encoding: {encoding}")  # вывод кодировки в логи 

        # Используем itertools.cycle для циклического перебора учетных данных
        credentials_cycle = itertools.cycle(credentials_list)

        for chunk_id, chunk in enumerate(pd.read_csv(csv_file, encoding=encoding, sep=';', chunksize=chunksize, dtype=str)): 
            logging.info(f'Processing chunk number: {chunk_id}') 
          
            chunk_file_path = f"{csv_file}_chunk_{chunk_id}.csv"
            chunk.to_csv(chunk_file_path, index=False)
            
            destination_blob_name = f"data/{chunk_file_path.split('/')[-1]}"
            upload_file_to_gcs(chunk_file_path, destination_blob_name)
            
            spreadsheet = process_last_modified_file(file_objects, service_drive)
            # Используем next(credentials_cycle) для получения следующего набора учетных данных
            credentials = next(credentials_cycle)
            service_account = credentials["client_email"]

            publish_messages_to_pubsub(destination_blob_name, service_account, spreadsheet)

    finally: 
        logging.info("Done processing and uploading files.")



