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


num_files = 6


# Set up logging
logging.basicConfig(level=logging.INFO)


drive_disk = 'https://drive.google.com/drive/folders/1vTrm1w6YsGbMv4AVLr-GdYdGdbGHooCw'
parent_folder_id = '1vTrm1w6YsGbMv4AVLr-GdYdGdbGHooCw'  # id папки в Google Drive


# Установка переменных окружения
os.environ['ONLINER_EMAIL'] = 'Watchshop'
os.environ['ONLINER_PASSWORD'] = 'O2203833'
filename = 'b2bonlinerAerae'

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

        credentials = get_credentials()

        parent_id = "1vTrm1w6YsGbMv4AVLr-GdYdGdbGHooCw"
        upload_to_drive(data_file_path, parent_id, credentials, filename)
        
        chunks = process_files(data_file_path) # перемещено перед удалением файла

        spreadsheet = search_file_create(filename, credentials, parent_folder_id, num_files)

        upload_to_gsheets(credentials, spreadsheet, chunks)

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


def resilient_request_execute(request, max_retries=5, sleep_time=5):
    for attempt in range(max_retries):
        try:
            return request.execute()
        except HttpError as e:
            if e.resp.status != 500:
                # If the error is not an internal server error, raise it.
                raise e
            else:
                print(f"Internal Server Error, retrying... (Attempt {attempt + 1})")
                time.sleep(sleep_time)
    # If all the retries failed, raise an exception.
    raise Exception("All retries failed.")


def get_credentials():
    # Создайте клиент Cloud Storage.
    storage_client = storage.Client()

    # Получите объект Blob для файла ключа сервисного аккаунта.
    bucket = storage_client.get_bucket('ia_sam')
    blob = bucket.blob('inner-nuance-389811-05efdb1df532.json')

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



def upload_to_drive(local_file, parent_id, credentials, filename):
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
            resilient_request_execute(request)
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
    resilient_request_execute(request)



def search_file_create(filename, credentials, parent_folder_id, num_files):
    

    gc = gspread.authorize(credentials)
    files = gc.list_spreadsheet_files()

    # Создание списка файлов и их имен
    file_names = [f"{filename}_{i}" for i in range(num_files)]
    file_objects = []

    # Создание или открытие файлов
    for name in file_names:
        file_exists = any(file['name'] == name for file in files)
        if not file_exists:
            file_objects.append(gc.create(name))
        else:
            file_objects.append(gc.open(name))

    # Перемещение файлов в требуемую папку
    service = build('drive', 'v3', credentials=credentials)
    for file in file_objects:
        file_id = file.id
        file_info = service.files().get(fileId=file_id, fields='parents').execute()
        current_parents = set(file_info.get('parents', []))
        if parent_folder_id not in current_parents:
            previous_parents = ",".join(current_parents)
            service.files().update(
                fileId=file_id,
                addParents=parent_folder_id,
                removeParents=previous_parents,
                fields='id, parents').execute()

    # Получение последнего измененного файла
    last_modified_file = min(
        file_objects,
        key=lambda file: datetime.datetime.fromisoformat(
            service.files().get(fileId=file.id, fields='modifiedTime')
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
        new_sheet.resize(rows=700000, cols=9)
    except Exception as e:
        print(f"Error while manipulating worksheets: {e}")
        return

    return last_modified_file


def detect_encoding(file_path, num_bytes=10000):
    with open(file_path, 'rb') as f:
        rawdata = f.read(num_bytes)
    result = chardet.detect(rawdata)
    return result['encoding']

def append_data(df, worksheet):
    # Разделите df на подчанки размером 40000 строк
    chunks = [df[i:i + 5000] for i in range(0, df.shape[0],5000)]

    for i, chunk in enumerate(chunks):
        try:
            chunk_str = chunk.astype(str)
            chunk_list = chunk_str.values.tolist()
            worksheet.append_rows(chunk_list)
            print(f"Successfully appended chunk {i+1} of {len(chunks)} to the worksheet.")
        except Exception as e:
            print(f"Error appending chunk {i+1} to the worksheet: {e}")
            return

def reauthorize(credentials):
        print("Reauthorizing credentials...")
        gc = gspread.authorize(credentials)
        print("Credentials reauthorized.")
        return gc.open_by_key(spreadsheet_id)  # Возвращаем новый объект Spreadsheet

def process_files(local_file_path): 
  
    try: 
        logging.info("Unzipping file...") 
        os.system('gunzip -c ' + local_file_path + ' > ' + local_file_path[:-3]) 
        logging.info("File unzipped.") 
  
        csv_file = local_file_path[:-3] 
  
        chunksize = 200000 
        header = None 
        chunks = [] 

        logging.info("Reading and processing CSV file...") 
        encoding = detect_encoding(csv_file) 
        logging.info(f"Detected encoding: {encoding}")  # вывод кодировки в логи 
        for chunk_id, chunk in enumerate(pd.read_csv(csv_file, encoding=encoding, sep=';', chunksize=chunksize, dtype=str)): 
            logging.info(f'Processing chunk number: {chunk_id}') 
  
            if header is None: 
                logging.info("Processing header...") 
                header = chunk.columns.values[:8].tolist() + ['Инфо Магазин'] 
  
            logging.info("Processing chunk data...") 
            chunk['Инфо Магазин'] = chunk.iloc[:, 8:].apply(lambda row: '_'.join(row.dropna().astype(str)), axis=1) 
  
            logging.info("Before selecting columns...") 
            chunk = chunk[header] 
            logging.info("After selecting columns...") 
  
            logging.info("Converting data to string format...") 
            chunk = chunk.astype(str) 
            logging.info("Data converted.") 
  
            chunks.append(chunk) 

    except Exception as e: 
        logging.error(f"An error occurred: {e}") 

    finally: 
        logging.info("Done processing files.") 
        return chunks 

def upload_to_gsheets(credentials, spreadsheet, chunks):
    print("Authorizing credentials...")
    gc = gspread.authorize(credentials)
    print("Credentials authorized.")

    spreadsheet_id = spreadsheet.id  # сохраняем id таблицы для повторной авторизации

    print("Appending data to spreadsheet...")
    worksheet = spreadsheet.worksheet("transit")
    try:
        for chunk in chunks:
            append_data(chunk, worksheet)
    except Exception as e:
        print("Error appending data to spreadsheet:", e)
        return
    print("Data appended.")

    # Переименовываем лист после обработки всех чанков
    print("Renaming sheet to 'ready'...")
    try:
        worksheet.update_title('ready')
    except Exception as e:
        print("Error renaming sheet:", e)

    print("Done uploading files.")
    return None




