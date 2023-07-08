    spreadsheet_id = spreadsheet.id  # сохраняем id таблицы для повторной авторизации

    print("Unzipping file...")
    try:
        os.system('gunzip -c ' + local_file_path + ' > ' + local_file_path[:-3])
    except Exception as e:
        print("Error unzipping file:", e)
        return
    print("File unzipped.")

    csv_file = local_file_path[:-3]

    chunksize = 200000
    header = None

    print("Loading chunk into Google Sheets...")
    try:
        print("Reading and processing CSV file...")
        encoding = detect_encoding(csv_file)
        print(f"Detected encoding: {encoding}")  # вывод кодировки в логи
        for chunk_id, chunk in enumerate(pd.read_csv(csv_file, encoding=encoding, sep=';', chunksize=chunksize, dtype=str)):
            print(f'Processing chunk number: {chunk_id}')

            # Обрабатываем заголовок
            if header is None:
                print("Processing header...")
                header = chunk.columns.values[:8].tolist() + ['Инфо Магазин']

            # Объединяем все столбцы после восьмого и пропускаем пустые ячейки
            print("Processing chunk data...")
            chunk['Инфо Магазин'] = chunk.iloc[:, 8:].apply(lambda row: '_'.join(row.dropna().astype(str)), axis=1)

            # Выбор столбцов
            print("Before selecting columns...")
            chunk = chunk[header]
            print("After selecting columns...")

            # Преобразуем все данные в строковый формат
            print("Converting data to string format...")
            chunk = chunk.astype(str)
            print("Data converted.")

            print("Appending data to spreadsheet...")
            worksheet = spreadsheet.worksheet("transit")
            try:
                append_data(chunk, worksheet)
            except Exception as e:
                print("Error appending data to spreadsheet:", e)
                return
            print("Data appended.")

            # удаляем чанк
            del chunk
            # принудительный вызов сборщика мусора
            garbage_collector.collect()

            # Если номер чанка кратен 5, выполняем паузу и повторную авторизацию
            if (chunk_id + 1) % 5 == 0:
                print("Pause for 60 seconds...")
                spreadsheet = reauthorize(credentials)

        # Переименовываем лист после обработки всех чанков
        print("Renaming sheet to 'ready'...")
        try:
            worksheet.update_title('ready')
        except Exception as e:
            print("Error renaming sheet:", e)

    except Exception as e:
        print("Error reading CSV file:", e)

    print("Done processing and uploading files.")
    return None



def process_files(credentials, spreadsheet, local_file_path):
    print("Authorizing credentials...")
    gc = gspread.authorize(credentials)
    print("Credentials authorized.")

    spreadsheet_id = spreadsheet.id  # сохраняем id таблицы для повторной авторизации

    print("Unzipping file...")
    try:
        os.system('gunzip -c ' + local_file_path + ' > ' + local_file_path[:-3])
    except Exception as e:
        print("Error unzipping file:", e)
        return
    print("File unzipped.")

    csv_file = local_file_path[:-3]

    chunksize = 200000
    header = None

    print("Reading and processing CSV file...")
    encoding = detect_encoding(csv_file)
    print(f"Detected encoding: {encoding}")  # вывод кодировки в логи
    chunks = []
    for chunk_id, chunk in enumerate(pd.read_csv(csv_file, encoding=encoding, sep=';', chunksize=chunksize, dtype=str)):
        print(f'Processing chunk number: {chunk_id}')

        # Обрабатываем заголовок
        if header is None:
            print("Processing header...")
            header = chunk.columns.values[:8].tolist() + ['Инфо Магазин']

        # Объединяем все столбцы после восьмого и пропускаем пустые ячейки
        print("Processing chunk data...")
        chunk['Инфо Магазин'] = chunk.iloc[:, 8:].apply(lambda row: '_'.join(row.dropna().astype(str)), axis=1)

        # Выбор столбцов
        print("Before selecting columns...")
        chunk = chunk[header]
        print("After selecting columns...")

        # Преобразуем все данные в строковый формат
        print("Converting data to string format...")
        chunk = chunk.astype(str)
        print("Data converted.")
        chunks.append(chunk)

    print("Done processing files.")
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