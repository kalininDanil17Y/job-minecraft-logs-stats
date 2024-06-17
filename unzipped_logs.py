import os
import gzip
import zipfile
import shutil
from tqdm import tqdm
import fnmatch
import re

# Путь к папке с архивами
input_folder = 'zipped_logs'
# Путь к папке для сохранения разархивированных файлов
output_folder = 'logs'

# Создаем папку для разархивированных файлов, если она не существует
os.makedirs(output_folder, exist_ok=True)

# Получаем список всех файлов в папке с архивами
file_list = [f for f in os.listdir(input_folder) if f.endswith('.gz') or f.endswith('.zip')]

archive_file_name_pattern = re.compile(r"(?P<year>\w+)-(?P<month>\w+)-(?P<day>\w+)-(?P<log_id>\w+).log.(?P<log_format>\w+)")

# Настраиваем tqdm для отображения прогресс бара
with tqdm(total=len(file_list), desc='Разархивация файлов', unit='file') as pbar:
    for filename in file_list:
        # Полный путь к архиву
        file_path = os.path.join(input_folder, filename)

        if fnmatch.fnmatch(filename, '*.gz'):
            try:
                # Открываем gz-файл и проверяем заголовок
                with gzip.open(file_path, 'rb') as f_in:
                    # Имя файла без расширения .gz
                    log_filename = filename[:-3]
                    # Полный путь к разархивированному файлу
                    log_file_path = os.path.join(output_folder, log_filename)

                    # Записываем разархивированное содержимое в лог файл
                    with open(log_file_path, 'wb') as f_out:
                       shutil.copyfileobj(f_in, f_out)
            except gzip.BadGzipFile:
                print(f"Пропуск некорректного gzip-файла: {filename}")
        elif fnmatch.fnmatch(filename, '*.zip'):
            archive_file_name_data = archive_file_name_pattern.search(filename)
            with zipfile.ZipFile(file_path, 'r') as zip_ref:

                invalid_file_name = archive_file_name_data.group('year') + '-' + archive_file_name_data.group('month') + '-' + archive_file_name_data.group('day') + '-1' + '.log'
                valid_file_name = archive_file_name_data.group('year') + '-' + archive_file_name_data.group('month') + '-' + archive_file_name_data.group('day') + '-' + archive_file_name_data.group('log_id') + '.log'
                temp_output_folder = output_folder + '/temp'

                zip_ref.extract(
                    invalid_file_name,
                    temp_output_folder,
                )

                os.rename(temp_output_folder + '/' + invalid_file_name, output_folder + '/' + valid_file_name)

        # Обновляем прогресс бар
        pbar.update(1)

print("Все корректные файлы успешно разархивированы.")