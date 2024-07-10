import os
import urllib.request
import logging
from datetime import datetime
import db
from decouple import config
import git_upload

def setup_logging(log_folder):
    """
    Настройка логгера для записи в файл 'prfDDMMYYYY.log' в указанной папке.

    Параметры:
    log_folder (str): Путь к папке, где будут сохраняться файлы логов.

    Действия:
    1. Создает папку для логов, если она не существует.
    2. Настраивает логгер для записи сообщений в файл с текущей датой в формате 'prfDDMMYYYY.log'.
    """
    os.makedirs(log_folder, exist_ok=True)
    log_file_name = datetime.now().strftime("%d%m%Y")
    log_file_path = os.path.join(log_folder, f"prf{log_file_name}.log")
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(message)s')

def download_file(url, file_url_prefix):
    """
    Скачивание файла по указанной ссылке, начинающейся с заданного префикса.

    Параметры:
    url (str): URL страницы, с которой будет скачан файл.
    file_url_prefix (str): Префикс URL файла для поиска на странице.

    Возвращает:
    str: Имя скачанного файла, если загрузка успешна, иначе None.
    """
    try:
        response = urllib.request.urlopen(url)
        html_content = response.read().decode('utf-8')

        start_index = html_content.find(file_url_prefix)
        if start_index == -1:
            logging.error("Ссылка не найдена")
            print("Ссылка не найдена")
            return None  # Добавлено возвращение None в случае, если ссылка не найдена

        end_index = html_content.find('"', start_index)
        partial_file_url = html_content[start_index:end_index]
        file_url = urllib.parse.urljoin(url, partial_file_url)

        file_name = os.path.basename(urllib.parse.urlparse(file_url).path)

        with urllib.request.urlopen(file_url) as file_response:
            with open(file_name, "wb") as file:
                file.write(file_response.read())
            logging.info(f"Скачан файл: {file_name}")
            print(f"Скачан файл: {file_name}")

        return file_name

    except urllib.error.URLError as e:
        logging.error(f"Ошибка URL: {e}")
        print(f"Ошибка URL: {e}")
    except urllib.error.HTTPError as e:
        logging.error(f"HTTP ошибка: {e}")
        print(f"HTTP ошибка: {e}")
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса: {e}")
        print(f"Ошибка при выполнении запроса: {e}")

    return None


def main():
    """
    Основная функция для выполнения сценария скачивания файла и записи логов.

    Действия:
    1. Читает настройки из конфигурационного файла.
    2. Настраивает логирование.
    3. Скачивает файл по указанному URL и префиксу.
    4. Если файл скачан, создает временную таблицу в базе данных.
    5. Загружает данные из CSV файла в базу данных.
    6. Выводит сообщение о завершении загрузки.
    """
    try:
        url = config("URL")
        file_url_prefix = config("FILE_URL_PREFIX")
        log_folder = config("LOG_FOLDER")
    except KeyError as e:
        logging.error(f"Ошибка конфигурации: отсутствует параметр {e}")
        print(f"Ошибка конфигурации: отсутствует параметр {e}")
        return
    db.set_cfg_ora_clnt()
    setup_logging(log_folder)
    file_name = download_file(url, file_url_prefix)

    if file_name:
        if db.is_safe_csv_file(file_name):
            try:
                db.create_temp_table()
                db.insert_csv_data(file_name)
            except Exception as e:
                logging.error(f"Ошибка при работе с базой данных: {e}")
                print(f"Ошибка при работе с базой данных: {e}")
        else:
            logging.error("CSV файл не прошел проверку на безопасность.")
            print("CSV файл не прошел проверку на безопасность.")
    user_input = input("Вы хотите запушить файл в Git? (y/n): ").strip().lower()
    if user_input in ('y', 'Y'):
        try:
            git_upload.upload_to_git()
        except Exception as e:
            logging.error(f"Ошибка при загрузке файла в Git: {e}")
            print(f"Ошибка при загрузке файла в Git: {e}")

    print("Загрузка завершена.")

if __name__ == "__main__":
    main()
