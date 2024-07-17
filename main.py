import os
import urllib.request
import logging
from datetime import datetime
from decouple import config
import db
import git_upload
from handlers import handle_data
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

def download_file(file_url):
    """
    Скачивание файла по указанной прямой ссылке.

    Параметры:
    file_url (str): Прямая ссылка на файл.

    Возвращает:
    str: Имя скачанного файла, если загрузка успешна, иначе None.
    """
    try:
        file_name = os.path.basename(urllib.parse.urlparse(file_url).path)

        with urllib.request.urlopen(file_url) as file_response:
            with open(file_name, "wb") as file:
                file.write(file_response.read())
            logging.info(f"Скачан файл: {file_name}")
            print(f"Скачан файл: {file_name}")

        return file_name

    except urllib.error.URLError as e:
        if hasattr(e, 'code') and e.code == 404:
            logging.error(f"Ошибка: Файл не найден по указанному URL. [URLError] {e.reason}")
            print("Ошибка: Файл не найден по указанному URL.")
        elif isinstance(e.reason, ConnectionResetError):
            logging.error(f"Ошибка: Нет подключения к интернету.[URLError] {e.reason}")
            print("Ошибка: Нет подключения к интернету.")
        elif 'EOF occurred in violation of protocol' in str(e.reason):
            logging.error(f"Ошибка: Произошел разрыв соединения SSL. [URLError] {e.reason}")
            print("Ошибка: Произошел разрыв соединения SSL.")
        else:
            logging.error(f"Ошибка: Ошибка сети. Проверьте интернет соединение [URLError] {e.reason}")
            print(f"Ошибка: {e.reason}")
    except urllib.error.HTTPError as e:
        logging.error(f"HTTP ошибка: {e}")
        print(f"HTTP ошибка: {e}")
    except Exception as e:
        logging.error(f"Ошибка при выполнении запроса: {e}")
        print(f"Ошибка при выполнении запроса: {e}")

    return None

def configure_proxy():
    """
    Настройка прокси для urllib на основе конфигурации в .env файле, с учетом логина и пароля.
    """
    use_proxy = config("USE_PROXY", default=False, cast=bool)
    if use_proxy:
        proxy_url = config("PROXY_URL", default="")
        proxy_username = config("PROXY_USERNAME", default="")
        proxy_password = config("PROXY_PASSWORD", default="")

        if proxy_url:
            try:
                if proxy_username and proxy_password:
                    proxy_handler = urllib.request.ProxyHandler({
                        "http": f"http://{proxy_username}:{proxy_password}@{proxy_url}",
                        "https": f"https://{proxy_username}:{proxy_password}@{proxy_url}",
                    })
                else:
                    proxy_handler = urllib.request.ProxyHandler({
                        "http": proxy_url,
                        "https": proxy_url,
                    })
                opener = urllib.request.build_opener(proxy_handler)
                urllib.request.install_opener(opener)
                logging.info("Прокси настроен")
            except Exception as e:
                logging.error("Ошибка: Некорректные данные прокси.")
                print("Ошибка: Некорректные данные прокси.")
        else:
            logging.error("Ошибка: URL прокси не указан в конфигурации.")
            print("Ошибка: URL прокси не указан в конфигурации.")

def main():
    """
    Основная функция для выполнения сценария скачивания файла и записи логов.

    Действия:
    1. Читает настройки из конфигурационного файла.
    2. Настраивает логирование.
    3. Настраивает прокси, если включен.
    4. Скачивает файл по указанной прямой ссылке.
    5. Если файл скачан, создает временную таблицу в базе данных.
    6. Загружает данные из CSV файла в базу данных.
    7. Выводит сообщение о завершении загрузки.
    """
    try:
        file_url = config("FILE_URL")
        log_folder = config("LOG_FOLDER")
    except KeyError as e:
        logging.error(f"Ошибка конфигурации: отсутствует параметр {e}")
        print(f"Ошибка конфигурации: отсутствует параметр {e}")
        return

    db.set_cfg_ora_clnt()
    setup_logging(log_folder)
    configure_proxy()
    file_name = download_file(file_url)

    if file_name:
        if db.is_safe_csv_file(file_name):
            try:
                db.create_temp_table()
                db.insert_csv_standart_data(file_name)
                handle_data()
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
