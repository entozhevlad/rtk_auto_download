import oracledb as ora
import logging
import csv
from decouple import config
import sys
import os
import re
from datetime import datetime
# Настройка логгирования
def setup_logging(log_folder):
    os.makedirs(log_folder, exist_ok=True)
    log_file_name = datetime.now().strftime("%d%m%Y")
    log_file_path = os.path.join(log_folder, f"prf{log_file_name}.log")
    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(message)s')

setup_logging("logs")

# Учетные данные для подключения к базе данных
try:
    username = config("DB_USERNAME")
    password = config("DB_PASSWORD")
    dsn = config("DB_DSN")
except Exception as e:
    logging.error(f"Ошибка при чтении учетных данных: {e}")
    sys.exit(1)

def set_cfg_ora_clnt():
    logging.info("Инициализация Oracle-клиента")
    try:
        if sys.platform.startswith("linux"):
            ora.defaults.config_dir = os.path.join(os.environ.get("HOME"), "instantclient_21_10")
        elif sys.platform.startswith("win32"):
            ora.defaults.config_dir = (r"C:\oracle\instantclient_21_10\network\admin")
        logging.info("Oracle-клиент успешно инициализирован")
    except Exception as err:
        logging.error("Ошибка инициализации Oracle-клиента!")
        logging.error(err)
        sys.exit(1)

def connect_db():
    logging.info("Подключение к базе данных")
    try:
        connection = ora.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()
        logging.info("Подключение к базе данных успешно установлено")
        return connection, cursor
    except ora.DatabaseError as e:
        error, = e.args
        logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        sys.exit(1)

def close_db(connection, cursor):
    logging.info("Закрытие подключения к базе данных")
    if cursor is not None:
        cursor.close()
    if connection is not None:
        connection.close()
    logging.info("Подключение к базе данных закрыто")

def execute_sql(cursor, sql, params=None):
    logging.info(f"Выполнение SQL-запроса: {sql}")
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        logging.info("SQL-запрос выполнен успешно")
    except ora.DatabaseError as e:
        error, = e.args
        logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        raise
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        raise

def create_temp_table():
    logging.info("Создание временной таблицы")
    connection, cursor = None, None
    try:
        connection, cursor = connect_db()

        drop_table_sql = """
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE "BIS"."TEASR_DEF"';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """
        create_table_sql = """
            CREATE TABLE "BIS"."TEASR_DEF" (
                 "DEF" VARCHAR2(20), 
                 "ST" VARCHAR2(20), 
                 "EN" VARCHAR2(20), 
                 "CO" VARCHAR2(20), 
                 "OP" VARCHAR2(200), 
                 "DIR" VARCHAR2(500), 
                 "INN" VARCHAR2(130)
                 )
            """
        execute_sql(cursor, drop_table_sql)
        logging.info("Существующая таблица удалена, если она была")
        execute_sql(cursor, create_table_sql)
        logging.info("Таблица для данных CSV создана")

    except Exception as e:
        logging.error(f"Ошибка: {e}")
        print(f"Ошибка: {e}")
    finally:
        close_db(connection, cursor)

def is_safe_csv_file(csv_path):
    logging.info(f"Проверка безопасности CSV файла: {csv_path}")
    suspicious_patterns = [
        r"\bSELECT\b", r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b",
        r"\bDROP\b", r"\bCREATE\b", r"\bALTER\b", r"\bEXEC\b", r"\bEVAL\b",
        r"\bos\.", r"\bsys\.", r"\bINTO OUTFILE\b", r"\bUNION\b", r"\bJOIN\b",
        r"\bWHERE\b", r"\bEXECUTE IMMEDIATE\b"
    ]

    safe = True

    try:
        with open(csv_path, newline='', encoding='utf-8') as csvfile:
            csvreader = csv.DictReader(csvfile, delimiter=';')

            for row in csvreader:
                for field_name, field_value in row.items():
                    for pattern in suspicious_patterns:
                        if re.search(pattern, field_value, flags=re.IGNORECASE):
                            logging.warning(f"Подозрительный паттерн найден в поле '{field_name}': {field_value}")
                            safe = False

    except Exception as e:
        logging.error(f"Ошибка при проверке CSV-файла: {e}")
        safe = False

    if safe:
        logging.info("CSV-файл прошел проверку на безопасность")
    else:
        logging.error("Обнаружены подозрительные паттерны в CSV-файле")

    return safe

def insert_csv_standart_data(file_path):
    logging.info(f"Загрузка данных из CSV файла: {file_path}")
    if not os.path.isfile(file_path):
        logging.error(f"Файл {file_path} не существует.")
        print(f"Файл {file_path} не существует.")
        return

    if not file_path.lower().endswith('.csv'):
        logging.error(f"Файл {file_path} не является CSV файлом.")
        print(f"Файл {file_path} не является CSV файлом.")
        return

    if not is_safe_csv_file(file_path):
        logging.error("CSV файл не прошел проверку на безопасность.")
        print("CSV файл не прошел проверку на безопасность.")
        return

    connection, cursor = None, None
    try:
        connection, cursor = connect_db()

        with open(file_path, newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=';')
            next(csv_reader)  # Пропускаем заголовок, если он есть
            batch_size = config('BATCH_SIZE', cast=int)
            sql = """INSERT INTO "BIS"."TEASR_DEF" ("DEF", "ST", "EN", "CO", "OP", "DIR",  "INN") VALUES (:1, :2, :3, :4, :5, :6, :7)"""
            data = []
            for line in csv_reader:
                if len(line) >= 8:  # Проверка на минимальное количество элементов в строке
                    prefix, start_range, end_range, capacity, operator, region, _, inn = line
                    try:
                        start_range = int(start_range)
                        end_range = int(end_range)
                        capacity = int(capacity)
                        data.append((prefix, start_range, end_range, capacity, operator, region, inn))
                    except ValueError:
                        logging.warning(f"Неверный формат данных в строке: {line}")
                        continue

                    if len(data) % batch_size == 0:
                        cursor.executemany(sql, data)
                        data = []

            if data:
                cursor.executemany(sql, data)

        connection.commit()
        logging.info(f"Данные из файла {file_path} успешно загружены в базу данных")

    except ora.DatabaseError as e:
        error, = e.args
        logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        print(f"Ошибка базы данных: {error.code}, {error.message}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных из файла: {e}")
        print(f"Ошибка при загрузке данных из файла: {e}")
    finally:
        close_db(connection, cursor)

def get_drct_id(name_csv):
    logging.info(f"Получение DRCT_DRCT_ID для NAME_CSV: {name_csv}")
    connection, cursor = None, None
    result = []

    try:
        connection, cursor = connect_db()
        query = "SELECT DRCT_DRCT_ID FROM BIS.TEASR_PREFIX_DIRECTIONS WHERE NAME_CSV = :name_csv"
        execute_sql(cursor, query, {'name_csv': name_csv})
        result = cursor.fetchall()
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        print(f"Ошибка: {e}")
    finally:
        close_db(connection, cursor)

    return result

def get_all_msisdn():
    logging.info("Получение всех строк из таблицы TEASR_PREFIX_MSISDN")
    connection, cursor = None, None
    result = []

    try:
        connection, cursor = connect_db()
        query = """
            SELECT MSISDN_C
            FROM BIS.TEASR_PREFIX_MSISDN
            WHERE LENGTH(MSISDN_C) = 10 AND MSISDN_C NOT LIKE '%[^0-9]%'
        """
        execute_sql(cursor, query)
        result = cursor.fetchall()
    except Exception as e:
        logging.error(f"Ошибка: {e}")
    finally:
        close_db(connection, cursor)

    return result

def execute_max_pset_id_query():
    logging.info("Получение максимального PSET_ID из двух таблиц")
    connection, cursor = None, None
    try:
        connection, cursor = connect_db()

        # Выполнение первого запроса
        cursor.execute("SELECT MAX(PSET_ID) FROM BIS.PREFIX_SETS")
        max_pset_id = cursor.fetchone()[0] or 0

        # Выполнение второго запроса
        cursor.execute("SELECT MAX(PSET_ID) FROM BIS.TEASR_PREFIX_SETS_EXP_CSV")
        max_teasr_pset_id = cursor.fetchone()[0] or 0

        # Логика обновления переменной c
        c = max(max_pset_id, max_teasr_pset_id)

        logging.info(f"Максимальное значение PSET_ID: {c}")

        return c

    except ora.DatabaseError as e:
        error, = e.args
        logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        print(f"Ошибка базы данных: {error.code}, {error.message}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        print(f"Ошибка: {e}")
    finally:
        close_db(connection, cursor)

def is_prefix_exists(cursor, prefix):
    logging.info(f"Проверка существования PREFIX: {prefix}")
    query = "SELECT 1 FROM \"BIS\".\"TEASR_PREFIX_SETS_EXP_CSV\" WHERE \"PREFIX\" = :prefix"
    cursor.execute(query, [prefix])
    exists = cursor.fetchone() is not None
    logging.info(f"PREFIX {'существует' if exists else 'не существует'}")
    return exists

def insert_csv_updated_data(file_path):
    logging.info(f"Загрузка данных из CSV файла: {file_path}")
    if not os.path.isfile(file_path):
        logging.error(f"Файл {file_path} не существует.")
        print(f"Файл {file_path} не существует.")
        return

    if not file_path.lower().endswith('.csv'):
        logging.error(f"Файл {file_path} не является CSV файлом.")
        print(f"Файл {file_path} не является CSV файлом.")
        return

    if not is_safe_csv_file(file_path):
        logging.error("CSV файл не прошел проверку на безопасность.")
        print("CSV файл не прошел проверку на безопасность.")
        return

    connection, cursor = None, None
    try:
        connection, cursor = connect_db()

        with open(file_path, newline='', encoding='utf-8') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',')
            headers = next(csv_reader)  # Пропускаем заголовок
            batch_size = config('BATCH_SIZE', cast=int)
            sql = """INSERT INTO "BIS"."TEASR_PREFIX_SETS_EXP_CSV" ("PSET_ID", "NUMBER_HISTORY", "OPER_OPER_ID", "PREFIX", "START_DATE",
                                           "END_DATE", "NAVI_USER", "NAVI_DATE", "DRCT_DRCT_ID", "CIT_CIT_ID",
                                           "COU_COU_ID", "PSET_COMMENT", "ODRC_ODRC_ID", "ZONE_ZONE_ID", "AOB_AOB_ID",
                                           "RTCM_RTCM_ID", "ACTION") VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, 
                                           :11, :12, :13, :14, :15, :16, :17)"""
            data = []
            for line in csv_reader:
                if len(line) == 17:  # Проверка на количество элементов в строке
                    pset_id, number_history, oper_oper_id, prefix, start_date, end_date, navi_user, navi_date, drct_drct_id, cit_cit_id, cou_cou_id, pset_comment, odrc_odrc_id, zone_zone_id, aob_aob_id, rtcm_rtcm_id, action = line
                    if is_prefix_exists(cursor, prefix):
                        logging.warning(f"Значение PREFIX '{prefix}' уже существует в таблице. Строка пропущена.")
                        continue
                    try:
                        start_date = datetime.strptime(start_date, '%d-%m-%Y')
                        end_date = datetime.strptime(end_date, '%d-%m-%Y')
                        navi_date = datetime.strptime(navi_date, '%d-%m-%Y %H:%M:%S')
                        data.append((pset_id, number_history, oper_oper_id, prefix, start_date, end_date, navi_user, navi_date, drct_drct_id,
                                     cit_cit_id, cou_cou_id, pset_comment, odrc_odrc_id, zone_zone_id, aob_aob_id, rtcm_rtcm_id, action))
                    except ValueError as e:
                        logging.warning(f"Неверный формат данных в строке: {line}. Ошибка: {e}")
                        continue
                    if len(data) % batch_size == 0:
                        cursor.executemany(sql, data)
                        data = []

            if data:
                cursor.executemany(sql, data)

        connection.commit()
        logging.info(f"Данные из файла {file_path} успешно загружены в базу данных")
        print(f"Данные из файла {file_path} успешно загружены в базу данных")

    except ora.DatabaseError as e:
        error, = e.args
        logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        print(f"Ошибка базы данных: {error.code}, {error.message}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных из файла: {e}")
        print(f"Ошибка при загрузке данных из файла: {e}")
    finally:
        close_db(connection, cursor)
