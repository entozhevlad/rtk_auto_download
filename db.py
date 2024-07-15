import oracledb as ora
import logging
import csv
from decouple import config
import sys
import os
import re
import pandas as pd

# Учетные данные для подключения к базе данных
try:
    username = config("DB_USERNAME")
    password = config("DB_PASSWORD")
    dsn = config("DB_DSN")
except Exception as e:
    logging.error(f"Ошибка при чтении учетных данных: {e}")
    sys.exit(1)


def set_cfg_ora_clnt():
    """
    Устанавливает конфигурацию Oracle-клиента в зависимости от операционной системы.

    Возвращает:
    str: Путь к каталогу конфигурации Oracle-клиента.
    """
    try:
        if sys.platform.startswith("linux"):
            ora.defaults.config_dir = os.path.join(os.environ.get("HOME"), "instantclient_21_10")
        elif sys.platform.startswith("win32"):
            ora.defaults.config_dir = (r"C:\oracle\instantclient_21_10\network\admin")
    except Exception as err:
        logging.error("Ошибка инициализации Oracle-клиента!")
        logging.error(err)
        sys.exit(1)
    return ora.defaults.config_dir


def create_temp_table():
    """
    Создает временную таблицу для хранения данных из CSV файла.

    Действия:
    1. Подключается к базе данных.
    2. Удаляет таблицу TEASR_DEF, если она существует.
    3. Создает новую таблицу TEASR_DEF с заданной структурой.
    4. Записывает результат в лог и выводит сообщение на экран в случае ошибки.
    """
    connection = None
    cursor = None
    try:
        connection = ora.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

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
        cursor.execute(drop_table_sql)
        logging.info("Существующая таблица удалена, если она была")

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
        cursor.execute(create_table_sql)
        logging.info("Таблица для данных CSV создана")

    except ora.DatabaseError as e:
        error, = e.args
        if error.code == 942:  # Ошибка - таблица не существует
            logging.info("Таблица TEASR_DEF не существует, создаем новую.")
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
            cursor.execute(create_table_sql)
            logging.info("Таблица для данных CSV создана успешно.")
        else:
            logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
            print(f"Ошибка базы данных: {error.code}, {error.message}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        print(f"Ошибка: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def is_safe_csv_file(csv_path):
    """
    Проверяет CSV файл на наличие подозрительных паттернов.

    Параметры:
    csv_path (str): Путь к CSV файлу.

    Возвращает:
    bool: True, если файл безопасен, иначе False.
    """
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


def insert_csv_data(file_path):
    """
    Загружает данные из CSV файла в таблицу TEASR_DEF.

    Параметры:
    file_path (str): Путь к файлу CSV, который нужно загрузить.

    Действия:
    1. Проверяет, что файл существует и является CSV.
    2. Проверяет файл на наличие подозрительных паттернов.
    3. Подключается к базе данных.
    4. Читает данные из CSV файла.
    5. Пакетно вставляет данные в таблицу TEASR_DEF.
    6. Записывает результат в лог и выводит сообщение на экран в случае ошибки.
    """
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

    connection = None
    cursor = None
    try:
        connection = ora.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

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
        if error.code == 942:  # Ошибка - таблица не существует
            logging.error(f"Таблица TEASR_DEF не существует.")
        elif error.code == 904:  # Ошибка - неверный идентификатор объекта (возможно, отсутствует столбец)
            logging.error(f"Неверный идентификатор объекта в SQL запросе: {error.message}")
        else:
            logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        print(f"Ошибка базы данных: {error.code}, {error.message}")
    except ora.InterfaceError as e:
        logging.error(f"Ошибка подключения к базе данных: {e}")
        print(f"Ошибка подключения к базе данных: {e}")
    except FileNotFoundError as e:
        logging.error(f"Файл не найден: {e}")
        print(f"Файл не найден: {e}")
    except Exception as e:
        logging.error(f"Ошибка при загрузке данных из файла: {e}")
        print(f"Ошибка при загрузке данных из файла: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def get_drct_id(name_csv):
    """
    Выполняет SQL-запрос для получения DRCT_DRCT_ID по указанному NAME_CSV.

    Параметры:
    name_csv (str): Значение NAME_CSV для поиска.

    Возвращает:
    list: Список результатов запроса.
    """
    connection = None
    cursor = None
    result = []

    try:
        connection = ora.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        query = "SELECT DRCT_DRCT_ID FROM BIS.TEASR_PREFIX_DIRECTIONS WHERE NAME_CSV = :name_csv"
        cursor.execute(query, name_csv=name_csv)

        result = cursor.fetchall()

    except ora.DatabaseError as e:
        error, = e.args
        if error.code == 942:  # Ошибка - таблица не существует
            logging.error(f"Таблица TEASR_PREFIX_DIRECTIONS не существует.")
        elif error.code == 904:  # Ошибка - неверный идентификатор объекта (возможно, отсутствует столбец)
            logging.error(f"Неверный идентификатор объекта в SQL запросе: {error.message}")
        else:
            logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        print(f"Ошибка базы данных: {error.code}, {error.message}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        print(f"Ошибка: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()

    return result


def get_all_msisdn():
    """
    Получает все строки из таблицы TEASR_PREFIX_MSISDN.

    Возвращает:
    list: Список кортежей с результатами запроса.
    """
    connection = None
    cursor = None
    try:
        connection = ora.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        query = """
            SELECT MSISDN_C
            FROM BIS.TEASR_PREFIX_MSISDN
        """
        cursor.execute(query)
        result = cursor.fetchall()

        return result

    except ora.DatabaseError as e:
        error, = e.args
        if error.code == 942:  # Ошибка - таблица не существует
            logging.error(f"Таблица TEASR_PREFIX_MSISDN не существует.")
        elif error.code == 904:  # Ошибка - неверный идентификатор объекта (возможно, отсутствует столбец)
            logging.error(f"Неверный идентификатор объекта в SQL запросе: {error.message}")
        else:
            logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        return None
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        return None
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()


def process_and_insert_data():
    """
    Выполняет логику обработки данных и вставки их в таблицы базы данных.
    """
    # Установим конфигурацию Oracle клиента
    set_cfg_ora_clnt()

    # Путь к CSV файлу
    file_path = 'output.csv'

    # Загрузка данных из CSV файла
    data = pd.read_csv(file_path)

    # Подключение к базе данных
    connection = None
    cursor = None
    try:
        connection = ora.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        # Определение максимальных значений PSET_ID
        cursor.execute("SELECT max(ps.PSET_ID) FROM prefix_sets ps")
        max_pset_id = cursor.fetchone()[0] or 0

        cursor.execute("SELECT max(p.PSET_ID) FROM teasr_prefix_sets_exp_csv p")
        max_teasr_pset_id = cursor.fetchone()[0] or 0

        c = max(max_pset_id, max_teasr_pset_id)

        # Вставка данных из CSV файла в соответствующие таблицы
        for index, row in data.iterrows():
            p, b = row['prefix'], row['region_id']
            c += 1

            if str(p).startswith('D'):
                cursor.execute("""
                    INSERT INTO teasr_prefix_sets_exp_csv (PSET_ID, PREFIX, REGION_ID)
                    VALUES (:1, :2, :3)
                """, (c, p, b))

                cursor.execute("""
                    INSERT INTO bis.teasr_mnp_rating_histories_exp_csv (PREFIX, REGION_ID)
                    VALUES (:1, :2)
                """, (p, b))
            else:
                cursor.execute("""
                    INSERT INTO teasr_prefix_sets_exp_csv (PSET_ID, PREFIX, REGION_ID)
                    VALUES (:1, :2, :3)
                """, (c, p, b))

            cursor.execute("""
                INSERT INTO teasr_ins_matrix_dir_hist_exp_csv (COLUMN1, COLUMN2, ...)
                SELECT md.COLUMN1, md.COLUMN2, ...
                FROM teasr_ins_matrix_dir_hist_csv md
                WHERE md.drct_drct_id = :1
            """, (b,))

        # Коммит транзакции
        connection.commit()
        logging.info("Данные успешно обработаны и вставлены в таблицы базы данных.")

    except ora.DatabaseError as e:
        error, = e.args
        logging.error(f"Ошибка базы данных: {error.code}, {error.message}")
        print(f"Ошибка базы данных: {error.code}, {error.message}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        print(f"Ошибка: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def create_final_table():
    """
    Создает временную таблицу для хранения данных из CSV файла.

    Действия:
    1. Подключается к базе данных.
    2. Удаляет таблицу TEASR_DEF, если она существует.
    3. Создает новую таблицу TEASR_DEF с заданной структурой.
    4. Записывает результат в лог и выводит сообщение на экран в случае ошибки.
    """
    connection = None
    cursor = None
    try:
        connection = ora.connect(user=username, password=password, dsn=dsn)
        cursor = connection.cursor()

        drop_table_sql = """
            BEGIN
                EXECUTE IMMEDIATE 'DROP TABLE "BIS"."TEASR_PREFIX_SETS_EXP_CSV"';
            EXCEPTION
                WHEN OTHERS THEN
                    IF SQLCODE != -942 THEN
                        RAISE;
                    END IF;
            END;
            """
        cursor.execute(drop_table_sql)
        logging.info("Существующая таблица удалена, если она была")

        create_table_sql = """
            CREATE TABLE "BIS"."TEASR_PREFIX_SETS_EXP_CSV" (
                     "PSET_ID" NUMBER(10,0) NOT NULL ENABLE, 
                     "NUMBER_HISTORY" NUMBER(10,0) NOT NULL ENABLE, 
                     "OPER_OPER_ID" NUMBER(10,0) NOT NULL ENABLE, 
                     "PREFIX" VARCHAR2(63) NOT NULL ENABLE, 
                     "START_DATE" DATE NOT NULL ENABLE, 
                     "END_DATE" DATE NOT NULL ENABLE, 
                     "NAVI_USER" VARCHAR2(70) NOT NULL ENABLE, 
                     "NAVI_DATE" DATE NOT NULL ENABLE, 
                     "DRCT_DRCT_ID" NUMBER(10,0), 
                     "CIT_CIT_ID" NUMBER(10,0), 
                     "COU_COU_ID" NUMBER(10,0), 
                     "PSET_COMMENT" VARCHAR2(2000), 
                     "ODRC_ODRC_ID" NUMBER(10,0), 
                     "ZONE_ZONE_ID" NUMBER(10,0) NOT NULL ENABLE, 
                     "AOB_AOB_ID" NUMBER(10,0), 
                     "RTCM_RTCM_ID" NUMBER(9,0), 
                     "ACTION" VARCHAR2(10)
                     )

            """
        cursor.execute(create_table_sql)
        logging.info("Таблица для данных CSV создана")
    except Exception as e:
        logging.error(f"Ошибка: {e}")
        print(f"Ошибка: {e}")
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
