import pandas as pd
import logging
from db import get_drct_id, get_all_msisdn, execute_max_pset_id_query, insert_csv_updated_data
from datetime import datetime
import os
from decouple import config

# Настройка логгера
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

    logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Логгируем информацию о настройке логгера
    logging.info(f'Настроен логгер для записи в файл: {log_file_path}')

    # Логгируем информацию о текущем времени
    logging.info(f'Текущее время: {datetime.now()}')


def bin_search(df, phone_number):
    prefix = phone_number[:3]
    number = phone_number[3:]

    # Применим бинарный поиск для поиска строки
    low, high = 0, len(df) - 1
    while low <= high:
        mid = (low + high) // 2
        mid_prefix = str(df.iloc[mid]['АВС/ DEF']).zfill(3)

        if mid_prefix == prefix:
            # Проверим диапазон
            if df.iloc[mid]['От'] <= number <= df.iloc[mid]['До']:
                return df.iloc[mid]
            # Если номер меньше текущего диапазона, ищем в левой половине
            if number < df.iloc[mid]['От']:
                high = mid - 1
            else:
                # Иначе ищем в правой половине
                low = mid + 1
        elif mid_prefix < prefix:
            low = mid + 1
        else:
            high = mid - 1
    return None


def compress_numbers(numbers):
    compressed = []
    i = 0
    while i < len(numbers):
        # Проверяем, есть ли 10 подряд идущих чисел с последовательными последними цифрами
        if i + 9 < len(numbers) and all(
                numbers[i + j] % 10 == j for j in range(10)
        ):
            # Добавляем первое число без последнего нуля
            compressed.append(numbers[i] // 10)
            i += 10
        else:
            compressed.append(numbers[i])
            i += 1
    return compressed


def compress_str(prefix, low, high):
    numbers = []
    for i in range(len(low) - 1, -1, -1):
        if int(high[i]) - int(low[i]) == 9:
            low = low[:-1]
            high = high[:-1]
        else:
            numbers = list(range(int(prefix + low), int(prefix + high) + 1))
            break

    return numbers


def write_to_csv(data, file_path):
    """
    Записывает данные в CSV файл с помощью pandas.

    Параметры:
    data (set): Множество кортежей, где каждый кортеж содержит (prefix, region_id).
    file_path (str): Путь к файлу, в который нужно записать данные.
    """
    # Преобразование данных в DataFrame
    df = pd.DataFrame(list(data), columns=['PSET_ID', 'NUMBER_HISTORY', 'OPER_OPER_ID', 'PREFIX', 'START_DATE',
                                           'END_DATE', 'NAVI_USER', 'NAVI_DATE', 'DRCT_DRCT_ID', 'CIT_CIT_ID',
                                           'COU_COU_ID', 'PSET_COMMENT', 'ODRC_ODRC_ID', 'ZONE_ZONE_ID', 'AOB_AOB_ID',
                                           'RTCM_RTCM_ID', 'ACTION'])

    # Сортировка DataFrame по столбцу 'PREFIX'
    df = df.sort_values(by='PREFIX')

    # Запись DataFrame в CSV файл
    df.to_csv(file_path, index=False)


def form_tuple(pset_id, prefix, region_id, nuser):
    """
    Формирует кортеж с данными для записи в CSV.

    Параметры:
    prefix (str): Префикс.
    region_id (int): Идентификатор региона.
    nuser (str): Имя пользователя.

    Возвращает:
    tuple: Кортеж с данными.
    """
    pset_id = pset_id
    number_history = 1
    oper_oper_id = 0
    prefix = prefix
    start_date = '01-01-2000'
    end_date = '31-12-2999'
    navi_user = nuser
    navi_date = datetime.now().strftime('%d-%m-%Y %H:%M:%S')
    drct_drct_id = region_id
    cit_cit_id = 0
    cou_cou_id = 0
    pset_comment = 'ВЗН'
    odrc_odrc_id = None
    zone_zone_id = 0
    aob_aob_id = None
    rtcm_rtcm_id = None
    action = 'MERGE'
    tup = (pset_id, number_history, oper_oper_id, prefix, start_date, end_date, navi_user, navi_date, drct_drct_id,
           cit_cit_id, cou_cou_id, pset_comment, odrc_odrc_id, zone_zone_id, aob_aob_id, rtcm_rtcm_id, action)
    return tup


def check_prefix(numbers, capacity, end):
    arr = numbers.copy()
    count = 0
    m = len(arr)
    for i in range(m):
        if i != len(arr) - 1:
            cur_l = str(arr[i])
            if len(cur_l) < 10:
                cur_l += '0' * (10 - len(cur_l))
            cur_r = str(arr[i + 1])
            if len(cur_r) < 10:
                cur_r += '0' * (10 - len(cur_r))
            tmp = int(cur_r) - int(cur_l)
            count += tmp
        else:
            cur_l = str(arr[i])
            if len(cur_l) < 10:
                cur_l += '0' * (10 - len(cur_l))
            cur_r = str(arr[i])[:3] + end
            if len(cur_r) < 10:
                cur_r = '0' * (10 - len(cur_r)) + cur_r
            tmp = int(cur_r) - int(cur_l)
            count += tmp
    count += 1
    return True if count == capacity else False


def form_prefix(prefix, low, high, capacity):
    numbers = compress_str(prefix, low, high)
    for _ in range(10):
        numbers = compress_numbers(numbers)
    if check_prefix(numbers, capacity, high):
        logging.info('Все префиксы корректны')
    else:
        logging.error('Ошибка при проверке префиксов. Проверьте корректность построения')

    return numbers


def handle_data():
    setup_logging('./logs')  # Настройка логгирования

    file_path = 'DEF-9xx.csv'
    df = pd.read_csv(file_path, delimiter=';', dtype={'От': str, 'До': str})
    phone_numbers = get_all_msisdn() #Вызов номеров в формате  [('9000000000',), ('9999999999',)]
    arr = set()
    if phone_numbers:
        nuser = input('Введите имя пользователя для NAVI_USER: ')
        pset_id = execute_max_pset_id_query()
        prefix_set = set()
        for phone_number in phone_numbers:
            result_str = bin_search(df, phone_number[0])
            if result_str is None:
                logging.warning(f'Для номера {phone_number[0]} не найден соответствующий префикс')
                continue
            prefix = str(result_str['АВС/ DEF'])
            low = result_str['От']
            high = result_str['До']
            region = result_str['Регион']
            capacity = result_str['Емкость']
            region_id = get_drct_id(region)[0][0]
            new_prefix = form_prefix(prefix, low, high, capacity)
            for i in range(len(new_prefix)):
                if new_prefix[i] not in prefix_set:
                    tup = form_tuple(pset_id, new_prefix[i], region_id, nuser)
                    pset_id += 1
                    arr.add(tup)
                    prefix_set.add(new_prefix[i])
        if not arr:
            logging.warning('Не удалось сформировать данные для записи в CSV')

    else:
        logging.warning('Номера не найдены')

    file_path = config('FILE_FOR_PUSH_NAME')
    print(file_path)
    write_to_csv(arr, file_path)
    insert_csv_updated_data(file_path)


if __name__ == '__main__':
    handle_data()
