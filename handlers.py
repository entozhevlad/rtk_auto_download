import pandas as pd
from db import get_drct_id, get_all_msisdn, process_and_insert_data, create_final_table

# Загрузим данные из CSV файла в pandas DataFrame
file_path = 'DEF-9xx.csv'
df = pd.read_csv(file_path, delimiter=';', dtype={'От': str, 'До': str})


# Функция для поиска строки по номеру телефона
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
    for i in range(len(low)-1, -1,-1):
        if int(high[i]) - int(low[i]) == 9:
            low = low[:-1]
            high = high[:-1]
        else:
            numbers = list(range(int(prefix+low), int(prefix+high)+1))
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
    df = pd.DataFrame(list(data), columns=['prefix', 'region_id'])
    # Запись DataFrame в CSV файл
    df.to_csv(file_path, index=False)



phone_numbers = get_all_msisdn()
arr = set()
if phone_numbers:
    for phone_number in phone_numbers:
        result_str = bin_search(df, phone_number[0])
        prefix = str(result_str['АВС/ DEF'])
        low = result_str['От']
        high = result_str['До']
        region = result_str['Регион']
        region_id = get_drct_id(region)[0][0]
        numbers = compress_str(prefix, low, high)
        new_prefix = compress_numbers(numbers)
        for i in range(len(new_prefix)):
            arr.add((new_prefix[i], region_id))
else:
    print('Номера не найдены')

file_path = 'output.csv'
write_to_csv(arr, file_path)
create_final_table()
process_and_insert_data()
