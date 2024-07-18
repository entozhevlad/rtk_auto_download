import os


def delete_csv_and_logs():
    # Получаем текущую рабочую директорию
    current_dir = os.getcwd()

    try:
        # Получаем список файлов и папок в текущей директории
        file_list = os.listdir(current_dir)

        # Проходим по всем элементам текущей директории
        for item in file_list:
            item_path = os.path.join(current_dir, item)

            # Удаляем .csv файлы
            if item.endswith('.csv') and os.path.isfile(item_path):
                os.remove(item_path)
                print(f'Удален файл: {item_path}')

            # Удаляем папку logs и её содержимое
            elif item == 'logs' and os.path.isdir(item_path):
                # Проходим по всем файлам в папке logs
                logs_files = os.listdir(item_path)
                for log_file in logs_files:
                    log_file_path = os.path.join(item_path, log_file)
                    os.remove(log_file_path)
                    print(f'Удален файл: {log_file_path}')

                # Удаляем саму папку logs
                os.rmdir(item_path)
                print(f'Удалена папка: {item_path}')

    except Exception as e:
        print(f'Ошибка при удалении файлов или папок: {e}')


# Вызываем функцию для удаления .csv файлов и папки logs
if __name__ == '__main__':
    delete_csv_and_logs()
