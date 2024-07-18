import os
import shutil
import git
from decouple import config
import stat
import time
import datetime
import gc
import sys
from typing import Callable, Optional

# Путь к локальному CSV файлу, который нужно отправить
csv_file_path: str = config('FILE_FOR_PUSH_NAME')

# Путь к целевому локальному репозиторию
target_repo_path: str = 'tmp/test'

# URL удалённого репозитория
remote_repo_url: str = config('GIT_URL')
remote_branch: str = config('SRC_REMOTE_BRANCH')
new_branch_name: str = config('NEW_REMOTE_BRANCH')


def handle_remove_readonly(func: Callable, path: str, exc: Optional[Exception]) -> None:
    """
    Изменяет права доступа для удаления файла или папки.

    Параметры:
    func (Callable): Функция удаления файла или папки.
    path (str): Путь к файлу или папке.
    exc (Optional[Exception]): Исключение, возникшее при попытке удаления.
    """
    os.chmod(path, stat.S_IWRITE)
    func(path)


def delete_tmp_folder(folder_path: str, max_retries: int = 5, delay: int = 2) -> None:
    """
    Удаляет папку, если она существует, с повторными попытками.

    Параметры:
    folder_path (str): Путь к папке, которую нужно удалить.
    max_retries (int): Максимальное количество попыток удаления.
    delay (int): Задержка между попытками удаления.
    """
    if os.path.exists(folder_path):
        for i in range(max_retries):
            try:
                shutil.rmtree(folder_path, onerror=handle_remove_readonly)
                print(f"Папка {folder_path} удалена.")
                return
            except PermissionError as e:
                print(f"Ошибка при удалении папки: {e}")
                time.sleep(delay)
        print(f"Не удалось удалить папку {folder_path} после {max_retries} попыток.")
    else:
        print(f"Папка {folder_path} не существует.")


def upload_to_git_via_ssh() -> None:
    """
    Копирует файл в локальный репозиторий и отправляет его в удалённый репозиторий на новую ветку.
    """
    try:
        # Проверка существования файла
        if not os.path.exists(csv_file_path):
            print(f"Файл {csv_file_path} не существует")
            return

        # Удаление старого репозитория, если он существует
        delete_tmp_folder('tmp')

        # Клонирование целевого репозитория
        try:
            target_repo = git.Repo.clone_from(remote_repo_url, target_repo_path, branch=remote_branch)
        except git.exc.GitCommandError as e:
            print(f"Ошибка при клонировании репозитория: {e}")
            return

        # Создание новой ветки
        new_branch = target_repo.create_head(new_branch_name)
        target_repo.head.reference = new_branch
        target_repo.head.reset(index=True, working_tree=True)

        # Получение текущей даты и времени
        current_time = datetime.datetime.now()
        formatted_time = current_time.strftime("%Y%d%m%H%M")

        # Создание директории для файла
        target_directory = os.path.join(target_repo_path, 'components', 'base', formatted_time)
        os.makedirs(target_directory, exist_ok=True)

        # Копирование файла в целевой репозиторий
        try:
            target_file_path = os.path.join(target_directory, os.path.basename(csv_file_path))
            shutil.copy2(csv_file_path, target_file_path)
        except (shutil.Error, IOError) as e:
            print(f"Ошибка при копировании файла: {e}")
            return

        # Проверка изменений в репозитории
        try:
            target_repo.git.add(A=True)
            if target_repo.is_dirty():
                # Коммит изменений
                target_repo.index.commit(f"Add {csv_file_path}")

                # Пуш изменений в новую ветку
                origin = target_repo.remotes.origin
                origin.push(refspec=f"HEAD:refs/heads/{new_branch_name}")
                print(f"Файл {csv_file_path} успешно запушен в {remote_repo_url} на ветку {new_branch_name}")
            else:
                print("Нет изменений для коммита.")
        except git.exc.GitCommandError as e:
            print(f"Ошибка при выполнении git-команды: {e}")

        # Закрытие репозитория и сборка мусора
        target_repo.close()
        gc.collect()

    except Exception as e:
        print(f"Произошла ошибка: {e}")
        sys.exit(1)

    # Удаление временной папки после завершения работы
    delete_tmp_folder('tmp')


# Пример использования
if __name__ == "__main__":
    upload_to_git_via_ssh()
