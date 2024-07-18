import os
import shutil
import git
from decouple import config

# Путь к локальному CSV файлу, который нужно отправить
csv_file_path = 'output.csv'

# Путь к целевому локальному репозиторию
target_repo_path = 'tmp/test'

# URL удалённого репозитория
remote_repo_url = config('GIT_URL')
remote_branch = config('REMOTE_BRANCH')

def upload_to_git():
    """
    Копирует файл в локальный репозиторий и отправляет его в удалённый репозиторий.
    """
    try:
        # Проверка существования файла
        if not os.path.exists(csv_file_path):
            print(f"Файл {csv_file_path} не существует")
            return

        # Клонирование целевого репозитория, если его нет
        if not os.path.exists(target_repo_path):
            os.makedirs(target_repo_path)
            try:
                target_repo = git.Repo.clone_from(remote_repo_url, target_repo_path)
            except git.exc.GitCommandError as e:
                print(f"Ошибка при клонировании репозитория: {e}")
                return
        else:
            try:
                target_repo = git.Repo(target_repo_path)
            except git.exc.InvalidGitRepositoryError as e:
                print(f"Неверный путь к репозиторию: {e}")
                return

        # Копирование файла в целевой репозиторий
        try:
            target_file_path = os.path.join(target_repo_path, os.path.basename(csv_file_path))
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

                # Настройка удалённого репозитория
                if 'origin' not in target_repo.remotes:
                    origin = target_repo.create_remote('origin', remote_repo_url)
                else:
                    origin = target_repo.remotes.origin
                origin.set_url(remote_repo_url)
                origin.push(refspec=f"HEAD:{remote_branch}")
                print(f"Файл {csv_file_path} успешно запушен в {remote_repo_url}")
            else:
                print("Нет изменений для коммита.")
        except git.exc.GitCommandError as e:
            print(f"Ошибка при выполнении git-команды: {e}")

    except Exception as e:
        print(f"Произошла ошибка: {e}")
