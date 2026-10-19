"""Конфигурация приложения: config.json, кэш, типизированный доступ.

Модуль вынесен из ``support_functions.py`` в Сессии S5 плана рефакторинга
(см. Tasks.md).

В S8 путь к файлу перестал зависеть от текущего каталога: в собранном .exe
настройки лежат рядом с исполняемым файлом (или в ``%APPDATA%``, если папка
доступна только для чтения), в режиме разработки — в корне проекта.
Повреждённый файл больше не перетирается молча: рядом остаётся его копия.
"""
import copy
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from flatregister.errors import ConfigError
from flatregister.logging_setup import get_logger

_log = get_logger("config")

CONFIG_FILE_NAME = "config.json"
CONFIG_PATH_ENV = "FLATREGISTER_CONFIG_PATH"
APP_DIR_NAME = "FlatRegister1C"

# Глобальная переменная для кэширования конфигурации
_config_cache: Dict[str, Any] = {}

# Значения по умолчанию для config.json
DEFAULT_CONFIG = {
    "accounts_without_subaccount": [
        ["50", "50", 1],
        ["51", "51", 1],
        ["52", "52", 1],
        ["55", "55", 1],
        ["57", "57", 1],
    ],
    "general_settings": {
        "parallel_processing": False
    },
    "keep_subaccounts": False  # True оставлять все субсчета (включая промежуточные)
}


PathLike = Union[str, Path, None]


def _is_frozen() -> bool:
    """Приложение собрано Nuitka/PyInstaller, а не запущено из исходников."""
    return bool(getattr(sys, "frozen", False) or "__compiled__" in globals())


def _appdata_dir() -> Path:
    """Каталог настроек пользователя — запасной вариант для .exe."""
    base = os.environ.get("APPDATA") or os.environ.get("LOCALAPPDATA") or Path.home()
    return Path(base) / APP_DIR_NAME


def _is_writable_dir(path: Path) -> bool:
    """Проверить запись реальной пробой, а не только правами из stat."""
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path, prefix=".write-test-", delete=True):
            pass
        return True
    except OSError:
        return False


def get_config_path() -> Path:
    """Полный путь к config.json.

    Приоритет: явная переменная окружения (нужна тестам и переносимым
    запускам) → папка рядом с .exe, если она доступна для записи → ``%APPDATA%``.
    В режиме разработки — корень проекта.
    """
    override = os.environ.get(CONFIG_PATH_ENV)
    if override:
        path = Path(override).expanduser()
        return path / CONFIG_FILE_NAME if path.is_dir() else path

    if _is_frozen():
        executable_dir = Path(sys.executable).resolve().parent
        if _is_writable_dir(executable_dir):
            return executable_dir / CONFIG_FILE_NAME
        return _appdata_dir() / CONFIG_FILE_NAME

    return Path(__file__).resolve().parent.parent / CONFIG_FILE_NAME


def _resolve_path(config_path: PathLike) -> Path:
    if config_path is None:
        return get_config_path()
    return Path(config_path)


# Добавляем функцию для очистки кэша, чтобы можно было перечитать конфиг
def clear_config_cache():
    global _config_cache
    _config_cache = {}


def write_default_config(config_path: PathLike = None):
    """Создает файл config.json со значениями по умолчанию.

    При недоступной для записи папке бросает :class:`ConfigError` наружу.
    Раньше ошибка уходила в ``print()``, которого в GUI-приложении не видно:
    пользователь получал настройки по умолчанию и не понимал, почему его
    выбор не применяется.
    """
    path = _resolve_path(config_path)

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(DEFAULT_CONFIG, file, ensure_ascii=False, indent=4)
    except OSError as error:
        _log.error("Не удалось создать файл конфигурации %s", path, exc_info=True)
        raise ConfigError(
            "Не удалось создать файл настроек config.json.",
            hint=f"Проверьте права на запись в папку {path.parent}. "
                 "Подробности — в журнале logs/app.log.",
        ) from error


def _backup_broken_config(path: Path) -> Optional[Path]:
    """Сохранить копию повреждённого файла рядом с оригиналом."""
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = path.with_name(f"{path.name}.bak-{stamp}")
    counter = 2
    while backup.exists():
        backup = path.with_name(f"{path.name}.bak-{stamp}-{counter}")
        counter += 1

    try:
        shutil.copy2(path, backup)
    except OSError:
        _log.error("Не удалось сохранить резервную копию повреждённого %s",
                   path, exc_info=True)
        return None
    return backup


def _reset_broken_config(path: Path, error: Exception) -> dict:
    """Бэкап повреждённого файла + честное предупреждение + сброс настроек."""
    backup = _backup_broken_config(path)
    if backup is None:
        _log.warning(
            "Файл конфигурации %s повреждён (%s); резервную копию создать не удалось. "
            "Работаю на настройках по умолчанию.",
            path, error,
        )
        return copy.deepcopy(DEFAULT_CONFIG)

    _log.warning(
        "Файл конфигурации %s повреждён (%s); резервная копия: %s. "
        "Заменяю значениями по умолчанию.",
        path, error, backup,
    )
    return _bootstrap_default_config(path)


def read_config(config_path: PathLike = None) -> dict:
    """
    Читает и возвращает текущую конфигурацию из JSON файла.
    Если файл не найден — создаёт его со значениями по умолчанию.
    Если файл повреждён — сохраняет копию и создаёт новый.
    """
    path = _resolve_path(config_path)

    # 1. Попытка прочитать файл
    try:
        with open(path, 'r', encoding='utf-8') as file:
            return json.load(file)

    # 2. Файла нет — создаём со значениями по умолчанию
    except FileNotFoundError:
        return _bootstrap_default_config(path)

    # 3. Некорректный формат или кодировка — бэкап и предупреждение,
    #    а не молчаливая потеря пользовательских настроек.
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        return _reset_broken_config(path, error)

    # 4. Остальное: папка вместо файла, нет прав на чтение.
    except Exception:
        _log.error("Не удалось прочитать конфигурацию %s", path, exc_info=True)
        return copy.deepcopy(DEFAULT_CONFIG)


def _bootstrap_default_config(path: Path) -> dict:
    """Создать config.json со значениями по умолчанию, не ломая чтение настроек.

    Отличается от :func:`write_default_config` тем, что ошибку записи
    проглатывает: невозможность создать файл настроек не должна превращать
    чтение конфигурации в отказ обработки файлов.
    """
    try:
        write_default_config(path)
    except ConfigError:
        _log.warning("Работаю на настройках по умолчанию: создать %s не удалось", path)
    return copy.deepcopy(DEFAULT_CONFIG)


def update_config(updates, config_path: PathLike = None):
    """
    Вносит изменения в файл конфигурации JSON

    Args:
        config_path (str): Путь к файлу config.json
        updates (dict): Словарь с обновлениями для конфигурации
    """
    path = _resolve_path(config_path)

    # Сначала читаем конфигурацию, которая теперь гарантированно вернет либо существующую, либо дефолтную
    config = read_config(path)

    try:
        # Рекурсивное обновление конфигурации
        def deep_update(current_dict, update_dict):
            for key, value in update_dict.items():
                if (key in current_dict and
                    isinstance(current_dict[key], dict) and
                    isinstance(value, dict)):
                    deep_update(current_dict[key], value)
                else:
                    current_dict[key] = value

        # Применение обновлений
        deep_update(config, updates)

        # Запись обновленной конфигурации
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(config, file, ensure_ascii=False, indent=4)

        clear_config_cache()
        return True

    except OSError as error:
        # Раньше здесь был голый `return False`, и настройка молча не
        # сохранялась: пользователь нажимал «Сохранить» и ничего не происходило.
        _log.error("Не удалось сохранить настройки в %s: %s", path, error)
        return False


def load_config(file_path: PathLike = None) -> Dict[str, Any]:
    global _config_cache
    if not _config_cache:
        _config_cache = read_config(file_path)
    return _config_cache


# КоррСчета, субсчета по которым не включаем в итоговые файлы, оставляем только счета
def get_accounts_without_subaccount() -> List[str]:
    config = read_config()
    accounts_without_subaccount = config.get("accounts_without_subaccount",
                                                          DEFAULT_CONFIG.get("accounts_without_subaccount", {}))
    return [i[0] for i in accounts_without_subaccount if i[2] == 1]


# Получить настройку - паралельная обработка или последовательная
def get_parallel_processing_option() -> bool:
    config = read_config()
    general_options = config.get("general_settings", DEFAULT_CONFIG.get("general_settings", {}))
    parallel_processing_option = bool(general_options.get("parallel_processing", 0))
    return parallel_processing_option


# Получить настройку для общей ОСВ - показываем все счета или только самого глубокого уровня
def get_keep_subaccounts() -> bool:
    config = read_config()
    keep_subaccounts = config.get("keep_subaccounts", DEFAULT_CONFIG.get("keep_subaccounts", True))
    return keep_subaccounts
