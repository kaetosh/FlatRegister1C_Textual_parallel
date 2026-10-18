"""Конфигурация приложения: config.json, кэш, типизированный доступ.

Модуль вынесен из ``support_functions.py`` в Сессии S5 плана рефакторинга
(см. Tasks.md). Поведение не менялось.
"""
import json
from pathlib import Path
from typing import Any, Dict, List

from flatregister.errors import ConfigError
from flatregister.logging_setup import get_logger

_log = get_logger("config")

# Глобальная переменная для кэширования конфигурации
_config_cache: Dict[str, Any] = {}
CONFIG_FILE_PATH = "config.json"

# Значения по умолчанию для config.json
# Используем значения из config.json, который вы предоставили в первом сообщении
DEFAULT_CONFIG = {
    "accounts_without_subaccount": [
	[
            "50",
            "50",
            1
        ],        
	[
            "51",
            "51",
            1
        ],	
	[
            "52",
            "52",
            1
        ],
        [
            "55",
            "55",
            1
        ],
        [
            "57",
            "57",
            1
        ]
    ],
    "general_settings": {
        "parallel_processing": False
    },
    "keep_subaccounts": False # True оставлять все субсчета (включая промежуточные)
}

# Добавляем функцию для очистки кэша, чтобы можно было перечитать конфиг
def clear_config_cache():
    global _config_cache
    _config_cache = {}

def write_default_config(config_path: str = None):
    """Создает файл config.json со значениями по умолчанию.

    При недоступной для записи папке бросает :class:`ConfigError` наружу.
    Раньше ошибка уходила в ``print()``, которого в GUI-приложении не видно:
    пользователь получал настройки по умолчанию и не понимал, почему его
    выбор не применяется.
    """
    if config_path is None:
        config_path = CONFIG_FILE_PATH

    try:
        with open(config_path, 'w', encoding='utf-8') as file:
            json.dump(DEFAULT_CONFIG, file, ensure_ascii=False, indent=4)
    except OSError as error:
        _log.error("Не удалось создать файл конфигурации %s", config_path, exc_info=True)
        raise ConfigError(
            "Не удалось создать файл настроек config.json.",
            hint=f"Проверьте права на запись в папку {Path(config_path).parent}. "
                 "Подробности — в журнале logs/app.log.",
        ) from error


def read_config(config_path: str = None) -> dict:
    """
    Читает и возвращает текущую конфигурацию из JSON файла.
    Если файл не найден или некорректен, создает его со значениями по умолчанию.
    """
    if config_path is None:
        config_path = CONFIG_FILE_PATH

    # 1. Попытка прочитать файл
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            return json.load(file)

    # 2. Файла нет — создаём со значениями по умолчанию
    except FileNotFoundError:
        return _bootstrap_default_config(config_path)

    # 3. Некорректный формат — перезаписываем значениями по умолчанию
    #    (само перетирание пользовательских настроек разбирается в S8)
    except json.JSONDecodeError:
        _log.warning("Файл конфигурации %s повреждён, заменяю значениями по умолчанию",
                     config_path)
        return _bootstrap_default_config(config_path)

    # 4. Остальное: битая кодировка, папка вместо файла, нет прав на чтение.
    #    Сужать до OSError здесь нельзя — json.load бросает ещё и UnicodeDecodeError,
    #    и раньше такой случай тихо уходил в значения по умолчанию.
    except Exception:
        _log.error("Не удалось прочитать конфигурацию %s", config_path, exc_info=True)
        return DEFAULT_CONFIG


def _bootstrap_default_config(config_path: str) -> dict:
    """Создать config.json со значениями по умолчанию, не ломая чтение настроек.

    Отличается от :func:`write_default_config` тем, что ошибку записи
    проглатывает: невозможность создать файл настроек не должна превращать
    чтение конфигурации в отказ обработки файлов.
    """
    try:
        write_default_config(config_path)
    except ConfigError:
        _log.warning("Работаю на настройках по умолчанию: создать %s не удалось",
                     config_path)
    return DEFAULT_CONFIG 

def update_config(updates, config_path: str = None):
    
    """
    Вносит изменения в файл конфигурации JSON
    
    Args:
        config_path (str): Путь к файлу config.json
        updates (dict): Словарь с обновлениями для конфигурации
    """
    
    if config_path is None:
        config_path = CONFIG_FILE_PATH
    
    # Сначала читаем конфигурацию, которая теперь гарантированно вернет либо существующую, либо дефолтную
    config = read_config(config_path)
    
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
        with open(config_path, 'w', encoding='utf-8') as file:
            json.dump(config, file, ensure_ascii=False, indent=4)

        clear_config_cache()
        return True

    except OSError as error:
        # Раньше здесь был голый `return False`, и настройка молча не
        # сохранялась: пользователь нажимал «Сохранить» и ничего не происходило.
        _log.error("Не удалось сохранить настройки в %s: %s", config_path, error)
        return False

def load_config(file_path: str = CONFIG_FILE_PATH) -> Dict[str, Any]:
    global _config_cache
    if not _config_cache:
        _config_cache = read_config(file_path)
    return _config_cache


# КоррСчета, субсчета по которым не включаем в итоговые файлы, оставляем только счета
def get_accounts_without_subaccount() -> List[str]:
    config = read_config()
    accounts_without_subaccount = config.get("accounts_without_subaccount",
                                                          DEFAULT_CONFIG.get("accounts_without_subaccount", {}))
    return [i[0] for i in accounts_without_subaccount if i[2]==1]

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
