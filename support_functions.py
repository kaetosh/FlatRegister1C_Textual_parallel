# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 09:45:22 2025

@author: a.karabedyan
"""
import json
import zipfile
from io import BytesIO
from pathlib import Path
from typing import List, Iterable, Dict, Any
import math
import re, os
import pandas as pd
from typing import Optional
from itertools import product


from custom_errors import IncorrectFolderOrFilesPath
# Глобальные константы
MAX_EXCEL_ROWS = 1_000_000
COLUMN_PREFIXES = ('Операция_', 'Содержание_', 'Документ_', 'Аналитика Дт_', 'Субконто Дт_', 'Аналитика Кт_', 'Субконто Кт_', 'Level_')

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
    ]
}

# Добавляем функцию для очистки кэша, чтобы можно было перечитать конфиг
def clear_config_cache():
    global _config_cache
    _config_cache = {}

def write_default_config(config_path: str = None):
    """Создает файл config.json со значениями по умолчанию."""
    if config_path is None:
        config_path = CONFIG_FILE_PATH
    
    try:
        with open(config_path, 'w', encoding='utf-8') as file:
            json.dump(DEFAULT_CONFIG, file, ensure_ascii=False, indent=4)
        # print(f"Создан файл конфигурации по умолчанию: {config_path}")
    except Exception as e:
        print(f"Ошибка при создании файла конфигурации по умолчанию: {e}")

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
            config = json.load(file)
            return config
    
    # 2. Обработка FileNotFoundError: файл не найден
    except FileNotFoundError:
        # print(f"Файл конфигурации {config_path} не найден. Создание файла по умолчанию.")
        write_default_config(config_path)
        return DEFAULT_CONFIG
        
    # 3. Обработка json.JSONDecodeError: некорректный формат
    except json.JSONDecodeError:
        # print(f"Ошибка: Неверный формат JSON в файле {config_path}. Файл будет перезаписан значениями по умолчанию.")
        write_default_config(config_path)
        return DEFAULT_CONFIG
        
    # 4. Обработка других ошибок
    except Exception as e:
        print(f"Непредвиденная ошибка при чтении конфигурации: {e}. Возврат значений по умолчанию.")
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
        
        # print("Конфигурация успешно обновлена")
        clear_config_cache()
        return True
        
    except Exception:
        return False

def load_config(file_path: str = CONFIG_FILE_PATH) -> Dict[str, Any]:
    global _config_cache
    if not _config_cache:
        _config_cache = read_config(file_path)
    return _config_cache


def sort_columns(df: pd.DataFrame, desired_order: List[str]) -> pd.DataFrame:
    cols = df.columns.tolist()
    
    # 1. Фиксированные колонки (не имеющие числового суффикса)
    fixed_cols = [
        col for col in desired_order 
        if col in cols and not any(col.startswith(prefix) for prefix in COLUMN_PREFIXES)
    ]

    # 2. Группируем колонки по префиксам
    grouped_cols = {}
    for prefix in COLUMN_PREFIXES:
        prefix_cols = [col for col in cols if col.startswith(prefix)]
        grouped_cols[prefix] = sorted(
            prefix_cols, 
            key=lambda x: int(re.search(rf"{re.escape(prefix)}(\d+)", x).group(1) or 0)
        )
    
    # --- Новая часть для обработки колонок вида:
    # [Количество_|ВалютнаяСумма_]<число или число.число или число.буквы>_<до|ко>
    
    # Определяем префиксы для группировки
    special_prefixes = ['', 'Количество_', 'ВалютнаяСумма_']
    # Суффиксы, которые идут в конце
    suffixes = ['_до', '_ко']
    
    # Функция для разбора ключа сортировки
    def parse_key(col):
        # Сначала выделим префикс из special_prefixes
        prefix = ''
        for sp in special_prefixes:
            if col.startswith(sp):
                prefix = sp
                break
        # Уберем префикс
        remainder = col[len(prefix):]
        
        # Проверим, что remainder заканчивается на _до или _ко
        for suf in suffixes:
            if remainder.endswith(suf):
                number_part = remainder[:-len(suf)]
                suffix_part = suf
                break
        else:
            # Если не подходит под шаблон, возвращаем None
            return None
        
        # Теперь number_part может быть число, число.число или число.буквы
        # Разберём число и буквенную часть
        # Например: 60, 8.0, 76.ФВ
        m = re.match(r"(\d+(?:\.\d+)?)(?:\.([А-Я]+))?$", number_part)
        if m:
            number = float(m.group(1))
            letters = m.group(2) or ''
        else:
            # Если не подошло, ставим очень большой ключ, чтобы в конец
            number = float('inf')
            letters = ''
        
        # Возвращаем кортеж для сортировки:
        # 1) индекс префикса в special_prefixes (чтобы сортировать по префиксу)
        # 2) число
        # 3) буквы
        # 4) суффикс _до раньше _ко (например)
        suf_order = {'_до': 0, '_ко': 1}
        
        return (special_prefixes.index(prefix), number, letters, suf_order.get(suffix_part, 10))
    
    # Собираем все колонки, которые подходят под этот шаблон
    special_cols = [col for col in cols if parse_key(col) is not None]
    # Сортируем их по ключу
    special_cols_sorted = sorted(special_cols, key=parse_key)
    
    # 3. Собираем итоговый порядок
    ordered_cols = fixed_cols[:]
    for prefix in COLUMN_PREFIXES:
        ordered_cols.extend(grouped_cols.get(prefix, []))
    ordered_cols.extend(special_cols_sorted)
    
    # 4. Добавляем остальные
    other_cols = set(cols) - set(ordered_cols)
    ordered_cols.extend(other_cols)
    
    return df[ordered_cols]


def write_df_in_chunks(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
    base_sheet_name: str,
    max_rows: int = MAX_EXCEL_ROWS
) -> None:
    """Записывает DataFrame в Excel частями с учетом ограничения на строки"""
    if df.empty:
        return
        
    n_chunks = math.ceil(len(df) / max_rows)
    
    for i in range(n_chunks):
        start = i * max_rows
        end = min((i + 1) * max_rows, len(df))
        sheet_name = f"{base_sheet_name}{i + 1}" if n_chunks > 1 else base_sheet_name
        
        df.iloc[start:end].to_excel(
            writer,
            sheet_name=sheet_name[:31],  # Ограничение длины имени листа
            index=False
        )

def validate_paths(paths: Iterable[Path]) -> List[Path]:
    """Проверяет валидность путей и возвращает список исправленных путей.
    Генерирует ошибку, если хоть один путь не существует и не может быть исправлен."""
    
    if not paths:
        raise IncorrectFolderOrFilesPath('Неверные пути к папке или файлу/файлам.')
    
    normalized_paths = []
    invalid_paths = []
    
    for path in paths:
        try:
            resolved_path = path.resolve()
            normalized = normalize_path(resolved_path)
            
            if normalized and normalized.exists():
                normalized_paths.append(normalized)
            else:
                # Запоминаем невалидный путь
                invalid_paths.append(str(resolved_path))
                normalized_paths.append(resolved_path)  # Все равно добавляем для возврата
                
        except (OSError, AttributeError) as e:
            # Запоминаем невалидный путь с информацией об ошибке
            invalid_paths.append(f"{path} (ошибка: {str(e)})")
            normalized_paths.append(path)  # Все равно добавляем для возврата
    
    # Если есть невалидные пути, генерируем ошибку
    if invalid_paths:
        error_message = 'Неверные пути к папке или файлу/файлам:\n' + '\n'.join(invalid_paths)
        raise IncorrectFolderOrFilesPath(error_message)
    
    return normalized_paths

def fix_1c_excel_case(file_path: Path) -> BytesIO:
    """Исправляет регистр имен в xlsx-архивах 1С"""
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            new_zip = BytesIO()
            
            with zipfile.ZipFile(new_zip, 'w') as new_z:
                for item in z.infolist():
                    # Исправляем только проблемные имена
                    new_name = (
                        'xl/sharedStrings.xml' 
                        if item.filename == 'xl/SharedStrings.xml' 
                        else item.filename
                    )
                    new_z.writestr(new_name, z.read(item))
        
        new_zip.seek(0)
        return new_zip
        
    except PermissionError as e:
        raise PermissionError(
            f"Файл {file_path.name} открыт в другой программе. Закройте его."
        ) from e
    except Exception as e:
        raise RuntimeError(
            f"Ошибка обработки файла {file_path.name}: {str(e)}"
        ) from e



def generate_dash_combinations(path_str: str) -> List[str]:
    """Генерирует все комбинации тире в строке."""
    # Находим индексы тире
    dash_indices = [i for i, char in enumerate(path_str) if char in ('-', '—')]
    
    if not dash_indices:
        return [path_str]  # Если нет тире, возвращаем оригинальную строку

    # Генерируем все комбинации замены
    combinations = []
    for combo in product(*[(path_str[i:i+1].replace('-', '—'), path_str[i:i+1].replace('—', '-')) 
                         if i in dash_indices else (path_str[i:i+1],) 
                         for i in range(len(path_str))]):
        combinations.append(''.join(combo))
    
    return combinations

def normalize_path(original_path: Path) -> Optional[Path]:
    """Находит валидный путь, перебирая комбинации тире."""
    # Проверяем изначальную валидность
    if original_path.exists():
        return original_path

    # Преобразуем объект Path в строку
    path_str = str(original_path)

    # Генерируем все комбинации тире
    for new_path_str in generate_dash_combinations(path_str):
        new_path = Path(new_path_str)

        # Проверяем валидность нового пути
        if new_path.exists():
            return new_path

    return None  # Если валидный путь не найден


def clear_console():
    """Очищает консоль"""
    os.system('cls' if os.name == 'nt' else 'clear')
    
def is_valid_dragged_path(path_str: str) -> bool:
    """
    Проверяет, является ли строка действительным путем к файлу или папке,
    полученным путем перетаскивания в терминал Windows.
    
    Args:
        path_str: Строка, полученная перетаскиванием файла/папки в терминал
    
    Returns:
        bool: True если путь существует (файл или папка), иначе False
    """
    if not path_str or not isinstance(path_str, str):
        return False
    
    # Убираем лишние пробелы по краям
    path_str = path_str.strip()
    
    # Обрабатываем возможные кавычки (Windows иногда добавляет их при путях с пробелами)
    if (path_str.startswith('"') and path_str.endswith('"')) or \
       (path_str.startswith("'") and path_str.endswith("'")):
        path_str = path_str[1:-1]
    
    # Если путь содержит пробелы, но не в кавычках, проверяем как есть
    # (пользователь мог добавить кавычки вручную)
    
    # Заменяем косые черты на прямые (иногда Windows использует обратные слеши)
    normalized_path = path_str.replace('/', '\\')
    
    # Проверяем, существует ли путь
    return os.path.exists(normalized_path)

def generate_failed_processing_markdown(error_dict):
    """
    Генерирует Markdown-текст с таблицей ошибок обработки файлов.
    
    Args:
        error_dict (dict): Словарь, где ключи - имена файлов, 
                          значения - причины неудачной обработки
    
    Returns:
        str: Текст в формате Markdown с заголовком и таблицей
    """
    if not error_dict:
        return ""
    
    # Заголовок
    markdown_text = "⚠️ Файлы не распознаны как регистры 1С или возникли ошибки:\n\n"
    
    # Заголовки таблицы
    markdown_text += "| Имя файла | Причина ошибки |\n"
    markdown_text += "|-----------|----------------|\n"
    
    # Заполняем таблицу данными из словаря
    for filename, error_reason in error_dict.items():
        # Экранируем символы, которые могут сломать Markdown таблицу
        safe_filename = str(filename).replace('|', '\\|')
        safe_error = str(error_reason).replace('|', '\\|')
        
        markdown_text += f"| {safe_filename} | {safe_error} |\n"
    
    return markdown_text

# КоррСчета, субсчета по которым не включаем в итоговые файлы, оставляем только счета
def get_accounts_without_subaccount() -> List[str]:
    config = read_config()
    accounts_without_subaccount = config.get("accounts_without_subaccount",
                                                          DEFAULT_CONFIG.get("accounts_without_subaccount", {}))
    return [i[0] for i in accounts_without_subaccount if i[2]==1]