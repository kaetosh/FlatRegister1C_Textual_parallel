"""Чтение и запись файлов Excel: 1С-выгрузки, отчёты, пути.

Модуль вынесен из ``support_functions.py`` (и ``FileHandler._open_file``)
в Сессии S5 плана рефакторинга (см. Tasks.md). Поведение не менялось.
"""
import math
import os
import subprocess
import sys
import zipfile
from io import BytesIO
from itertools import product
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd

from flatregister.errors import FileFailure, InputPathError, SheetUnreadableError
from flatregister.logging_setup import get_logger

_log = get_logger("excel_io")

# Глобальные константы
MAX_EXCEL_ROWS = 1_000_000


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
        raise InputPathError(
            'Неверные пути к папке или файлу/файлам.',
            hint='Укажите существующий файл .xlsx или папку с такими файлами.')
    
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
        raise InputPathError(
            error_message,
            hint='Перетащите файл или папку в окно программы. Если в пути есть '
                 'тире, попробуйте вставить путь с дефисом вместо длинного тире.')
    
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
        # Самый частый случай на Windows: файл открыт в Excel.
        raise SheetUnreadableError(
            f"Файл {file_path.name} открыт в другой программе. Закройте его.",
            hint="Закройте книгу в Excel и повторите обработку.",
        ) from e
    except (zipfile.BadZipFile, OSError, ValueError, KeyError) as e:
        raise SheetUnreadableError(
            f"Файл {file_path.name} не читается как таблица Excel.",
            hint="Файл повреждён или это не .xlsx. Откройте его в Excel и "
                 "пересохраните как Книга Excel (*.xlsx).",
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

def generate_failed_processing_markdown(failures: Dict[str, FileFailure]) -> str:
    """Генерирует Markdown-текст с таблицей ошибок обработки файлов.

    Args:
        failures: словарь ``имя файла -> FileFailure``. Строковые значения
            принимаются на всякий случай и показываются как есть — так отчёт
            не рассыпается, если откуда-то пришла строка вместо FileFailure.

    Returns:
        Текст в формате Markdown. Технические подробности (трейсбеки) сюда
        намеренно не попадают: они живут в журнале logs/app.log.
    """
    if not failures:
        return ""

    def cell(value) -> str:
        # Вертикальная черта и перенос строки ломают таблицу Markdown.
        text = str(value or "").replace("|", "\\|").replace("\n", " ").strip()
        return text or "—"

    lines = [
        "## Не обработаны",
        "",
        "Остальные файлы обработаны и попали в итоговый файл.",
        "",
        "| Файл | Причина | Что можно сделать |",
        "|---|---|---|",
    ]
    for file_name, failure in failures.items():
        reason = getattr(failure, "user_message", failure)
        hint = getattr(failure, "hint", None)
        lines.append(f"| {cell(file_name)} | {cell(reason)} | {cell(hint)} |")

    return "\n".join(lines) + "\n"


def open_file(file_path: str) -> None:
    """Открыть файл в ассоциированном приложении.

    Файл уже записан к этому моменту, поэтому неудача открытия — не
    провал обработки: пользователю показываем предупреждение, а результат
    всё равно лежит на диске.
    """
    try:
        if sys.platform == "win32":
            os.startfile(file_path)
        elif sys.platform == "darwin":
            subprocess.run(["open", file_path], check=False)
        else:
            subprocess.run(["xdg-open", file_path], check=False)
    except OSError as error:
        _log.warning("Итоговый файл создан, но не открылся автоматически: %s (%s)",
                     file_path, error)
