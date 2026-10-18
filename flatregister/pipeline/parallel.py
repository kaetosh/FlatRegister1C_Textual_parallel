"""Обработка одного файла в отдельном процессе (ProcessPoolExecutor).

Разделение ``file_handler.py`` — Сессия S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось.
"""
from pathlib import Path

from flatregister.errors import FileFailure, NotSpreadsheetError, describe_exception
from flatregister.pipeline.factory import ExcelValidator, FileProcessorFactory


def _process_file_parallel(file_path: Path, type_register: str) -> tuple:
    """Функция для выполнения в отдельном процессе.

    Бросать исключения отсюда нельзя: они не переживут границу процессов.
    Вместо этого наружу отдаётся :class:`FileFailure` — он picklable и несёт
    готовый текст для пользователя вместе с полным трейсбеком для журнала.

    Журналирование здесь намеренно **нет**: несколько процессов писали бы в
    один rotating-файл и мешали друг другу при ротации. Трейсбек приезжает
    в родительский процесс строкой и дописывается им (см.
    :meth:`FileHandler._record_failure`).
    """
    try:
        if not ExcelValidator.is_valid_excel(file_path):
            return file_path.name, None, None, None, FileFailure(
                file_name=file_path.name,
                user_message=NotSpreadsheetError.default_user_message,
                hint='Обрабатываются только файлы .xlsx.',
            )
        processor = FileProcessorFactory.get_processor(file_path, type_register)
        result, check = processor.process_file(file_path)
        return file_path.name, processor.__class__, result, check, None
    except Exception as error:
        return (file_path.name, None, None, None,
                describe_exception(error, file_name=file_path.name))
