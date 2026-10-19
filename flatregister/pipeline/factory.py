"""Фабрика процессоров: определение варианта регистра по заголовкам выгрузки.

Разделение ``file_handler.py`` — Сессия S5 плана рефакторинга (см. Tasks.md).
В S8 единственным источником заголовков стал :data:`flatregister.registry.REGISTER_SPEC`.
"""
from pathlib import Path
from typing import AbstractSet

import pandas as pd

from flatregister.errors import RegisterNotRecognizedError
from flatregister.excel_io import fix_1c_excel_case
from flatregister.registers.base import FileProcessor
from flatregister.registry import REGISTER_SPEC


class ExcelValidator:
    """Валидатор Excel файлов"""
    @staticmethod
    def is_valid_excel(file_path: Path) -> bool:
        return file_path.suffix.lower() == '.xlsx'


class FileProcessorFactory:
    """Фабрика для создания обработчиков файлов"""

    @classmethod
    def get_processor(cls, file_path: Path, type_register: str) -> FileProcessor:
        """Получить процессор для файла на основе шаблонов заголовков"""
        entry = REGISTER_SPEC.get(type_register)
        if entry is None:
            raise RegisterNotRecognizedError(
                "Файл не является выбранным регистром из 1С.",
                hint="Выбран неизвестный тип регистра. Перезапустите программу "
                     "и выберите регистр из списка.",
            )

        fixed_data = fix_1c_excel_case(file_path)

        # Чтение только первых строк для анализа
        df = pd.read_excel(fixed_data, header=None, nrows=20)
        str_df = df.map(lambda x: str(x).strip().lower() if pd.notna(x) else '')

        for pattern_config in entry.patterns:
            if cls._contains_pattern(str_df, pattern_config.headers):
                return pattern_config.processor()

        raise RegisterNotRecognizedError(
            "Файл не является выбранным регистром из 1С.",
            hint="Проверьте, что выбран тип регистра, совпадающий с выгрузкой. "
                 "Карточка счёта и отчёт по проводкам из 1С УПП имеют другие "
                 "заголовки, чем одноимённые выгрузки из остальных конфигураций.",
        )

    @staticmethod
    def _contains_pattern(df: pd.DataFrame, pattern: AbstractSet[str]) -> bool:
        """Проверить, содержит ли DataFrame заданный паттерн"""
        for _, row in df.iterrows():
            if pattern.issubset(set(row)):
                return True
        return False
