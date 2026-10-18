"""Фабрика процессоров: определение варианта регистра по заголовкам выгрузки.

Разделение ``file_handler.py`` — Сессия S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось.
"""
from pathlib import Path

import pandas as pd

from flatregister.errors import RegisterNotRecognizedError
from flatregister.excel_io import fix_1c_excel_case
from flatregister.registers.account_osv import AccountOSV_NonUPPFileProcessor, AccountOSV_UPPFileProcessor
from flatregister.registers.analysis import Analisys_NonUPPFileProcessor, Analisys_UPPFileProcessor
from flatregister.registers.base import FileProcessor
from flatregister.registers.card import Card_NonUPPFileProcessor, Card_UPPFileProcessor
from flatregister.registers.general_osv import GeneralOSV_NonUPPFileProcessor, GeneralOSV_UPPFileProcessor
from flatregister.registers.posting import Posting_NonUPPFileProcessor, Posting_UPPFileProcessor
from flatregister.registers.turnover import Turnover_NonUPPFileProcessor, Turnover_UPPFileProcessor


class ExcelValidator:
    """Валидатор Excel файлов"""
    @staticmethod
    def is_valid_excel(file_path: Path) -> bool:
        return file_path.suffix.lower() == '.xlsx'


class FileProcessorFactory:
    """Фабрика для создания обработчиков файлов"""
    
    REGISTER_DISPLAY_NAMES = {
        'posting': 'Отчет по проводкам',
        'card': 'Карточка счета', 
        'analisys': 'Анализ счета',
        'turnover': 'Обороты счета',
        'accountosv': 'ОСВ счета',
        'generalosv': 'общая ОСВ'
    }
    
    REGISTER_PATTERNS = {
        'card': [
            {'pattern': {'дата', 'документ', 'операция'}, 'processor': Card_UPPFileProcessor},
            {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Card_NonUPPFileProcessor}
        ],
        'posting': [
            {'pattern': {'дата', 'документ', 'содержание', 'дт', 'кт', 'сумма'}, 'processor': Posting_UPPFileProcessor},
            {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Posting_NonUPPFileProcessor}
        ],
        'analisys': [
            {'pattern': {'счет', 'кор.счет', 'с кред. счетов', 'в дебет счетов'}, 'processor': Analisys_UPPFileProcessor},
            {'pattern': {'счет', 'кор. счет', 'дебет', 'кредит'}, 'processor': Analisys_NonUPPFileProcessor}
        ],
        'turnover': [
            {'pattern': {'субконто', 'нач. сальдо деб.', 'нач. сальдо кред.', 'деб. оборот', 'кред. оборот', 'кон. сальдо деб.', 'кон. сальдо кред.'}, 'processor': Turnover_UPPFileProcessor},
            {'pattern': {'счет', 'начальное сальдо дт', 'начальное сальдо кт', 'оборот дт', 'оборот кт', 'конечное сальдо дт', 'конечное сальдо кт'}, 'processor': Turnover_NonUPPFileProcessor}
        ],
        'accountosv': [
            {'pattern': {'субконто', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': AccountOSV_UPPFileProcessor},
            {'pattern': {'счет', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': AccountOSV_NonUPPFileProcessor}
        ],
        'generalosv': [
            {'pattern': {'счет', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_UPPFileProcessor},
            {'pattern': {'счет', 'наименование счета', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_NonUPPFileProcessor}
        ]
    }

    @classmethod
    def get_processor(cls, file_path: Path, type_register: str) -> FileProcessor:
        """Получить процессор для файла на основе шаблонов заголовков"""
        fixed_data = fix_1c_excel_case(file_path)
        
        # Чтение только первых строк для анализа
        df = pd.read_excel(fixed_data, header=None, nrows=20)
        str_df = df.map(lambda x: str(x).strip().lower() if pd.notna(x) else '')
        
        for pattern_config in cls.REGISTER_PATTERNS[type_register]:
            if cls._contains_pattern(str_df, pattern_config['pattern']):
                return pattern_config['processor']()
        
        raise RegisterNotRecognizedError(
            "Файл не является выбранным регистром из 1С.",
            hint="Проверьте, что выбран тип регистра, совпадающий с выгрузкой. "
                 "Карточка счёта и отчёт по проводкам из 1С УПП имеют другие "
                 "заголовки, чем одноимённые выгрузки из остальных конфигураций.",
        )

    @staticmethod
    def _contains_pattern(df: pd.DataFrame, pattern: set) -> bool:
        """Проверить, содержит ли DataFrame заданный паттерн"""
        for _, row in df.iterrows():
            if pattern.issubset(set(row)):
                return True
        return False
