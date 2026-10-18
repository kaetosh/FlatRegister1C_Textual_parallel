# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

from abc import ABC, abstractmethod
from pathlib import Path

import numpy as np
import pandas as pd

from flatregister.dtypes import ensure_column_types, to_numeric
from flatregister.errors import RegisterEmptyError, RegisterNotRecognizedError, RegisterStructureError
from register_processors.class_processor import FileProcessor
from support_functions import fix_1c_excel_case, get_keep_subaccounts


def pivot_hierarchy_to_columns(df, separator='_'):
    """
    Разносит вертикальные иерархические данные в горизонтальные столбцы

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame с колонками 'Уровень', 'Счет', 'Наименование'
    separator : str, default='_'
        Разделитель между счетом и наименованием

    Returns
    -------
    pandas.DataFrame
        DataFrame с добавленными колонками Level_0, Level_1, ...
    """
    max_level = df['Уровень'].max()
    for level in range(max_level + 1):
        level_mask = df['Уровень'] == level
        combined = df['Счет'].astype(str) + separator + df['Наименование'].astype(str)
        df[f'Level_{level}'] = combined.where(level_mask)
        df[f'Level_{level}'] = df[f'Level_{level}'].ffill()
        higher_level_mask = df['Уровень'] < level
        df.loc[higher_level_mask, f'Level_{level}'] = None
    return df


def get_col_sum_or_zero(df, col):
    """Возвращает сумму столбца или 0, если столбца нет"""
    if col in df.columns:
        numeric_series = to_numeric(df[col], fill_zero=True)
        return numeric_series.sum().round()
    return 0


def keep_leaf_rows_vectorized(df):
    """
    Удаляет родительские строки, оставляя только те, которые не имеют дочерних.
    """
    df = df.copy()
    df['next_level'] = df['Уровень'].shift(-1)
    mask_to_keep = ~((df['next_level'] > df['Уровень']) & df['next_level'].notna())
    result = df[mask_to_keep].drop('next_level', axis=1).reset_index(drop=True)
    return result


class BaseOSVFileProcessor(FileProcessor, ABC):
    """Базовый класс для обработчиков ОСВ 1С."""

    def __init__(self):
        super().__init__()
        self.df_type_connection = pd.DataFrame()
        self.file = None

    @abstractmethod
    def _process_dataframe_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Поиск шапки таблицы, переименование заголовков, очистка.
        Должен быть реализован в наследниках.
        """
        pass

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Общие шаги подготовки после _process_dataframe_optimized."""
        # Добавляем ведущий ноль для однозначных счетов
        mask = df['Счет'].astype(str).str.len() == 1
        df['Счет'] = np.where(mask, '0' + df['Счет'].astype(str), df['Счет'])
        df['Счет'] = df['Счет'].fillna('').astype(str)

        # Очистка наименований
        df['Наименование'] = (
            df['Наименование']
            .fillna('')
            .astype(str)
            .str.strip()
            .replace('', np.nan)
        )
        df.dropna(subset=['Наименование'], inplace=True)

        # Добавляем имя исходного файла
        df['Исх.файл'] = self.file
        return df

    def process_file(self, file_path: Path):
        """Основной метод обработки файла."""
        self.file = file_path.name

        # Исправляем возможный регистр в выгрузке 1С
        fixed_data = fix_1c_excel_case(file_path)

        # Предобработка (добавление столбцов Уровень и Курсив)
        df = self._preprocessor_openpyxl(fixed_data)
        del fixed_data

        # Установка заголовков и очистка (специфично для формата)
        df = self._process_dataframe_optimized(df)

        # Общая пост-обработка
        df = self._prepare_dataframe(df)

        # Проверка настройки – оставляем только субсчета или все
        keep_subaccounts = get_keep_subaccounts()
        self.table_for_check = pd.DataFrame()  # по умолчанию пустая

        if keep_subaccounts:
            df = pivot_hierarchy_to_columns(df)
            df = keep_leaf_rows_vectorized(df)
            df.drop(columns=['Уровень'], errors='ignore', inplace=True)

            # Создаём проверочную таблицу со свёрнутыми остатками/оборотами
            self.table_for_check = pd.DataFrame({
                'Сальдо_начало_свернуто': [
                    get_col_sum_or_zero(df, 'Дебет_начало') - get_col_sum_or_zero(df, 'Кредит_начало')
                ],
                'Оборот_свернуто': [
                    get_col_sum_or_zero(df, 'Дебет_оборот') - get_col_sum_or_zero(df, 'Кредит_оборот')
                ],
                'Сальдо_конец_свернуто': [
                    get_col_sum_or_zero(df, 'Дебет_конец') - get_col_sum_or_zero(df, 'Кредит_конец')
                ],
                'Исх.файл': [self.file]
            })
        
        df.drop(columns=['Уровень'], errors='ignore', inplace=True)
        df = ensure_column_types(df)
        return df, self.table_for_check


class GeneralOSV_UPPFileProcessor(BaseOSVFileProcessor):
    """Обработчик для ОСВ из 1С УПП (старый формат)."""

    def _process_dataframe_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        MAX_HEADER_ROWS = 30

        df = df.replace('', np.nan)
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)

        max_rows_to_check = min(MAX_HEADER_ROWS, len(df))

        # Поиск столбца со словом "счет"
        account_col_idx = None
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if 'счет' in col_values.values:
                account_col_idx = col_idx
                break
        if account_col_idx is None:
            raise RegisterStructureError(
                'Не найден столбец с "Счет" в первых 30 строках.',
                hint='Похоже, это не ОСВ. Проверьте, что выбран тот тип регистра, '
                     'который соответствует выгрузке.')

        first_col = df.iloc[:, account_col_idx].astype(str)
        mask = first_col == 'Счет'
        if not mask.any():
            raise RegisterNotRecognizedError(
                'Файл не является ОСВ 1с.',
                hint='В выгрузке нет строки заголовков со словом «Счет».')

        date_row_idx = mask.idxmax()
        df.columns = df.iloc[date_row_idx]
        df = df.iloc[date_row_idx + 1:].copy()

        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()

        # Поиск столбца "Наименование"
        first_row = df.iloc[0]
        mask_name = first_row == 'Наименование'
        cols_name_acc = np.where(mask_name)[0]
        if cols_name_acc.size == 0:
            raise RegisterStructureError(
                'ОСВ выгружена без наименований счёта.',
                hint='В выгрузке нет столбца «Наименование». Выгрузите ОСВ заново '
                     'через меню «Сохранить как» в 1С.')
        col_index_name_acc = cols_name_acc[0]

        required_cols = [
            'Сальдо на начало периода',
            'Оборот за период',
            'Сальдо на конец периода'
        ]
        for col in required_cols:
            if col not in df.columns:
                raise RegisterStructureError(
                    f'Отсутствует обязательный столбец: {col}',
                    hint='Столбец нужен для разноса оборотов по дебету и кредиту. '
                         'Проверьте, что в 1С выгружена ОСВ с оборотом за период.')

        target_idx_a = df.columns.get_loc('Сальдо на начало периода')
        target_idx_b = df.columns.get_loc('Оборот за период')
        target_idx_c = df.columns.get_loc('Сальдо на конец периода')

        new_cols = list(df.columns)
        new_cols[col_index_name_acc] = 'Наименование'

        def find_column_index(df, start_idx, word):
            # 1. Берем срез первой строки от нужной позиции
            row_slice = df.iloc[0, start_idx + 1:]
            
            # 2. Метод eq() безопасно сравнит все значения. 
            # Если встретится pd.NA, он просто вернет False, без ошибок.
            mask = row_slice.eq(word)
            
            # 3. Если совпадение нашлось
            if mask.any():
                # mask.argmax() вернет позиционный индекс первого True внутри среза.
                # Нам нужно прибавить сдвиг (start_idx + 1), чтобы получить абсолютный индекс в df
                return (start_idx + 1) + mask.argmax()
                
            return None        
        new_cols[target_idx_a] = 'Дебет_начало'
        cred_start_idx = find_column_index(df, target_idx_a, 'Кредит')
        # cred_start_idx = find_column_index(cols, df, target_idx_a, 'Кредит')
        if cred_start_idx is not None:
            new_cols[cred_start_idx] = 'Кредит_начало'
        
        
        
        new_cols[target_idx_b] = 'Дебет_оборот'
        cred_turn_idx = find_column_index(df, target_idx_b, 'Кредит')
        # cred_turn_idx = find_column_index(cols, df, target_idx_b, 'Кредит')
        
        
        
        if cred_turn_idx is not None:
            new_cols[cred_turn_idx] = 'Кредит_оборот'
        
        
        
        new_cols[target_idx_c] = 'Дебет_конец'
        cred_end_idx = find_column_index(df, target_idx_c, 'Кредит')
        # cred_end_idx = find_column_index(cols, df, target_idx_c, 'Кредит')
        if cred_end_idx is not None:
            new_cols[cred_end_idx] = 'Кредит_конец'

        df.columns = new_cols

        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)
        df = df.iloc[1:]  # удаляем строку "Дебет Кредит ..."

        if df['Уровень'].max() == 0:
            raise RegisterEmptyError(
                'ОСВ пустая.',
                hint='В выгрузке нет ни одной строки с данными.')

        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterStructureError(
                'Найдены пустые значения в столбцах Уровень или Курсив.',
                hint='В Excel не выставлены уровни группировки (знаки «+» и «−» слева). '
                     'Выгрузите регистр заново через меню «Сохранить как» в 1С.')

        df.drop(columns=['Курсив'], errors='ignore', inplace=True)
        return df


class GeneralOSV_NonUPPFileProcessor(BaseOSVFileProcessor):
    """Обработчик для ОСВ из 1С (стандартный формат)."""

    def _process_dataframe_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.replace('', np.nan)
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)

        # Поиск столбца "Счет"
        account_col_idx = None
        max_rows_to_check = min(30, df.shape[0])
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if 'счет' in col_values.values:
                account_col_idx = col_idx
                break
        if account_col_idx is None:
            raise RegisterStructureError(
                'Не найден столбец с "Счет" в первых 30 строках.',
                hint='Похоже, это не ОСВ. Проверьте, что выбран тот тип регистра, '
                     'который соответствует выгрузке.')

        first_col = df.iloc[:, account_col_idx].astype(str)
        mask = first_col == 'Счет'
        if not mask.any():
            raise RegisterNotRecognizedError(
                'Файл не является ОСВ 1с.',
                hint='В выгрузке нет строки заголовков со словом «Счет».')

        date_row_idx = mask.idxmax()
        df.columns = df.iloc[date_row_idx]
        df = df.iloc[date_row_idx + 1:].copy()

        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()

        cols = df.columns.tolist()
        target_idx_0 = cols.index('Наименование счета')
        target_idx_a = cols.index('Сальдо на начало периода')
        target_idx_b = cols.index('Обороты за период')
        target_idx_c = cols.index('Сальдо на конец периода')

        new_cols = cols.copy()
        new_cols[target_idx_0] = 'Наименование'
        new_cols[target_idx_a] = 'Дебет_начало'
        new_cols[target_idx_a + 1] = 'Кредит_начало'
        new_cols[target_idx_b] = 'Дебет_оборот'
        new_cols[target_idx_b + 1] = 'Кредит_оборот'
        new_cols[target_idx_c] = 'Дебет_конец'
        new_cols[target_idx_c + 1] = 'Кредит_конец'

        df.columns = new_cols
        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)
        df = df.iloc[1:]  # удаляем строку с "Дебет Кредит ..."

        if df['Уровень'].max() == 0:
            raise RegisterEmptyError(
                'ОСВ пустая.',
                hint='В выгрузке нет ни одной строки с данными.')

        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterStructureError(
                'Найдены пустые значения в столбцах Уровень или Курсив.',
                hint='В Excel не выставлены уровни группировки (знаки «+» и «−» слева). '
                     'Выгрузите регистр заново через меню «Сохранить как» в 1С.')

        df.drop(columns=['Курсив'], errors='ignore', inplace=True)
        return df
