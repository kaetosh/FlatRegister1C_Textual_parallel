# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import numpy as np
import pandas as pd

from flatregister.dtypes import MONEY_COLUMNS, ensure_column_types, numeric_total
from flatregister.errors import RegisterEmptyError, RegisterNotRecognizedError, RegisterStructureError
from flatregister.excel_io import fix_1c_excel_case
from flatregister.hierarchy import (
    drop_interim_total_rows,
    find_word_column_and_row,
    get_col_or_zeros,
    spread_levels,
)
from flatregister.registers.base import FileProcessor, exclude_values
from flatregister.registers.spec import RegisterSpec


def find_account_from_text(df: pd.DataFrame, search_text: str = "Оборотно-сальдовая ведомость по счету ") -> Optional[str]:
    """
    Ищет в DataFrame ячейку с search_text и извлекает номер счета.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame для поиска
    search_text : str
        Текст для поиска
    
    Returns:
    --------
    str or None: Номер счета или None если не найден
    """
    for col in df.columns:
        for idx, value in df[col].items():
            if isinstance(value, str) and search_text in value:
                # Вариант 1: через split
                try:
                    after_text = value.split(search_text)[1]
                    account = after_text.split()[0]
                    return account
                except (IndexError, AttributeError):
                    # Вариант 2: через regex если split не сработал
                    pattern = rf"{re.escape(search_text)}(\S+)"
                    match = re.search(pattern, value)
                    if match:
                        return match.group(1)
    return None


def calculate_check_values(df: pd.DataFrame, columns_config: dict) -> pd.DataFrame:
    """Расчет контрольных значений для проверки оборотов"""
    df_check = df.copy()
    
    df_check['Сальдо_начало_до_обработки'] = (
        get_col_or_zeros(df_check, columns_config['start_debit']) - 
        get_col_or_zeros(df_check, columns_config['start_credit'])
    )
    df_check['Сальдо_конец_до_обработки'] = (
        get_col_or_zeros(df_check, columns_config['end_debit']) - 
        get_col_or_zeros(df_check, columns_config['end_credit'])
    )
    df_check['Оборот_до_обработки'] = (
        get_col_or_zeros(df_check, columns_config['debit_turnover']) - 
        get_col_or_zeros(df_check, columns_config['credit_turnover'])
    )
    
    return df_check[['Сальдо_начало_до_обработки', 'Сальдо_конец_до_обработки', 'Оборот_до_обработки']].reset_index()


def create_check_after_process(df: pd.DataFrame, columns_config: dict) -> pd.DataFrame:
    """Создание таблицы с данными после обработки"""
    return pd.DataFrame({
        'Сальдо_начало_после_обработки': [
            get_col_or_zeros(df, columns_config['start_debit']).sum() - 
            get_col_or_zeros(df, columns_config['start_credit']).sum()
        ],
        'Оборот_после_обработки': [
            get_col_or_zeros(df, columns_config['debit_turnover']).sum() - 
            get_col_or_zeros(df, columns_config['credit_turnover']).sum()
        ],
        'Сальдо_конец_после_обработки': [
            get_col_or_zeros(df, columns_config['end_debit']).sum() - 
            get_col_or_zeros(df, columns_config['end_credit']).sum()
        ]
    })


@dataclass(frozen=True)
class AccountOSVSpec(RegisterSpec):
    """Различия между ОСВ счёта УПП и не-УПП.

    ``currency_shift`` — это D2 из «Открытых решений»: в УПП валютная сумма
    лежит через две строки (код валюты + количество + сумма), в не-УПП — через
    одну. Значения сохранены как есть; названы явно, чтобы различие не
    выглядело случайным.
    """

    quantity_shift: int
    currency_shift: int


UPP_SPEC = AccountOSVSpec(
    header_word='субконто',
    header_exact='Субконто',
    account_column='Субконто',
    indicator_column='Показа-\nтели',
    structure_message='Не найден столбец с "Субконто" в первых 30 строках.',
    recognized_hint='В выгрузке нет строки заголовков со словом «Субконто».',
    empty_message='ОСВ счета вероятно пустая.',
    quantity_shift=-1,
    currency_shift=-2,
)

NON_UPP_SPEC = AccountOSVSpec(
    header_word='счет',
    header_exact='Счет',
    account_column='Счет',
    indicator_column='Показа-\nтели',
    structure_message='Не найден столбец с "Счет" в первых 30 строках.',
    recognized_hint='В выгрузке нет строки заголовков со словом «Счет».',
    empty_message='ОСВ счета вероятно пустая.',
    quantity_shift=-1,
    currency_shift=-1,
)


class BaseAccountOSVProcessor(FileProcessor):
    """Базовый обработчик для Анализа счета 1С"""

    spec: AccountOSVSpec

    def __init__(self):
        super().__init__()
        self.df_type_connection = pd.DataFrame()
        self._columns_config = {
            'start_debit': 'Дебет_начало',
            'start_credit': 'Кредит_начало',
            'end_debit': 'Дебет_конец',
            'end_credit': 'Кредит_конец',
            'debit_turnover': 'Дебет_оборот',
            'credit_turnover': 'Кредит_оборот'
        }
    
    @staticmethod
    def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Очистка DataFrame от пустых строк и столбцов"""
        df = df.replace('', np.nan)
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        return df
    
    def _find_header_column(self, df: pd.DataFrame, search_value: str, max_rows: int = 30) -> Tuple[Optional[int], Optional[int]]:
        """
        Поиск столбца с заголовком и строки с искомым значением

        Returns:
            (col_idx, row_idx) или (None, None) если не найдено
        """
        return find_word_column_and_row(df, search_value, max_rows)
    
    def _process_header(self, df: pd.DataFrame, header_row_idx: int, rename_columns: bool = False) -> pd.DataFrame:
        """Установка заголовков и очистка данных"""
        df.columns = df.iloc[header_row_idx]
        df = df.iloc[header_row_idx + 1:].copy()
        
        if rename_columns:
            df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
        
        return df
    
    def _rename_balance_columns(self, df: pd.DataFrame, cols: list, target_indices: list) -> pd.DataFrame:
        """Переименование столбцов сальдо и оборотов"""
        new_cols = cols.copy()
        
        for target_idx, new_names in target_indices:
            for i, new_name in enumerate(new_names):
                new_cols[target_idx + i] = new_name
        
        df.columns = new_cols
        return df
    
    def _clean_after_header(self, df: pd.DataFrame) -> pd.DataFrame:
        """Очистка после установки заголовков"""
        # удалим столбцы с пустыми заголовками
        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)
        
        # удалим строку с остатками от шапки
        df = df.iloc[1:]
        return df
    
    def _validate_empty_osv(self, df: pd.DataFrame) -> None:
        """Проверка на пустую ОСВ"""
        if df['Уровень'].max() == 0:
            if numeric_total(df, MONEY_COLUMNS) == 0:
                raise RegisterEmptyError(
                    'ОСВ счета вероятно пустая.',
                    hint='В выгрузке нет ни одной строки с данными.')
            else:
                df.loc[:, 'Уровень'] = 1
    
    def _validate_level_columns(self, df: pd.DataFrame) -> None:
        """Проверка наличия пустых значений в столбцах Уровень и Курсив"""
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterStructureError(
                'Найдены пустые значения в столбцах Уровень или Курсив.',
                hint='В Excel не выставлены уровни группировки (знаки «+» и «−» слева). '
                     'Выгрузите регистр заново через меню «Сохранить как» в 1С.')
    
    def _process_missing_values(self, df: pd.DataFrame, account_col: str) -> pd.DataFrame:
        """Обработка пропущенных значений"""
        df = df.copy()
        if 'Показа-\nтели' in df.columns:
            mask = df['Показа-\nтели'].str.contains('Кол.|Вал.', na=False)
            df.loc[~mask, account_col] = df.loc[~mask, account_col].fillna('Не_заполнено')
            df[account_col] = df[account_col].ffill()
        else:
            # Проставляем значение "Количество"
            df[account_col] = np.where(
                df[account_col].isna() & df['Уровень'].eq(df['Уровень'].shift(1)),
                'Количество',
                df[account_col]
            )
            
            # Удаляем строки с "Количество" ниже строки с Итого
            if (df[account_col] == 'Итого').any():
                index_total = df[df[account_col] == 'Итого'].index[0]
                df = df[(df.index <= index_total) | ((df.index > index_total) & (df[account_col] != 'Количество'))]
            
            df.loc[:, account_col] = df[account_col].fillna('Не_заполнено')
        
        # Добавление ведущего нуля для счетов до 10
        mask = (df[account_col].str.len() == 1) & self._is_accounting_code_vectorized(df[account_col])
        df.loc[mask, account_col] = '0' + df.loc[mask, account_col]
        df.loc[:, account_col] = df[account_col].astype(str)
        
        return df
    
    def _spread_vertical_data(self, df: pd.DataFrame, account_col: str) -> Tuple[pd.DataFrame, int]:
        """Разнос вертикальных данных в горизонтальные"""
        max_level = df['Уровень'].max()
        
        # Обработка специального случая для "Количество"
        quantity_mask = df[account_col] == 'Количество'
        
        if quantity_mask.any():
            last_level_values = pd.Series(index=df[quantity_mask].index, dtype=object)
            
            for idx in df[quantity_mask].index:
                for level in range(max_level, -1, -1):
                    level_col = f'Level_{level}'
                    if level_col in df.columns and pd.notna(df.at[idx, level_col]):
                        last_level_values[idx] = df.at[idx, level_col]
                        break
            
            df.loc[quantity_mask, account_col] = last_level_values
        
        # Создаем Level колонки
        spread_levels(df, df[account_col])
        
        df.loc[df[quantity_mask].index, account_col] = 'Количество'
        
        return df, max_level
    
    def _handle_quantity_currency_data(self, df: pd.DataFrame, desired_order: list) -> pd.DataFrame:
        """Обработка количественных и валютных данных"""
        indicator = self.spec.indicator_column
        if indicator in df.columns:
            # Обработка количественных данных
            if df[indicator].str.startswith('Кол.').any():
                for col in desired_order:
                    df[f'Количество_{col}'] = df[col].shift(self.spec.quantity_shift)

            # Обработка валютных данных
            if df[indicator].str.startswith('Вал.').any():
                for col in desired_order:
                    df[f'ВалютнаяСумма_{col}'] = df[col].shift(self.spec.currency_shift)

            # Очистка от строк с количеством и валютой
            mask = (
                (df[indicator] == 'Кол.') |
                (df[indicator] == 'Вал.') |
                (df[indicator].str.startswith('Валюта'))
            )
            df = df[~mask]

        return df
    
    def _remove_duplicate_rows(self, df: pd.DataFrame, account_col: str, max_level: int) -> pd.DataFrame:
        """Удаление дублирующихся строк"""
        return drop_interim_total_rows(df, account_col, max_level)
    
    def _create_check_tables(self, df: pd.DataFrame, df_for_check: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """Создание и возврат таблицы для проверки"""
        # Вычисление итоговых значений после обработки
        df_check_after_process = create_check_after_process(df, self._columns_config)
        
        # Объединение таблиц
        pivot_df_check = pd.concat([df_for_check, df_check_after_process], axis=1).fillna(0)
        
        # Вычисление отклонений
        for field in ['Сальдо_начало_разница', 'Оборот_разница', 'Сальдо_конец_разница']:
            pivot_df_check[field] = (
                pivot_df_check[field.replace('_разница', '_до_обработки')] -
                pivot_df_check[field.replace('_разница', '_после_обработки')]
            ).round()
        
        pivot_df_check['Исх.файл'] = file_name
        return pivot_df_check
    
    def _get_desired_columns(self, df: pd.DataFrame) -> Tuple[list, list]:
        """Получение списков необходимых столбцов"""
        desired_order_not_with_suff = [
            col for col in ['Дебет_начало', 'Кредит_начало', 'Дебет_оборот',
                           'Кредит_оборот', 'Дебет_конец', 'Кредит_конец']
            if col in df.columns
        ]
        desired_order = desired_order_not_with_suff.copy()
        
        do_ko_columns = df.filter(regex='(_до|_ко)$').columns.tolist()
        if do_ko_columns:
            desired_order += do_ko_columns
        
        return desired_order_not_with_suff, desired_order


class AccountOSV_UPPFileProcessor(BaseAccountOSVProcessor):
    """Обработчик для Анализа счета 1С УПП"""

    spec = UPP_SPEC

    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        """Поиск шапки таблицы, переименование заголовков, очистка"""
        df = BaseAccountOSVProcessor._clean_dataframe(df)
        
        processor = AccountOSV_UPPFileProcessor()
        spec = processor.spec
        col_idx, header_row_idx = processor._find_header_column(df, spec.header_word)

        if col_idx is None or header_row_idx is None:
            raise RegisterStructureError(
                spec.structure_message,
                hint='Похоже, это не ОСВ счёта. Проверьте, что выбран тот тип '
                     'регистра, который соответствует выгрузке.')

        # Проверка на наличие значения "Субконто"
        first_col = df.iloc[:, col_idx].astype(str)
        if not (first_col == spec.header_exact).any():
            raise RegisterNotRecognizedError(
                'Файл не является ОСВ счета 1с.',
                hint=spec.recognized_hint)
        
        df = processor._process_header(df, header_row_idx, rename_columns=True)
        
        # Переименование столбцов
        cols = df.columns.tolist()
        target_idx_a = cols.index('Сальдо на начало периода')
        target_idx_b = cols.index('Оборот за период')
        target_idx_c = cols.index('Сальдо на конец периода')
        
        def find_credit_index(cols, df, start_idx, word):
            for idx in range(start_idx + 1, len(cols)):
                if df.iloc[0, idx] == word:
                    return idx
            return None
        
        target_indices = [
            (target_idx_a, ['Дебет_начало']),
            (find_credit_index(cols, df, target_idx_a, 'Кредит'), ['Кредит_начало']),
            (target_idx_b, ['Дебет_оборот']),
            (find_credit_index(cols, df, target_idx_b, 'Кредит'), ['Кредит_оборот']),
            (target_idx_c, ['Дебет_конец']),
            (find_credit_index(cols, df, target_idx_c, 'Кредит'), ['Кредит_конец'])
        ]
        
        df = processor._rename_balance_columns(df, cols, [(idx, names) for idx, names in target_indices if idx is not None])
        df = processor._clean_after_header(df)
        processor._validate_empty_osv(df)
        processor._validate_level_columns(df)
        
        return df
    
    def process_file(self, file_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Основная обработка таблицы"""
        self.file = file_path.name
        
        fixed_data = fix_1c_excel_case(file_path)
        df = self._preprocessor_openpyxl(fixed_data)
        del fixed_data
        
        account_for_table = find_account_from_text(df)
        df = self._process_dataframe_optimized(df)
        df['Исх.файл'] = self.file
        
        # Обработка пропущенных значений
        df = self._process_missing_values(df, 'Субконто')
        
        
        
        # Разнос вертикальных данных
        df, max_level = self._spread_vertical_data(df, 'Субконто')
        
        

        # Получение списков столбцов
        desired_order_not_with_suff, desired_order = self._get_desired_columns(df)

        # Обработка количественных и валютных данных
        if 'Показа-\nтели' in df.columns or df['Субконто'].isin(['Количество', 'Валютная сумма']).any():
            # Создание столбцов с количеством
            if df['Субконто'].isin(['Количество']).any() or 'Показа-\nтели' in df.columns:
                for col in desired_order:
                    df[f'Количество_{col}'] = df[col].shift(self.spec.quantity_shift)

            # Создание столбцов с валютой
            if df['Субконто'].isin(['Валютная сумма']).any() or 'Показа-\nтели' in df.columns:
                if df['Субконто'].str.startswith('Валюта').any():
                    df['Валюта'] = df['Субконто'].shift(-1)
                for col in desired_order:
                    df[f'ВалютнаяСумма_{col}'] = df[col].shift(self.spec.currency_shift)
            
            # Очистка от строк с количеством и валютой
            mask = (
                (df['Субконто'] == 'Количество') |
                (df['Субконто'] == 'Валютная сумма') |
                (df['Субконто'].str.startswith('Валюта'))
            )
            df = df[~mask]
        

        
        # Создание контрольной таблицы
        if df[df['Субконто'] == 'Итого'][desired_order].empty:
            raise RegisterStructureError(
                'В выгрузке нет значений по строке «Итого».',
                hint='Сверка оборотов до и после обработки считается по строке «Итого». '
                     'Без неё достоверно проверить результат невозможно.')
        
        df_for_check = df[df['Субконто'] == 'Итого'][['Субконто'] + desired_order_not_with_suff].copy().tail(2).iloc[[0]]
        df_for_check[desired_order_not_with_suff] = df_for_check[desired_order_not_with_suff].astype(float).fillna(0)
        df_for_check = calculate_check_values(df_for_check, self._columns_config)

        # Удаление дублирующихся строк
        df = self._remove_duplicate_rows(df, 'Субконто', max_level)
        
        

        
        
        
        # Удаление строк без числовых данных (например, "Ответственный:" под таблицей).
        # Это необходимо для корректной обработки одно-строчных выгрузок (только "Итого"):
        # без этого хвост выгрузки даёт len(df) > 1, и ветка ниже выбрасывает
        # 'Итого' как промежуточный итог, хотя он единственный несёт суммы.
        numeric_cols_for_filter = [col for col in MONEY_COLUMNS if col in df.columns]
        if numeric_cols_for_filter:
            df = df[df[numeric_cols_for_filter].notna().any(axis=1)].copy()
        
        # Удаление строк с Итого и exclude_values
        if len(df) > 1:
            df = df[~df['Субконто'].str.contains('Итого')]
            df = df[~df['Субконто'].isin(exclude_values)]
            

            
            if account_for_table:
                level_columns = [col for col in df.columns if col.startswith('Level_')]
                for col in level_columns:
                    if self._is_accounting_code_vectorized(df[col]).any():
                        break
                    else:
                        condition = (df[col] == 'Итого') | (df[col].isna())
                        df[col] = df[col].mask(condition, account_for_table)
                    

        else:
            # Субсчетов нет: 'Итого' здесь не дублирующий итог, а сами данные счёта.
            first_index = df.index[0]
            if df.loc[first_index, 'Субконто'] == 'Итого' and account_for_table:
                df.loc[first_index, 'Субконто'] = account_for_table
                level_columns = [col for col in df.columns if col.startswith('Level_')]
                for col in level_columns:
                    condition = (df[col] == 'Итого') | (df[col].isna())
                    df[col] = df[col].mask(condition, account_for_table)
        
        

        
        
        
        
        df = df.rename(columns={'Счет': 'Субконто'})
        df.drop('Уровень', axis=1, inplace=True)
        

        # Фильтрация строк с данными
        df = df[df[desired_order].notna().any(axis=1)]
        

        
        # Удаление ненужных столбцов
        for col in ['Показа-\nтели', 'Курсив']:
            if col in df.columns:
                df = df.drop(columns=[col])
        

        # Выравнивание столбцов
        df = self.shiftable_level(df)
        

        
        # Создание таблицы для проверки
        self.table_for_check = self._create_check_tables(df, df_for_check, file_path.name)
        
        # Обработка таблиц без счетов
        if account_for_table:
            df = self._shift_level_columns_vectorized(df, account_for_table)

        df = ensure_column_types(df)
        return df, self.table_for_check
    
    def _shift_level_columns_vectorized(self, df: pd.DataFrame, account_for_table: str) -> pd.DataFrame:
        """Для ОСВ выгруженных без счетов добавляет столбец вручную"""
        level_columns = [col for col in df.columns if col.startswith('Level_')]
        
        if not level_columns:
            df['Level_0'] = account_for_table
            return df
        
        level_columns.sort()
        level_df = df[level_columns]
        all_values = level_df.stack().dropna().unique()
        
        if len(all_values) > 0:
            check_series = pd.Series(all_values)
            has_accounting = self._is_accounting_code_vectorized(check_series).any()
        else:
            has_accounting = False
        
        if not has_accounting:
            for i in range(len(level_columns) - 1, -1, -1):
                current_col = level_columns[i]
                next_col = f'Level_{i + 1}'
                if next_col not in df.columns:
                    df[next_col] = None
                df[next_col] = df[current_col]
            df['Level_0'] = account_for_table
        
        return df


class AccountOSV_NonUPPFileProcessor(BaseAccountOSVProcessor):
    """Обработчик для Анализа счета 1С не УПП"""

    spec = NON_UPP_SPEC

    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        """Поиск шапки таблицы, переименование заголовков, очистка"""
        df = BaseAccountOSVProcessor._clean_dataframe(df)
        
        processor = AccountOSV_NonUPPFileProcessor()
        spec = processor.spec
        col_idx, header_row_idx = processor._find_header_column(df, spec.header_word)

        if col_idx is None or header_row_idx is None:
            raise RegisterStructureError(
                spec.structure_message,
                hint='Похоже, это не ОСВ счёта. Проверьте, что выбран тот тип '
                     'регистра, который соответствует выгрузке.')

        # Проверка на наличие значения "Счет"
        first_col = df.iloc[:, col_idx].astype(str)
        if not (first_col == spec.header_exact).any():
            raise RegisterNotRecognizedError(
                'Файл не является ОСВ счета 1с.',
                hint=spec.recognized_hint)
        
        df = processor._process_header(df, header_row_idx, rename_columns=True)
        
        # Переименование столбцов
        cols = df.columns.tolist()
        target_indices = [
            (cols.index('Сальдо на начало периода'), ['Дебет_начало', 'Кредит_начало']),
            (cols.index('Обороты за период'), ['Дебет_оборот', 'Кредит_оборот']),
            (cols.index('Сальдо на конец периода'), ['Дебет_конец', 'Кредит_конец'])
        ]
        
        df = processor._rename_balance_columns(df, cols, target_indices)
        df = processor._clean_after_header(df)
        processor._validate_empty_osv(df)
        processor._validate_level_columns(df)
        
        return df
    
    def process_file(self, file_path: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Основная обработка таблицы"""
        self.file = file_path.name
        
        fixed_data = fix_1c_excel_case(file_path)
        df = self._preprocessor_openpyxl(fixed_data)
        del fixed_data
        
        df = self._process_dataframe_optimized(df)
        df['Исх.файл'] = self.file
        
        # Обработка пропущенных значений
        df = self._process_missing_values(df, 'Счет')
        
        # Разнос вертикальных данных
        df, max_level = self._spread_vertical_data(df, 'Счет')
        
        # Получение списков столбцов
        desired_order_not_with_suff, desired_order = self._get_desired_columns(df)
        
        # Обработка количественных и валютных данных
        df = self._handle_quantity_currency_data(df, desired_order)
        
        # Создание контрольной таблицы
        if df[df['Счет'] == 'Итого'][desired_order].empty:
            raise RegisterStructureError(
                'В выгрузке нет значений по строке «Итого».',
                hint='Сверка оборотов до и после обработки считается по строке «Итого». '
                     'Без неё достоверно проверить результат невозможно.')
        
        df_for_check = df[df['Счет'] == 'Итого'][['Счет'] + desired_order_not_with_suff].copy().tail(2).iloc[[0]]
        df_for_check[desired_order_not_with_suff] = df_for_check[desired_order_not_with_suff].astype(float).fillna(0)
        df_for_check = calculate_check_values(df_for_check, self._columns_config)
        
        # Удаление дублирующихся строк
        df = self._remove_duplicate_rows(df, 'Счет', max_level)
        
        # Удаление строк с exclude_values
        df = df[~df['Счет'].isin(exclude_values)]
        
        df = df.rename(columns={'Счет': 'Субконто'})
        df.drop('Уровень', axis=1, inplace=True)
        
        # Фильтрация строк с данными
        df = df[df[desired_order].notna().any(axis=1)]
        
        # Удаление ненужных столбцов
        for col in ['Показа-\nтели', 'Курсив']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        # Выравнивание столбцов
        df = self.shiftable_level(df)
        
        # Создание таблицы для проверки
        self.table_for_check = self._create_check_tables(df, df_for_check, file_path.name)

        df = ensure_column_types(df)
        return df, self.table_for_check