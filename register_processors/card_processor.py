# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

import os
import numpy as np
import pandas as pd

from pathlib import Path
from typing import List, Tuple

from register_processors.class_processor import FileProcessor
from custom_errors import RegisterProcessingError
from support_functions import fix_1c_excel_case




class Card_UPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С УПП"""
    
    @staticmethod
    def _fast_keep_first_unique_per_row(df: pd.DataFrame) -> pd.DataFrame:
        arr = df.values
        mask = np.ones_like(arr, dtype=bool)
        
        for i in range(arr.shape[0]):
            seen = set()
            for j in range(arr.shape[1]):
                val = arr[i, j]
                if pd.isna(val):
                    continue
                if val in seen:
                    mask[i, j] = False
                else:
                    seen.add(val)
        
        return pd.DataFrame(np.where(mask, arr, np.nan), columns=df.columns)
    
    def _sum_columns_between(self, df: pd.DataFrame, start_col: str, end_col: str) -> pd.Series:
        """Суммирует значения всех столбцов между start_col и end_col"""
        try:
            start_idx = df.columns.get_loc(start_col)
            end_idx = df.columns.get_loc(end_col)
            
            if end_idx - start_idx > 1:
                cols_between = df.columns[start_idx + 1 : end_idx]
                return (
                    df[cols_between]
                    .apply(lambda x: pd.to_numeric(x, errors='coerce').fillna(0))
                    .sum(axis=1)
                )
            else:
                return pd.Series(0, index=df.index)
        except KeyError:
            return pd.Series(0, index=df.index)
    
    def _extract_special_data(
        self, 
        df: pd.DataFrame, 
        operation_filter: str, 
        cols_to_extract: List[str]
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Извлекает данные по количеству и валюте"""
        try:
            filtered = df[df['Операция'] == operation_filter].copy()
        except KeyError:
            raise RegisterProcessingError('Не найден или пустой столбец Операция.')
        if filtered.empty:
            return pd.DataFrame(), df
        
        # Для количества
        if operation_filter == 'Кол-во':
            result = filtered[['Документ']].copy()
            result['Дт_количество'] = self._sum_columns_between(filtered, 'Дебет', 'Кредит')
            result['Кт_количество'] = self._sum_columns_between(filtered, 'Кредит', 'Текущее сальдо')
            return result, df[df['Операция'] != operation_filter]
        
        # Для валюты
        filtered.replace([np.nan, '\n', '\t', ' '], '', inplace=True)
        filtered.replace(r'^\s+$', '', inplace=True, regex=True)

        result = filtered[['Документ']].copy()
        
        result['Дт_валюта'] = filtered['Дебет']
        
        # result['Дт_валютая_сумма'] = filtered.iloc[:, filtered.columns.get_loc('Дебет') + 1]
        result['Дт_валютая_сумма'] = self._sum_columns_between(filtered, 'Дебет', 'Кредит')
        result['Кт_валюта'] = filtered['Кредит']
        # result['Кт_валютая_сумма'] = filtered.iloc[:, filtered.columns.get_loc('Кредит') + 1]
        result['Кт_валютая_сумма'] = self._sum_columns_between(filtered, 'Кредит', 'Текущее сальдо')
        
        return result, df[df['Операция'] != operation_filter]
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df = df.dropna(axis=1, how='all')
        df = self._process_dataframe_optimized(df)
        
        # Извлечение данных по количеству и валюте
        df_count, df = self._extract_special_data(df, 'Кол-во', ['Документ', 'Дт_количество', 'Кт_количество'])
        
        
        df_currency, df = self._extract_special_data(df, 'В валюте :', ['Документ', 'Дт_валюта', 'Дт_валютая_сумма', 'Кт_валюта', 'Кт_валютая_сумма'])
                 
        if df.empty:
            raise RegisterProcessingError(f"Карточка 1с пустая в файле {file_path.name}, обработка невозможна.")
        
        # Подготовка данных
        operations_pivot = (
            df.assign(row_num=df.groupby(['Дата', 'Документ']).cumcount() + 1)
            .pivot_table(index=['Дата', 'Документ'], columns='row_num', values='Операция', aggfunc='first')
            .reset_index()
            .rename(columns=lambda x: f'Операция_{x}' if isinstance(x, int) else x)
        )
        
        operations_pivot = self._fast_keep_first_unique_per_row(operations_pivot)
        operations_pivot = operations_pivot.dropna(how='all', axis=0).dropna(how='all', axis=1)
        
        doc_attributes = (
            df.drop_duplicates(subset=['Документ', 'Дебет', 'Кредит'])
            .set_index('Документ')
            .drop(columns=['Дата'])
        )

        # Объединение данных
        result = doc_attributes.join(operations_pivot.set_index('Документ'), how='left')
        
        for df_special in [df_count, df_currency]:
            if not df_special.empty:
                result = result.join(df_special.set_index('Документ'), how='left')
        
        result = result.reset_index()
        
        # Обработка колонок
        result = result.dropna(subset=['Дебет', 'Кредит'], how='all')
        result['Документ'] = result['Документ'].str.replace(r'_end\d+$', '', regex=True)
        result = result.dropna(how='all', axis=0).dropna(how='all', axis=1)
        

        cols = result.columns.tolist()

        new_columns = []
        for i, col in enumerate(cols):
            if str(col).startswith("NoNameCol"):
                if i == 0:
                    # Для первого столбца слева нет предыдущего, можно задать дефолтное имя
                    new_name = 'NoNameCol0'
                else:
                    left_col = cols[i - 1]
                    new_name = f'{left_col}_значение'
                new_columns.append(new_name)
            else:
                new_columns.append(col)
        result.columns = new_columns


        # Реорганизация колонок
        if 'Дата' in result.columns:
            result = result[['Дата'] + [col for col in result.columns if col != 'Дата']]
        
        result = result.drop(columns=['Операция'], errors='ignore')
        result.insert(0, 'Имя_файла', os.path.basename(file_path))

        return result, self.table_for_check

class Card_NonUPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С (не УПП)"""

    @staticmethod
    def _split_and_expand(df: pd.DataFrame, col_name: str, prefix: str) -> None:
        """Разбивает столбец с разделителем \n"""
        if col_name not in df.columns:
            return
            
        new_cols = df[col_name].str.split('\n', expand=True)
        if new_cols is None or new_cols.empty:
            df.drop(columns=[col_name], inplace=True)
            return
            
        new_cols.columns = [f'{prefix}_{i+1}' for i in range(new_cols.shape[1])]
        df[new_cols.columns] = new_cols
        df.drop(columns=[col_name], inplace=True)

    def _extract_special_data(
        self,
        df: pd.DataFrame,
        value_filter: str,
        columns: List[str]
    ) -> pd.DataFrame:
        """Извлекает специальные данные (количество/валюта)"""
        col_name = next((col for col in df.columns if str(col).startswith('Показ')), None)
        if not col_name:
            return pd.DataFrame()
            
        filtered = df[df[col_name] == value_filter].iloc[1:-1].copy()
        if filtered.empty:
            return pd.DataFrame()
            
        result = pd.DataFrame()
        
        try:
            if 'Дебет' in filtered.columns:
                dt_index = filtered.columns.get_loc('Дебет')
                if value_filter == 'Кол.':
                    # Пробуем первый столбец
                    debit_qty = pd.to_numeric(filtered.iloc[:, dt_index + 1], errors='coerce').fillna(0)
                    
                    # Проверяем сумму ВСЕГО столбца
                    if debit_qty.sum() == 0:
                        # Если сумма всех значений в столбце равна 0, берем другой столбец
                        debit_qty = pd.to_numeric(filtered.iloc[:, dt_index + 2], errors='coerce').fillna(0)
                    
                    result['Дебет_количество'] = debit_qty
                elif value_filter == 'Вал.':
                    result['Дебет_валюта'] = filtered.iloc[:, dt_index + 1]
                    result['Дебет_валютное_количество'] = pd.to_numeric(filtered.iloc[:, dt_index + 2], errors='coerce').fillna(0)
        except (KeyError, IndexError):
            pass
            
        try:
            if 'Кредит' in filtered.columns:
                kt_index = filtered.columns.get_loc('Кредит')
                if value_filter == 'Кол.':
                    
                    
                    # Пробуем первый столбец
                    credit_qty = pd.to_numeric(filtered.iloc[:, kt_index + 1], errors='coerce').fillna(0)
                    
                    # Проверяем сумму ВСЕГО столбца
                    if credit_qty.sum() == 0:
                        # Если сумма всех значений в столбце равна 0, берем другой столбец
                        credit_qty = pd.to_numeric(filtered.iloc[:, kt_index + 2], errors='coerce').fillna(0)
                    
                    result['Кредит_количество'] = credit_qty
                    
                    
                elif value_filter == 'Вал.':
                    result['Кредит_валюта'] = filtered.iloc[:, kt_index + 1]
                    result['Кредит_валютное_количество'] = pd.to_numeric(filtered.iloc[:, kt_index + 2], errors='coerce').fillna(0)
        except (KeyError, IndexError):
            pass
        result.reset_index(drop=True, inplace=True)
        
        return result

    def process_file(self, file_path: Path) -> pd.DataFrame:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df = df.dropna(axis=1, how='all')
        
        # Поиск строки с заголовками
        period_rows = df.index[df.iloc[:, 0] == 'Период'].tolist()
        if not period_rows:
            raise RegisterProcessingError('Не найден заголовок Период в шапке таблицы')
            
        header_row = period_rows[0]
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:].reset_index(drop=True)
        
        # Извлечение специальных данных
        df_count = self._extract_special_data(df, 'Кол.', ['Дебет_количество', 'Кредит_количество'])
        df_currency = self._extract_special_data(df, 'Вал.', ['Дебет_валюта', 'Дебет_валютное_количество', 'Кредит_валюта', 'Кредит_валютное_количество'])

        

        # Фильтрация по дате
        df['Период'] = pd.to_datetime(df['Период'], format='%d.%m.%Y', errors='coerce')
        df = df[df['Период'].notna()].copy().reset_index(drop=True)
        
        
        

        # Добавление извлеченных данных
        if not df_count.empty and len(df_count) == len(df):
            df = pd.concat([df, df_count], axis=1)
        if not df_currency.empty and len(df_currency) == len(df):
            df = pd.concat([df, df_currency], axis=1)

        # Обработка колонок
        for col in ['Документ', 'Аналитика Дт', 'Аналитика Кт']:
            self._split_and_expand(df, col, col)

        cols = df.columns.tolist()

        new_columns = [
                        f'{cols[i - 1]}_значение' if (pd.isna(col) or col == '') and i > 0 
                        else col
                        for i, col in enumerate(df.columns)
                    ]
        
        
        df.columns = new_columns
        df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
        df.insert(0, 'Имя_файла', os.path.basename(file_path))
        
        if df.empty:
            raise RegisterProcessingError(f"Карточка 1с пустая в файле {file_path.name}, обработка невозможна. Файл не УПП")
        
        return df, self.table_for_check