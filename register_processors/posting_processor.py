# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 12:20:46 2025

@author: a.karabedyan
"""

import numpy as np
import pandas as pd
from pathlib import Path

from register_processors.class_processor import FileProcessor
from custom_errors import RegisterProcessingError
from support_functions import fix_1c_excel_case

pd.set_option('future.no_silent_downcasting', True)


class Posting_UPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С УПП"""
    
    @staticmethod
    def _fast_keep_first_unique_per_row(df: pd.DataFrame) -> pd.DataFrame:
        """
        Оптимизированная версия без использования stack().
        Оставляет только первое вхождение значения в строке, остальные заменяет на NaN.
        Работает во всех версиях Pandas.
        """
        result = df.copy()
        
        # Применяем к каждой строке
        for idx in df.index:
            row = df.loc[idx]
            # Находим дубликаты в строке
            duplicates = row.duplicated()
            # Заменяем дубликаты на NaN
            result.loc[idx, duplicates] = np.nan
        
        return result

    @staticmethod
    def _process_quantity_section(df: pd.DataFrame) -> pd.DataFrame:
        """Обработка раздела с количеством"""
        try:
            if not (df['Содержание'] == 'Количество').any():
                return pd.DataFrame()
        except KeyError:
            raise RegisterProcessingError('Не найден или пустой столбец Содержание.')
            
        df_with_count = df[df['Содержание'] == 'Количество'].copy()
        
        # Получаем индексы столбцов 'Дт' и 'Кт'
        dt_idx = df_with_count.columns.get_loc('Дт')
        kt_idx = df_with_count.columns.get_loc('Кт')
        
        # Преобразование в числовые типы
        dt_col = pd.to_numeric(df_with_count.iloc[:, dt_idx], errors='coerce').fillna(0)
        dt_next_col = pd.to_numeric(df_with_count.iloc[:, dt_idx + 1], errors='coerce').fillna(0)
        
        kt_col = pd.to_numeric(df_with_count.iloc[:, kt_idx], errors='coerce').fillna(0)
        kt_next_col = pd.to_numeric(df_with_count.iloc[:, kt_idx + 1], errors='coerce').fillna(0)
        
        # Суммирование
        df_with_count.loc[:, 'Дт_количество'] = dt_col + dt_next_col
        df_with_count.loc[:, 'Кт_количество'] = kt_col + kt_next_col
        
        return df_with_count[['Документ', 'Дт_количество', 'Кт_количество']]

    @staticmethod
    def _process_currency_section(df: pd.DataFrame) -> pd.DataFrame:
        """Обработка раздела с валютой"""
        try:
            if not (df['Содержание'] == 'Валюта').any():
                return pd.DataFrame()
        except KeyError:
            raise RegisterProcessingError('Не найден или пустой столбец Содержание.')
            
        df_with_currency = df[df['Содержание'] == 'Валюта'].copy()
        
        # Получаем индексы столбцов 'Дт' и 'Кт'
        dt_idx = df_with_currency.columns.get_loc('Дт')
        kt_idx = df_with_currency.columns.get_loc('Кт')
        
        # Комплексная замена пустых значений
        df_with_currency.replace(['', '\n', '\t', ' ', r'^\s+$'], np.nan, 
                               inplace=True)
        df_with_currency.replace([r'^\s+$'], np.nan, 
                               inplace=True, regex=True)
        
        # Создание новых столбцов
        df_with_currency['Дт_валюта'] = df_with_currency.iloc[:, dt_idx]
        df_with_currency['Дт_валюта_количество'] = df_with_currency.iloc[:, dt_idx + 1]
        df_with_currency['Кт_валюта'] = df_with_currency.iloc[:, kt_idx]
        df_with_currency['Кт_валюта_количество'] = df_with_currency.iloc[:, kt_idx + 1]
        
        return df_with_currency[['Документ', 'Дт_валюта', 'Дт_валюта_количество', 
                               'Кт_валюта', 'Кт_валюта_количество']]

    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Основной метод обработки файла УПП"""
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df = df.dropna(axis=1, how='all')

        # Обработка DataFrame
        df = self._process_dataframe_optimized(df)

        
        # Обработка специальных разделов
        df_with_count = self._process_quantity_section(df)
        df_with_currency = self._process_currency_section(df)
        # Удаление служебных строк
        # df = df[~df['Содержание'].isin(['Количество', 'Валюта'])]
        
        if df.empty:
            raise RegisterProcessingError("Отчет по проводкам 1с пустой в файле, обработка невозможна.")

        # Подготовка к pivot
        df = df.fillna({'Содержание': '', 'Субконто Дт': '', 'Субконто Кт': ''})
        
        # Создание сводной таблицы
        operations_pivot = (
            df.assign(row_num=df.groupby(['Дата', 'Документ']).cumcount() + 1)
            .pivot_table(
                index=['Дата', 'Документ'], 
                columns='row_num', 
                values=['Содержание', 'Субконто Дт', 'Субконто Кт'], 
                aggfunc='first', 
                fill_value=''
            )
            .reset_index()
        )
        
        # Удаление служебных строк
        df = df[~df['Содержание'].isin(['Количество', 'Валюта'])]
        # Обработка дубликатов
        operations_pivot = self._fast_keep_first_unique_per_row(operations_pivot)
        
        operations_pivot = operations_pivot.dropna(how='all').dropna(how='all', axis=1)
        

        # Упрощение мультииндекса
        operations_pivot.columns = [
            '_'.join(map(str, col)).strip() if isinstance(col, tuple) else col
            for col in operations_pivot.columns.values
        ]
        
        operations_pivot.columns = [col.rstrip('_') if col.endswith('_') else col for col in operations_pivot.columns]

        # Атрибуты документов (без дубликатов)
        doc_attributes = (
            df.drop_duplicates(subset=['Документ', 'Дт', 'Кт'])
            .set_index('Документ')
            .drop(columns=['Дата', 'Субконто Дт', 'Субконто Кт'])
        )
        

        # Объединение результатов
        result = doc_attributes.join(operations_pivot.set_index('Документ'), how='left')
        
        # Добавление специальных разделов
        for section_df in [df_with_count, df_with_currency]:
            if not section_df.empty:
                result = result.join(section_df.set_index('Документ'), how='left')
        
        result = result.reset_index()
        
        
        # Финальная очистка
        result = result.dropna(subset=['Дт', 'Кт'], how='all')
        result['Документ'] = result['Документ'].str.replace(r'_end\d+$', '', regex=True)
        result = result.dropna(how='all').dropna(how='all', axis=1)
        
        
        
        # Переименование колонок
        new_columns = []
        cols = result.columns.tolist()
        for i, col in enumerate(cols):
            if str(col).startswith("NoNameCol"):
                new_name = f'{cols[i-1]}_значение' if i > 0 else 'NoNameCol0'
                new_columns.append(new_name)
            else:
                new_columns.append(col)
        
        result.columns = new_columns
        
        
        
        # Реорганизация колонок
        updated_cols = list(result.columns)
        updated_cols.insert(0, updated_cols.pop(updated_cols.index('Дата')))
        result = result[updated_cols]
        result = result.drop(columns=['Содержание'], errors='ignore')
        
        # Добавление имени файла
        result['Имя_файла'] = file_path.name
        updated_cols = ['Имя_файла'] + [col for col in result.columns if col != 'Имя_файла']
        result = result[updated_cols]
        
        # Замена пустых значений
        result.replace(
            ['', '\n', '\t', ' ', r'^\s+$'], 
            np.nan, 
            inplace=True,
        )
        result.replace(
            [r'^\s+$'], 
            np.nan, 
            inplace=True,
            regex=True
        )
        
        # Удаление пустых строк и столбцов
        result.dropna(how='all', inplace=True)
        result.dropna(how='all', axis=1, inplace=True)
        
        return result, self.table_for_check
    

class Posting_NonUPPFileProcessor(FileProcessor):
    """Обработчик для файлов из 1С (не УПП)"""


# for col_prefix in ['Документ', 'Аналитика Дт', 'Аналитика Кт']:
#     self._split_and_expand(df, 'Аналитика Дт', 'Аналитика_Дт')



    @staticmethod
    def _split_and_expand(df: pd.DataFrame, col_name: str, prefix: str) -> None:
        """Оптимизированное разбиение столбца с разделителем \n"""
        if col_name not in df.columns:
            return
            
        new_cols = df[col_name].str.split('\n', expand=True)
        if new_cols.empty:
            df.drop(columns=[col_name], inplace=True)
            return
            
        n_cols = new_cols.shape[1]
        new_cols.columns = [f'{prefix}_{i+1}' for i in range(n_cols)]
        df[new_cols.columns] = new_cols
        df.drop(columns=[col_name], inplace=True)
    
    @staticmethod    
    def _rename_columns_after_pokaz(df: pd.DataFrame) -> pd.DataFrame:
        """Корректировка столбцов для версии ERP"""
        # Поиск столбца "Показ"
        pokaz_cols = [col for col in df.columns if str(col).startswith("Показ")]
        if not pokaz_cols:
            return df
            
        pokaz_idx = df.columns.get_loc(pokaz_cols[0])
        
        # Проверка следующих 4 столбцов
        if pokaz_idx + 4 >= len(df.columns):
            return df
            
        # Проверка пустых имен
        next_cols = df.columns[pokaz_idx+1:pokaz_idx+5]
        if not all(pd.isna(col) for col in next_cols):
            return df
            
        # Переименование
        new_names = ["Дебет", "Дебет_значение", "Кредит", "Кредит_значение"]
        cols = list(df.columns)
        for i, new_name in enumerate(new_names, start=1):
            cols[pokaz_idx + i] = new_name
            
        df.columns = cols
        return df

    def process_file(self, file_path: Path) -> pd.DataFrame:
        fixed_data = fix_1c_excel_case(file_path)
        df = pd.read_excel(fixed_data, header=None)
        df.dropna(axis=1, how='all', inplace=True)

        # Поиск строки с заголовками
        period_rows = df.index[df.iloc[:, 0] == 'Период'].tolist()
        if not period_rows:
            raise RegisterProcessingError('Не найден заголовок Период в шапке таблицы')
            
        header_row = period_rows[0]
        df.columns = df.iloc[header_row]
        df = df.iloc[header_row + 1:].reset_index(drop=True)
        
        # Обработка специальных разделов
        df_with_col = pd.DataFrame()
        df_with_currency = pd.DataFrame()
        
        pokaz_cols = [col for col in df.columns if str(col).startswith('Показ')]
        if pokaz_cols:
            col_name = pokaz_cols[0]
            
            # Обработка количества
            if (df[col_name] == 'Кол.').any():
                df_with_col = df[df[col_name]=='Кол.'].copy()
                if not df_with_col.empty:
                    try:
                        dt_idx = df_with_col.columns.get_loc('Дебет')
                        df_with_col['Дебет_количество'] = pd.to_numeric(
                            df_with_col.iloc[:, dt_idx + 1], errors='coerce').fillna(0)
                    except (KeyError, IndexError):
                        pass
                        
                    try:
                        kt_idx = df_with_col.columns.get_loc('Кредит')
                        df_with_col['Кредит_количество'] = pd.to_numeric(
                            df_with_col.iloc[:, kt_idx + 1], errors='coerce').fillna(0)
                    except (KeyError, IndexError):
                        pass
                    
                    # Фильтрация валидных колонок
                    cols = ['Дебет_количество', 'Кредит_количество']
                    df_with_col = df_with_col[[col for col in cols if col in df_with_col.columns]].copy()
                    df_with_col = df_with_col.iloc[:-1]  # Удаление последней строки

            # Обработка валюты
            if (df[col_name] == 'Вал.').any():
                df_with_currency = df[df[col_name]=='Вал.'].copy()
                if not df_with_currency.empty:
                    try:
                        dt_idx = df_with_currency.columns.get_loc('Дебет')
                        df_with_currency['Дебет_валюта'] = df_with_currency.iloc[:, dt_idx + 1]
                        df_with_currency['Дебет_валютное_количество'] = pd.to_numeric(
                            df_with_currency.iloc[:, dt_idx + 2], errors='coerce').fillna(0)
                    except (KeyError, IndexError):
                        pass
                        
                    try:
                        kt_idx = df_with_currency.columns.get_loc('Кредит')
                        df_with_currency['Кредит_валюта'] = df_with_currency.iloc[:, kt_idx + 1]
                        df_with_currency['Кредит_валютное_количество'] = pd.to_numeric(
                            df_with_currency.iloc[:, kt_idx + 2], errors='coerce').fillna(0)
                    except (KeyError, IndexError):
                        pass
                    
                    cols = ['Дебет_валюта', 'Дебет_валютное_количество', 
                           'Кредит_валюта', 'Кредит_валютное_количество']
                    df_with_currency = df_with_currency[[col for col in cols if col in df_with_currency.columns]].copy()
                    df_with_currency = df_with_currency.iloc[:-1]
        
        # Фильтрация по дате
        df['Период'] = pd.to_datetime(df['Период'], format='%d.%m.%Y', errors='coerce')
        df = df[df['Период'].notna()].copy().reset_index(drop=True)
        
        # Добавление специальных разделов
        for section_df in [df_with_col, df_with_currency]:
            if not section_df.empty and len(section_df) == len(df):
                df = pd.concat([df, section_df.reset_index(drop=True)], axis=1)
        
        # Дополнительная обработка
        df.dropna(axis=1, how='all', inplace=True)
        df = self._rename_columns_after_pokaz(df)
        
        
        
        # Разбиение столбцов
        for col_prefix in ['Документ', 'Аналитика Дт', 'Аналитика Кт']:
            self._split_and_expand(df, col_prefix, col_prefix)
            # self._split_and_expand(df, col_prefix, col_prefix.replace(' ', '_'))
        
        
        # Переименование колонок
        new_columns = []
        cols = df.columns.tolist()
        for i, col in enumerate(cols):
            if pd.isna(col) or col == '':
                new_name = f'{cols[i-1]}_значение' if i > 0 else 'NoNameCol0'
                new_columns.append(new_name)
            else:
                new_columns.append(col)
                
        df.columns = new_columns
        
        # Очистка
        df.dropna(how='all', inplace=True)
        df.dropna(how='all', axis=1, inplace=True)

        # Добавление имени файла
        df.insert(0, 'Имя_файла', file_path.name)
        
        if df.empty:
            raise RegisterProcessingError("Отчет по проводкам 1с пустой, обработка невозможна.")
            
        return df, self.table_for_check
