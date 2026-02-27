# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

import numpy as np
import pandas as pd
from pathlib import Path

from register_processors.class_processor import FileProcessor
from custom_errors import RegisterProcessingError
from support_functions import fix_1c_excel_case



class GeneralOSV_UPPFileProcessor(FileProcessor):
    """Обработчик для Анализа счета 1С УПП"""
    def __init__(self):
        super().__init__()
        self.df_type_connection = pd.DataFrame()  # хранение данных анализа счета с полем Вид связи КА за период
        
    
    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        '''
        Поиск шапки таблицы, переименование заголовков для единообразия
        выгрузок из других 1С, очистка от пустых строк и столбцов
        

        Parameters
        ----------
        df : pd.DataFrame
            сырая выгрузка в pd.DataFrame из Excel.

        Raises
        ------
        RegisterProcessingError
            Возникает, если обрабатываемый файл не является ОСВ
            или является пустой ОСВ. Такой файл не обрабатывается.
            Его имя сохраняется в списке, выводимом в конце обработки.

        Returns
        -------
        df : pd.DataFrame
            Готовая к дальнейшей обработке таблица.

        '''
        
        MAX_HEADER_ROWS = 30  # Максимальное количество строк для поиска шапки

        # Заменяем пустые строки на NaN для удобства очистки
        df = df.replace('', np.nan)

        # Удаляем полностью пустые столбцы и строки
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)

        # Ограничиваем количество строк для поиска шапки
        max_rows_to_check = min(MAX_HEADER_ROWS, len(df))

        # Ищем индекс столбца, где в первых max_rows_to_check строках встречается 'счет' (регистр не важен)
        account_col_idx = None
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if 'счет' in col_values.values:
                account_col_idx = col_idx
                break
        if account_col_idx is None:
            raise RegisterProcessingError('Не найден столбец с "Счет" в первых 30 строках.')

        # Проверяем, что в найденном столбце есть строка 'Счет' (с точным регистром)
        first_col = df.iloc[:, account_col_idx].astype(str)
        mask = first_col == 'Счет'
        if not mask.any():
            raise RegisterProcessingError('Файл не является ОСВ 1с.')

        date_row_idx = mask.idxmax()  # индекс строки с заголовком

        # Устанавливаем заголовки и отбрасываем строки шапки
        df.columns = df.iloc[date_row_idx]
        df = df.iloc[date_row_idx + 1:].copy()

        # Переименовываем первые два столбца в 'Уровень' и 'Курсив'
        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()

        # Находим индекс столбца с "Наименование" в первой строке данных
        first_row = df.iloc[0]
        
        # Находим булев маской, где значение равно 'Наименование'
        mask = first_row == 'Наименование'
        
        # Получаем индексы столбцов, в которос есть Наименование
        cols_name_acc = np.where(mask)[0]
        if cols_name_acc.size > 0:
            col_index_name_acc = cols_name_acc[0]
        else:
            raise RegisterProcessingError('ОСВ выгружена без наименований счета')

        # Проверяем наличие необходимых столбцов для переименования
        required_cols = [
            'Сальдо на начало периода',
            'Оборот за период',
            'Сальдо на конец периода'
        ]

        for col in required_cols:
            if col not in df.columns:
                raise RegisterProcessingError(f'Отсутствует обязательный столбец: {col}')

        # Получаем индексы для переименования
        target_idx_a = df.columns.get_loc('Сальдо на начало периода')
        target_idx_b = df.columns.get_loc('Оборот за период')
        target_idx_c = df.columns.get_loc('Сальдо на конец периода')

        # Создаем копию списка столбцов для переименования
        new_cols = list(df.columns)

        if col_index_name_acc is not None:
            new_cols[col_index_name_acc] = 'Наименование'

        # Переименование парных столбцов (Дебет и Кредит)
        new_cols[target_idx_a] = 'Дебет_начало'
        new_cols[target_idx_a + 1] = 'Кредит_начало'

        new_cols[target_idx_b] = 'Дебет_оборот'
        new_cols[target_idx_b + 1] = 'Кредит_оборот'

        new_cols[target_idx_c] = 'Дебет_конец'
        new_cols[target_idx_c + 1] = 'Кредит_конец'

        df.columns = new_cols

        # Удаляем столбцы с пустыми заголовками
        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)

        # Удаляем строку с остатками шапки (Дебет Кредит Дебет Кредит ...)
        df = df.iloc[1:]

        # Проверяем, что есть иерархия (максимум 'Уровень' > 0)
        if df['Уровень'].max() == 0:
            raise RegisterProcessingError('ОСВ пустая.')

        # Проверяем отсутствие пустых значений в 'Уровень' и 'Курсив'
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterProcessingError('Найдены пустые значения в столбцах Уровень или Курсив.')

        # Удаляем вспомогательные столбцы
        df.drop(columns=['Уровень', 'Курсив'], errors='ignore', inplace=True)

        return df

    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        '''
        Основная обработка таблицы.

        Parameters
        ----------
        file_path : Path
            Путь к обрабатываемому файлу (Excel - выгрузка из 1С).

        Returns
        -------
        df : pd.DataFrame
            Плоская таблица, готовая к конкатенации с другими выгрузками.

        '''
        # Имя файла для включения в отдельный столбец итоговой таблицы
        self.file = file_path.name
        
        # исправляем ошибку выгрузки из 1С в старую версию Excel
        fixed_data = fix_1c_excel_case(file_path)
        
        # предобработка (добавление столбцов Уровень и Курсив)
        df = self._preprocessor_openpyxl(fixed_data)

        del fixed_data  # Освобождаем память
        
        # Установка заголовков таблицы и чистка данных
        df = self._process_dataframe_optimized(df)
        
        # Очистка от пустых строк после строки с Итого
        mask = df['Счет'].astype(str).str.len() == 1
        df['Счет'] = np.where(mask, '0' + df['Счет'].astype(str), df['Счет'])
        df['Счет'] = df['Счет'].fillna('').astype('str')
        df.dropna(how='all', inplace=True)

        df.loc[:, 'Наименование'] = (
            df.loc[:, 'Наименование']
            .fillna('')  # Заменяет None и np.nan на ''
            .astype(str)
            .str.strip()  # Убирает пробелы
            .replace('', np.nan)  # Заменяет '' на np.nan
        )
        
        # Удаление строк с np.nan в 'Наименование'
        df = df.dropna(subset=['Наименование'])
        
        # Столбец с именем файла
        df['Исх.файл'] = self.file
        
        return df, self.table_for_check

        

class GeneralOSV_NonUPPFileProcessor(FileProcessor):
    """Обработчик для Анализа счета 1С не УПП"""
    def __init__(self):
        super().__init__()
        self.df_type_connection = pd.DataFrame()  # хранение данных анализа счета с полем Вид связи КА за период
        
    
    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        '''
        Поиск шапки таблицы, переименование заголовков для единообразия
        выгрузок из других 1С, очистка от пустых строк и столбцов
        

        Parameters
        ----------
        df : pd.DataFrame
            сырая выгрузка в pd.DataFrame из Excel.

        Raises
        ------
        RegisterProcessingError
            Возникает, если обрабатываемый файл не является ОСВ
            или является пустой ОСВ. Такой файл не обрабатывается.
            Его имя сохраняется в списке, выводимом в конце обработки.

        Returns
        -------
        df : pd.DataFrame
            Готовая к дальнейшей обработке таблица.

        '''
        
        # Заменяем все пустые строки '' на NaN во всём DataFrame (векторизованно)
        df = df.replace('', np.nan)
        
        # удалим пустые строки и столбцы        
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        
        # Ищем столбец, содержащий "Счет" в первых 30 строках (или меньше, если строк меньше)
        account_col_idx = None
        max_rows_to_check = min(30, df.shape[0])  # Проверяем не больше 30 строк, так как ищем в шапке, т.е. вверху таблицы
        
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if 'счет' in col_values.values:
                account_col_idx = col_idx
                break
        if account_col_idx is None:
            raise RegisterProcessingError('Не найден столбец с "Счет" в первых 30 строках.')
        
        # Теперь используем найденный столбец
        first_col = df.iloc[:, account_col_idx].astype(str)
        mask = first_col == 'Счет'

        # ошибка, если нет искомого значения
        if not mask.any():
            raise RegisterProcessingError('Файл не является ОСВ 1с.')
        # индекс строки с искомым словом
        date_row_idx = mask.idxmax()
        
        # Установка заголовков и очистка
        df.columns = df.iloc[date_row_idx]
        
        df = df.iloc[date_row_idx + 1:].copy()
    
        # Переименуем столбцы, в которых находятся уровни и признак курсива
        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
        
        cols = df.columns.tolist()
        
        target_idx_0 = cols.index('Наименование счета')
        target_idx_a = cols.index('Сальдо на начало периода')
        target_idx_b = cols.index('Обороты за период')
        target_idx_c = cols.index('Сальдо на конец периода')
       
        # Новый список имен столбцов — копируем текущие
        new_cols = cols.copy()
        
        # Переименовываем целевой столбец по индексу
        new_cols[target_idx_0] = 'Наименование'
        
        new_cols[target_idx_a] = 'Дебет_начало'
        new_cols[target_idx_a + 1] = 'Кредит_начало'
        
        new_cols[target_idx_b] = 'Дебет_оборот'
        new_cols[target_idx_b + 1] = 'Кредит_оборот'
        
        new_cols[target_idx_c] = 'Дебет_конец'
        new_cols[target_idx_c + 1] = 'Кредит_конец'
        
        # Присваиваем новый список имен столбцов DataFrame
        df.columns = new_cols
        
        # удалим столбцы с пустыми заголовками
        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)
        
        # удалим строку содержащую остатки от шапки (Дебет Кредит Дебет Кредит Дебет Кредит)
        df = df.iloc[1:]
        
        # если отсутствует иерархия (+ и - на полях excel файла), значит он пуст
        if df['Уровень'].max() == 0:
            raise RegisterProcessingError('ОСВ пустая.')
    
        # Уровень и Курсив должны иметь 0 или 1, иначе - ошибка
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterProcessingError('Найдены пустые значения в столбцах Уровень или Курсив.')
        
        df.drop(columns=['Уровень', 'Курсив'], errors='ignore', inplace=True)
        
        return df

    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        '''
        Основная обработка таблицы.

        Parameters
        ----------
        file_path : Path
            Путь к обрабатываемому файлу (Excel - выгрузка из 1С).

        Returns
        -------
        df : pd.DataFrame
            Плоская таблица, готовая к конкатенации с другими выгрузками.

        '''
        # Имя файла для включения в отдельный столбец итоговой таблицы
        self.file = file_path.name
        
        # исправляем ошибку выгрузки из 1С в старую версию Excel
        fixed_data = fix_1c_excel_case(file_path)
        
        # предобработка (добавление столбцов Уровень и Курсив)
        df = self._preprocessor_openpyxl(fixed_data)
        
        del fixed_data  # Освобождаем память
        
        # Установка заголовков таблицы и чистка данных
        df = self._process_dataframe_optimized(df)
        
        mask = df['Счет'].astype(str).str.len() == 1
        df['Счет'] = np.where(mask, '0' + df['Счет'].astype(str), df['Счет'])
        df['Счет'] = df['Счет'].fillna('').astype('str')
        df.dropna(how='all', inplace=True)

        df.loc[:, 'Наименование'] = (
            df.loc[:, 'Наименование']
            .fillna('')  # Заменяет None и np.nan на ''
            .astype(str)
            .str.strip()  # Убирает пробелы
            .replace('', np.nan)  # Заменяет '' на np.nan
        )
        
        # Удаление строк с np.nan в 'Наименование'
        df = df.dropna(subset=['Наименование'])
        
        # Столбец с именем файла
        df['Исх.файл'] = self.file
        
        return df, self.table_for_check