# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

import numpy as np
import pandas as pd
from pathlib import Path

from register_processors.class_processor import FileProcessor, exclude_values
from custom_errors import RegisterProcessingError
from support_functions import fix_1c_excel_case


class AccountOSV_UPPFileProcessor(FileProcessor):
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
        
        # Заменяем все пустые строки '' на NaN во всём DataFrame (векторизованно)
        df = df.replace('', np.nan)
        
        # удалим пустые строки и столбцы        
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        
        
        
        # Ищем столбец, содержащий "Субконто" в первых 30 строках (или меньше, если строк меньше)
        subkonto_col_idx = None
        max_rows_to_check = min(MAX_HEADER_ROWS, df.shape[0])  # Проверяем не больше 30 строк
        
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if 'субконто' in col_values.values:
                subkonto_col_idx = col_idx
                break
        
        if subkonto_col_idx is None:
            raise RegisterProcessingError('Не найден столбец с "Субконто" в первых 30 строках.')
        
        # Теперь используем найденный столбец
        first_col = df.iloc[:, subkonto_col_idx].astype(str)
        mask = first_col == 'Субконто'

        # ошибка, если нет искомого значения
        if not mask.any():
            raise RegisterProcessingError('Файл не является ОСВ счета 1с.')
        # индекс строки с искомым словом
        date_row_idx = mask.idxmax()
        
        # Установка заголовков и очистка
        df.columns = df.iloc[date_row_idx]
        df = df.iloc[date_row_idx + 1:].copy()

        # Переименуем столбцы, в которых находятся уровни и признак курсива
        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
        
        cols = df.columns.tolist()
        target_idx_a = cols.index('Сальдо на начало периода')
        target_idx_b = cols.index('Оборот за период')
        target_idx_c = cols.index('Сальдо на конец периода')
       
        # Новый список имен столбцов — копируем текущие
        new_cols = cols.copy()
        
        
        def find_column_index(cols, df, ind_col, word):
            
            # Проходим по столбцам после заданного индекса
            for idx in range(ind_col + 1, len(cols)):
                # Проверяем первую строку (iloc[0]) в текущем столбце
                if df.iloc[0, idx] == word:
                    return idx
            
            # Если подходящий столбец не найден
            return None
        
        # Переименовываем целевой столбец по индексу
        new_cols[target_idx_a] = 'Дебет_начало'
        # new_cols[target_idx_a + 1] = 'Кредит_начало'
        new_cols[find_column_index(cols, df, target_idx_a, 'Кредит')] = 'Кредит_начало'
        
        new_cols[target_idx_b] = 'Дебет_оборот'
        # new_cols[target_idx_b + 2] = 'Кредит_оборот'
        new_cols[find_column_index(cols, df, target_idx_b, 'Кредит')] = 'Кредит_оборот'
        
        new_cols[target_idx_c] = 'Дебет_конец'
        # new_cols[target_idx_c + 1] = 'Кредит_конец'
        new_cols[find_column_index(cols, df, target_idx_c, 'Кредит')] = 'Кредит_конец'
        
        # Присваиваем новый список имен столбцов DataFrame
        df.columns = new_cols
        
        # удалим столбцы с пустыми заголовками
        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)
        
        # удалим строку содержащую остатки от шапки (Дебет Кредит Дебет Кредит Дебет Кредит)
        df = df.iloc[1:]
        
        # если отсутствует иерархия (+ и - на полях excel файла), значит он пуст
        if df['Уровень'].max() == 0:
            raise RegisterProcessingError('ОСВ счета пустая.')
    
        # Уровень и Курсив должны иметь 0 или 1, иначе - ошибка
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterProcessingError('Найдены пустые значения в столбцах Уровень или Курсив.')
        
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
        
        
        
        # Столбец с именем файла
        df['Исх.файл'] = self.file
        
        

        '''Обработка пропущенных значений'''

        # Для выгрузок с полем "Количество"
        if 'Показа-\nтели' in df.columns:
            mask = df['Показа-\nтели'].str.contains('Кол.|Вал.', na=False)
            df.loc[~mask, 'Субконто'] = df.loc[~mask, 'Субконто'].fillna('Не_заполнено')
            df['Субконто'] = df['Субконто'].ffill()
        else:
            # Проставляем значение "Количество" (для ОСВ, так как строки с количеством не обозначены)
            df['Субконто'] = np.where(
                                        df['Субконто'].isna() & df['Уровень'].eq(df['Уровень'].shift(1)),
                                        'Количество',
                                        df['Субконто'])
            # Удалим строки, содержащие значение "Количество" ниже строки с Итого. Предыдущий Код "Количество" ниже Итого проставляет даже в регистрах
            # Без количественных значений.
            # Найдем индекс строки, где находится 'Итого'.
            # Проверяем, есть ли 'Итого' в столбце.
            if (df['Субконто'] == 'Итого').any():
                # Если 'Итого' существует, получаем индекс
                index_total = df[df['Субконто'] == 'Итого'].index[0]
                # Фильтруем DataFrame
                df = df[(df.index <= index_total) | ((df.index > index_total) & (df['Субконто'] != 'Количество'))]

            df.loc[:, 'Субконто'] = df['Субконто'].fillna('Не_заполнено')
    
        # Преобразование в строки и добавление ведущего нуля для счетов до 10 (01, 02 и т.д.)
        mask = (df['Субконто'].str.len() == 1) & self._is_accounting_code_vectorized(df['Субконто'])
        df.loc[mask, 'Субконто'] = '0' + df.loc[mask, 'Субконто']
        df['Субконто'] = df['Субконто'].astype(str)
        
        
        
        '''Разносим вертикальные данные в горизонтальные'''
        
        max_level = df['Уровень'].max()
        
        # Обрабатываем специальный случай для "Количество" векторизованно
        quantity_mask = df['Субконто'] == 'Количество'
        
        if quantity_mask.any():
            # Создаем Series с последними непустыми значениями уровней для строк с "Количество"
            last_level_values = pd.Series(index=df[quantity_mask].index, dtype=object)
            
            # Для каждой строки с "Количество" находим последний непустой Level
            for idx in df[quantity_mask].index:
                for level in range(max_level, -1, -1):
                    level_col = f'Level_{level}'
                    if level_col in df.columns and pd.notna(df.at[idx, level_col]):
                        last_level_values[idx] = df.at[idx, level_col]
                        break
            
            # Заменяем "Количество" на найденные значения
            df.loc[quantity_mask, 'Субконто'] = last_level_values
        
        # Сначала создаем все Level колонки
        for level in range(max_level + 1):
            # Маска для строк данного уровня
            level_mask = df['Уровень'] == level
            
            # Заполняем значения для данного уровня
            df[f'Level_{level}'] = df['Субконто'].where(level_mask)
            
            # Forward fill для значений этого уровня
            df[f'Level_{level}'] = df[f'Level_{level}'].ffill()
            
            # Обнуляем значения там, где уровень выше текущего
            higher_level_mask = df['Уровень'] < level
            df.loc[higher_level_mask, f'Level_{level}'] = None
            
        df.loc[df[quantity_mask].index, 'Субконто'] = 'Количество'
        
        
        
        '''Транспонируем количественные и валютные данные'''
        
        # Если таблица с количественными данными, дополним ее столбцами с количеством путем
        # сдвига соответствующего столбца на строку вверх, так как строки с количеством/валютой чередуются с денежными значениями
        
        
        # Получим список столбцов с сальдо и оборотами и оставим только те, которые есть в таблице
        desired_order_not_with_suff_do_ko = [col for col in ['Дебет_начало',
                                                             'Кредит_начало',
                                                             'Дебет_оборот',
                                                             'Кредит_оборот',
                                                             'Дебет_конец',
                                                             'Кредит_конец',
                                                             ] if col in df.columns]
        desired_order = desired_order_not_with_suff_do_ko.copy()

        # Находим столбцы в таблице, заканчивающиеся на '_до' и '_ко'
        do_ko_columns = df.filter(regex='(_до|_ко)$').columns.tolist()

        # Добавим столбцы, заканчивающиеся на '_до' и '_ко', в таблицу
        if do_ko_columns:
            desired_order += do_ko_columns
        
        if df['Субконто'].isin(['Количество']).any() or 'Показа-\nтели' in df.columns:
            for i in desired_order:
                df[f'Количество_{i}'] = df[i].shift(-1)
        
        if df['Субконто'].isin(['Валютная сумма']).any() or 'Показа-\nтели' in df.columns:
            if df['Субконто'].str.startswith('Валюта').any():
                df['Валюта'] = df['Субконто'].shift(-1)
            for i in desired_order:
                df[f'ВалютнаяСумма_{i}'] = df[i].shift(-2)
            
        # Очистим таблицу от строк с количеством и валютой
        mask = (
            (df['Субконто'] == 'Количество') |
            (df['Субконто'] == 'Валютная сумма') |
            (df['Субконто'].str.startswith('Валюта'))
        )
        df = df[~mask]
        
        
        '''Сохраняем данные по оборотам до обработки в таблицах'''
        
        if df[df['Субконто'] == 'Итого'][desired_order].empty:
            raise RegisterProcessingError('Нет значений по строке Итого')
            
        df_for_check = df[df['Субконто'] == 'Итого'][['Субконто'] + desired_order_not_with_suff_do_ko].copy().tail(2).iloc[[0]]
        df_for_check[desired_order_not_with_suff_do_ko] = df_for_check[desired_order_not_with_suff_do_ko].astype(float).fillna(0)
        
        # Списки необходимых столбцов для каждой новой колонки
        start_debit = 'Дебет_начало'
        start_credit = 'Кредит_начало'
        end_debit = 'Дебет_конец'
        end_credit = 'Кредит_конец'
        debit_turnover = 'Дебет_оборот'
        credit_turnover = 'Кредит_оборот'
        
        # Функция для безопасного получения столбца или Series из нулей, если столбца нет
        def get_col_or_zeros(df, col):
            if col in df.columns:
                return df[col]
            else:
                return 0
        
        # Создаем новые столбцы с проверкой наличия исходных
        df_for_check['Сальдо_начало_до_обработки'] = get_col_or_zeros(df_for_check, start_debit) - get_col_or_zeros(df_for_check, start_credit)
        df_for_check['Сальдо_конец_до_обработки'] = get_col_or_zeros(df_for_check, end_debit) - get_col_or_zeros(df_for_check, end_credit)
        df_for_check['Оборот_до_обработки'] = get_col_or_zeros(df_for_check, debit_turnover) - get_col_or_zeros(df_for_check, credit_turnover)
        df_for_check = df_for_check[['Сальдо_начало_до_обработки', 'Сальдо_конец_до_обработки', 'Оборот_до_обработки']].reset_index()

        
        ''' После разнесения строк в плоский вид, в таблице остаются строки с дублирующими оборотами.
        Например, итоговые обороты, итоги по субконто и т.д. Удаляем.'''
        
        max_level = df['Уровень'].max()
        conditions = []
        
        for i in range(max_level):
            condition = (
                (df['Уровень'] == i) & 
                (df['Субконто'] == df[f'Level_{i}']) & 
                (df['Уровень'].shift(-1) > i)
            )
            conditions.append(condition)
        
        # Объединяем все условия
        mask = pd.concat(conditions, axis=1).any(axis=1)
        df = df[~mask]
        
        # Удаляем строки, содержащие значения Итого
        df = df[~df['Субконто'].str.contains('Итого')]
        
        # Удаляем строки, содержащие значения из списка exclude_values
        df = df[~df['Субконто'].isin(exclude_values)]

        df = df.rename(columns={'Счет': 'Субконто'})
        df.drop('Уровень', axis=1, inplace=True)

        # отберем только те строки, в которых хотя бы в одном из столбцов, определенных в existing_columns, есть непропущенные значения (не NaN)
        df = df[df[desired_order].notna().any(axis=1)]
        
        if 'Показа-\nтели' in df.columns:
            df = df.drop(columns=['Показа-\nтели'])
        if 'Курсив' in df.columns:
            df = df.drop(columns=['Курсив'])
        
        '''
        Выровняем столбцы так, чтобы счета оказались в одном столбце без аналитики и субконто,
        затем обновим значения столбца Субсчет (сейчас в нем счета), включив в него именно субсчета.
        '''
        
        df = self.shiftable_level(df)
        
        """
        Добавляет к таблице с оборотами до обработки, созданной выше,
        данные по оборотам после обработки и отклонениями между ними.
        """

        # Вычисление итоговых значений - свернутые значения сальдо и оборотов - обработанных таблиц
        df_check_after_process = pd.DataFrame({
            'Сальдо_начало_после_обработки': [get_col_or_zeros(df, start_debit).sum() - get_col_or_zeros(df, start_credit).sum()],
            'Оборот_после_обработки': [get_col_or_zeros(df, debit_turnover).sum() - get_col_or_zeros(df, credit_turnover).sum()],
            'Сальдо_конец_после_обработки': [get_col_or_zeros(df, end_debit).sum() - get_col_or_zeros(df, end_credit).sum()]
        })


        # Объединение таблиц - обороты до и после обработки таблиц
        pivot_df_check = pd.concat([df_for_check, df_check_after_process], axis=1).fillna(0)

        # Вычисление отклонений в данных до и после обработки таблиц
        for field in ['Сальдо_начало_разница', 'Оборот_разница', 'Сальдо_конец_разница']:
            pivot_df_check[field] = (pivot_df_check[field.replace('_разница', '_до_обработки')] -
                                      pivot_df_check[field.replace('_разница', '_после_обработки')]).round()

        # Помечаем данные именем файла
        pivot_df_check['Исх.файл'] = file_path.name

        # Запись таблицы в хранилище таблиц
        self.table_for_check = pivot_df_check
        
        return df, self.table_for_check

        

class AccountOSV_NonUPPFileProcessor(FileProcessor):
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
        
        MAX_HEADER_ROWS = 30  # Максимальное количество строк для поиска шапки
        # Заменяем все пустые строки '' на NaN во всём DataFrame (векторизованно)
        df = df.replace('', np.nan)
        
        # удалим пустые строки и столбцы        
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        
        # Ищем столбец, содержащий "Субконто" в первых 30 строках (или меньше, если строк меньше)
        subkonto_col_idx = None
        max_rows_to_check = min(MAX_HEADER_ROWS, df.shape[0])  # Проверяем не больше 30 строк
        
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if 'счет' in col_values.values:
                subkonto_col_idx = col_idx
                break
        
        if subkonto_col_idx is None:
            raise RegisterProcessingError('Не найден столбец с "Субконто" в первых 30 строках.')
        
        # Теперь используем найденный столбец
        first_col = df.iloc[:, subkonto_col_idx].astype(str)
        mask = first_col == 'Счет'

        # ошибка, если нет искомого значения
        if not mask.any():
            raise RegisterProcessingError('Файл не является ОСВ счета 1с.')
        # индекс строки с искомым словом
        date_row_idx = mask.idxmax()
        
    
        # Установка заголовков и очистка
        df.columns = df.iloc[date_row_idx]
        
        df = df.iloc[date_row_idx + 1:].copy()

        # Переименуем столбцы, в которых находятся уровни и признак курсива
        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
        
        
        cols = df.columns.tolist()

        target_idx_a = cols.index('Сальдо на начало периода')
        target_idx_b = cols.index('Обороты за период')
        target_idx_c = cols.index('Сальдо на конец периода')
       
        # Новый список имен столбцов — копируем текущие
        new_cols = cols.copy()
        
        # Переименовываем целевой столбец по индексу
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
            raise RegisterProcessingError('ОСВ счета пустая.')
    
        # Уровень и Курсив должны иметь 0 или 1, иначе - ошибка
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterProcessingError('Найдены пустые значения в столбцах Уровень или Курсив.')
        
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
        
        # Столбец с именем файла
        df['Исх.файл'] = self.file
        
        
        '''Обработка пропущенных значений'''

        # Для выгрузок с полем "Количество"
        if 'Показа-\nтели' in df.columns:
            mask = df['Показа-\nтели'].str.contains('Кол.|Вал.', na=False)
            df.loc[~mask, 'Счет'] = df.loc[~mask, 'Счет'].fillna('Не_заполнено')
            df['Счет'] = df['Счет'].ffill()
        else:
            # Проставляем значение "Количество" (для ОСВ, так как строки с количеством не обозначены)
            df['Счет'] = np.where(
                                        df['Счет'].isna() & df['Уровень'].eq(df['Уровень'].shift(1)),
                                        'Количество',
                                        df['Счет'])
            # Удалим строки, содержащие значение "Количество" ниже строки с Итого. Предыдущий Код "Количество" ниже Итого проставляет даже в регистрах
            # Без количественных значений.
            # Найдем индекс строки, где находится 'Итого'.
            # Проверяем, есть ли 'Итого' в столбце.
            if (df['Счет'] == 'Итого').any():
                # Если 'Итого' существует, получаем индекс
                index_total = df[df['Счет'] == 'Итого'].index[0]
                # Фильтруем DataFrame
                df = df[(df.index <= index_total) | ((df.index > index_total) & (df['Счет'] != 'Количество'))]

            df.loc[:, 'Счет'] = df['Счет'].fillna('Не_заполнено')

        # Преобразование в строки и добавление ведущего нуля для счетов до 10 (01, 02 и т.д.)
        mask = (df['Счет'].str.len() == 1) & self._is_accounting_code_vectorized(df['Счет'])
        df.loc[mask, 'Счет'] = '0' + df.loc[mask, 'Счет']
        df['Счет'] = df['Счет'].astype(str)
        
        
        '''Разносим вертикальные данные в горизонтальные'''
        
        max_level = df['Уровень'].max()
        
        # Обрабатываем специальный случай для "Количество" векторизованно
        quantity_mask = df['Счет'] == 'Количество'
        
        if quantity_mask.any():
            # Создаем Series с последними непустыми значениями уровней для строк с "Количество"
            last_level_values = pd.Series(index=df[quantity_mask].index, dtype=object)
            
            # Для каждой строки с "Количество" находим последний непустой Level
            for idx in df[quantity_mask].index:
                for level in range(max_level, -1, -1):
                    level_col = f'Level_{level}'
                    if level_col in df.columns and pd.notna(df.at[idx, level_col]):
                        last_level_values[idx] = df.at[idx, level_col]
                        break
            
            # Заменяем "Количество" на найденные значения
            df.loc[quantity_mask, 'Счет'] = last_level_values
        
        # Сначала создаем все Level колонки
        for level in range(max_level + 1):
            # Маска для строк данного уровня
            level_mask = df['Уровень'] == level
            
            # Заполняем значения для данного уровня
            df[f'Level_{level}'] = df['Счет'].where(level_mask)
            
            # Forward fill для значений этого уровня
            df[f'Level_{level}'] = df[f'Level_{level}'].ffill()
            
            # Обнуляем значения там, где уровень выше текущего
            higher_level_mask = df['Уровень'] < level
            df.loc[higher_level_mask, f'Level_{level}'] = None
            
        df.loc[df[quantity_mask].index, 'Счет'] = 'Количество'
        
        
        '''Транспонируем количественные и валютные данные'''
        
        # Если таблица с количественными данными, дополним ее столбцами с количеством путем
        # сдвига соответствующего столбца на строку вверх, так как строки с количеством/валютой чередуются с денежными значениями
        
        
        # Получим список столбцов с сальдо и оборотами и оставим только те, которые есть в таблице
        desired_order_not_with_suff_do_ko = [col for col in ['Дебет_начало',
                                                             'Кредит_начало',
                                                             'Дебет_оборот',
                                                             'Кредит_оборот',
                                                             'Дебет_конец',
                                                             'Кредит_конец',
                                                             ] if col in df.columns]
        desired_order = desired_order_not_with_suff_do_ko.copy()
        

        # Находим столбцы в таблице, заканчивающиеся на '_до' и '_ко'
        do_ko_columns = df.filter(regex='(_до|_ко)$').columns.tolist()

        # Добавим столбцы, заканчивающиеся на '_до' и '_ко', в таблицу
        if do_ko_columns:
            desired_order += do_ko_columns
            
        if 'Показа-\nтели' in df.columns and df['Показа-\nтели'].str.startswith('Кол.').any():
            for i in desired_order:
                df[f'Количество_{i}'] = df[i].shift(-1)
        
        if 'Показа-\nтели' in df.columns and df['Показа-\nтели'].str.startswith('Вал.').any():
            for i in desired_order:
                df[f'ВалютнаяСумма_{i}'] = df[i].shift(-1)
        
        # if df['Счет'].isin(['Валютная сумма']).any() or 'Показа-\nтели' in df.columns:
        #     if df['Счет'].str.startswith('Валюта').any():
        #         df['Валюта'] = df['Счет'].shift(-1)
        #     for i in desired_order:
        #         df[f'ВалютнаяСумма_{i}'] = df[i].shift(-2)
        
        
        
        # Очистим таблицу от строк с количеством и валютой
        if 'Показа-\nтели' in df.columns:
            mask = (
                (df['Показа-\nтели'] == 'Кол.') |
                (df['Показа-\nтели'] == 'Вал.') |
                (df['Показа-\nтели'].str.startswith('Валюта'))
            )
            df = df[~mask]
        
        '''Сохраняем данные по оборотам до обработки в таблицах'''
        
        if df[df['Счет'] == 'Итого'][desired_order].empty:
            raise RegisterProcessingError
            
        df_for_check = df[df['Счет'] == 'Итого'][['Счет'] + desired_order_not_with_suff_do_ko].copy().tail(2).iloc[[0]]
        df_for_check[desired_order_not_with_suff_do_ko] = df_for_check[desired_order_not_with_suff_do_ko].astype(float).fillna(0)
        
        
        # Списки необходимых столбцов для каждой новой колонки
        start_debit = 'Дебет_начало'
        start_credit = 'Кредит_начало'
        end_debit = 'Дебет_конец'
        end_credit = 'Кредит_конец'
        debit_turnover = 'Дебет_оборот'
        credit_turnover = 'Кредит_оборот'
        
        # Функция для безопасного получения столбца или Series из нулей, если столбца нет
        def get_col_or_zeros(df, col):
            if col in df.columns:
                return df[col]
            else:
                return 0
        
        # Создаем новые столбцы с проверкой наличия исходных
        df_for_check['Сальдо_начало_до_обработки'] = get_col_or_zeros(df_for_check, start_debit) - get_col_or_zeros(df_for_check, start_credit)
        df_for_check['Сальдо_конец_до_обработки'] = get_col_or_zeros(df_for_check, end_debit) - get_col_or_zeros(df_for_check, end_credit)
        df_for_check['Оборот_до_обработки'] = get_col_or_zeros(df_for_check, debit_turnover) - get_col_or_zeros(df_for_check, credit_turnover)
        df_for_check = df_for_check[['Сальдо_начало_до_обработки', 'Сальдо_конец_до_обработки', 'Оборот_до_обработки']].reset_index()

    
        ''' После разнесения строк в плоский вид, в таблице остаются строки с дублирующими оборотами.
        Например, итоговые обороты, итоги по субконто и т.д. Удаляем.'''
        
        max_level = df['Уровень'].max()
        conditions = []
        
        for i in range(max_level):
            condition = (
                (df['Уровень'] == i) & 
                (df['Счет'] == df[f'Level_{i}']) & 
                (df['Уровень'].shift(-1) > i)
            )
            conditions.append(condition)
        
        # Объединяем все условия
        mask = pd.concat(conditions, axis=1).any(axis=1)
        df = df[~mask]

        
        # Удаляем строки, содержащие значения из списка exclude_values
        df = df[~df['Счет'].isin(exclude_values)]
        

        df = df.rename(columns={'Счет': 'Субконто'})
        df.drop('Уровень', axis=1, inplace=True)

        # отберем только те строки, в которых хотя бы в одном из столбцов, определенных в existing_columns, есть непропущенные значения (не NaN)
        df = df[df[desired_order].notna().any(axis=1)]
        
        if 'Показа-\nтели' in df.columns:
            df = df.drop(columns=['Показа-\nтели'])
        if 'Курсив' in df.columns:
            df = df.drop(columns=['Курсив'])
            
        '''
        Выровняем столбцы так, чтобы счета оказались в одном столбце без аналитики и субконто,
        затем обновим значения столбца Субсчет (сейчас в нем счета), включив в него именно субсчета.
        '''
        
        df = self.shiftable_level(df)
        
        
        """
        Добавляет к таблице с оборотами до обработки, созданной выше,
        данные по оборотам после обработки и отклонениями между ними.
        """
        
        # Вычисление итоговых значений - свернутые значения сальдо и оборотов - обработанных таблиц
        df_check_after_process = pd.DataFrame({
            'Сальдо_начало_после_обработки': [get_col_or_zeros(df, start_debit).sum() - get_col_or_zeros(df, start_credit).sum()],
            'Оборот_после_обработки': [get_col_or_zeros(df, debit_turnover).sum() - get_col_or_zeros(df, credit_turnover).sum()],
            'Сальдо_конец_после_обработки': [get_col_or_zeros(df, end_debit).sum() - get_col_or_zeros(df, end_credit).sum()]
        })

        # Объединение таблиц - обороты до и после обработки таблиц
        pivot_df_check = pd.concat([df_for_check, df_check_after_process], axis=1).fillna(0)

        # Вычисление отклонений в данных до и после обработки таблиц
        for field in ['Сальдо_начало_разница', 'Оборот_разница', 'Сальдо_конец_разница']:
            pivot_df_check[field] = (pivot_df_check[field.replace('_разница', '_до_обработки')] -
                                      pivot_df_check[field.replace('_разница', '_после_обработки')]).round()

        # Помечаем данные именем файла
        pivot_df_check['Исх.файл'] = file_path.name
        
        
        

        # Запись таблицы в хранилище таблиц
        self.table_for_check = pivot_df_check
        
        return df, self.table_for_check
        
        
        
