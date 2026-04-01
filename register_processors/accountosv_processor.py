# # -*- coding: utf-8 -*-
# """
# Created on Mon Aug 25 11:23:20 2025

# @author: a.karabedyan
# """

# import re
# import numpy as np
# import pandas as pd
# from pathlib import Path

# from register_processors.class_processor import FileProcessor, exclude_values
# from custom_errors import RegisterProcessingError
# from support_functions import fix_1c_excel_case



# def find_account_from_text(df, search_text="Оборотно-сальдовая ведомость по счету "):
#     """
#     Ищет в DataFrame ячейку с search_text и извлекает номер счета.
    
#     Parameters:
#     -----------
#     df : pd.DataFrame
#         DataFrame для поиска
#     search_text : str
#         Текст для поиска
    
#     Returns:
#     --------
#     str or None: Номер счета или None если не найден
#     """
#     for col in df.columns:
#         for idx, value in df[col].items():
#             if isinstance(value, str) and search_text in value:
#                 # Вариант 1: через split
#                 try:
#                     after_text = value.split(search_text)[1]
#                     account = after_text.split()[0]
#                     return account
#                 except (IndexError, AttributeError):
#                     # Вариант 2: через regex если split не сработал
#                     pattern = rf"{re.escape(search_text)}(\S+)"
#                     match = re.search(pattern, value)
#                     if match:
#                         return match.group(1)
#     return None

# class AccountOSV_UPPFileProcessor(FileProcessor):
#     """Обработчик для Анализа счета 1С УПП"""
#     def __init__(self):
#         super().__init__()
#         self.df_type_connection = pd.DataFrame()  # хранение данных анализа счета с полем Вид связи КА за период
        
    
#     @staticmethod
#     def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
#         '''
#         Поиск шапки таблицы, переименование заголовков для единообразия
#         выгрузок из других 1С, очистка от пустых строк и столбцов
        

#         Parameters
#         ----------
#         df : pd.DataFrame
#             сырая выгрузка в pd.DataFrame из Excel.

#         Raises
#         ------
#         RegisterProcessingError
#             Возникает, если обрабатываемый файл не является ОСВ
#             или является пустой ОСВ. Такой файл не обрабатывается.
#             Его имя сохраняется в списке, выводимом в конце обработки.

#         Returns
#         -------
#         df : pd.DataFrame
#             Готовая к дальнейшей обработке таблица.

#         '''
        
#         MAX_HEADER_ROWS = 30  # Максимальное количество строк для поиска шапки
        
#         # Заменяем все пустые строки '' на NaN во всём DataFrame (векторизованно)
#         df = df.replace('', np.nan)
        
#         # удалим пустые строки и столбцы        
#         df.dropna(axis=1, how='all', inplace=True)
#         df.dropna(axis=0, how='all', inplace=True)
        
        
        
#         # Ищем столбец, содержащий "Субконто" в первых 30 строках (или меньше, если строк меньше)
#         subkonto_col_idx = None
#         max_rows_to_check = min(MAX_HEADER_ROWS, df.shape[0])  # Проверяем не больше 30 строк
        
#         for col_idx in range(df.shape[1]):
#             col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
#             if 'субконто' in col_values.values:
#                 subkonto_col_idx = col_idx
#                 break
        
#         if subkonto_col_idx is None:
#             raise RegisterProcessingError('Не найден столбец с "Субконто" в первых 30 строках.')
        
#         # Теперь используем найденный столбец
#         first_col = df.iloc[:, subkonto_col_idx].astype(str)
#         mask = first_col == 'Субконто'

#         # ошибка, если нет искомого значения
#         if not mask.any():
#             raise RegisterProcessingError('Файл не является ОСВ счета 1с.')
#         # индекс строки с искомым словом
#         date_row_idx = mask.idxmax()
        
#         # Установка заголовков и очистка
#         df.columns = df.iloc[date_row_idx]
#         df = df.iloc[date_row_idx + 1:].copy()

#         # Переименуем столбцы, в которых находятся уровни и признак курсива
#         df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
        
#         cols = df.columns.tolist()
#         target_idx_a = cols.index('Сальдо на начало периода')
#         target_idx_b = cols.index('Оборот за период')
#         target_idx_c = cols.index('Сальдо на конец периода')
       
#         # Новый список имен столбцов — копируем текущие
#         new_cols = cols.copy()
        
        
#         def find_column_index(cols, df, ind_col, word):
            
#             # Проходим по столбцам после заданного индекса
#             for idx in range(ind_col + 1, len(cols)):
#                 # Проверяем первую строку (iloc[0]) в текущем столбце
#                 if df.iloc[0, idx] == word:
#                     return idx
            
#             # Если подходящий столбец не найден
#             return None
        
#         # Переименовываем целевой столбец по индексу
#         new_cols[target_idx_a] = 'Дебет_начало'
#         # new_cols[target_idx_a + 1] = 'Кредит_начало'
#         new_cols[find_column_index(cols, df, target_idx_a, 'Кредит')] = 'Кредит_начало'
        
#         new_cols[target_idx_b] = 'Дебет_оборот'
#         # new_cols[target_idx_b + 2] = 'Кредит_оборот'
#         new_cols[find_column_index(cols, df, target_idx_b, 'Кредит')] = 'Кредит_оборот'
        
#         new_cols[target_idx_c] = 'Дебет_конец'
#         # new_cols[target_idx_c + 1] = 'Кредит_конец'
#         new_cols[find_column_index(cols, df, target_idx_c, 'Кредит')] = 'Кредит_конец'
        
#         # Присваиваем новый список имен столбцов DataFrame
#         df.columns = new_cols
        
#         # удалим столбцы с пустыми заголовками
#         df = df.loc[:, df.columns.notna()]
#         df.columns = df.columns.astype(str)
        
#         # удалим строку содержащую остатки от шапки (Дебет Кредит Дебет Кредит Дебет Кредит)
#         df = df.iloc[1:]
        
        
        
        
#         # если отсутствует иерархия (+ и - на полях excel файла), значит он пуст
#         if df['Уровень'].max() == 0:
#             check_final_sum_osv = df[['Дебет_начало', 'Кредит_начало', 'Дебет_оборот', 'Кредит_оборот', 'Дебет_конец', 'Кредит_конец']].sum().sum()
#             if check_final_sum_osv == 0:
#                 raise RegisterProcessingError('ОСВ счета вероятно пустая.')
#             else:
#                 df.loc[:, 'Уровень'] = 1
                
    
#         # Уровень и Курсив должны иметь 0 или 1, иначе - ошибка
#         if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
#             raise RegisterProcessingError('Найдены пустые значения в столбцах Уровень или Курсив.')
        
#         return df

    
#     def process_file(self, file_path: Path) -> pd.DataFrame:
#         '''
#         Основная обработка таблицы.

#         Parameters
#         ----------
#         file_path : Path
#             Путь к обрабатываемому файлу (Excel - выгрузка из 1С).

#         Returns
#         -------
#         df : pd.DataFrame
#             Плоская таблица, готовая к конкатенации с другими выгрузками.
        
#         '''
        
#         # Имя файла для включения в отдельный столбец итоговой таблицы
#         self.file = file_path.name
        
        
#         # исправляем ошибку выгрузки из 1С в старую версию Excel
#         fixed_data = fix_1c_excel_case(file_path)
#         # предобработка (добавление столбцов Уровень и Курсив)
#         df = self._preprocessor_openpyxl(fixed_data)
        
        
#         del fixed_data  # Освобождаем память
        
#         # Сохраним счет для выгрузок без указания счетов (так они формируются, если у счета нет субсчетов)
#         account_for_table_with_one_row = find_account_from_text(df, search_text="Оборотно-сальдовая ведомость по счету ")
        
#         # Установка заголовков таблицы и чистка данных
#         df = self._process_dataframe_optimized(df)
        
        
        
#         # Столбец с именем файла
#         df['Исх.файл'] = self.file
        
        

#         '''Обработка пропущенных значений'''

#         # Для выгрузок с полем "Количество"
#         if 'Показа-\nтели' in df.columns:
#             mask = df['Показа-\nтели'].str.contains('Кол.|Вал.', na=False)
#             df.loc[~mask, 'Субконто'] = df.loc[~mask, 'Субконто'].fillna('Не_заполнено')
#             df['Субконто'] = df['Субконто'].ffill()
#         else:
#             # Проставляем значение "Количество" (для ОСВ, так как строки с количеством не обозначены)
#             df['Субконто'] = np.where(
#                                         df['Субконто'].isna() & df['Уровень'].eq(df['Уровень'].shift(1)),
#                                         'Количество',
#                                         df['Субконто'])
#             # Удалим строки, содержащие значение "Количество" ниже строки с Итого. Предыдущий Код "Количество" ниже Итого проставляет даже в регистрах
#             # Без количественных значений.
#             # Найдем индекс строки, где находится 'Итого'.
#             # Проверяем, есть ли 'Итого' в столбце.
#             if (df['Субконто'] == 'Итого').any():
#                 # Если 'Итого' существует, получаем индекс
#                 index_total = df[df['Субконто'] == 'Итого'].index[0]
#                 # Фильтруем DataFrame
#                 df = df[(df.index <= index_total) | ((df.index > index_total) & (df['Субконто'] != 'Количество'))]

#             df.loc[:, 'Субконто'] = df['Субконто'].fillna('Не_заполнено')
    
        
        
#         # Преобразование в строки и добавление ведущего нуля для счетов до 10 (01, 02 и т.д.)
#         mask = (df['Субконто'].str.len() == 1) & self._is_accounting_code_vectorized(df['Субконто'])
#         df.loc[mask, 'Субконто'] = '0' + df.loc[mask, 'Субконто']
#         df['Субконто'] = df['Субконто'].astype(str)
        
        
        
#         '''Разносим вертикальные данные в горизонтальные'''
        
#         max_level = df['Уровень'].max()
        
#         # Обрабатываем специальный случай для "Количество" векторизованно
#         quantity_mask = df['Субконто'] == 'Количество'
        
#         if quantity_mask.any():
#             # Создаем Series с последними непустыми значениями уровней для строк с "Количество"
#             last_level_values = pd.Series(index=df[quantity_mask].index, dtype=object)
            
#             # Для каждой строки с "Количество" находим последний непустой Level
#             for idx in df[quantity_mask].index:
#                 for level in range(max_level, -1, -1):
#                     level_col = f'Level_{level}'
#                     if level_col in df.columns and pd.notna(df.at[idx, level_col]):
#                         last_level_values[idx] = df.at[idx, level_col]
#                         break
            
#             # Заменяем "Количество" на найденные значения
#             df.loc[quantity_mask, 'Субконто'] = last_level_values
        
#         # Сначала создаем все Level колонки
#         for level in range(max_level + 1):
#             # Маска для строк данного уровня
#             level_mask = df['Уровень'] == level
            
#             # Заполняем значения для данного уровня
#             df[f'Level_{level}'] = df['Субконто'].where(level_mask)
            
#             # Forward fill для значений этого уровня
#             df[f'Level_{level}'] = df[f'Level_{level}'].ffill()
            
#             # Обнуляем значения там, где уровень выше текущего
#             higher_level_mask = df['Уровень'] < level
#             df.loc[higher_level_mask, f'Level_{level}'] = None
            
#         df.loc[df[quantity_mask].index, 'Субконто'] = 'Количество'
        
        
        
#         '''Транспонируем количественные и валютные данные'''
        
#         # Если таблица с количественными данными, дополним ее столбцами с количеством путем
#         # сдвига соответствующего столбца на строку вверх, так как строки с количеством/валютой чередуются с денежными значениями
        
        
#         # Получим список столбцов с сальдо и оборотами и оставим только те, которые есть в таблице
#         desired_order_not_with_suff_do_ko = [col for col in ['Дебет_начало',
#                                                              'Кредит_начало',
#                                                              'Дебет_оборот',
#                                                              'Кредит_оборот',
#                                                              'Дебет_конец',
#                                                              'Кредит_конец',
#                                                              ] if col in df.columns]
#         desired_order = desired_order_not_with_suff_do_ko.copy()

#         # Находим столбцы в таблице, заканчивающиеся на '_до' и '_ко'
#         do_ko_columns = df.filter(regex='(_до|_ко)$').columns.tolist()

#         # Добавим столбцы, заканчивающиеся на '_до' и '_ко', в таблицу
#         if do_ko_columns:
#             desired_order += do_ko_columns
        
#         if df['Субконто'].isin(['Количество']).any() or 'Показа-\nтели' in df.columns:
#             for i in desired_order:
#                 df[f'Количество_{i}'] = df[i].shift(-1)
        
#         if df['Субконто'].isin(['Валютная сумма']).any() or 'Показа-\nтели' in df.columns:
#             if df['Субконто'].str.startswith('Валюта').any():
#                 df['Валюта'] = df['Субконто'].shift(-1)
#             for i in desired_order:
#                 df[f'ВалютнаяСумма_{i}'] = df[i].shift(-2)
            
#         # Очистим таблицу от строк с количеством и валютой
#         mask = (
#             (df['Субконто'] == 'Количество') |
#             (df['Субконто'] == 'Валютная сумма') |
#             (df['Субконто'].str.startswith('Валюта'))
#         )
#         df = df[~mask]
        
        
        
#         '''Сохраняем данные по оборотам до обработки в таблицах'''
        
#         if df[df['Субконто'] == 'Итого'][desired_order].empty:
#             raise RegisterProcessingError('Нет значений по строке Итого')
            
#         df_for_check = df[df['Субконто'] == 'Итого'][['Субконто'] + desired_order_not_with_suff_do_ko].copy().tail(2).iloc[[0]]
#         df_for_check[desired_order_not_with_suff_do_ko] = df_for_check[desired_order_not_with_suff_do_ko].astype(float).fillna(0)
        
#         # Списки необходимых столбцов для каждой новой колонки
#         start_debit = 'Дебет_начало'
#         start_credit = 'Кредит_начало'
#         end_debit = 'Дебет_конец'
#         end_credit = 'Кредит_конец'
#         debit_turnover = 'Дебет_оборот'
#         credit_turnover = 'Кредит_оборот'
        
#         # Функция для безопасного получения столбца или Series из нулей, если столбца нет
#         def get_col_or_zeros(df, col):
#             if col in df.columns:
#                 return df[col]
#             else:
#                 return 0
        
#         # Создаем новые столбцы с проверкой наличия исходных
#         df_for_check['Сальдо_начало_до_обработки'] = get_col_or_zeros(df_for_check, start_debit) - get_col_or_zeros(df_for_check, start_credit)
#         df_for_check['Сальдо_конец_до_обработки'] = get_col_or_zeros(df_for_check, end_debit) - get_col_or_zeros(df_for_check, end_credit)
#         df_for_check['Оборот_до_обработки'] = get_col_or_zeros(df_for_check, debit_turnover) - get_col_or_zeros(df_for_check, credit_turnover)
#         df_for_check = df_for_check[['Сальдо_начало_до_обработки', 'Сальдо_конец_до_обработки', 'Оборот_до_обработки']].reset_index()

        
#         ''' После разнесения строк в плоский вид, в таблице остаются строки с дублирующими оборотами.
#         Например, итоговые обороты, итоги по субконто и т.д. Удаляем.'''
        
#         max_level = df['Уровень'].max()
#         conditions = []
        
#         for i in range(max_level):
#             condition = (
#                 (df['Уровень'] == i) & 
#                 (df['Субконто'] == df[f'Level_{i}']) & 
#                 (df['Уровень'].shift(-1) > i)
#             )
#             conditions.append(condition)
        
        
        
#         # Объединяем все условия
#         mask = pd.concat(conditions, axis=1).any(axis=1)
#         df = df[~mask]
        
#         level_columns = [col for col in df.columns if col.startswith('Level_')]
#         if len(df)>1:
#             # Удаляем строки, содержащие значения Итого
#             df = df[~df['Субконто'].str.contains('Итого')]
#             # Удаляем строки, содержащие значения из списка exclude_values
#             df = df[~df['Субконто'].isin(exclude_values)]
#             if account_for_table_with_one_row:
#                 for col in level_columns:
#                     # Создаем условие: значение равно 'Итого' или является NaN
#                     condition = (df[col] == 'Итого') | (df[col].isna())
#                     # Применяем замену
#                     df[col] = df[col].mask(condition, account_for_table_with_one_row)
                
#         else:
#             # заменим Итого на номер счета в переменной account_for_table_with_one_row
#             first_index = df.index[0]
#             if (df.loc[first_index, 'Субконто'] == 'Итого' and account_for_table_with_one_row):
#                 df.loc[first_index, 'Субконто'] = account_for_table_with_one_row
                
#                 for col in level_columns:
#                     # Создаем условие: значение равно 'Итого' или является NaN
#                     condition = (df[col] == 'Итого') | (df[col].isna())
#                     # Применяем замену
#                     df[col] = df[col].mask(condition, account_for_table_with_one_row)

#         df = df.rename(columns={'Счет': 'Субконто'})
#         df.drop('Уровень', axis=1, inplace=True)

#         # отберем только те строки, в которых хотя бы в одном из столбцов, определенных в existing_columns, есть непропущенные значения (не NaN)
#         df = df[df[desired_order].notna().any(axis=1)]
        
#         if 'Показа-\nтели' in df.columns:
#             df = df.drop(columns=['Показа-\nтели'])
#         if 'Курсив' in df.columns:
#             df = df.drop(columns=['Курсив'])
        
#         '''
#         Выровняем столбцы так, чтобы счета оказались в одном столбце без аналитики и субконто,
#         затем обновим значения столбца Субсчет (сейчас в нем счета), включив в него именно субсчета.
#         '''
        
#         df = self.shiftable_level(df)
        
#         """
#         Добавляет к таблице с оборотами до обработки, созданной выше,
#         данные по оборотам после обработки и отклонениями между ними.
#         """

#         # Вычисление итоговых значений - свернутые значения сальдо и оборотов - обработанных таблиц
#         df_check_after_process = pd.DataFrame({
#             'Сальдо_начало_после_обработки': [get_col_or_zeros(df, start_debit).sum() - get_col_or_zeros(df, start_credit).sum()],
#             'Оборот_после_обработки': [get_col_or_zeros(df, debit_turnover).sum() - get_col_or_zeros(df, credit_turnover).sum()],
#             'Сальдо_конец_после_обработки': [get_col_or_zeros(df, end_debit).sum() - get_col_or_zeros(df, end_credit).sum()]
#         })


#         # Объединение таблиц - обороты до и после обработки таблиц
#         pivot_df_check = pd.concat([df_for_check, df_check_after_process], axis=1).fillna(0)

#         # Вычисление отклонений в данных до и после обработки таблиц
#         for field in ['Сальдо_начало_разница', 'Оборот_разница', 'Сальдо_конец_разница']:
#             pivot_df_check[field] = (pivot_df_check[field.replace('_разница', '_до_обработки')] -
#                                       pivot_df_check[field.replace('_разница', '_после_обработки')]).round()

#         # Помечаем данные именем файла
#         pivot_df_check['Исх.файл'] = file_path.name

#         # Запись таблицы в хранилище таблиц
#         self.table_for_check = pivot_df_check
        
#         def shift_level_columns_vectorized(df, account_for_table_with_one_row):
#             """
#             Для ОСВ выгруженных без счетов пробуем добавить столбец вручную.
#             Номер счета предварительно пытаемся вытянуть из названия регистра.
#             """
#             # Получаем столбцы Level_
#             level_columns = [col for col in df.columns if col.startswith('Level_')]
            
#             if not level_columns:
#                 df['Level_0'] = account_for_table_with_one_row
#                 return df
            
#             # Сортируем столбцы
#             level_columns.sort()
            
#             # Проверяем ВСЕ значения во ВСЕХ строках
#             # Создаем DataFrame только с Level_ столбцами
#             level_df = df[level_columns]
            
#             # Собираем все уникальные значения
#             all_values = level_df.stack().dropna().unique()
            
#             # Проверяем, есть ли хотя бы один бухсчет
#             if len(all_values) > 0:
#                 check_series = pd.Series(all_values)
#                 is_accounting = self._is_accounting_code_vectorized(check_series)
#                 has_accounting = is_accounting.any()
#             else:
#                 has_accounting = False
            
#             # Если нет бухсчетов - выполняем преобразование
#             if not has_accounting:
#                 # Создаем новые столбцы для сдвинутых значений
#                 for i in range(len(level_columns) - 1, -1, -1):
#                     current_col = level_columns[i]
#                     next_col = f'Level_{i + 1}'
                    
#                     if next_col not in df.columns:
#                         df[next_col] = None
                    
#                     # Векторизованное копирование значений
#                     df[next_col] = df[current_col]
                
#                 # Вставляем новое значение в Level_0 для всех строк
#                 df['Level_0'] = account_for_table_with_one_row
            
#             return df
#         if account_for_table_with_one_row:
#             df = shift_level_columns_vectorized(df, account_for_table_with_one_row)
        
#         return df, self.table_for_check

        

# class AccountOSV_NonUPPFileProcessor(FileProcessor):
#     """Обработчик для Анализа счета 1С не УПП"""
#     def __init__(self):
#         super().__init__()
#         self.df_type_connection = pd.DataFrame()  # хранение данных анализа счета с полем Вид связи КА за период
        
    
#     @staticmethod
#     def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
#         '''
#         Поиск шапки таблицы, переименование заголовков для единообразия
#         выгрузок из других 1С, очистка от пустых строк и столбцов
        

#         Parameters
#         ----------
#         df : pd.DataFrame
#             сырая выгрузка в pd.DataFrame из Excel.

#         Raises
#         ------
#         RegisterProcessingError
#             Возникает, если обрабатываемый файл не является ОСВ
#             или является пустой ОСВ. Такой файл не обрабатывается.
#             Его имя сохраняется в списке, выводимом в конце обработки.

#         Returns
#         -------
#         df : pd.DataFrame
#             Готовая к дальнейшей обработке таблица.

#         '''
        
#         MAX_HEADER_ROWS = 30  # Максимальное количество строк для поиска шапки
#         # Заменяем все пустые строки '' на NaN во всём DataFrame (векторизованно)
#         df = df.replace('', np.nan)
        
#         # удалим пустые строки и столбцы        
#         df.dropna(axis=1, how='all', inplace=True)
#         df.dropna(axis=0, how='all', inplace=True)
        
#         # Ищем столбец, содержащий "Субконто" в первых 30 строках (или меньше, если строк меньше)
#         subkonto_col_idx = None
#         max_rows_to_check = min(MAX_HEADER_ROWS, df.shape[0])  # Проверяем не больше 30 строк
        
#         for col_idx in range(df.shape[1]):
#             col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
#             if 'счет' in col_values.values:
#                 subkonto_col_idx = col_idx
#                 break
        
#         if subkonto_col_idx is None:
#             raise RegisterProcessingError('Не найден столбец с "Субконто" в первых 30 строках.')
        
#         # Теперь используем найденный столбец
#         first_col = df.iloc[:, subkonto_col_idx].astype(str)
#         mask = first_col == 'Счет'

#         # ошибка, если нет искомого значения
#         if not mask.any():
#             raise RegisterProcessingError('Файл не является ОСВ счета 1с.')
#         # индекс строки с искомым словом
#         date_row_idx = mask.idxmax()
        
    
#         # Установка заголовков и очистка
#         df.columns = df.iloc[date_row_idx]
        
#         df = df.iloc[date_row_idx + 1:].copy()

#         # Переименуем столбцы, в которых находятся уровни и признак курсива
#         df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
        
        
#         cols = df.columns.tolist()

#         target_idx_a = cols.index('Сальдо на начало периода')
#         target_idx_b = cols.index('Обороты за период')
#         target_idx_c = cols.index('Сальдо на конец периода')
       
#         # Новый список имен столбцов — копируем текущие
#         new_cols = cols.copy()
        
#         # Переименовываем целевой столбец по индексу
#         new_cols[target_idx_a] = 'Дебет_начало'
#         new_cols[target_idx_a + 1] = 'Кредит_начало'
        
#         new_cols[target_idx_b] = 'Дебет_оборот'
#         new_cols[target_idx_b + 1] = 'Кредит_оборот'
        
#         new_cols[target_idx_c] = 'Дебет_конец'
#         new_cols[target_idx_c + 1] = 'Кредит_конец'
        
#         # Присваиваем новый список имен столбцов DataFrame
#         df.columns = new_cols
        
#         # удалим столбцы с пустыми заголовками
#         df = df.loc[:, df.columns.notna()]
#         df.columns = df.columns.astype(str)
        
#         # удалим строку содержащую остатки от шапки (Дебет Кредит Дебет Кредит Дебет Кредит)
#         df = df.iloc[1:]
        
    
#         # если отсутствует иерархия (+ и - на полях excel файла), значит он пуст
#         if df['Уровень'].max() == 0:
#             raise RegisterProcessingError('ОСВ счета пустая.')
    
#         # Уровень и Курсив должны иметь 0 или 1, иначе - ошибка
#         if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
#             raise RegisterProcessingError('Найдены пустые значения в столбцах Уровень или Курсив.')
        
#         return df

    
#     def process_file(self, file_path: Path) -> pd.DataFrame:
#         '''
#         Основная обработка таблицы.

#         Parameters
#         ----------
#         file_path : Path
#             Путь к обрабатываемому файлу (Excel - выгрузка из 1С).

#         Returns
#         -------
#         df : pd.DataFrame
#             Плоская таблица, готовая к конкатенации с другими выгрузками.

#         '''
        
#         # Имя файла для включения в отдельный столбец итоговой таблицы
#         self.file = file_path.name
        
#         # исправляем ошибку выгрузки из 1С в старую версию Excel
#         fixed_data = fix_1c_excel_case(file_path)
        
#         # предобработка (добавление столбцов Уровень и Курсив)
#         df = self._preprocessor_openpyxl(fixed_data)
        
#         del fixed_data  # Освобождаем память
        
#         # Установка заголовков таблицы и чистка данных
#         df = self._process_dataframe_optimized(df)
        
#         # Столбец с именем файла
#         df['Исх.файл'] = self.file
        
        
#         '''Обработка пропущенных значений'''

#         # Для выгрузок с полем "Количество"
#         if 'Показа-\nтели' in df.columns:
#             mask = df['Показа-\nтели'].str.contains('Кол.|Вал.', na=False)
#             df.loc[~mask, 'Счет'] = df.loc[~mask, 'Счет'].fillna('Не_заполнено')
#             df['Счет'] = df['Счет'].ffill()
#         else:
#             # Проставляем значение "Количество" (для ОСВ, так как строки с количеством не обозначены)
#             df['Счет'] = np.where(
#                                         df['Счет'].isna() & df['Уровень'].eq(df['Уровень'].shift(1)),
#                                         'Количество',
#                                         df['Счет'])
#             # Удалим строки, содержащие значение "Количество" ниже строки с Итого. Предыдущий Код "Количество" ниже Итого проставляет даже в регистрах
#             # Без количественных значений.
#             # Найдем индекс строки, где находится 'Итого'.
#             # Проверяем, есть ли 'Итого' в столбце.
#             if (df['Счет'] == 'Итого').any():
#                 # Если 'Итого' существует, получаем индекс
#                 index_total = df[df['Счет'] == 'Итого'].index[0]
#                 # Фильтруем DataFrame
#                 df = df[(df.index <= index_total) | ((df.index > index_total) & (df['Счет'] != 'Количество'))]

#             df.loc[:, 'Счет'] = df['Счет'].fillna('Не_заполнено')

#         # Преобразование в строки и добавление ведущего нуля для счетов до 10 (01, 02 и т.д.)
#         mask = (df['Счет'].str.len() == 1) & self._is_accounting_code_vectorized(df['Счет'])
#         df.loc[mask, 'Счет'] = '0' + df.loc[mask, 'Счет']
#         df['Счет'] = df['Счет'].astype(str)
        
        
#         '''Разносим вертикальные данные в горизонтальные'''
        
#         max_level = df['Уровень'].max()
        
#         # Обрабатываем специальный случай для "Количество" векторизованно
#         quantity_mask = df['Счет'] == 'Количество'
        
#         if quantity_mask.any():
#             # Создаем Series с последними непустыми значениями уровней для строк с "Количество"
#             last_level_values = pd.Series(index=df[quantity_mask].index, dtype=object)
            
#             # Для каждой строки с "Количество" находим последний непустой Level
#             for idx in df[quantity_mask].index:
#                 for level in range(max_level, -1, -1):
#                     level_col = f'Level_{level}'
#                     if level_col in df.columns and pd.notna(df.at[idx, level_col]):
#                         last_level_values[idx] = df.at[idx, level_col]
#                         break
            
#             # Заменяем "Количество" на найденные значения
#             df.loc[quantity_mask, 'Счет'] = last_level_values
        
#         # Сначала создаем все Level колонки
#         for level in range(max_level + 1):
#             # Маска для строк данного уровня
#             level_mask = df['Уровень'] == level
            
#             # Заполняем значения для данного уровня
#             df[f'Level_{level}'] = df['Счет'].where(level_mask)
            
#             # Forward fill для значений этого уровня
#             df[f'Level_{level}'] = df[f'Level_{level}'].ffill()
            
#             # Обнуляем значения там, где уровень выше текущего
#             higher_level_mask = df['Уровень'] < level
#             df.loc[higher_level_mask, f'Level_{level}'] = None
            
#         df.loc[df[quantity_mask].index, 'Счет'] = 'Количество'
        
        
#         '''Транспонируем количественные и валютные данные'''
        
#         # Если таблица с количественными данными, дополним ее столбцами с количеством путем
#         # сдвига соответствующего столбца на строку вверх, так как строки с количеством/валютой чередуются с денежными значениями
        
        
#         # Получим список столбцов с сальдо и оборотами и оставим только те, которые есть в таблице
#         desired_order_not_with_suff_do_ko = [col for col in ['Дебет_начало',
#                                                              'Кредит_начало',
#                                                              'Дебет_оборот',
#                                                              'Кредит_оборот',
#                                                              'Дебет_конец',
#                                                              'Кредит_конец',
#                                                              ] if col in df.columns]
#         desired_order = desired_order_not_with_suff_do_ko.copy()
        

#         # Находим столбцы в таблице, заканчивающиеся на '_до' и '_ко'
#         do_ko_columns = df.filter(regex='(_до|_ко)$').columns.tolist()

#         # Добавим столбцы, заканчивающиеся на '_до' и '_ко', в таблицу
#         if do_ko_columns:
#             desired_order += do_ko_columns
            
#         if 'Показа-\nтели' in df.columns and df['Показа-\nтели'].str.startswith('Кол.').any():
#             for i in desired_order:
#                 df[f'Количество_{i}'] = df[i].shift(-1)
        
#         if 'Показа-\nтели' in df.columns and df['Показа-\nтели'].str.startswith('Вал.').any():
#             for i in desired_order:
#                 df[f'ВалютнаяСумма_{i}'] = df[i].shift(-1)
        
#         # if df['Счет'].isin(['Валютная сумма']).any() or 'Показа-\nтели' in df.columns:
#         #     if df['Счет'].str.startswith('Валюта').any():
#         #         df['Валюта'] = df['Счет'].shift(-1)
#         #     for i in desired_order:
#         #         df[f'ВалютнаяСумма_{i}'] = df[i].shift(-2)
        
        
        
#         # Очистим таблицу от строк с количеством и валютой
#         if 'Показа-\nтели' in df.columns:
#             mask = (
#                 (df['Показа-\nтели'] == 'Кол.') |
#                 (df['Показа-\nтели'] == 'Вал.') |
#                 (df['Показа-\nтели'].str.startswith('Валюта'))
#             )
#             df = df[~mask]
        
#         '''Сохраняем данные по оборотам до обработки в таблицах'''
        
#         if df[df['Счет'] == 'Итого'][desired_order].empty:
#             raise RegisterProcessingError
            
#         df_for_check = df[df['Счет'] == 'Итого'][['Счет'] + desired_order_not_with_suff_do_ko].copy().tail(2).iloc[[0]]
#         df_for_check[desired_order_not_with_suff_do_ko] = df_for_check[desired_order_not_with_suff_do_ko].astype(float).fillna(0)
        
        
#         # Списки необходимых столбцов для каждой новой колонки
#         start_debit = 'Дебет_начало'
#         start_credit = 'Кредит_начало'
#         end_debit = 'Дебет_конец'
#         end_credit = 'Кредит_конец'
#         debit_turnover = 'Дебет_оборот'
#         credit_turnover = 'Кредит_оборот'
        
#         # Функция для безопасного получения столбца или Series из нулей, если столбца нет
#         def get_col_or_zeros(df, col):
#             if col in df.columns:
#                 return df[col]
#             else:
#                 return 0
        
#         # Создаем новые столбцы с проверкой наличия исходных
#         df_for_check['Сальдо_начало_до_обработки'] = get_col_or_zeros(df_for_check, start_debit) - get_col_or_zeros(df_for_check, start_credit)
#         df_for_check['Сальдо_конец_до_обработки'] = get_col_or_zeros(df_for_check, end_debit) - get_col_or_zeros(df_for_check, end_credit)
#         df_for_check['Оборот_до_обработки'] = get_col_or_zeros(df_for_check, debit_turnover) - get_col_or_zeros(df_for_check, credit_turnover)
#         df_for_check = df_for_check[['Сальдо_начало_до_обработки', 'Сальдо_конец_до_обработки', 'Оборот_до_обработки']].reset_index()

    
#         ''' После разнесения строк в плоский вид, в таблице остаются строки с дублирующими оборотами.
#         Например, итоговые обороты, итоги по субконто и т.д. Удаляем.'''
        
#         max_level = df['Уровень'].max()
#         conditions = []
        
#         for i in range(max_level):
#             condition = (
#                 (df['Уровень'] == i) & 
#                 (df['Счет'] == df[f'Level_{i}']) & 
#                 (df['Уровень'].shift(-1) > i)
#             )
#             conditions.append(condition)
        
#         # Объединяем все условия
#         mask = pd.concat(conditions, axis=1).any(axis=1)
#         df = df[~mask]

        
#         # Удаляем строки, содержащие значения из списка exclude_values
#         df = df[~df['Счет'].isin(exclude_values)]
        

#         df = df.rename(columns={'Счет': 'Субконто'})
#         df.drop('Уровень', axis=1, inplace=True)

#         # отберем только те строки, в которых хотя бы в одном из столбцов, определенных в existing_columns, есть непропущенные значения (не NaN)
#         df = df[df[desired_order].notna().any(axis=1)]
        
#         if 'Показа-\nтели' in df.columns:
#             df = df.drop(columns=['Показа-\nтели'])
#         if 'Курсив' in df.columns:
#             df = df.drop(columns=['Курсив'])
            
#         '''
#         Выровняем столбцы так, чтобы счета оказались в одном столбце без аналитики и субконто,
#         затем обновим значения столбца Субсчет (сейчас в нем счета), включив в него именно субсчета.
#         '''
        
#         df = self.shiftable_level(df)
        
        
#         """
#         Добавляет к таблице с оборотами до обработки, созданной выше,
#         данные по оборотам после обработки и отклонениями между ними.
#         """
        
#         # Вычисление итоговых значений - свернутые значения сальдо и оборотов - обработанных таблиц
#         df_check_after_process = pd.DataFrame({
#             'Сальдо_начало_после_обработки': [get_col_or_zeros(df, start_debit).sum() - get_col_or_zeros(df, start_credit).sum()],
#             'Оборот_после_обработки': [get_col_or_zeros(df, debit_turnover).sum() - get_col_or_zeros(df, credit_turnover).sum()],
#             'Сальдо_конец_после_обработки': [get_col_or_zeros(df, end_debit).sum() - get_col_or_zeros(df, end_credit).sum()]
#         })

#         # Объединение таблиц - обороты до и после обработки таблиц
#         pivot_df_check = pd.concat([df_for_check, df_check_after_process], axis=1).fillna(0)

#         # Вычисление отклонений в данных до и после обработки таблиц
#         for field in ['Сальдо_начало_разница', 'Оборот_разница', 'Сальдо_конец_разница']:
#             pivot_df_check[field] = (pivot_df_check[field.replace('_разница', '_до_обработки')] -
#                                       pivot_df_check[field.replace('_разница', '_после_обработки')]).round()

#         # Помечаем данные именем файла
#         pivot_df_check['Исх.файл'] = file_path.name
        
        
        

#         # Запись таблицы в хранилище таблиц
#         self.table_for_check = pivot_df_check
        
#         return df, self.table_for_check
        
        
        
# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

import re
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional

from register_processors.class_processor import FileProcessor, exclude_values
from custom_errors import RegisterProcessingError
from support_functions import fix_1c_excel_case


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


def get_col_or_zeros(df: pd.DataFrame, col: str):
    """Безопасное получение столбца или Series из нулей, если столбца нет"""
    return df[col] if col in df.columns else 0


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


class BaseAccountOSVProcessor(FileProcessor):
    """Базовый обработчик для Анализа счета 1С"""
    
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
        max_rows_to_check = min(max_rows, df.shape[0])
        
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if search_value in col_values.values:
                # Находим индекс строки с точным совпадением
                mask = col_values == search_value
                if mask.any():
                    row_idx = mask.idxmax()
                    return col_idx, row_idx
        
        return None, None
    
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
            check_columns = ['Дебет_начало', 'Кредит_начало', 'Дебет_оборот', 
                           'Кредит_оборот', 'Дебет_конец', 'Кредит_конец']
            check_columns = [col for col in check_columns if col in df.columns]
            
            if df[check_columns].sum().sum() == 0:
                raise RegisterProcessingError('ОСВ счета вероятно пустая.')
            else:
                df.loc[:, 'Уровень'] = 1
    
    def _validate_level_columns(self, df: pd.DataFrame) -> None:
        """Проверка наличия пустых значений в столбцах Уровень и Курсив"""
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterProcessingError('Найдены пустые значения в столбцах Уровень или Курсив.')
    
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
    
    def _spread_vertical_data(self, df: pd.DataFrame, account_col: str) -> pd.DataFrame:
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
        for level in range(max_level + 1):
            level_mask = df['Уровень'] == level
            df[f'Level_{level}'] = df[account_col].where(level_mask)
            df[f'Level_{level}'] = df[f'Level_{level}'].ffill()
            
            higher_level_mask = df['Уровень'] < level
            df.loc[higher_level_mask, f'Level_{level}'] = None
        
        df.loc[df[quantity_mask].index, account_col] = 'Количество'
        
        return df, max_level
    
    def _handle_quantity_currency_data(self, df: pd.DataFrame, desired_order: list) -> pd.DataFrame:
        """Обработка количественных и валютных данных"""
        if 'Показа-\nтели' in df.columns:
            # Обработка количественных данных
            if df['Показа-\nтели'].str.startswith('Кол.').any():
                for col in desired_order:
                    df[f'Количество_{col}'] = df[col].shift(-1)
            
            # Обработка валютных данных
            if df['Показа-\nтели'].str.startswith('Вал.').any():
                for col in desired_order:
                    df[f'ВалютнаяСумма_{col}'] = df[col].shift(-1)
            
            # Очистка от строк с количеством и валютой
            mask = (
                (df['Показа-\nтели'] == 'Кол.') |
                (df['Показа-\nтели'] == 'Вал.') |
                (df['Показа-\nтели'].str.startswith('Валюта'))
            )
            df = df[~mask]
        
        return df
    
    def _remove_duplicate_rows(self, df: pd.DataFrame, account_col: str, max_level: int) -> pd.DataFrame:
        """Удаление дублирующихся строк"""
        conditions = []
        
        for i in range(max_level):
            condition = (
                (df['Уровень'] == i) & 
                (df[account_col] == df[f'Level_{i}']) & 
                (df['Уровень'].shift(-1) > i)
            )
            conditions.append(condition)
        
        mask = pd.concat(conditions, axis=1).any(axis=1)
        df = df[~mask]
        
        return df
    
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
    
    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        """Поиск шапки таблицы, переименование заголовков, очистка"""
        df = BaseAccountOSVProcessor._clean_dataframe(df)
        
        processor = AccountOSV_UPPFileProcessor()
        col_idx, header_row_idx = processor._find_header_column(df, 'субконто')
        
        if col_idx is None or header_row_idx is None:
            raise RegisterProcessingError('Не найден столбец с "Субконто" в первых 30 строках.')
        
        # Проверка на наличие значения "Субконто"
        first_col = df.iloc[:, col_idx].astype(str)
        if not (first_col == 'Субконто').any():
            raise RegisterProcessingError('Файл не является ОСВ счета 1с.')
        
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
                    df[f'Количество_{col}'] = df[col].shift(-1)
            
            # Создание столбцов с валютой
            if df['Субконто'].isin(['Валютная сумма']).any() or 'Показа-\nтели' in df.columns:
                if df['Субконто'].str.startswith('Валюта').any():
                    df['Валюта'] = df['Субконто'].shift(-1)
                for col in desired_order:
                    df[f'ВалютнаяСумма_{col}'] = df[col].shift(-2)
            
            # Очистка от строк с количеством и валютой
            mask = (
                (df['Субконто'] == 'Количество') |
                (df['Субконто'] == 'Валютная сумма') |
                (df['Субконто'].str.startswith('Валюта'))
            )
            df = df[~mask]
        

        
        # Создание контрольной таблицы
        if df[df['Субконто'] == 'Итого'][desired_order].empty:
            raise RegisterProcessingError('Нет значений по строке Итого')
        
        df_for_check = df[df['Субконто'] == 'Итого'][['Субконто'] + desired_order_not_with_suff].copy().tail(2).iloc[[0]]
        df_for_check[desired_order_not_with_suff] = df_for_check[desired_order_not_with_suff].astype(float).fillna(0)
        df_for_check = calculate_check_values(df_for_check, self._columns_config)

        # Удаление дублирующихся строк
        df = self._remove_duplicate_rows(df, 'Субконто', max_level)
        
        

        
        
        
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
    
    @staticmethod
    def _process_dataframe_optimized(df: pd.DataFrame) -> pd.DataFrame:
        """Поиск шапки таблицы, переименование заголовков, очистка"""
        df = BaseAccountOSVProcessor._clean_dataframe(df)
        
        processor = AccountOSV_NonUPPFileProcessor()
        col_idx, header_row_idx = processor._find_header_column(df, 'счет')
        
        if col_idx is None or header_row_idx is None:
            raise RegisterProcessingError('Не найден столбец с "Счет" в первых 30 строках.')
        
        # Проверка на наличие значения "Счет"
        first_col = df.iloc[:, col_idx].astype(str)
        if not (first_col == 'Счет').any():
            raise RegisterProcessingError('Файл не является ОСВ счета 1с.')
        
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
            raise RegisterProcessingError('Нет значений по строке Итого')
        
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
        
        return df, self.table_for_check