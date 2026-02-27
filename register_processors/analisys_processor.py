# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List

from register_processors.class_processor import FileProcessor, exclude_values
from custom_errors import RegisterProcessingError
from support_functions import fix_1c_excel_case, get_accounts_without_subaccount


def top_level_accounts(d: Dict[str, int]) -> List[str]:
    """
    Возвращает список счетов верхнего уровня, для которых:
      – либо хотя бы один их субсчёт (любой уровень вложенности) имеет value > 1,
      – либо у счёта нет субсчетов и его собственное value > 1.
    """
    # 1. Какие счета вообще имеют value > 1
    with_value_gt1 = {acc for acc, val in d.items() if val > 1}

    # 2. Для каждого верхнего счёта проверим условие
    result = []
    for acc in d:
        # берём только «голые» счета (без точек)
        if '.' in acc:
            continue

        # есть ли субсчета с value > 1
        has_sub = any(sub in with_value_gt1 for sub in d if sub.startswith(acc + '.'))

        # либо субсчета есть и хотя бы один > 1, либо сам счёт > 1
        if has_sub or acc in with_value_gt1:
            result.append(acc)

    return sorted(result)

class Analisys_UPPFileProcessor(FileProcessor):
    """Обработчик для Анализа счета 1С УПП"""
    def __init__(self):
        super().__init__()
        self.df_type_connection = pd.DataFrame()  # хранение данных анализа счета с полем Вид связи КА за период
        self.used_indices = set() # Множество для отслеживания использованных индексов
        self.counter_clean_used_indices = 0 # счетчик количества очисток used_indices

    def find_sum_indices(self, df, column_name, tolerance=0.005):
        """
        Находит индексы значений, для которых можно подобрать сумму из следующих значений.
        
        Parameters:
        df (pd.DataFrame): Входной DataFrame
        column_name (str): Имя столбца для анализа
        tolerance (float): Допустимая погрешность для сравнения сумм
        
        Returns:
        list: Список индексов, для которых найдена подходящая сумма
        """
        result_indices = []
        
        # Преобразуем столбец в массив, фильтруя NaN значения
        values = df[column_name].dropna().values
        indices = df[column_name].dropna().index
        
        # Проходим по всем значениям кроме последнего
        for i in range(len(values) - 1):
            target_value = values[i]
            current_sum = 0.0
            
            # Проходим по всем следующим значениям
            for j in range(i + 1, len(values)):
                current_sum += values[j]
                
                # Проверяем, попадает ли сумма в диапазон с учетом погрешности
                if abs(current_sum - target_value) <= tolerance:
                    result_indices.append(indices[i])
                    break
                # Если сумма превысила целевое значение с запасом, выходим
                elif current_sum > target_value + tolerance:
                    break
        
        return result_indices
    
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
        
        # удалим пустые строки и столбцы
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)
        
        # Ищем столбец, содержащий "Счет" в первых 30 строках (или меньше, если строк меньше)
        subkonto_col_idx = None
        max_rows_to_check = min(MAX_HEADER_ROWS, df.shape[0])  # Проверяем не больше 30 строк
        
        for col_idx in range(df.shape[1]):
            col_values = df.iloc[:max_rows_to_check, col_idx].astype(str).str.strip().str.lower()
            if 'счет' in col_values.values:
                subkonto_col_idx = col_idx
                break
        
        if subkonto_col_idx is None:
            raise RegisterProcessingError('Не найден столбец с "Счет" в первых 30 строках.')
        
        # Теперь используем найденный столбец
        first_col = df.iloc[:, subkonto_col_idx].astype(str)
        mask = first_col == 'Счет'
        
        # ошибка, если нет искомого значения
        if not mask.any():
            raise RegisterProcessingError('Файл не является Анализом счета 1с.')
        
        # индекс строки с искомым словом
        date_row_idx = mask.idxmax()
    
        # Установка заголовков и очистка
        df.columns = df.iloc[date_row_idx]
        df = df.iloc[date_row_idx + 1:].copy()
    
        # Переименуем столбцы, в которых находятся уровни и признак курсива
        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
    
        # удалим столбцы с пустыми заголовками
        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)
    
        # если отсутствует иерархия (+ и - на полях excel файла), значит он пуст
        if df['Уровень'].max() == 0:
            raise RegisterProcessingError('Анализ счета пустой.')
    
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
        
        # Если выгрузка с валютными данными, переименуем столбцы с валютой
        cols = df.columns.tolist()
        duplicates = ['С кред. счетов', 'В дебет счетов']
        
        for dup_name in duplicates:
            # Найти все индексы столбцов с этим именем
            indices = [i for i, col in enumerate(cols) if col == dup_name]
            # Переименовать второй и последующие
            for count, idx in enumerate(indices[1:], start=2):
                cols[idx] = f"{dup_name}_ВАЛ"
        df.columns = cols
        
        # Найти столбцы, заканчивающиеся на '_ВАЛ'
        # Если выгрузка валютная, промежуточные суммирующие строки могут быть не курсивные
        # и скипт не удалит такие строки, что приведет к ошибкам при формировании
        # плоской таблицы. В этом случае ставим флаг того, что в случае таких ошибок
        # скрипт попытается определить такие строки подбором значений
        # с помощью функции find_sum_indices
        use_find_sum_indices = False
        columns_ending_with_VAL = df.columns[df.columns.str.endswith('_ВАЛ')]
        
        # Проверить, есть ли такие столбцы
        if not columns_ending_with_VAL.empty:
            use_find_sum_indices = True
       
        # Сохраним столбец "Вид связи КА" в отдельный фрейм
        if 'Вид связи КА за период' in df.columns and 'Счет' in df.columns:
            df_type_connection = (
                df
                .drop_duplicates(subset=['Счет', 'Вид связи КА за период'])
                .dropna(subset=['Счет', 'Вид связи КА за период'])
                .loc[:, ['Счет', 'Вид связи КА за период']]
            )
            self.df_type_connection = df_type_connection
    
        # Оптимизация: один раз вычисляем is_valid_account
        kor_schet = df['Кор.счет'].astype(str)
        is_valid_account = self._is_accounting_code_vectorized(kor_schet)
        
    
        # Оптимизированное условие для mask
        mask = df['Счет'].isna()
        mask = mask & ~is_valid_account
        mask = mask & (kor_schet != 'Кол-во:')
        mask = mask & kor_schet.isin(exclude_values)
        
        # df.to_excel('Косячный.xlsx')
        # mask.to_excel('Косячный_mask.xlsx')
        
        # Заполнение пропусков
        df['Счет'] = np.where(mask, 'Не_заполнено', df['Счет'])
        df['Счет'] = df['Счет'].ffill().astype(str)
    
        # Добавляем '0' для счетов до 10
        account_is_valid = self._is_accounting_code_vectorized(df['Счет'])
        mask_pad = (df['Счет'].str.len() == 1) & account_is_valid
        df.loc[mask_pad, 'Счет'] = '0' + df.loc[mask_pad, 'Счет']
        

        '''Разносим вертикальные данные в горизонтальные'''
        
        max_level = df['Уровень'].max()
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
    
        # Создаем столбец Корр_счет
        df['Корр_счет'] = np.where(is_valid_account, kor_schet, None)
        
        # Добавляем '0' для корр. счетов - исправляем проблему типов
        korr_schet_str = df['Корр_счет'].astype(str)
        single_digit_mask = df['Корр_счет'].notna() & (korr_schet_str.str.len() == 1)
        
        # Используем .loc с явным приведением типов
        korr_single_digit = df.loc[single_digit_mask, 'Корр_счет']
        if not korr_single_digit.empty:
            df.loc[single_digit_mask, 'Корр_счет'] = '0' + korr_single_digit.astype(str)
        
        # Заполняем пропущенные значения корр.счетом сверху вниз
        df['Корр_счет'] = df['Корр_счет'].ffill()
        
        
        
        
        '''Сохраним данные по оборотам до обработки в разрезе корр счетов для проверки с данными после обработки'''
        
        df_for_check = df[['Кор.счет', 'С кред. счетов', 'В дебет счетов']].copy()

        # Векторизированная маска
        valid_mask = self._is_accounting_code_vectorized(df_for_check['Кор.счет']) | (df_for_check['Кор.счет'].astype(str) == "0")
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет'].astype(str).where(valid_mask)
        
        # Удаление NaN и заполнение пустыми строками
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].dropna().fillna('')
        
        # Векторизированное добавление '0' к односимвольным значениям
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].str.zfill(2)

        # Фильтрация
        if '94.Н' in df_for_check['Кор.счет_ЧЕК'].values:
            # Создаем регулярное выражение для поиска значений, начинающихся на "94." и имеющих 2 символа после точки
            pattern = r'^94\..{2}$'
            
            # Фильтруем данные
            df_for_check = df_for_check[
                (df_for_check['Кор.счет_ЧЕК'] == '94.Н') | 
                ~(
                    df_for_check['Кор.счет_ЧЕК'].isin(['94']) |  # Точное значение "94"
                    df_for_check['Кор.счет_ЧЕК'].str.match(pattern, na=False)  # Значения по шаблону "94.XX"
                )
            ]
        else:
            # Удаляем строки с NaN перед фильтрацией
            df_for_check = df_for_check[df_for_check['Кор.счет_ЧЕК'].notna()]
        
        
        # Группировка и сброс индекса
        df_for_check = df_for_check.groupby('Кор.счет_ЧЕК')[['С кред. счетов', 'В дебет счетов']].sum().reset_index()
                
        # Счета с субсчетами
        all_acc_dict = df_for_check['Кор.счет_ЧЕК'].value_counts().to_dict()
        acc_with_sub = [i for i in all_acc_dict if self._is_parent(i, list(all_acc_dict.keys()))]
        clean_acc = [i for i in all_acc_dict if all_acc_dict[i] == 1 and i not in acc_with_sub]
        del_acc = set(all_acc_dict.keys()) - set(clean_acc)
        df_for_check = df_for_check[~df_for_check['Кор.счет_ЧЕК'].isin(del_acc)].copy()
        
        # Удаление субсчетов для счетов без субсчетов
        accounts_without_subaccount = get_accounts_without_subaccount()
        
        # Функция для преобразования счета
        def transform_account(account):
            # Разделяем счет по точке и берем первую часть
            account_parts = str(account).split('.')
            main_account = account_parts[0]
            
            # Если основная часть счета есть в списке для преобразования
            if main_account in accounts_without_subaccount:
                return main_account
            else:
                return account
        
        # Применяем преобразование к столбцу Кор.счет_ЧЕК
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].apply(transform_account)
        
        # Агрегируем данные суммированием
        df_for_check = df_for_check.groupby('Кор.счет_ЧЕК', as_index=False).agg({
            'С кред. счетов': 'sum',
            'В дебет счетов': 'sum'
        })
        
        # Сохраним для дальнейшего использования
        self.table_for_check = df_for_check
        


        '''Удаляем лишние строки (суммирующие, со счетами, которые расшифрованы по субсчетам и тд)'''
        
        df_delete = df[~df['Кор.счет'].isin(exclude_values)].dropna(subset=['Кор.счет'])
        df_delete = df_delete[df_delete['Курсив'] == 0][['Кор.счет', 'Корр_счет']]
        unique_df = df_delete.drop_duplicates(subset=['Кор.счет', 'Корр_счет']).dropna(subset=['Корр_счет'])
        
        # Подсчет уникальных значений
        all_acc_dict = unique_df['Корр_счет'].value_counts().to_dict()
        
        # Счета с субсчетами
        acc_with_sub = [i for i in all_acc_dict if self._is_parent(i, list(all_acc_dict.keys()))]
        
        clean_acc = [i for i in all_acc_dict if all_acc_dict[i] == 1 and i not in acc_with_sub]
        
        del_acc = set(all_acc_dict.keys()) - set(clean_acc)
        
        # Обработка счетов 94
        acc_with_94 = [i for i in all_acc_dict if '94' in i]
        if '94.Н' in acc_with_94:
            del_acc.update(i for i in acc_with_94 if i != '94.Н')
        
        # Добавление счетов без нулей
        accounts_without_zeros = []
        for item in del_acc:
            if isinstance(item, str) and item.isdigit() and item.startswith('0') and len(item) > 1:
                accounts_without_zeros.append(str(int(item)))
        del_acc.update(accounts_without_zeros)
        
        # Преобразование типов и фильтрация
        df['Кор.счет'] = df['Кор.счет'].astype(str)
        
        # Обработка колонок с количеством
        values_with_quantity = False
        if df['Кор.счет'].isin(['Кол-во:']).any():
            df['С кред. счетов_КОЛ'] = df['С кред. счетов'].shift(-1)
            df['В дебет счетов_КОЛ'] = df['В дебет счетов'].shift(-1)
            values_with_quantity = True
        
        # Заполнение пропущенных значений в столбце Вид_связи
        if 'Вид связи КА за период' in df.columns:
            merged = df.merge(self.df_type_connection, on='Счет', how='left', suffixes=('', '_B'))
            df['Вид связи КА за период'] = df['Вид связи КА за период'].fillna(merged['Вид связи КА за период_B'])

        # Основная фильтрация
        df = df[
            ~df['Кор.счет'].isin(exclude_values) & 
            ~df['Кор.счет'].isin(del_acc) &
            (df['Курсив'] == 0)
        ].copy()
        
        # если в пользовательских настройках коррсчетов, по которым не нужны субсчета, указаны
        # счета, по которым есть расшифровка субконто, то в финальном df нужно заменить субсчета на счета
        # т.е., например, 60.01 или 60.02 будут заменены на 60
        
        # Получаем основную часть счета (до точки)
        main_account = df['Корр_счет'].str.split('.').str[0]
        
        # Создаем маску: где основная часть есть в списке
        mask = main_account.isin(accounts_without_subaccount)
        
        # Заменяем значения по маске
        df.loc[mask, 'Корр_счет'] = main_account[mask]
        
        shiftable_level = 'Level_0'
        list_level_col = [i for i in df.columns.to_list() if i.startswith('Level')]
        for i in list_level_col[::-1]:
            if all(self._is_accounting_code_vectorized(df[i])):
                shiftable_level = i
                break
        
        # Создание столбца Субсчет
        df['Субсчет'] = np.where(
                                df[shiftable_level].astype(str) != '7',
                                df[shiftable_level].astype(str),
                                '0' + df[shiftable_level].astype(str)
                            )
        mask = self._is_accounting_code_vectorized(df['Субсчет'])
        df['Субсчет'] = np.where(mask, df['Субсчет'], 'Без_субсчетов')
        
        # Переименование столбцов
        df = df.rename(columns={
            'Кор.счет': 'Субконто_корр_счета',
            'Счет': 'Аналитика'
        })
        
        
        
        # Указание порядка столбцов
        desired_order = ['Исх.файл', 'Субсчет', 'Аналитика', 'Вид связи КА за период', 
                         'Корр_счет', 'Субконто_корр_счета', 'С кред. счетов', 'С кред. счетов_ВАЛ', 'В дебет счетов', 'В дебет счетов_ВАЛ']
        
        if values_with_quantity:
            desired_order = ['Исх.файл', 'Субсчет', 'Аналитика', 'Вид связи КА за период', 
                            'Корр_счет', 'Субконто_корр_счета', 'С кред. счетов', 'С кред. счетов_КОЛ', 'С кред. счетов_ВАЛ',
                            'В дебет счетов', 'В дебет счетов_КОЛ', 'В дебет счетов_ВАЛ']
        
        # Добавление level колонок
        level_columns = [col for col in df.columns if 'Level_' in col]
        new_order = [col for col in desired_order if col in df.columns] + level_columns
        

        
        df = df[new_order]
        
        # Финальная обработка
        mask = self._is_accounting_code_vectorized(df['Субконто_корр_счета'])
        df['Субконто_корр_счета'] = np.where(mask, 'Не расшифровано', df['Субконто_корр_счета'])
        
        # Удаление пустых строк
        df = df.dropna(subset=['С кред. счетов', 'В дебет счетов'], how='all')
        df = df[(df['С кред. счетов'] != 0) | (df['С кред. счетов'].notna())]
        df = df[(df['В дебет счетов'] != 0) | (df['В дебет счетов'].notna())]
        
        
        
        '''Подбиваем обороты в разрезе корр. субсчетов и считаем разницы с оборотами до обработки'''
        
        df_for_check = self.table_for_check
        
        def reconciliation_interim_results(df: pd.DataFrame,
                                           ind_del_list: set,
                                           last_general_deviation: float,
                                           df_before_changes: pd.DataFrame,
                                           diff_col,
                                           data_col,
                                           corr_account) -> set:
            indices_to_remove = set()

            # Преобразуем в список и сортируем для детерминированного порядка
            sorted_ind_del_list = sorted(ind_del_list)

            for ind_del in sorted_ind_del_list:
                # Создаем временный набор с потенциально новым индексом
                temp_to_remove = indices_to_remove | {ind_del}

                # Удаляем строки и обрабатываем данные
                df_after_changes = df[['Корр_счет', 'С кред. счетов', 'В дебет счетов']].drop(temp_to_remove, errors='ignore').copy()

                df_after_changes['Кор.счет_ЧЕК'] = df_after_changes.loc[:, 'Корр_счет']

                # Группировка
                df_after_changes = df_after_changes.groupby('Кор.счет_ЧЕК', as_index=False).agg({
                    'С кред. счетов': 'sum',
                    'В дебет счетов': 'sum'
                })
                
                # Объединяем и обрабатываем данные
                merged_df = df_before_changes[df_before_changes['Кор.счет_ЧЕК']==corr_account].merge(
                    df_after_changes[df_after_changes['Кор.счет_ЧЕК']==corr_account], 
                    on='Кор.счет_ЧЕК', 
                    how='outer',
                    suffixes=('_base', '_current')
                ).fillna(0)

                
                # Приводим столбцы к числовому типу
                numeric_cols = ['С кред. счетов_base', 'В дебет счетов_base', 
                               'С кред. счетов_current', 'В дебет счетов_current']
                merged_df[numeric_cols] = merged_df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
                
                # Вычисляем разницы
                merged_df[diff_col] = abs((merged_df[f'{data_col}_base'] - 
                                                merged_df[f'{data_col}_current'])).round()
                
                current_general_deviation = abs(merged_df[diff_col].sum())
                
                if current_general_deviation <= last_general_deviation:
                    last_general_deviation = current_general_deviation
                    indices_to_remove.add(ind_del)

                if 0<=last_general_deviation<=1:
                    break
            return indices_to_remove
            
        for _ in range(3):
            # Векторизованное создание колонки
            df_for_check_2 = df[['Корр_счет', 'С кред. счетов', 'В дебет счетов']].copy()
            df_for_check_2['Кор.счет_ЧЕК'] = df_for_check_2.loc[:,'Корр_счет']
            
            # Группировка
            df_for_check_2 = df_for_check_2.groupby('Кор.счет_ЧЕК', as_index=False).agg({
                'С кред. счетов': 'sum',
                'В дебет счетов': 'sum'
            })
            
            # Объединяем и обрабатываем данные
            merged_df = df_for_check.merge(
                df_for_check_2, 
                on='Кор.счет_ЧЕК', 
                how='outer',
                suffixes=('_base', '_current')
            ).fillna(0)
            
            # Приводим столбцы к числовому типу более эффективно
            numeric_cols = ['С кред. счетов_base', 'В дебет счетов_base', 
                           'С кред. счетов_current', 'В дебет счетов_current']
            merged_df[numeric_cols] = merged_df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
            
            # Вычисляем разницы
            merged_df['Разница_С_кред'] = (merged_df['С кред. счетов_base'] - 
                                            merged_df['С кред. счетов_current']).round()
            merged_df['Разница_В_дебет'] = (merged_df['В дебет счетов_base'] - 
                                             merged_df['В дебет счетов_current']).round()
            
            # Добавляем информацию о файле
            merged_df['Исх.файл'] = self.file
            
            # Объединенная обработка для кредита и дебета
            if use_find_sum_indices:
                indices_to_remove = set()
                
                # Обрабатываем оба типа расхождений в одном цикле
                for diff_col, data_col in [('Разница_С_кред', 'С кред. счетов'), 
                                          ('Разница_В_дебет', 'В дебет счетов')]:
                    
                    if merged_df[diff_col].sum() != 0:
                        problem_accounts = merged_df.loc[merged_df[diff_col] != 0, 'Кор.счет_ЧЕК'].tolist()
                        
                        for account_prefix in problem_accounts:
                            # Находим все уникальные корр_счета с данным префиксом
                            mask = df['Корр_счет'].astype(str).str.startswith(account_prefix)
                            unique_corr_accounts = df.loc[mask, 'Корр_счет'].unique()
                            
                            for corr_account in unique_corr_accounts:
                                # Фильтруем и находим индексы для удаления
                                filtered_df = df[(df['Корр_счет'] == corr_account) & 
                                               (df['Субконто_корр_счета'] != "Не расшифровано")]
                                
                                indices_to_remove.update(self.find_sum_indices(filtered_df, data_col))
                                
                
                                last_general_deviation = abs(merged_df.loc[merged_df['Кор.счет_ЧЕК'] == corr_account, diff_col].sum())

                                if indices_to_remove:
                                    final_ind_to_remove = reconciliation_interim_results(df,
                                                                                         indices_to_remove,
                                                                                         last_general_deviation,
                                                                                         df_for_check,
                                                                                         diff_col,
                                                                                         data_col,
                                                                                         corr_account)
                                    indices_to_remove.clear()
                                    if final_ind_to_remove:
                                        df = df.drop(final_ind_to_remove, errors='ignore')
        
        # Сохраняем результат
        self.table_for_check = merged_df
        
        
        '''
        Выровняем столбцы так, чтобы счета оказались в одном столбце без аналитики и субконто,
        затем обновим значения столбца Субсчет (сейчас в нем счета), включив в него именно субсчета
        '''
        
        df = self.shiftable_level(df)
        col_with_subacc = self.find_max_level_column(df)
        
        if col_with_subacc:
            df['Субсчет'] = df.loc[:, col_with_subacc]
        
        return df, self.table_for_check

        

class Analisys_NonUPPFileProcessor(FileProcessor):
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
            raise RegisterProcessingError('Не найден столбец с "Счет" в первых 30 строках.')
        
        # Теперь используем найденный столбец
        first_col = df.iloc[:, subkonto_col_idx].astype(str)
        mask = first_col == 'Счет'
        
        # ошибка, если нет искомого значения
        if not mask.any():
            raise RegisterProcessingError('Файл не является Анализом счета 1с.')
        
        # индекс строки с искомым словом
        date_row_idx = mask.idxmax()
    
        # Установка заголовков и очистка
        df.columns = df.iloc[date_row_idx]
        df = df.iloc[date_row_idx + 1:].copy()
    
        # Переименуем столбцы, в которых находятся уровни и признак курсива
        df.columns = ['Уровень', 'Курсив'] + df.columns[2:].tolist()
    
        # удалим столбцы с пустыми заголовками
        df = df.loc[:, df.columns.notna()]
        df.columns = df.columns.astype(str)
    
        # если отсутствует иерархия (+ и - на полях excel файла), значит он пуст
        if df['Уровень'].max() == 0:
            raise RegisterProcessingError('Анализ счета пустой.')
    
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
        
        # Сохраним столбец "Вид связи КА" в отдельный фрейм
        if 'Вид связи КА за период' in df.columns and 'Счет' in df.columns:
            df_type_connection = (
                df
                .drop_duplicates(subset=['Счет', 'Вид связи КА за период'])
                .dropna(subset=['Счет', 'Вид связи КА за период'])
                .loc[:, ['Счет', 'Вид связи КА за период']]
            )
            self.df_type_connection = df_type_connection
    
        # Оптимизация: один раз вычисляем is_valid_account
        kor_schet = df['Кор. Счет'].astype(str)
        is_valid_account = self._is_accounting_code_vectorized(kor_schet)
        

        # Оптимизированное условие для mask
        mask = df['Счет'].isna()
        mask = mask & ~is_valid_account
        mask = mask & (kor_schet != 'Кол-во:')
        mask = mask & kor_schet.isin(exclude_values)
    
        # Заполнение пропусков
        df['Счет'] = np.where(mask, 'Не_заполнено', df['Счет'])
        
        df['Счет'] = df['Счет'].ffill().astype(str)

        # Добавляем '0' для счетов до 10
        account_is_valid = self._is_accounting_code_vectorized(df['Счет'])
        mask_pad = (df['Счет'].str.len() == 1) & account_is_valid
        df.loc[mask_pad, 'Счет'] = '0' + df.loc[mask_pad, 'Счет']
        

        '''Разносим вертикальные данные в горизонтальные'''
        
        max_level = df['Уровень'].max()
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
        
        # Создаем столбец Корр_счет
        df['Корр_счет'] = np.where(is_valid_account, kor_schet, None)
        
        # Добавляем '0' для корр. счетов - исправляем проблему типов
        korr_schet_str = df['Корр_счет'].astype(str)
        single_digit_mask = df['Корр_счет'].notna() & (korr_schet_str.str.len() == 1)
        
        # Используем .loc с явным приведением типов
        korr_single_digit = df.loc[single_digit_mask, 'Корр_счет']
        if not korr_single_digit.empty:
            df.loc[single_digit_mask, 'Корр_счет'] = '0' + korr_single_digit.astype(str)
        
        # Заполняем пропущенные значения корр.счетом сверху вниз
        df['Корр_счет'] = df['Корр_счет'].ffill()
        
        
        '''Сохраним данные по оборотам до обработки в разрезе корр счетов для проверки с данными после обработки'''
        
        df_for_check = df[['Кор. Счет', 'Дебет', 'Кредит']].copy()

        # Векторизированная маска
        valid_mask = self._is_accounting_code_vectorized(df_for_check['Кор. Счет']) | (df_for_check['Кор. Счет'].astype(str) == "0")
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор. Счет'].astype(str).where(valid_mask)
        
        # Удаление NaN и заполнение пустыми строками
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].dropna().fillna('')
        
        # Векторизированное добавление '0' к односимвольным значениям
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].str.zfill(2)
        
        # Фильтрация
        if '94.Н' in df_for_check['Кор.счет_ЧЕК'].values:
            df_for_check = df_for_check[
                (df_for_check['Кор.счет_ЧЕК'] == '94.Н') | ~df_for_check['Кор.счет_ЧЕК'].isin(['94'])
            ]
        else:
            # Удаляем строки с NaN перед фильтрацией
            df_for_check = df_for_check[df_for_check['Кор.счет_ЧЕК'].notna()]
        
        # Замена '94.Н' на '94'
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].replace('94.Н', '94')
        
        # Группировка и сброс индекса
        df_for_check = df_for_check.groupby('Кор.счет_ЧЕК')[['Дебет', 'Кредит']].sum().reset_index()
        
        
        #------------------------------------------
        # Счета с субсчетами
        all_acc_dict = df_for_check['Кор.счет_ЧЕК'].value_counts().to_dict()
        acc_with_sub = [i for i in all_acc_dict if self._is_parent(i, list(all_acc_dict.keys()))]
        
        clean_acc = [i for i in all_acc_dict if all_acc_dict[i] == 1 and i not in acc_with_sub]
        
        del_acc = set(all_acc_dict.keys()) - set(clean_acc)
               
        df_for_check = df_for_check[~df_for_check['Кор.счет_ЧЕК'].isin(del_acc)].copy()
        
        # Удаление субсчетов для счетов без субсчетов
        accounts_without_subaccount = get_accounts_without_subaccount()

        # Функция для преобразования счета
        def transform_account(account):
            # Разделяем счет по точке и берем первую часть
            account_parts = str(account).split('.')
            main_account = account_parts[0]
            
            # Если основная часть счета есть в списке для преобразования
            if main_account in accounts_without_subaccount:
                return main_account
            else:
                return account

        # Применяем преобразование к столбцу Кор.счет_ЧЕК
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].apply(transform_account)

        # Агрегируем данные суммированием
        df_for_check = df_for_check.groupby('Кор.счет_ЧЕК', as_index=False).agg({
            'Дебет': 'sum',
            'Кредит': 'sum'
        })
        
        
        # Переименование столбцов для единообразия с УПП
        df_for_check = df_for_check.rename(columns={
            'Дебет': 'С кред. счетов',
            'Кредит': 'В дебет счетов'
        })
        
        # Сохраним для дальнейшего использования
        self.table_for_check = df_for_check
        
                
        '''Удаляем лишние строки (суммирующие, со счетами, которые расшифрованы по субсчетам и тд)'''
        
        df_delete = df[~df['Кор. Счет'].isin(exclude_values)].dropna(subset=['Кор. Счет'])
        df_delete = df_delete[df_delete['Курсив'] == 0][['Кор. Счет', 'Корр_счет']]
        unique_df = df_delete.drop_duplicates(subset=['Кор. Счет', 'Корр_счет']).dropna(subset=['Корр_счет'])
    
        # Подсчет уникальных значений
        all_acc_dict = unique_df['Корр_счет'].value_counts().to_dict()
        
        # Счета с субсчетами
        acc_with_sub = [i for i in all_acc_dict if self._is_parent(i, list(all_acc_dict.keys()))]
        clean_acc = [i for i in all_acc_dict if all_acc_dict[i] == 1 and i not in acc_with_sub]
        del_acc = set(all_acc_dict.keys()) - set(clean_acc)
        
        
        # Обработка счетов 94
        acc_with_94 = [i for i in all_acc_dict if '94' in i]
        if '94.Н' in acc_with_94:
            del_acc.update(i for i in acc_with_94 if i != '94.Н')
            
        # Добавление счетов без нулей
        accounts_without_zeros = []
        for item in del_acc:
            if isinstance(item, str) and item.isdigit() and item.startswith('0') and len(item) > 1:
                accounts_without_zeros.append(str(int(item)))
        del_acc.update(accounts_without_zeros)
        
        # Преобразование типов и фильтрация
        df['Кор. Счет'] = df['Кор. Счет'].astype(str)
        
        # Обработка колонок с количеством и валютой
        values_with_quantity, values_with_currency = False, False
        if 'Показа-\nтели' in df.columns:
            if df['Показа-\nтели'].isin(['Кол.']).any():
                df['С кред. счетов_КОЛ'] = df['Дебет'].shift(-1)
                df['В дебет счетов_КОЛ'] = df['Кредит'].shift(-1)
                df = df[~df['Показа-\nтели'].isin(['Кол.', 'Вал.'])].copy()
                values_with_quantity = True
            elif df['Показа-\nтели'].isin(['Вал.']).any():
                df['С кред. счетов_ВАЛ'] = df['Дебет'].shift(-1)
                df['В дебет счетов_ВАЛ'] = df['Кредит'].shift(-1)
                df = df[~df['Показа-\nтели'].isin(['Кол.', 'Вал.'])].copy()
                values_with_currency = True
        
        # Заполнение пропущенных значений в столбце Вид_связи
        if 'Вид связи КА за период' in df.columns:
            merged = df.merge(self.df_type_connection, on='Счет', how='left', suffixes=('', '_B'))
            df['Вид связи КА за период'] = df['Вид связи КА за период'].fillna(merged['Вид связи КА за период_B'])

        
        # Основная фильтрация
        df = df[
            ~df['Кор. Счет'].isin(exclude_values) & 
            ~df['Кор. Счет'].isin(del_acc) &
            (df['Курсив'] == 0)
        ].copy()
        
        
        # если в пользовательских настройках коррсчетов, по которым не нужны субсчета, указаны
        # счета, по которым есть расшифровка субконто, то в финальном df нужно заменить субсчета на счета
        # т.е., например, 60.01 или 60.02 будут заменены на 60
        
        # Получаем основную часть счета (до точки)
        main_account = df['Корр_счет'].str.split('.').str[0]
        
        # Создаем маску: где основная часть есть в списке
        mask = main_account.isin(accounts_without_subaccount)
        
        # Заменяем значения по маске
        df.loc[mask, 'Корр_счет'] = main_account[mask]
        
        shiftable_level = 'Level_0'
        list_level_col = [i for i in df.columns.to_list() if i.startswith('Level')]
        for i in list_level_col[::-1]:
            if all(self._is_accounting_code_vectorized(df[i])):
                shiftable_level = i
                break
        
        # Создание столбца Субсчет
        df['Субсчет'] = np.where(
                                df[shiftable_level].astype(str) != '7',
                                df[shiftable_level].astype(str),
                                '0' + df[shiftable_level].astype(str)
                            )
        mask = self._is_accounting_code_vectorized(df['Субсчет'])
        df['Субсчет'] = np.where(mask, df['Субсчет'], 'Без_субсчетов')
        
        # Переименование столбцов
        df = df.rename(columns={
            'Кор. Счет': 'Субконто_корр_счета',
            'Счет': 'Аналитика',
            'Дебет': 'С кред. счетов',
            'Кредит': 'В дебет счетов'
        })
        
        # Указание порядка столбцов
        desired_order = ['Исх.файл', 'Субсчет', 'Аналитика', 'Вид связи КА за период', 
                         'Корр_счет', 'Субконто_корр_счета', 'С кред. счетов', 'В дебет счетов']
        
        if values_with_quantity:
            desired_order = ['Исх.файл', 'Субсчет', 'Аналитика', 'Вид связи КА за период', 
                            'Корр_счет', 'Субконто_корр_счета', 'С кред. счетов', 'С кред. счетов_КОЛ', 
                            'В дебет счетов', 'В дебет счетов_КОЛ']
        if values_with_currency:
            desired_order = ['Исх.файл', 'Субсчет', 'Аналитика', 'Вид связи КА за период', 
                            'Корр_счет', 'Субконто_корр_счета', 'С кред. счетов', 'С кред. счетов_ВАЛ', 
                            'В дебет счетов', 'В дебет счетов_ВАЛ']
        
        # Добавление level колонок
        level_columns = [col for col in df.columns if 'Level_' in col]
        new_order = [col for col in desired_order if col in df.columns] + level_columns
        df = df[new_order]
        
        # Финальная обработка
        mask = self._is_accounting_code_vectorized(df['Субконто_корр_счета'])
        df['Субконто_корр_счета'] = np.where(mask, 'Не расшифровано', df['Субконто_корр_счета'])
        
        # Удаление пустых строк
        df = df.dropna(subset=['С кред. счетов', 'В дебет счетов'], how='all')
        df = df[(df['С кред. счетов'] != 0) | (df['С кред. счетов'].notna())]
        df = df[(df['В дебет счетов'] != 0) | (df['В дебет счетов'].notna())]
        
        
        '''Подбиваем обороты в разрезе корр. субсчетов и считаем разницы с оборотами до обработки'''
        
        df_for_check = self.table_for_check

        # Векторизованное создание колонки
        df_for_check_2 = df[['Корр_счет', 'С кред. счетов', 'В дебет счетов']].copy()

        df_for_check_2['Кор.счет_ЧЕК'] = df_for_check_2.loc[:,'Корр_счет']
        
        # Группировка
        df_for_check_2 = df_for_check_2.groupby('Кор.счет_ЧЕК', as_index=False).agg({
            'С кред. счетов': 'sum',
            'В дебет счетов': 'sum'
        })
    
        # Объединяем DataFrame
        merged_df = df_for_check.merge(
                df_for_check_2, 
                on='Кор.счет_ЧЕК', 
                how='outer',
                suffixes=('_base', '_current')
            ).fillna(0)

        
        # Приводим столбцы к числовому типу более эффективно
        numeric_cols = ['С кред. счетов_base', 'В дебет счетов_base', 
                       'С кред. счетов_current', 'В дебет счетов_current']
        merged_df[numeric_cols] = merged_df[numeric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
        
        # Вычисляем разницы
        merged_df['Разница_С_кред'] = (merged_df['С кред. счетов_base'] - 
                                        merged_df['С кред. счетов_current']).round()
        merged_df['Разница_В_дебет'] = (merged_df['В дебет счетов_base'] - 
                                         merged_df['В дебет счетов_current']).round()
        
        # Добавляем информацию о файле
        merged_df['Исх.файл'] = self.file
        
        # Сохраняем результат
        self.table_for_check = merged_df
        
        
        '''
        Выровняем столбцы так, чтобы счета оказались в одном столбце без аналитики и субконто,
        затем обновим значения столбца Субсчет (сейчас в нем счета), включив в него именно субсчета.
        '''
        
        df = self.shiftable_level(df)
        col_with_subacc = self.find_max_level_column(df)
        
        if col_with_subacc:
            df['Субсчет'] = df.loc[:, col_with_subacc]
        
        return df, self.table_for_check