# -*- coding: utf-8 -*-
"""
Created on Mon Aug 25 11:23:20 2025

@author: a.karabedyan
"""

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from flatregister.config import get_accounts_without_subaccount
from flatregister.dtypes import coerce_numeric, ensure_column_types
from flatregister.errors import RegisterEmptyError, RegisterNotRecognizedError, RegisterStructureError
from flatregister.excel_io import fix_1c_excel_case
from flatregister.hierarchy import (
    collapse_subaccount,
    find_word_column,
    hierarchy_drop_set,
    spread_levels,
)
from flatregister.registers.analysis_correction import find_sum_indices, reconciliation_interim_results
from flatregister.registers.base import FileProcessor, exclude_values
from flatregister.registers.spec import RegisterSpec


@dataclass(frozen=True)
class AnalisysSpec(RegisterSpec):
    """Различия между выгрузками анализа счёта УПП и не-УПП.

    Расхождения D4/D5 из «Открытых решений» не устраняются: они названы
    флагами спецификации и воспроизводятся как есть.
    """

    kor_account_column: str
    amount_credit_initial: str
    amount_debit_initial: str
    rename_columns: dict
    rename_amount_columns: dict
    duplicate_currency_columns: bool
    narrow_94_filter: bool
    collapse_94n_to_94: bool
    quantity_from_kor_marker: bool
    quantity_from_indicator: bool
    currency_from_indicator: bool
    always_include_currency: bool
    reconciliation_correction: bool


UPP_SPEC = AnalisysSpec(
    header_word='счет',
    header_exact='Счет',
    account_column='Счет',
    indicator_column='Показа-\nтели',
    structure_message='Не найден столбец с "Счет" в первых 30 строках.',
    recognized_hint='В выгрузке нет строки заголовков со словом «Счет».',
    empty_message='Анализ счета пустой.',
    kor_account_column='Кор.счет',
    amount_credit_initial='С кред. счетов',
    amount_debit_initial='В дебет счетов',
    rename_columns={'Кор.счет': 'Субконто_корр_счета', 'Счет': 'Аналитика'},
    rename_amount_columns={},
    duplicate_currency_columns=True,
    narrow_94_filter=True,
    collapse_94n_to_94=False,
    quantity_from_kor_marker=True,
    quantity_from_indicator=False,
    currency_from_indicator=False,
    always_include_currency=True,
    reconciliation_correction=True,
)

NON_UPP_SPEC = AnalisysSpec(
    header_word='счет',
    header_exact='Счет',
    account_column='Счет',
    indicator_column='Показа-\nтели',
    structure_message='Не найден столбец с "Счет" в первых 30 строках.',
    recognized_hint='В выгрузке нет строки заголовков со словом «Счет».',
    empty_message='Анализ счета пустой.',
    kor_account_column='Кор. Счет',
    amount_credit_initial='Дебет',
    amount_debit_initial='Кредит',
    rename_columns={'Кор. Счет': 'Субконто_корр_счета',
                    'Счет': 'Аналитика',
                    'Дебет': 'С кред. счетов',
                    'Кредит': 'В дебет счетов'},
    rename_amount_columns={'Дебет': 'С кред. счетов', 'Кредит': 'В дебет счетов'},
    duplicate_currency_columns=False,
    narrow_94_filter=False,
    collapse_94n_to_94=True,
    quantity_from_kor_marker=False,
    quantity_from_indicator=True,
    currency_from_indicator=True,
    always_include_currency=False,
    reconciliation_correction=False,
)


class AnalisysFileProcessor(FileProcessor):
    """Обработчик анализа счёта 1С; вариант задаётся :class:`AnalisysSpec`."""

    spec: AnalisysSpec

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
        FlatRegisterError
            Возникает, если обрабатываемый файл не является Анализом счёта
            или является пустым. Такой файл не обрабатывается, его имя
            попадает в отчёт о неудачных файлах в конце обработки.

        Returns
        -------
        df : pd.DataFrame
            Готовая к дальнейшей обработке таблица.

        '''

        # удалим пустые строки и столбцы
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)

        # Ищем столбец, содержащий "Счет" в первых 30 строках (или меньше, если строк меньше)
        subkonto_col_idx = find_word_column(df, 'счет')

        if subkonto_col_idx is None:
            raise RegisterStructureError(
                'Не найден столбец с "Счет" в первых 30 строках.',
                hint='Похоже, это не анализ счёта. Проверьте, что выбран тот тип '
                     'регистра, который соответствует выгрузке.')

        # Теперь используем найденный столбец
        first_col = df.iloc[:, subkonto_col_idx].astype(str)
        mask = first_col == 'Счет'

        # ошибка, если нет искомого значения
        if not mask.any():
            raise RegisterNotRecognizedError(
                'Файл не является Анализом счета 1с.',
                hint='В выгрузке нет строки заголовков со словом «Счет».')

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
            raise RegisterEmptyError(
                'Анализ счета пустой.',
                hint='В выгрузке нет ни одной строки с данными.')

        # Уровень и Курсив должны иметь 0 или 1, иначе - ошибка
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterStructureError(
                'Найдены пустые значения в столбцах Уровень или Курсив.',
                hint='В Excel не выставлены уровни группировки (знаки «+» и «−» слева). '
                     'Выгрузите регистр заново через меню «Сохранить как» в 1С.')

        return df

    def _build_check_frame(self, df: pd.DataFrame, df_for_check: pd.DataFrame) -> pd.DataFrame:
        """Сверка оборотов в разрезе корр. счетов: было / стало / разница."""
        df_for_check_2 = df[['Корр_счет', 'С кред. счетов', 'В дебет счетов']].copy()
        df_for_check_2['Кор.счет_ЧЕК'] = df_for_check_2.loc[:, 'Корр_счет']

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

        # Приводим столбцы к числовому типу
        numeric_cols = ['С кред. счетов_base', 'В дебет счетов_base',
                        'С кред. счетов_current', 'В дебет счетов_current']
        coerce_numeric(merged_df, numeric_cols, fill_zero=True)

        # Вычисляем разницы
        merged_df['Разница_С_кред'] = (merged_df['С кред. счетов_base'] -
                                       merged_df['С кред. счетов_current']).round()
        merged_df['Разница_В_дебет'] = (merged_df['В дебет счетов_base'] -
                                        merged_df['В дебет счетов_current']).round()

        # Добавляем информацию о файле
        merged_df['Исх.файл'] = self.file
        return merged_df

    def _apply_reconciliation_correction(self, df: pd.DataFrame, df_for_check: pd.DataFrame,
                                         merged_df: pd.DataFrame) -> pd.DataFrame:
        """Удаляет строки, чья сумма объясняет расхождение (только УПП)."""
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

                        indices_to_remove.update(find_sum_indices(filtered_df, data_col))

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
        spec = self.spec
        account_col = spec.account_column
        kor_col = spec.kor_account_column
        credit_col = spec.amount_credit_initial
        debit_col = spec.amount_debit_initial

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
        use_find_sum_indices = False
        if spec.duplicate_currency_columns:
            cols = df.columns.tolist()
            duplicates = [credit_col, debit_col]

            for dup_name in duplicates:
                # Найти все индексы столбцов с этим именем
                indices = [i for i, col in enumerate(cols) if col == dup_name]
                # Переименовать второй и последующие
                for count, idx in enumerate(indices[1:], start=2):
                    cols[idx] = f"{dup_name}_ВАЛ"
            df.columns = cols

            # Если выгрузка валютная, промежуточные суммирующие строки могут быть не курсивные
            # и скипт не удалит такие строки, что приведет к ошибкам при формировании
            # плоской таблицы. В этом случае ставим флаг того, что в случае таких ошибок
            # скрипт попытается определить такие строки подбором значений
            # с помощью функции find_sum_indices
            columns_ending_with_VAL = df.columns[df.columns.str.endswith('_ВАЛ')]
            if not columns_ending_with_VAL.empty:
                use_find_sum_indices = True

        # Сохраним столбец "Вид связи КА" в отдельный фрейм
        if 'Вид связи КА за период' in df.columns and account_col in df.columns:
            df_type_connection = (
                df
                .drop_duplicates(subset=[account_col, 'Вид связи КА за период'])
                .dropna(subset=[account_col, 'Вид связи КА за период'])
                .loc[:, [account_col, 'Вид связи КА за период']]
            )
            self.df_type_connection = df_type_connection

        # Оптимизация: один раз вычисляем is_valid_account
        kor_schet = df[kor_col].astype(str)
        is_valid_account = self._is_accounting_code_vectorized(kor_schet)

        # Оптимизированное условие для mask
        mask = df[account_col].isna()
        mask = mask & ~is_valid_account
        mask = mask & (kor_schet != 'Кол-во:')
        mask = mask & kor_schet.isin(exclude_values)

        # Заполнение пропусков
        df[account_col] = np.where(mask, 'Не_заполнено', df[account_col])
        df[account_col] = df[account_col].ffill().astype(str)

        # Добавляем '0' для счетов до 10
        account_is_valid = self._is_accounting_code_vectorized(df[account_col])
        mask_pad = (df[account_col].str.len() == 1) & account_is_valid
        df.loc[mask_pad, account_col] = '0' + df.loc[mask_pad, account_col]

        '''Разносим вертикальные данные в горизонтальные'''

        spread_levels(df, df[account_col])

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

        df_for_check = df[[kor_col, credit_col, debit_col]].copy()

        # Векторизированная маска
        valid_mask = self._is_accounting_code_vectorized(df_for_check[kor_col]) | (df_for_check[kor_col].astype(str) == "0")
        df_for_check['Кор.счет_ЧЕК'] = df_for_check[kor_col].astype(str).where(valid_mask)

        # Удаление NaN и заполнение пустыми строками
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].dropna().fillna('')

        # Векторизированное добавление '0' к односимвольным значениям
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].str.zfill(2)

        # Фильтрация
        if '94.Н' in df_for_check['Кор.счет_ЧЕК'].values:
            if spec.narrow_94_filter:
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
                df_for_check = df_for_check[
                    (df_for_check['Кор.счет_ЧЕК'] == '94.Н') | ~df_for_check['Кор.счет_ЧЕК'].isin(['94'])
                ]
        else:
            # Удаляем строки с NaN перед фильтрацией
            df_for_check = df_for_check[df_for_check['Кор.счет_ЧЕК'].notna()]

        # Замена '94.Н' на '94'
        if spec.collapse_94n_to_94:
            df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].replace('94.Н', '94')

        # Группировка и сброс индекса
        df_for_check = df_for_check.groupby('Кор.счет_ЧЕК')[[credit_col, debit_col]].sum().reset_index()

        # Счета с субсчетами
        all_acc_dict = df_for_check['Кор.счет_ЧЕК'].value_counts().to_dict()
        del_acc = hierarchy_drop_set(all_acc_dict)
        df_for_check = df_for_check[~df_for_check['Кор.счет_ЧЕК'].isin(del_acc)].copy()

        # Удаление субсчетов для счетов без субсчетов
        accounts_without_subaccount = get_accounts_without_subaccount()

        # Применяем преобразование к столбцу Кор.счет_ЧЕК
        df_for_check['Кор.счет_ЧЕК'] = df_for_check['Кор.счет_ЧЕК'].apply(
            lambda account: collapse_subaccount(account, accounts_without_subaccount)
        )

        # Агрегируем данные суммированием
        df_for_check = df_for_check.groupby('Кор.счет_ЧЕК', as_index=False).agg({
            credit_col: 'sum',
            debit_col: 'sum'
        })

        # Переименование столбцов для единообразия с УПП
        df_for_check = df_for_check.rename(columns=spec.rename_amount_columns)

        # Сохраним для дальнейшего использования
        self.table_for_check = df_for_check

        '''Удаляем лишние строки (суммирующие, со счетами, которые расшифрованы по субсчетам и тд)'''

        df_delete = df[~df[kor_col].isin(exclude_values)].dropna(subset=[kor_col])
        df_delete = df_delete[df_delete['Курсив'] == 0][[kor_col, 'Корр_счет']]
        unique_df = df_delete.drop_duplicates(subset=[kor_col, 'Корр_счет']).dropna(subset=['Корр_счет'])

        # Подсчет уникальных значений
        all_acc_dict = unique_df['Корр_счет'].value_counts().to_dict()

        # Счета с субсчетами
        del_acc = hierarchy_drop_set(all_acc_dict)

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
        df[kor_col] = df[kor_col].astype(str)

        # Обработка колонок с количеством и валютой
        values_with_quantity, values_with_currency = False, False

        if spec.quantity_from_kor_marker:
            if df[kor_col].isin(['Кол-во:']).any():
                df['С кред. счетов_КОЛ'] = df[credit_col].shift(-1)
                df['В дебет счетов_КОЛ'] = df[debit_col].shift(-1)
                values_with_quantity = True

        if spec.quantity_from_indicator or spec.currency_from_indicator:
            if spec.indicator_column in df.columns:
                if spec.quantity_from_indicator and df[spec.indicator_column].isin(['Кол.']).any():
                    df['С кред. счетов_КОЛ'] = df[credit_col].shift(-1)
                    df['В дебет счетов_КОЛ'] = df[debit_col].shift(-1)
                    df = df[~df[spec.indicator_column].isin(['Кол.', 'Вал.'])].copy()
                    values_with_quantity = True
                elif spec.currency_from_indicator and df[spec.indicator_column].isin(['Вал.']).any():
                    df['С кред. счетов_ВАЛ'] = df[credit_col].shift(-1)
                    df['В дебет счетов_ВАЛ'] = df[debit_col].shift(-1)
                    df = df[~df[spec.indicator_column].isin(['Кол.', 'Вал.'])].copy()
                    values_with_currency = True

        # Заполнение пропущенных значений в столбце Вид_связи
        if 'Вид связи КА за период' in df.columns:
            merged = df.merge(self.df_type_connection, on=account_col, how='left', suffixes=('', '_B'))
            df['Вид связи КА за период'] = df['Вид связи КА за период'].fillna(merged['Вид связи КА за период_B'])

        # Основная фильтрация
        df = df[
            ~df[kor_col].isin(exclude_values) &
            ~df[kor_col].isin(del_acc) &
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
        df = df.rename(columns=spec.rename_columns)

        # Указание порядка столбцов
        base_order = ['Исх.файл', 'Субсчет', 'Аналитика', 'Вид связи КА за период',
                      'Корр_счет', 'Субконто_корр_счета']

        def order(credit_extra=(), debit_extra=()):
            return (base_order
                    + ['С кред. счетов', *credit_extra,
                       'В дебет счетов', *debit_extra])

        if spec.always_include_currency:
            desired_order = order(('С кред. счетов_ВАЛ',), ('В дебет счетов_ВАЛ',))
            if values_with_quantity:
                desired_order = order(('С кред. счетов_КОЛ', 'С кред. счетов_ВАЛ'),
                                      ('В дебет счетов_КОЛ', 'В дебет счетов_ВАЛ'))
        else:
            desired_order = order()
            if values_with_quantity:
                desired_order = order(('С кред. счетов_КОЛ',), ('В дебет счетов_КОЛ',))
            if values_with_currency:
                desired_order = order(('С кред. счетов_ВАЛ',), ('В дебет счетов_ВАЛ',))

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

        if spec.reconciliation_correction:
            for _ in range(3):
                merged_df = self._build_check_frame(df, df_for_check)
                if use_find_sum_indices:
                    df = self._apply_reconciliation_correction(df, df_for_check, merged_df)
        else:
            merged_df = self._build_check_frame(df, df_for_check)

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

        df = ensure_column_types(df)
        return df, self.table_for_check


class Analisys_UPPFileProcessor(AnalisysFileProcessor):
    """Обработчик для Анализа счета 1С УПП."""

    spec = UPP_SPEC


class Analisys_NonUPPFileProcessor(AnalisysFileProcessor):
    """Обработчик для Анализа счета 1С не УПП."""

    spec = NON_UPP_SPEC
