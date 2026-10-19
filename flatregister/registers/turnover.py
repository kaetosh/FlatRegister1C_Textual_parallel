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
from flatregister.dtypes import ensure_column_types, numeric_total
from flatregister.errors import RegisterEmptyError, RegisterNotRecognizedError, RegisterStructureError
from flatregister.excel_io import fix_1c_excel_case
from flatregister.hierarchy import (
    drop_interim_total_rows,
    find_word_column,
    get_col_or_zeros,
    hierarchy_drop_set,
    spread_levels,
)
from flatregister.registers.base import FileProcessor, exclude_values
from flatregister.registers.spec import RegisterSpec


@dataclass(frozen=True)
class TurnoverSpec(RegisterSpec):
    """Различия между выгрузками оборотов УПП и не-УПП.

    ``currency_shift`` — это D1 из «Открытых решений»: в УПП валютная сумма
    стоит на две строки ниже денежной, в не-УПП — на одну. Здесь различие
    названо явно, а не спрятано в двух копиях тела метода.
    """

    rename_map: dict
    replace_empty: bool
    indicator_regex: str
    indicator_any_enables: bool
    currency_shift: int
    derive_currency_from_account: bool
    cleanup_by_indicator: bool


UPP_SPEC = TurnoverSpec(
    header_word='субконто',
    header_exact='Субконто',
    account_column='Субконто',
    indicator_column='Показа-\nтели',
    structure_message='Не найден столбец с "Субконто" в первых 30 строках.',
    recognized_hint='В выгрузке нет строки заголовков со словом «Субконто».',
    empty_message='Обороты счета пустые.',
    rename_map={'Нач. сальдо деб.': 'Дебет_начало',
                'Нач. сальдо кред.': 'Кредит_начало',
                'Деб. оборот': 'Дебет_оборот',
                'Кред. оборот': 'Кредит_оборот',
                'Кон. сальдо деб.': 'Дебет_конец',
                'Кон. сальдо кред.': 'Кредит_конец'},
    replace_empty=True,
    indicator_regex='Кол.',
    indicator_any_enables=True,
    currency_shift=-2,
    derive_currency_from_account=True,
    cleanup_by_indicator=False,
)

NON_UPP_SPEC = TurnoverSpec(
    header_word='счет',
    header_exact='Счет',
    account_column='Счет',
    indicator_column='Показа-\nтели',
    structure_message='Не найден столбец с "Счет" в первых 30 строках.',
    recognized_hint='В выгрузке нет строки заголовков со словом «Счет».',
    empty_message='Обороты счета пустые.',
    rename_map={'Начальное сальдо Дт': 'Дебет_начало',
                'Начальное сальдо Кт': 'Кредит_начало',
                'Оборот Дт': 'Дебет_оборот',
                'Оборот Кт': 'Кредит_оборот',
                'Конечное сальдо Дт': 'Дебет_конец',
                'Конечное сальдо Кт': 'Кредит_конец'},
    replace_empty=False,
    indicator_regex='Кол.|Вал.',
    indicator_any_enables=False,
    currency_shift=-1,
    derive_currency_from_account=False,
    cleanup_by_indicator=True,
)


class TurnoverFileProcessor(FileProcessor):
    """Обработчик оборотов счёта 1С; вариант задаётся :class:`TurnoverSpec`."""

    spec: TurnoverSpec

    def __init__(self):
        super().__init__()
        self.df_type_connection = pd.DataFrame()  # хранение данных анализа счета с полем Вид связи КА за период

    def _process_dataframe_optimized(self, df: pd.DataFrame) -> pd.DataFrame:
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
            Возникает, если обрабатываемый файл не является Оборотами счёта
            или является пустым. Такой файл не обрабатывается, его имя
            попадает в отчёт о неудачных файлах в конце обработки.

        Returns
        -------
        df : pd.DataFrame
            Готовая к дальнейшей обработке таблица.

        '''
        spec = self.spec

        # УПП-выгрузка приносит пустые строки как ''; в не-УПП их нет
        if spec.replace_empty:
            df = df.replace('', np.nan)

        # удалим пустые строки и столбцы
        df.dropna(axis=1, how='all', inplace=True)
        df.dropna(axis=0, how='all', inplace=True)

        # Ищем столбец, содержащий "Субконто"/"Счет" в первых 30 строках
        subkonto_col_idx = find_word_column(df, spec.header_word)

        if subkonto_col_idx is None:
            raise RegisterStructureError(
                spec.structure_message,
                hint='Похоже, это не обороты счёта. Проверьте, что выбран тот тип '
                     'регистра, который соответствует выгрузке.')

        # Теперь используем найденный столбец
        first_col = df.iloc[:, subkonto_col_idx].astype(str)
        mask = first_col == spec.header_exact

        # ошибка, если нет искомого значения
        if not mask.any():
            raise RegisterNotRecognizedError(
                'Файл не является Оборотами счета 1с.',
                hint=spec.recognized_hint)

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
                spec.empty_message,
                hint='В выгрузке нет ни одной строки с данными.')

        # Уровень и Курсив должны иметь 0 или 1, иначе - ошибка
        if df['Уровень'].isnull().any() or df['Курсив'].isnull().any():
            raise RegisterStructureError(
                'Найдены пустые значения в столбцах Уровень или Курсив.',
                hint='В Excel не выставлены уровни группировки (знаки «+» и «−» слева). '
                     'Выгрузите регистр заново через меню «Сохранить как» в 1С.')

        return df

    def _process_suffix(self, df: pd.DataFrame, cols: list, start_idx: int, end_idx: int,
                        suffix: str, accounts_without_subaccount: list) -> pd.DataFrame:
        """Отделяет корр. счета одной стороны, отбрасывая счета высшего порядка."""
        new_cols = cols.copy()
        for i in range(start_idx + 1, end_idx):
            new_cols[i] = f"{cols[i]}{suffix}"
        df.columns = new_cols

        # Получаем список столбцов с нужным суффиксом
        cols_with_suffix = [col for col in df.columns if col.endswith(suffix)]
        base_cols = [col[:-len(suffix)] for col in cols_with_suffix]

        # Подсчёт частот
        counts = pd.Series(base_cols).value_counts().to_dict()

        # Определяем счета с субсчетами и чистые счета
        del_acc = hierarchy_drop_set(counts)

        # Обработка счетов с '94'
        acc_with_94 = [acc for acc in counts if '94' in acc]
        if '94.Н' in acc_with_94:
            del_acc.update(acc for acc in acc_with_94 if acc != '94.Н')

        # Удаление субсчетов для счетов без субсчетов
        for acc in accounts_without_subaccount:
            unwanted_subaccounts = [n for n in counts if acc in n and n != acc]
            del_acc.update(unwanted_subaccounts)

        # Исключаем счета без субсчетов из удаления
        del_acc.difference_update(accounts_without_subaccount)

        # Формируем полный список столбцов для удаления с суффиксом
        del_acc_full = {acc + suffix for acc in del_acc}

        # Удаляем столбцы из df
        df.drop(columns=del_acc_full, inplace=True)

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

        # Избавимся от столбцов с пустыми наименованиями, поскольку это полностью пустые столбцы
        df = df.loc[:, df.columns.notna()]

        # Приведем наименования столбцов к строковому формату
        df.columns = df.columns.astype(str)

        # переименуем для разнообразия
        df.rename(columns=spec.rename_map, inplace=True)

        '''Столбцы с корр счетами разделим на дебетовые и кредитовые.
        Удалим столбцы с счетами высшего порядка, оставим только субсчета.
        '''
        accounts_without_subaccount = get_accounts_without_subaccount()

        cols = df.columns.tolist()
        debit_idx = cols.index('Дебет_оборот')
        credit_idx = cols.index('Кредит_оборот')
        end_debit_idx = cols.index('Дебет_конец')

        df = self._process_suffix(df, cols, debit_idx, credit_idx, '_до', accounts_without_subaccount)
        cols = df.columns.tolist()  # обновляем после переименования
        credit_idx = cols.index('Кредит_оборот')
        end_debit_idx = cols.index('Дебет_конец')

        df = self._process_suffix(df, cols, credit_idx, end_debit_idx, '_ко', accounts_without_subaccount)

        '''Обработка пропущенных значений'''

        indicator_present = spec.indicator_column in df.columns

        # Для выгрузок с полем "Количество" / "Валюта"
        if indicator_present:
            mask = df[spec.indicator_column].str.contains(spec.indicator_regex, na=False)
            df.loc[~mask, account_col] = df.loc[~mask, account_col].fillna('Не_заполнено')
            df[account_col] = df[account_col].ffill()

        else:
            # Проставляем значение "Количество" (для ОСВ, так как строки с количеством не обозначены)
            df[account_col] = np.where(
                                        df[account_col].isna() & df['Уровень'].eq(df['Уровень'].shift(1)),
                                        'Количество',
                                        df[account_col])
            # Удалим строки, содержащие значение "Количество" ниже строки с Итого. Предыдущий Код "Количество" ниже Итого проставляет даже в регистрах
            # Без количественных значений.
            # Найдем индекс строки, где находится 'Итого'.
            # Проверяем, есть ли 'Итого' в столбце.
            if (df[account_col] == 'Итого').any():
                # Если 'Итого' существует, получаем индекс
                index_total = df[df[account_col] == 'Итого'].index[0]
                # Фильтруем DataFrame
                df = df[(df.index <= index_total) | ((df.index > index_total) & (df[account_col] != 'Количество'))]

            df.loc[:, account_col] = df[account_col].fillna('Не_заполнено')

        # Преобразование в строки и добавление ведущего нуля для счетов до 10 (01, 02 и т.д.)
        mask = (df[account_col].str.len() == 1) & self._is_accounting_code_vectorized(df[account_col])
        df.loc[mask, account_col] = '0' + df.loc[mask, account_col]
        df[account_col] = df[account_col].astype(str)

        '''Разносим вертикальные данные в горизонтальные'''

        max_level = df['Уровень'].max()

        # Обрабатываем специальный случай для "Количество" векторизованно
        quantity_mask = df[account_col] == 'Количество'

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
            df.loc[quantity_mask, account_col] = last_level_values

        spread_levels(df, df[account_col])

        df.loc[df[quantity_mask].index, account_col] = 'Количество'

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

        def indicator_has(value: str) -> bool:
            """Включён ли количественный/валютный блок по столбцу-индикатору."""
            if not indicator_present:
                return False
            if spec.indicator_any_enables:
                return True
            return (df[spec.indicator_column] == value).any()

        if df[account_col].isin(['Количество']).any() or indicator_has('Кол.'):
            for i in desired_order:
                df[f'Количество_{i}'] = df[i].shift(-1)

        if df[account_col].isin(['Валютная сумма']).any() or indicator_has('Вал.'):
            if spec.derive_currency_from_account and df[account_col].str.startswith('В валюте').any():
                df['Валюта'] = df[account_col].shift(-1)
            for i in desired_order:
                df[f'ВалютнаяСумма_{i}'] = df[i].shift(spec.currency_shift)

        # Очистим таблицу от строк с количеством и валютой
        if spec.cleanup_by_indicator:
            if indicator_present:
                mask = (
                    (df[spec.indicator_column] == 'Кол.') |
                    (df[spec.indicator_column] == 'Вал.')
                )
                df = df[~mask]
        else:
            mask = (
                (df[account_col] == 'Количество') |
                (df[account_col] == 'Валютная сумма') |
                (df[account_col].str.startswith('В валюте'))
            )
            df = df[~mask]

        '''Сохраняем данные по оборотам до обработки в таблицах'''

        if df[df[account_col] == 'Итого'][desired_order].empty:
            raise RegisterStructureError(
                'Строка «Итого» пустая — не с чем сверять результат.',
                hint='Проверьте, что в выгрузке есть строка «Итого» с итогами по счёту. '
                     'Без неё сверка оборотов невозможна.')

        df_for_check = df[df[account_col] == 'Итого'][[account_col] + desired_order_not_with_suff_do_ko].copy().tail(2).iloc[[0]]
        df_for_check[desired_order_not_with_suff_do_ko] = df_for_check[desired_order_not_with_suff_do_ko].astype(float).fillna(0)

        # Списки необходимых столбцов для каждой новой колонки
        start_debit = 'Дебет_начало'
        start_credit = 'Кредит_начало'
        end_debit = 'Дебет_конец'
        end_credit = 'Кредит_конец'
        debit_turnover = 'Дебет_оборот'
        credit_turnover = 'Кредит_оборот'

        # Создаем новые столбцы с проверкой наличия исходных
        df_for_check['Сальдо_начало_до_обработки'] = get_col_or_zeros(df_for_check, start_debit) - get_col_or_zeros(df_for_check, start_credit)
        df_for_check['Сальдо_конец_до_обработки'] = get_col_or_zeros(df_for_check, end_debit) - get_col_or_zeros(df_for_check, end_credit)
        df_for_check['Оборот_до_обработки'] = get_col_or_zeros(df_for_check, debit_turnover) - get_col_or_zeros(df_for_check, credit_turnover)
        df_for_check = df_for_check[['Сальдо_начало_до_обработки', 'Сальдо_конец_до_обработки', 'Оборот_до_обработки']].reset_index()

        ''' После разнесения строк в плоский вид, в таблице остаются строки с дублирующими оборотами.
        Например, итоговые обороты, итоги по субконто и т.д. Удаляем.'''
        max_level = df['Уровень'].max()
        df = drop_interim_total_rows(df, account_col, max_level)

        # Удаляем строки, содержащие значения из списка exclude_values
        df = df[~df[account_col].isin(exclude_values)]

        df = df.rename(columns={account_col: 'Субконто'})
        df.drop('Уровень', axis=1, inplace=True)

        # отберем только те строки, в которых хотя бы в одном из столбцов, определенных в existing_columns, есть непропущенные значения (не NaN)
        df = df[df[desired_order].notna().any(axis=1)]

        if spec.indicator_column in df.columns:
            df = df.drop(columns=[spec.indicator_column])
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

        # столбцы с дебетовыми оборотами по корр.счетам
        filtered_cols_do = [
            col for col in df.columns
            if col.endswith('_до')
            and not col.startswith('Количество_')
            and not col.startswith('ВалютнаяСумма_')
        ]

        # столбцы с кредитовыми оборотами по корр.счетам
        filtered_cols_ko = [
            col for col in df.columns
            if col.endswith('_ко')
            and not col.startswith('Количество_')
            and not col.startswith('ВалютнаяСумма_')
        ]

        # Вычисление итоговых значений - свернутые значения сальдо и оборотов - обработанных таблиц
        df_check_after_process = pd.DataFrame({
            'Сальдо_начало_после_обработки': [get_col_or_zeros(df, start_debit).sum() - get_col_or_zeros(df, start_credit).sum()],
            'Оборот_после_обработки': [numeric_total(df, filtered_cols_do) - numeric_total(df, filtered_cols_ko)],
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

        df = ensure_column_types(df)
        return df, self.table_for_check


class Turnover_UPPFileProcessor(TurnoverFileProcessor):
    """Обработчик для Оборотов счёта 1С УПП."""

    spec = UPP_SPEC


class Turnover_NonUPPFileProcessor(TurnoverFileProcessor):
    """Обработчик для Оборотов счёта 1С не УПП."""

    spec = NON_UPP_SPEC
