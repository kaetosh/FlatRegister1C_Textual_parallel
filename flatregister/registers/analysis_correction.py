# -*- coding: utf-8 -*-
"""Досчёт промежуточных итогов в анализе счёта УПП.

Механизм есть только в выгрузках УПП: если после разворота иерархии сверка
не сходится, процессор ищет строки, сумма которых объясняет расхождение, и
удаляет лишние. Вынесено из ``analysis.py`` отдельно — это самостоятельный
алгоритм, а не часть общей обработки.
"""

import pandas as pd

from flatregister.dtypes import coerce_numeric

AMOUNT_COLUMNS = ('С кред. счетов', 'В дебет счетов')


def find_sum_indices(df: pd.DataFrame, column_name: str, tolerance: float = 0.005) -> list:
    """Находит индексы значений, для которых можно подобрать сумму из следующих.

    Parameters
    ----------
    df : pd.DataFrame
        Входной DataFrame.
    column_name : str
        Имя столбца для анализа.
    tolerance : float
        Допустимая погрешность для сравнения сумм.

    Returns
    -------
    list
        Список индексов, для которых найдена подходящая сумма.
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


def reconciliation_interim_results(df: pd.DataFrame,
                                   ind_del_list: set,
                                   last_general_deviation: float,
                                   df_before_changes: pd.DataFrame,
                                   diff_col: str,
                                   data_col: str,
                                   corr_account: str) -> set:
    """Подбирает наименьший набор строк, удаление которых уменьшает расхождение."""
    indices_to_remove = set()

    # Преобразуем в список и сортируем для детерминированного порядка
    sorted_ind_del_list = sorted(ind_del_list)

    for ind_del in sorted_ind_del_list:
        # Создаем временный набор с потенциально новым индексом
        temp_to_remove = indices_to_remove | {ind_del}

        # Удаляем строки и обрабатываем данные
        df_after_changes = df[['Корр_счет', *AMOUNT_COLUMNS]].drop(temp_to_remove, errors='ignore').copy()

        df_after_changes['Кор.счет_ЧЕК'] = df_after_changes.loc[:, 'Корр_счет']

        # Группировка
        df_after_changes = df_after_changes.groupby('Кор.счет_ЧЕК', as_index=False).agg({
            'С кред. счетов': 'sum',
            'В дебет счетов': 'sum'
        })

        # Объединяем и обрабатываем данные
        merged_df = df_before_changes[df_before_changes['Кор.счет_ЧЕК'] == corr_account].merge(
            df_after_changes[df_after_changes['Кор.счет_ЧЕК'] == corr_account],
            on='Кор.счет_ЧЕК',
            how='outer',
            suffixes=('_base', '_current')
        ).fillna(0)

        # Приводим столбцы к числовому типу
        numeric_cols = ['С кред. счетов_base', 'В дебет счетов_base',
                        'С кред. счетов_current', 'В дебет счетов_current']
        coerce_numeric(merged_df, numeric_cols, fill_zero=True)

        # Вычисляем разницы
        merged_df[diff_col] = abs((merged_df[f'{data_col}_base'] -
                                   merged_df[f'{data_col}_current'])).round()

        current_general_deviation = abs(merged_df[diff_col].sum())

        if current_general_deviation <= last_general_deviation:
            last_general_deviation = current_general_deviation
            indices_to_remove.add(ind_del)

        if 0 <= last_general_deviation <= 1:
            break
    return indices_to_remove
