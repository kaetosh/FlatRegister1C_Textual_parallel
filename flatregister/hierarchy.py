"""Общие примитивы иерархии счетов: распознавание счетов, сдвиг уровней,
поиск шапки, разнос уровней, отсев промежуточных итогов и служебные операции
над колонками.

Функции вынесены из методов ``FileProcessor``
(``register_processors/class_processor.py`` → ``flatregister/registers/base.py``)
в Сессии S5 плана рефакторинга (см. Tasks.md), а общие для нескольких
регистров копии собраны здесь в Сессии S6. ``FileProcessor`` сохраняет
одноимённые методы-обёртки (``shiftable_level`` и т.п.), чтобы вызовы в
наследниках не менялись.
"""
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from flatregister.dtypes import level_columns


def is_accounting_code(series: pd.Series) -> pd.Series:
    """Векторизованная проверка значений на бухгалтерский счет."""
    # Конвертируем в строку
    str_series = series.astype(str)
    
    # Быстрые проверки
    result = pd.Series(False, index=series.index)
    
    # Специальные значения
    special_mask = str_series.isin(["0", "00", "000"])
    result[special_mask] = True
    
    # Проверяем наличие точки
    has_dot = str_series.str.contains('.', regex=False)
    
    # Для значений без точки - простые цифровые проверки
    no_dot_mask = ~has_dot
    
    # Проверяем значения без точки
    numeric_no_dot = str_series[no_dot_mask].str.isdigit()
    valid_length_no_dot = str_series[no_dot_mask].str.len() <= 2
    
    # Создаем маски для значений без точки
    valid_numeric_no_dot = pd.Series(False, index=series.index)
    valid_length_mask = pd.Series(False, index=series.index)
    
    # Используем .loc для присвоения значений
    valid_numeric_no_dot.loc[no_dot_mask] = numeric_no_dot
    valid_length_mask.loc[no_dot_mask] = valid_length_no_dot
    
    # Объединяем маски
    valid_no_dot_mask = no_dot_mask & valid_numeric_no_dot & valid_length_mask
    
    # Обновляем результат для значений без точки
    result[valid_no_dot_mask] = True
    
    # Для значений с точкой - сложная проверка
    dot_values = str_series[has_dot]
    if not dot_values.empty:
        # Разделяем на части
        parts = dot_values.str.split('.')
        
        # Проверяем каждую часть
        valid_parts = parts.apply(lambda x: all(
            (p.isdigit() and len(p) <= 2) or (p.isalpha() and len(p) <= 2)
            for p in x if p  # Пропускаем пустые части
        ))
        
        # Проверяем наличие хотя бы одной цифровой части
        has_digit = parts.apply(lambda x: any(p.isdigit() for p in x))
        
        # Приведение к типу bool
        result[has_dot] = (valid_parts & has_digit).to_numpy().astype(bool)
    
    return result


def shift_levels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Выравнивает столбцы таким образом, чтобы бухгалтерские счета находились в одном столбце.
    """
    

    if not df.empty:
        list_lev = level_columns(df)
        continue_shifting = True
        iteration = 0
        
    
        while continue_shifting:
            continue_shifting = False
            iteration += 1
            previous_table_state = df.copy()

            for i in list_lev:
                # Проверяем, есть ли в столбце и субсчет, и субконто
                if is_accounting_code(df[i]).nunique() == 2:
                    lm = int(i.split('_')[-1])

                    # Сдвиг переносит значение из Level_{lm-1} в Level_{lm}, поэтому исходная
                    # колонка обязана содержать счет. Иначе вертикальный сдвиг ломает неверно
                    # распознанный уровень: например инвентарный номер "59" считается счетом
                    # (любое число до 2 цифр), и весь блок уровней уезжает вправо.
                    if lm == 0 or not is_accounting_code(df[list_lev[lm - 1]]).any():
                        continue

                    new_list_lev = list_lev[lm:]
                    
                    
                    # Создаем булеву маску для строк, где первый столбец new_list_lev содержит бухгалтерский код
                    
                    mask = is_accounting_code(df[new_list_lev[0]])
                    
                    # Применяем сдвиг только к строкам, где mask == True
                    df.loc[mask, new_list_lev] = df.loc[mask, new_list_lev].values
                    
                    # Для остальных строк сдвигаем по-другому (если нужно)
                    if lm > 0:
                        other_cols = list_lev[lm - 1:-1]
                        df.loc[~mask, new_list_lev[:len(other_cols)]] = df.loc[~mask, other_cols].values
                    
                    continue_shifting = True

            if previous_table_state.equals(df):
                break
    return df


def max_level_column(df: pd.DataFrame) -> str:
    """
    Находит столбец Level_ с максимальным индексом, все значения которого возвращают True
    в функции is_accounting_code.
    """
    # Получаем все столбцы, начинающиеся с 'Level_'
    levels = level_columns(df)
    
    max_level_column = None
    max_index = -1

    for col in levels:
        # Проверяем, все ли значения в столбце возвращают True
        if is_accounting_code(df[col]).all():
            # Извлекаем индекс из имени столбца
            index = int(col.split('_')[1])
            # Обновляем максимальный индекс и соответствующий столбец
            if index > max_index:
                max_index = index
                max_level_column = col

    return max_level_column


def is_parent(account: str, accounts: List[str]) -> bool:
    """Есть ли у счёта хотя бы один субсчёт в списке."""
    return any(acc.startswith(account + '.') and acc != account for acc in accounts)


def _header_values(df: pd.DataFrame, col_idx: int, max_rows: int) -> pd.Series:
    """Значения колонки в первых ``max_rows`` строках, нормализованные к нижнему регистру."""
    return df.iloc[:max_rows, col_idx].astype(str).str.strip().str.lower()


def find_word_column(df: pd.DataFrame, word: str, max_rows: int = 30) -> Optional[int]:
    """Индекс первого столбца, в первых 30 строках которого встречается ``word``.

    Слово передаётся уже в нижнем регистре. Возвращает ``None``, если столбец
    не найден.
    """
    max_rows_to_check = min(max_rows, df.shape[0])
    for col_idx in range(df.shape[1]):
        if word in _header_values(df, col_idx, max_rows_to_check).values:
            return col_idx
    return None


def find_word_column_and_row(df: pd.DataFrame, word: str,
                             max_rows: int = 30) -> Tuple[Optional[int], Optional[int]]:
    """Индекс столбца со словом ``word`` и строки с точным совпадением ``word``.

    Возвращает ``(col_idx, row_idx)`` или ``(None, None)``. Точное совпадение
    ищется по нормализованному значению (нижний регистр, без пробелов).
    """
    max_rows_to_check = min(max_rows, df.shape[0])
    for col_idx in range(df.shape[1]):
        values = _header_values(df, col_idx, max_rows_to_check)
        if word not in values.values:
            continue
        mask = values == word
        if mask.any():
            return col_idx, mask.idxmax()
    return None, None


def spread_levels(df: pd.DataFrame, values: pd.Series,
                  level_col: str = 'Уровень') -> int:
    """Разносит вертикальные иерархические данные в столбцы ``Level_0``, ``Level_1``, ...

    ``values`` — источник значений для уровней (счёт, склейка счёта с
    наименованием и т.п.). Возвращает максимальный уровень.
    """
    max_level = df[level_col].max()
    for level in range(max_level + 1):
        level_mask = df[level_col] == level
        df[f'Level_{level}'] = values.where(level_mask)
        df[f'Level_{level}'] = df[f'Level_{level}'].ffill()
        df.loc[df[level_col] < level, f'Level_{level}'] = None
    return max_level


def drop_interim_total_rows(df: pd.DataFrame, account_col: str, max_level: int) -> pd.DataFrame:
    """Удаляет строки промежуточных итогов.

    После разноса строк в плоский вид остаются дублирующие обороты (итоги по
    субконто и т.п.). Строка уровня нижележащего счёта, совпадающая с
    значением ``Level_i`` и открывающая более глубокий уровень, отбрасывается.
    """
    conditions = []
    for i in range(max_level):
        conditions.append(
            (df['Уровень'] == i)
            & (df[account_col] == df[f'Level_{i}'])
            & (df['Уровень'].shift(-1) > i)
        )
    mask = pd.concat(conditions, axis=1).any(axis=1)
    return df[~mask]


def hierarchy_drop_set(counts: Dict[str, int]) -> set:
    """Счета, раскрытые субсчетами либо задвоенные, — их колонки уходят из отчёта.

    ``counts`` — частоты встречаемости счетов (результат ``value_counts``).
    """
    keys = list(counts.keys())
    acc_with_sub = [acc for acc in counts if is_parent(acc, keys)]
    clean_acc = [acc for acc in counts if counts[acc] == 1 and acc not in acc_with_sub]
    return set(counts.keys()) - set(clean_acc)


def collapse_subaccount(account, accounts_without_subaccount: List[str]):
    """Сводит субсчёт к основному счёту, если тот указан в настройках."""
    main_account = str(account).split('.')[0]
    if main_account in accounts_without_subaccount:
        return main_account
    return account


def get_col_or_zeros(df: pd.DataFrame, col: str):
    """Столбец ``col`` или ``0``, если такого столбца нет."""
    return df[col] if col in df.columns else 0


def split_and_expand(df: pd.DataFrame, col_name: str, prefix: str) -> None:
    """Разбивает столбец с разделителем ``\\n`` на столбцы ``{prefix}_1``, ``{prefix}_2``, ..."""
    if col_name not in df.columns:
        return

    new_cols = df[col_name].str.split('\n', expand=True)
    if new_cols is None or new_cols.empty:
        df.drop(columns=[col_name], inplace=True)
        return

    new_cols.columns = [f'{prefix}_{i + 1}' for i in range(new_cols.shape[1])]
    df[new_cols.columns] = new_cols
    df.drop(columns=[col_name], inplace=True)


def keep_first_unique_per_row(df: pd.DataFrame) -> pd.DataFrame:
    """Оставляет только первое вхождение значения в каждой строке, остальные — ``NaN``."""
    result = df.copy()
    for idx in df.index:
        duplicates = df.loc[idx].duplicated()
        result.loc[idx, duplicates] = np.nan
    return result
