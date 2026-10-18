"""Общие примитивы иерархии счетов: распознавание счетов, сдвиг уровней.

Функции вынесены из методов ``FileProcessor``
(``register_processors/class_processor.py`` → ``flatregister/registers/base.py``)
в Сессии S5 плана рефакторинга (см. Tasks.md). ``FileProcessor`` сохраняет
одноимённые методы-обёртки, чтобы вызовы ``self.shiftable_level(...)`` и т.п.
в наследниках не менялись. Полное объединение копий по регистрам — Сессия S6.
"""
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
