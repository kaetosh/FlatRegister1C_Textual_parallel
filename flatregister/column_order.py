"""Порядок колонок в итоговых отчётах: префиксы и сортировка.

Эталонный порядок по регистрам живёт в :mod:`flatregister.registry` —
там же, где заголовки для распознавания и тексты требований.
"""
import re
from typing import List

import pandas as pd

COLUMN_PREFIXES = (
    'Операция_', 'Содержание_', 'Документ_', 'Аналитика Дт_', 'Субконто Дт_',
    'Аналитика Кт_', 'Субконто Кт_', 'Level_',
)


def _group_sort_key(prefix: str, column: str, original_index: int):
    """Ключ сортировки колонок с префиксом.

    Колонка обязана начинаться с префикса, но числового суффикса может и не
    быть (например, ``Level_`` из битой выгрузки). Раньше ``re.search(...).group(1)``
    на такой колонке падал с ``AttributeError`` — теперь она уезжает в конец
    группы, сохраняя исходный порядок.
    """
    match = re.search(rf"{re.escape(prefix)}(\d+)", column)
    if match is None:
        return (1, 0, original_index)
    return (0, int(match.group(1)), original_index)


def sort_columns(df: pd.DataFrame, desired_order: List[str]) -> pd.DataFrame:
    cols = df.columns.tolist()

    # 1. Фиксированные колонки (не имеющие числового суффикса)
    fixed_cols = [
        col for col in desired_order
        if col in cols and not any(col.startswith(prefix) for prefix in COLUMN_PREFIXES)
    ]

    # 2. Группируем колонки по префиксам
    grouped_cols = {}
    for prefix in COLUMN_PREFIXES:
        prefix_cols = [col for col in cols if col.startswith(prefix)]
        grouped_cols[prefix] = sorted(
            prefix_cols,
            key=lambda column, p=prefix: _group_sort_key(p, column, cols.index(column)),
        )

    # --- Новая часть для обработки колонок вида:
    # [Количество_|ВалютнаяСумма_]<число или число.число или число.буквы>_<до|ко>

    # Определяем префиксы для группировки
    special_prefixes = ['', 'Количество_', 'ВалютнаяСумма_']
    # Суффиксы, которые идут в конце
    suffixes = ['_до', '_ко']

    # Функция для разбора ключа сортировки
    def parse_key(col):
        # Сначала выделим префикс из special_prefixes
        prefix = ''
        for sp in special_prefixes:
            if col.startswith(sp):
                prefix = sp
                break
        # Уберем префикс
        remainder = col[len(prefix):]

        # Проверим, что remainder заканчивается на _до или _ко
        for suf in suffixes:
            if remainder.endswith(suf):
                number_part = remainder[:-len(suf)]
                suffix_part = suf
                break
        else:
            # Если не подходит под шаблон, возвращаем None
            return None

        # Теперь number_part может быть число, число.число или число.буквы
        # Разберём число и буквенную часть
        # Например: 60, 8.0, 76.ФВ
        m = re.match(r"(\d+(?:\.\d+)?)(?:\.([А-Я]+))?$", number_part)
        if m:
            number = float(m.group(1))
            letters = m.group(2) or ''
        else:
            # Если не подошло, ставим очень большой ключ, чтобы в конец
            number = float('inf')
            letters = ''

        # Возвращаем кортеж для сортировки:
        # 1) индекс префикса в special_prefixes (чтобы сортировать по префиксу)
        # 2) число
        # 3) буквы
        # 4) суффикс _до раньше _ко (например)
        suf_order = {'_до': 0, '_ко': 1}

        return (special_prefixes.index(prefix), number, letters, suf_order.get(suffix_part, 10))

    # Собираем все колонки, которые подходят под этот шаблон
    special_cols = [col for col in cols if parse_key(col) is not None]
    # Сортируем их по ключу; при равных ключах сохраняется порядок в выгрузке.
    special_cols_sorted = sorted(special_cols, key=parse_key)

    # 3. Собираем итоговый порядок
    ordered_cols = fixed_cols[:]
    for prefix in COLUMN_PREFIXES:
        ordered_cols.extend(grouped_cols.get(prefix, []))
    ordered_cols.extend(special_cols_sorted)

    # 4. Добавляем остальные в порядке появления в кадре. Через set() порядок
    #    зависел от хеширования и менялся между запусками.
    ordered_set = set(ordered_cols)
    ordered_cols.extend(col for col in cols if col not in ordered_set)

    return df[ordered_cols]
