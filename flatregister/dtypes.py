# -*- coding: utf-8 -*-
"""Роли колонок и приведение типов в результатах обработки.

Денежные колонки приходят из ``_preprocessor_openpyxl`` как ``object`` или
``str``: кадр собирается из строк выгрузки, где рядом с числами стоят
текстовые подзаголовки 1С («Дебет», «Кредит»), и pandas фиксирует dtype по
всей колонке ещё до того, как шапка вырезается. Числа в такой колонке лежат
питоновскими int/float, поэтому ``.sum()`` работает, а вот колонка «всё
пусто» получает dtype ``str`` и ``.sum()`` по ней возвращает строку ``'0'``
вместо числа — из-за этого и появилась :func:`numeric_total`.

Здесь роли колонок объявлены один раз и применяются в конце ``process_file``:
денежные суммы приводятся к числам, счета и уровни остаются текстом, а
постусловие :func:`ensure_column_types` падает, если контракт нарушен.
"""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from flatregister.errors import RegisterStructureError

#: Денежные суммы ОСВ: сальдо и обороты. Приводятся к числам.
MONEY_COLUMNS = ('Дебет_начало', 'Кредит_начало', 'Дебет_оборот',
                 'Кредит_оборот', 'Дебет_конец', 'Кредит_конец')

#: Колонки-счета: бухгалтерский код, а не сумма. Должны оставаться текстом,
#: иначе Excel покажет счёт как число (например, «60.01» превращается в 60.01).
ACCOUNT_COLUMNS = ('Счет', 'Субсчет', 'Субконто', 'Корр_счет',
                   'С кред. счетов', 'В дебет счетов', 'Дт', 'Кт')

#: Колонки иерархии создаются динамически (Level_0..Level_N), поэтому основная
#: проверка идёт по префиксу; кортеж фиксирует базовый набор для справки.
LEVEL_PREFIX = 'Level_'
LEVEL_COLUMNS = ('Level_0', 'Level_1', 'Level_2')


def is_level_column(name) -> bool:
    """Является ли колонка уровнем иерархии счёта."""
    return str(name).startswith(LEVEL_PREFIX)


def level_columns(df: pd.DataFrame) -> list:
    """Список колонок-уровней в кадре."""
    return [col for col in df.columns if is_level_column(col)]


def to_numeric(series: pd.Series, fill_zero: bool = False) -> pd.Series:
    """Приводит серию к числу. Нечисловое становится NaN (при ``fill_zero`` — 0).

    Единая точка вместо разбросанных ``pd.to_numeric(..., errors='coerce')``:
    правило приведения типов должно быть одно на весь проект.
    """
    result = pd.to_numeric(series, errors='coerce')
    return result.fillna(0) if fill_zero else result


def coerce_numeric(df: pd.DataFrame, columns: Iterable[str],
                   fill_zero: bool = False) -> pd.DataFrame:
    """Приводит перечисленные колонки к числовому типу. Отсутствующие пропускает."""
    present = [col for col in columns if col in df.columns]
    if present:
        df[present] = df[present].apply(lambda series: to_numeric(series, fill_zero))
    return df


def numeric_total(df: pd.DataFrame, columns: Iterable[str]) -> float:
    """Сумма числовых значений по колонкам. Отсутствующие колонки дают 0.

    Заменяет конструкцию ``df[cols].sum().sum()``, которая на pandas 3 падает:
    колонка, где все значения пустые, имеет dtype ``str``, и ``DataFrame.sum()``
    возвращает по ней строку ``'0'``. Повторное сложение даёт
    ``TypeError: can only concatenate str (not "float") to str``.
    """
    if df is None or df.empty:
        return 0.0

    present = [col for col in columns if col in df.columns]
    if not present:
        return 0.0

    numeric = df[present].apply(to_numeric)
    return float(numeric.sum().sum())


def is_numeric_column(series: pd.Series) -> bool:
    """Является ли колонка числовой по dtype."""
    return pd.api.types.is_numeric_dtype(series)


def ensure_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """Постусловие обработки: деньги — числом, счета и уровни — текстом.

    Приводит денежные колонки к числам и падает :class:`RegisterStructureError`,
    если контракт выполнить не удалось. Вызывается в конце ``process_file``,
    чтобы дальше по конвейеру кадр был предсказуемым по типам.
    """
    coerce_numeric(df, MONEY_COLUMNS)

    for col in MONEY_COLUMNS:
        if col in df.columns and not is_numeric_column(df[col]):
            raise RegisterStructureError(
                f'Денежная колонка «{col}» осталась нечисловой ({df[col].dtype}).',
                hint='Это внутренняя ошибка обработки, а не свойство выгрузки. '
                     'Приложите файл к обращению разработчику.')

    for col in ACCOUNT_COLUMNS:
        if col in df.columns and is_numeric_column(df[col]):
            raise RegisterStructureError(
                f'Колонка-счёт «{col}» оказалась числовой.',
                hint='Номер счёта должен оставаться текстом, иначе Excel покажет '
                     'его как число. Это внутренняя ошибка обработки.')

    for col in level_columns(df):
        if is_numeric_column(df[col]):
            raise RegisterStructureError(
                f'Колонка уровня «{col}» оказалась числовой.',
                hint='Иерархия счёта — текст вида «60.01_Материалы». '
                     'Это внутренняя ошибка обработки.')

    return df
