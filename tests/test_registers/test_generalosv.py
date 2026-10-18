# -*- coding: utf-8 -*-
"""Общая ОСВ — самый простой регистр, на нём отлаживался генератор фикстур."""

from __future__ import annotations

import pandas as pd
import pytest

from register_processors.generalosv_processor import GeneralOSV_NonUPPFileProcessor, GeneralOSV_UPPFileProcessor

REGISTER = "generalosv"


@pytest.mark.parametrize("processor_class", [GeneralOSV_UPPFileProcessor,
                                             GeneralOSV_NonUPPFileProcessor])
def test_общая_осв_даёт_плоскую_таблицу(processor_class, generalosv_upp_file, generalosv_non_upp_file):
    """Пакетный режим: обе реализации обязаны вести себя одинаково."""
    path = generalosv_upp_file if processor_class is GeneralOSV_UPPFileProcessor \
        else generalosv_non_upp_file

    result, check = processor_class().process_file(path)

    assert not result.empty
    assert "Исх.файл" in result.columns
    assert set(result["Исх.файл"]) == {path.name}
    # Родительский счёт 60 имеет субсчета и при keep_subaccounts=True отбрасывается,
    # а 62 — самостоятельный, он остаётся.
    assert set(result["Счет"]) == {"60.01", "60.02", "62"}


@pytest.mark.parametrize("processor_class", [GeneralOSV_UPPFileProcessor,
                                             GeneralOSV_NonUPPFileProcessor])
def test_общая_осв_колонки_соответствуют_описанию(processor_class, generalosv_upp_file,
                                                 generalosv_non_upp_file):
    path = generalosv_upp_file if processor_class is GeneralOSV_UPPFileProcessor \
        else generalosv_non_upp_file

    result, _ = processor_class().process_file(path)

    expected = {
        "Исх.файл", "Счет", "Наименование",
        "Дебет_начало", "Кредит_начало",
        "Дебет_оборот", "Кредит_оборот",
        "Дебет_конец", "Кредит_конец",
        "Level_0",
    }
    assert expected <= set(result.columns)
    # Служебные колонки предобработки наружу не должны просачиваться
    assert "Уровень" not in result.columns
    assert "Курсив" not in result.columns


@pytest.mark.parametrize("processor_class", [GeneralOSV_UPPFileProcessor,
                                             GeneralOSV_NonUPPFileProcessor])
def test_общая_осв_контрольная_сумма_сходится(processor_class, generalosv_upp_file,
                                             generalosv_non_upp_file):
    """Свёрнутые суммы считаются по всем оставшимся строкам.

    Это главный инвариант: если разнос по уровням что-то теряет или задваивает,
    разойдутся именно эти три числа. Считаем вручную по трём строкам-листьям
    60.01, 60.02 и 62 из фикстуры.
    """
    path = generalosv_upp_file if processor_class is GeneralOSV_UPPFileProcessor \
        else generalosv_non_upp_file

    _, check = processor_class().process_file(path)

    assert not check.empty
    row = check.iloc[0]

    debit_turnover = 30_000 + 20_000 + 5_000
    credit_turnover = 28_000 + 20_000 + 4_500
    assert row["Оборот_свернуто"] == pytest.approx(debit_turnover - credit_turnover)
    assert row["Сальдо_начало_свернуто"] == pytest.approx(6_000 + 4_000 + 1_000)
    assert row["Сальдо_конец_свернуто"] == pytest.approx(7_000 + 5_000 + 1_500)
    assert row["Исх.файл"] == path.name


def test_общая_осв_пустая_даёт_понятную_ошибку(out_dir):
    """Файл без данных должен отказывать словами, а не падать внутри pandas."""
    from tests import factories

    empty = factories.generalosv_upp(
        out_dir / "пустая.xlsx",
        rows=[("60", 0, 0, 0, 0, 0)],
    )
    with pytest.raises(Exception) as caught:
        GeneralOSV_UPPFileProcessor().process_file(empty)

    error = caught.value
    assert type(error).__name__ in ("RegisterEmptyError", "RegisterStructureError"), \
        f"неожиданный тип ошибки: {type(error).__name__}: {error}"
    assert "пуст" in error.user_message.lower()


def test_общая_осв_денежные_колонки_числовые(generalosv_upp_file):
    """Должны стать числами, а не object.

    До S3 колонки приходили как object, и суммы по ним считались только через
    numeric_total. Теперь приведение типов делает process_file.
    """
    result, _ = GeneralOSV_UPPFileProcessor().process_file(generalosv_upp_file)

    for column in ("Дебет_начало", "Кредит_начало", "Дебет_оборот",
                   "Кредит_оборот", "Дебет_конец", "Кредит_конец"):
        assert column in result.columns
        assert pd.api.types.is_numeric_dtype(result[column]), \
            f"{column} имеет dtype {result[column].dtype}"


def test_общая_осв_суммы_читаются_несмотря_на_object(generalosv_upp_file):
    """Пока колонки object, суммировать их нельзя напрямую.

    numeric_total — обходной путь. Тест фиксирует, что он работает и что без
    него сумма падает: именно этим аварийно ловился баг pandas 3 с пустыми
    колонками.
    """
    from flatregister.dtypes import MONEY_COLUMNS, numeric_total

    result, _ = GeneralOSV_UPPFileProcessor().process_file(generalosv_upp_file)

    # Сумма по всем шести денежным колонкам трёх строк-листьев:
    # 11 000 + 0 + 55 000 + 52 500 + 13 500 + 0
    assert numeric_total(result, MONEY_COLUMNS) == pytest.approx(132_000)


def test_общая_осв_двузначные_счета_дополнены_нулём(out_dir):
    """Счёт 5 должен превратиться в «05»: иначе он не распознаётся как счёт."""
    from tests import factories

    path = factories.generalosv_upp(out_dir / "счёт 5.xlsx", rows=[
        ("5", 1, 1_000, 2_000, 3_000, 3_000),
        ("5.1", 2, 1_000, 2_000, 3_000, 3_000),
    ])
    result, _ = GeneralOSV_UPPFileProcessor().process_file(path)
    assert "5.1" in set(result["Счет"])
    assert not any(str(value).startswith("05.") for value in result["Счет"])