# -*- coding: utf-8 -*-
"""Распознавание бухгалтерских счетов и выравнивание уровней.

Это ядро, на котором держится разнос «плоского» вида, и оно же — источник
самых дорогих ошибок: любое неверное решение здесь тихо перекладывает данные
между колонками, а итоговые суммы остаются верными.
"""

from __future__ import annotations

import pandas as pd
import pytest

from register_processors.class_processor import FileProcessor


def _is_account(values):
    return FileProcessor._is_accounting_code_vectorized(pd.Series(values))


@pytest.mark.parametrize("value", ["60", "05", "0", "00", "000", "60.01",
                                   "76.ФВ", "8.0", "94.Н"])
def test_похожие_на_счёт_значения_признаются_счетами(value):
    assert bool(_is_account([value])[0]), value


@pytest.mark.parametrize("value", ["Контрагент 1", "ООО Ромашка", "", "59_000",
                                   "60001", "60.012", "Продажа", "Итого"])
def test_не_счета_считаются_прочим(value):
    """Инвентарные номера вида «59» — счёт, а «59000» уже нет.

    Такая граница стоила бага в S0: уровень с коротким числом уезжал вправо
    по всей таблице. Проверка фиксирует границу, чтобы её не сдвинули снова.
    """
    assert not bool(_is_account([value])[0]), value


def test_пустые_значения_не_являются_счетами():
    """NaN и пустая строка не должны попасть в разряд счетов."""
    assert not bool(_is_account([None, float("nan"), ""]).any())


def test_смешанный_столбец_даёт_смешанную_маску():
    mask = _is_account(["60.01", "Контрагент 1", "62", None])
    assert list(mask) == [True, False, True, False]


def test_счёт_имеет_субсчета():
    accounts = ["60", "60.01", "60.02", "62"]
    assert FileProcessor._is_parent("60", accounts)
    assert not FileProcessor._is_parent("62", accounts)


def test_выравнивание_уровней_не_ломает_кадр(bare_processor):
    """Сдвиг выполняется, когда в колонке смешаны счета и аналитика.

    Проверяем главное: данные не теряются и не дублируются, а сумма денег
    остаётся прежней.
    """
    frame = pd.DataFrame({
        "Level_0": ["60", None, None],
        "Level_1": [None, "60.01", "Контрагент 1"],
        "Сумма": [100, 60, 40],
    })
    processor = bare_processor
    before = frame["Сумма"].sum()

    result = processor.shiftable_level(frame)

    assert result["Сумма"].sum() == before
    assert len(result) == len(frame)


def test_выравнивание_идемпотентно(bare_processor):
    """Второй прогон по уже выровненному кадру ничего не меняет.

    Если это не так, обработка одного и того же файла дважды даст разный
    результат — а именно такое и происходит при повторном открытии папки.
    """
    frame = pd.DataFrame({
        "Level_0": ["60", None, None],
        "Level_1": [None, "60.01", "Контрагент 1"],
        "Сумма": [100, 60, 40],
    })
    processor = bare_processor

    once = processor.shiftable_level(frame)
    twice = processor.shiftable_level(once)

    pd.testing.assert_frame_equal(once, twice)


def test_пустой_кадр_не_ломает_выравнивание(bare_processor):
    """Пустой кадр обязан вернуться как есть, а не упасть."""
    frame = pd.DataFrame({"Level_0": [], "Сумма": []})
    result = bare_processor.shiftable_level(frame)
    assert result.empty


def test_поиск_глубочайшей_колонки_уровней(bare_processor):
    frame = pd.DataFrame({
        "Level_0": ["60", "62"],
        "Level_1": ["60.01", "62.01"],
        "Level_2": ["Контрагент 1", "Контрагент 2"],
    })
    assert bare_processor.find_max_level_column(frame) == "Level_1"


