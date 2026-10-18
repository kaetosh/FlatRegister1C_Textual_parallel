# -*- coding: utf-8 -*-
"""Карточка счёта и отчёт по проводкам.

У этих двух регистров нет сверки оборотов и нет уровней иерархии: это плоские
списки проводок. Проверяется поэтому по сути другое — что документ,
аналитика и суммы доехали на место, а служебные колонки предобработки не
просочились в отчёт.
"""

from __future__ import annotations

import pytest

from register_processors.card_processor import Card_NonUPPFileProcessor, Card_UPPFileProcessor
from register_processors.posting_processor import Posting_NonUPPFileProcessor, Posting_UPPFileProcessor


@pytest.mark.parametrize("processor_class,file_name", [
    (Card_UPPFileProcessor, "card_upp_file"),
    (Card_NonUPPFileProcessor, "card_non_upp_file"),
])
def test_карточка_даёт_по_строке_на_документ(processor_class, file_name, request):
    path = request.getfixturevalue(file_name)

    result, _ = processor_class().process_file(path)

    assert len(result) == 3, result.to_string()
    assert set(result["Имя_файла"]) == {path.name}
    assert "Операция" not in result.columns


@pytest.mark.parametrize("processor_class,file_name", [
    (Posting_UPPFileProcessor, "posting_upp_file"),
    (Posting_NonUPPFileProcessor, "posting_non_upp_file"),
])
def test_проводки_дают_по_строке_на_документ(processor_class, file_name, request):
    path = request.getfixturevalue(file_name)

    result, _ = processor_class().process_file(path)

    assert len(result) == 3, result.to_string()
    assert set(result["Имя_файла"]) == {path.name}
    assert "Содержание" not in result.columns


@pytest.mark.parametrize("processor_class,file_name", [
    (Card_UPPFileProcessor, "card_upp_file"),
    (Posting_UPPFileProcessor, "posting_upp_file"),
])
def test_даты_разбираются_с_часами(processor_class, file_name, request):
    """УПП читает дату по двум шаблонам: сначала со временем, потом без."""
    import pandas as pd

    path = request.getfixturevalue(file_name)

    result, _ = processor_class().process_file(path)

    date_column = "Дата" if "Дата" in result.columns else "Период"
    assert pd.api.types.is_datetime64_any_dtype(result[date_column]), \
        f"{date_column} имеет dtype {result[date_column].dtype}"


def test_карточка_без_строки_период_даёт_понятную_ошибку(out_dir):
    """Файл без строки «Период» и без строки «Дата» должен отказывать словами."""
    from tests.factories import GROUP_MARK, SheetBuilder

    builder = SheetBuilder()
    builder.add("Карточка счёта")
    builder.add(GROUP_MARK, "Документ", "Дебет")
    builder.add(GROUP_MARK, "Продажа 1", 100)
    path = builder.save(out_dir / "не карточка.xlsx")

    with pytest.raises(Exception) as caught:
        Card_NonUPPFileProcessor().process_file(path)

    error = caught.value
    assert "Период" in error.user_message, error.user_message
    assert error.hint


def test_проводки_пустой_файл_даёт_понятную_ошибку(out_dir):
    """Пустой отчёт по проводкам должен отказывать словами, а не падать."""
    from tests.factories import GROUP_MARK, SheetBuilder

    builder = SheetBuilder()
    builder.add("Отчёт по проводкам")
    builder.add(GROUP_MARK, "Документ", "Дт", "Кт")
    path = builder.save(out_dir / "пустые проводки.xlsx")

    with pytest.raises(Exception) as caught:
        Posting_NonUPPFileProcessor().process_file(path)

    error = caught.value
    assert error.user_message and error.user_message.strip()
    assert error.hint