# -*- coding: utf-8 -*-
"""Анализ счёта.

Ключевое отличие от остальных регистров — работа с КУРСИВОМ. 1С помечает им
строки с промежуточными итогами, обработчик их выбрасывает, а сверка после
этого обязана сойтись.
"""

from __future__ import annotations

import pytest

from flatregister.registers.analysis import Analisys_NonUPPFileProcessor, Analisys_UPPFileProcessor

REGISTER = "analisys"


def _path_for(processor_class, upp_file, non_upp_file):
    return upp_file if processor_class is Analisys_UPPFileProcessor else non_upp_file


@pytest.mark.parametrize("processor_class", [Analisys_UPPFileProcessor,
                                             Analisys_NonUPPFileProcessor])
def test_анализ_отбрасывает_строки_с_курсивом(processor_class, analysis_upp_file,
                                            analysis_non_upp_file):
    """Строка счёта помечена курсивом и должна исчезнуть из отчёта."""
    path = _path_for(processor_class, analysis_upp_file, analysis_non_upp_file)

    result, _ = processor_class().process_file(path)

    assert not result.empty
    assert set(result["Аналитика"]) == {"Контрагент 1", "Контрагент 2"}
    assert "Курсив" not in result.columns
    assert "Уровень" not in result.columns


@pytest.mark.parametrize("processor_class", [Analisys_UPPFileProcessor,
                                             Analisys_NonUPPFileProcessor])
def test_анализ_сверка_нулевая(processor_class, analysis_upp_file,
                               analysis_non_upp_file):
    """Обороты не теряются и не задваиваются.

    У родителя нет корр. счёта, поэтому в сверку «до обработки» он не попадает,
    и сумма до совпадает с суммой после.
    """
    path = _path_for(processor_class, analysis_upp_file, analysis_non_upp_file)

    _, check = processor_class().process_file(path)

    assert not check.empty
    row = check.iloc[0]
    assert row["Разница_С_кред"] == 0, row.to_dict()
    assert row["Разница_В_дебет"] == 0, row.to_dict()
    assert row["Исх.файл"] == path.name


@pytest.mark.parametrize("processor_class", [Analisys_UPPFileProcessor,
                                             Analisys_NonUPPFileProcessor])
def test_анализ_субсчёт_взят_из_глубины_иерархии(processor_class, analysis_upp_file,
                                                  analysis_non_upp_file):
    """Субсчёт 60.01 лежит на первом уровне и обязан попасть в «Субсчет»."""
    path = _path_for(processor_class, analysis_upp_file, analysis_non_upp_file)

    result, _ = processor_class().process_file(path)

    assert set(result["Субсчет"]) == {"60.01"}
    assert "Корр_счет" in result.columns
    assert set(result["Корр_счет"]) == {"60.01"}


def test_упп_и_не_упп_дают_одинаковый_результат(analysis_upp_file, analysis_non_upp_file):
    """Две реализации обязаны сходиться на одинаковых данных.

    Они различаются только словами в заголовках; расхождение в цифрах означало
    бы, что одна из них считает иначе.
    """
    upp_result, _ = Analisys_UPPFileProcessor().process_file(analysis_upp_file)
    non_upp_result, _ = Analisys_NonUPPFileProcessor().process_file(analysis_non_upp_file)

    assert list(upp_result.columns) == list(non_upp_result.columns)
    assert len(upp_result) == len(non_upp_result)

    money = ["С кред. счетов", "В дебет счетов"]
    for column in money:
        assert list(upp_result[column]) == list(non_upp_result[column]), column


def test_анализ_не_распознанный_файл_даёт_понятную_ошибку(out_dir):
    """Файл без строки «Счет» должен отказывать словами, а не падать в pandas."""
    from tests.factories import GROUP_MARK, SheetBuilder

    builder = SheetBuilder()
    builder.add("Анализ счёта")
    builder.add(GROUP_MARK, GROUP_MARK, "Период", "Сумма")
    builder.add(GROUP_MARK, GROUP_MARK, "01.01.2024", 100)
    path = builder.save(out_dir / "не анализ.xlsx")

    with pytest.raises(Exception) as caught:
        Analisys_UPPFileProcessor().process_file(path)

    error = caught.value
    assert "Счет" in error.user_message, error.user_message
    assert error.hint, "у ошибки обязана быть подсказка"