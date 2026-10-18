# -*- coding: utf-8 -*-
"""Общие приспособления для тестов.

Две вещи здесь требуют внимания при чтении любого теста.

**Конфигурация.** Она читается из файла ``config.json`` в текущем каталоге,
поэтому тесты выполняются не из корня проекта, а из временной папки со своим
``config.json``. Иначе тесты зависели бы от настроек, которые на машине
разработчика меняют руками.

**Excel.** Ветка xlwings включается сама собой, когда в одной строке выгрузки
оборотов собраны все семь заголовков — а именно так выглядит настоящая выгрузка
1С. Фикстура :func:`без_excel` подменяет COM заглушкой.
"""

from __future__ import annotations

import json

import pytest

from flatregister.registers.base import FileProcessor
from tests import factories

#: Настройки, дающие однозначный результат: субсчета склеиваются, а
#: параллельная обработка выключена (её проверяют отдельным тестом).
TEST_CONFIG = {
    "accounts_without_subaccount": [["50", "50", 1]],
    "general_settings": {"parallel_processing": False},
    "keep_subaccounts": True,
}


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    """Тесты работают в своей временной папке со своим config.json."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(TEST_CONFIG, ensure_ascii=False, indent=4),
                           encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    from flatregister.config import clear_config_cache

    clear_config_cache()
    yield config_path
    clear_config_cache()


@pytest.fixture
def out_dir(tmp_path):
    """Папка для собранных .xlsx — всегда внутри tmp_path, в git не попадает."""
    folder = tmp_path / "fixtures"
    folder.mkdir()
    return folder


# --------------------------------------------------------------------------
# Фикстуры файлов: по одной на каждый из двенадцати вариантов выгрузки
# --------------------------------------------------------------------------

@pytest.fixture
def generalosv_upp_file(out_dir):
    return factories.generalosv_upp(out_dir / "ОСВ общая УПП.xlsx")


@pytest.fixture
def generalosv_non_upp_file(out_dir):
    return factories.generalosv_non_upp(out_dir / "ОСВ общая.xlsx")


@pytest.fixture
def accountosv_upp_file(out_dir):
    return factories.accountosv_upp(out_dir / "ОСВ счета УПП.xlsx")


@pytest.fixture
def accountosv_non_upp_file(out_dir):
    return factories.accountosv_non_upp(out_dir / "ОСВ счета.xlsx")


@pytest.fixture
def accountosv_upp_processed(accountosv_upp_file):
    """Готовый результат УПП-ОСВ — чтобы не прогонять обработку ради колонок."""
    from flatregister.registers.account_osv import AccountOSV_UPPFileProcessor
    return AccountOSV_UPPFileProcessor().process_file(accountosv_upp_file)


@pytest.fixture
def turnover_upp_file(out_dir):
    return factories.turnover_upp(out_dir / "Обороты счета УПП.xlsx")


@pytest.fixture
def turnover_non_upp_file(out_dir):
    return factories.turnover_non_upp(out_dir / "Обороты счета.xlsx")


@pytest.fixture
def analysis_upp_file(out_dir):
    return factories.analysis_upp(out_dir / "Анализ счета УПП.xlsx")


@pytest.fixture
def analysis_non_upp_file(out_dir):
    return factories.analysis_non_upp(out_dir / "Анализ счета.xlsx")


@pytest.fixture
def card_upp_file(out_dir):
    return factories.card_upp(out_dir / "Карточка счета УПП.xlsx")


@pytest.fixture
def card_non_upp_file(out_dir):
    return factories.card_non_upp(out_dir / "Карточка счета.xlsx")


@pytest.fixture
def posting_upp_file(out_dir):
    return factories.posting_upp(out_dir / "Проводки УПП.xlsx")


@pytest.fixture
def posting_non_upp_file(out_dir):
    return factories.posting_non_upp(out_dir / "Проводки.xlsx")


# --------------------------------------------------------------------------
# Носитель базовых методов
# --------------------------------------------------------------------------

class BareProcessor(FileProcessor):
    """Минимальный конкретный обработчик для юнит-тестов базовых методов.

    Сам :class:`FileProcessor` абстрактный, поэтому вызвать его
    ``shiftable_level`` и ``find_max_level_column`` нельзя. Этот класс ничего
    не делает — он нужен только носителем методов.
    """

    def process_file(self, file_path):
        raise NotImplementedError("BareProcessor используется только в юнит-тестах")


@pytest.fixture
def bare_processor():
    return BareProcessor()


# --------------------------------------------------------------------------
# Заглушка вместо настоящего Excel
# --------------------------------------------------------------------------

class _StubCell:
    """Ячейка с тем же интерфейсом, который использует код."""

    def __init__(self, value):
        self.api = self
        self.value = value

    @property
    def Text(self):
        """Отображаемое значение.

        У настоящей ячейки это строка, которую видит пользователь. В тестовых
        файлах форматирования нет, поэтому она совпадает со значением.
        """
        return "" if self.value is None else str(self.value)


class _StubSheet:
    def __init__(self, worksheet):
        self._sheet = worksheet

    def cells(self, row, column):
        return _StubCell(self._sheet.cell(row=row, column=column).value)


class _StubWorkbook:
    def __init__(self, path):
        import openpyxl

        self._workbook = openpyxl.load_workbook(path)
        self.sheets = [_StubSheet(self._workbook.active)]

    def close(self):
        self._workbook.close()


class _StubApp:
    """Заменяет и приложение Excel, и его коллекцию книг.

    Нужно ровно то, что вызывает код: ``xw.App()`` → ``app.books.open(path)``
    → ``workbook.sheets[0]`` → ``sheet.cells(row, col)`` → ``cell.api.Text``,
    плюс ``workbook.close()`` и ``app.quit()``. Поэтому ``books`` возвращает
    сам объект приложения — ровно так же устроен настоящий xlwings.
    """

    @property
    def books(self):
        return self

    def open(self, book_path):
        return _StubWorkbook(book_path)

    def quit(self):
        pass


class _StubXw:
    """Замена модуля ``xlwings``: от него нужен только вызов ``App``."""

    @staticmethod
    def App(visible=True):
        return _StubApp()


@pytest.fixture(autouse=True)
def без_excel(monkeypatch):
    """Подменяет xlwings заглушкой, чтобы тесты не поднимали настоящий Excel.

    Ветка xlwings включается, когда в одной строке выгрузки оборотов собраны
    все семь заголовков — а именно так выглядит настоящая выгрузка 1С. Значит
    фикстуры оборотов по определению запускают COM, и при десятках прогонов
    подряд Excel падал с ``0x800706ba`` (RPC недоступен): набор тестов
    становился нестабильным, а падение выглядело как ошибка обработки.

    Настоящая ветка xlwings проверяется отдельно — с маркером
    ``requires_excel`` на живой машине с установленным Excel.
    """
    import flatregister.registers.base as class_processor

    monkeypatch.setattr(class_processor, "xw", _StubXw)
    # Пауза в полсекунды нужна ради COM, в тестах она только тянет время.
    monkeypatch.setattr(class_processor.time, "sleep", lambda seconds: None)