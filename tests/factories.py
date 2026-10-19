# -*- coding: utf-8 -*-
"""Генератор правдоподобных выгрузок регистров 1С в виде .xlsx.

Зачем нужны
-----------
Реальные выгрузки 1С не попадают в репозиторий: они крупные и содержат
конфиденциальные данные. Но без файлов нечем проверить ни одну правку.

Поэтому тесты работают на маленьких синтетических файлах, собранных так же,
как их собирает 1С при «Сохранить как»:

* уровни группировки — это ``row_dimensions[i].outline_level`` в Excel;
* курсив — это ``cell.font.italic``;
* объединённые ячейки в шапке оборотов — чтобы сработала ветка xlwings;
* пустые столбцы между «Дебет» и «Кредит» — как в настоящих выгрузках.

Что генератор НЕ делает
-----------------------
Не ставит шрифт в объединённой шапке оборотов. Тогда openpyxl вернёт её
как ``Unnamed``-колонку и ветка xlwings не сработает — а её проверка живёт
в тесте с маркером ``requires_excel``.

Общий каркас
------------
``SheetBuilder`` собирает строки, уровни, курсив и объединения; ниже —
по одной функции на каждый регистр и его вариант УПП / не-УПП.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Sequence

from openpyxl import Workbook
from openpyxl.styles import Font


@dataclass
class SheetBuilder:
    """Сборка одного листа Excel «как из 1С»."""

    rows: list[list[Any]] = field(default_factory=list)
    levels: dict[int, int] = field(default_factory=dict)      # номер строки Excel -> уровень
    italics: set[tuple[int, int]] = field(default_factory=set)  # (строка, столбец) Excel, 1-based
    merged: list[str] = field(default_factory=list)

    def add(self, *values: Any, level: int = 0) -> "SheetBuilder":
        """Добавить строку. ``level`` — уровень группировки (0 = без группировки)."""
        self.rows.append(list(values))
        if level:
            self.levels[len(self.rows)] = level
        return self

    def blank(self, count: int = 1) -> "SheetBuilder":
        for _ in range(count):
            self.add(None)
        return self

    def italic(self, row: int, *columns: int) -> "SheetBuilder":
        """Пометить курсивом. Номера строк и столбцов — как в Excel, с единицы."""
        for column in columns:
            self.italics.add((row, column))
        return self

    def merge(self, cell_range: str) -> "SheetBuilder":
        self.merged.append(cell_range)
        return self

    def save(self, path: Path) -> Path:
        workbook = Workbook()
        sheet = workbook.active
        for row_number, values in enumerate(self.rows, start=1):
            for column_number, value in enumerate(values, start=1):
                if value is not None:
                    sheet.cell(row=row_number, column=column_number, value=value)

        for row_number, level in self.levels.items():
            sheet.row_dimensions[row_number].outline_level = level

        for row_number, column_number in self.italics:
            sheet.cell(row=row_number, column=column_number).font = Font(italic=True)

        for cell_range in self.merged:
            sheet.merge_cells(cell_range)

        path.parent.mkdir(parents=True, exist_ok=True)
        workbook.save(path)
        return path


# --------------------------------------------------------------------------
# Общая ОСВ (generalosv)
# --------------------------------------------------------------------------

#: Маркер группировки в первых двух колонках Excel.
#:
#: Настоящая выгрузка 1С несёт в них знаки «+»/«−» слева от строк, и это
#: единственная причина, по которой предобработка **не выбрасывает** эти
#: колонки: в коде стоит ``dropna(axis=1, how='all')``. Если колонки пустые,
#: они исчезают, а шапка сдвигается — и колонки данных молча меняются местами.
#: Поэтому генератор кладёт маркер в каждую строку, включая шапку, и разметка
#: становится детерминированной.
GROUP_MARK = "+"


def generalosv_upp(path: Path, *, rows: Sequence[tuple[str, int, int, int, int, int]] | None = None) -> Path:
    """ОСВ из 1С УПП.

    Шапка состоит из двух строк: в первой названия блоков («Сальдо на начало
    периода»), во второй — «Дебет»/«Кредит» под каждым блоком. Именно по второй
    строке процессор ищет колонку «Кредит».
    """
    rows = rows or _DEFAULT_GENERAL_ROWS
    builder = SheetBuilder()
    builder.add("Оборотно-сальдовая ведомость")            # строка 1 — отбрасывается предобработкой
    builder.add("Период: 01.01.2024 - 31.12.2024")
    builder.add(GROUP_MARK, GROUP_MARK, "Счет", "Наименование",
                "Сальдо на начало периода", None,
                "Сальдо на конец периода", None,
                "Оборот за период", None)
    # Подшапка «Дебет/Кредит» — её процессор снимает после разметки колонок
    builder.add(GROUP_MARK, GROUP_MARK, None, "Наименование",
                "Дебет", "Кредит", "Дебет", "Кредит", "Дебет", "Кредит")

    for account, level, saldo_start, saldo_end, debit_turnover, credit_turnover in rows:
        builder.add(GROUP_MARK, GROUP_MARK, account, account.replace(".", " "),
                    saldo_start, 0, saldo_end, 0, debit_turnover, credit_turnover,
                    level=level)
    return builder.save(path)


def generalosv_non_upp(path: Path, *, rows: Sequence[tuple[str, int, int, int, int, int]] | None = None) -> Path:
    """ОСВ из 1С (не УПП): те же данные, но названия блоков во множественном числе
    и колонка называется «Наименование счета»."""
    rows = rows or _DEFAULT_GENERAL_ROWS
    builder = SheetBuilder()
    builder.add("Оборотно-сальдовая ведомость")
    builder.add("Период: 01.01.2024 - 31.12.2024")
    builder.add(GROUP_MARK, GROUP_MARK, "Счет", "Наименование счета",
                "Сальдо на начало периода", None,
                "Сальдо на конец периода", None,
                "Обороты за период", None)
    builder.add(GROUP_MARK, GROUP_MARK, None, "Наименование счета",
                "Дебет", "Кредит", "Дебет", "Кредит", "Дебет", "Кредит")
    for account, level, saldo_start, saldo_end, debit_turnover, credit_turnover in rows:
        builder.add(GROUP_MARK, GROUP_MARK, account, account.replace(".", " "),
                    saldo_start, 0, saldo_end, 0, debit_turnover, credit_turnover,
                    level=level)
    return builder.save(path)


#: (счёт, уровень, сальдо на начало Дт, сальдо на конец Дт, оборот Дт, оборот Кт)
_DEFAULT_GENERAL_ROWS = [
    ("60", 1, 10_000, 12_000, 50_000, 48_000),
    ("60.01", 2, 6_000, 7_000, 30_000, 28_000),
    ("60.02", 2, 4_000, 5_000, 20_000, 20_000),
    ("62", 1, 1_000, 1_500, 5_000, 4_500),
]


# --------------------------------------------------------------------------
# ОСВ счёта (accountosv)
# --------------------------------------------------------------------------

#: (субконто, уровень, сальдо на начало Дт, сальдо на конец Дт, оборот Дт, оборот Кт)
_DEFAULT_ACCOUNTOSV_ROWS = [
    ("60.01", 1, 1_000, 2_000, 5_000, 4_000),
    ("Контрагент 1", 2, 500, 1_000, 3_000, 2_000),
    ("Контрагент 2", 2, 500, 1_000, 2_000, 2_000),
]


def _accountosv(path: Path, rows: Sequence[tuple[str, int, int, int, int, int]],
                *, account: str, account_header: str, turnover_header: str,
                row_header: str) -> Path:
    """Общий каркас ОСВ счёта: отличается только набором заголовков.

    Разница между УПП и не-УПП ровно в трёх словах, поэтому собирать
    приходится одной функцией: если разъедутся, тесты это покажут, а не
    «как-нибудь разошлось».
    """
    rows = list(rows)
    builder = SheetBuilder()
    # Строка 1 отбрасывается предобработкой, но именно в ней живёт название
    # счёта, из которого AccountOSV_UPPFileProcessor берёт номер счёта.
    builder.add(f"Оборотно-сальдовая ведомость по счету {account}")
    builder.add("Период: 01.01.2024 - 31.12.2024")
    builder.add(GROUP_MARK, GROUP_MARK, account_header, row_header,
                "Сальдо на начало периода", None,
                "Сальдо на конец периода", None,
                turnover_header, None)
    builder.add(GROUP_MARK, GROUP_MARK, None, row_header,
                "Дебет", "Кредит", "Дебет", "Кредит", "Дебет", "Кредит")

    for subkonto, level, saldo_start, saldo_end, debit_turnover, credit_turnover in rows:
        builder.add(GROUP_MARK, GROUP_MARK, subkonto, subkonto if subkonto == account else "",
                    saldo_start, 0, saldo_end, 0, debit_turnover, credit_turnover,
                    level=level)

    # Строка «Итого» — по ней идёт сверка оборотов; без неё обработчик
    # сознательно отказывается работать.
    #
    # Считаем итог ТОЛЬКО по строкам-листьям. Родительские строки при
    # обработке отбрасываются как задвоенные, и если включить их в «Итого»,
    # сверка покажет ненулевую разницу на заведомо корректной выгрузке.
    builder.add(GROUP_MARK, GROUP_MARK, "Итого", "", *_leaves_total(rows), level=1)
    return builder.save(path)


def _leaves_total(rows: Sequence[tuple[str, int, int, int, int, int]]) -> list[int]:
    """Сумма денежных значений по строкам, у которых нет дочерних."""
    deepest = max(level for _, level, *_ in rows)
    leaves = [row for row in rows if row[1] == deepest]
    totals = [0, 0, 0, 0, 0, 0]
    for _, _, saldo_start, saldo_end, debit_turnover, credit_turnover in leaves:
        for position, value in enumerate([saldo_start, 0, saldo_end, 0,
                                         debit_turnover, credit_turnover]):
            totals[position] += value
    return totals


def accountosv_upp(path: Path, *, rows=None) -> Path:
    return _accountosv(path, rows or _DEFAULT_ACCOUNTOSV_ROWS,
                       account="60.01", account_header="Субконто",
                       row_header="Наименование", turnover_header="Оборот за период")


def accountosv_non_upp(path: Path, *, rows=None) -> Path:
    return _accountosv(path, rows or _DEFAULT_ACCOUNTOSV_ROWS,
                       account="60.01", account_header="Счет",
                       row_header="Наименование", turnover_header="Обороты за период")


# --------------------------------------------------------------------------
# Обороты счёта (turnover)
# --------------------------------------------------------------------------

#: Одна строка оборотов.
#: (субконто, уровень, нач. Дт, нач. Кт, об. Дт, об. Кт, кон. Дт, кон. Кт,
#:  корр. Дт 62, корр. Дт 62.01, корр. Кт 90, корр. Кт 90.01)
#:
#: Колонки корреспондирующих счетов стоят МЕЖДУ денежными блоками — это и есть
#: признак, по которому процессор их находит: он берёт всё, что между
#: «Дебет_оборот» и «Кредит_оборот», и добавляет суффикс `_до`, а между
#: «Кредит_оборот» и «Дебет_конец» — `_ко`.
#:
#: Обороты кладутся именно в колонки СУБСЧЁТОВ (62.01, 90.01), а в колонках
#: родителей (62, 90) — нули. Родительская колонка отбрасывается как
#: задвоенная: если положить в неё суммы, они молча исчезнут вместе с ней.
#:
#: Суммы корр. счетов на листьях совпадают с оборотами строки: сверка считает
#: «оборот после» как сумму колонок `_до` минус сумма `_ко`, и ненулевая
#: разница на корректной выгрузке означала бы дыру в алгоритме.
_TurnoverRow = tuple[str, int, int, int, int, int, int, int, int, int, int, int]

_DEFAULT_TURNOVER_ROWS: list[_TurnoverRow] = [
    ("60.01", 1, 1_000, 0, 5_000, 4_000, 2_000, 0, 0, 0, 0, 0),
    ("Поставщик А", 2, 600, 0, 3_000, 2_000, 1_600, 0, 0, 3_000, 0, 2_000),
    ("Поставщик Б", 2, 400, 0, 2_000, 2_000, 400, 0, 0, 2_000, 0, 2_000),
]


def _turnover(path: Path, rows: Sequence[_TurnoverRow], *, account_header: str,
              balance_headers: tuple[str, str, str, str, str, str]) -> Path:
    """Обороты счёта. Разница УПП и не-УПП — только в словах заголовков.

    Пустых строк перед шапкой здесь нет намеренно: обработчик ищет строку
    заголовков через ``idxmax()`` (метка) и читает её через ``iloc``
    (позиция). Любая полностью пустая строка перед шапкой сдвигает одно
    относительно другого, и шапка выбирается не та.
    """
    (start_debit, start_credit, debit_turnover, credit_turnover,
     end_debit, end_credit) = balance_headers

    builder = SheetBuilder()
    builder.add("Обороты счёта")
    builder.add(GROUP_MARK, GROUP_MARK, account_header, start_debit, start_credit,
                "60", "60.01", debit_turnover, "62", "62.01",
                credit_turnover, "90", "90.01", end_debit, end_credit)
    builder.add("Период: 01.01.2024 - 31.12.2024")

    for (subkonto, level, nd, nk, td, tk, kd, kk,
         corr_debit, corr_debit_sub, corr_credit, corr_credit_sub) in rows:
        builder.add(GROUP_MARK, GROUP_MARK, subkonto, nd, nk,
                    0, 0, td,
                    corr_debit, corr_debit_sub,
                    tk, corr_credit, corr_credit_sub, kd, kk,
                    level=level)

    builder.add(GROUP_MARK, GROUP_MARK, "Итого", *_leaves_total_turnover(rows), level=1)
    return builder.save(path)


def _leaves_total_turnover(rows: Sequence[_TurnoverRow]) -> list[int]:
    """Итог по листьям, разложенный по колонкам строки.

    Возвращает ровно столько же значений, сколько колонок у строки данных.
    Нули в колонках корр. счетов обязательны: если строка «Итого» окажется
    короче шапки, openpyxl не допишет ей хвост, и все денежные значения после
    неё поедут на соседние колонки.

    Нумерация ниже — это номера колонок Excel в строке «Итого» начиная с
    третьей (нулевая и первая заняты маркерами группировки, вторая — словом
    «Итого»).
    """
    deepest = max(row[1] for row in rows)
    leaves = [row for row in rows if row[1] == deepest]
    totals = [0] * 12
    # столбец Excel -> индекс в кортеже строки: 0 субконто, 1 уровень,
    # далее нач.Дт, нач.Кт, об.Дт, об.Кт, кон.Дт, кон.Кт
    for target, source in {0: 2, 1: 3, 4: 4, 7: 5, 10: 6, 11: 7}.items():
        totals[target] = sum(row[source] for row in leaves)
    return totals


_UPP_BALANCE = ("Нач. сальдо деб.", "Нач. сальдо кред.",
                "Деб. оборот", "Кред. оборот",
                "Кон. сальдо деб.", "Кон. сальдо кред.")

_NON_UPP_BALANCE = ("Начальное сальдо Дт", "Начальное сальдо Кт",
                    "Оборот Дт", "Оборот Кт",
                    "Конечное сальдо Дт", "Конечное сальдо Кт")


def turnover_upp(path: Path, *, rows=None) -> Path:
    return _turnover(path, rows or _DEFAULT_TURNOVER_ROWS,
                     account_header="Субконто", balance_headers=_UPP_BALANCE)


def turnover_non_upp(path: Path, *, rows=None) -> Path:
    return _turnover(path, rows or _DEFAULT_TURNOVER_ROWS,
                     account_header="Счет", balance_headers=_NON_UPP_BALANCE)


# --------------------------------------------------------------------------
# Анализ счёта (analysis)
# --------------------------------------------------------------------------

#: (аналитика, уровень, корр. счёт, оборот в кредит, оборот в дебет, курсив)
#:
#: Анализ счёта — единственный регистр, где значим КУРСИВ. 1С помечает им
#: строки с промежуточными итогами, и обработчик их удаляет. Поэтому курсив
#: стоит на РОДИТЕЛЕ, а данные несут строки без курсива. Если пометить
#: наоборот, из отчёта выпадет всё, что в нём должно быть.
#:
#: У родителя корр. счёта НЕТ: это строка самого счёта. Из-за этого он не
#: попадает в сверку «до обработки» — иначе обороты задваивались бы и разница
#: никогда не была бы нулевой.
_DEFAULT_ANALYSIS_ROWS = [
    ("60.01", 1, None, 5_000, 4_000, True),
    ("Контрагент 1", 2, "60.01", 3_000, 2_000, False),
    ("Контрагент 2", 2, "60.01", 2_000, 2_000, False),
]

#: (аналитика, уровень, корр. счёт, оборот, курсив)
_AnalysisRow = tuple[str, int, str | None, int, int, bool]


def _analysis(path: Path, rows: Sequence[_AnalysisRow],
              *, kor_schet_header: str, credit_header: str, debit_header: str,
              italic_column: int) -> Path:
    """Анализ счёта.

    Отличие УПП от не-УПП — три слова в заголовках: «Кор.счет» / «Кор. Счет»
    и «С кред. счетов» / «В дебет счетов» против «Дебет» / «Кредит».
    Обработчик приводит не-УПП к тем же именам переименованием, поэтому сами
    данные в фикстуре одинаковые.
    """
    builder = SheetBuilder()
    builder.add("Анализ счёта")
    builder.add(GROUP_MARK, GROUP_MARK, "Счет", kor_schet_header,
                credit_header, debit_header)

    for analytic, level, kor_schet, credit, debit, italic in rows:
        builder.add(GROUP_MARK, GROUP_MARK, analytic, kor_schet, credit, debit,
                    level=level)
        if italic:
            builder.italic(len(builder.rows), italic_column)

    return builder.save(path)


def analysis_upp(path: Path, *, rows=None) -> Path:
    return _analysis(path, rows or _DEFAULT_ANALYSIS_ROWS,
                     kor_schet_header="Кор.счет", credit_header="С кред. счетов",
                     debit_header="В дебет счетов", italic_column=4)


def analysis_non_upp(path: Path, *, rows=None) -> Path:
    return _analysis(path, rows or _DEFAULT_ANALYSIS_ROWS,
                     kor_schet_header="Кор. Счет", credit_header="Дебет",
                     debit_header="Кредит", italic_column=4)


# --------------------------------------------------------------------------
# Карточка счёта (card) и отчёт по проводкам (posting)
# --------------------------------------------------------------------------

#: (дата, документ, содержание/операция, дебет, кредит, текущее сальдо)
_CARD_ROWS = [
    ("01.01.2024 10:00:00", "Продажа 1", "Запись", 100, 0, 100),
    ("01.01.2024 11:30:00", "Покупка 1", "Запись", 0, 50, 50),
    ("02.01.2024 09:15:00", "Продажа 2", "Запись", 200, 0, 250),
]


def card_upp(path: Path, *, rows=None) -> Path:
    """Карточка счёта из 1С УПП.

    Отличается от не-УПП шапкой: здесь строка заголовков ищется по слову
    «Дата» в первой колонке, там — по слову «Период».
    """
    rows = rows or _CARD_ROWS
    builder = SheetBuilder()
    builder.add("Карточка счёта")
    builder.add("Дата", "Документ", "Операция", "Дебет", "Кредит", "Текущее сальдо")
    for date, document, operation, debit, credit, balance in rows:
        builder.add(date, document, operation, debit, credit, balance)
    return builder.save(path)


def card_non_upp(path: Path, *, rows=None) -> Path:
    """Карточка счёта из 1С (не УПП): шапка ищется по строке «Период».

    Пустая колонка «Показатели» нужна, чтобы отработала ветка извлечения
    количественных и валютных значений: на пустой выгрузке она обязана
    молча ничего не добавить.

    Даты здесь БЕЗ времени: не-УПП читает период только по шаблону
    ``%d.%m.%Y``, и при наличии часов все строки превращаются в NaT и
    отбрасываются — файл уходит пустым.
    """
    rows = rows or _CARD_ROWS
    builder = SheetBuilder()
    builder.add("Карточка счёта")
    builder.add("Период", "Документ", "Дебет", None, "Кредит", None,
                "Аналитика Дт", None, "Аналитика Кт", None, "Показатели")
    for date, document, _operation, debit, credit, _balance in rows:
        builder.add(date.split(" ")[0], document, debit, "", credit, "",
                    "ООО Ромашка", "", "ООО Ромашка", "", "")
    return builder.save(path)


def posting_upp(path: Path, *, rows=None) -> Path:
    """Отчёт по проводкам из 1С УПП."""
    rows = rows or _CARD_ROWS
    builder = SheetBuilder()
    builder.add("Отчёт по проводкам")
    builder.add("Дата", "Документ", "Содержание", "Субконто Дт", "Субконто Кт",
                "Дт", "Кт", "Сумма")
    for date, document, content, debit, credit, _balance in rows:
        builder.add(date, document, content, "Контрагент 1", "Контрагент 2",
                    debit, credit, debit + credit)
    return builder.save(path)


def posting_non_upp(path: Path, *, rows=None) -> Path:
    """Отчёт по проводкам из 1С (не УПП): шапка ищется по строке «Период».

    Даты без времени — по той же причине, что и в карточке счёта.
    """
    rows = rows or _CARD_ROWS
    builder = SheetBuilder()
    builder.add("Отчёт по проводкам")
    builder.add("Период", "Документ", "Дебет", None, "Кредит", None,
                "Аналитика Дт", None, "Аналитика Кт", None, "Показатели")
    for date, document, _content, debit, credit, _balance in rows:
        builder.add(date.split(" ")[0], document, debit, "", credit, "",
                    "ООО Ромашка", "", "ООО Ромашка", "", "")
    return builder.save(path)