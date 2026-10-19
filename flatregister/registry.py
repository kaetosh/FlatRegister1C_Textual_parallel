# -*- coding: utf-8 -*-
"""Единый реестр регистров: меню, распознавание, порядок колонок, требования.

До Сессии S8 регистр приходилось прописывать в четырёх местах — в меню
``Select``, в ``FileProcessorFactory.REGISTER_PATTERNS``, в ``DESIRED_ORDER``
и в ``text.REQUIREMENTS``. Пропущенный пункт не ломал импорт: регистр просто
исчезал из меню или падал на первом же файле. Теперь все четыре вещи лежат
в одной записи :class:`RegisterEntry`.
"""
from dataclasses import dataclass
from typing import AbstractSet, Dict, List, Optional, Tuple, Type

from flatregister.registers.account_osv import AccountOSV_NonUPPFileProcessor, AccountOSV_UPPFileProcessor
from flatregister.registers.analysis import Analisys_NonUPPFileProcessor, Analisys_UPPFileProcessor
from flatregister.registers.base import FileProcessor
from flatregister.registers.card import Card_NonUPPFileProcessor, Card_UPPFileProcessor
from flatregister.registers.general_osv import GeneralOSV_NonUPPFileProcessor, GeneralOSV_UPPFileProcessor
from flatregister.registers.posting import Posting_NonUPPFileProcessor, Posting_UPPFileProcessor
from flatregister.registers.turnover import Turnover_NonUPPFileProcessor, Turnover_UPPFileProcessor
from text import (
    TEXT_ACCOUNT_ANALYSIS,
    TEXT_ACCOUNT_CARD,
    TEXT_ACCOUNT_TURNOVER,
    TEXT_BALANCE_SHEET_ACCOUNT,
    TEXT_REQUIREMENT_TRANSACTION_REPORT,
    TEXT_TOTAL_TURNOVER_BALANCE_SHEET,
)

#: Вариант выгрузки: УПП или остальные конфигурации.
VARIANT_UPP = "upp"
VARIANT_NOT_UPP = "not_upp"

#: Как результат раскладывается по листам итоговой книги.
KIND_UPP_NON_UPP = "upp_non_upp"
KIND_ANALYTICAL = "analytical"


@dataclass(frozen=True)
class RegisterPattern:
    """Набор заголовков, по которому узнаётся вариант выгрузки."""

    headers: AbstractSet[str]
    processor: Type[FileProcessor]
    variant: str


@dataclass(frozen=True)
class RegisterEntry:
    """Всё, что нужно знать о регистре за пределами его процессоров."""

    key: str
    title: str
    menu_number: int
    requirements: str
    patterns: Tuple[RegisterPattern, ...]
    column_order: Dict[str, Tuple[str, ...]]
    kind: str

    def variant_for(self, processor_class: Type[FileProcessor]) -> Optional[str]:
        """Вернуть ``upp``/``not_upp`` для класса процессора."""
        for pattern in self.patterns:
            if pattern.processor is processor_class:
                return pattern.variant
        return None


def _pattern(headers: AbstractSet[str], processor: Type[FileProcessor], variant: str) -> RegisterPattern:
    return RegisterPattern(headers=frozenset(headers), processor=processor, variant=variant)


# Порядок вставки сохранён прежним (как в REGISTER_PATTERNS): от него зависит
# порядок разбора файлов, подходящих под несколько регистров.
REGISTER_SPEC: Dict[str, RegisterEntry] = {
    "card": RegisterEntry(
        key="card",
        title="Карточка счета",
        menu_number=2,
        requirements=TEXT_ACCOUNT_CARD,
        patterns=(
            _pattern({"дата", "документ", "операция"}, Card_UPPFileProcessor, VARIANT_UPP),
            _pattern({"период", "аналитика дт", "аналитика кт"}, Card_NonUPPFileProcessor, VARIANT_NOT_UPP),
        ),
        column_order={
            VARIANT_UPP: (
                "Имя_файла", "Дата", "Документ", "Дебет", "Дебет_значение", "Кредит",
                "Кредит_значение", "Текущее сальдо", "Текущее сальдо_значение",
                "Операция_1", "Операция_2", "Операция_3", "Операция_4", "Операция_5",
                "Операция_6", "Дт_количество", "Кт_количество", "Дт_валюта",
                "Дт_валютая_сумма", "Кт_валюта", "Кт_валютая_сумма",
            ),
            VARIANT_NOT_UPP: (
                "Имя_файла", "Период", "Дебет", "Дебет_значение", "Кредит",
                "Кредит_значение", "Текущее сальдо", "Текущее сальдо_значение",
                "Документ_1", "Документ_2", "Аналитика Дт_1", "Аналитика Дт_2",
                "Аналитика Дт_3", "Аналитика Дт_4", "Аналитика Кт_1", "Аналитика Кт_2",
                "Аналитика Кт_3", "Аналитика Кт_4", "Дебет_количество",
                "Кредит_количество", "Дебет_валюта", "Дебет_валютное_количество",
                "Кредит_валюта", "Кредит_валютное_количество",
            ),
        },
        kind=KIND_UPP_NON_UPP,
    ),
    "posting": RegisterEntry(
        key="posting",
        title="Отчет по проводкам",
        menu_number=1,
        requirements=TEXT_REQUIREMENT_TRANSACTION_REPORT,
        patterns=(
            _pattern({"дата", "документ", "содержание", "дт", "кт", "сумма"}, Posting_UPPFileProcessor, VARIANT_UPP),
            _pattern({"период", "аналитика дт", "аналитика кт"}, Posting_NonUPPFileProcessor, VARIANT_NOT_UPP),
        ),
        column_order={
            VARIANT_UPP: (
                "Имя_файла", "Дата", "Документ", "Дт", "Кт", "Сумма",
                "Содержание_1", "Содержание_2", "Содержание_3",
                "Субконто Дт1", "Субконто Дт2", "Субконто Дт3",
                "Субконто Кт1", "Субконто Кт2", "Субконто Кт3",
                "Дт_количество", "Кт_количество",
                "Дт_валюта", "Дт_валюта_количество",
                "Кт_валюта", "Кт_валюта_количество",
            ),
            VARIANT_NOT_UPP: (
                "Имя_файла", "Период", "Дебет", "Дебет_значение", "Кредит", "Кредит_значение",
                "Документ_1", "Документ_2",
                "Аналитика Дт_1", "Аналитика Дт_2", "Аналитика Дт_3", "Аналитика Дт_4",
                "Аналитика Кт_1", "Аналитика Кт_2", "Аналитика Кт_3", "Аналитика Кт_4",
                "Дебет_количество", "Кредит_количество",
                "Дебет_валюта", "Дебет_валютное_количество",
                "Кредит_валюта", "Кредит_валютное_количество",
            ),
        },
        kind=KIND_UPP_NON_UPP,
    ),
    "analisys": RegisterEntry(
        key="analisys",
        title="Анализ счета",
        menu_number=3,
        requirements=TEXT_ACCOUNT_ANALYSIS,
        patterns=(
            _pattern({"счет", "кор.счет", "с кред. счетов", "в дебет счетов"}, Analisys_UPPFileProcessor, VARIANT_UPP),
            _pattern({"счет", "кор. счет", "дебет", "кредит"}, Analisys_NonUPPFileProcessor, VARIANT_NOT_UPP),
        ),
        column_order={
            VARIANT_UPP: (
                "Исх.файл", "Субсчет", "Аналитика", "Корр_счет", "Субконто_корр_счета",
                "Вид связи КА за период", "С кред. счетов",
                "С кред. счетов_КОЛ", "С кред. счетов_ВАЛ", "В дебет счетов",
                "В дебет счетов_КОЛ", "В дебет счетов_ВАЛ",
                "Level_0", "Level_1", "Level_2",
            ),
            VARIANT_NOT_UPP: (
                "Исх.файл", "Субсчет", "Аналитика", "Корр_счет", "Субконто_корр_счета",
                "Вид связи КА за период", "С кред. счетов",
                "С кред. счетов_КОЛ", "С кред. счетов_ВАЛ", "В дебет счетов",
                "В дебет счетов_КОЛ", "В дебет счетов_ВАЛ",
                "Level_0", "Level_1", "Level_2",
            ),
        },
        kind=KIND_ANALYTICAL,
    ),
    "turnover": RegisterEntry(
        key="turnover",
        title="Обороты счета",
        menu_number=4,
        requirements=TEXT_ACCOUNT_TURNOVER,
        patterns=(
            _pattern(
                {"субконто", "нач. сальдо деб.", "нач. сальдо кред.", "деб. оборот",
                 "кред. оборот", "кон. сальдо деб.", "кон. сальдо кред."},
                Turnover_UPPFileProcessor,
                VARIANT_UPP,
            ),
            _pattern(
                {"счет", "начальное сальдо дт", "начальное сальдо кт", "оборот дт",
                 "оборот кт", "конечное сальдо дт", "конечное сальдо кт"},
                Turnover_NonUPPFileProcessor,
                VARIANT_NOT_UPP,
            ),
        ),
        column_order={
            VARIANT_UPP: (
                "Исх.файл", "Субконто",
                "Дебет_начало", "Количество_Дебет_начало", "ВалютнаяСумма_Дебет_начало",
                "Кредит_начало", "Количество_Кредит_начало", "ВалютнаяСумма_Кредит_начало",
                "Дебет_оборот", "Количество_Дебет_оборот", "ВалютнаяСумма_Дебет_оборот",
                "Кредит_оборот", "Количество_Кредит_оборот", "ВалютнаяСумма_Кредит_оборот",
                "Дебет_конец", "Количество_Дебет_конец", "ВалютнаяСумма_Дебет_конец",
                "Кредит_конец", "Количество_Кредит_конец", "ВалютнаяСумма_Кредит_конец",
                "Валюта", "Начало периода  для вида связи", "Конец  периода для вида связи",
                "Вид связи КА за период", "Level_0", "Level_1", "Level_2",
            ),
            VARIANT_NOT_UPP: (
                "Исх.файл", "Субконто",
                "Дебет_начало", "Количество_Дебет_начало", "ВалютнаяСумма_Дебет_начало",
                "Кредит_начало", "Количество_Кредит_начало", "ВалютнаяСумма_Кредит_начало",
                "Дебет_оборот", "Количество_Дебет_оборот", "ВалютнаяСумма_Дебет_оборот",
                "Кредит_оборот", "Количество_Кредит_оборот", "ВалютнаяСумма_Кредит_оборот",
                "Дебет_конец", "Количество_Дебет_конец", "ВалютнаяСумма_Дебет_конец",
                "Кредит_конец", "Количество_Кредит_конец", "ВалютнаяСумма_Кредит_конец",
                "Валюта", "Начало периода  для вида связи", "Конец  периода для вида связи",
                "Вид связи КА за период", "Level_0", "Level_1", "Level_2",
            ),
        },
        kind=KIND_ANALYTICAL,
    ),
    "accountosv": RegisterEntry(
        key="accountosv",
        title="ОСВ счета",
        menu_number=5,
        requirements=TEXT_BALANCE_SHEET_ACCOUNT,
        patterns=(
            _pattern({"субконто", "сальдо на начало периода", "оборот за период",
                      "сальдо на конец периода"}, AccountOSV_UPPFileProcessor, VARIANT_UPP),
            _pattern({"счет", "сальдо на начало периода", "обороты за период",
                      "сальдо на конец периода"}, AccountOSV_NonUPPFileProcessor, VARIANT_NOT_UPP),
        ),
        column_order={
            VARIANT_UPP: (
                "Исх.файл", "Субконто",
                "Дебет_начало", "Количество_Дебет_начало", "ВалютнаяСумма_Дебет_начало",
                "Кредит_начало", "Количество_Кредит_начало", "ВалютнаяСумма_Кредит_начало",
                "Дебет_оборот", "Количество_Дебет_оборот", "ВалютнаяСумма_Дебет_оборот",
                "Кредит_оборот", "Количество_Кредит_оборот", "ВалютнаяСумма_Кредит_оборот",
                "Дебет_конец", "Количество_Дебет_конец", "ВалютнаяСумма_Дебет_конец",
                "Кредит_конец", "Количество_Кредит_конец", "ВалютнаяСумма_Кредит_конец",
                "Валюта", "Начало периода  для вида связи", "Конец  периода для вида связи",
                "Вид связи КА за период", "Level_0", "Level_1", "Level_2",
            ),
            VARIANT_NOT_UPP: (
                "Исх.файл", "Субконто",
                "Дебет_начало", "Количество_Дебет_начало", "ВалютнаяСумма_Дебет_начало",
                "Кредит_начало", "Количество_Кредит_начало", "ВалютнаяСумма_Кредит_начало",
                "Дебет_оборот", "Количество_Дебет_оборот", "ВалютнаяСумма_Дебет_оборот",
                "Кредит_оборот", "Количество_Кредит_оборот", "ВалютнаяСумма_Кредит_оборот",
                "Дебет_конец", "Количество_Дебет_конец", "ВалютнаяСумма_Дебет_конец",
                "Кредит_конец", "Количество_Кредит_конец", "ВалютнаяСумма_Кредит_конец",
                "Валюта", "Начало периода  для вида связи", "Конец  периода для вида связи",
                "Вид связи КА за период", "Level_0", "Level_1", "Level_2",
            ),
        },
        kind=KIND_ANALYTICAL,
    ),
    "generalosv": RegisterEntry(
        key="generalosv",
        title="ОСВ общая",
        menu_number=6,
        requirements=TEXT_TOTAL_TURNOVER_BALANCE_SHEET,
        patterns=(
            _pattern({"счет", "сальдо на начало периода", "оборот за период",
                      "сальдо на конец периода"}, GeneralOSV_UPPFileProcessor, VARIANT_UPP),
            _pattern({"счет", "наименование счета", "сальдо на начало периода",
                      "обороты за период", "сальдо на конец периода"},
                     GeneralOSV_NonUPPFileProcessor, VARIANT_NOT_UPP),
        ),
        column_order={
            VARIANT_UPP: (
                "Исх.файл", "Счет", "Наименование", "Дебет_начало", "Кредит_начало",
                "Дебет_оборот", "Кредит_оборот", "Дебет_конец", "Кредит_конец",
            ),
            VARIANT_NOT_UPP: (
                "Исх.файл", "Счет", "Наименование", "Дебет_начало", "Кредит_начало",
                "Дебет_оборот", "Кредит_оборот", "Дебет_конец", "Кредит_конец",
            ),
        },
        kind=KIND_ANALYTICAL,
    ),
}


def get_register(register_type: str) -> Optional[RegisterEntry]:
    """Запись реестра или ``None`` для неизвестного ключа."""
    return REGISTER_SPEC.get(register_type)


def column_order(register_type: str, variant: str) -> List[str]:
    """Порядок колонок регистра; пустой список для неизвестного регистра."""
    entry = get_register(register_type)
    if entry is None:
        return []
    return list(entry.column_order.get(variant, ()))


def select_options() -> List[Tuple[str, str]]:
    """Пункты меню в порядке нумерации: ``[(«1. …», key), …]``."""
    entries = sorted(REGISTER_SPEC.values(), key=lambda entry: entry.menu_number)
    return [(f"{entry.menu_number}. {entry.title}", entry.key) for entry in entries]
