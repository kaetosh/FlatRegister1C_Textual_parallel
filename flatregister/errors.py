# -*- coding: utf-8 -*-
"""Иерархия ошибок приложения и единый формат отчёта о неудачной обработке файла.

Зачем это отдельный модуль
--------------------------
Раньше ошибки были восемью плоскими классами без общего предка. Следствия:

* пользователю показывался ``str(exception)`` — то есть либо служебное
  сообщение внутренней библиотеки, либо текст исключения без объяснения, что делать;
* полный трейсбек терялся — восстановить, на какой строке упало, было нечем;
* классифицировать ошибки в пакетном режиме было невозможно: все они были
  «прочие».

Теперь у любой ошибки приложения есть две части:

``user_message``
    Что показать пользователю. По-русски, без указания файлов и строк кода.
``hint``
    Что пользователю делать дальше. Необязателен, но почти всегда полезен.

Всё техническое — тип исключения, трейсбек, имя модуля — уходит в лог
(см. :mod:`flatregister.logging_setup`), а не на экран.
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass


class FlatRegisterError(Exception):
    """Базовая ошибка приложения.

    Наружу отдаёт :attr:`user_message` и :attr:`hint`, а не ``str(self)``:
    так текст на экране и текст в логе гарантированно не смешиваются.
    """

    #: Текст по умолчанию, если подкласс не передал его явно.
    default_user_message = "Не удалось обработать файл."

    def __init__(self, user_message: str | None = None, *, hint: str | None = None):
        self.user_message = user_message or self.default_user_message
        self.hint = hint
        super().__init__(self.user_message)

    def __str__(self) -> str:
        # Осознанно НЕ включаем hint: str() идёт в сообщения там, где достаточно
        # одной строки. Полная картина — в FileFailure и в логе.
        return self.user_message


# --------------------------------------------------------------------------
# Ввод: путь, файл, папка
# --------------------------------------------------------------------------

class InputPathError(FlatRegisterError):
    """Указанный путь не существует или нечитаем."""

    default_user_message = "Неверный путь к файлу или папке."


class NotSpreadsheetError(FlatRegisterError):
    """Файл выбран, но это не .xlsx."""

    default_user_message = "Этот файл не является таблицей Excel (.xlsx)."


class SheetUnreadableError(FlatRegisterError):
    """Файл не удалось прочитать: открыт в Excel, повреждён или защищён."""

    default_user_message = (
        "Не удалось прочитать файл Excel. "
        "Проверьте, что он не открыт в другой программе."
    )


class NoInputFilesError(FlatRegisterError):
    """В выбранной папке нет ни одного .xlsx."""

    default_user_message = "В папке нет файлов Excel (.xlsx)."


# --------------------------------------------------------------------------
# Содержимое выгрузки
# --------------------------------------------------------------------------

class RegisterNotRecognizedError(FlatRegisterError):
    """Файл не похож ни на один вариант выбранного регистра."""

    default_user_message = "Файл не является выбранным регистром из 1С."


class RegisterStructureError(FlatRegisterError):
    """Регистр распознан, но в нём не хватает обязательных заголовков или уровней.

    Обычно значит одно из двух: либо выгрузка сделана в другой конфигурации 1С,
    либо уровни группировки в файле не выставлены.
    """

    default_user_message = "Структура выгрузки не соответствует ожидаемой."


class RegisterEmptyError(FlatRegisterError):
    """Регистр распознан, но данных в нём нет."""

    default_user_message = "Выгрузка регистра пустая."


class NoRecognizedRegistersError(FlatRegisterError):
    """В папке есть .xlsx, но ни один не распознан как регистр 1С."""

    default_user_message = "В папке не найдены выгрузки регистров 1С."


# --------------------------------------------------------------------------
# Результат, конфигурация
# --------------------------------------------------------------------------

class OutputError(FlatRegisterError):
    """Не удалось записать или открыть итоговый файл."""

    default_user_message = "Не удалось сохранить итоговый файл."


class ConfigError(FlatRegisterError):
    """Файл конфигурации config.json повреждён или недоступен."""

    default_user_message = "Файл настроек config.json недоступен или повреждён."


# --------------------------------------------------------------------------
# Отчёт о неудачной обработке
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class FileFailure:
    """Единый формат «файл не обработан».

    Им пользуются все три слоя: перехват исключений в процессоре, сборщик
    результатов и экран с отчётом. Это важно — пока формат записи неудачи
    определялся на месте, список в интерфейсе и текст в логе расходились.

    Датакласс объявлен на верхнем уровне и остаётся picklable: параллельный
    обработчик возвращает его из другого процесса.

    ``technical_detail`` намеренно **не** показывается пользователю — только
    пишется в лог. Иначе экран превращается в простыню трейсбеков.
    """

    file_name: str = ""
    user_message: str = "Не удалось обработать файл."
    hint: str | None = None
    technical_detail: str | None = None

    @property
    def is_empty(self) -> bool:
        return not self.user_message


def describe_exception(error: BaseException, *, file_name: str = "") -> FileFailure:
    """Превращает любое исключение в :class:`FileFailure`.

    Разделение на два случая принципиально:

    * :class:`FlatRegisterError` — мы знаем, что произошло и что делать,
      поэтому пользователю уходит :attr:`user_message` и подсказка;
    * всё остальное — внутренняя ошибка (pandas, openpyxl, COM, опечатка в коде).
      Пользователю про неё ничего полезного сказать нельзя, поэтому он видит
      короткое сообщение и ссылку на лог, а трейсбек целиком уходит в лог.
    """
    detail = "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    ).strip()

    if isinstance(error, FlatRegisterError):
        return FileFailure(
            file_name=file_name,
            user_message=error.user_message,
            hint=error.hint,
            technical_detail=detail,
        )

    return FileFailure(
        file_name=file_name,
        user_message="При обработке файла произошла внутренняя ошибка.",
        hint="Подробности записаны в журнал logs/app.log — сообщите о них разработчику.",
        technical_detail=detail,
    )