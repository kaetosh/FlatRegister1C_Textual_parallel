# -*- coding: utf-8 -*-
r"""Дымовая проверка слоя ошибок после Сессии S1.

    uv run python tools\smoke_errors.py

До Сессии S2 (тесты) это единственный автоматический способ убедиться, что
слой ошибок не развалился. В S2 проверки переездуют в tests/test_errors.py,
а этот скрипт останется для ручной проверки на живой машине.
"""
import logging
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flatregister.errors import FileFailure, RegisterEmptyError, RegisterNotRecognizedError, describe_exception
from flatregister.logging_setup import get_log_path, log_failure, setup_logging
from support_functions import generate_failed_processing_markdown

failures = []


def check(name, condition, detail=""):
    if condition:
        print(f"  OK   {name}")
    else:
        print(f"  FAIL {name} {detail}")
        failures.append(name)


print("=" * 70)
print("1. Логирование создаёт файл журнала")
setup_logging()
log_path = get_log_path()
log_failure(FileFailure(file_name="плохой.xlsx", user_message="Тестовая ошибка",
                        hint="Подсказка", technical_detail="Traceback (most recent call last):\n  KeyError: 'С кред. счетов'"))
for handler in logging.getLogger("flatregister").handlers:
    handler.flush()
check("файл журнала существует", log_path.exists(), str(log_path))
check("трейсбек попал в журнал",
      log_path.exists() and "KeyError" in log_path.read_text(encoding="utf-8"))

print()
print("2. Известная ошибка отдаёт понятный текст, а не str(exc)")
try:
    raise RegisterNotRecognizedError("Файл не является ОСВ.", hint="Выберите другой регистр.")
except Exception as error:
    failure = describe_exception(error, file_name="x.xlsx")
check("user_message без служебного текста",
      failure.user_message == "Файл не является ОСВ.", failure.user_message)
check("подсказка доехала", failure.hint == "Выберите другой регистр.")
check("имя файла на месте", failure.file_name == "x.xlsx")
check("трейсбек приложен", "RegisterNotRecognizedError" in (failure.technical_detail or ""))

print()
print("3. Неожиданная ошибка НЕ показывает пользователю внутренности pandas")
try:
    raise KeyError("С кред. счетов")
except Exception as error:
    failure = describe_exception(error, file_name="y.xlsx")
check("текст понятный", failure.user_message == "При обработке файла произошла внутренняя ошибка.",
      failure.user_message)
check("нет KeyError в user_message", "KeyError" not in failure.user_message)
check("есть ссылка на журнал", "logs/app.log" in (failure.hint or ""))
check("причина осталась в technical_detail", "KeyError" in (failure.technical_detail or ""))

print()
print("4. str(ошибки) == текст для пользователя")
check("str() без мусора",
      str(RegisterEmptyError("Карточка пустая.")) == "Карточка пустая.")

print()
print("5. Таблица неудачных файлов")
markdown = generate_failed_processing_markdown({
    "ООО Ромашка.xlsx": failure,
    "bad|name.xlsx": FileFailure(file_name="bad|name.xlsx",
                                 user_message="Пусто|пусто", hint=None),
})
check("три колонки", "| Файл | Причина | Что можно сделать |" in markdown, markdown)
check("вертикальная черта экранирована", "bad\\|name.xlsx" in markdown, markdown)
check("подсказка попала в таблицу", "logs/app.log" in markdown)
check("пустое поле не ломает таблицу", "| — |" in markdown, markdown)
check("пустой список даёт пустую строку", generate_failed_processing_markdown({}) == "")

print()
print("6. Сквозной путь: мусорный .xlsx в папке не роняет пакетную обработку")
from file_handler import FileHandler  # импорт после проверок: тяжёлый

with tempfile.TemporaryDirectory() as tmp:
    folder = Path(tmp)
    (folder / "мусор.xlsx").write_bytes(b"this is not a zip archive")
    # Расширение в верхнем регистре: glob("*.xlsx") его не подхватит — проверяем,
    # что «тихо ничего не нашлось» не выглядит как успех.
    (folder / "выгрузка.XLS").write_bytes(b"never read")

    handler = FileHandler()

    def progress(*args, **kwargs):
        """Заглушка прогресс-колбэка: в дымовой проверке он ничего не рисует."""

    try:
        handler.handle_input(folder, "analisys", progress_callback=progress)
        outcome = "без исключения"
    except Exception as error:  # noqa: BLE001 - здесь нам важен сам факт
        outcome = f"{type(error).__name__}: {error}"

    check("отказ не упал наружу сырым исключением",
          handler.not_correct_files or "NoInputFilesError" in outcome, outcome)
    for name, failure in handler.not_correct_files.items():
        is_failure = isinstance(failure, FileFailure)
        check(f"отказ по {name} — FileFailure", is_failure, repr(failure))
        check(f"у {name} есть текст для пользователя",
              is_failure and bool(failure.user_message))
        check(f"у {name} нет трейсбека в user_message",
              is_failure and "Traceback" not in failure.user_message)
        print(f"       -> {failure.user_message}")
        if failure.hint:
            print(f"          подсказка: {failure.hint}")

print()
print("7. Ошибка пути — понятный текст, а не 'путь не найден'")
from flatregister.errors import InputPathError
from support_functions import validate_paths

try:
    validate_paths([Path("Z:/нет/такой/папки")])
    check("validate_paths поднял ошибку", False)
except InputPathError as error:
    check("InputPathError поднят", True)
    check("в тексте есть сам путь", "нет" in error.user_message, error.user_message)
    check("есть подсказка", bool(error.hint))

print()
print("=" * 70)
print(f"ИТОГ: {'ВСЁ ЗЕЛЁНОЕ' if not failures else 'ПРОВАЛЫ: ' + ', '.join(failures)}")
print(f"журнал: {log_path}")
sys.exit(1 if failures else 0)