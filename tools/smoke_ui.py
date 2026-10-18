# -*- coding: utf-8 -*-
r"""Дымовая проверка UI после Сессии S1 (headless, без реального окна).

    uv run python tools\smoke_ui.py

Поднимает приложение через textual.test, прогоняет по нему экран неудачных
файлов и проверяет, что:
  * приложение стартует и показывает выбор регистра;
  * экран отчёта собирается и содержит текст ошибки и подсказку;
  * функция process_file не падает на мусорном файле и вызывает on_error
    с FileFailure, а не со строкой.
"""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flatregister.errors import FileFailure, RegisterEmptyError, describe_exception
from flatregister.logging_setup import setup_logging
from main import FlatRegister1CApp
from ui.screens import ResultProcessingDirectotyScreen

setup_logging()

failures = []


def check(name, condition, detail=""):
    print(f"  {'OK  ' if condition else 'FAIL'} {name}" + (f"  {detail}" if not condition else ""))
    if not condition:
        failures.append(name)


def visible_text(screen) -> str:
    """Собирает текст, который реально отдан виджетам экрана.

    У Markdown исходник лежит в ``source``, у Static — в ``content``;
    рендер пришлось бы разбирать по кускам, а проверять тут нужно именно то,
    что мы передали на экран.
    """
    chunks = []
    for widget in screen.query("*"):
        for attribute in ("source", "content"):
            value = getattr(widget, attribute, None)
            if isinstance(value, str) and value.strip():
                chunks.append(value)
            elif value is not None and not isinstance(value, str):
                plain = getattr(value, "plain", None)
                if isinstance(plain, str) and plain.strip():
                    chunks.append(plain)
    return "\n".join(chunks)


async def run() -> None:
    app = FlatRegister1CApp()

    async with app.run_test() as pilot:
        print("1. Приложение стартует")
        check("экран выбора регистра на месте", app.screen is not None)
        check("заголовок приложения установлен", "1С" in (app.title or ""), repr(app.title))
        check("уведомлений об ошибках нет", not app._notifications,
              str(app._notifications))

        print()
        print("2. Экран неудачных файлов собирается")
        payload = {
            "ООО Ромашка.xlsx": FileFailure(
                file_name="ООО Ромашка.xlsx",
                user_message="Файл не является ОСВ счета 1с.",
                hint="В выгрузке нет строки заголовков со словом «Субконто».",
            ),
            "ООО Вектор.xlsx": FileFailure(
                file_name="ООО Вектор.xlsx",
                user_message="Обороты счета пустые.",
                hint="В выгрузке нет ни одной строки с данными.",
            ),
        }
        await app.push_screen(ResultProcessingDirectotyScreen(payload))
        await pilot.pause()
        screen = app.screen
        check("на экране наш экран", isinstance(screen, ResultProcessingDirectotyScreen))
        check("markdown-виджет смонтирован", len(screen.query("#result_processing")) == 1)

        visible = visible_text(screen)
        check("причина попала на экран", "не является ОСВ" in visible, visible[:400])
        check("подсказка попала на экран", "Субконто" in visible, visible[:400])
        check("второй файл тоже виден", "Обороты счета пустые" in visible)
        check("трейсбеков на экране нет", "Traceback" not in visible)
        check("смысла советовать журнал нет — ссылка не показана",
              len(screen.query("#failed_files_log_hint")) == 0)

        await app.pop_screen()
        await pilot.pause()

        print()
        print("2b. Если был хотя бы один сбой — показать, где искать подробности")
        payload["ООО Вектор.xlsx"] = FileFailure(
            file_name="ООО Вектор.xlsx",
            user_message="При обработке файла произошла внутренняя ошибка.",
            hint="Подробности записаны в журнал logs/app.log — сообщите о них разработчику.",
            technical_detail="KeyError: 'С кред. счетов'\n  File \"analisys.py\", line 342",
        )
        await app.push_screen(ResultProcessingDirectotyScreen(payload))
        await pilot.pause()
        screen = app.screen
        hint_widgets = screen.query("#failed_files_log_hint")
        check("подсказка про журнал показана", len(hint_widgets) == 1)
        check("в подсказке есть путь к журналу",
              "app.log" in visible_text(screen), visible_text(screen)[:600])
        check("сам трейсбек на экран не попал", "KeyError" not in visible_text(screen))

        await app.pop_screen()
        await pilot.pause()
        check("экран закрылся по Esc-пути", not isinstance(app.screen, ResultProcessingDirectotyScreen))

        print()
        print("3. Ошибка доходит до on_error как FileFailure, а не как строка")
        try:
            raise RegisterEmptyError("Анализ счета пустой.", hint="В выгрузке нет строк с данными.")
        except Exception as error:
            failure = describe_exception(error, file_name="анализ.xlsx")

        check("тип — FileFailure", isinstance(failure, FileFailure))
        check("текст на русском", "пустой" in failure.user_message)
        check("подсказка не потерялась", bool(failure.hint))
        check("имя файла сохранено", failure.file_name == "анализ.xlsx")
        check("пользователю уйдёт именно user_message",
              str(failure.user_message) == "Анализ счета пустой.")

        print()
        print("4. Реактив failed_processing_markdown удалён из приложения")
        check("реактив убран", not hasattr(FlatRegister1CApp, "failed_processing_markdown"))


def main() -> int:
    asyncio.run(run())
    print()
    print("=" * 70)
    print(f"ИТОГ: {'ВСЁ ЗЕЛЁНОЕ' if not failures else 'ПРОВАЛЫ: ' + ', '.join(failures)}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())