# -*- coding: utf-8 -*-
"""Дымовые проверки UI (перенесены из tools/smoke_ui.py в pytest)."""

from __future__ import annotations

import asyncio

from flatregister.errors import FileFailure, RegisterEmptyError, describe_exception
from ui.app import FlatRegister1CApp
from ui.screens import ResultProcessingDirectoryScreen


def _run(coro):
    asyncio.run(coro)


def visible_text(screen) -> str:
    """Собирает текст, который реально отдан виджетам экрана."""
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


async def _smoke_ui_flow() -> None:
    app = FlatRegister1CApp()
    async with app.run_test() as pilot:
        await pilot.pause()

        # 1. Приложение стартует
        assert app.screen is not None
        assert "1С" in (app.title or ""), repr(app.title)
        assert not app._notifications, str(app._notifications)

        # 2. Экран неудачных файлов собирается
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
        await app.push_screen(ResultProcessingDirectoryScreen(payload))
        await pilot.pause()
        screen = app.screen
        assert isinstance(screen, ResultProcessingDirectoryScreen)
        assert len(screen.query("#result_processing")) == 1

        visible = visible_text(screen)
        assert "не является ОСВ" in visible, visible[:400]
        assert "Субконто" in visible, visible[:400]
        assert "Обороты счета пустые" in visible
        assert "Traceback" not in visible
        assert len(screen.query("#failed_files_log_hint")) == 0

        await app.pop_screen()
        await pilot.pause()

        # 2b. Подсказка про журнал при наличии технической детали
        payload["ООО Вектор.xlsx"] = FileFailure(
            file_name="ООО Вектор.xlsx",
            user_message="При обработке файла произошла внутренняя ошибка.",
            hint="Подробности записаны в журнал logs/app.log — сообщите о них разработчику.",
            technical_detail="KeyError: 'С кред. счетов'\n  File \"analisys.py\", line 342",
        )
        await app.push_screen(ResultProcessingDirectoryScreen(payload))
        await pilot.pause()
        screen = app.screen
        assert len(screen.query("#failed_files_log_hint")) == 1
        assert "app.log" in visible_text(screen)
        assert "KeyError" not in visible_text(screen)

        await app.pop_screen()
        await pilot.pause()
        assert not isinstance(app.screen, ResultProcessingDirectoryScreen)

        # 3. Ошибка доходит до пользователя как FileFailure, а не как строка
        try:
            raise RegisterEmptyError(
                "Анализ счета пустой.", hint="В выгрузке нет строк с данными."
            )
        except Exception as error:
            failure = describe_exception(error, file_name="анализ.xlsx")

        assert isinstance(failure, FileFailure)
        assert "пустой" in failure.user_message
        assert bool(failure.hint)
        assert failure.file_name == "анализ.xlsx"
        assert str(failure.user_message) == "Анализ счета пустой."

        # 4. Реактив failed_processing_markdown удалён из приложения
        assert not hasattr(FlatRegister1CApp, "failed_processing_markdown")


def test_smoke_ui():
    _run(_smoke_ui_flow())
