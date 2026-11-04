"""Главное приложение: выбор регистра, старт обработки.

Вынесено из ``main.py`` в Сессии S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось. Точка входа осталась в ``main.py``.

Default-экран — ``MenuScreen`` (см. get_default_screen). Вся логика выбора
регистра и старта обработки живет там; App только держит состояние
обработки (``processing_state``) и общие реактивы.
"""

import sys
from pathlib import Path
from typing import List

from textual.app import App, ComposeResult
from textual.reactive import reactive
from textual.widgets import Footer, Header

from flatregister.logging_setup import setup_logging
from ui.processing import ProcessingState
from ui.screens import MenuScreen

# Журнал настраивается один раз при импорте, до создания приложения: иначе
# об отказе файла сообщить некуда, а первое же обращение к конфигурации
# происходит ещё в compose. NOTE: выполняется и в каждом процессе
# ProcessPoolExecutor при импорте ui.app (см. S10).
setup_logging()

# Textual ищет CSS_PATH относительно файла, где объявлен класс приложения.
# style.tcss остался в корне (требование Nuitka) — путь разворачиваем отсюда.
# Страховка для собранного .exe: если ui/app.py не лежит рядом с style.tcss,
# берём каталог исполняемого файла.
_candidate = Path(__file__).resolve().parent.parent / "style.tcss"
if not _candidate.exists():
    _candidate = Path(sys.argv[0]).resolve().parent / "style.tcss"
_CSS_PATH = str(_candidate)


class FlatRegister1CApp(App):
    CSS_PATH = _CSS_PATH

    # BINDINGS пусто: F1/F3 живут на MenuScreen и исчезают с ним.
    BINDINGS: list = []

    # выбранный тип регистра (ОСВ, анализ счета, обороты счета и т.д.)
    register = reactive("Не выбран регистр!")

    # описание требований к выбранному регистру
    register_requirements = reactive("Отсутствует описание, т.к. не выбран регистр!")

    # список выбранных корр.счетов, по которым не нужна расшифровка по субсчетам
    accounts_without_subaccount: List[str] = reactive(["НЕ ВЫБРАНЫ"])

    def __init__(self) -> None:
        super().__init__()
        self.processing_state = ProcessingState()

    def get_default_screen(self) -> MenuScreen:
        """Главный экран — выбор регистра и старт обработки."""
        return MenuScreen()

    def compose(self) -> ComposeResult:
        # Не используется для основного контента (get_default_screen возвращает
        # MenuScreen), но требуется интерфейсом App.
        yield Header(show_clock=True)
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        self.title = "🤖 Обработчик регистров 1С 6-в-1"
