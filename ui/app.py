"""Главное приложение: выбор регистра, старт обработки.

Вынесено из ``main.py`` в Сессии S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось. Точка входа осталась в ``main.py``.
"""
from pathlib import Path
from typing import List

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Center, Horizontal
from textual.reactive import reactive
from textual.widgets import Button, Footer, Header, Markdown, Select

from flatregister.logging_setup import setup_logging
from flatregister.registry import REGISTER_SPEC, select_options
from text import START_WINDOW
from ui.screens import ContactsScreen, RegisterScreen, SettingsScreen

# Журнал настраивается один раз при импорте, до создания приложения: иначе
# об отказе файла сообщить некуда, а первое же обращение к конфигурации
# происходит ещё в compose.
setup_logging()

# Textual ищет CSS_PATH относительно файла, где объявлен класс приложения.
# style.tcss остался в корне (требование Nuitka) — путь разворачиваем отсюда.
_CSS_PATH = str(Path(__file__).resolve().parent.parent / "style.tcss")

class FlatRegister1CApp(App):
    CSS_PATH = _CSS_PATH
    
    BINDINGS = [
        Binding(key="f1",
                action="open_contacts",
                description="Контакты",
                key_display="F1"),        
        Binding(key="f3",
                action="open_settings",
                description="Настройки обработки",
                key_display="F3")
                        ]
    
    contact_visibility = reactive(1, bindings=True)
    
    # выбранный тип регистра (ОСВ, анализ счета, обороты счета и т.д.)
    register = reactive("Не выбран регистр!")

    # описание требований к выбранному регистру
    register_requirements = reactive("Отсутствует описание, т.к. не выбран регистр!")

    # список выбранных корр.счетов, по которым не нужна расшифровка по субсчетам
    accounts_without_subaccount: List[str] = reactive(['НЕ ВЫБРАНЫ'])
    
    def compose(self) -> ComposeResult:
        markdown = Markdown(START_WINDOW, id='markdown_start')
        
        #отключает отображение вертикальных линий (направляющих отступов)
        # в блоках кода внутри Markdown-виджета.
        # markdown.code_indent_guides = False
        
        # Даем markdown класс для управления высотой
        # markdown.classes = "scrollable-content"
        
        yield Header(show_clock=True)
        yield markdown
        yield Horizontal(
            Center(
                Select(
                    select_options(),
                    prompt='выбрать регистр',
                ),
                id='left_select'
            ),
            Center(
                Button('Продолжить', variant="primary", disabled=True, id='continue'),
                id='right_button'
            ),
            id='horizontal_select_register',
            classes="bottom-bar"
        )
        self.footer = Footer(show_command_palette=False)
        yield self.footer
    
    def on_mount(self) -> None:
        self.title = '🤖 Обработчик регистров 1С 6-в-1'
        # self.sub_title = 'Формирует удобную для анализа плоскую таблицу'    
    
    def action_open_settings(self):
        self.app.push_screen(SettingsScreen())
    
    def action_open_contacts(self):
        self.app.push_screen(ContactsScreen())
    
    @on(Select.Changed)
    def select_changed(self, event: Select.Changed) -> None:
        button = self.query_one(Button)
        if event.value is Select.BLANK:
            button.disabled = True
        else:
            button.disabled = False
    
    @on(Button.Pressed)
    def button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "continue":
            select = self.query_one(Select)
            if select.value is not Select.BLANK:
                self.register = select.value
                entry = REGISTER_SPEC.get(select.value)
                self.register_requirements = (
                    entry.requirements if entry is not None
                    else "Не выбран регистр для обработки!"
                )
                self.contact_visibility = 0
                self.push_screen(RegisterScreen())
                
    # скрытие кнопки с контактами
    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Check if an action may run."""
        if action == "open_contacts" and self.contact_visibility == 0:
            return False
        return True
