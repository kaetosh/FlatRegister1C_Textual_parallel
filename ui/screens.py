"""Экраны приложения: требования регистра, настройки, контакты, отчёт об отказах.

Вынесено из ``main.py`` в Сессии S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось.
"""
import io
from functools import lru_cache
from pathlib import Path

import segno
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, ScrollableContainer, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.validation import Function
from textual.widgets import (
    Button,
    Footer,
    Header,
    Input,
    Markdown,
    RadioButton,
    RadioSet,
    SelectionList,
    Static,
    Switch,
)

from flatregister.config import (
    DEFAULT_CONFIG,
    get_parallel_processing_option,
    read_config,
    update_config,
)
from flatregister.errors import FileFailure, describe_exception
from flatregister.excel_io import generate_failed_processing_markdown, is_valid_dragged_path
from flatregister.logging_setup import get_log_path, get_logger, log_failure
from flatregister.pipeline.handler import FileHandler
from ui.widgets import LoaderIndicatorCustom, ProgressBarCustom

_log = get_logger("ui")

class ResultProcessingDirectotyScreen(ModalScreen):
    """Экран с выводом необработанных файлов в виде таблицы:
    Имя файла / причина отказа / что можно сделать"""
    BINDINGS = [("escape", "dismiss", "Закрыть")]

    def __init__(self, failures: dict, **kwargs):
        super().__init__(**kwargs)
        self.failures = failures
        self.markdown_text = generate_failed_processing_markdown(failures)
        self.log_path = get_log_path()

    def compose(self) -> ComposeResult:
        yield Markdown(self.markdown_text, id='result_processing')
        if any(getattr(f, "technical_detail", None) for f in self.failures.values()):
            yield Static(
                f"Подробности (включая трейсбэки) — в журнале: {self.log_path}",
                id="failed_files_log_hint",
                markup=False,
            )
        yield Footer(show_command_palette=False)

class SettingsScreenRegister(ModalScreen):
    """
    Окно с настройками обработчика анализа счетов / оборотов счета.
    """
    def __init__(self, register_type: str, **kwargs):
        super().__init__(**kwargs)
        self.register_type = register_type
        self.config = read_config()
        
    def compose(self) -> ComposeResult:
                
        if self.app.register in ["analisys", "turnover"]:
            # Устанавливаем значения по умолчанию
            self.app.accounts_without_subaccount = self.config.get("accounts_without_subaccount",
                                                              DEFAULT_CONFIG.get("accounts_without_subaccount", {}))
            yield Container(
                Static(
                    "Отметьте корр.счета¹, по которым не нужна расшифровка по суб.счетам",
                    id="static-settings-register-modal"
                ),
                
                VerticalScroll(
                    SelectionList(id="selection-list-settings-modal"),
                    id="scrollable-selection-list"
                ),
                
                Static(
                    "¹нужно изменить список? см. Контакты",
                    id="static_help-settings-modal"
                ),
                
                Horizontal(
                    Button("Сохранить", variant="success", id="button-save-settings-modal", classes='buttons-settings-modal'),
                    Button("Отмена", variant="primary", id="button-cancel-settings-modal", classes='buttons-settings-modal'),
                    id="horizontals-button-settings-register-modal"
                ),
                
                id="container-settings-screen-modal"
            )
            yield Footer(show_command_palette=False)
        
        elif self.app.register in ["generalosv"]:
            keep_subaccounts = self.config.get("keep_subaccounts", DEFAULT_CONFIG.get("keep_subaccounts", True))
            
            yield Container(
                Horizontal(
                    Static("Только субсчета нижнего уровня:", classes="statics-settings-modal"),
                    Switch(value=keep_subaccounts, id='switch-keep-subaccounts', classes="switchs-settings-modal"),
                    id='horizontal-general-header-settings-modal'
                    ),
                Horizontal(
                    Button("Сохранить", variant="success", id='button-save-settings-modal'),
                    id="horizontals-button-settings-modal"),
                id="container-settings-modal",
            )
    
    def on_mount(self) -> None:
        if self.app.register in ["analisys", "turnover"]:
            self.query_one(SelectionList).clear_options()
            self.query_one(SelectionList).add_options([tuple(item) for item in self.app.accounts_without_subaccount])
        elif self.app.register in ["generalosv"]:   
            self.query_one('#container-settings-modal').tooltip = 'Если включено, в отчете будут показаны только счета, не имеющие дочерних субсчетов (например, только 60.01.1, но не 60, 60.01, 60.01.1).'
        
    def on_button_pressed(self, event: Button.Pressed):
        """Обрабатывает нажатие кнопки "Сохранить"."""
        if event.button.id == "button-save-settings-modal":
            if self.app.register in ["analisys", "turnover"]:           
                selected_values = self.query_one(SelectionList).selected
                selected_set = set(selected_values)
                
                # Обновляем словарь
                for account in self.app.accounts_without_subaccount:
                    account_code = account[0]  # первый элемент - код счета
                    account[2] = 1 if account_code in selected_set else 0
                
                updates={"accounts_without_subaccount": self.app.accounts_without_subaccount}
            elif self.app.register in ["generalosv"]:
                keep_subaccounts = bool(self.query_one('#switch-keep-subaccounts', Switch).value)
                updates={"keep_subaccounts": keep_subaccounts}
            # Обновляем конфигурацию
            update_config(updates=updates)
            
            # Закрываем модальное окно
            self.dismiss()
        if event.button.id == "button-cancel-settings-modal":
            self.dismiss()

class RegisterScreen(Screen):
    """Экран с требованиями к выбранному регистру
    и полем для перетягивания в него файлов/папки для обработки"""

    BINDINGS = [
                Binding(key="escape",
                        action="app.pop_screen",
                        description="Закрыть",
                        key_display="esc"),
                Binding(key="f3",
                        action="open_settings",
                        description="Настройки регистра",
                        key_display="F3")
                        ]
    
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield ScrollableContainer(Markdown(self.app.register_requirements),
                                  id='scroll_input',
                                  can_focus=False # Отключаем возможность фокуса
                                  ) 
        yield Horizontal(
            Input(placeholder="Укажите путь: перетащите файл/папку в окно программы или вставьте из буфера (Ctrl+V)",
                  tooltip="Окно программы должно быть активным.",
                  type="text",
                  id='path_register',
                  validators=[Function(is_valid_dragged_path, "Неверный путь к файлу(ам) или папке!")]),
            id='horizontal_input',
            classes="bottom-bar"
            )
        self.footer = Footer(show_command_palette=False)
        yield self.footer
    
    def on_mount(self) -> None:
        # Фокусируемся на Input при монтировании экрана
        self.title = '🤖 Обработчик регистров 1С 6-в-1'
        # self.sub_title = 'Формирует удобную для анализа плоскую таблицу'
        self.query_one("#path_register").focus()

    def action_open_settings(self):
        if self.app.register not in ["analisys", "turnover", "generalosv"]:
            self.notify(
                "Настройки недоступны для этого типа регистра",
                severity="warning",
                timeout=3
            )
            return
        self.app.push_screen(SettingsScreenRegister(register_type=self.app.register))
    
    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Обработчик нажатия enter в поле ввода"""
        if event.input.id == "path_register":
            
            # Проверяем, есть ли ошибки валидации
            if event.validation_result and event.validation_result.is_valid:
                # Если валидация пройдена
                self.notify("Начинаем обработку")
                
                normal_input_path = Path(str(event.value).strip('"\''))
                
                if normal_input_path.is_file():
                    progress_screen = LoaderIndicatorCustom()
                else:
                    progress_screen = ProgressBarCustom()
                self.app.push_screen(progress_screen)
                
                self.process_file(normal_input_path, self.app.register, progress_screen)  # Запускаем фоновую задачу с передачей ссылки на экран
            else:
                # Если валидация не пройдена
                self.notify("Проверьте пути!")
    
    @work(thread=True)  # Запускаем в отдельном потоке, чтобы не блокировать UI
    def process_file(self, input_path, register, progress_screen) -> None:
        parallel_processing_option = get_parallel_processing_option()
        file_handler = FileHandler(parallel=parallel_processing_option)

        try:
            file_handler.handle_input(input_path,
                                      register,
                                      progress_callback=progress_screen.update_progress)  # Передаем callback

            if file_handler.storage_processed_registers:
                file_handler._save_and_open_batch_result(progress_callback=progress_screen.update_progress) # Передаем callback

            # Снимаем отчёты и чистим хранилище до возврата в UI-поток: иначе
            # фоновый поток продолжал бы держать в памяти все кадры.
            failures = dict(file_handler.not_correct_files)
            processed_count = len(file_handler.storage_processed_registers)
            file_handler.not_correct_files.clear()
            file_handler.storage_processed_registers.clear()
            file_handler.check.clear()

            # Возвращаемся в основной поток
            self.app.call_from_thread(self.on_success, failures, processed_count)
        except Exception as error:
            # Единственная точка, где ошибка обработки превращается в то, что
            # видит пользователь. Раньше здесь был `str(error_description)`:
            # на экране появлялось либо служебное сообщение pandas, либо пустое
            # «KeyError: 'С кред. счетов'». Теперь — понятный текст, а трейсбек
            # целиком уходит в журнал.
            failure = describe_exception(error, file_name=getattr(input_path, "name", ""))
            log_failure(failure, logger=_log)
            self.app.call_from_thread(self.on_error, failure)

    def on_success(self, failures: dict, processed_count: int) -> None:

        while len(self.app.screen_stack) > 1:
            self.app.pop_screen()

        if processed_count:
            self.notify(f"Обработка завершена! Обработано файлов: {processed_count}.")
        else:
            self.notify("Обработка завершена, но ни один файл не распознан как выбранный регистр.",
                        severity="warning", timeout=8, markup=False)

        self.app.contact_visibility = 1

        if failures:
            self.app.push_screen(ResultProcessingDirectotyScreen(failures))

    def on_error(self, failure: FileFailure) -> None:

        while len(self.app.screen_stack) > 1:
            self.app.pop_screen()

        # markup=False обязателен: текст ошибки приходит из файла выгрузки
        # (имя файла, название счёта) и квадратные скобки в нём ломали разметку.
        self.notify(failure.user_message, severity="error", timeout=10, markup=False)
        if failure.hint:
            self.notify(failure.hint, severity="warning", timeout=10, markup=False)
        self.app.contact_visibility = 1
        
    def key_escape(self):
        self.app.contact_visibility = 1

class SettingsScreen(ModalScreen):
    """
    Окно с настройками.
    """
    
    def compose(self) -> ComposeResult:
        config = read_config()
        general_options = config.get("general_settings", DEFAULT_CONFIG.get("general_settings", {}))
        general_header_value = bool(general_options.get("parallel_processing", 0))
        
        yield Container(
            Horizontal(
                Static("Параллельная обработка в пакетном режиме¹:", classes="statics-settings-modal"),
                Switch(value=general_header_value, id='switch-general-header', classes="switchs-settings-modal"),
                id='horizontal-general-header-settings-modal'
                ),
            Static(
                "¹не применяется к регистрам Обороты счета",
                id="statics-no-turnover-settings-modal"
            ),
            Horizontal(
                Button("Сохранить", variant="success", id="button-settings-modal"),
                id="horizontals-button-settings-modal"),
            id="container-settings-modal"
        )
    
    def on_mount(self) -> None:
        self.query_one('#horizontal-general-header-settings-modal').tooltip = 'Ускоряет обработку, но загружает ресурсы компьютера'
    
    def on_button_pressed(self, event: Button.Pressed):
        """Обрабатывает нажатие кнопки "Сохранить"."""
        if event.button.id == "button-settings-modal":
            general_header_val = bool(self.query_one('#switch-general-header', Switch).value)
            updates = {
                        "general_settings": {
                            "parallel_processing": general_header_val,
                                            }
                      }

            update_config(updates=updates)
            self.dismiss()

class ContactsScreen(ModalScreen):
    """Окно с контактами — улучшенная версия с горизонтальным расположением."""

    DEFAULT_CSS = """
    ContactsScreen {
        background: $surface;
    }

    #info_text {
        text-align: center;
        margin: 1 1;
        padding: 1;
        width: 100%;
    }

    .flex-row {
        layout: horizontal;
        align: center middle;
        margin: 1 1;
        padding: 1;
        height: auto;
        width: 100%;
    }

    .selector-container {
        width: 30%;
        align: center middle;
        padding: 0;
    }

    .selector-label {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #messenger_selector {
        width: 100%;
        margin: 0;
    }

    .qr-container {
        width: 65%;
        align: center middle;
        padding: 0;
        border-title-align: center;
    }

    .qr-label {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #qr_code {
        text-align: center;
        margin: 0;
        padding: 1;
        width: 100%;
        height: auto;
        background: $surface;
    }
    """

    BINDINGS = [
        Binding(key="escape", action="app.pop_screen", description="Закрыть", key_display="esc")
    ]

    @staticmethod
    @lru_cache(maxsize=2)
    def get_qr_ascii(data: str) -> str:
        """Генерирует QR код с кэшированием"""
        qr = segno.make(data)
        buffer = io.StringIO()
        qr.terminal(out=buffer, border=2, compact=True)
        return buffer.getvalue()

    def compose(self) -> ComposeResult:
        # Информационный текст
        yield Static(
            "Следите 👀 за релизами и обновлениями в наших группах.\n"
            "По всем вопросам и сообщениям об ошибках обращайтесь на почту 📧 me@karabedyan.ru или в указанные 💬 мессенджеры.",
            id="info_text"
        )

        # Горизонтальный контейнер: переключатель + QR-код
        with Static(classes="flex-row"):
            # Левая часть: переключатель
            with Static(classes="selector-container"):
                yield Static("Выберите мессенджер:", classes="selector-label")
                with RadioSet(id="messenger_selector"):
                    yield RadioButton("Telegram", id="telegram", value=True)
                    yield RadioButton("MAX", id="max", value=False)

            # Правая часть: QR-код
            with Static(classes="qr-container"):
                yield Static("", id="qr_label", classes="qr-label")
                yield Static("", id="qr_code", classes="qr-code", markup=False)

        yield Footer()

    def on_mount(self) -> None:
        """При монтировании окна устанавливаем начальный QR-код"""
        self.update_qr_code("telegram")

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        """Обработчик изменения выбора в RadioSet"""
        selected_id = event.pressed.id
        if selected_id in ("telegram", "max"):
            self.update_qr_code(selected_id)

    def update_qr_code(self, messenger: str) -> None:
        """Обновляет QR-код в зависимости от выбранного мессенджера"""
        if messenger == "telegram":
            data = "https://t.me/Python_for_the_clerk"
            label = "Группа в Telegram:"
        else:  # max
            data = "https://max.ru/join/tA7p1FrtqWBh4br4hEvmP_p7VgzQBC_Xy1LXT-u95-Q"
            label = "Группа в MAX:"

        qr_ascii = self.get_qr_ascii(data)
        self.query_one("#qr_label", Static).update(label)
        self.query_one("#qr_code", Static).update(qr_ascii)

    def key_escape(self) -> None:
        """Обработчик нажатия клавиши ESC"""
        self.dismiss()
