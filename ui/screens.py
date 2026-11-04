"""Экраны приложения: требования регистра, настройки, контакты, отчёт об отказах.

Вынесено из ``main.py`` в Сессии S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось.
"""

import io
from functools import lru_cache
from pathlib import Path

import segno
from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Center, Container, Horizontal, VerticalScroll
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
    Select,
    SelectionList,
    Static,
    Switch,
)

from flatregister.config import (
    DEFAULT_CONFIG,
    read_config,
    update_config,
)
from flatregister.errors import FileFailure
from flatregister.excel_io import generate_failed_processing_markdown, is_valid_dragged_path
from flatregister.logging_setup import get_log_path
from flatregister.registry import REGISTER_SPEC, select_options
from text import START_WINDOW
from ui.messages import ProgressAdapter
from ui.processing import ProcessingState, run_processing
from ui.progress import ProgressScreen

ESC_BINDING = [Binding("escape", "dismiss", "Закрыть", show=True)]


class MenuScreen(Screen):
    """Главный экран: выбор регистра и старт обработки.

    Является default-экраном App (см. FlatRegister1CApp.get_default_screen).
    F1/F3 живут здесь — при уходе с экрана они автоматически исчезают,
    отдельная реактив contact_visibility и check_action не нужны.
    """

    BINDINGS = [
        Binding("f1", "open_contacts", "Контакты", key_display="F1"),
        Binding("f3", "open_settings", "Настройки обработки", key_display="F3"),
    ]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Markdown(START_WINDOW, id="markdown_start")
        yield Horizontal(
            Center(
                Select(select_options(), prompt="выбрать регистр", id="select_register"),
                id="left_select",
            ),
            Center(
                Button("Продолжить", variant="primary", disabled=True, id="continue"),
                id="right_button",
            ),
            id="horizontal_select_register",
            classes="bottom-bar",
        )
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        self.title = "🤖 Обработчик регистров 1С 6-в-1"

    def action_open_contacts(self) -> None:
        self.app.push_screen(ContactsScreen())

    def action_open_settings(self) -> None:
        self.app.push_screen(SettingsScreen())

    @on(Select.Changed, "#select_register")
    def select_changed(self, event: Select.Changed) -> None:
        button = self.query_one("#continue", Button)
        button.disabled = (event.value is Select.BLANK)

    @on(Button.Pressed, "#continue")
    def button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "continue":
            select = self.query_one("#select_register", Select)
            if select.value is not Select.BLANK:
                entry = REGISTER_SPEC.get(select.value)
                self.app.register_requirements = (
                    entry.requirements
                    if entry is not None
                    else "Не выбран регистр для обработки!"
                )
                self.app.register = select.value
                self.app.push_screen(RegisterScreen())


class RegisterScreen(Screen):
    """Экран с требованиями к выбранному регистру
    и полем для перетягивания в него файлов/папки для обработки"""

    BINDINGS = ESC_BINDING + [
        Binding("f3", "open_settings", "Настройки регистра", key_display="F3"),
    ]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Container(
            Markdown(self.app.register_requirements, id="register_requirements"),
            id="scroll_input",
        )
        yield Horizontal(
            Input(
                placeholder="Укажите путь: перетащите файл/папку в окно программы или вставьте из буфера (Ctrl+V)",
                tooltip="Окно программы должно быть активным.",
                type="text",
                id="path_register",
                validators=[Function(is_valid_dragged_path, "Неверный путь к файлу(ам) или папке!")],
            ),
            id="horizontal_input",
            classes="bottom-bar",
        )
        yield Footer(show_command_palette=False)

    def on_mount(self) -> None:
        self.title = "🤖 Обработчик регистров 1С 6-в-1"
        self.query_one("#path_register").focus()

    def action_open_settings(self):
        if self.app.register not in ["analisys", "turnover", "generalosv"]:
            self.notify(
                "Настройки недоступны для этого типа регистра",
                severity="warning",
                timeout=3,
            )
            return
        self.app.push_screen(SettingsScreen(register_type=self.app.register))

    def check_action(self, action: str, parameters: tuple) -> bool | None:
        if action == "open_settings":
            return self.app.register in ["analisys", "turnover", "generalosv"]
        return True

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Обработчик нажатия Enter в поле ввода."""
        if event.input.id == "path_register":
            if event.validation_result and event.validation_result.is_valid:
                self.notify("Начинаем обработку")

                normal_input_path = Path(str(event.value).strip('"\''))

                # Запуск обработки через ProgressScreen
                self._start_processing(normal_input_path)
            else:
                self.notify("Проверьте пути!")

    def _start_processing(self, input_path: Path) -> None:
        app = self.app
        state: ProcessingState = app.processing_state

        def _done(failure: FileFailure | None, failures, processed_count):
            # Вызывается из фонового потока — marshal'им в UI-поток,
            # иначе попадём на виджеты из чужого потока.
            def _done_ui() -> None:
                state.discard()
                if failure is not None:
                    self._show_error(failure)
                else:
                    self._show_success(failures, processed_count)

            app.call_from_thread(_done_ui)

        progress_screen = ProgressScreen()
        app.push_screen(progress_screen)

        # Сообщения прогресса публикуются в очередь самого ProgressScreen,
        # а не App — иначе обработчик on_progress_update на экране никогда
        # не вызовется.
        adapter = ProgressAdapter(progress_screen.post_message)

        worker = run_processing(
            app,
            input_path=input_path,
            register=app.register,
            adapter=adapter,
            on_done=_done,
        )
        state.start(worker)

    def _show_success(self, failures: dict, processed_count: int) -> None:
        while len(self.app.screen_stack) > 1:
            self.app.pop_screen()

        if processed_count:
            self.notify(f"Обработка завершена! Обработано файлов: {processed_count}.")
        else:
            self.notify(
                "Обработка завершена, но ни один файл не распознан как выбранный регистр.",
                severity="warning",
                timeout=8,
                markup=False,
            )

        if failures:
            self.app.push_screen(ResultProcessingDirectoryScreen(failures))

    def _show_error(self, failure: FileFailure) -> None:
        while len(self.app.screen_stack) > 1:
            self.app.pop_screen()

        # markup=False обязателен: текст ошибки приходит из файла выгрузки
        # (имя файла, название счёта) и квадратные скобки в нём ломали разметку.
        self.notify(failure.user_message, severity="error", timeout=10, markup=False)
        if failure.hint:
            self.notify(failure.hint, severity="warning", timeout=10, markup=False)

    def key_escape(self) -> None:
        pass


class SettingsScreen(ModalScreen):
    """Окно с настройками."""

    BINDINGS = ESC_BINDING

    def __init__(self, register_type: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.register_type = register_type
        self.config = read_config()

    def compose(self) -> ComposeResult:
        if self.register_type is None:
            # Общие настройки
            general_options = self.config.get("general_settings", DEFAULT_CONFIG.get("general_settings", {}))
            general_header_value = bool(general_options.get("parallel_processing", 0))
            yield Container(
                Horizontal(
                    Static("Параллельная обработка в пакетном режиме¹:", classes="statics-settings-modal"),
                    Switch(value=general_header_value, id="switch-general-header", classes="switchs-settings-modal"),
                    id="horizontal-general-header-settings-modal",
                ),
                Static(
                    "¹не применяется к регистрам Обороты счета",
                    id="statics-no-turnover-settings-modal",
                ),
                Horizontal(
                    Button("Сохранить", variant="success", id="button-settings-modal"),
                    id="horizontals-button-settings-modal",
                ),
                id="container-settings-modal",
            )
        elif self.register_type in ["analisys", "turnover"]:
            # Устанавливаем значения по умолчанию
            self.app.accounts_without_subaccount = self.config.get(
                "accounts_without_subaccount",
                DEFAULT_CONFIG.get("accounts_without_subaccount", {}),
            )
            yield Container(
                Static(
                    "Отметьте корр.счета¹, по которым не нужна расшифровка по суб.счетам",
                    id="static-settings-register-modal",
                ),
                VerticalScroll(
                    SelectionList(id="selection-list-settings-modal"),
                    id="scrollable-selection-list",
                ),
                Static(
                    "¹нужно изменить список? см. Контакты",
                    id="static_help-settings-modal",
                ),
                Horizontal(
                    Button(
                        "Сохранить",
                        variant="success",
                        id="button-save-settings-modal",
                        classes="buttons-settings-modal",
                    ),
                    Button(
                        "Отмена",
                        variant="primary",
                        id="button-cancel-settings-modal",
                        classes="buttons-settings-modal",
                    ),
                    id="horizontals-button-settings-register-modal",
                ),
                id="container-settings-screen-modal",
            )
            yield Footer(show_command_palette=False)
        elif self.register_type in ["generalosv"]:
            keep_subaccounts = self.config.get(
                "keep_subaccounts", DEFAULT_CONFIG.get("keep_subaccounts", True)
            )
            yield Container(
                Horizontal(
                    Static(
                        "Только субсчета нижнего уровня:",
                        classes="statics-settings-modal",
                    ),
                    Switch(
                        value=keep_subaccounts,
                        id="switch-keep-subaccounts",
                        classes="switchs-settings-modal",
                    ),
                    id="horizontal-general-header-settings-modal",
                ),
                Horizontal(
                    Button(
                        "Сохранить",
                        variant="success",
                        id="button-save-settings-modal",
                    ),
                    id="horizontals-button-settings-modal",
                ),
                id="container-settings-modal",
            )

    def on_mount(self) -> None:
        if self.register_type in ["analisys", "turnover"]:
            self.query_one(SelectionList).clear_options()
            self.query_one(SelectionList).add_options(
                [tuple(item) for item in self.app.accounts_without_subaccount]
            )
        elif self.register_type in ["generalosv"]:
            self.query_one("#container-settings-modal").tooltip = (
                "Если включено, в отчете будут показаны только счета, не имеющие "
                "дочерних субсчетов (например, только 60.01.1, но не 60, 60.01, 60.01.1)."
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Обрабатывает нажатие кнопки."""
        if event.button.id == "button-settings-modal":
            # Общие настройки: параллельная обработка
            parallel_value = bool(self.query_one("#switch-general-header", Switch).value)
            updates = {"general_settings": {"parallel_processing": parallel_value}}
            update_config(updates=updates)

            # Закрываем модальное окно
            self.dismiss()
        if event.button.id == "button-save-settings-modal":
            if self.register_type in ["analisys", "turnover"]:
                selected_values = self.query_one(SelectionList).selected
                selected_set = set(selected_values)

                # Обновляем словарь
                for account in self.app.accounts_without_subaccount:
                    account_code = account[0]  # первый элемент - код счета
                    account[2] = 1 if account_code in selected_set else 0

                updates = {"accounts_without_subaccount": self.app.accounts_without_subaccount}
            elif self.register_type in ["generalosv"]:
                keep_subaccounts = bool(self.query_one("#switch-keep-subaccounts", Switch).value)
                updates = {"keep_subaccounts": keep_subaccounts}
            # Обновляем конфигурацию
            update_config(updates=updates)

            # Закрываем модальное окно
            self.dismiss()
        if event.button.id == "button-cancel-settings-modal":
            self.dismiss()


class SettingsScreenRegister(ModalScreen):
    """Окно с настройками обработчика анализа счетов / оборотов счета."""

    def __init__(self, register_type: str, **kwargs):
        super().__init__(**kwargs)
        self.register_type = register_type
        self.config = read_config()

    def compose(self) -> ComposeResult:
        if self.register_type is None:
            # Общие настройки
            general_options = self.config.get("general_settings", DEFAULT_CONFIG.get("general_settings", {}))
            general_header_value = bool(general_options.get("parallel_processing", 0))
            yield Container(
                Horizontal(
                    Static("Параллельная обработка в пакетном режиме¹:", classes="statics-settings-modal"),
                    Switch(value=general_header_value, id="switch-general-header", classes="switchs-settings-modal"),
                    id="horizontal-general-header-settings-modal",
                ),
                Static(
                    "¹не применяется к регистрам Обороты счета",
                    id="statics-no-turnover-settings-modal",
                ),
                Horizontal(
                    Button("Сохранить", variant="success", id="button-settings-modal"),
                    id="horizontals-button-settings-modal",
                ),
                id="container-settings-modal",
            )
        elif self.register_type in ["analisys", "turnover"]:
            # Устанавливаем значения по умолчанию
            self.app.accounts_without_subaccount = self.config.get(
                "accounts_without_subaccount",
                DEFAULT_CONFIG.get("accounts_without_subaccount", {}),
            )
            yield Container(
                Static(
                    "Отметьте корр.счета¹, по которым не нужна расшифровка по суб.счетам",
                    id="static-settings-register-modal",
                ),
                VerticalScroll(
                    SelectionList(id="selection-list-settings-modal"),
                    id="scrollable-selection-list",
                ),
                Static(
                    "¹нужно изменить список? см. Контакты",
                    id="static_help-settings-modal",
                ),
                Horizontal(
                    Button(
                        "Сохранить",
                        variant="success",
                        id="button-save-settings-modal",
                        classes="buttons-settings-modal",
                    ),
                    Button(
                        "Отмена",
                        variant="primary",
                        id="button-cancel-settings-modal",
                        classes="buttons-settings-modal",
                    ),
                    id="horizontals-button-settings-register-modal",
                ),
                id="container-settings-screen-modal",
            )
            yield Footer(show_command_palette=False)
        elif self.register_type in ["generalosv"]:
            keep_subaccounts = self.config.get(
                "keep_subaccounts", DEFAULT_CONFIG.get("keep_subaccounts", True)
            )
            yield Container(
                Horizontal(
                    Static(
                        "Только субсчета нижнего уровня:",
                        classes="statics-settings-modal",
                    ),
                    Switch(
                        value=keep_subaccounts,
                        id="switch-keep-subaccounts",
                        classes="switchs-settings-modal",
                    ),
                    id="horizontal-general-header-settings-modal",
                ),
                Horizontal(
                    Button(
                        "Сохранить",
                        variant="success",
                        id="button-save-settings-modal",
                    ),
                    Button(
                        "Отмена",
                        variant="primary",
                        id="button-cancel-settings-modal",
                    ),
                    id="horizontals-button-settings-modal",
                ),
                id="container-settings-modal",
            )

    def on_mount(self) -> None:
        if self.register_type in ["analisys", "turnover"]:
            self.query_one(SelectionList).clear_options()
            self.query_one(SelectionList).add_options(
                [tuple(item) for item in self.app.accounts_without_subaccount]
            )
        elif self.register_type in ["generalosv"]:
            self.query_one("#container-settings-modal").tooltip = (
                "Если включено, в отчете будут показаны только счета, не имеющие "
                "дочерних субсчетов (например, только 60.01.1, но не 60, 60.01, 60.01.1)."
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        """Обрабатывает нажатие кнопки."""
        if event.button.id == "button-settings-modal":
            # Общие настройки: параллельная обработка
            parallel_value = bool(self.query_one("#switch-general-header", Switch).value)
            updates = {"general_settings": {"parallel_processing": parallel_value}}
            update_config(updates=updates)

            # Закрываем модальное окно
            self.dismiss()
        if event.button.id == "button-save-settings-modal":
            if self.register_type in ["analisys", "turnover"]:
                selected_values = self.query_one(SelectionList).selected
                selected_set = set(selected_values)

                # Обновляем словарь
                for account in self.app.accounts_without_subaccount:
                    account_code = account[0]  # первый элемент - код счета
                    account[2] = 1 if account_code in selected_set else 0

                updates = {"accounts_without_subaccount": self.app.accounts_without_subaccount}
            elif self.register_type in ["generalosv"]:
                keep_subaccounts = bool(self.query_one("#switch-keep-subaccounts", Switch).value)
                updates = {"keep_subaccounts": keep_subaccounts}
            # Обновляем конфигурацию
            update_config(updates=updates)

            # Закрываем модальное окно
            self.dismiss()
        if event.button.id == "button-cancel-settings-modal":
            self.dismiss()


class ContactsScreen(ModalScreen):
    """Окно с контактами — улучшенная версия с горизонтальным расположением."""

    BINDINGS = ESC_BINDING

    @staticmethod
    @lru_cache(maxsize=2)
    def get_qr_ascii(data: str) -> str:
        """Генерирует QR код с кэшированием."""
        qr = segno.make(data)
        buffer = io.StringIO()
        qr.terminal(out=buffer, border=2, compact=True)
        return buffer.getvalue()

    def compose(self) -> ComposeResult:
        # Информационный текст
        yield Static(
            "Следите 👀 за релизами и обновлениями в наших группах.\n"
            "По всем вопросам и сообщениям об ошибках обращайтесь на почту 📧 me@karabedyan.ru или в указанные 💬 мессенджеры.",
            id="info_text",
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
        """При монтировании окна устанавливаем начальный QR-код."""
        self.update_qr_code("telegram")

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        """Обработчик изменения выбора в RadioSet."""
        selected_id = event.pressed.id
        if selected_id in ("telegram", "max"):
            self.update_qr_code(selected_id)

    def update_qr_code(self, messenger: str) -> None:
        """Обновляет QR-код в зависимости от выбранного мессенджера."""
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
        """Обработчик нажатия клавиши ESC."""
        self.dismiss()


class ResultProcessingDirectoryScreen(ModalScreen):
    """Экран с выводом необработанных файлов в виде таблицы:
    Имя файла / причина отказа / что можно сделать"""

    BINDINGS = [("escape", "dismiss", "Закрыть")]

    def __init__(self, failures: dict, **kwargs):
        super().__init__(**kwargs)
        self.failures = failures
        self.markdown_text = generate_failed_processing_markdown(failures)
        self.log_path = get_log_path()

    def compose(self) -> ComposeResult:
        yield Markdown(self.markdown_text, id="result_processing")
        if any(getattr(f, "technical_detail", None) for f in self.failures.values()):
            yield Static(
                f"Подробности (включая трейсбэки) — в журнале: {self.log_path}",
                id="failed_files_log_hint",
                markup=False,
            )
        yield Footer(show_command_palette=False)
