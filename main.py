import time
import segno
import io
from functools import lru_cache
from typing import List
from pathlib import Path

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import (Center,
                                Container,
                                Horizontal,
                                ScrollableContainer,
                                VerticalScroll)

from textual.reactive import reactive
from textual.screen import ModalScreen, Screen
from textual.validation import Function
from textual.widgets import (Button,
                             Footer,
                             Input,
                             Header,
                             Label,
                             LoadingIndicator,
                             Markdown,
                             ProgressBar,
                             RadioButton,
                             RadioSet,
                             Select,
                             SelectionList,
                             Static,
                             Switch)

from file_handler import FileHandler
from support_functions import (DEFAULT_CONFIG,
                               generate_failed_processing_markdown,
                               is_valid_dragged_path,
                               get_parallel_processing_option,
                               read_config,
                               update_config)
from text import REQUIREMENTS, START_WINDOW

class LoaderIndicatorCustom(ModalScreen):
    def compose(self) -> ComposeResult:
        yield Container(
            Label("Обработка", id="label_loader"),
            LoadingIndicator(id="outer_loadindicator"),
            id='screen_loader_indicator',
                   )
    def update_progress(self, stage_description: str = ''):
        self.query_one("#label_loader").update(stage_description)
        
class ProgressBarCustom(ModalScreen):
    """Модальное окно с индикатором прогресса"""
    
    def compose(self) -> ComposeResult:
        yield Container(
            Label("Обработка", id="label_progress"),
            ProgressBar(id="progress"),
            id='screen_progress_bar'
        )
    
    def update_progress(self, current: int = 0, total: int = 0, stage_description: str = ''):
        """Обновление прогресса"""
        
        if total == 0:
            # Планируем замену виджета после обновления
            time.sleep(1)
            self.call_after_refresh(self._switch_to_loading_indicator)
        else:
            # Обновляем ProgressBar
            self.query_one(ProgressBar).update(total=total, progress=current)
        self.query_one("#label_progress").update(stage_description)
        
    
    async def _switch_to_loading_indicator(self):
        """Внутренний метод для переключения на LoadingIndicator"""
        container = self.query_one(Container)
        old_widget = self.query_one(ProgressBar)
        new_widget = LoadingIndicator(id="inner_loadindicator")
        
        await container.mount(new_widget, before=old_widget)
        await old_widget.remove()

class ResultProcessingDirectotyScreen(ModalScreen):
    """Экран с выводом необработанных файлов в виде таблицы:
    Имя файла/причина неудачной обработки"""
    BINDINGS = [("escape", "dismiss", "Закрыть")]
    
    def compose(self) -> ComposeResult:
        yield Markdown(app.failed_processing_markdown, id='result_processing')
        yield Footer(show_command_palette=False)

class SettingsScreenRegister(ModalScreen):
    """
    Окно с настройками обработчика анализа счетов.
    """
    
    def compose(self) -> ComposeResult:
                
        # 1. Читаем конфигурацию при создании окна
        config = read_config()
        
        # Устанавливаем значения по умолчанию
        self.app.accounts_without_subaccount = config.get("accounts_without_subaccount",
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
    
    def on_mount(self) -> None:
        self.query_one(SelectionList).clear_options()
        self.query_one(SelectionList).add_options([tuple(item) for item in self.app.accounts_without_subaccount])
        
    def on_button_pressed(self, event: Button.Pressed):
        """Обрабатывает нажатие кнопки "Сохранить"."""
        if event.button.id == "button-save-settings-modal":
                       
            selected_values = self.query_one(SelectionList).selected
            selected_set = set(selected_values)
            
            # Обновляем словарь
            for account in self.app.accounts_without_subaccount:
                account_code = account[0]  # первый элемент - код счета
                account[2] = 1 if account_code in selected_set else 0
            
            updates={"accounts_without_subaccount": self.app.accounts_without_subaccount}
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
        yield ScrollableContainer(Markdown(app.register_requirements),
                                  id='scroll_input',
                                  can_focus=False # Отключаем возможность фокуса
                                  ) 
        yield Horizontal(
            Input(placeholder="Перетащите .xlsx файл регистра или папку c .xlsx файлами регистров в окно программы и нажмите Enter.",
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
        if self.app.register not in ["analisys", "turnover"]:
            self.notify(
                "Настройки недоступны для этого типа регистра",
                severity="warning",
                timeout=3
            )
            return
        self.app.push_screen(SettingsScreenRegister())
    
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
                
                self.process_file(normal_input_path, app.register, progress_screen)  # Запускаем фоновую задачу с передачей ссылки на экран
            else:
                # Если валидация не пройдена
                self.notify("Проверьте пути!")
    
    @work(thread=True)  # Запускаем в отдельном потоке, чтобы не блокировать UI
    def process_file(self, input_path, register, progress_screen) -> None:
        parallel_processing_option = get_parallel_processing_option()
        file_handler = FileHandler(parallel = parallel_processing_option)
        failed_processing_markdown = ''
        
        try:
            file_handler.handle_input(input_path,
                                      register,
                                      progress_callback=progress_screen.update_progress)  # Передаем callback

            if file_handler.storage_processed_registers:
                file_handler._save_and_open_batch_result(progress_callback=progress_screen.update_progress) # Передаем callback
            
            if file_handler.not_correct_files:
                failed_processing_markdown = generate_failed_processing_markdown(file_handler.not_correct_files)
                file_handler.not_correct_files.clear()
            
            # Очистка хранилища
            if file_handler.storage_processed_registers:
                file_handler.storage_processed_registers.clear()
            if file_handler.check:
                file_handler.check.clear()

            self.app.call_from_thread(self.on_success, failed_processing_markdown)  # Возвращаемся в основной поток
        except Exception as error_description:
            self.app.call_from_thread(self.on_error, str(error_description))  # Обработка ошибок
    
    def on_success(self, failed_processing_markdown) -> None:
        
        while len(self.app.screen_stack) > 1:
            self.app.pop_screen()
        self.notify("Обработка завершена!")
        self.app.contact_visibility = 1
        if failed_processing_markdown:
            app.failed_processing_markdown = failed_processing_markdown
            self.app.push_screen(ResultProcessingDirectotyScreen())
    
    def on_error(self, error: str) -> None:
        
        while len(self.app.screen_stack) > 1:
            self.app.pop_screen()
        self.notify(f"Ошибка обработки: {error}")
        self.app.contact_visibility = 1
        
    def key_escape(self):
        print('Нажали escape!!!')
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

class FlatRegister1CApp(App):
    CSS_PATH = "style.tcss"
    
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
    
    # тип регистра (ОСВ, анализ счета, обороты счета и т.д.)
    register = reactive("Не выбран регистр!")
    
    # описание требований к выбранному регистру
    register_requirements = reactive("Отсутствует описание, т.к. не выбран регистр!")
    
    # вывод информации о необработанных в результате ошибок файлов
    failed_processing_markdown = reactive("")
    
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
                    (
                        ("1. Отчет по проводкам", 'posting'),
                        ("2. Карточка счета", 'card'),
                        ("3. Анализ счета", 'analisys'),
                        ("4. Обороты счета", 'turnover'),
                        ("5. ОСВ счета", 'accountosv'),
                        ("6. ОСВ общая", 'generalosv')
                    ),
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
                self.register_requirements = REQUIREMENTS.get(select.value, "Не выбран регистр для обработки!")
                self.contact_visibility = 0
                self.push_screen(RegisterScreen())
                
    # скрытие кнопки с контактами
    def check_action(self, action: str, parameters: tuple[object, ...]) -> bool | None:
        """Check if an action may run."""
        if action == "open_contacts" and self.contact_visibility == 0:
            return False
        return True
            
if __name__ == "__main__":
    app = FlatRegister1CApp()
    app.run()
    
    
    
    
    
    
    
    
    
    