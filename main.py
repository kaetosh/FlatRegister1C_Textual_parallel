import time
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
                             Select,
                             SelectionList,
                             Static)

from file_handler import FileHandler
from support_functions import (DEFAULT_CONFIG,
                               generate_failed_processing_markdown,
                               is_valid_dragged_path,
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

class SettingsScreen(ModalScreen):
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
                id="static-settings-modal"
            ),
            
            VerticalScroll(
                SelectionList(id="selection-list-settings-modal"),
                id="scrollable-selection-list"
            ),
            
            Static(
                "¹нужно изменить список? Напишите в https://t.me/Python_for_the_clerk",
                id="static_help-settings-modal"
            ),
            
            Horizontal(
                Button("Сохранить", variant="success", id="button-save-settings-modal", classes='buttons-settings-modal'),
                Button("Отмена", variant="primary", id="button-cancel-settings-modal", classes='buttons-settings-modal'),
                id="horizontals-button-settings-modal"
            ),
            
            id="container-settings-modal"
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
                        description="Настройки",
                        key_display="F3")
                        ]
    
    
    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield ScrollableContainer(Markdown(app.register_requirements),
                                  id='scroll_input',
                                  can_focus=False # Отключаем возможность фокуса
                                  ) 
        yield Horizontal(
            Input(placeholder="Перетащите файл или папку в окно программы и нажмите Enter.",
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
        self.sub_title = 'Формирует удобную для анализа плоскую таблицу'
        self.query_one("#path_register").focus()

    def action_open_settings(self):
        if self.app.register not in ["analisys", "turnover"]:
            self.notify(
                "Настройки недоступны для этого типа регистра",
                severity="warning",
                timeout=3
            )
            return
        self.app.push_screen(SettingsScreen())
    
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
        file_handler = FileHandler()
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
        if failed_processing_markdown:
            app.failed_processing_markdown = failed_processing_markdown
            self.app.push_screen(ResultProcessingDirectotyScreen())
    
    def on_error(self, error: str) -> None:
        
        while len(self.app.screen_stack) > 1:
            self.app.pop_screen()
        self.notify(f"Ошибка обработки: {error}")

class FlatRegister1CApp(App):
    CSS_PATH = "style.tcss"

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
    
    def on_mount(self) -> None:
        self.title = '🤖 Обработчик регистров 1С 6-в-1'
        self.sub_title = 'Формирует удобную для анализа плоскую таблицу'    
        
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
                self.push_screen(RegisterScreen())
            
if __name__ == "__main__":
    app = FlatRegister1CApp()
    app.run()
    
    
    
    
    
    
    
    
    
    