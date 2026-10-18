"""Вспомогательные виджеты прогресса: индикатор и полоса.

Вынесено из ``main.py`` в Сессии S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось.
"""
import time

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import Label, LoadingIndicator, ProgressBar


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
