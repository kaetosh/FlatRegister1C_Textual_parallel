"""Экран прогресса: единый индикатор для одиночного файла и папки.

Раньше здесь было два экрана — ``LoaderIndicatorCustom`` (спиннер) и
``ProgressBarCustom`` (счётчик + полоса) — и UI сам выбирал их по тому,
файл это или папка. В S10 остался один экран: ``ProgressBar`` умеет
неопределённый режим ``total=None``, поэтому спиннер и пересборка
виджета (``time.sleep(1)`` + ``mount``/``remove``) не нужны.

Экран не закрывается по Esc: обработка выполняется в фоновом потоке и
не прерывается нажатием клавиши. Живой поток после закрытия окна продолжил
бы работу, держа Excel и ``ProcessPoolExecutor`` — поэтому кнопка «Отмена»
не ожидается (см. план S10, открытое решение E1).

Обработчик :meth:`on_progress_update` вызывается из UI-потока: сообщения
:class:`ProgressUpdate` публикует адаптер, работающий в рабочем потоке
через потокобезопасный ``App.post_message`` — виджеты фоновый поток
напрямую не трогает.

Режимы индикатора:

* ``total=None`` — неопределённый прогресс (волна). Используется для
  одиночного файла и для этапа «Сохраняем и открываем итоговый файл»,
  когда общее число шагов неизвестно.
* ``total=N`` — детерминированный прогресс с полосой. Используется для
  пакетной обработки папки: ядро передаёт
  ``progress_callback(i, total_files, ...)`` на каждом файле.
"""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import Label, ProgressBar

from ui.messages import ProgressUpdate


class ProgressScreen(ModalScreen):
    """Модальный индикатор обработки."""

    # пусто намеренно: экран не закрывается по Esc
    BINDINGS: list = []

    def compose(self) -> ComposeResult:
        yield Container(
            Label("Обработка", id="label_progress"),
            ProgressBar(
                total=None,
                show_percentage=False,
                show_eta=False,
                id="progress",
            ),
            id="screen_progress_bar",
        )

    def on_progress_update(self, event: ProgressUpdate) -> None:
        bar = self.query_one("#progress", ProgressBar)
        label = self.query_one("#label_progress", Label)

        if event.total is not None:
            # Детерминированный режим: известно общее число шагов.
            bar.update(total=event.total, progress=event.current or 0)
            label.update(event.stage)
        else:
            # Неопределённый режим: только текст этапа и волна.
            bar.update(total=None, progress=0)
            label.update(event.stage)
