"""Сообщения UI: прогресс обработки.

Ядро (``flatregister.pipeline.handler.FileHandler``) зовёт переданный колбэк
прогресса ``progress_callback(current, total, stage_description)`` — и это
делается в рабочем потоке ``@work(thread=True)``. Прямое присваивание виджетов
из того потока нарушило бы потоковую модель Textual, поэтому виджеты из
воркера не дёргаются. Вместо этого UI поставляет в ядро дешёвый
адаптер :class:`ProgressAdapter`, который лишь публикует :class:`ProgressUpdate`
через ``post_message`` (он потокобезопасен).
"""

from __future__ import annotations

from typing import Callable, Optional

from textual.message import Message


class ProgressUpdate(Message):
    """Обновление индикатора прогресса.

    ``total=None`` — индикатор неопределённый (этап без измеримого объёма:
    начало обработки одиночного файла или сохранение итогового файла).
    ``current`` при ``total=None`` не используется.
    """

    def __init__(self, stage: str, current: Optional[int] = None, total: Optional[int] = None) -> None:
        super().__init__()
        self.stage = stage
        self.current = current
        self.total = total


class ProgressAdapter:
    """Адаптер между сигнатурой ядра и сообщением UI.

    Ядро вызывает ``progress_callback(current, total, stage_description)``;
    адаптер переводит вызов в :class:`ProgressUpdate` и публикует его через
    переданный ``poster`` (``App.post_message`` — потокобезопасен).
    """

    def __init__(self, poster: Callable[[Message], bool]) -> None:
        self._poster = poster

    def __call__(self, current: int = 0, total: int = 0, stage_description: str = "") -> None:
        # Сентинел ядра «Сохраняем и открываем итоговый файл»: total == 0,
        # current == 0 — это неопределённый этап, а не «прогресс 0 из 0».
        if total == 0:
            self._poster(ProgressUpdate(stage_description, current=None, total=None))
        else:
            self._poster(ProgressUpdate(stage_description, current=current, total=total))

