"""Точка запуска обработки: единственный путь из UI в ядро.

Воркер стартует из UI-потока через ``app.run_worker(..., group="processing",
exclusive=True)``, прогресс — сообщениями :class:`ProgressUpdate` через
потокобезопасный ``App.post_message``.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from textual.app import App
from textual.worker import Worker, WorkerState

from flatregister.config import get_parallel_processing_option
from flatregister.errors import describe_exception
from flatregister.logging_setup import get_logger, log_failure
from flatregister.pipeline.handler import FileHandler
from ui.messages import ProgressAdapter

_log = get_logger("ui")


class ProcessingState:
    """Статус обработки на App — носитель воркера."""

    def __init__(self) -> None:
        self._worker: Worker | None = None

    def start(self, worker: Worker) -> None:
        self._worker = worker

    def discard(self, *, clear: bool = True) -> None:
        if clear:
            self._worker = None

    @property
    def active_worker(self) -> Worker | None:
        return self._worker if self._worker and self._worker.state in (WorkerState.RUNNING, WorkerState.PENDING) else None


def _processing_thread_body(
    input_path: Path, register: str, adapter: ProgressAdapter, on_done, cancel_event=None
) -> None:
    """Тело рабочего потока. В UI-поток возвращается только через ``on_done``."""
    file_handler = FileHandler(parallel=get_parallel_processing_option())
    try:
        # Единственный публичный вход в ядро: process() сам решает «файл/папка»,
        # сам валидирует путь и сам сохраняет результат.
        file_handler.process(input_path, register, on_progress=adapter, cancel_event=cancel_event)

        failures = dict(file_handler.result.failures)
        processed_count = file_handler.result.processed_count
        file_handler.result.clear()

        on_done(None, failures, processed_count)
    except Exception as error:
        failure = describe_exception(
            error, file_name=getattr(input_path, "name", "")
        )
        log_failure(failure, logger=_log)
        on_done(failure, dict(file_handler.result.failures), 0)


def run_processing(
    app: App,
    *,
    input_path: Path,
    register: str,
    adapter: ProgressAdapter,
    on_done,
    cancel_event=None,
) -> Worker:
    """Запустить обработку в фоновом потоке и вернуть воркер.

    ``app.run_worker`` проверяет, что вызван из UI-потока, и сам стартует
    воркер. ``exclusive=True`` отменяет предыдущий воркер той же группы —
    то есть повторный запуск режет старую обработку.
    """

    async def _worker_body() -> None:
        await asyncio.to_thread(
            _processing_thread_body, input_path, register, adapter, on_done, cancel_event
        )

    worker = app.run_worker(
        _worker_body(),
        name="processing",
        group="processing",
        exclusive=True,
        thread=False,
    )
    return worker
