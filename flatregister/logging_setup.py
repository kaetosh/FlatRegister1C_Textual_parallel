# -*- coding: utf-8 -*-
"""Журналирование: единственное место, где живёт полный трейсбек.

До Сессии S1 в проекте не было ни одного вызова ``logging``. Ошибка, не
поймавшаяся узким ``except``, исчезала: пользователь видел одну строку, а
восстановить, на какой строке упало, было нечем — а именно это нужно для
поиска ошибки в будущих прогонах на реальных выгрузках.

Раскладка
---------
* в лог уходят **все** ошибки, включая ожидаемые: без них нельзя понять,
  какие файлы и по какой причине выпали из обработки;
* на экран уходят только ``user_message`` и ``hint`` (см.
  :mod:`flatregister.errors`), трейсбек туда не попадает никогда;
* файлы и папки журнала исключены из git, но сам журнал переживает перезапуск
  приложения.
"""

from __future__ import annotations

import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_FILE_NAME = "app.log"
MAX_BYTES = 2 * 1024 * 1024   # 2 МБ на файл
BACKUP_COUNT = 3              # app.log.1 … app.log.3
LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_configured = False


def get_log_dir() -> Path:
    """Каталог журналов.

    В собранном .exe рядом с исполняемым файлом — так пользователь найдёт журнал
    там же, где саму программу. В режиме разработки — в корне проекта.
    Если каталог недоступен для записи, откатываемся на %LOCALAPPDATA%,
    чтобы приложение не падало из-за отсутствия прав.
    """
    if getattr(sys, "frozen", False):
        preferred = Path(sys.executable).resolve().parent / "logs"
    else:
        preferred = Path(__file__).resolve().parent.parent / "logs"

    try:
        preferred.mkdir(parents=True, exist_ok=True)
        return preferred
    except OSError:
        fallback = Path(os.environ.get("LOCALAPPDATA") or Path.home()) / "FlatRegister1C" / "logs"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


def get_log_path() -> Path:
    """Полный путь к текущему файлу журнала — его показываем пользователю."""
    return get_log_dir() / LOG_FILE_NAME


def setup_logging(level: int = logging.INFO) -> Path:
    """Настраивает журнал приложения. Повторные вызовы безопасны.

    Возвращает путь к файлу журнала, чтобы вызывающий код мог показать его
    пользователю в тексте ошибки.
    """
    global _configured
    if _configured:
        return get_log_path()

    root = logging.getLogger("flatregister")
    root.setLevel(level)
    root.propagate = False

    log_path = get_log_path()
    try:
        handler = RotatingFileHandler(
            log_path, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
        )
    except OSError:
        # Диск занят/защищён — не даём логированию сломать обработку.
        root.addHandler(logging.NullHandler())
        _configured = True
        return log_path

    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    root.addHandler(handler)
    _configured = True
    return log_path


def get_logger(name: str) -> logging.Logger:
    """Логгер внутри пространства имён ``flatregister``."""
    return logging.getLogger(name if name.startswith("flatregister") else f"flatregister.{name}")


def log_failure(failure, *, logger: logging.Logger | None = None) -> None:
    """Записывает в журнал результат :func:`flatregister.errors.describe_exception`.

    Ожидаемые отказы регистрируются на уровне WARNING, всё остальное — на
    ERROR с полным трейсбеком. Благодаря этому по журналу видно, что было
    штатным отказом отдельного файла, а что — падением обработки.
    """
    target = logger or get_logger("failures")
    if not failure.technical_detail:
        target.warning("ФАЙЛ: %s — %s", failure.file_name or "-", failure.user_message)
        return
    target.error(
        "ФАЙЛ: %s — %s\n%s",
        failure.file_name or "-",
        failure.user_message,
        failure.technical_detail,
    )