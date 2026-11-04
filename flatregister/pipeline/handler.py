"""Оркестратор обработки: одиночный файл, папка, последовательно и параллельно.

Разделение ``file_handler.py`` — Сессия S5 плана рефакторинга (см. Tasks.md).
В S8 одиночный и пакетный режимы идут через один и тот же ``_process_one``,
а результаты складываются в :class:`ProcessingResult`, а не в три словаря.
"""
import concurrent.futures
import contextlib
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Type

import pandas as pd

from flatregister.errors import (
    FileFailure,
    InputPathError,
    NoInputFilesError,
    NoRecognizedRegistersError,
    NotSpreadsheetError,
    OutputError,
    describe_exception,
)
from flatregister.excel_io import open_file, sanitize_sheet_name, write_df_in_chunks
from flatregister.logging_setup import get_logger, log_failure
from flatregister.pipeline.factory import ExcelValidator, FileProcessorFactory
from flatregister.pipeline.parallel import _process_file_parallel
from flatregister.pipeline.result_collector import ResultCollector
from flatregister.registers.base import FileProcessor

_log = get_logger("pipeline")


@dataclass
class ProcessingResult:
    """Всё, что обработка отдаёт наружу: кадры, проверки и отказы.

    Раньше это были три независимых словаря (``storage_processed_registers``,
    ``check``, ``not_correct_files``), которые очищались вручную и могли
    разъехаться: файл попадал в один, но не в другой.
    """

    register_type: Optional[str] = None
    files: Dict[str, pd.DataFrame] = field(default_factory=dict)
    checks: Dict[str, pd.DataFrame] = field(default_factory=dict)
    failures: Dict[str, FileFailure] = field(default_factory=dict)
    processed_count: int = 0

    def clear(self) -> None:
        self.files.clear()
        self.checks.clear()
        self.failures.clear()
        self.processed_count = 0


class FileHandler:
    """Обработчик файлов с последовательной обработкой"""

    # Открытие итогового файла: реализация в flatregister.excel_io.open_file.
    # Обёртка на классе нужна, чтобы тесты могли подменять FileHandler._open_file.
    _open_file = staticmethod(open_file)

    def __init__(self, parallel: bool = False):
        self.parallel = parallel
        self.validator = ExcelValidator()
        self.processor_factory = FileProcessorFactory()
        self.result = ProcessingResult()
        self.cancel_event = None

    def _record_failure(self, file_name: str, failure: FileFailure) -> None:
        """Единственное место, где отказ файла попадает в отчёт и в журнал.

        Двукратная запись в отчёт и в лог раньше была размазана по шести
        ``except`` в трёх методах, из-за чего один и тот же файл мог попасть
        в список с текстом ``str(e)``, а в журнал — не попасть вовсе.
        """
        self.result.failures[file_name] = failure
        log_failure(failure, logger=_log)

    def _process_one(
        self,
        file_path: Path,
        *,
        collect_failures: bool,
    ) -> Tuple[Optional[Type[FileProcessor]], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Обработать файл и записать отказ в журнал.

        Это единственный путь обработки одного файла. Разница между одиночным
        и пакетным режимом — только в том, глотать ли отказ: в пакете один
        плохой файл не должен останавливать папку, а в одиночном режиме ошибка
        идёт пользователю.
        """
        try:
            if not self.validator.is_valid_excel(file_path):
                raise NotSpreadsheetError(
                    NotSpreadsheetError.default_user_message,
                    hint="Укажите файл .xlsx — выгрузку регистра из 1С.",
                )
            processor = self.processor_factory.get_processor(file_path, self.result.register_type)
            result, check = processor.process_file(file_path)
            self.result.processed_count += 1
            return processor.__class__, result, check
        except Exception as error:
            self._record_failure(
                file_path.name,
                describe_exception(error, file_name=file_path.name),
            )
            if collect_failures:
                return None, None, None
            raise

    def process(
        self,
        input_path: Path,
        type_register: str,
        on_progress=None,
        cancel_event=None,

    ) -> ProcessingResult:
        """Единственная публичная точка входа в обработку.

        Сам решает «файл или папка», сам проверяет путь через
        :func:`validate_paths` (единая воронка с починкой тире),
        сам сохраняет и открывает итоговый файл.

        Args:
            input_path: Путь к файлу или папке.
            type_register: Ключ регистра из ``REGISTER_SPEC``.
            on_progress: Колбэк прогресса ``(current, total, stage_description)``.

        Returns:
            :class:`ProcessingResult` с кадрами, проверками и отказами.

        Raises:
            InputPathError: Путь не существует и не чинится заменой тире.
            NotSpreadsheetError: Файл не .xlsx.
            NoInputFilesError: В папке нет .xlsx.
            NoRecognizedRegistersError: Ни один файл не распознан.
            OutputError: Не удалось записать/открыть итоговый файл.
        """
        from flatregister.excel_io import validate_paths

        # Единая воронка валидации путей: починка тире + проверка существования.
        validate_paths([input_path])

        self.result.register_type = type_register
        self.cancel_event = cancel_event
        progress_callback = on_progress

        if input_path.is_file():
            self._process_single_file(input_path)
            if self.result.files:
                self._save_and_open_batch_result(progress_callback=progress_callback)
        elif input_path.is_dir():
            self._process_directory(input_path, progress_callback)
        else:
            raise InputPathError(
                f"Путь не существует: {input_path}",
                hint="Укажите существующий файл .xlsx или папку с выгрузками 1С.",
            )

        return self.result

    def handle_input(self, input_path: Path, type_register: str, progress_callback) -> None:
        """Обработать входной путь.

        Оставлена для обратной совместимости с тестами. Новый код должен
        пользоваться :meth:`process`, который сам решает «файл/папка» и сам
        сохраняет результат.
        """
        self.process(input_path, type_register, on_progress=progress_callback)

    def _process_single_file(self, file_path: Path) -> None:
        """Обработать одиночный файл.

        Отказ пробрасывается наверх: файла всего один, показать ошибку нужно
        непосредственно пользователю, а не прятать её в отчёте.
        """
        _processor_class, result, check = self._process_one(file_path, collect_failures=False)
        self.result.files[file_path.name] = result
        self.result.checks[file_path.name] = check

    def _process_directory(self, dir_path: Path, progress_callback) -> None:
        excel_files = self._get_excel_files(dir_path)
        result_collector = ResultCollector(self.result.register_type)

        # Проверка условия параллельной обработки
        if self.parallel and self.result.register_type != 'turnover':
            self._process_directory_parallel(excel_files, progress_callback, result_collector)
        else:
            total_files = len(excel_files)
            for i, file_path in enumerate(excel_files):
                if self.cancel_event is not None and self.cancel_event.is_set():
                    break
                processor_class, result, check = self._process_one(file_path, collect_failures=True)
                if processor_class is not None:
                    result_collector.add_result(processor_class, result, check)
                if progress_callback:
                    progress_callback(
                        i + 1,
                        total_files,
                        stage_description=f"Обработано файлов {i + 1}/{total_files}"
                    )
            if self.cancel_event is not None and self.cancel_event.is_set():
                return
            if progress_callback:
                progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
            self._save_combined_results(result_collector)

    def _process_directory_parallel(self, excel_files: List[Path], progress_callback, result_collector: ResultCollector):
        total_files = len(excel_files)

        # Определяем количество воркеров: не больше числа CPU/файлов и не
        # больше разумного потолка (много одновременных Excel через xlwings
        # нестабильно). Переопределяется через config/general_settings.
        from flatregister.config import get_max_parallel_workers

        configured = get_max_parallel_workers()
        auto = min(os.cpu_count() or 4, total_files, 4)
        max_workers = min(configured if configured > 0 else auto, total_files)
        if max_workers < 1:
            max_workers = 1

        completed: list = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(_process_file_parallel, file_path, self.result.register_type): file_path
                for file_path in excel_files
            }

            for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
                if self.cancel_event is not None and self.cancel_event.is_set():
                    for f in future_to_file:
                        if not f.done():
                            f.cancel()
                    break
                file_path = future_to_file[future]
                try:
                    file_name, processor_class, result, check, failure = future.result()
                    completed.append((file_name, processor_class, result, check, failure))
                except Exception as error:
                    # Сюда попадает только падение самого ProcessPoolExecutor
                    # (процесс убит, нет свободной памяти) — не отказ файла.
                    completed.append((
                        file_path.name, None, None, None,
                        describe_exception(error, file_name=file_path.name),
                    ))
                finally:
                    if progress_callback:
                        progress_callback(
                            i + 1,
                            total_files,
                            stage_description=f"Обработано файлов {i + 1}/{total_files}"
                        )

        if self.cancel_event is not None and self.cancel_event.is_set():
            return

        # Собираем результаты в детерминированном порядке (по имени файла),
        # чтобы итоговый отчёт не зависел от порядка завершения future.
        completed.sort(key=lambda item: item[0])
        for file_name, processor_class, result, check, failure in completed:
            if failure is not None:
                self._record_failure(file_name, failure)
            else:
                result_collector.add_result(processor_class, result, check)
                self.result.processed_count += 1

        if progress_callback:
            progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
        self._save_combined_results(result_collector)

    @staticmethod
    def _get_excel_files(dir_path: Path) -> List[Path]:
        """Получить список Excel файлов в директории"""
        files = sorted(dir_path.glob("*.xlsx"), key=lambda p: p.name)
        if not files:
            raise NoInputFilesError(
                "В папке нет файлов Excel (.xlsx).",
                hint="Положите в папку выгрузки регистров 1С, сохранённые как .xlsx.",
            )
        return files

    @staticmethod
    def _write_workbook(temp_filename: str, write_sheets) -> None:
        """Пишет книгу во временный файл и убирает его, если запись сорвалась.

        Раньше при ошибке записи временный файл оставался на диске — на диске
        накапливались недооткрытые копии итоговых отчётов, и непонятно было,
        какой из них «настоящий».
        """
        try:
            with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
                write_sheets(writer)
        except Exception as error:
            with contextlib.suppress(OSError):
                os.unlink(temp_filename)
            raise OutputError(
                "Не удалось сохранить итоговый файл.",
                hint="Проверьте свободное место на диске и что папка с результатом "
                     "доступна для записи. Подробности — в журнале logs/app.log.",
            ) from error

    def _save_combined_results(self, result_collector: ResultCollector) -> None:
        """Сохранить объединенные результаты"""
        results = result_collector.get_all_results()

        # Проверка наличия результатов
        if not any(not df.empty for df, _ in results.values()):
            raise NoRecognizedRegistersError(
                "В папке не найдено выгрузок выбранного регистра 1С.",
                hint="Файлы в папке есть, но ни один не распознан как выбранный "
                     "тип регистра. Проверьте выбор регистра и то, что файлы "
                     "сохранены именно как .xlsx.",
            )

        sheet_mapping = {
            'upp': 'UPP',
            'non_upp': 'Non_UPP',
            'analisys': ('analisys', 'analisys_check'),
            'turnover': ('turnover', 'turnover_check'),
            'accountosv': ('accountosv', 'accountosv_check'),
            'generalosv': ('generalosv', 'generalosv_check')
        }

        def write_sheets(writer) -> None:
            for key, sheets in sheet_mapping.items():
                df, df_check = results[key]
                if not df.empty:
                    df = df.map(lambda x: x.rstrip() if isinstance(x, str) else x)
                    write_df_in_chunks(writer, df, sheets[0] if isinstance(sheets, tuple) else sheets)
                if isinstance(sheets, tuple) and not df_check.empty:
                    write_df_in_chunks(writer, df_check, sheets[1])

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name

        self._write_workbook(temp_filename, write_sheets)
        self._open_file(temp_filename)

    def _save_and_open_batch_result(self, progress_callback) -> None:
        """Сохранить и открыть результаты обработки одиночных файлов"""
        if not self.result.files and not self.result.checks:
            return

        progress_callback(stage_description='Сохраняем и открываем итоговый файл')

        def write_sheets(writer) -> None:
            # Основные данные
            for sheet_name, df in self.result.files.items():
                safe_name = sanitize_sheet_name(sheet_name)
                df = df.map(lambda x: x.rstrip() if isinstance(x, str) else x)
                write_df_in_chunks(writer, df, safe_name)

            # Данные проверки
            for sheet_name, df in self.result.checks.items():
                safe_name = sanitize_sheet_name(f"Проверка_{sheet_name}")
                if not df.empty:
                    df = df.map(lambda x: x.rstrip() if isinstance(x, str) else x)
                    write_df_in_chunks(writer, df, safe_name)

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name

        self._write_workbook(temp_filename, write_sheets)
        self._open_file(temp_filename)
