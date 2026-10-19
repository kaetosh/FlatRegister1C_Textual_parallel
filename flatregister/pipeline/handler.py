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

    def handle_input(self, input_path: Path, type_register: str, progress_callback) -> None:
        self.result.register_type = type_register
        if input_path.is_file():
            self._process_single_file(input_path)
        elif input_path.is_dir():
            self._process_directory(input_path, progress_callback)

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
            self._process_directory_parallel(dir_path, progress_callback, result_collector)
        else:
            total_files = len(excel_files)
            for i, file_path in enumerate(excel_files):
                processor_class, result, check = self._process_one(file_path, collect_failures=True)
                if processor_class is not None:
                    result_collector.add_result(processor_class, result, check)
                progress_callback(
                    i + 1,
                    total_files,
                    stage_description=f"Обработано файлов {i + 1}/{total_files}"
                )
            progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
            self._save_combined_results(result_collector)

    def _process_directory_parallel(self, dir_path: Path, progress_callback, result_collector: ResultCollector):
        excel_files = self._get_excel_files(dir_path)
        total_files = len(excel_files)

        # Определяем количество воркеров (например, не больше числа CPU и не больше количества файлов)
        max_workers = min(os.cpu_count() or 4, total_files)

        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(_process_file_parallel, file_path, self.result.register_type): file_path
                for file_path in excel_files
            }

            for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
                file_path = future_to_file[future]
                try:
                    file_name, processor_class, result, check, failure = future.result()
                    if failure is not None:
                        self._record_failure(file_name, failure)
                    else:
                        result_collector.add_result(processor_class, result, check)
                        self.result.processed_count += 1
                except Exception as error:
                    # Сюда попадает только падение самого ProcessPoolExecutor
                    # (процесс убит, нет свободной памяти) — не отказ файла.
                    self._record_failure(
                        file_path.name,
                        describe_exception(error, file_name=file_path.name),
                    )
                finally:
                    progress_callback(
                        i + 1,
                        total_files,
                        stage_description=f"Обработано файлов {i + 1}/{total_files}"
                    )

        progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
        self._save_combined_results(result_collector)

    @staticmethod
    def _get_excel_files(dir_path: Path) -> List[Path]:
        """Получить список Excel файлов в директории"""
        files = list(dir_path.glob("*.xlsx"))
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
                df.to_excel(writer, sheet_name=safe_name, index=False)

            # Данные проверки
            for sheet_name, df in self.result.checks.items():
                safe_name = sanitize_sheet_name(f"Проверка_{sheet_name}")
                if not df.empty:
                    df.to_excel(writer, sheet_name=safe_name, index=False)

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name

        self._write_workbook(temp_filename, write_sheets)
        self._open_file(temp_filename)
