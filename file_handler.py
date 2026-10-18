
import concurrent.futures
import contextlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Literal, Tuple

import pandas as pd

from flatregister.errors import (
    FileFailure,
    NoInputFilesError,
    NoRecognizedRegistersError,
    NotSpreadsheetError,
    OutputError,
    RegisterNotRecognizedError,
    describe_exception,
)
from flatregister.logging_setup import get_logger, log_failure
from register_processors.accountosv_processor import AccountOSV_NonUPPFileProcessor, AccountOSV_UPPFileProcessor
from register_processors.analisys_processor import Analisys_NonUPPFileProcessor, Analisys_UPPFileProcessor
from register_processors.card_processor import Card_NonUPPFileProcessor, Card_UPPFileProcessor

# Импорт процессоров
from register_processors.class_processor import DESIRED_ORDER, FileProcessor
from register_processors.generalosv_processor import GeneralOSV_NonUPPFileProcessor, GeneralOSV_UPPFileProcessor
from register_processors.posting_processor import Posting_NonUPPFileProcessor, Posting_UPPFileProcessor
from register_processors.turnover_processor import Turnover_NonUPPFileProcessor, Turnover_UPPFileProcessor
from support_functions import fix_1c_excel_case, sort_columns, write_df_in_chunks

_log = get_logger("pipeline")


def _process_file_parallel(file_path: Path, type_register: str) -> tuple:
    """Функция для выполнения в отдельном процессе.

    Бросать исключения отсюда нельзя: они не переживут границу процессов.
    Вместо этого наружу отдаётся :class:`FileFailure` — он picklable и несёт
    готовый текст для пользователя вместе с полным трейсбеком для журнала.

    Журналирование здесь намеренно **нет**: несколько процессов писали бы в
    один rotating-файл и мешали друг другу при ротации. Трейсбек приезжает
    в родительский процесс строкой и дописывается им (см.
    :meth:`FileHandler._record_failure`).
    """
    try:
        if not ExcelValidator.is_valid_excel(file_path):
            return file_path.name, None, None, None, FileFailure(
                file_name=file_path.name,
                user_message=NotSpreadsheetError.default_user_message,
                hint='Обрабатываются только файлы .xlsx.',
            )
        processor = FileProcessorFactory.get_processor(file_path, type_register)
        result, check = processor.process_file(file_path)
        return file_path.name, processor.__class__, result, check, None
    except Exception as error:
        return (file_path.name, None, None, None,
                describe_exception(error, file_name=file_path.name))

class ExcelValidator:
    """Валидатор Excel файлов"""
    @staticmethod
    def is_valid_excel(file_path: Path) -> bool:
        return file_path.suffix.lower() == '.xlsx'


class FileProcessorFactory:
    """Фабрика для создания обработчиков файлов"""
    
    REGISTER_DISPLAY_NAMES = {
        'posting': 'Отчет по проводкам',
        'card': 'Карточка счета', 
        'analisys': 'Анализ счета',
        'turnover': 'Обороты счета',
        'accountosv': 'ОСВ счета',
        'generalosv': 'общая ОСВ'
    }
    
    REGISTER_PATTERNS = {
        'card': [
            {'pattern': {'дата', 'документ', 'операция'}, 'processor': Card_UPPFileProcessor},
            {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Card_NonUPPFileProcessor}
        ],
        'posting': [
            {'pattern': {'дата', 'документ', 'содержание', 'дт', 'кт', 'сумма'}, 'processor': Posting_UPPFileProcessor},
            {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Posting_NonUPPFileProcessor}
        ],
        'analisys': [
            {'pattern': {'счет', 'кор.счет', 'с кред. счетов', 'в дебет счетов'}, 'processor': Analisys_UPPFileProcessor},
            {'pattern': {'счет', 'кор. счет', 'дебет', 'кредит'}, 'processor': Analisys_NonUPPFileProcessor}
        ],
        'turnover': [
            {'pattern': {'субконто', 'нач. сальдо деб.', 'нач. сальдо кред.', 'деб. оборот', 'кред. оборот', 'кон. сальдо деб.', 'кон. сальдо кред.'}, 'processor': Turnover_UPPFileProcessor},
            {'pattern': {'счет', 'начальное сальдо дт', 'начальное сальдо кт', 'оборот дт', 'оборот кт', 'конечное сальдо дт', 'конечное сальдо кт'}, 'processor': Turnover_NonUPPFileProcessor}
        ],
        'accountosv': [
            {'pattern': {'субконто', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': AccountOSV_UPPFileProcessor},
            {'pattern': {'счет', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': AccountOSV_NonUPPFileProcessor}
        ],
        'generalosv': [
            {'pattern': {'счет', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_UPPFileProcessor},
            {'pattern': {'счет', 'наименование счета', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_NonUPPFileProcessor}
        ]
    }

    @classmethod
    def get_processor(cls, file_path: Path, type_register: str) -> FileProcessor:
        """Получить процессор для файла на основе шаблонов заголовков"""
        fixed_data = fix_1c_excel_case(file_path)
        
        # Чтение только первых строк для анализа
        df = pd.read_excel(fixed_data, header=None, nrows=20)
        str_df = df.map(lambda x: str(x).strip().lower() if pd.notna(x) else '')
        
        for pattern_config in cls.REGISTER_PATTERNS[type_register]:
            if cls._contains_pattern(str_df, pattern_config['pattern']):
                return pattern_config['processor']()
        
        raise RegisterNotRecognizedError(
            "Файл не является выбранным регистром из 1С.",
            hint="Проверьте, что выбран тип регистра, совпадающий с выгрузкой. "
                 "Карточка счёта и отчёт по проводкам из 1С УПП имеют другие "
                 "заголовки, чем одноимённые выгрузки из остальных конфигураций.",
        )

    @staticmethod
    def _contains_pattern(df: pd.DataFrame, pattern: set) -> bool:
        """Проверить, содержит ли DataFrame заданный паттерн"""
        for _, row in df.iterrows():
            if pattern.issubset(set(row)):
                return True
        return False


class ResultCollector:
    def __init__(self):
        self.upp_results = []
        self.non_upp_results = []
        self.analisys_results = {}      # key: class -> (results_list, checks_list)
        self.turnover_results = {}       # аналогично
        self.accountosv_results = {}     # аналогично
        self.generalosv_results = {}     # аналогично
        self.type_register = None

    def add_result(self, processor_class, result: pd.DataFrame, check: pd.DataFrame):
        # UPP
        if processor_class in (Card_UPPFileProcessor, Posting_UPPFileProcessor):
            self.upp_results.append(result)
        # NonUPP (кроме аналитических)
        elif processor_class in (Card_NonUPPFileProcessor, Posting_NonUPPFileProcessor):
            self.non_upp_results.append(result)
        # Analisys
        elif processor_class in (Analisys_UPPFileProcessor, Analisys_NonUPPFileProcessor):
            self.analisys_results.setdefault(processor_class, ([], []))[0].append(result)
            if not check.empty:
                self.analisys_results[processor_class][1].append(check)
        # Turnover
        elif processor_class in (Turnover_UPPFileProcessor, Turnover_NonUPPFileProcessor):
            self.turnover_results.setdefault(processor_class, ([], []))[0].append(result)
            if not check.empty:
                self.turnover_results[processor_class][1].append(check)
        # AccountOSV
        elif processor_class in (AccountOSV_UPPFileProcessor, AccountOSV_NonUPPFileProcessor):
            self.accountosv_results.setdefault(processor_class, ([], []))[0].append(result)
            if not check.empty:
                self.accountosv_results[processor_class][1].append(check)
        # GeneralOSV
        elif processor_class in (GeneralOSV_UPPFileProcessor, GeneralOSV_NonUPPFileProcessor):
            self.generalosv_results.setdefault(processor_class, ([], []))[0].append(result)
            if not check.empty:
                self.generalosv_results[processor_class][1].append(check)
        else:
            self.non_upp_results.append(result)
    
    def get_all_results(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        # Обработка UPP
        upp_df = self._concat_and_sort(self.upp_results, 'upp') if self.upp_results else pd.DataFrame()
        
        # Обработка NonUPP
        non_upp_df = self._concat_and_sort(self.non_upp_results, 'not_upp') if self.non_upp_results else pd.DataFrame()
        

        analisys_df, analisys_check_df = self._process_analisys_like(self.analisys_results)
        turnover_df, turnover_check_df = self._process_analisys_like(self.turnover_results)
        accountosv_df, accountosv_check_df = self._process_analisys_like(self.accountosv_results)
        generalosv_df, generalosv_check_df = self._process_analisys_like(self.generalosv_results)
        
        return {
            'upp': (upp_df, pd.DataFrame()),
            'non_upp': (non_upp_df, pd.DataFrame()),
            'analisys': (analisys_df, analisys_check_df),
            'turnover': (turnover_df, turnover_check_df),  # Используем сохраненные переменные
            'accountosv': (accountosv_df, accountosv_check_df),  # Используем сохраненные переменные
            'generalosv': (generalosv_df, generalosv_check_df)  # Используем сохраненные переменные
        }

    def _process_analisys_like(self, results_dict: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Обработка группы результатов с возможным вызовом shiftable_level к финальному df"""
        all_dfs = []
        all_check_dfs = []
        last_shiftable_cls = None  # для финального вызова
        
        for cls, (res_list, check_list) in results_dict.items():
            if res_list:
                combined = pd.concat(res_list)
                # Убрали вызов shiftable_level для каждого класса
                all_dfs.append(combined)
            if check_list:
                all_check_dfs.append(pd.concat(check_list))
            last_shiftable_cls = cls  # запоминаем последний класс с shiftable_level
        
        df = pd.concat(all_dfs) if all_dfs else pd.DataFrame()
        check_df = pd.concat(all_check_dfs) if all_check_dfs else pd.DataFrame()
        
        # Применяем shiftable_level к финальному df, используя последний класс с shiftable_level
        if not df.empty and last_shiftable_cls is not None:
            instance = last_shiftable_cls()
            df = instance.shiftable_level(df)
        
        # Сортировка (если нужна)
        if not df.empty and hasattr(self, 'type_register'):
            df = sort_columns(df, DESIRED_ORDER[self.type_register].get('upp', []))
        
        return df, check_df
    
    
        
    def _concat_and_sort(self, results: List[pd.DataFrame], processor_type: str) -> pd.DataFrame:
        if not results:
            return pd.DataFrame()
        combined = pd.concat(results)
        if hasattr(self, 'type_register'):
            order = DESIRED_ORDER[self.type_register].get(processor_type, [])
            return sort_columns(combined, order)
        return combined

    
    


class FileHandler:
    """Обработчик файлов с последовательной обработкой"""

    def __init__(self, parallel: bool = False):
        self.parallel = parallel
        self.validator = ExcelValidator()
        self.processor_factory = FileProcessorFactory()

        self.not_correct_files: Dict[str, FileFailure] = {}  # имя файла: причина отказа
        self.storage_processed_registers = {}
        self.check = {}
        self.type_register = None

    def _record_failure(self, file_name: str, failure: FileFailure) -> None:
        """Единственное место, где отказ файла попадает в отчёт и в журнал.

        Двукратная запись в отчёт и в лог раньше была размазана по шести
        ``except`` в трёх методах, из-за чего один и тот же файл мог попасть
        в список с текстом ``str(e)``, а в журнал — не попасть вовсе.
        """
        self.not_correct_files[file_name] = failure
        log_failure(failure, logger=_log)

    def _process_file_and_record(self, file_path: Path) -> tuple:
        """Обработать один файл, при отказе записать его в отчёт.

        Возвращает ``(processor_class, result, check)``. Отказ **не**
        пробрасывается: в пакетном режиме один плохой файл не должен
        останавливать папку.
        """
        try:
            processor = self.processor_factory.get_processor(file_path, self.type_register)
            result, check = processor.process_file(file_path)
            return processor.__class__, result, check
        except Exception as error:
            self._record_failure(file_path.name, describe_exception(error, file_name=file_path.name))
            return None, None, None

    def handle_input(self, input_path: Path,
                    type_register: Literal['posting', 'card', 'analisys', 'turnover',
                                          'accountosv', 'generalosv'],
                    progress_callback) -> None:
        self.type_register = type_register
        if input_path.is_file():
            self._process_single_file(input_path)
        elif input_path.is_dir():
            self._process_directory(input_path, progress_callback)

    def _process_single_file(self, file_path: Path) -> None:
        """Обработать одиночный файл.

        В отличие от пакетного режима отказ здесь пробрасывается наверх: файла
        всего один, показать ошибку нужно непосредственно пользователю, а не
        прятать её в отчёте.
        """
        if not self.validator.is_valid_excel(file_path):
            failure = FileFailure(
                file_name=file_path.name,
                user_message=NotSpreadsheetError.default_user_message,
                hint="Укажите файл .xlsx — выгрузку регистра из 1С.",
            )
            self._record_failure(file_path.name, failure)
            raise NotSpreadsheetError(failure.user_message, hint=failure.hint)

        processor = self.processor_factory.get_processor(file_path, self.type_register)
        result, check = processor.process_file(file_path)
        self.storage_processed_registers[file_path.name] = result
        self.check[file_path.name] = check
    
    
    
    def _process_directory(self, dir_path: Path, progress_callback) -> None:
        excel_files = self._get_excel_files(dir_path)
        result_collector = ResultCollector()
        result_collector.type_register = self.type_register

        # Проверка условия параллельной обработки
        if self.parallel and self.type_register != 'turnover':
            self._process_directory_parallel(dir_path, progress_callback, result_collector)
        else:
            # Существующая последовательная обработка (с изменениями)
            total_files = len(excel_files)
            for i, file_path in enumerate(excel_files):
                processor_class, result, check = self._process_file_and_record(file_path)
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
                executor.submit(_process_file_parallel, file_path, self.type_register): file_path
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
    
    def _process_single_file_consistently(self, file_path: Path, type_register: str) -> Tuple[FileProcessor, pd.DataFrame, pd.DataFrame]:
        """Обработать файл последовательно (переименовано для совместимости)

        Оставлен для внешних вызовов. Внутри пайплайна обработка папки идёт
        через :meth:`_process_file_and_record`, которая к тому же пишет отказ
        в отчёт и в журнал.
        """
        if not ExcelValidator.is_valid_excel(file_path):
            return None, None, None

        processor = FileProcessorFactory.get_processor(file_path, type_register)
        result, check = processor.process_file(file_path)

        return processor, result, check

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

    @staticmethod
    def _open_file(file_path: str) -> None:
        """Открыть файл в ассоциированном приложении.

        Файл уже записан к этому моменту, поэтому неудача открытия — не
        провал обработки: пользователю показываем предупреждение, а результат
        всё равно лежит на диске.
        """
        try:
            if sys.platform == "win32":
                os.startfile(file_path)
            elif sys.platform == "darwin":
                subprocess.run(["open", file_path], check=False)
            else:
                subprocess.run(["xdg-open", file_path], check=False)
        except OSError as error:
            _log.warning("Итоговый файл создан, но не открылся автоматически: %s (%s)",
                         file_path, error)

    def _save_and_open_batch_result(self, progress_callback) -> None:
        """Сохранить и открыть результаты пакетной обработки"""
        if not self.storage_processed_registers and not self.check:
            return

        progress_callback(stage_description='Сохраняем и открываем итоговый файл')

        def write_sheets(writer) -> None:
            # Основные данные
            for sheet_name, df in self.storage_processed_registers.items():
                safe_name = sheet_name[:31]
                df = df.map(lambda x: x.rstrip() if isinstance(x, str) else x)
                df.to_excel(writer, sheet_name=safe_name, index=False)

            # Данные проверки
            for sheet_name, df in self.check.items():
                safe_name = f"Проверка_{sheet_name}"[:31]
                if not df.empty:
                    df.to_excel(writer, sheet_name=safe_name, index=False)

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name

        self._write_workbook(temp_filename, write_sheets)
        self._open_file(temp_filename)





