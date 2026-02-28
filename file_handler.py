# import concurrent.futures
# import subprocess
# import sys
# import os
# import pandas as pd
# import tempfile

# from pathlib import Path
# from typing import List, Literal, Dict, Tuple

# # Импорт процессоров
# from register_processors.class_processor import FileProcessor, DESIRED_ORDER
# from register_processors.card_processor import Card_UPPFileProcessor, Card_NonUPPFileProcessor
# from register_processors.posting_processor import Posting_UPPFileProcessor, Posting_NonUPPFileProcessor
# from register_processors.analisys_processor import Analisys_UPPFileProcessor, Analisys_NonUPPFileProcessor
# from register_processors.turnover_processor import Turnover_UPPFileProcessor, Turnover_NonUPPFileProcessor
# from register_processors.accountosv_processor import AccountOSV_UPPFileProcessor, AccountOSV_NonUPPFileProcessor
# from register_processors.generalosv_processor import GeneralOSV_UPPFileProcessor, GeneralOSV_NonUPPFileProcessor

# from support_functions import fix_1c_excel_case, sort_columns, write_df_in_chunks
# from custom_errors import RegisterProcessingError, NoRegisterFilesFoundError, NoExcelFilesFoundError, NotXLSXFileError, ProcessingError


# def _process_file_parallel(file_path: Path, type_register: str) -> tuple:
#     """Функция для выполнения в отдельном процессе"""
#     try:
#         if not ExcelValidator.is_valid_excel(file_path):
#             return file_path.name, None, None, None, "не является .xlsx"
#         processor = FileProcessorFactory.get_processor(file_path, type_register)
#         result, check = processor.process_file(file_path)
#         return file_path.name, processor.__class__, result, check, None
#     except Exception as e:
#         return file_path.name, None, None, None, str(e)

# class ExcelValidator:
#     """Валидатор Excel файлов"""
#     @staticmethod
#     def is_valid_excel(file_path: Path) -> bool:
#         return file_path.suffix.lower() == '.xlsx'


# class FileProcessorFactory:
#     """Фабрика для создания обработчиков файлов"""
    
#     REGISTER_DISPLAY_NAMES = {
#         'posting': 'Отчет по проводкам',
#         'card': 'Карточка счета', 
#         'analisys': 'Анализ счета',
#         'turnover': 'Обороты счета',
#         'accountosv': 'ОСВ счета',
#         'generalosv': 'общая ОСВ'
#     }
    
#     REGISTER_PATTERNS = {
#         'card': [
#             {'pattern': {'дата', 'документ', 'операция'}, 'processor': Card_UPPFileProcessor},
#             {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Card_NonUPPFileProcessor}
#         ],
#         'posting': [
#             {'pattern': {'дата', 'документ', 'содержание', 'дт', 'кт', 'сумма'}, 'processor': Posting_UPPFileProcessor},
#             {'pattern': {'период', 'аналитика дт', 'аналитика кт'}, 'processor': Posting_NonUPPFileProcessor}
#         ],
#         'analisys': [
#             {'pattern': {'счет', 'кор.счет', 'с кред. счетов', 'в дебет счетов'}, 'processor': Analisys_UPPFileProcessor},
#             {'pattern': {'счет', 'кор. счет', 'дебет', 'кредит'}, 'processor': Analisys_NonUPPFileProcessor}
#         ],
#         'turnover': [
#             {'pattern': {'субконто', 'нач. сальдо деб.', 'нач. сальдо кред.', 'деб. оборот', 'кред. оборот', 'кон. сальдо деб.', 'кон. сальдо кред.'}, 'processor': Turnover_UPPFileProcessor},
#             {'pattern': {'счет', 'начальное сальдо дт', 'начальное сальдо кт', 'оборот дт', 'оборот кт', 'конечное сальдо дт', 'конечное сальдо кт'}, 'processor': Turnover_NonUPPFileProcessor}
#         ],
#         'accountosv': [
#             {'pattern': {'субконто', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': AccountOSV_UPPFileProcessor},
#             {'pattern': {'счет', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': AccountOSV_NonUPPFileProcessor}
#         ],
#         'generalosv': [
#             {'pattern': {'счет', 'сальдо на начало периода', 'оборот за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_UPPFileProcessor},
#             {'pattern': {'счет', 'наименование счета', 'сальдо на начало периода', 'обороты за период', 'сальдо на конец периода'}, 'processor': GeneralOSV_NonUPPFileProcessor}
#         ]
#     }

#     @classmethod
#     def get_processor(cls, file_path: Path, type_register: str) -> FileProcessor:
#         """Получить процессор для файла на основе шаблонов заголовков"""
#         fixed_data = fix_1c_excel_case(file_path)
        
#         # Чтение только первых строк для анализа
#         df = pd.read_excel(fixed_data, header=None, nrows=20)
#         str_df = df.map(lambda x: str(x).strip().lower() if pd.notna(x) else '')
        
#         for pattern_config in cls.REGISTER_PATTERNS[type_register]:
#             if cls._contains_pattern(str_df, pattern_config['pattern']):
#                 return pattern_config['processor']()
        
#         raise RegisterProcessingError("Файл не является выбранным регистром из 1С.")

#     @staticmethod
#     def _contains_pattern(df: pd.DataFrame, pattern: set) -> bool:
#         """Проверить, содержит ли DataFrame заданный паттерн"""
#         for _, row in df.iterrows():
#             if pattern.issubset(set(row)):
#                 return True
#         return False


# class ResultCollector:
#     def __init__(self):
#         self.upp_results = []
#         self.non_upp_results = []
#         self.analisys_results = {}      # key: class -> (results_list, checks_list)
#         self.turnover_results = {}       # аналогично
#         self.accountosv_results = {}     # аналогично
#         self.generalosv_results = {}     # аналогично
#         self.type_register = None

#     def add_result(self, processor_class, result: pd.DataFrame, check: pd.DataFrame):
#         # UPP
#         if processor_class in (Card_UPPFileProcessor, Posting_UPPFileProcessor):
#             self.upp_results.append(result)
#         # NonUPP (кроме аналитических)
#         elif processor_class in (Card_NonUPPFileProcessor, Posting_NonUPPFileProcessor):
#             self.non_upp_results.append(result)
#         # Analisys
#         elif processor_class in (Analisys_UPPFileProcessor, Analisys_NonUPPFileProcessor):
#             self.analisys_results.setdefault(processor_class, ([], []))[0].append(result)
#             if not check.empty:
#                 self.analisys_results[processor_class][1].append(check)
#         # Turnover
#         elif processor_class in (Turnover_UPPFileProcessor, Turnover_NonUPPFileProcessor):
#             self.turnover_results.setdefault(processor_class, ([], []))[0].append(result)
#             if not check.empty:
#                 self.turnover_results[processor_class][1].append(check)
#         # AccountOSV
#         elif processor_class in (AccountOSV_UPPFileProcessor, AccountOSV_NonUPPFileProcessor):
#             self.accountosv_results.setdefault(processor_class, ([], []))[0].append(result)
#             if not check.empty:
#                 self.accountosv_results[processor_class][1].append(check)
#         # GeneralOSV
#         elif processor_class in (GeneralOSV_UPPFileProcessor, GeneralOSV_NonUPPFileProcessor):
#             self.generalosv_results.setdefault(processor_class, ([], []))[0].append(result)
#             if not check.empty:
#                 self.generalosv_results[processor_class][1].append(check)
#         else:
#             self.non_upp_results.append(result)

#     def get_all_results(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
#         # Обработка UPP
#         upp_df = self._concat_and_sort(self.upp_results, 'upp') if self.upp_results else pd.DataFrame()
        
#         # Обработка NonUPP (простые)
#         non_upp_df = self._concat_and_sort(self.non_upp_results, 'not_upp') if self.non_upp_results else pd.DataFrame()
        
#         # Обработка Analisys
#         analisys_df, analisys_check_df = self._process_analisys_like(self.analisys_results)
        
#         turnover_df, turnover_check_df = self._process_analisys_like(self.turnover_results)
        
#         accountosv_df, accountosv_check_df = self._process_analisys_like(self.accountosv_results)
        
#         generalosv_df, generalosv_check_df = self._process_analisys_like(self.generalosv_results)
#         # Аналогично для turnover, accountosv, generalosv...
        
#         return {
#             'upp': (upp_df, pd.DataFrame()),
#             'non_upp': (non_upp_df, pd.DataFrame()),
#             'analisys': (analisys_df, analisys_check_df),
#             'turnover': (self._process_analisys_like(self.turnover_results)),
#             'accountosv': (self._process_analisys_like(self.accountosv_results)),
#             'generalosv': (self._process_analisys_like(self.generalosv_results))
#         }

#     def _process_analisys_like(self, results_dict: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
#         """Обработка группы результатов с возможным вызовом shiftable_level по классам"""
#         all_dfs = []
#         all_check_dfs = []
#         for cls, (res_list, check_list) in results_dict.items():
#             if res_list:
#                 combined = pd.concat(res_list)
#                 # Применяем shiftable_level, если метод существует
#                 if hasattr(cls, 'shiftable_level'):
#                     instance = cls()  # предполагается конструктор без аргументов
#                     combined = instance.shiftable_level(combined)
#                 all_dfs.append(combined)
#             if check_list:
#                 all_check_dfs.append(pd.concat(check_list))
#         df = pd.concat(all_dfs) if all_dfs else pd.DataFrame()
#         check_df = pd.concat(all_check_dfs) if all_check_dfs else pd.DataFrame()
#         # Сортировка (если нужна)
#         if not df.empty and hasattr(self, 'type_register'):
#             df = sort_columns(df, DESIRED_ORDER[self.type_register].get('upp', []))
#         return df, check_df

#     def _concat_and_sort(self, results: List[pd.DataFrame], processor_type: str) -> pd.DataFrame:
#         if not results:
#             return pd.DataFrame()
#         combined = pd.concat(results)
#         if hasattr(self, 'type_register'):
#             order = DESIRED_ORDER[self.type_register].get(processor_type, [])
#             return sort_columns(combined, order)
#         return combined


# class FileHandler:
#     """Обработчик файлов с последовательной обработкой"""
    
#     def __init__(self, parallel: bool = False):
#         self.parallel = parallel
#         self.validator = ExcelValidator()
#         self.processor_factory = FileProcessorFactory()
        
#         self.not_correct_files = {} # имя файла: причина не обработки
#         self.storage_processed_registers = {}
#         self.check = {}
#         self.type_register = None

#     def handle_input(self, input_path: Path, 
#                     type_register: Literal['posting', 'card', 'analisys', 'account', 'generalosv'],
#                     progress_callback) -> None:
#         self.type_register = type_register
#         if input_path.is_file():
#             self._process_single_file(input_path)
#         elif input_path.is_dir():
#             self._process_directory(input_path, progress_callback)
            
#     def _process_single_file(self, file_path: Path) -> None:
#         """Обработать одиночный файл"""
#         if not self.validator.is_valid_excel(file_path):
#             self.not_correct_files[file_path.name] = 'не является .xlsx'
#             raise NotXLSXFileError(f"Файл {file_path.name} не является .xlsx")
        
#         try:
#             processor = self.processor_factory.get_processor(file_path, self.type_register)
#             result, check = processor.process_file(file_path)
            
#             self.storage_processed_registers[file_path.name] = result
#             self.check[file_path.name] = check
        
#         except Exception as e:
#             self.not_correct_files[file_path.name] = str(e)
#             raise ProcessingError(e)
    
    
    
#     def _process_directory(self, dir_path: Path, progress_callback) -> None:
#         try:
#             excel_files = self._get_excel_files(dir_path)
#             result_collector = ResultCollector()
#             result_collector.type_register = self.type_register
    
#             # Проверка условия параллельной обработки
#             if self.parallel and self.type_register != 'turnover':
#                 self._process_directory_parallel(dir_path, progress_callback, result_collector)
#             else:
#                 # Существующая последовательная обработка (с изменениями)
#                 total_files = len(excel_files)
#                 # progress_callback(0, total_files)
#                 for i, file_path in enumerate(excel_files):
#                     try:
#                         processor, result, check = self._process_single_file_consistently(file_path, self.type_register)
#                         if result is not None:
#                             result_collector.add_result(processor.__class__, result, check)
#                         else:
#                             self.not_correct_files[file_path.name] = "Не удалось обработать файл"
#                     except Exception as e:
#                         self.not_correct_files[file_path.name] = str(e)
#                     finally:
#                         progress_callback(
#                             i + 1,
#                             total_files,
#                             stage_description=f"Обработано файлов {i + 1}/{total_files}"
#                         )
#                 progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
#                 self._save_combined_results(result_collector)
#         except Exception as e:
#             raise e
    
#     def _process_directory_parallel(self, dir_path: Path, progress_callback, result_collector: ResultCollector):
#         excel_files = self._get_excel_files(dir_path)
#         total_files = len(excel_files)

#         # Определяем количество воркеров (например, не больше числа CPU*5 для IO-bound задач)
#         max_workers = min((os.cpu_count() or 4) * 5, total_files, 32)  # Разумный верхний предел

#         # Используем ThreadPoolExecutor вместо ProcessPoolExecutor
#         with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
#             future_to_file = {
#                 executor.submit(_process_file_parallel, file_path, self.type_register): file_path
#                 for file_path in excel_files
#             }
            
#             for i, future in enumerate(concurrent.futures.as_completed(future_to_file)):
#                 file_path = future_to_file[future]
#                 try:
#                     file_name, processor_class, result, check, error = future.result()
#                     if error:
#                         self.not_correct_files[file_name] = error
#                     else:
#                         result_collector.add_result(processor_class, result, check)
#                 except Exception as e:
#                     self.not_correct_files[file_path.name] = str(e)
#                 finally:
#                     progress_callback(
#                         i + 1,
#                         total_files,
#                         stage_description=f"Обработано файлов {i + 1}/{total_files}"
#                     )
        
#         progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
#         self._save_combined_results(result_collector)
    
#     def _process_single_file_consistently(self, file_path: Path, type_register: str) -> Tuple[FileProcessor, pd.DataFrame, pd.DataFrame]:
#         """Обработать файл последовательно (переименовано для совместимости)"""
#         if not ExcelValidator.is_valid_excel(file_path):
#             return None, None, None
        
#         try:
#             processor = FileProcessorFactory.get_processor(file_path, type_register)
#             result, check = processor.process_file(file_path)
#             return processor, result, check
#         except Exception as e:
#             raise e

#     @staticmethod
#     def _get_excel_files(dir_path: Path) -> List[Path]:
#         """Получить список Excel файлов в директории"""
#         files = list(dir_path.glob("*.xlsx"))
#         if not files:
#             raise NoExcelFilesFoundError("В папке нет файлов Excel.")
#         return files

#     def _save_combined_results(self, result_collector: ResultCollector) -> None:
#         """Сохранить объединенные результаты"""
#         results = result_collector.get_all_results()
        
#         # Проверка наличия результатов
#         if not any(not df.empty for df, _ in results.values()):
#             raise NoRegisterFilesFoundError('В папке не найдены регистры 1С.')
        
#         with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
#             temp_filename = tmp.name
        
#         with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
#             sheet_mapping = {
#                 'upp': 'UPP',
#                 'non_upp': 'Non_UPP', 
#                 'analisys': ('analisys', 'analisys_check'),
#                 'turnover': ('turnover', 'turnover_check'),
#                 'accountosv': ('accountosv', 'accountosv_check'),
#                 'generalosv': ('generalosv', 'generalosv_check')
#             }
            
#             for key, sheets in sheet_mapping.items():
#                 df, df_check = results[key]
#                 if not df.empty:
#                     write_df_in_chunks(writer, df, sheets[0] if isinstance(sheets, tuple) else sheets)
#                 if isinstance(sheets, tuple) and not df_check.empty:
#                     write_df_in_chunks(writer, df_check, sheets[1])
        
#         self._open_file(temp_filename)
#         # print('Обработка завершена.' + ' ' * 20)

#     @staticmethod
#     def _open_file(file_path: str) -> None:
#         """Открыть файл в ассоциированном приложении"""
#         if sys.platform == "win32":
#             os.startfile(file_path)
#         elif sys.platform == "darwin":
#             subprocess.run(["open", file_path], check=False)
#         else:
#             subprocess.run(["xdg-open", file_path], check=False)

#     def _save_and_open_batch_result(self, progress_callback) -> None:
#         """Сохранить и открыть результаты пакетной обработки"""
#         if not self.storage_processed_registers and not self.check:
#             return
        
#         progress_callback(stage_description='Сохраняем и открываем итоговый файл')
        
#         with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
#             temp_filename = tmp.name
        
#         with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
#             # Основные данные
#             for sheet_name, df in self.storage_processed_registers.items():
#                 safe_name = sheet_name[:31]
#                 df.to_excel(writer, sheet_name=safe_name, index=False)
            
#             # Данные проверки
#             for sheet_name, df in self.check.items():
#                 safe_name = f"Проверка_{sheet_name}"[:31]
#                 if not df.empty:
#                     df.to_excel(writer, sheet_name=safe_name, index=False)
        
#         self._open_file(temp_filename)
        




import concurrent.futures
import subprocess
import sys
import os
import pandas as pd
import tempfile

from pathlib import Path
from typing import List, Literal, Dict, Tuple

# Импорт процессоров
from register_processors.class_processor import FileProcessor, DESIRED_ORDER
from register_processors.card_processor import Card_UPPFileProcessor, Card_NonUPPFileProcessor
from register_processors.posting_processor import Posting_UPPFileProcessor, Posting_NonUPPFileProcessor
from register_processors.analisys_processor import Analisys_UPPFileProcessor, Analisys_NonUPPFileProcessor
from register_processors.turnover_processor import Turnover_UPPFileProcessor, Turnover_NonUPPFileProcessor
from register_processors.accountosv_processor import AccountOSV_UPPFileProcessor, AccountOSV_NonUPPFileProcessor
from register_processors.generalosv_processor import GeneralOSV_UPPFileProcessor, GeneralOSV_NonUPPFileProcessor

from support_functions import fix_1c_excel_case, sort_columns, write_df_in_chunks
from custom_errors import RegisterProcessingError, NoRegisterFilesFoundError, NoExcelFilesFoundError, NotXLSXFileError, ProcessingError


def _process_file_parallel(file_path: Path, type_register: str) -> tuple:
    """Функция для выполнения в отдельном процессе"""
    try:
        if not ExcelValidator.is_valid_excel(file_path):
            return file_path.name, None, None, None, "не является .xlsx"
        processor = FileProcessorFactory.get_processor(file_path, type_register)
        result, check = processor.process_file(file_path)
        return file_path.name, processor.__class__, result, check, None
    except Exception as e:
        return file_path.name, None, None, None, str(e)

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
        
        raise RegisterProcessingError("Файл не является выбранным регистром из 1С.")

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
        
        # Обработка NonUPP (простые)
        non_upp_df = self._concat_and_sort(self.non_upp_results, 'not_upp') if self.non_upp_results else pd.DataFrame()
        
        # Обработка Analisys
        analisys_df, analisys_check_df = self._process_analisys_like(self.analisys_results)
        
        turnover_df, turnover_check_df = self._process_analisys_like(self.turnover_results)
        
        accountosv_df, accountosv_check_df = self._process_analisys_like(self.accountosv_results)
        
        generalosv_df, generalosv_check_df = self._process_analisys_like(self.generalosv_results)
        # Аналогично для turnover, accountosv, generalosv...
        
        return {
            'upp': (upp_df, pd.DataFrame()),
            'non_upp': (non_upp_df, pd.DataFrame()),
            'analisys': (analisys_df, analisys_check_df),
            'turnover': (self._process_analisys_like(self.turnover_results)),
            'accountosv': (self._process_analisys_like(self.accountosv_results)),
            'generalosv': (self._process_analisys_like(self.generalosv_results))
        }

    def _process_analisys_like(self, results_dict: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Обработка группы результатов с возможным вызовом shiftable_level по классам"""
        all_dfs = []
        all_check_dfs = []
        for cls, (res_list, check_list) in results_dict.items():
            if res_list:
                combined = pd.concat(res_list)
                # Применяем shiftable_level, если метод существует
                if hasattr(cls, 'shiftable_level'):
                    instance = cls()  # предполагается конструктор без аргументов
                    combined = instance.shiftable_level(combined)
                all_dfs.append(combined)
            if check_list:
                all_check_dfs.append(pd.concat(check_list))
        df = pd.concat(all_dfs) if all_dfs else pd.DataFrame()
        check_df = pd.concat(all_check_dfs) if all_check_dfs else pd.DataFrame()
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
        
        self.not_correct_files = {} # имя файла: причина не обработки
        self.storage_processed_registers = {}
        self.check = {}
        self.type_register = None

    def handle_input(self, input_path: Path, 
                    type_register: Literal['posting', 'card', 'analisys', 'account', 'generalosv'],
                    progress_callback) -> None:
        self.type_register = type_register
        if input_path.is_file():
            self._process_single_file(input_path)
        elif input_path.is_dir():
            self._process_directory(input_path, progress_callback)
            
    def _process_single_file(self, file_path: Path) -> None:
        """Обработать одиночный файл"""
        if not self.validator.is_valid_excel(file_path):
            self.not_correct_files[file_path.name] = 'не является .xlsx'
            raise NotXLSXFileError(f"Файл {file_path.name} не является .xlsx")
        
        try:
            processor = self.processor_factory.get_processor(file_path, self.type_register)
            result, check = processor.process_file(file_path)
            
            self.storage_processed_registers[file_path.name] = result
            self.check[file_path.name] = check
        
        except Exception as e:
            self.not_correct_files[file_path.name] = str(e)
            raise ProcessingError(e)
    
    
    
    def _process_directory(self, dir_path: Path, progress_callback) -> None:
        try:
            excel_files = self._get_excel_files(dir_path)
            result_collector = ResultCollector()
            result_collector.type_register = self.type_register
    
            # Проверка условия параллельной обработки
            if self.parallel and self.type_register != 'turnover':
                self._process_directory_parallel(dir_path, progress_callback, result_collector)
            else:
                # Существующая последовательная обработка (с изменениями)
                total_files = len(excel_files)
                # progress_callback(0, total_files)
                for i, file_path in enumerate(excel_files):
                    try:
                        processor, result, check = self._process_single_file_consistently(file_path, self.type_register)
                        if result is not None:
                            result_collector.add_result(processor.__class__, result, check)
                        else:
                            self.not_correct_files[file_path.name] = "Не удалось обработать файл"
                    except Exception as e:
                        self.not_correct_files[file_path.name] = str(e)
                    finally:
                        progress_callback(
                            i + 1,
                            total_files,
                            stage_description=f"Обработано файлов {i + 1}/{total_files}"
                        )
                progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
                self._save_combined_results(result_collector)
        except Exception as e:
            raise e
    
    def _process_directory_parallel(self, dir_path: Path, progress_callback, result_collector: ResultCollector):
        excel_files = self._get_excel_files(dir_path)
        total_files = len(excel_files)
        # progress_callback(0, total_files)

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
                    file_name, processor_class, result, check, error = future.result()
                    if error:
                        self.not_correct_files[file_name] = error
                    else:
                        result_collector.add_result(processor_class, result, check)
                except Exception as e:
                    self.not_correct_files[file_path.name] = str(e)
                finally:
                    progress_callback(
                        i + 1,
                        total_files,
                        stage_description=f"Обработано файлов {i + 1}/{total_files}"
                    )
        
        progress_callback(0, 0, stage_description='Сохраняем и открываем итоговый файл')
        self._save_combined_results(result_collector)
    
    def _process_single_file_consistently(self, file_path: Path, type_register: str) -> Tuple[FileProcessor, pd.DataFrame, pd.DataFrame]:
        """Обработать файл последовательно (переименовано для совместимости)"""
        if not ExcelValidator.is_valid_excel(file_path):
            return None, None, None
        
        try:
            processor = FileProcessorFactory.get_processor(file_path, type_register)
            result, check = processor.process_file(file_path)
            return processor, result, check
        except Exception as e:
            raise e

    @staticmethod
    def _get_excel_files(dir_path: Path) -> List[Path]:
        """Получить список Excel файлов в директории"""
        files = list(dir_path.glob("*.xlsx"))
        if not files:
            raise NoExcelFilesFoundError("В папке нет файлов Excel.")
        return files

    def _save_combined_results(self, result_collector: ResultCollector) -> None:
        """Сохранить объединенные результаты"""
        results = result_collector.get_all_results()
        
        # Проверка наличия результатов
        if not any(not df.empty for df, _ in results.values()):
            raise NoRegisterFilesFoundError('В папке не найдены регистры 1С.')
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            sheet_mapping = {
                'upp': 'UPP',
                'non_upp': 'Non_UPP', 
                'analisys': ('analisys', 'analisys_check'),
                'turnover': ('turnover', 'turnover_check'),
                'accountosv': ('accountosv', 'accountosv_check'),
                'generalosv': ('generalosv', 'generalosv_check')
            }
            
            for key, sheets in sheet_mapping.items():
                df, df_check = results[key]
                if not df.empty:
                    write_df_in_chunks(writer, df, sheets[0] if isinstance(sheets, tuple) else sheets)
                if isinstance(sheets, tuple) and not df_check.empty:
                    write_df_in_chunks(writer, df_check, sheets[1])
        
        self._open_file(temp_filename)
        # print('Обработка завершена.' + ' ' * 20)

    @staticmethod
    def _open_file(file_path: str) -> None:
        """Открыть файл в ассоциированном приложении"""
        if sys.platform == "win32":
            os.startfile(file_path)
        elif sys.platform == "darwin":
            subprocess.run(["open", file_path], check=False)
        else:
            subprocess.run(["xdg-open", file_path], check=False)

    def _save_and_open_batch_result(self, progress_callback) -> None:
        """Сохранить и открыть результаты пакетной обработки"""
        if not self.storage_processed_registers and not self.check:
            return
        
        progress_callback(stage_description='Сохраняем и открываем итоговый файл')
        
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
            temp_filename = tmp.name
        
        with pd.ExcelWriter(temp_filename, engine='openpyxl') as writer:
            # Основные данные
            for sheet_name, df in self.storage_processed_registers.items():
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)
            
            # Данные проверки
            for sheet_name, df in self.check.items():
                safe_name = f"Проверка_{sheet_name}"[:31]
                if not df.empty:
                    df.to_excel(writer, sheet_name=safe_name, index=False)
        
        self._open_file(temp_filename)





