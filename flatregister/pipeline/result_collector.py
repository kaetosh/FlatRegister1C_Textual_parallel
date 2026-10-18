"""Сборка результатов обработки файлов в итоговые кадры.

Разделение ``file_handler.py`` — Сессия S5 плана рефакторинга (см. Tasks.md).
Поведение не менялось.
"""
from typing import Dict, List, Tuple

import pandas as pd

from flatregister.column_order import DESIRED_ORDER, sort_columns
from flatregister.registers.account_osv import AccountOSV_NonUPPFileProcessor, AccountOSV_UPPFileProcessor
from flatregister.registers.analysis import Analisys_NonUPPFileProcessor, Analisys_UPPFileProcessor
from flatregister.registers.card import Card_NonUPPFileProcessor, Card_UPPFileProcessor
from flatregister.registers.general_osv import GeneralOSV_NonUPPFileProcessor, GeneralOSV_UPPFileProcessor
from flatregister.registers.posting import Posting_NonUPPFileProcessor, Posting_UPPFileProcessor
from flatregister.registers.turnover import Turnover_NonUPPFileProcessor, Turnover_UPPFileProcessor


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
