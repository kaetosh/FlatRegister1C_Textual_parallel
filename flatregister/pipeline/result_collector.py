"""Сборка результатов обработки файлов в итоговые кадры.

Разделение ``file_handler.py`` — Сессия S5 плана рефакторинга (см. Tasks.md).
В S8 сборщик получает тип регистра в конструкторе и берёт порядок колонок
из единого реестра, не падая на неизвестном ключе.
"""
from typing import Dict, List, Tuple, Type

import pandas as pd

from flatregister.column_order import sort_columns
from flatregister.registers.base import FileProcessor
from flatregister.registry import (
    KIND_UPP_NON_UPP,
    REGISTER_SPEC,
    VARIANT_UPP,
    column_order,
    get_register,
)


class ResultCollector:
    def __init__(self, register_type: str):
        self.register_type = register_type
        self.entry = get_register(register_type)
        self.upp_results: List[pd.DataFrame] = []
        self.non_upp_results: List[pd.DataFrame] = []
        self.analytical_results: Dict[Type[FileProcessor], Tuple[List[pd.DataFrame], List[pd.DataFrame]]] = {}

    def add_result(self, processor_class: Type[FileProcessor], result: pd.DataFrame, check: pd.DataFrame):
        if self.entry is None:
            self.non_upp_results.append(result)
            return

        if self.entry.kind == KIND_UPP_NON_UPP:
            if self.entry.variant_for(processor_class) == VARIANT_UPP:
                self.upp_results.append(result)
            else:
                self.non_upp_results.append(result)
            return

        bucket = self.analytical_results.setdefault(processor_class, ([], []))
        bucket[0].append(result)
        if not check.empty:
            bucket[1].append(check)

    def get_all_results(self) -> Dict[str, Tuple[pd.DataFrame, pd.DataFrame]]:
        empty = pd.DataFrame()
        results: Dict[str, Tuple[pd.DataFrame, pd.DataFrame]] = {
            key: (empty.copy(), empty.copy()) for key in REGISTER_SPEC
        }
        results['upp'] = (
            self._concat_and_sort(self.upp_results, 'upp') if self.upp_results else empty.copy(),
            empty.copy(),
        )
        results['non_upp'] = (
            self._concat_and_sort(self.non_upp_results, 'not_upp') if self.non_upp_results else empty.copy(),
            empty.copy(),
        )

        if self.entry is not None and self.entry.kind != KIND_UPP_NON_UPP:
            results[self.entry.key] = self._process_analisys_like(self.analytical_results)

        return results

    def _process_analisys_like(self, results_dict: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Обработка группы результатов с возможным вызовом shiftable_level к финальному df"""
        all_dfs = []
        all_check_dfs = []
        last_shiftable_cls = None  # для финального вызова

        for cls, (res_list, check_list) in results_dict.items():
            if res_list:
                all_dfs.append(pd.concat(res_list))
            if check_list:
                all_check_dfs.append(pd.concat(check_list))
            last_shiftable_cls = cls  # запоминаем последний класс с shiftable_level

        df = pd.concat(all_dfs) if all_dfs else pd.DataFrame()
        check_df = pd.concat(all_check_dfs) if all_check_dfs else pd.DataFrame()

        # Применяем shiftable_level к финальному df, используя последний класс с shiftable_level
        if not df.empty and last_shiftable_cls is not None:
            df = last_shiftable_cls().shiftable_level(df)

        # Сортировка: вариант 'upp' выбран осознанно — для аналитических
        # регистров списки upp/not_upp пока совпадают (см. Tasks.md).
        if not df.empty and self.entry is not None:
            df = sort_columns(df, column_order(self.register_type, 'upp'))

        return df, check_df

    def _concat_and_sort(self, results: List[pd.DataFrame], processor_type: str) -> pd.DataFrame:
        if not results:
            return pd.DataFrame()
        combined = pd.concat(results)
        if self.entry is None:
            return combined
        return sort_columns(combined, column_order(self.register_type, processor_type))
