# -*- coding: utf-8 -*-
"""Пайплайн целиком: папка, параллельность, автоопределение регистра.

Эти проверки закрывают то, что юнит-тесты процессоров не видят: разница между
пакетным и одиночным режимом и между последовательной и параллельной
обработкой. Именно там раньше расходились числа — два пути обработки были
написаны отдельно и правились отдельно.
"""

from __future__ import annotations

import pytest

from file_handler import FileHandler, FileProcessorFactory
from flatregister.errors import FileFailure
from tests import factories


def _progress(*args, **kwargs):
    """Заглушка прогресс-колбэка."""


@pytest.fixture
def captured(monkeypatch):
    """Перехватывает итог пакетной обработки до записи в Excel.

    В папочном режиме результаты уходят в :class:`ResultCollector`, а не в
    ``storage_processed_registers`` (тот остаётся пустым — это поле одиночного
    режима). Подменяем сохранение, чтобы получить сам сборщик.
    """
    box = {}

    def fake_save(self, result_collector):
        box["collector"] = result_collector
        box["results"] = result_collector.get_all_results()

    monkeypatch.setattr(FileHandler, "_save_combined_results", fake_save)
    monkeypatch.setattr(FileHandler, "_open_file", staticmethod(lambda *a, **k: None))
    return box


def _frames(box, register_key):
    """Достаёт итоговые кадры по ключу регистра."""
    frame, check = box["results"][register_key]
    return frame, check


# --------------------------------------------------------------------------
# Автоопределение регистра
# --------------------------------------------------------------------------

DETECTION_CASES = [
    ("card_upp", factories.card_upp, "card", "Card_UPPFileProcessor"),
    ("card_non_upp", factories.card_non_upp, "card", "Card_NonUPPFileProcessor"),
    ("posting_upp", factories.posting_upp, "posting", "Posting_UPPFileProcessor"),
    ("posting_non_upp", factories.posting_non_upp, "posting", "Posting_NonUPPFileProcessor"),
    ("analisys_upp", factories.analysis_upp, "analisys", "Analisys_UPPFileProcessor"),
    ("analisys_non_upp", factories.analysis_non_upp, "analisys", "Analisys_NonUPPFileProcessor"),
    ("turnover_upp", factories.turnover_upp, "turnover", "Turnover_UPPFileProcessor"),
    ("turnover_non_upp", factories.turnover_non_upp, "turnover", "Turnover_NonUPPFileProcessor"),
    ("accountosv_upp", factories.accountosv_upp, "accountosv", "AccountOSV_UPPFileProcessor"),
    ("accountosv_non_upp", factories.accountosv_non_upp, "accountosv", "AccountOSV_NonUPPFileProcessor"),
    ("generalosv_upp", factories.generalosv_upp, "generalosv", "GeneralOSV_UPPFileProcessor"),
    ("generalosv_non_upp", factories.generalosv_non_upp, "generalosv", "GeneralOSV_NonUPPFileProcessor"),
]


@pytest.mark.parametrize("builder_name,builder,register,expected_class", DETECTION_CASES)
def test_регистр_определяется_по_заголовкам(builder_name, builder, register, expected_class, out_dir):
    """Каждый из 12 вариантов выгрузки опознаётся своим классом обработчика.

    Раньше это было единственное место в проекте, где регистр регистрировался
    для интерфейса; ошибка здесь означала бы «файл не является регистром»
    на корректной выгрузке.
    """
    path = builder(out_dir / f"{builder_name}.xlsx")

    processor = FileProcessorFactory.get_processor(path, register)

    assert processor.__class__.__name__ == expected_class


def test_чужой_файл_отвергается_по_выбранному_регистру(out_dir):
    """Файл другого регистра не должен молча обрабатываться чужим."""
    path = factories.generalosv_upp(out_dir / "осв.xlsx")

    with pytest.raises(Exception) as caught:
        FileProcessorFactory.get_processor(path, "card")

    assert "регистр" in caught.value.user_message.lower()


def test_все_регистры_имеют_описание_в_текстах():
    """У каждого зарегистрированного регистра есть текст с требованиями.

    Проверка закрывает одну из четырёх точек, где раньше надо было что-то
    не забыть при добавлении регистра.
    """
    from text import REQUIREMENTS

    for register in FileProcessorFactory.REGISTER_PATTERNS:
        assert register in REQUIREMENTS, f"нет текста для регистра {register}"
        assert REQUIREMENTS[register].strip(), f"пустой текст для регистра {register}"


# --------------------------------------------------------------------------
# Пакетная обработка
# --------------------------------------------------------------------------

def test_папка_из_двух_файлов_склеивается(out_dir, captured):
    """Два файла одного регистра дают один результат с обоими именами."""
    factories.accountosv_upp(out_dir / "ООО Ромашка.xlsx")
    factories.accountosv_upp(out_dir / "ООО Вектор.xlsx")

    handler = FileHandler()
    handler.handle_input(out_dir, "accountosv", _progress)

    assert not handler.not_correct_files, handler.not_correct_files
    frame, check = _frames(captured, "accountosv")
    assert not frame.empty
    assert set(frame["Исх.файл"]) == {"ООО Ромашка.xlsx", "ООО Вектор.xlsx"}
    # Каждый файл даёт по два контрагента
    assert len(frame) == 4, frame.to_string()
    assert not check.empty


def test_плохой_файл_не_останавливает_папку(out_dir, captured):
    """Один нечитаемый файл не должен ронять обработку всей папки."""
    factories.accountosv_upp(out_dir / "хороший.xlsx")
    (out_dir / "мусор.xlsx").write_bytes(b"not a zip archive")

    handler = FileHandler()
    handler.handle_input(out_dir, "accountosv", _progress)

    assert set(handler.not_correct_files) == {"мусор.xlsx"}
    frame, _ = _frames(captured, "accountosv")
    assert set(frame["Исх.файл"]) == {"хороший.xlsx"}

    failure = handler.not_correct_files["мусор.xlsx"]
    assert isinstance(failure, FileFailure)
    assert failure.user_message
    assert "Traceback" not in failure.user_message


def test_параллельный_воркер_даёт_тот_же_результат_что_и_последовательный(out_dir):
    """Ключевая проверка: два пути обработки обязаны давать одно и то же.

    Раньше параллельный режим был отдельной копией последовательного, и
    расхождение между ними можно было заметить только на реальных выгрузках.

    Воркер вызывается напрямую, а не через ``ProcessPoolExecutor``. Сам запуск
    процессов — не наш код и не наша логика, а вот что он делает — да.
    Реальный запуск в пуле проверяется отдельным тестом на передачу в процесс.
    """
    import pandas as pd

    from file_handler import _process_file_parallel

    path = factories.accountosv_upp(out_dir / "ООО Ромашка.xlsx")

    _name, processor_class, parallel_result, parallel_check, failure = \
        _process_file_parallel(path, "accountosv")
    assert failure is None, failure

    sequential_result, sequential_check = processor_class().process_file(path)

    pd.testing.assert_frame_equal(parallel_result.sort_index(axis=1),
                                  sequential_result.sort_index(axis=1),
                                  check_like=True)
    assert not parallel_check.empty
    assert parallel_check["Оборот_разница"].tolist() == \
        sequential_check["Оборот_разница"].tolist()


def test_отказ_в_параллельном_воркере_возвращается_файлом_а_не_строкой(out_dir):
    """Воркер не бросает исключения: их не пережить границу процессов."""
    from file_handler import _process_file_parallel

    broken = out_dir / "мусор.xlsx"
    broken.write_bytes(b"not a zip archive")

    name, processor_class, result, check, failure = \
        _process_file_parallel(broken, "accountosv")

    assert name == "мусор.xlsx"
    assert processor_class is None and result is None and check is None
    assert isinstance(failure, FileFailure)
    assert failure.user_message
    assert failure.technical_detail, "в воркере теряется трейсбек для журнала"


def test_воркер_пригоден_для_передачи_в_дочерний_процесс():
    """Функция воркера обязана быть верхнего уровня и сериализуемой.

    На Windows ``ProcessPoolExecutor`` запускает процессы через spawn: функция
    передаётся по pickle. Вложенная функция или лямбда здесь означали бы
    «параллельная обработка не работает», и заметить это можно было бы только
    на реальной папке.
    """
    import pickle

    from file_handler import _process_file_parallel

    assert pickle.loads(pickle.dumps(_process_file_parallel)) is _process_file_parallel


def test_последовательный_и_параллельный_режимы_совпадают(out_dir, monkeypatch):
    """Тот же воркер, но прогнанный через пул процессов.

    Проверяется ровно то, чего не покрывает вызов функции напрямую: пул
    действительно собирает результаты так же, как последовательный цикл.
    """
    import pandas as pd

    factories.accountosv_upp(out_dir / "ООО Ромашка.xlsx")
    factories.accountosv_upp(out_dir / "ООО Вектор.xlsx")
    factories.accountosv_upp(out_dir / "ООО Магнит.xlsx")

    def run(parallel):
        box = {}
        monkeypatch.setattr(
            FileHandler, "_save_combined_results",
            lambda self, collector: box.setdefault("results", collector.get_all_results()),
        )
        monkeypatch.setattr(FileHandler, "_open_file", staticmethod(lambda *a, **k: None))

        handler = FileHandler(parallel=parallel)
        handler.handle_input(out_dir, "accountosv", _progress)
        assert not handler.not_correct_files, handler.not_correct_files
        return box["results"]

    sequential = run(parallel=False)
    parallel = run(parallel=True)

    for key in sequential:
        left, _ = sequential[key]
        right, _ = parallel[key]
        if left.empty and right.empty:
            continue
        pd.testing.assert_frame_equal(left.sort_index(axis=1),
                                      right.sort_index(axis=1), check_like=True)


def test_пустая_папка_даёт_понятную_ошибку(tmp_path):
    """В папке нет xlsx — пользователь должен получить слова, а не стек."""
    empty = tmp_path / "пусто"
    empty.mkdir()

    handler = FileHandler()
    with pytest.raises(Exception) as caught:
        handler.handle_input(empty, "card", _progress)

    assert ".xlsx" in caught.value.user_message


def test_одиночный_файл_кладётся_в_результат(out_dir, monkeypatch):
    """Одиночный режим идёт другим кодом — проверяем его отдельно."""
    path = factories.card_upp(out_dir / "Карточка.xlsx")

    monkeypatch.setattr(FileHandler, "_open_file", staticmethod(lambda *a, **k: None))

    handler = FileHandler()
    handler.handle_input(path, "card", _progress)

    assert not handler.not_correct_files
    assert set(handler.storage_processed_registers) == {"Карточка.xlsx"}


def test_отказ_в_одиночном_режиме_пробрасывается(out_dir):
    """В одиночном режиме отказ должен дойти до пользователя, а не в отчёт."""
    path = factories.generalosv_upp(out_dir / "осв.xlsx")

    handler = FileHandler()
    with pytest.raises(Exception) as caught:
        handler.handle_input(path, "card", _progress)

    assert "регистр" in caught.value.user_message.lower()