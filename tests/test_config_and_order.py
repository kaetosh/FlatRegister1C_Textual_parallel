# -*- coding: utf-8 -*-
"""Конфигурация и порядок колонок.

Конфигурация читается из файла ``config.json`` в текущем каталоге, поэтому
фикстура ``isolated_config`` уводит тест во временную папку. Здесь же
проверяется, что ``requirements.txt`` не разъехался с ``pyproject.toml``:
две независимые копии одного списка версий разъезжаются гарантированно.
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pytest

from flatregister.column_order import sort_columns
from flatregister.config import DEFAULT_CONFIG, clear_config_cache, read_config, update_config

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# --------------------------------------------------------------------------
# Конфигурация
# --------------------------------------------------------------------------

def test_конфиг_создаётся_со_значениями_по_умолчанию(isolated_config):
    isolated_config.unlink()
    clear_config_cache()

    config = read_config()

    assert isolated_config.exists()
    assert config["accounts_without_subaccount"] == \
        DEFAULT_CONFIG["accounts_without_subaccount"]


def test_битый_конфиг_не_теряет_молча(isolated_config, caplog):
    """Повреждённый файл заменяется значениями по умолчанию, но об этом известно.

    Раньше здесь стоял ``print``, которого в графическом приложении не видно,
    и пользователь обнаруживал потерю настроек задним числом.
    """
    isolated_config.write_text("{ это не json", encoding="utf-8")
    clear_config_cache()

    with caplog.at_level("WARNING", logger="flatregister.support"):
        config = read_config()

    assert config == DEFAULT_CONFIG
    assert any("повреждён" in record.message for record in caplog.records)


def test_настройки_сохраняются(isolated_config):
    before = read_config()

    update_config({"keep_subaccounts": True,
                   "general_settings": {"parallel_processing": True}})
    clear_config_cache()

    config = read_config()

    assert config["keep_subaccounts"] is True
    assert config["general_settings"]["parallel_processing"] is True
    # Остальные настройки не должны затираться при точечном обновлении
    assert config["accounts_without_subaccount"] == before["accounts_without_subaccount"]


def test_неизвестный_тип_файла_понятен():
    """Ошибка чтения пути говорит по-русски и подсказывает, что делать."""
    from flatregister.errors import InputPathError
    from flatregister.excel_io import validate_paths

    with pytest.raises(InputPathError) as caught:
        validate_paths([Path("Z:/нет/такой/папки")])

    assert caught.value.user_message
    assert caught.value.hint
    assert "Traceback" not in caught.value.user_message


def test_список_версий_совпадает_с_pyproject():
    """``requirements.txt`` обязан соответствовать ``pyproject.toml``.

    Файл генерируется командой ``uv export``; если кто-то добавит зависимость
    в одно место и забудет про другое, окружения на разных машинах разойдутся
    незаметно.

    Сверяются только РАБОЧИЕ зависимости. ``Nuitka`` лежит в группе ``build``
    и в requirements.txt не попадает намеренно: он нужен только при сборке
    .exe, а не при разработке.
    """
    pyproject = (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    block = pyproject.split("dependencies = [", 1)[1].split("]", 1)[0]
    declared = dict(re.findall(r'"([A-Za-z0-9_.\-]+)==([^"]+)"', block))

    exported = {}
    for line in (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z0-9_.\-]+)==([^;]+)", line.strip())
        if match:
            exported[match.group(1).lower().replace("_", "-")] = match.group(2).strip()

    normalised = {name.lower().replace("_", "-"): version for name, version in declared.items()}
    assert normalised, "не нашлось ни одной закреплённой версии в pyproject.toml"
    for name, version in normalised.items():
        assert name in exported, f"{name} есть в pyproject.toml, но нет в requirements.txt"
        assert exported[name] == version, \
            f"{name}: pyproject {version} против requirements {exported[name]}"


# --------------------------------------------------------------------------
# Порядок колонок
# --------------------------------------------------------------------------

def test_фиксированные_колонки_идут_первыми():
    """Колонки из описания регистра идут первыми, нумерованные — следом.

    Порядок именно такой: фиксированные по списку регистра, затем группы
    ``Документ_1/2/3`` по номеру, затем колонки корр. счетов.
    """
    frame = pd.DataFrame(columns=["Кредит", "Дата", "Документ_2", "Документ_1", "Дебет"])

    result = sort_columns(frame, ["Дата", "Документ", "Дебет", "Кредит"])

    assert list(result.columns) == ["Дата", "Дебет", "Кредит", "Документ_1", "Документ_2"]


def test_колонки_с_суффиксами_стоят_после_фиксированных():
    """``[Количество_|ВалютнаяСумма_]<счёт>_до/_ко`` собираются после остальных."""
    frame = pd.DataFrame(columns=[
        "Счёт", "60.01_до", "8_до", "60_ко", "Количество_8_до", "ВалютнаяСумма_8_до",
    ])

    result = sort_columns(frame, ["Счёт"])

    assert list(result.columns) == [
        "Счёт", "8_до", "60_ко", "60.01_до", "Количество_8_до", "ВалютнаяСумма_8_до",
    ]


@pytest.mark.xfail(
    reason="S8: прочие колонки собираются через set(cols) - set(ordered_cols), "
           "поэтому их порядок зависит от хеширования и меняется между прогонами. "
           "На одних и тех же данных два запуска дают разный порядок колонок.",
    strict=True,
)
def test_порядок_прочих_колонок_стабилен():
    """Прочие колонки обязаны идти в порядке появления в выгрузке."""
    columns = ["Значение1", "Значение2", "Значение3", "Значение4", "Значение5"]
    frame = pd.DataFrame(columns=columns)

    first = list(sort_columns(frame, []).columns)
    second = list(sort_columns(frame, []).columns)

    assert first == columns
    assert second == columns