# -*- coding: utf-8 -*-
"""
Регрессионный стенд для выгрузок 1С.

Прогоняет папку с .xlsx через те же процессоры, что и приложение, и отвечает
на вопросы, на которые у проекта нет других ответов:

  * какие файлы падают и на какой именно строке кода;
  * где «Итого» из файла не сходится с суммой строк после обработки;
  * что было до правки и что стало после (нужен --out в оба прогона и compare.py).

Зачем он нужен именно сейчас: проект живёт на pandas 3, где колонка, в которой
все значения пустые, получает dtype 'str'. Из-за этого DataFrame.sum() по такой
колонке возвращает строку, а не число, и суммирование падает
(TypeError: can only concatenate str (not "float") to str). Регрессия в этом
месте не видна ни по diff, ни по глазам — нужен прогон по реальным выгрузкам.

Запуск (обязательно из корня проекта, там лежит config.json):

    chcp 65001
    python tools\\regress.py --mode detect  --root "C:\\path\\to\\папке"
    python tools\\regress.py --mode process --root "C:\\path\\to\\папке" --out before.json

    python tools\\regress.py --mode process --root "..." --out after.json

Сравнение двух прогонов:

    python tools\\regress.py --compare before.json after.json

Регистры определяются автоопределением по заголовкам (см. FileProcessorFactory).
Карточка и отчёт по проводкам пересекаются по паттернам — приложение разруливает
это выбором регистра пользователем, поэтому стенд пробует все подходящие варианты
и считает успешным тот, который отработал. AMBIGUOUS-файлы здесь не баг.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import traceback
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from flatregister.config import clear_config_cache
from flatregister.errors import describe_exception
from flatregister.excel_io import fix_1c_excel_case
from flatregister.pipeline.factory import FileProcessorFactory

# Порог, начиная с которого расхождение сумм считается настоящим.
# Нули и -0.0 после округления до целых — обычное дело, а не тревога.
DELTA_THRESHOLD = 0.5

DELTA_COLUMNS = ("Сальдо_начало_разница", "Оборот_разница", "Сальдо_конец_разница")

# Служебные и выходные папки, которые не нужно прогонять.
SKIP_PARTS = {"_OUTPUT_DATA", "_archive", "_REFERENCE_DATA", "_corrections",
              "__pycache__", "node_modules", ".git", ".idea", ".venv"}

STATUS_RANK = {"OK": 0, "DELTA": 1, "FAIL": 2}


def _stable_id(text: str) -> str:
    """Короткий устойчивый идентификатор файла.

    Имя выгрузки из 1С содержит название организации, номер счёта и период —
    то есть то, что не должно попасть в репозиторий. Хеш даёт тот же ключ
    между прогонами (значит, сравнение работает), но не раскрывает содержание.
    """
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def anonymize(records: list) -> list:
    """Заменяет имена файлов на хеши.

    Эталон предназначен для коммита в репозиторий, а названия компаний и
    номера счетов — это бухгалтерские данные клиентов. Консольный отчёт
    остаётся читаемым: он печатается до обезличивания, поэтому по сообщению
    «[FAIL] <имя файла>» всегда видно, о каком файле речь.
    """
    result = []
    for record in records:
        copy = dict(record)
        name = record.get("file", "")
        parent = Path(name).parent
        stem = Path(name).stem
        anonymized = f"{_stable_id(stem)}.xlsx"
        copy["file"] = str(parent / anonymized) if str(parent) != "." else anonymized
        result.append(copy)
    return result


def deepest_project_frame(tb) -> str:
    """Самая глубокая рамка трейсбека, лежащая внутри проекта.

    Нужна, чтобы падение показывало строку кода проекта, а не внутренности pandas.
    """
    frames = traceback.extract_tb(tb)
    root_lower = str(ROOT).lower()
    self_name = Path(__file__).name
    for frame in reversed(frames):
        try:
            resolved = Path(frame.filename).resolve()
        except Exception:
            continue
        if str(resolved).lower().startswith(root_lower) and resolved.name != self_name:
            return f"{resolved.relative_to(ROOT)}:{frame.lineno}  |  {(frame.line or '').strip()}"
    return frames[-1].filename if frames else "?"


def describe(error: BaseException, *, file_name: str = "") -> tuple[str, str]:
    """Возвращает ``(причина, сообщение для пользователя)``.

    Причина — последняя строка трейсбека, то есть ``KeyError: 'С кред. счетов'``.
    Именно её показываетем в отчёте: по ней видно, что именно сломалось в коде.
    Сообщение для пользователя берём из flatregister.errors — так стенд и
    приложение показывают об одном и том же отказе.
    """
    failure = describe_exception(error, file_name=file_name)
    detail = failure.technical_detail or ""
    cause = next((line.strip() for line in reversed(detail.splitlines()) if line.strip()), "")
    return cause or failure.user_message, failure.user_message


def collect_files(root: Path) -> list:
    files = []
    for path in sorted(root.rglob("*.xlsx")):
        if any(part in SKIP_PARTS for part in path.parts):
            continue
        files.append(path)
    return files


def detect_registers(path: Path) -> list:
    """Читает первые 20 строк один раз и проверяет все паттерны всех регистров."""
    frame = pd.read_excel(fix_1c_excel_case(path), header=None, nrows=20)
    normalized = frame.map(lambda x: str(x).strip().lower() if pd.notna(x) else "")
    hits = []
    for register, configs in FileProcessorFactory.REGISTER_PATTERNS.items():
        for index, config in enumerate(configs):
            if FileProcessorFactory._contains_pattern(normalized, config["pattern"]):
                hits.append((register, index, config["processor"].__name__))
    return hits


def read_deltas(check) -> dict:
    """Достаёт из контрольной таблицы разницы 'до минус после'."""
    deltas = {}
    if check is None or getattr(check, "empty", True):
        return deltas
    row = check.iloc[0]
    for column in DELTA_COLUMNS:
        if column in row.index:
            try:
                deltas[column] = round(float(row[column]), 2)
            except (TypeError, ValueError):
                pass
    return deltas


def try_variant(path: Path, register: str, index: int, class_name: str) -> dict:
    """Прогоняет один файл через один вариант обработчика."""
    attempt = {"variant": f"{register}#{index} {class_name}"}
    try:
        processor = FileProcessorFactory.REGISTER_PATTERNS[register][index]["processor"]()
        result, check = processor.process_file(path)
        attempt["status"] = "OK"
        attempt["rows"] = int(len(result))

        # ФОРМА результата, а не только статус. Раньше стенд ловил падения и
        # расхождения сумм, но молчал, если обработка стала возвращать вдвое
        # меньше строк или потеряла колонку: статус был тот же — OK.
        attempt["columns"] = sorted(map(str, result.columns))

        deltas = read_deltas(check)
        if deltas:
            attempt["deltas"] = deltas
        if any(abs(value) > DELTA_THRESHOLD for value in deltas.values()):
            attempt["status"] = "DELTA"
    except Exception as error:
        cause, message = describe(error, file_name=path.name)
        attempt["status"] = "FAIL"
        attempt["error"] = cause
        attempt["message"] = message
        attempt["where"] = deepest_project_frame(sys.exc_info()[2])
    return attempt


def run(root: Path, mode: str, only_register: str | None, limit: int) -> list:
    files = collect_files(root)
    if limit:
        files = files[:limit]

    records = []
    started = time.perf_counter()

    for number, path in enumerate(files, 1):
        record = {"file": str(path.relative_to(root))}

        try:
            hits = detect_registers(path)
        except Exception as error:
            cause, message = describe(error, file_name=path.name)
            record.update(status="ERROR_DETECT", register="-", processor="-",
                          error=cause, message=message,
                          where=deepest_project_frame(sys.exc_info()[2]))
            records.append(record)
            continue

        record["detected"] = [f"{r}#{n} {c}" for r, n, c in hits]

        if not hits:
            record.update(status="NO_MATCH", register="-", processor="-")
        elif mode == "detect":
            record.update(status="DETECTED", register=hits[0][0], processor=hits[0][2])
        elif only_register and not any(h[0] == only_register for h in hits):
            record.update(status="SKIPPED", register=hits[0][0], processor=hits[0][2])
        else:
            candidates = [h for h in hits if not only_register or h[0] == only_register]
            attempts = [try_variant(path, *h) for h in candidates]
            best = min(attempts, key=lambda a: STATUS_RANK[a["status"]])

            record["attempts"] = attempts
            record["chosen"] = best["variant"]
            record["register"] = best["variant"].split("#")[0]
            record["processor"] = best["variant"].split(" ")[-1]
            record["status"] = best["status"]
            for key in ("error", "message", "where", "deltas", "rows", "columns"):
                if key in best:
                    record[key] = best[key]

        records.append(record)
        if number % 20 == 0 or number == len(files):
            print(f"  ... {number}/{len(files)}  ({time.perf_counter() - started:.0f}s)", flush=True)

    return records


def report(records: list, root: Path, mode: str, elapsed: float) -> None:
    statuses = Counter(r["status"] for r in records)
    registers = Counter(r.get("register", "?") for r in records)

    print("=" * 78)
    print(f"РЕЖИМ: {mode}   КОРЕНЬ: {root}")
    print(f"ФАЙЛОВ: {len(records)}   ВРЕМЯ: {elapsed:.0f}s")
    print("=" * 78)
    print("статусы:")
    for status, count in statuses.most_common():
        print(f"  {status:<14} {count}")
    print("регистры (по выбранному варианту):")
    for register, count in registers.most_common():
        print(f"  {register:<14} {count}")

    problems = [r for r in records if r["status"] in ("FAIL", "DELTA", "NO_MATCH", "ERROR_DETECT")]
    if problems:
        print(f"\n--- требуют внимания: {len(problems)} ---")
        for record in problems:
            print(f"[{record['status']}] {record['file']}")
            if record.get("chosen"):
                print(f"      вариант: {record['chosen']}")
            if record.get("message") and record["message"] != record.get("error"):
                print(f"      сообщение: {record['message']}")
            if record.get("error"):
                print(f"      {record['error']}")
            if record.get("where"):
                print(f"      {record['where']}")
            if record.get("deltas"):
                print(f"      разница: {record['deltas']}")

    sites = Counter(r["where"] for r in records if r.get("where"))
    if sites:
        print("\n--- падения, сгруппированные по месту ---")
        for site, count in sites.most_common():
            print(f"{count:>4} шт.  {site}")


def compare(before_path: Path, after_path: Path) -> None:
    before = {r["file"]: r for r in json.loads(before_path.read_text(encoding="utf-8"))}
    after = {r["file"]: r for r in json.loads(after_path.read_text(encoding="utf-8"))}
    common = sorted(set(before) & set(after))

    def real_deltas(record):
        return {k: v for k, v in (record.get("deltas") or {}).items() if abs(v) > DELTA_THRESHOLD}

    print("=" * 78)
    print(f"ДО: {before_path.name}   ПОСЛЕ: {after_path.name}   общих файлов: {len(common)}")
    print("=" * 78)
    print(f"{'СТАТУС':<14} {'ДО':>6} {'ПОСЛЕ':>6}")
    for status in ("OK", "DELTA", "FAIL", "NO_MATCH", "ERROR_DETECT"):
        b = sum(1 for f in common if before[f]["status"] == status)
        a = sum(1 for f in common if after[f]["status"] == status)
        if b or a:
            print(f"{status:<14} {b:>6} {a:>6}")

    regress = [f for f in common
               if before[f]["status"] not in ("FAIL", "NO_MATCH", "ERROR_DETECT")
               and after[f]["status"] in ("FAIL", "ERROR_DETECT")]
    improved = [f for f in common
                if before[f]["status"] in ("FAIL", "ERROR_DETECT")
                and after[f]["status"] not in ("FAIL", "ERROR_DETECT")]
    b_delta = {f: real_deltas(before[f]) for f in common}
    a_delta = {f: real_deltas(after[f]) for f in common}
    new_delta = sorted(f for f in common if a_delta[f] and not b_delta[f])
    fixed_delta = sorted(f for f in common if b_delta[f] and not a_delta[f])
    changed = sorted(f for f in common if b_delta[f] and a_delta[f] and b_delta[f] != a_delta[f])

    print()
    print(f"РЕГРЕССИИ (стало FAIL, раньше нет) : {len(regress)}")
    for f in regress:
        print(f"   {f}")
        print(f"      {after[f].get('error')}")
        print(f"      {after[f].get('where')}")
    print(f"ИСПРАВЛЕНО падений                   : {len(improved)}")
    print(f"НОВЫЕ расхождения сумм               : {len(new_delta)}")
    for f in new_delta:
        print(f"   {f}: {a_delta[f]}")
    print(f"ИСЧЕЗЛИ расхождения сумм             : {len(fixed_delta)}")
    print(f"ИЗМЕНИЛИСЬ величины расхождений      : {len(changed)}")
    for f in changed:
        print(f"   {f}\n      было : {b_delta[f]}\n      стало: {a_delta[f]}")


def assert_no_regression(before_path: Path, after_path: Path) -> int:
    """Сравнивает два отчёта и возвращает код возврата для CI и для агента.

    Это то, ради чего написан регрессионный стенд: одна команда, которая
    отвечает на вопрос «изменилось ли поведение обработки». Код возврата
    ненулевой при любой регрессии — пройти мимо невозможно.

    Проверяется четыре вещи:
      * файл перестал обрабатываться (было не FAIL, стало FAIL);
      * изменилось число строк;
      * изменился набор колонок;
      * появились или исчезли расхождения по сверке оборотов.
    """
    before = {r["file"]: r for r in json.loads(before_path.read_text(encoding="utf-8"))}
    after = {r["file"]: r for r in json.loads(after_path.read_text(encoding="utf-8"))}
    common = sorted(set(before) & set(after))

    regress, row_changed, column_changed = [], [], []

    def real_deltas(record):
        return {k: v for k, v in (record.get("deltas") or {}).items()
                if abs(v) > DELTA_THRESHOLD}

    for name in common:
        was, now = before[name], after[name]
        if was.get("status") in ("FAIL", "ERROR_DETECT", "NO_MATCH") and \
                now.get("status") == "OK":
            regress.append((name, "файл теперь обрабатывается, раньше нет"))
        elif was.get("status") == "OK" and now.get("status") != "OK":
            regress.append((name, f"статус {was['status']} -> {now['status']}"))
        elif was.get("status") == "OK" and now.get("status") == "OK":
            if was.get("rows") != now.get("rows"):
                row_changed.append((name, was.get("rows"), now.get("rows")))
            if set(was.get("columns") or []) != set(now.get("columns") or []):
                lost = set(was.get("columns") or []) - set(now.get("columns") or [])
                column_changed.append((name, sorted(lost)))

    new_delta = [n for n in common if real_deltas(after[n]) and not real_deltas(before[n])]
    fixed_delta = [n for n in common if real_deltas(before[n]) and not real_deltas(after[n])]

    print("=" * 78)
    print(f"ЭТАЛОН: {before_path.name}   ПРОГОН: {after_path.name}   общих файлов: {len(common)}")
    print("=" * 78)

    print(f"\nРЕГРЕССИИ (стало хуже)        : {len(regress)}")
    for name, why in regress:
        print(f"    {name}\n        {why}")
    print(f"изменилось число строк        : {len(row_changed)}")
    for name, was, now in row_changed[:20]:
        print(f"    {name}: {was} -> {now}")
    print(f"пропали колонки               : {len(column_changed)}")
    for name, lost in column_changed[:20]:
        print(f"    {name}: нет {lost}")
    print(f"исчезли расхождения сверки    : {len(fixed_delta)}")
    print(f"новые расхождения сверки      : {len(new_delta)}")
    for name in new_delta[:20]:
        print(f"    {name}: {after[name].get('deltas')}")

    broken = regress or row_changed or column_changed or new_delta
    if broken:
        print("\nИТОГ: ЕСТЬ РЕГРЕССИИ")
        return 1
    print("\nИТОГ: регрессий нет")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Регрессионный стенд обработки выгрузок 1С")
    parser.add_argument("--root", help="папка с .xlsx для прогона")
    parser.add_argument("--mode", choices=["detect", "process"], default="process",
                        help="detect — только определить регистр, без Excel")
    parser.add_argument("--out", help="куда сохранить отчёт в json "
                                      "(имена файлов обезличиваются)")
    parser.add_argument("--raw-names", action="store_true",
                        help="не обезличивать имена файлов в json-отчёте. "
                             "Небезопасно для отчёта, который попадёт в git")
    parser.add_argument("--only-register",
                        help="прогнать только этот регистр: card|posting|analisys|"
                             "turnover|accountosv|generalosv")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число файлов")
    parser.add_argument("--compare", nargs=2, metavar=("ДО", "ПОСЛЕ"),
                        help="сравнить два json-отчёта вместо прогона")
    parser.add_argument("--assert-no-regression", nargs=2, metavar=("ЭТАЛОН", "ПРОГОН"),
                        help="то же, но с кодом возврата: 1 при любой регрессии. "
                             "Сравнивает статус, число строк, набор колонок "
                             "и расхождения сверки")
    args = parser.parse_args()

    if args.assert_no_regression:
        return assert_no_regression(Path(args.assert_no_regression[0]),
                                    Path(args.assert_no_regression[1]))

    if args.compare:
        compare(Path(args.compare[0]), Path(args.compare[1]))
        return 0

    if not args.root:
        parser.error("нужен --root с путём к папке (или --compare с двумя отчётами)")

    root = Path(args.root)
    if not root.is_dir():
        print(f"Нет такой папки: {root}")
        return 2

    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    clear_config_cache()

    started = time.perf_counter()
    records = run(root, args.mode, args.only_register, args.limit)
    report(records, root, args.mode, time.perf_counter() - started)

    if args.out:
        to_write = records if args.raw_names else anonymize(records)
        Path(args.out).write_text(json.dumps(to_write, ensure_ascii=False, indent=2),
                                  encoding="utf-8")
        note = "" if args.raw_names else "  (имена файлов обезличены)"
        print(f"\nотчёт: {args.out}{note}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())