# Руководство для ИИ-агента (AGENTS.md)

## Язык общения
* **С пользователем — только на русском.** Внутренние рассуждения — на любом языке.
* Файлы репозитория — UTF-8. **PowerShell 5.1 в консоли не отображает кириллицу** (`Get-Content` даёт мошибейк) — читай через инструмент чтения файлов или ставь `chcp 65001`. Это артефакт консоли, а не файлов.

## Что это
Настольная Windows-программа: превращает выгрузку регистра из 1С в «плоскую» таблицу, удобную для анализа в Excel. Legacy-проект, предок «Собирателя расшифровок». 6 регистров, у каждого две реализации — **УПП** и **не-УПП** (разные заголовки выгрузки). В S9 структура: ядро в `flatregister/`, интерфейс в `ui/`, точка входа `main.py`.

## Запуск и сборка
* **Точка входа:** `main.py:7-11` → `FlatRegister1CApp().run()` (класс в `ui/app.py:30`). Запуск: `uv run python main.py` из корня проекта (или `python main.py` из активированного `.venv`).
* **GUI:** Textual 8.0.0, стили — `style.tcss`. Textual ищет CSS относительно файла класса приложения, поэтому путь развёрнут абсолютно: `ui/app.py:28`. Не tkinter/Qt: логика пишется как `Screen`/`ModalScreen` с `compose()`/`on_*`-хендлерами, тяжёлая работа — через `@work(thread=True)`.
* **Зависимости:** единственный источник — `pyproject.toml` (pandas 3.0.1, openpyxl 3.1.5, xlwings 0.33.20, pywin32, textual 8.0.0, segno, rich). `requirements.txt` — сгенерированная копия (`uv export --no-dev`), сверяется тестом. Нужен **установленный Excel** (xlwings работает только через COM). Окружение: `.venv` на Python 3.12, `requires-python = ">=3.11,<3.14"`.
* **Сборка в .exe:** Nuitka лежит в группе `build` (`uv run --group build nuitka ...`):
  `nuitka --standalone --include-data-file="style.tcss=style.tcss" --include-package=flatregister --include-package=ui --include-package=rich --include-package=rich._unicode_data main.py`
  Обязателен `--include-data-file` для `style.tcss` (он остаётся в корне). `config.json` при сборке не нужен: он создаётся при первом запуске рядом с `.exe` (или в `%APPDATA%`), там же ищется. `text.py` — обычный Python-модуль, подхватывается импортом.
* **Тесты и регрессия — есть.** См. «Проверка» ниже: `uv run pytest` (111 passed) и `tools/regress.py` с эталонами `tools/baseline_gaz.json` / `tools/baseline_mez.json`.

## Поток обработки
1. `FlatRegister1CApp.compose` — `Select(select_options())` из единого реестра (`ui/app.py:69-73`, `flatregister/registry.py:283-286`).
2. Кнопка «Продолжить» → `register` + текст требований `entry.requirements` → `RegisterScreen` (`ui/app.py:103-115`).
3. `RegisterScreen`: путь вводится перетаскиванием в `Input#path_register`, валидатор — `is_valid_dragged_path`; Enter → `on_input_submitted` (`ui/screens.py:202-222`).
4. Прогресс-экран: **файл** → `LoaderIndicatorCustom`, **папка** → `ProgressBarCustom` (`ui/screens.py:213-217`).
5. `@work(thread=True) process_file` (`ui/screens.py:224-253`) → `FileHandler(parallel=get_parallel_processing_option()).handle_input(path, register, progress_callback=progress_screen.update_progress)`.
6. Возврат в UI-поток — **только** через `self.app.call_from_thread(...)` (`ui/screens.py:244,253`). Из `@work(thread=True)` нельзя трогать виджеты напрямую. Перед возвратом `file_handler.result.clear()` — чтобы фоновый поток не держал кадры в памяти.

## Реестр регистров (единая точка регистрации)
`flatregister/registry.py:73-267` — `REGISTER_SPEC: Dict[str, RegisterEntry]`. Это **единственное** место, где регистр описан снаружи процессоров: до S8 он был прописан в `Select`, `REGISTER_PATTERNS`, `DESIRED_ORDER` и `text.REQUIREMENTS`.

`RegisterEntry` (`registry.py:47-64`): `key`, `title`, `menu_number` (порядок в меню), `requirements` (Markdown-текст требований), `patterns` (кортеж `RegisterPattern`), `column_order: {variant: tuple(...)}`, `kind`.

`RegisterPattern` (`registry.py:38-44`): `headers` (множество заголовков), `processor` (класс), `variant` (`upp`/`not_upp`, `registry.py:30-31`).

* Распознавание: `FileProcessorFactory.get_processor` (`flatregister/pipeline/factory.py:28-53`) читает **первые 20 строк** (`pd.read_excel(..., header=None, nrows=20)`, строка 41), нормализует в нижний регистр и пробелы, сравнивает `pattern.issubset(set(row))` (`_contains_pattern`, `factory.py:55-61`). Первое совпадение побеждает — порядок паттернов и **порядок вставки регистров** значимы.
* Ни один паттерн не совпал → `RegisterNotRecognizedError`. Неизвестный ключ регистра → тоже `RegisterNotRecognizedError` (не `KeyError`).
* Вариант результата: `RegisterEntry.variant_for(processor_class)` (`registry.py:59-64`) сравнивает класс по идентичности. `ResultCollector` берёт ветку (`upp`/`non_upp`/аналитика) по `entry.kind`, а не по имени класса.

**Добавить регистр** = реализовать процессоры и добавить **одну** запись в `REGISTER_SPEC`: меню, требования, порядок колонок и распознавание подтянутся сами. Ни `Select`, ни `text.py`, ни отдельного `DESIRED_ORDER` трогать не нужно.

## Пакетная обработка каталога
* Файлы: `dir_path.glob("*.xlsx")`, пусто → `NoInputFilesError` (`flatregister/pipeline/handler.py:189-198`).
* Параллельный режим — **только если** `parallel=True` **и** регистр != `turnover` (`handler.py:135`). Это осознанное условие, отражено в подсказке настроек.
* Воркеров: `min(os.cpu_count() or 4, файлов)`, `ProcessPoolExecutor` (`handler.py:156-158`).
* Воркер `_process_file_parallel` (`flatregister/pipeline/parallel.py:12-36`) **обязан быть top-level** (picklable) и **возвращать кортеж**, а не бросать: `(file_name, processor_class, result, check, failure)`. Ошибка едет обратно `FileFailure`. Журналирования внутри воркеров нет намеренно (несколько процессов мешали бы ротации одного файла) — трейсбек пишет родитель в `_record_failure`.

## Сборка результата
`ProcessingResult` (`handler.py:36-56`) — единый контейнер: `files`, `checks`, `failures`, `register_type`; `processed_count` и `clear()`. Заменил три параллельных словаря (`not_correct_files`, `storage_processed_registers`, `check`). `processed_count` — число **успешно обработанных файлов** (растёт и в папочном режиме, где `files` пуст), а не `len(files)`.

`ResultCollector` (`flatregister/pipeline/result_collector.py:22`):
* регистр передаётся в **конструктор** (`ResultCollector(register_type)`, `handler.py:132`), а не ставится атрибутом вручную;
* `card` / `posting` → списки `upp_results` / `non_upp_results`;
* аналитические регистры → `analytical_results: {класс: (results, checks)}`;
* `_process_analisys_like`: конкатенация → **один** вызов `shiftable_level` на итоговом кадре инстансом **последнего** класса (`result_collector.py:66-91`). На каждой итерации `shift_levels` делает `df.copy()` — на широких кадрах это заметная цена;
* сортировка аналитики идёт по ключу `'upp'` независимо от варианта (`result_collector.py:89`). Сегодня безобидно: у аналитических регистров списки `upp` и `not_upp` идентичны. Если разойдёшь — non-УПП-аналитика молча отсортируется по УПП-порядку.

Запись (`FileHandler._save_combined_results`, `handler.py:220-255`) — во **временный** xlsx, затем `os.startfile`:
| Регистр | Листы |
|---|---|
| card, posting | `UPP`, `Non_UPP` |
| analisys | `analisys` + `analisys_check` |
| turnover | `turnover` + `turnover_check` |
| accountosv | `accountosv` + `accountosv_check` |
| generalosv | `generalosv` + `generalosv_check` |

* `write_df_in_chunks` (`flatregister/excel_io.py:49-70`) режет кадр по 1 000 000 строк; при нескольких частях к имени листа добавляется `1`, `2`, … Каждое имя проходит `sanitize_sheet_name`.
* Перед записью везде выполняется `df.map(lambda x: x.rstrip() if isinstance(x, str) else x)` — хвостовые пробелы убираются **на выходе**, не в процессорах.
* Режим **одного файла** идёт отдельно: `_save_and_open_batch_result` (`handler.py:257-281`) делает лист на каждый файл, имя листа = имя файла, прогнанное через `sanitize_sheet_name` (`[]:*?/\` → `_`, пустое → `Лист`, обрезка до 31).
* Неудачные файлы → `ProcessingResult.failures` → `generate_failed_processing_markdown` (`excel_io.py:213-246`) → экран `ResultProcessingDirectotyScreen` (`ui/screens.py:44-63`).

## Чтение выгрузок 1С — самая хрупкая часть
1. **`fix_1c_excel_case`** (`excel_io.py:112-142`): перепаковывает xlsx, переименовывая `xl/SharedStrings.xml` → `xl/sharedStrings.xml`. Причина: 1С пишет имя с большой буквы, openpyxl ждёт lowercase, иначе файл не читается. Возвращает `BytesIO`, не путь; ошибки → `SheetUnreadableError` (файл открыт в Excel / повреждён).
2. **`FileProcessor._preprocessor_openpyxl`** (`flatregister/registers/base.py:78-176`):
   * читает всё через openpyxl с `values_only`;
   * добавляет служебные колонки **`Уровень_группировки`** (Excel outline level) и **`Курсив`** (курсив в колонке `Кор. Счет`/`Кор.счет`) — по ним определяется принадлежность строки коррсчёту;
   * **если в первых 30 строках найдена строка со всеми заголовками оборотов — перечитывает её через xlwings (`cell.api.Text`)**: объединённые и сгруппированные ячейки openpyxl отдаёт не так, как их видит Excel. xlwings не работает с `BytesIO` → файл пишется во временный, поднимается скрытый Excel, читается, всё закрывается, `time.sleep(0.5)`. Очистка — в `finally` (`base.py:152-161`), «зависших» `EXCEL.EXE` быть не должно;
   * возвращает DataFrame **без заголовков** (`columns=None`).
3. **`FileProcessor._process_dataframe_optimized`** (`base.py:180-242`):
   * строка с «дата» — это шапка; не нашлась → `RegisterNotRecognizedError`;
   * `Дата` парсится как `%d.%m.%Y %H:%M:%S`, затем `%d.%m.%Y`;
   * **повторяющиеся `Документ` нумеруются**: `Документ_end1`, `_end2`, … через `cumcount`. Без этого повторы схлопываются при конкатенации;
   * `Дата` и `Документ` добираются `ffill`;
   * пустые заголовки → `NoNameCol N`;
   * отбрасываются строки без `Дата` и полностью пустые строки/колонки.

## Примитивы иерархии
Общие функции — в `flatregister/hierarchy.py`; `FileProcessor` держит одноимённые методы-делегаты (`shiftable_level`, `find_max_level_column`, `_is_parent`, `_is_accounting_code_vectorized`), чтобы наследники не переписывались.

* **`shift_levels`** (`hierarchy.py:77-125`) — итеративно сдвигает `Level_N`, чтобы счета и субконто встали в одну колонку. Критерий «в колонке есть и счёт, и субконто» = `is_accounting_code(col).nunique() == 2` **плюс** guard (`hierarchy.py:103`): колонка `Level_{lm-1}`, из которой берётся значение, сама должна содержать хотя бы один счёт. Без него любые 1–2-значные числа (инвентарные номера вида `59`) распознаются как счета и сдвиг уезжает вправо по всей таблице. Цикл крутится, пока кадр не перестанет меняться (`df.copy()` на каждой итерации, строка 92).
* **`max_level_column`** (`hierarchy.py:128-149`) — самый глубокий `Level_N`, где **все** значения распознаны как счета.
* **`is_accounting_code`** (`hierarchy.py:20-74`): `"0"/"00"/"000"` — счёт; без точки — только цифры длиной ≤ 2; с точкой — каждая часть цифры ≤ 2 либо буквы ≤ 2, и хотя бы одна часть цифровая.
* Также здесь: `find_word_column` / `find_word_column_and_row`, `spread_levels`, `drop_interim_total_rows`, `hierarchy_drop_set`, `collapse_subaccount`, `get_col_or_zeros`, `split_and_expand`, `keep_first_unique_per_row`.

## Порядок колонок
Эталонный порядок — в реестре (`REGISTER_SPEC[...].column_order`), читается через `flatregister.registry.column_order(register_type, variant)` (`registry.py:275-280`). Никогда не хардкодь порядок в процессоре.

`sort_columns` (`flatregister/column_order.py:31-115`) собирает итог в четыре прохода:
1. фиксированные колонки из `desired_order`;
2. группы по `COLUMN_PREFIXES` (`column_order.py:11-14`) — `Операция_`, `Содержание_`, `Документ_`, `Аналитика Дт_`, `Субконто Дт_`, `Аналитика Кт_`, `Субконто Кт_`, `Level_`, сортировка по числовому суффиксу (`_group_sort_key`, `column_order.py:17-28`; колонка-префикс без числового суффикса не роняет `re.search(...).group(1)`);
3. «спец-колонки» вида `[Количество_|ВалютнаяСумма_]<число[.буквы]>_до|_ко` (`parse_key`);
4. остальные — **в порядке появления в кадре** (`column_order.py:110-113`), а не через `set()`.

⚠️ **Новый вид колонок обязан попасть в `COLUMN_PREFIXES`** — иначе уедет в «остальные».

## Конфиг `config.json`
`flatregister/config.py`. Эталон значений — `DEFAULT_CONFIG` (`config.py:34-46`). Доступ: `read_config` (`config.py:167-192`), `update_config` (`config.py:209-248`), `load_config` (`config.py:251-255`), типизированные геттеры `get_accounts_without_subaccount` / `get_parallel_processing_option` / `get_keep_subaccounts` (`config.py:259-278`).

**Путь** (`get_config_path`, `config.py:74-92`), в порядке приоритета:
1. переменная окружения `FLATREGISTER_CONFIG_PATH` (нужна тестам и переносимым запускам; если путь — папка, добавляется `config.json`);
2. в собранном `.exe`: папка рядом с исполняемым файлом, если она writable, иначе `%APPDATA%\FlatRegister1C\config.json`;
3. в разработке — корень проекта.

| Ключ | Смысл | Где редактируется |
|---|---|---|
| `accounts_without_subaccount` | список `[счёт, корр_счёт, флаг]`; в отчёт идут **только строки с флагом `1`** → «по каким корр.счетам не разворачивать в субсчета» | F3 на `RegisterScreen`, регистры `analisys` / `turnover` |
| `general_settings.parallel_processing` | пакетный режим в `ProcessPoolExecutor` | F3 → `SettingsScreen` (`ui/screens.py:286-326`) |
| `keep_subaccounts` | только `generalosv`: `True` → в отчёте счета **без** дочерних субсчетов (только `60.01.1`, но не `60` и `60.01`) | F3 на `RegisterScreen`, `generalosv` |

⚠️ Битый JSON или кодировка больше **не** перетираются молча: файл сохраняется как `config.json.bak-<штамп>`, в лог пишется предупреждение, настройки сбрасываются на `DEFAULT_CONFIG` (`_reset_broken_config`, `config.py:148-164`).
⚠️ Глобальный кэш `_config_cache` (модульная переменная) — для тестов зови `clear_config_cache()`.

## Пути, которые приходят из 1С
* `is_valid_dragged_path` (`excel_io.py:182-211`) — снимает кавычки, `/` → `\`.
* `normalize_path` (`excel_io.py:162-179`) + `generate_dash_combinations` (`excel_io.py:145-160`) — перебирают **все комбинации** замены дефисов `-` ↔ `—` в пути. Для пути с N дефисами это 2^N вариантов: длинные пути с несколькими дефисами дают взрыв. Имей в виду при отладке «путь не находится».
* `validate_paths` (`excel_io.py:72-109`) — собирает все битые пути и бросает `InputPathError` одним сообщением.

## Исключения и журнал
`flatregister/errors.py` — иерархия от общего предка `FlatRegisterError` (`errors.py:31-49`). У каждой ошибки `user_message` (что показать) и `hint` (что делать); `str(ошибки)` возвращает только `user_message`.

| Исключение | Где объявлено | Смысл |
|---|---|---|
| `InputPathError` | `errors.py:56` | путь не существует и не чинится заменой тире |
| `NotSpreadsheetError` | `errors.py:62` | расширение не `.xlsx` |
| `SheetUnreadableError` | `errors.py:68` | файл открыт в Excel / повреждён |
| `NoInputFilesError` | `errors.py:77` | в папке нет xlsx |
| `RegisterNotRecognizedError` | `errors.py:87` | файл не распознан как выбранный регистр |
| `RegisterStructureError` | `errors.py:93` | не хватает обязательных заголовков/уровней или нарушено постусловие типов |
| `RegisterEmptyError` | `errors.py:103` | регистр распознан, но данных нет |
| `NoRecognizedRegistersError` | `errors.py:109` | ни один файл в папке не распознан |
| `OutputError` | `errors.py:119` | не удалось записать/открыть итоговый файл |
| `ConfigError` | `errors.py:125` | `config.json` недоступен или повреждён |

* `FileFailure` (`errors.py:135-157`) — единый picklable-формат «файл не обработан» (`user_message`, `hint`, `technical_detail`). `technical_detail` уходит **только** в лог.
* `describe_exception` (`errors.py:160-188`) — единственное место, где любое исключение становится `FileFailure`: известная ошибка → её текст и подсказка; неизвестная → «произошла внутренняя ошибка» + ссылка на журнал.
* `flatregister/logging_setup.py` — rotating-лог 2 МБ × 3, `logs/app.log` (`get_log_dir`, `logging_setup.py:36-55`: рядом с `.exe` в сборке, в разработке — корень проекта, при недоступности — `%LOCALAPPDATA%`). `setup_logging()` вызывается при импорте `ui/app.py` (`ui/app.py:24`), до первого чтения конфигурации. `log_failure` (`logging_setup.py:99-115`) пишет ожидаемые отказы как WARNING, остальное — ERROR с трейсбеком.
* Единая воронка в UI: `RegisterScreen.process_file` ловит `Exception`, `describe_exception(...)` → `on_error` показывает `user_message` и `hint` через `notify` (`ui/screens.py:245-253,271-281`). Пользователь не видит трейсбек.

## Подводные камни
1. **Порядок регистров в `REGISTER_SPEC` значим.** Файл, подходящий под несколько паттернов, относит первое совпадение; порядок вставки сохранён прежним (card первым и т.д.), порядок меню задаётся `menu_number`.
2. **`FileHandler._process_one` — единственный путь обработки файла** (`handler.py:82-111`). Разница одиночного и пакетного режима — в `collect_failures`: пакет глотает отказ (`return None, None, None`), одиночный пробрасывает наверх. Прежние почти-дубликаты `_process_single_file` / `_process_single_file_consistently` удалены в S8.
3. **xlwings поднимает реальный Excel.** В параллельном режиме — несколько Excel одновременно; без установленного Excel обработка упадёт. Закрытие книги/приложения в `_preprocessor_openpyxl` продублировано в `try`/`finally` — при правках не оставляй «зависшие» процессы Excel.
4. **pandas 3.0.1** — мажорная версия: `df.map`, `concat`, `infer_objects` ведут себя иначе, чем в 2.x. Колонка «всё пусто» получает dtype `str`, и `df[cols].sum().sum()` падает — для этого есть `numeric_total` (`flatregister/dtypes.py:69-85`). Проверяй правки на реальных выгрузках.
5. **Типы колонок — постусловие.** `ensure_column_types` (`dtypes.py:93-123`) вызывается в конце каждого `process_file`: денежные колонки (`MONEY_COLUMNS`, `dtypes.py:26-27`) — число, счета (`ACCOUNT_COLUMNS`, `dtypes.py:31-32`) и `Level_*` — текст, иначе `RegisterStructureError`. Суммы карточки/проводок (`Дебет`, `Кредит`, `Сумма`) в `MONEY_COLUMNS` намеренно не входят: в проводках не-УПП `Дебет`/`Кредит` — это счета, а не суммы.
6. **`exclude_values`** (`registers/base.py:33-45`) — строки-маркеры промежуточных итогов, которые удаляются, чтобы исключить двойной счёт. Список в коде, не в конфиге: расширять аккуратно, две позиции уже закомментированы.
7. **Тесты заглушают xlwings** (`tests/conftest.py:216-233`): фикстуры оборотов по определению задевают COM, иначе десятки прогонов подряд роняют Excel с `0x800706ba`. Настоящая ветка xlwings непокрыта — нужен маркер `requires_excel` и живая машина.
8. **Пробелы убираются на выходе**, не в процессорах (`df.map(...rstrip...)` перед записью).

## Правила для ИИ-агента
* Не хардкодь порядок колонок — только `REGISTER_SPEC[...].column_order`. Новый вид колонок → в `COLUMN_PREFIXES`, а эталонный порядок — в реестр.
* **Новый регистр** = процессоры + **одна** запись в `REGISTER_SPEC` (`registry.py:73-267`). Меню (`menu_number`), требования (`requirements`), распознавание (`patterns`) и порядок колонок (`column_order`) берутся из неё.
* **Парность УПП/не-УПП** — держи имена `*_UPPFileProcessor` / `*_NonUPPFileProcessor` и указывай обоих в `patterns`. `ResultCollector` раскладывает по `kind`/`variant_for`, а фабрика сравнивает классы по идентичности: рассинхрон молча уедет в `non_upp_results`.
* Порядок этапов внутри регистра задаёт сам процессор (`FileProcessor.process_file`), `handler.py` только склеивает.
* Порядок воркеров в `ProcessPoolExecutor` **не сохраняется** — не полагайся на него при сортировке, сортируй явно.
* Из `@work(thread=True)` — только через `app.call_from_thread`.
* Правки в логике проверяй **на реальной выгрузке 1С** через `tools/regress.py --assert-no-regression` и сверяй с эталоном. Тестов и эталонов уже достаточно, но числа важнее зелёной галочки.

## Проверка (тесты и регрессия)
```powershell
uv run pytest                      # ожидается 111 passed
uv run ruff check .                # ожидается 0 замечаний

# регрессия: сначала прогон, потом сравнение с эталоном (код возврата 1 = регрессия)
uv run python tools\regress.py --root "<корень>" --out tools\after_gaz.json
uv run python tools\regress.py --assert-no-regression tools\baseline_gaz.json tools\after_gaz.json
```

⚠️ **Корень прогона должен совпадать с раскладкой эталона**, иначе сравнение пройдёт впустую («общих файлов: 0» — это не «0 регрессий»):
* GAZ (112 файлов): `...\1С\__ВЫГРУЗКИ_ГАЗ\__ВЫГРУЗКИ` — **вложенная** папка, не внешняя;
* МЭЗ (66 файлов): `...\1С\Для фикса Обработчиув регистров\МЭЗ`.

Эталон чувствителен к `config.json` (`keep_subaccounts: true`, счета 50/51/52/55/57): смена настроек делает сравнение ложным. `--out` обезличивает имена файлов (sha1), консольный отчёт читаемый.

## Граф знаний
Проект проиндексирован в `codebase-memory-mcp` под именем **`FlatRegister1C_Textual_parallel`** (676 узлов / 2826 рёбер на 2026-10-18, покрытие без пропусков файлов). Артефакт — `.codebase-memory/graph.db.zst` (в `.gitignore`).

**⚠️ Отличие от «Собирателя расшифровок», не переносите реализацию молча:** `is_accounting_code` здесь и `utils/column_utils.is_accounting_code` там считают бухгалтерский счёт **по разным правилам** (здесь длина частей ≤ 2 и `0`/`00`/`000` считаются счётом; там — без ограничения длины и не считаются). Обработка ошибок тоже разная: здесь иерархия `FlatRegisterError` + `FileFailure` + `notify`, там `PipelineError` + декоратор `handle_pipeline_errors` с диагностикой в `mismatches/`.
