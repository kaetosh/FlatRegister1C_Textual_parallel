# Руководство для ИИ-агента (AGENTS.md)

## Язык общения
* **С пользователем — только на русском.** Внутренние рассуждения — на любом языке.
* Файлы репозитория — UTF-8. **PowerShell 5.1 в консоли не отображает кириллицу** (`Get-Content` даёт мошибейк) — читай через инструмент чтения файлов или ставь `chcp 65001`. Это артефакт консоли, а не файлов.

## Что это
Настольная Windows-программа: превращает выгрузку регистра из 1С в «плоскую» таблицу, удобную для анализа в Excel. Legacy-проект, предок «Собирателя расшифровок». 6 регистров, у каждого две реализации — **УПП** и **не-УПП** (разные заголовки выгрузки).

## Запуск и сборка
* **Точка входа:** `main.py:572-574` → `FlatRegister1CApp().run()`. Запуск: `python main.py` из корня проекта.
* **GUI:** Textual 8.0.0, стили — `style.tcss` (`main.py:473`). Не tkinter/Qt: логика пишется как `Screen`/`ModalScreen` с `compose()`/`on_*`-хендлерами, тяжёлая работа — через `@work(thread=True)`.
* **Зависимости:** `requirements.txt` — pandas **3.0.1**, openpyxl 3.1.5, xlwings 0.33.20, pywin32, textual 8.0.0, rich, Nuitka 4.0.1. Нужен **установленный Excel** (xlings работает только через COM).
* **Сборка в .exe:** `nuitka --standalone --include-data-file="style.tcss=style.tcss" --include-package=rich --include-package=rich._unicode_data main.py` (README). Не забудь `--include-data-file` для `style.tcss`, `config.json` и текстов.
* **Тестов нет.** Единственная проверка — прогон на реальной выгрузке 1С и сверка с ручным расчётом. Компиляция (`python -m py_compile`) — не проверка логики.

## Поток обработки
1. `FlatRegister1CApp.compose` — `Select` из 6 регистров (`main.py:513-524`).
2. Кнопка «Продолжить» → `register` + текст требований из `text.REQUIREMENTS` → `RegisterScreen` (`main.py:555-563`).
3. `RegisterScreen`: путь вводится перетаскиванием в `Input#path_register`, валидатор — `is_valid_dragged_path`; Enter → `on_input_submitted` (`main.py:232-252`).
4. Прогресс-экран: **файл** → `LoaderIndicatorCustom`, **папка** → `ProgressBarCustom` (`main.py:243-247`).
5. `@work(thread=True) process_file` (`main.py:254-280`) → `FileHandler(parallel=get_parallel_processing_option()).handle_input(path, register, progress_callback=progress_screen.update_progress)`.
6. Возврат в UI-поток — **только** через `self.app.call_from_thread(...)`. Из `@work(thread=True)` нельзя трогать виджеты напрямую.

## Маршрутизация по типу файла
`FileProcessorFactory.REGISTER_PATTERNS` (`file_handler.py:56-81`) — единственное место, где регистр сопоставляется с классом обработчика. Для каждого регистра список пар: *множество заголовков* → *класс процессора*.

* Заголовки читаются из **первых 20 строк** (`pd.read_excel(..., header=None, nrows=20)`, `file_handler.py:89`), сравнение — `pattern.issubset(set(row))` (`_contains_pattern`, `file_handler.py:98-104`), регистр и пробелы нормализованы в нижний.
* Ни один паттерн не совпал → `RegisterProcessingError` («Файл не является выбранным регистром из 1С»). Это **штатный** отказ одного файла, не падение программы.
* Первое совпадение побеждает — порядок внутри списка значим.

## Пакетная обработка каталога
* Файлы: `dir_path.glob("*.xlsx")`, пусто → `NoExcelFilesFoundError` (`file_handler.py:344-350`).
* Параллельный режим — **только если** `parallel=True` **и** регистр != `turnover` (`file_handler.py:268`). Это отражено в подсказке настроек; не «убирай» условие бездумно.
* Воркеров: `min(os.cpu_count(), файлов)`, `ProcessPoolExecutor` (`file_handler.py:300`).
* Функция воркера `_process_file_parallel` (`file_handler.py:26-35`) **обязана бытьtop-level** (picklable) и возвращать кортеж, а не бросать — ошибка едет обратно строкой.

## Сборка результата
`ResultCollector` (`file_handler.py:107-196`):
* `card` / `posting` → списки `upp_results` / `non_upp_results`;
* `analisys` / `turnover` / `accountosv` / `generalosv` → словари `{класс: (results, checks)}`;
* `_process_analisys_like`: конкатенация → **один** вызов `shiftable_level` на итоговом кадре инстансом **последнего** класса (`file_handler.py:188-190`). Раньше вызывался на каждый класс — если «чинишь» производительность, помни, что это осознанное изменение поведения.

Запись (`_save_combined_results`, `file_handler.py:352-381`) — во **временный** xlsx, затем `os.startfile`:
| Регистр | Листы |
|---|---|
| card, posting | `UPP`, `Non_UPP` |
| analisys | `analisys` + `analisys_check` |
| turnover | `turnover` + `turnover_check` |
| accountosv | `accountosv` + `accountosv_check` |
| generalosv | `generalosv` + `generalosv_check` |

* `write_df_in_chunks` (`support_functions.py:247-268`) режет кадр по 1 000 000 строк; при нескольких частях к имени листа добавляется `1`, `2`, … Имя листа всегда режется до 31 символа.
* Перед записью везде выполняется `df.map(lambda x: x.rstrip() if isinstance(x, str) else x)` — хвостовые пробелы убираются **на выходе**, не в процессорах.
* Режим **одного файла** идёт отдельно: `_save_and_open_batch_result` (`file_handler.py:393-416`) делает лист на каждый файл, имя листа = имя файла.
* Неудачные файлы → `not_correct_files` → `generate_failed_processing_markdown` (`support_functions.py:411-440`) → экран `ResultProcessingDirectotyScreen` (`main.py:86-93`).

## Чтение выгрузок 1С — самая хрупкая часть
1. **`fix_1c_excel_case`** (`support_functions.py:310-336`): перепаковывает xlsx, переименовывая `xl/SharedStrings.xml` → `xl/sharedStrings.xml`. Причина: 1С пишет имя с большой буквы, openpyxl ждёт lowercase, иначе файл не читается. Возвращает `BytesIO`, не путь.
2. **`FileProcessor._preprocessor_openpyxl`** (`class_processor.py:226-322`):
   * читает всё через openpyxl с `values_only`;
   * добавляет служебные колонки **`Уровень_группировки`** (Excel outline level) и **`Курсив`** (курсив в колонке `Кор. Счет`/`Кор.счет`) — по ним определяется принадлежность строки коррсчёту;
   * **если в первых 30 строках найдена строка со всеми заголовками оборотов — перечитывает её через xlwings (`cell.api.Text`)**: объединённые и сгруппированные ячейки openpyxl отдаёт не так, как их видит Excel. xlwings не работает с `BytesIO` → файл пишется во временный, поднимается скрытый Excel, читается, всё закрывается, `time.sleep(0.5)`;
   * возвращает DataFrame **без заголовков** (`columns=None`).
3. **`FileProcessor._process_dataframe_optimized`** (`class_processor.py:327-384`):
   * строка с «дата» — это шапка; не нашлась → `RegisterProcessingError`;
   * `Дата` парсится как `%d.%m.%Y %H:%M:%S`, затем `%d.%m.%Y`;
   * **повторяющиеся `Документ` нумеруются**: `Документ_end1`, `_end2`, … через `cumcount`. Без этого повторы схлопываются при конкатенации;
   * `Дата` и `Документ` добираются `ffill`;
   * пустые заголовки → `NoNameCol N`;
   * отбрасываются строки без `Дата` и полностью пустые строки/колонки.

## Колонки уровней
* **`shiftable_level`** (`class_processor.py:386-434`) — итеративно сдвигает `Level_N`, чтобы счета и субконто встали в одну колонку. Критерий «в колонке есть и счёт, и субконто» = `_is_accounting_code_vectorized(col).nunique() == 2` **плюс** guard: колонка `Level_{lm-1}`, из которой берётся значение, сама должна содержать хотя бы один счёт (`class_processor.py:412`). Без него любые 1–2-значные числа (инвентарные номера вида `59`) распознаются как счета и сдвиг уезжает вправо по всей таблице. Цикл крутится, пока кадр перестаёт меняться. **На каждой итерации делается `df.copy()`** (`class_processor.py:401`) — на широких кадрах это заметная цена.
* **`find_max_level_column`** (`class_processor.py:437-458`) — самый глубокий `Level_N`, где **все** значения распознаны как счета.
* **`_is_accounting_code_vectorized`** (`class_processor.py:166-222`): `"0"/"00"/"000"` — счёт; без точки — только цифры длиной ≤ 2; с точкой — каждая часть цифры ≤ 2 либо буквы ≤ 2, и хотя бы одна часть цифровая.

## Порядок колонок
`DESIRED_ORDER` (`class_processor.py:46-146`) — **единственный источник правды**: `{регистр: {'upp': [...], 'not_upp': [...]}}`. Никогда не хардкодь порядок в процессоре.

`sort_columns` (`support_functions.py:161-244`) собирает итог в четыре прохода:
1. фиксированные колонки из `desired_order`;
2. группы по `COLUMN_PREFIXES` (`support_functions.py:23`) — `Операция_`, `Содержание_`, `Документ_`, `Аналитика Дт_`, `Субконто Дт_`, `Аналитика Кт_`, `Субконто Кт_`, `Level_`, сортировка по числовому суффиксу;
3. «спец-колонки» вида `[Количество_|ВалютнаяСумма_]<число[.буквы]>_до|_ко` (`parse_key`);
4. остальные.

⚠️ **Новый вид колонок обязан попасть в `COLUMN_PREFIXES`** — иначе уедет в «остальные» и порядок станет случайным.
⚠️ «остальные» собираются как `set(cols) - set(ordered_cols)` → **порядок между прогонами не гарантирован**.
⚠️ `_process_analisys_like` сортирует аналитические листы по ключу `'upp'` независимо от варианта (`file_handler.py:194`). Сегодня безобидно: у `analisys`/`turnover`/`accountosv`/`generalosv` списки `upp` и `not_upp` идентичны. Если их разойдёшь — non-УПП-аналитика отсортируется по УПП-порядку молча.

## Конфиг `config.json`
Через `read_config` / `update_config` / `load_config` (`support_functions.py:66-158`), эталон значений — `DEFAULT_CONFIG` (`support_functions.py:31-63`).

| Ключ | Смысл | Где редактируется |
|---|---|---|
| `accounts_without_subaccount` | список `[счёт, корр_счёт, флаг]`; в отчёт идут **только строки с флагом `1`** → «по каким корр.счетам не разворачивать в субсчета» | F3 на `RegisterScreen`, регистры `analisys` / `turnover` |
| `general_settings.parallel_processing` | пакетный режим в `ProcessPoolExecutor` | F3 → `SettingsScreen` |
| `keep_subaccounts` | только `generalosv`: `True` → в отчёте счета **без** дочерних субсчетов (только `60.01.1`, но не `60` и `60.01`) | F3 на `RegisterScreen`, `generalosv` |

⚠️ `read_config` при битом JSON **перезаписывает файл значениями по умолчанию** — пользовательские настройки теряются без предупреждения.
⚠️ Путь к конфигу относительный (`CONFIG_FILE_PATH = "config.json"`), глобальный кэш `_config_cache` — работает только при запуске из корня проекта (и в `.exe` — рядом с ним). Для тестов зови `clear_config_cache()`.

## Пути, которые приходят из 1С
* `is_valid_dragged_path` (`support_functions.py:380-409`) — снимает кавычки, `/` → `\`.
* `normalize_path` (`support_functions.py:356-373`) + `generate_dash_combinations` (`support_functions.py:339-354`) — перебирают **все комбинации** замены дефисов `-` ↔ `—` в пути. Для пути с N дефисами это 2^N вариантов: длинные пути с несколькими дефисами дают взрыв. Имей в виду при отладке «путь не находится».
* `validate_paths` — собирает все битые пути и бросает `IncorrectFolderOrFilesPath` одним сообщением.

## Исключения
`custom_errors.py` — 8 классов, все от `Exception`, иерархии нет:

| Исключение | Где | Смысл |
|---|---|---|
| `RegisterProcessingError` | все процессоры + фабрика | файл не распознан как регистр 1С / получен пустой кадр |
| `NotXLSXFileError` | `file_handler.py:239` | расширение не `.xlsx` |
| `NoExcelFilesFoundError` | `file_handler.py:349` | в папке нет xlsx |
| `NoRegisterFilesFoundError` | `file_handler.py:358` | ни один файл не распознан |
| `ProcessingError` | `file_handler.py:257` | обёртка исключения одиночной обработки |
| `IncorrectFolderOrFilesPath` | `support_functions.py:275,300` | пути не существуют и не чинятся заменой тире |
| `PermissionFileExcelError` | — | **объявлено, не используется** |
| `PathError` | — | **объявлено, не используется** |

⚠️ Единой точки «показать пользователю» нет: `RegisterScreen.process_file` ловит `Exception` и показывает `str(error_description)` через `notify` (`main.py:279-280`). Пользователь видит текст исключения, а не инструкцию. Мёртвые `PermissionFileExcelError` / `PathError` не вычищай, пока не проверишь импорты.

## Подводные камни
1. **Type hint врёт.** `handle_input` объявлен как `Literal['posting','card','analisys','account','generalosv']` (`file_handler.py:227`), а реальные ключи — `posting`, `card`, `analisys`, `turnover`, `accountosv`, `generalosv` (см. `Select`, `main.py:513-524`, и словари `REGISTER_PATTERNS` / `DESIRED_ORDER`). Верь словарям, не хинту.
2. **`ResultCollector.type_register` — атрибут, а не параметр конструктора.** Ставится вручную в `_process_directory` (`file_handler.py:265`). Забудешь — сортировка колонок молча пропустится через `hasattr`-проверки (`file_handler.py:193,204`).
3. **`_process_single_file` и `_process_single_file_consistently` — почти дубликаты** с разным поведением при ошибке: первый бросает, второй возвращает `(None, None, None)`. Не объединяй, не разобравшись.
4. **xlwings поднимает реальный Excel.** В параллельном режиме — несколько Excel одновременно; без установленного Excel обработка упадёт. `app.quit()`/`wb.close()` в `_preprocessor_openpyxl` дублируются в `try` и в `finally` — при правках не оставляй «зависшие» процессы Excel.
5. **pandas 3.0.1** — мажорная версия: `df.map`, `concat`, `infer_objects` ведут себя иначе, чем в привычном 2.x. Проверяй правки на реальных выгрузках.
6. **`accountosv_processor.py`: строки ~70-790 — закомментированный legacy-код.** Живой код начинается с `AccountOSV_UPPFileProcessor` (~строка 1213). Не читай середину файла как актуальную логику.
7. **Имена листов Excel** режутся до 31 символа, но **не санируются**. В режиме одного файла имя листа = имя файла (`file_handler.py:406`) — символы `[]:*?/\` в имени сломают запись.
8. **`exclude_values`** (`class_processor.py:31-43`) — строки-маркеры промежуточных итогов, которые удаляются, чтобы исключить двойной счёт. Список в коде, не в конфиге: расширять аккуратно, две позиции уже закомментированы.

## Правила для ИИ-агента
* Не хардкодь порядок колонок — только `DESIRED_ORDER`. Новый вид колонок → в `COLUMN_PREFIXES`.
* **Новый регистр** требует правки в четырёх местах: `Select` (`main.py:513-524`), `FileProcessorFactory.REGISTER_PATTERNS`, `DESIRED_ORDER`, `text.REQUIREMENTS`. Пропустишь любое — регистр либо не появится в UI, либо упадёт на `DESIRED_ORDER[...]`/`REQUIREMENTS[...]`.
* **Парность УПП/не-УПП** — держи одинаковые имена `*_UPPFileProcessor` / `*_NonUPPFileProcessor`: и `REGISTER_PATTERNS`, и `ResultCollector.add_result` сравнивают классы по имени. Новый регистр без второй реализации молча уедет в `non_upp_results`.
* Порядок этапов внутри регистра задаёт сам процессор, а не оркестратор: `FileProcessor.process_file` решает сам, `file_handler` только склеивает.
* Порядок воркеров в `ProcessPoolExecutor` **не сохраняется** — не полагайся на него при сортировке, сортируй явно.
* Из `@work(thread=True)` — только через `app.call_from_thread`.
* Правки в логике проверяй **на реальной выгрузке 1С** и сверяй суммы с ручным расчётом: автотестов и смоуков в проекте нет.

## Граф знаний
Проект проиндексирован в `codebase-memory-mcp` под именем **`FlatRegister1C_Textual_parallel`** (271 узел / 1163 ребра, покрытие полное). Артефакт — `.codebase-memory/graph.db.zst` (в `.gitignore`).

**⚠️ Отличие от «Собирателя расшифровок», не переносите реализацию молча:** `_is_accounting_code_vectorized` здесь и `utils/column_utils.is_accounting_code` там считают бухгалтерский счёт **по разным правилам** (здесь длина частей ≤ 2 и `0`/`00`/`000` считаются счётом; там — без ограничения длины и не считаются). Обработка ошибок тоже разная: здесь плоские исключения + `notify`, там `PipelineError` + декоратор `handle_pipeline_errors` с диагностикой в `mismatches/`.