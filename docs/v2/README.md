# TradingSystem v2: ядро и подключение QUIK

Разрабатывается управляемая офлайн-симуляция. Работает цепочка: завершённая свеча → стратегия → намерение → подтверждение/исполнение → позиция → закрытая сделка → эквити. Во второй итерации добавлены лимитная цена, отмена, замена и гонки исполнения с отменой. Третья добавляет надёжный журнал, SQLite-проекцию, восстановление учёта и запрет торговли до сверки. Четвёртая — pause/resume, checkpoints стратегии и `OPTIMIZING` с отдельным CPU-процессом. Пятая — общая история завершённых свечей, прогрев, synthetic/correction и первоначальный одиночный бэктест. Шестая — промежуточная проверка, исправления очередей/хранения и первый планировщик капитала по снимкам счетов. Седьмая — допуски маршрутов, атомарные резервы OMS и их восстановление. Восьмая связывает снимки источника и текущую занятость OMS с пересчётом бюджетов. Девятая добавляет сверку внешних позиций/заявок, ручного владельца, частичную передачу и лимитное закрытие без фиктивных fills. Десятая — групповые планы, атомарный допуск, три политики сбоя ноги и локальная защита совокупного исполненного P&L. Одиннадцатая — ожидание и пропорциональное сокращение внешнего дефицита с отменами, ограниченными повторными попытками и восстановлением. Двенадцатая добавляет TP/trailing, сохранение максимума P&L и изменение параметров защиты открытого плана. Тринадцатая защищает одиночные входы и существующие обычные/manual-позиции, завершая базовую офлайн-часть этапа 6. Четырнадцатая начинает этап 7: строгий контракт QUIK, replay брокерских callbacks и блокировка неопределённых исходов. Пятнадцатая сохраняет идентификаторы и callbacks в отдельном журнале шлюза, восстанавливает доставку в OMS без повторной подготовки заявок. Шестнадцатая добавляет ограниченный read-only транспорт, два диагностических чтения таблиц и команду проверки демо-терминала. Семнадцатая — чтение финансовых параметров фьючерса, проверка единиц и отдельный отчёт инструмента.

Документы разделены по назначению:

- [Итоговая проверка этапа 6](stage-06-review.md): завершённая офлайн-часть защиты и оставшиеся требования перед QUIK/приёмкой.
- [Промежуточная проверка](midpoint-review.md): соответствие всем 32 use-case, найденные дефекты и оставшиеся границы.
- [Согласованный реестр use-case](use-cases.md): 32 сценария и закрытые решения Q1–Q4.
- [Архитектурный эскиз](architecture.md): компоненты, владельцы состояния, принятые правила.
- [План XP/TDD](development-plan.md): последовательность итераций и приёмочные сценарии.
- [Результат первой итерации](iteration-01.md): реализованное поведение, проверки и границы.
- [Результат второй итерации](iteration-02.md): контракт отмены, объём замены, проверки гонок.
- [Результат третьей итерации](iteration-03.md): журнал, восстановление, сверка и ограничения.
- [Результат четвёртой итерации](iteration-04.md): lifecycle, состояние и оптимизация.
- [Результат пятой итерации](iteration-05.md): данные, next-bar исполнение и одиночное исследование.
- [Результат шестой итерации](iteration-06.md): счета, модифицированные веса, FX и план капитала.
- [Результат седьмой итерации](iteration-07.md): допуски, резервы submit/replace/fill/cancel, лимиты и восстановление.
- [Результат восьмой итерации](iteration-08.md): источник снимков, пересчёт бюджетов и защита от запоздавших обновлений.
- [Результат девятой итерации](iteration-09.md): внешний учёт, ручная стратегия, передача владения и восстановление.
- [Результат десятой итерации](iteration-10.md): планы корзин, защита/достроение/закрытие, alert/pause и восстановление.
- [Результат одиннадцатой итерации](iteration-11.md): внешний дефицит, ожидание, пропорциональное сокращение и повтор после отказа.
- [Результат двенадцатой итерации](iteration-12.md): TP/trailing, максимум P&L, управление параметрами и совместимость журнала.
- [Результат тринадцатой итерации](iteration-13.md): одиночные/manual-позиции, явное снятие защиты, частичная передача и восстановление.
- [Результат четырнадцатой итерации](iteration-14.md): QUIK-контракт, ранние/поздние callbacks и таймаут без потери резерва.
- [Результат пятнадцатой итерации](iteration-15.md): durable QUIK, восстановление идентификаторов и доставки callbacks.
- [Результат шестнадцатой итерации](iteration-16.md): read-only TCP, Lua-защита, снимки и диагностика.
- [Результат семнадцатой итерации](iteration-17.md): денежная стоимость шага, поля ГО, Decimal и повторяемость параметров.
- [Проверка финансовых параметров SiZ6](quik-financial-check.md): обновление Lua и запуск --financial.
- [Подготовка Windows и QUIK](quik-environment.md): подтверждённые контуры demo/ПСБ и параметры среды.
- [Результат проверки демо QUIK](quik-demo-validation.md): подтверждённые версии, таблицы и справочник SiZ6; границы проверок 15–16 октября.
- [Первое подключение демо QUIK](quik-readonly-check.md): пошаговый запуск Lua bridge и команды диагностики.

## Запуск примеров и тестов

Нужен Python 3.12 или новее. Ядро и Python-клиент используют стандартную библиотеку. Полный набор тестов требует dev-зависимостей, включая Lupa 2.6 для проверки Lua 5.4: предварительно установите `python -m pip install -e ".[dev]"` в своём виртуальном окружении. Команды выполняются из корня репозитория и тем же Python. Исходный прототип в `core/` и `gui/` имеет собственные зависимости и не участвует в новом запуске.

Linux/macOS:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -m unittest discover -s tests_v2 -v
.venv/bin/python examples/v2_round_trip.py
.venv/bin/python examples/v2_cancel_replace.py
.venv/bin/python examples/v2_recovery.py
.venv/bin/python examples/v2_optimization.py
.venv/bin/python examples/v2_backtest.py
.venv/bin/python examples/v2_capital.py
.venv/bin/python examples/v2_reservations.py
.venv/bin/python examples/v2_rebalance.py
.venv/bin/python examples/v2_manual.py
.venv/bin/python examples/v2_plans.py
.venv/bin/python examples/v2_deficit.py
.venv/bin/python examples/v2_protection.py
.venv/bin/python examples/v2_position_protection.py
.venv/bin/python examples/v2_quik_contract.py
.venv/bin/python examples/v2_quik_recovery.py
.venv/bin/python examples/v2_quik_readonly.py
.venv/bin/python examples/v2_quik_parameters.py
```

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m unittest discover -s tests_v2 -v
.\.venv\Scripts\python.exe examples/v2_round_trip.py
.\.venv\Scripts\python.exe examples/v2_cancel_replace.py
.\.venv\Scripts\python.exe examples/v2_recovery.py
.\.venv\Scripts\python.exe examples/v2_optimization.py
.\.venv\Scripts\python.exe examples/v2_backtest.py
.\.venv\Scripts\python.exe examples/v2_capital.py
.\.venv\Scripts\python.exe examples/v2_reservations.py
.\.venv\Scripts\python.exe examples/v2_rebalance.py
.\.venv\Scripts\python.exe examples/v2_manual.py
.\.venv\Scripts\python.exe examples/v2_plans.py
.\.venv\Scripts\python.exe examples/v2_deficit.py
.\.venv\Scripts\python.exe examples/v2_protection.py
.\.venv\Scripts\python.exe examples/v2_position_protection.py
.\.venv\Scripts\python.exe examples/v2_quik_contract.py
.\.venv\Scripts\python.exe examples/v2_quik_recovery.py
.\.venv\Scripts\python.exe examples/v2_quik_readonly.py
.\.venv\Scripts\python.exe examples/v2_quik_parameters.py
```

Демонстрация покупает 2 контракта по 100 и закрывает по 110, с комиссией 1 за каждое исполнение. Ожидаемые результаты: позиция 0, одна закрытая сделка, P&L и эквити 18 RUB. Символ TEST и счёт demo-account вымышленные.

В `v2_cancel_replace.py` целевой объём покупки равен 5. Исполняются 2 лота и ещё 1 во время отмены; замена получает остаток 2 по новой лимитной цене 99. После выхода по 110 закрытый P&L равен 48 RUB с учётом четырёх комиссий по 1.

`v2_recovery.py` использует временный каталог: сохраняет покупку, перезапускает runtime, проверяет повтор fill и закрывает позицию после независимой сверки. Результат: сохранённый ID, позиция 0, P&L и эквити 18 RUB. Для собственного офлайн-сценария передайте `DurableJournal(Path(...))` в `TradingRuntime(..., journal=journal)`; держите контекст журнала открытым до завершения runtime. Рабочие журналы и базы не добавляйте в git.

`v2_optimization.py` показывает `OPTIMIZING`, работу соседней стратегии, fill во время расчёта, отдельный worker и восстановление параметров/состояния. Для собственных стратегий доступны `save_state`, `restore_state`, `state_version`, `apply_parameters`, `on_restore`, `context.status` и `context.start_optimization`. Ожидание handle выполняется вне callbacks. Полный контракт и границы описаны в отчёте итерации 4.

`v2_backtest.py` запускает ту же торговую логику через обычные OMS/ledger на четырёх барах, включая один синтетический. Лимитные исполнения 100/110 и две комиссии по 1 дают 18 RUB; повторный запуск идентичен. API: `context.load_history`, `on_correction`, `accepts_candle`, `fill_gaps`, `BacktestConfig` и `SingleBacktest`. Модель одного инструмента/таймфрейма не исполняет заявки на полученном баре, не торгует по синтетическим ценам и показывает оставшиеся позиции/заявки. Контракты и ограничения — в отчёте итерации 5.

`v2_capital.py` рассчитывает план по двум счетам с резервом и FX: сохраняет 30 RUB занятого ресурса ручной стратегии, выделяет 360 RUB с пропорцией ног 2:1 и показывает остаток 10 RUB. После уменьшения средств показывает дефицит 20 RUB без изменения позиций. Сам план не резервирует ёмкость в OMS. В итерации 7 появился отдельный слой допуска по явной политике оператора.

`v2_reservations.py` связывает план одного RUB-счёта с контролем OMS: replace удерживает 200 RUB, конкурирующая заявка отклоняется, fill переносит 100 RUB в позицию. Перезапуск сохраняет резерв и требует сверки вместе с новой политикой; ограниченный выход после уменьшения средств устраняет дефицит 100 RUB. Для этого режима используйте `require_capital=True` и `runtime.configure_capital`; контракты оценки и ограничения описаны в итерации 7.

`v2_rebalance.py` получает полный пакет счетов/оценок и формирует бюджеты из текущих позиций и заявок OMS. Внешняя занятость 200 RUB учитывается один раз; смена весов сохраняет 500 RUB первой стратегии и выделяет второй 300. После restart следующий пакет показывает дефицит 100 без автоматического закрытия. API: `runtime.rebalance_capital(source, targets)`; детали источника, TTL и конкурентных обновлений — в итерации 8.

`v2_manual.py` импортирует внешнюю частично исполненную заявку, отменяет остаток, передаёт половину позиции из manual другой стратегии и закрывает обе позиции реальными fills. P&L после импорта — 38 RUB; restart сохраняет владение/историю и требует сверки позиций. API и учёт неполных по владению циклов — в итерации 9.

`v2_plans.py` исполняет три объявленные политики отказа ноги: protect, complete и close. Полный план проходит допуск до отправки первой заявки; восстановление ждёт барьеров отмены и учитывает поздний fill. Три истории дают −22, +27 и −43 RUB, затем проверяют restart. Локальный стоп требует явных свежих PlanMarks, таймауты — check_plan_deadlines; детали и ограничения в итерации 10.

`v2_deficit.py` показывает дефицит 200 RUB, ожидание, выход 3:2 из корзины 6:4 на двух счетах, отказ второй ноги, явный повтор только остатка и восстановление пяти заявок без повторной отправки. Контракт, округление по лотам и ограничения — в итерации 11.

`v2_protection.py` показывает TP и trailing с комиссиями, изменением дистанции и двумя перезапусками. TP срабатывает при 20 RUB и закрывает сделку с 18 RUB; trailing сохраняет максимум 50, срабатывает при 35 и завершает цикл с 33 RUB. Настройки, максимум и причина восстанавливаются без повторных заявок. Контракт — в итерации 12.

`v2_position_protection.py` защищает импортированный остаток, снимает защиту для передачи 1 из 4 лотов и подключает новую к остатку. Два перезапуска сохраняют максимум, владение и две выходные заявки; результаты владельцев 23 и 5 RUB дают 28 RUB общей закрытой эквити. Контракт — в итерации 13.

`v2_quik_contract.py` воспроизводит синтетические OnTransReply/OnOrder/OnTrade, ранний fill и поздний fill после отмены. Результат 18 RUB, затем таймаут сохраняет заявку и резерв 150 RUB; OMS restart не повторяет отправку. Реальной сети нет.

`v2_quik_recovery.py` восстанавливает сделку, сохранённую шлюзом до потери доставки в OMS. Два перезапуска сохраняют 1 лот, комиссию 1.25 RUB, резерв 450 RUB и следующий TRANS_ID 2; новых подготовок при restart нет. Используются отдельные временные журналы OMS и QUIK.

`v2_quik_readonly.py` запускает синтетический TCP-терминал на локальных случайных портах, проверяет два чтения таблиц и сохраняет временный диагностический журнал. Реальное подключение запускается только явной командой `python -m trading_system.quik_probe` по [отдельной инструкции](quik-readonly-check.md); примеры к брокеру не подключаются.

`v2_quik_parameters.py` показывает различие lot_size=1000 и стоимости пункта 1, недоступное ГО без подстановки нуля и изменившееся значение. Все суммы ГО в нём вымышленные. Для фактической проверки используется [режим --financial](quik-financial-check.md) с обновлённым Lua-wrapper.

## Среда для разработки

Достаточно одного репозитория. Новое ядро — `src/trading_system`, проверки — `tests_v2`, примеры — `examples`. Первые три PR (две итерации и реестр use-case) объединены пользователем в свои целевые ветки. Третья итерация развивается в `rewrite/v2-recovery` от `rewrite/v2-orders`, где уже находится согласованный реестр. Четвёртая итерация — отдельная ветка `rewrite/v2-lifecycle` от `rewrite/v2-recovery`. Пятая — `rewrite/v2-backtest` от `rewrite/v2-lifecycle`. Шестая — `rewrite/v2-capital` от `rewrite/v2-backtest`, с отдельными коммитами проверки и планирования. Седьмая — `rewrite/v2-reservations` от `rewrite/v2-capital`. Восьмая — `rewrite/v2-rebalance` от `rewrite/v2-reservations`. Девятая — `rewrite/v2-manual` от `rewrite/v2-rebalance`. Десятая — `rewrite/v2-plans` от `rewrite/v2-manual`. Одиннадцатая — `rewrite/v2-deficit` от `rewrite/v2-plans`. Двенадцатая — `rewrite/v2-protection` от `rewrite/v2-deficit`. Тринадцатая — `rewrite/v2-position-protection` от `rewrite/v2-protection`. Четырнадцатая — `rewrite/v2-quik-contract` от `rewrite/v2-position-protection`. Пятнадцатая — `rewrite/v2-quik-durability` от `rewrite/v2-quik-contract`. Шестнадцатая — `rewrite/v2-quik-readonly` от `rewrite/v2-quik-durability`. Семнадцатая — `rewrite/v2-quik-parameters` от `rewrite/v2-quik-readonly`. Перенос всей цепочки в `main` остаётся отдельным решением пользователя.

```sh
python3 -m venv .venv
# Linux/macOS: source .venv/bin/activate
# PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e '.[dev]'
python -m unittest discover -s tests_v2 -v
python -m mypy
python examples/v2_round_trip.py
python examples/v2_cancel_replace.py
python examples/v2_recovery.py
python examples/v2_optimization.py
python examples/v2_backtest.py
python examples/v2_capital.py
python examples/v2_reservations.py
python examples/v2_rebalance.py
python examples/v2_manual.py
python examples/v2_plans.py
python examples/v2_deficit.py
python examples/v2_protection.py
python examples/v2_position_protection.py
python examples/v2_quik_contract.py
python examples/v2_quik_recovery.py
python examples/v2_quik_readonly.py
python examples/v2_quik_parameters.py
```

CI проверяет новое ядро на Linux и Windows с Python 3.12: установку пакета, поведенческие тесты, типы и демонстрацию. Старые тесты остаются рядом как материал для сравнения; CI v2 не заявляет о совместимости нового API с прототипом.

Для replay-примеров QUIK и дополнительные репозитории не нужны. [Подготовка Windows и QUIK](quik-environment.md) описывает следующий интеграционный контур; текущий шлюз не отправляет реальные заявки.
