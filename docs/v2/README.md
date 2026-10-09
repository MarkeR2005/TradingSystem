# TradingSystem v2: офлайн-ядро

Разрабатывается управляемая офлайн-симуляция. Работает цепочка: завершённая свеча → стратегия → намерение → подтверждение/исполнение → позиция → закрытая сделка → эквити. Во второй итерации добавлены лимитная цена, отмена, замена и гонки исполнения с отменой. Третья добавляет надёжный журнал, SQLite-проекцию, восстановление учёта и запрет торговли до сверки. Четвёртая — pause/resume, checkpoints стратегии и `OPTIMIZING` с отдельным CPU-процессом. Пятая — общая история завершённых свечей, прогрев, synthetic/correction и первоначальный одиночный бэктест.

Документы разделены по назначению:

- [Согласованный реестр use-case](use-cases.md): 32 сценария и закрытые решения Q1–Q4.
- [Архитектурный эскиз](architecture.md): компоненты, владельцы состояния, принятые правила.
- [План XP/TDD](development-plan.md): последовательность итераций и приёмочные сценарии.
- [Результат первой итерации](iteration-01.md): реализованное поведение, проверки и границы.
- [Результат второй итерации](iteration-02.md): контракт отмены, объём замены, проверки гонок.
- [Результат третьей итерации](iteration-03.md): журнал, восстановление, сверка и ограничения.
- [Результат четвёртой итерации](iteration-04.md): lifecycle, состояние и оптимизация.
- [Результат пятой итерации](iteration-05.md): данные, next-bar исполнение и одиночное исследование.

## Запуск без дополнительных библиотек

Нужен Python 3.12 или новее. Команды выполняются из корня репозитория. Исходный прототип в `core/` и `gui/` имеет собственные зависимости и не участвует в новом запуске.

Linux/macOS:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests_v2 -v
PYTHONPATH=src python3 examples/v2_round_trip.py
PYTHONPATH=src python3 examples/v2_cancel_replace.py
PYTHONPATH=src python3 examples/v2_recovery.py
PYTHONPATH=src python3 examples/v2_optimization.py
PYTHONPATH=src python3 examples/v2_backtest.py
```

Windows PowerShell:

```powershell
$env:PYTHONPATH = 'src'
py -3.12 -m unittest discover -s tests_v2 -v
py -3.12 examples/v2_round_trip.py
py -3.12 examples/v2_cancel_replace.py
py -3.12 examples/v2_recovery.py
py -3.12 examples/v2_optimization.py
py -3.12 examples/v2_backtest.py
```

Демонстрация покупает 2 контракта по 100 и закрывает по 110, с комиссией 1 за каждое исполнение. Ожидаемые результаты: позиция 0, одна закрытая сделка, P&L и эквити 18 RUB. Символ TEST и счёт demo-account вымышленные.

В `v2_cancel_replace.py` целевой объём покупки равен 5. Исполняются 2 лота и ещё 1 во время отмены; замена получает остаток 2 по новой лимитной цене 99. После выхода по 110 закрытый P&L равен 48 RUB с учётом четырёх комиссий по 1.

`v2_recovery.py` использует временный каталог: сохраняет покупку, перезапускает runtime, проверяет повтор fill и закрывает позицию после независимой сверки. Результат: сохранённый ID, позиция 0, P&L и эквити 18 RUB. Для собственного офлайн-сценария передайте `DurableJournal(Path(...))` в `TradingRuntime(..., journal=journal)`; держите контекст журнала открытым до завершения runtime. Рабочие журналы и базы не добавляйте в git.

`v2_optimization.py` показывает `OPTIMIZING`, работу соседней стратегии, fill во время расчёта, отдельный worker и восстановление параметров/состояния. Для собственных стратегий доступны `save_state`, `restore_state`, `state_version`, `apply_parameters`, `on_restore`, `context.status` и `context.start_optimization`. Ожидание handle выполняется вне callbacks. Полный контракт и границы описаны в отчёте итерации 4.

`v2_backtest.py` запускает ту же торговую логику через обычные OMS/ledger на четырёх барах, включая один синтетический. Лимитные исполнения 100/110 и две комиссии по 1 дают 18 RUB; повторный запуск идентичен. API: `context.load_history`, `on_correction`, `accepts_candle`, `fill_gaps`, `BacktestConfig` и `SingleBacktest`. Модель одного инструмента/таймфрейма не исполняет заявки на полученном баре, не торгует по синтетическим ценам и показывает оставшиеся позиции/заявки. Контракты и ограничения — в отчёте итерации 5.

## Среда для разработки

Достаточно одного репозитория. Новое ядро — `src/trading_system`, проверки — `tests_v2`, примеры — `examples`. Первые три PR (две итерации и реестр use-case) объединены пользователем в свои целевые ветки. Третья итерация развивается в `rewrite/v2-recovery` от `rewrite/v2-orders`, где уже находится согласованный реестр. Четвёртая итерация — отдельная ветка `rewrite/v2-lifecycle` от `rewrite/v2-recovery`. Пятая — `rewrite/v2-backtest` от `rewrite/v2-lifecycle`. Перенос всей цепочки в `main` остаётся отдельным решением пользователя.

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
```

CI проверяет новое ядро на Linux и Windows с Python 3.12: установку пакета, поведенческие тесты, типы и демонстрацию. Старые тесты остаются рядом как материал для сравнения; CI v2 не заявляет о совместимости нового API с прототипом.

Для этой итерации QUIK, брокерские ключи и дополнительные репозитории не нужны. Установка QUIK bridge и интеграционная среда Windows будут описаны перед этапом 7.
