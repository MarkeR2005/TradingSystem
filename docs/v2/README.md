# TradingSystem v2: начало разработки

Первая итерация — управляемая офлайн-симуляция. Работает цепочка: завершённая свеча → стратегия → намерение → подтверждение/исполнение → позиция → закрытая сделка → эквити.

Документы разделены по назначению:

- [Архитектурный эскиз](architecture.md): компоненты, владельцы состояния, принятые правила.
- [План XP/TDD](development-plan.md): последовательность итераций и приёмочные сценарии.
- [Результат первой итерации](iteration-01.md): реализованное поведение, проверки и границы.

## Запуск без дополнительных библиотек

Нужен Python 3.12 или новее. Команды выполняются из корня репозитория. Исходный прототип в `core/` и `gui/` имеет собственные зависимости и не участвует в новом запуске.

Linux/macOS:

```sh
PYTHONPATH=src python3 -m unittest discover -s tests_v2 -v
PYTHONPATH=src python3 examples/v2_round_trip.py
```

Windows PowerShell:

```powershell
$env:PYTHONPATH = 'src'
py -3.12 -m unittest discover -s tests_v2 -v
py -3.12 examples/v2_round_trip.py
```

Демонстрация покупает 2 контракта по 100 и закрывает по 110, с комиссией 1 за каждое исполнение. Ожидаемые результаты: позиция 0, одна закрытая сделка, P&L и эквити 18 RUB. Символ TEST и счёт demo-account вымышленные.

## Среда для разработки

Достаточно одного репозитория. Новое ядро — `src/trading_system`, проверки — `tests_v2`, примеры — `examples`. Изменения рассматриваются в PR из `rewrite/v2` в `main`.

```sh
python3 -m venv .venv
# Linux/macOS: source .venv/bin/activate
# PowerShell: .venv\Scripts\Activate.ps1
python -m pip install -e '.[dev]'
python -m unittest discover -s tests_v2 -v
python -m mypy
python examples/v2_round_trip.py
```

CI проверяет новое ядро на Linux и Windows с Python 3.12: установку пакета, поведенческие тесты, типы и демонстрацию. Старые тесты остаются рядом как материал для сравнения; CI v2 не заявляет о совместимости нового API с прототипом.

Для этой итерации QUIK, брокерские ключи и дополнительные репозитории не нужны. Установка QUIK bridge и интеграционная среда Windows будут описаны перед этапом 7.
