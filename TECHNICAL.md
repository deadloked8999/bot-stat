# Техническая документация

## Архитектура проекта

### Модульная структура

Проект состоит из независимых модулей, каждый из которых отвечает за свою функциональность:

```
bot.py          - Главный модуль, обработка команд и координация
config.py       - Конфигурация и настройки
database.py     - Работа с базой данных SQLite
parser.py       - Парсинг и валидация входных данных
reports.py      - Генерация отчетов и экспорт
utils.py        - Вспомогательные функции (даты, команды)
```

### Диаграмма взаимодействия

```
Telegram API
     ↓
  bot.py (главный обработчик)
     ↓
     ├→ parser.py (парсинг данных)
     ├→ database.py (сохранение/чтение)
     ├→ reports.py (отчеты)
     └→ utils.py (утилиты)
```

---

## Модуль: config.py

### Назначение
Централизованное хранение конфигурации.

### Основные параметры

```python
BOT_TOKEN       # Токен Telegram бота
TIMEZONE        # Временная зона (Europe/Riga)
CLUBS           # Поддерживаемые клубы
CHANNELS        # Типы каналов (нал/безнал)
DATABASE_PATH   # Путь к БД SQLite
```

### Безопасность
Токен загружается из переменной окружения `TELEGRAM_BOT_TOKEN`.

---

## Модуль: database.py

### Назначение
Управление SQLite базой данных.

### Класс Database

#### Методы:

**`init_database()`**
- Создание таблиц при первом запуске
- Создание индексов для оптимизации

**`add_or_update_operation(club, date, code, name, channel, amount, original_line, aggregate)`**
- Добавление или обновление операции
- `aggregate=True`: складывать суммы
- `aggregate=False`: заменять сумму

**`get_operations_by_date(club, date)`**
- Получить все операции за дату

**`get_operations_by_period(club, date_from, date_to)`**
- Получить операции за период

**`update_operation(club, date, code, channel, new_amount)`**
- Исправить сумму существующей операции

**`delete_operation(club, date, code, channel)`**
- Удалить операцию

### Схема базы данных

#### Таблица: operations
```sql
CREATE TABLE operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    club TEXT NOT NULL,                 -- Клуб (Москвич/Анора)
    date TEXT NOT NULL,                 -- Дата (YYYY-MM-DD)
    code TEXT NOT NULL,                 -- Код сотрудника (D1, R7)
    name_snapshot TEXT NOT NULL,        -- Имя (снимок)
    channel TEXT NOT NULL,              -- Канал (нал/безнал)
    amount REAL NOT NULL,               -- Сумма
    original_line TEXT,                 -- Исходная строка
    created_at TEXT NOT NULL,           -- Время создания
    UNIQUE(club, date, code, channel)   -- Уникальность
)
```

#### Таблица: edit_log
```sql
CREATE TABLE edit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    club TEXT NOT NULL,
    date TEXT NOT NULL,
    code TEXT NOT NULL,
    channel TEXT NOT NULL,
    action TEXT NOT NULL,          -- Тип действия
    old_value REAL,                -- Старое значение
    new_value REAL,                -- Новое значение
    edited_at TEXT NOT NULL        -- Время изменения
)
```

#### Индексы
```sql
CREATE INDEX idx_operations_club_date ON operations(club, date);
CREATE INDEX idx_operations_code ON operations(code);
```

---

## Модуль: parser.py

### Назначение
Парсинг и валидация блочного ввода данных.

### Класс DataParser

#### Статические методы:

**`normalize_code(code: str) -> str`**
- Нормализация кода (регистр, кириллица→латиница)
- `Д1` → `D1`, `р7` → `R7`

**`parse_amount(amount_str: str) -> Tuple[bool, float, str]`**
- Парсинг суммы
- Поддержка запятой и точки: `1200,50` или `1200.50`
- Валидация (неотрицательное число, без пробелов)

**`parse_line(line: str, line_number: int) -> Tuple[bool, Dict, str]`**
- Парсинг одной строки
- Формат: `<код> <имя> <сумма>`
- Возвращает данные или ошибку

**`parse_block(text: str) -> Tuple[List[Dict], List[str]]`**
- Парсинг блока данных (множество строк)
- Возвращает: успешные строки и список ошибок

**`format_parse_result(...) -> str`**
- Форматирование результата для вывода пользователю

### Логика парсинга

1. Разбиение по пробелам и табуляции: `re.split(r'\s+', line)`
2. Первый элемент — код
3. Последний элемент — сумма
4. Всё между ними — имя (может быть из нескольких слов)

### Нормализация кодов

Карта замен кириллица → латиница:
```python
{
    'Д': 'D', 'Р': 'R', 'К': 'K', 'О': 'O',
    'Е': 'E', 'Т': 'T', 'В': 'B', 'А': 'A',
    'Н': 'H', 'М': 'M', 'С': 'C', 'Х': 'X'
}
```

---

## Модуль: reports.py

### Назначение
Генерация отчетов и экспорт данных.

### Класс ReportGenerator

#### Статические методы:

**`calculate_report(operations: List[Dict]) -> Tuple`**
- Расчет отчета по операциям
- Группировка по сотрудникам (код)
- Расчет итогов и проверка

Возвращает:
- `report_rows`: строки отчета
- `totals_by_rows`: итоги по строкам
- `totals_recalc`: итоги пересчетом
- `check_ok`: результат проверки

**`format_report_text(...) -> str`**
- Форматирование отчета для Telegram
- Моноширинный шрифт для таблицы

**`generate_csv(...) -> str`**
- Генерация CSV файла

**`generate_xlsx(...) -> str`**
- Генерация XLSX файла с форматированием

### Формулы расчета

```python
# 10% от безнала
minus10 = round(beznal_total * 0.10, 2)

# Итог
itog = round(nal_total + (beznal_total - minus10), 2)
```

### Проверка совпадения

Сравниваются:
1. Итоги по строкам отчета (сумма по сотрудникам)
2. Итоги пересчетом из первичных операций

Если расхождение — выдается предупреждение ❗

---

## Модуль: utils.py

### Назначение
Вспомогательные функции для работы с датами и командами.

### Функции:

**`get_current_date(timezone_str) -> str`**
- Текущая дата в формате YYYY-MM-DD
- С учетом временной зоны

**`parse_date(date_str) -> Tuple[bool, str, str]`**
- Парсинг даты из строки
- Валидация формата YYYY-MM-DD

**`get_week_range(reference_date) -> Tuple[str, str]`**
- Диапазон недели (понедельник - воскресенье)

**`parse_period(period_str) -> Tuple`**
- Парсинг периода из строки
- Поддержка: `"2025-11-03..2025-11-09"`, `"неделя"`

**`normalize_command(text) -> str`**
- Нормализация команды (регистр, пробелы)

**`extract_club_from_text(text) -> Optional[str]`**
- Извлечение клуба из текста

**`format_operations_list(operations, date, club) -> str`**
- Форматирование списка операций для вывода

---

## Модуль: bot.py

### Назначение
Главный модуль бота, обработка команд и координация.

### Класс UserState

Хранит состояние пользователя:
```python
club: Optional[str]          # Активный клуб
mode: Optional[str]          # Режим ввода ('нал', 'безнал', None)
temp_data: list              # Временные данные блочного ввода
current_date: str            # Текущая дата
```

### Глобальные переменные

**`USER_STATES`**: словарь состояний пользователей
- Ключ: `user_id`
- Значение: объект `UserState`

**`db`**: экземпляр `Database`

### Обработчики команд

**`start_command(update, context)`**
- Обработка `/старт москвич` или `/старт анора`
- Установка активного клуба

**`handle_message(update, context)`**
- Главный обработчик текстовых сообщений
- Роутинг к специфическим обработчикам

**`handle_save_command(update, context, state)`**
- Сохранение данных в БД
- Команда: `дата/записать`

**`handle_report_command(update, context, state, text)`**
- Генерация и вывод отчета
- Команда: `прошу отчёт`

**`handle_list_command(...)`**
- Просмотр записей за дату
- Команда: `список`

**`handle_edit_command(...)`**
- Редактирование записи
- Команда: `исправить`

**`handle_delete_command(...)`**
- Удаление записи
- Команда: `удалить`

**`handle_export_command(...)`**
- Экспорт отчета в XLSX
- Команда: `экспорт`

### Поток данных

#### Ввод данных:
```
1. Пользователь: /старт москвич
   → Установка state.club = "Москвич"

2. Пользователь: нал
   → Установка state.mode = "нал"
   → Очистка state.temp_data

3. Пользователь вставляет данные
   → Накопление в state.temp_data

4. Пользователь: готово
   → Парсинг state.temp_data (parser.parse_block)
   → Сохранение в context.user_data['ready_нал']
   → Сброс state.mode

5. Пользователь: дата/записать
   → Чтение context.user_data['ready_нал']
   → Запись в БД (db.add_or_update_operation)
   → Очистка context.user_data
```

#### Генерация отчета:
```
1. Пользователь: прошу отчёт
   → Определение клуба и периода
   
2. Чтение операций из БД
   → db.get_operations_by_period(club, date_from, date_to)
   
3. Расчет отчета
   → ReportGenerator.calculate_report(operations)
   
4. Форматирование и вывод
   → ReportGenerator.format_report_text(...)
```

---

## Обработка ошибок

### Валидация на уровне парсера
- Проверка формата строки (3 элемента)
- Проверка формата суммы
- Проверка неотрицательности

### Валидация на уровне команд
- Проверка выбранного клуба
- Проверка формата даты
- Проверка существования записей

### Сообщения об ошибках
Все ошибки сопровождаются:
- Символом ❌
- Кратким описанием проблемы
- Подсказкой по правильному формату

---

## Производительность

### Индексы БД
- `(club, date)` — быстрый поиск по клубу и дате
- `(code)` — быстрый поиск по коду сотрудника

### Оптимизация памяти
- Блочный ввод накапливается в памяти до команды `готово`
- После парсинга данные сохраняются компактно

### Масштабируемость
- SQLite поддерживает миллионы записей
- Отчеты генерируются только по запрошенному периоду
- Нет глобальных блокировок

---

## Безопасность

### Токен бота
- Хранится в переменной окружения
- Не коммитится в репозиторий

### База данных
- Локальное хранение (SQLite)
- Нет сетевого доступа
- Автоматическое резервное копирование не требуется

### Валидация входных данных
- Все входные данные валидируются
- SQL-инъекции невозможны (параметризованные запросы)

---

## Расширение функциональности

### Добавление нового клуба

1. Добавить в `config.py`:
```python
CLUBS = {
    'москвич': 'Москвич',
    'анора': 'Анора',
    'новый': 'Новый Клуб'
}
```

2. Обновить `utils.py` → `extract_club_from_text()`:
```python
if 'новый' in text_lower:
    return 'Новый Клуб'
```

### Добавление нового канала

1. Добавить в `config.py`:
```python
CHANNELS = {
    'нал': 'нал',
    'безнал': 'безнал',
    'карта': 'карта'
}
```

2. Добавить обработчик в `bot.py` → `handle_message()`:
```python
if text_lower == 'карта':
    state.mode = 'карта'
    ...
```

### Добавление нового типа отчета

1. Добавить метод в `reports.py`:
```python
@staticmethod
def generate_custom_report(operations):
    # Ваша логика
    pass
```

2. Добавить команду в `bot.py`:
```python
if 'мой отчет' in text_lower:
    await handle_custom_report(...)
```

---

## Тестирование

### Ручное тестирование
1. Используйте файл `example_data.txt` для тестовых данных
2. Проверьте все команды из `COMMANDS.md`

### Автоматизированное тестирование
Для добавления юнит-тестов создайте файл `tests.py`:

```python
import unittest
from parser import DataParser

class TestParser(unittest.TestCase):
    def test_parse_line(self):
        success, data, error = DataParser.parse_line("Д1 Жанна 2200", 1)
        self.assertTrue(success)
        self.assertEqual(data['code'], 'D1')
        self.assertEqual(data['amount'], 2200.0)

if __name__ == '__main__':
    unittest.main()
```

---

## Развертывание

### Локальное развертывание
```bash
python bot.py
```

### Развертывание на сервере (Linux)

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Создайте systemd service `/etc/systemd/system/statbot.service`:
```ini
[Unit]
Description=Telegram Stat Bot
After=network.target

[Service]
Type=simple
User=botuser
WorkingDirectory=/path/to/bot
Environment="TELEGRAM_BOT_TOKEN=ваш_токен"
ExecStart=/usr/bin/python3 /path/to/bot/bot.py
Restart=always

[Install]
WantedBy=multi-user.target
```

3. Запустите сервис:
```bash
sudo systemctl start statbot
sudo systemctl enable statbot
```

### Развертывание с Docker

Создайте `Dockerfile`:
```dockerfile
FROM python:3.10-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "bot.py"]
```

Запуск:
```bash
docker build -t statbot .
docker run -e TELEGRAM_BOT_TOKEN=ваш_токен statbot
```

---

## Мониторинг и логи

### Логирование
Добавьте в начало `bot.py`:
```python
import logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
```

### Мониторинг работы
- Проверяйте файл `bot.log`
- Мониторьте размер `bot_data.db`
- Отслеживайте использование памяти

---

## Резервное копирование

### Автоматическое резервное копирование БД

Создайте скрипт `backup.py`:
```python
import shutil
from datetime import datetime

db_file = 'bot_data.db'
backup_file = f'backups/bot_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'

shutil.copy2(db_file, backup_file)
print(f"Backup created: {backup_file}")
```

Запускайте через cron (Linux) или Task Scheduler (Windows).

---

## FAQ для разработчиков

**Q: Как добавить новое поле в операцию?**

A: 
1. Обновите схему БД в `database.py` → `init_database()`
2. Добавьте миграцию или пересоздайте БД
3. Обновите методы работы с БД

**Q: Как изменить формулу расчета в отчете?**

A: Измените метод `calculate_report()` в `reports.py`

**Q: Как сделать бота многопользовательским?**

A: Бот уже многопользовательский! Состояние хранится отдельно для каждого `user_id`.

**Q: Можно ли ограничить доступ к боту?**

A: Да, добавьте проверку в `bot.py`:
```python
ALLOWED_USERS = [123456789, 987654321]  # ID пользователей

async def start_command(update, context):
    if update.effective_user.id not in ALLOWED_USERS:
        await update.message.reply_text("Доступ запрещен")
        return
    # остальной код
```

