"""
Утилиты для работы с датами и командами
"""
from datetime import datetime, timedelta
import pytz
import re
from typing import Tuple, Optional
import config


def get_current_date(timezone_str: str = config.TIMEZONE) -> str:
    """Получить текущую дату в формате YYYY-MM-DD"""
    tz = pytz.timezone(timezone_str)
    return datetime.now(tz).strftime('%Y-%m-%d')


def parse_short_date(date_str: str, timezone_str: str = config.TIMEZONE) -> Tuple[bool, Optional[str], str]:
    """
    Парсинг короткого формата даты: 30,10 или 30.10 или 3,10 -> 2025-10-30
    Возвращает: (успех, дата, сообщение об ошибке)
    """
    date_str = date_str.strip().replace(',', '.')
    
    try:
        # Получаем текущий год
        tz = pytz.timezone(timezone_str)
        current_year = datetime.now(tz).year
        
        # Разбиваем на день и месяц
        parts = date_str.split('.')
        if len(parts) == 2:
            day = int(parts[0])
            month = int(parts[1])
            
            # Валидация
            if month < 1 or month > 12:
                return False, None, f"Неверный месяц: {month}. Должен быть от 1 до 12"
            if day < 1 or day > 31:
                return False, None, f"Неверный день: {day}. Должен быть от 1 до 31"
            
            # Формируем дату
            date_obj = datetime(current_year, month, day)
            return True, date_obj.strftime('%Y-%m-%d'), ""
        else:
            return False, None, f"Неверный формат даты: '{date_str}'. Используйте формат: 30,10 или 3,10"
    
    except ValueError as e:
        return False, None, f"Ошибка парсинга даты: {e}"


def parse_date_range(range_str: str, timezone_str: str = config.TIMEZONE) -> Tuple[bool, str, str, str]:
    """
    Парсинг диапазона дат: 30,10-1,11 -> (2025-10-30, 2025-11-01)
    Возвращает: (успех, дата_от, дата_до, сообщение об ошибке)
    """
    range_str = range_str.strip()
    
    if '-' not in range_str:
        return False, "", "", f"Неверный формат диапазона: '{range_str}'. Используйте формат: 30,10-1,11"
    
    parts = range_str.split('-')
    if len(parts) != 2:
        return False, "", "", f"Неверный формат диапазона: '{range_str}'. Используйте формат: 30,10-1,11"
    
    # Парсим начальную дату
    success1, date_from, error1 = parse_short_date(parts[0], timezone_str)
    if not success1:
        return False, "", "", error1
    
    # Парсим конечную дату
    success2, date_to, error2 = parse_short_date(parts[1], timezone_str)
    if not success2:
        return False, "", "", error2
    
    return True, date_from, date_to, ""


def parse_date(date_str: str) -> Tuple[bool, Optional[str], str]:
    """
    Парсинг даты в формате YYYY-MM-DD
    Возвращает: (успех, дата, сообщение об ошибке)
    """
    try:
        parsed = datetime.strptime(date_str, '%Y-%m-%d')
        return True, parsed.strftime('%Y-%m-%d'), ""
    except ValueError:
        return False, None, f"Неверный формат даты: '{date_str}'. Используйте ГГГГ-ММ-ДД (например, 2025-11-03)"


def get_week_range(reference_date: Optional[str] = None, timezone_str: str = config.TIMEZONE) -> Tuple[str, str]:
    """
    Получить диапазон недели (понедельник - воскресенье)
    Если reference_date не указан, используется текущая дата
    """
    tz = pytz.timezone(timezone_str)
    
    if reference_date:
        try:
            dt = datetime.strptime(reference_date, '%Y-%m-%d')
        except ValueError:
            dt = datetime.now(tz)
    else:
        dt = datetime.now(tz)
    
    # Находим понедельник текущей недели
    monday = dt - timedelta(days=dt.weekday())
    # Воскресенье
    sunday = monday + timedelta(days=6)
    
    return monday.strftime('%Y-%m-%d'), sunday.strftime('%Y-%m-%d')


def parse_period(period_str: str) -> Tuple[bool, str, str, str]:
    """
    Парсинг периода из строки
    Примеры:
    - "2025-11-03..2025-11-09"
    - "неделя" (текущая неделя)
    
    Возвращает: (успех, дата_от, дата_до, сообщение об ошибке)
    """
    period_str = period_str.strip().lower()
    
    # Проверка на явный диапазон
    if '..' in period_str:
        parts = period_str.split('..')
        if len(parts) == 2:
            success1, date1, err1 = parse_date(parts[0].strip())
            success2, date2, err2 = parse_date(parts[1].strip())
            
            if success1 and success2:
                return True, date1, date2, ""
            else:
                return False, "", "", f"Ошибка в диапазоне: {err1 or err2}"
    
    # "неделя" - текущая неделя
    if 'неделя' in period_str or 'недел' in period_str:
        date_from, date_to = get_week_range()
        return True, date_from, date_to, ""
    
    return False, "", "", f"Неверный формат периода: '{period_str}'"


def normalize_command(text: str) -> str:
    """
    Нормализация команды: удаление лишних пробелов, приведение к нижнему регистру, ё→е
    """
    # Заменяем ё на е
    text = text.replace('ё', 'е').replace('Ё', 'Е')
    return ' '.join(text.strip().lower().split())


def parse_command_parts(text: str) -> list:
    """
    Разбор команды на части
    """
    return normalize_command(text).split()


def extract_club_from_text(text: str) -> Optional[str]:
    """
    Извлечение клуба из текста команды
    """
    text_lower = text.lower()
    
    if 'москвич' in text_lower:
        return 'Москвич'
    elif 'анора' in text_lower or 'anora' in text_lower:
        return 'Анора'
    
    return None


def format_operations_list(operations: list, date: str, club: str) -> str:
    """
    Форматирование списка операций для вывода
    """
    if not operations:
        return f"📋 Записи за {date} ({club})\n\nДанных нет."
    
    result = []
    result.append(f"📋 Записи за {date}")
    result.append(f"Клуб: {club}")
    result.append(f"Всего записей: {len(operations)}")
    result.append("")
    
    # Группируем по коду
    from collections import defaultdict
    by_code = defaultdict(list)
    
    for op in operations:
        by_code[op['code']].append(op)
    
    for code in sorted(by_code.keys()):
        ops = by_code[code]
        result.append(f"▫️ {code}")
        
        for op in ops:
            result.append(
                f"  {op['channel'].upper()}: {op['name']} — {op['amount']:.0f}"
            )
        
        result.append("")
    
    return '\n'.join(result)

