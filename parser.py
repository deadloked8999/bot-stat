"""
Модуль парсинга блочного ввода данных
"""
import re
from typing import List, Dict, Tuple


class DataParser:
    """Парсер для блочного ввода данных"""
    
    # Специальные коды (только буквы, без цифр)
    SPECIAL_CODES = ['СБ', 'СБН', 'УБОРЩИЦА']
    
    @staticmethod
    def normalize_code(code: str) -> str:
        """
        Нормализация кода сотрудника
        Оставляем кириллицу как есть, только приводим к верхнему регистру
        """
        return code.strip().upper()
    
    @staticmethod
    def is_code(text: str) -> bool:
        """
        Проверка, является ли текст кодом
        Код = буква(ы) + цифра ИЛИ специальный код
        """
        text_upper = text.strip().upper()
        
        # Проверка на специальные коды
        if text_upper in DataParser.SPECIAL_CODES:
            return True
        
        # Проверка на наличие цифры (обычный код типа Д17, СБ5)
        return any(c.isdigit() for c in text)
    
    @staticmethod
    def is_name(text: str) -> bool:
        """
        Проверка, является ли текст именем
        Имя = только буквы БЕЗ цифр И не в списке специальных кодов
        """
        text_upper = text.strip().upper()
        
        # Если в списке специальных - это код
        if text_upper in DataParser.SPECIAL_CODES:
            return False
        
        # Если есть цифра - это код
        if any(c.isdigit() for c in text):
            return False
        
        # Только буквы - это имя
        return True
    
    @staticmethod
    def parse_amount(amount_str: str) -> Tuple[bool, float, str]:
        """
        Парсинг суммы (только целые числа, без копеек)
        Возвращает: (успех, значение, сообщение об ошибке)
        """
        amount_str = amount_str.strip()
        
        # Проверка на пробелы внутри числа
        if ' ' in amount_str:
            return False, 0.0, "Пробелы внутри числа не допускаются. Пишите 12000"
        
        # Автоматически удаляем точки и запятые (разделители тысяч)
        # 40,000 или 40.000 → 40000
        amount_str_cleaned = amount_str.replace(',', '').replace('.', '')
        
        try:
            amount = int(amount_str_cleaned)
            if amount < 0:
                return False, 0.0, "Сумма не может быть отрицательной"
            return True, float(amount), ""
        except ValueError:
            return False, 0.0, f"Неверный формат числа: '{amount_str}'. Используйте целое число (например: 2200)"
    
    @staticmethod
    def parse_line(line: str, line_number: int) -> Tuple[bool, Dict, str]:
        """
        Парсинг одной строки
        НОВАЯ ЛОГИКА:
        - КОД = буква(ы)+цифра ИЛИ специальный код (СБ, СБН, УБОРЩИЦА)
        - ИМЯ = только буквы без цифр
        - Примеры: "юля д17 1000" = код:Д17 имя:юля сумма:1000
                   "д17 юля 1000" = код:Д17 имя:юля сумма:1000
        Возвращает: (успех, данные, сообщение об ошибке)
        """
        line = line.strip()
        if not line:
            return False, {}, "Пустая строка"
        
        # Обработка формата с дефисом: "имя-сумма" или "код имя-сумма"
        if '-' in line:
            # Находим последний дефис (перед суммой)
            last_dash = line.rfind('-')
            before_dash = line[:last_dash].strip()
            amount_str = line[last_dash + 1:].strip()
            
            if not before_dash or not amount_str:
                return False, {}, f"Строка {line_number}: неверный формат с дефисом. Строка: '{line}'"
            
            # Парсим сумму
            success, amount, error = DataParser.parse_amount(amount_str)
            if not success:
                return False, {}, f"Строка {line_number}: {error}. Строка: '{line}'"
            
            # Разбиваем часть до дефиса
            before_parts = re.split(r'\s+', before_dash)
            
            # Ищем код
            code = None
            name_parts = []
            
            for part in before_parts:
                if DataParser.is_code(part) and code is None:
                    code = part
                else:
                    name_parts.append(part)
            
            # Если код не найден - первая часть считается кодом
            if code is None:
                code = before_parts[0] if before_parts else ""
                name_parts = before_parts[1:] if len(before_parts) > 1 else []
            
            name = ' '.join(name_parts)
            
        else:
            # Формат без дефиса: "код имя сумма"
            parts = re.split(r'\s+', line)
            
            if len(parts) < 2:
                return False, {}, f"Строка {line_number}: недостаточно элементов. Строка: '{line}'"
            
            # Последний элемент - сумма
            amount_str = parts[-1]
            
            # Парсим сумму
            success, amount, error = DataParser.parse_amount(amount_str)
            if not success:
                return False, {}, f"Строка {line_number}: {error}. Строка: '{line}'"
            
            # Ищем код среди оставшихся элементов
            remaining_parts = parts[:-1]
            code = None
            name_parts = []
            
            for part in remaining_parts:
                if DataParser.is_code(part) and code is None:
                    code = part
                else:
                    name_parts.append(part)
            
            # Если код не найден - первая часть считается кодом
            if code is None:
                code = remaining_parts[0] if remaining_parts else ""
                name_parts = remaining_parts[1:] if len(remaining_parts) > 1 else []
            
            name = ' '.join(name_parts)
        
        # Нормализуем код (только регистр, кириллица остаётся)
        normalized_code = DataParser.normalize_code(code)
        
        # Капитализируем имя (каждое слово с заглавной буквы)
        if name:
            name = ' '.join(word.capitalize() for word in name.split())
        
        return True, {
            'code': normalized_code,
            'original_code': code,
            'name': name,
            'amount': amount,
            'original_line': line
        }, ""
    
    @staticmethod
    def parse_block(text: str) -> Tuple[List[Dict], List[str]]:
        """
        Парсинг блока данных
        Возвращает: (успешные_строки, ошибки)
        """
        lines = text.strip().split('\n')
        successful = []
        errors = []
        
        for i, line in enumerate(lines, 1):
            success, data, error = DataParser.parse_line(line, i)
            if success:
                successful.append(data)
            elif error and 'Пустая строка' not in error:
                errors.append(error)
        
        return successful, errors
    
    @staticmethod
    def find_duplicates(data_list: List[Dict]) -> List[Dict]:
        """
        Поиск возможных дубликатов (один код с именем и без имени)
        Возвращает список кандидатов на объединение
        """
        # Группируем по коду
        by_code = {}
        for item in data_list:
            code = item['code']
            if code not in by_code:
                by_code[code] = []
            by_code[code].append(item)
        
        # Ищем дубликаты (код встречается больше 1 раза И есть разница в именах)
        duplicates = []
        for code, items in by_code.items():
            if len(items) > 1:
                # Проверяем, есть ли записи с именем и без
                has_name = any(item['name'] for item in items)
                has_no_name = any(not item['name'] for item in items)
                
                if has_name and has_no_name:
                    duplicates.append({
                        'code': code,
                        'items': items
                    })
        
        return duplicates
    
    @staticmethod
    def format_parse_result(successful: List[Dict], errors: List[str], 
                           channel: str, club: str) -> str:
        """
        Форматирование результата парсинга для вывода пользователю
        """
        result = []
        
        if successful:
            total = sum(item['amount'] for item in successful)
            result.append(f"✅ Принято {len(successful)} строк")
            result.append(f"Канал: {channel.upper()}")
            result.append(f"Клуб: {club}")
            result.append("")
            
            # Показываем примеры (первые 3 строки)
            result.append("Примеры:")
            for item in successful[:3]:
                result.append(f"  {item['code']} {item['name']} {item['amount']}")
            
            if len(successful) > 3:
                result.append(f"  ... и ещё {len(successful) - 3} строк")
            
            result.append("")
            result.append(f"💰 Итого по блоку: {total:.2f}")
        
        if errors:
            result.append("")
            result.append(f"⚠️ Ошибок: {len(errors)}")
            for error in errors[:5]:  # Показываем первые 5 ошибок
                result.append(f"  • {error}")
            if len(errors) > 5:
                result.append(f"  ... и ещё {len(errors) - 5} ошибок")
        
        return '\n'.join(result)

