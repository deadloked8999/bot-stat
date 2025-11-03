"""
Модуль парсинга блочного ввода данных
"""
import re
from typing import List, Dict, Tuple


class DataParser:
    """Парсер для блочного ввода данных"""
    
    @staticmethod
    def normalize_code(code: str) -> str:
        """
        Нормализация кода сотрудника
        Д/D, Р/R и т.д. приводим к единому виду
        """
        code = code.strip().upper()
        
        # Карта замен кириллица -> латиница
        cyrillic_to_latin = {
            'А': 'A', 'В': 'B', 'Д': 'D', 'Е': 'E', 'Ё': 'E',
            'Ж': 'ZH', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K',
            'Л': 'L', 'М': 'M', 'Н': 'H', 'О': 'O', 'П': 'P',
            'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'Y', 'Ф': 'F',
            'Х': 'X', 'Ц': 'TS', 'Ч': 'CH', 'Ш': 'SH', 'Щ': 'SCH',
            'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'YU', 'Я': 'YA'
        }
        
        # Заменяем кириллические буквы на латинские
        normalized = ''
        for char in code:
            if char in cyrillic_to_latin:
                normalized += cyrillic_to_latin[char]
            else:
                normalized += char
        
        return normalized
    
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
        
        # Проверка на точку или запятую (копейки не разрешены)
        if '.' in amount_str or ',' in amount_str:
            return False, 0.0, "Копейки не разрешены. Используйте только целые числа (например: 2200)"
        
        try:
            amount = int(amount_str)
            if amount < 0:
                return False, 0.0, "Сумма не может быть отрицательной"
            return True, float(amount), ""
        except ValueError:
            return False, 0.0, f"Неверный формат числа: '{amount_str}'. Используйте целое число (например: 2200)"
    
    @staticmethod
    def parse_line(line: str, line_number: int) -> Tuple[bool, Dict, str]:
        """
        Парсинг одной строки
        Поддерживаемые форматы:
        1. <код> <имя> <сумма>  (например: Д7 Нади 6800)
        2. <код> <имя>-<сумма>  (например: Д7 Нади-6800)
        3. <код без номера>-<сумма> (например: Уборщица-2000)
        4. <код> <имя фамилия>-<сумма> (например: СБ Дмитрий Васенев-4000)
        5. <только код>-<сумма> (например: P8-1000)
        Возвращает: (успех, данные, сообщение об ошибке)
        """
        line = line.strip()
        if not line:
            return False, {}, "Пустая строка"
        
        # Разбиваем по пробелам и табуляции
        parts = re.split(r'\s+', line)
        
        code = None
        name = None
        amount_str = None
        
        # Определяем формат
        if len(parts) >= 3:
            # Формат 1: код имя сумма (три или более элемента)
            code = parts[0]
            amount_str = parts[-1]
            name_parts = parts[1:-1]
            name = ' '.join(name_parts) if name_parts else ""
            
        elif len(parts) == 2:
            # Формат 2: код имя-сумма (два элемента, второй содержит дефис)
            code = parts[0]
            name_amount = parts[1]
            
            # Ищем последний дефис (для случаев типа "Нади-Мари-6800")
            if '-' in name_amount:
                last_dash_index = name_amount.rfind('-')
                name = name_amount[:last_dash_index]
                amount_str = name_amount[last_dash_index + 1:]
                
                if not amount_str:
                    return False, {}, f"Строка {line_number}: отсутствует сумма после дефиса. Строка: '{line}'"
            else:
                return False, {}, f"Строка {line_number}: неверный формат. Ожидается 'код имя-сумма' или 'код имя сумма'. Строка: '{line}'"
        
        elif len(parts) == 1:
            # Формат 3: всё слитно с дефисом (Уборщица-2000)
            if '-' in parts[0]:
                last_dash_index = parts[0].rfind('-')
                code = parts[0][:last_dash_index]
                amount_str = parts[0][last_dash_index + 1:]
                name = ""
                
                if not code:
                    return False, {}, f"Строка {line_number}: отсутствует код. Строка: '{line}'"
                if not amount_str:
                    return False, {}, f"Строка {line_number}: отсутствует сумма. Строка: '{line}'"
            else:
                return False, {}, f"Строка {line_number}: неверный формат. Строка: '{line}'"
        
        else:
            return False, {}, f"Строка {line_number}: недостаточно элементов. Строка: '{line}'"
        
        # Парсим сумму
        success, amount, error = DataParser.parse_amount(amount_str)
        if not success:
            return False, {}, f"Строка {line_number}: {error}. Строка: '{line}'"
        
        # Нормализуем код
        normalized_code = DataParser.normalize_code(code)
        
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

