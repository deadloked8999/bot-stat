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
        - ФОРМАТ С %: "Р8 Дамир-11.000 % 750" = сумма1 + сумма2 = 11750
        - СТРОКИ НАЧИНАЮЩИЕСЯ С %: "%Р1-2750" = доплата к коду Р1
        Возвращает: (успех, данные, сообщение об ошибке)
        """
        line = line.strip()
        if not line:
            return False, {}, "Пустая строка"
        
        # Проверка на строку начинающуюся с %
        # Это доплата к существующему коду
        is_additional_payment = False
        if line.startswith('%'):
            is_additional_payment = True
            line = line[1:].strip()  # Убираем % в начале
        
        # Проверка на формат с процентом: "код имя-сумма1 % сумма2"
        if '%' in line:
            # Разделяем по %
            parts_by_percent = line.split('%')
            if len(parts_by_percent) == 2:
                left_part = parts_by_percent[0].strip()  # "Р8 Дамир-11.000"
                right_part = parts_by_percent[1].strip()  # "750" или "-750"
                
                # Убираем лишний минус в начале (если есть)
                # %-750 → 750
                if right_part.startswith('-'):
                    right_part = right_part[1:].strip()
                
                # Парсим вторую сумму (после %)
                success2, amount2, error2 = DataParser.parse_amount(right_part)
                if not success2:
                    return False, {}, f"Строка {line_number}: ошибка парсинга суммы после '%': {error2}. Строка: '{line}'"
                
                # Обрабатываем левую часть как обычную строку
                # Пытаемся найти сумму в левой части
                if '-' in left_part:
                    # Формат: "Р8 Дамир-11.000"
                    last_dash = left_part.rfind('-')
                    before_dash = left_part[:last_dash].strip()
                    amount1_str = left_part[last_dash + 1:].strip()
                    
                    success1, amount1, error1 = DataParser.parse_amount(amount1_str)
                    if not success1:
                        return False, {}, f"Строка {line_number}: ошибка парсинга первой суммы: {error1}. Строка: '{line}'"
                    
                    # Складываем суммы
                    total_amount = amount1 + amount2
                    
                    # Парсим код и имя
                    before_parts = re.split(r'\s+', before_dash)
                    code = None
                    name_parts = []
                    
                    for part in before_parts:
                        if DataParser.is_code(part) and code is None:
                            code = part
                        else:
                            name_parts.append(part)
                    
                    if code is None:
                        code = before_parts[0] if before_parts else ""
                        name_parts = before_parts[1:] if len(before_parts) > 1 else []
                    
                    name = ' '.join(name_parts)
                    
                    # Нормализуем код
                    normalized_code = DataParser.normalize_code(code)
                    
                    # Капитализируем имя
                    if name:
                        name = ' '.join(word.capitalize() for word in name.split())
                    
                    return True, {
                        'code': normalized_code,
                        'original_code': code,
                        'name': name,
                        'amount': total_amount,
                        'original_line': line
                    }, ""
        
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
            'original_line': line,
            'is_additional': is_additional_payment  # Флаг доплаты (строка начиналась с %)
        }, ""
    
    @staticmethod
    def clean_excel_duplicates(line: str) -> Tuple[str, int]:
        """
        Очистка дублей из Excel (когда копируют несколько колонок)
        Возвращает: (очищенная_строка, количество_удаленных_дублей)
        
        Пример:
        "Д4 Дарина-18000  Д4 Дарина-  Д4 Дарина-" -> ("Д4 Дарина-18000", 2)
        """
        parts = line.split()
        
        if len(parts) < 3:
            return line, 0  # Слишком мало частей, дублей быть не может
        
        # Ищем повторяющиеся коды
        from collections import Counter
        
        # Собираем все части которые могут быть кодами
        potential_codes = []
        for part in parts:
            if DataParser.is_code(part):
                potential_codes.append(DataParser.normalize_code(part))
        
        # Считаем повторения
        code_counts = Counter(potential_codes)
        
        # Если есть код который повторяется 2+ раза
        for code, count in code_counts.items():
            if count >= 2:
                # Находим первое вхождение этого кода
                first_occurrence_idx = None
                for i, part in enumerate(parts):
                    if DataParser.is_code(part) and DataParser.normalize_code(part) == code:
                        first_occurrence_idx = i
                        break
                
                if first_occurrence_idx is not None:
                    # Ищем где заканчивается первая запись (до следующего вхождения кода)
                    # Берем код + следующую часть (имя) + следующую (сумма если есть)
                    # Пример: "Д4 Дарина-18000" или "Д4 Дарина 18000"
                    
                    # Собираем первую запись
                    first_record_parts = []
                    i = first_occurrence_idx
                    found_amount = False
                    
                    while i < len(parts):
                        current_part = parts[i]
                        
                        # Если это снова наш код и это не первое вхождение - стоп
                        if i > first_occurrence_idx and DataParser.is_code(current_part) and DataParser.normalize_code(current_part) == code:
                            break
                        
                        first_record_parts.append(current_part)
                        
                        # Проверяем есть ли сумма в этой части
                        if '-' in current_part:
                            # Формат "Дарина-18000"
                            found_amount = True
                            break
                        elif current_part.replace('.', '').replace(',', '').isdigit():
                            # Отдельная сумма
                            found_amount = True
                            break
                        
                        i += 1
                        
                        # Максимум 3 части (код имя сумма)
                        if len(first_record_parts) >= 3:
                            break
                    
                    # Если нашли запись с суммой, возвращаем её
                    if found_amount and first_record_parts:
                        cleaned_line = ' '.join(first_record_parts)
                        removed_count = count - 1
                        return cleaned_line, removed_count
        
        return line, 0
    
    @staticmethod
    def parse_block(text: str) -> Tuple[List[Dict], List[str]]:
        """
        Парсинг блока данных
        Возвращает: (успешные_строки, ошибки)
        """
        lines = text.strip().split('\n')
        successful = []
        errors = []
        cleaned_lines_info = []  # Информация об очищенных строках
        
        for i, line in enumerate(lines, 1):
            # Сначала очищаем от Excel дублей
            cleaned_line, removed_count = DataParser.clean_excel_duplicates(line)
            
            if removed_count > 0:
                cleaned_lines_info.append({
                    'line_num': i,
                    'original': line,
                    'cleaned': cleaned_line,
                    'removed': removed_count
                })
            
            # Парсим очищенную строку
            success, data, error = DataParser.parse_line(cleaned_line, i)
            if success:
                successful.append(data)
                # Добавляем информацию об очистке если была
                if removed_count > 0:
                    data['_excel_cleaned'] = True
                    data['_original_line'] = line
            elif error and 'Пустая строка' not in error:
                errors.append(error)
        
        # Если были очищены строки, добавляем информационное сообщение
        if cleaned_lines_info:
            info_lines = ["⚠️ Обнаружены дубли из Excel (автоматически очищены):"]
            for info in cleaned_lines_info[:5]:  # Показываем первые 5
                info_lines.append(f"   Строка {info['line_num']}: удалено {info['removed']} дублей")
            
            if len(cleaned_lines_info) > 5:
                info_lines.append(f"   ... и ещё {len(cleaned_lines_info) - 5} строк")
            
            # Добавляем как предупреждение в ошибки (но не критичное)
            errors.insert(0, '\n'.join(info_lines))
        
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
    def find_additional_payments(data_list: List[Dict]) -> Dict:
        """
        Поиск доплат (записей начинающихся с %) и поиск основных записей для объединения
        
        Возвращает:
        {
            'merges': [список объединений с найденными основными записями],
            'not_found': [список доплат без основной записи],
            'no_code': [список доплат без кода (только имя)]
        }
        """
        # Разделяем на обычные и доплаты
        regular = []
        additional = []
        
        for item in data_list:
            if item.get('is_additional', False):
                additional.append(item)
            else:
                regular.append(item)
        
        # Индексируем обычные записи по коду
        # ВАЖНО: Для СБ индексируем по (код + имя), чтобы разные СБ не объединялись
        by_code = {}
        for item in regular:
            code = item['code']
            name = item.get('name', '')
            
            # Для СБ используем комбинацию код+имя как ключ
            if code == 'СБ' and name:
                key = f"{code}_{name}"
            else:
                key = code
            
            if key not in by_code:
                by_code[key] = []
            by_code[key].append(item)
        
        merges = []
        not_found = []
        no_code = []
        
        for add_item in additional:
            code = add_item['code']
            add_name = add_item.get('name', '')
            
            # Проверяем есть ли код
            if not code or not DataParser.is_code(code):
                # Код не определен (только имя)
                no_code.append(add_item)
                continue
            
            # Для СБ ищем по комбинации код+имя
            if code == 'СБ' and add_name:
                search_key = f"{code}_{add_name}"
            else:
                search_key = code
            
            # Ищем основную запись с таким ключом
            if search_key in by_code:
                # Нашли! Создаем объединение
                main_items = by_code[search_key]
                merges.append({
                    'code': code,
                    'main_items': main_items,  # Может быть несколько основных записей
                    'additional_item': add_item,
                    'total_amount': sum(item['amount'] for item in main_items) + add_item['amount']
                })
            else:
                # Не нашли основную запись
                not_found.append(add_item)
        
        return {
            'merges': merges,
            'not_found': not_found,
            'no_code': no_code
        }
    
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
    
    @staticmethod
    def parse_stylist_expenses(text: str) -> Tuple[List[Dict], List[str]]:
        """
        Парсинг данных о расходах на стилистов
        
        Формат входных данных (БЕЗ периода):
        Д14Бритни 2000
        А13Варя 1500
        Н5 деля 1500
        
        Args:
            text: Текст с данными о расходах (без периода)
        
        Returns:
            (expenses_list, errors)
            expenses_list: [{'code': 'Д14', 'name': 'Бритни', 'amount': 2000}, ...]
            errors: список строк с ошибками
        """
        lines = text.strip().split('\n')
        expenses = []
        errors = []
        
        # Паттерн для расхода: КОД (буквы+цифры) + ИМЯ (буквы) + СУММА (цифры)
        # Учитываем возможные пробелы между кодом и именем
        expense_pattern = r'^([А-ЯЁA-Z]+\d+)\s*([А-ЯЁа-яёA-Za-z]+)\s+(\d+)$'
        
        # Карта латинских букв на кириллические
        latin_to_cyrillic = {
            'A': 'А', 'B': 'В', 'C': 'С', 'E': 'Е', 'H': 'Н', 'K': 'К',
            'M': 'М', 'O': 'О', 'P': 'Р', 'T': 'Т', 'X': 'Х', 'Y': 'У'
        }
        
        def normalize_code_cyrillic(code: str) -> str:
            """Заменить латинские буквы на кириллические в коде"""
            result = []
            for char in code.upper():
                if char in latin_to_cyrillic:
                    result.append(latin_to_cyrillic[char])
                else:
                    result.append(char)
            return ''.join(result)
        
        # Парсим строки с расходами
        for line in lines:
            line = line.strip()
            
            # Пропускаем пустые строки
            if not line:
                continue
            
            # Пропускаем строки с текстом (приветствия, имена визажистов и т.д.)
            # Если строка не содержит цифр - пропускаем
            if not any(c.isdigit() for c in line):
                continue
            
            # Пытаемся распарсить как расход
            match = re.match(expense_pattern, line, re.IGNORECASE)
            if match:
                code = match.group(1).strip()
                name = match.group(2).strip()
                amount = int(match.group(3))
                
                # Нормализуем код (латиница -> кириллица, верхний регистр)
                code = normalize_code_cyrillic(code)
                
                # Нормализуем имя (первая буква заглавная)
                name = name.capitalize()
                
                expenses.append({
                    'code': code,
                    'name': name,
                    'amount': amount
                })
            else:
                # Строка содержит цифры, но не подходит под паттерн
                # Возможно, это ошибка ввода
                if any(c.isdigit() for c in line) and len(line) > 2:
                    errors.append(f"Не удалось распарсить строку: {line}")
        
        return expenses, errors

