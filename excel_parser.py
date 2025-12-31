"""
Модуль для парсинга Excel файлов (извлечение блока Примечания)
"""
import io
import logging
import sys
sys.path.append('.')
from typing import Dict, List, Any
from difflib import SequenceMatcher
import pandas as pd
from parser import DataParser

logger = logging.getLogger(__name__)


def name_similarity(name1: str, name2: str) -> float:
    """
    Вычисление похожести двух имен с приоритетом фамилии (0.0 - 1.0)
    Фамилия (последнее слово) имеет вес 70%, имя - 30%
    + Словарь сокращений (Дима→Дмитрий)
    + Нормализация Ё→Е
    """
    if not name1 or not name2:
        return 0.0
    
    name1_clean = name1.lower().strip()
    name2_clean = name2.lower().strip()
    
    # Разбиваем на части
    parts1 = name1_clean.split()
    parts2 = name2_clean.split()
    
    if not parts1 or not parts2:
        return 0.0
    
    # Если одно из имен содержит только одно слово - обычное сравнение
    if len(parts1) == 1 or len(parts2) == 1:
        return SequenceMatcher(None, name1_clean, name2_clean).ratio()
    
    # Извлекаем фамилию (последнее слово) и имя (остальное)
    surname1 = parts1[-1]
    surname2 = parts2[-1]
    firstname1 = ' '.join(parts1[:-1])
    firstname2 = ' '.join(parts2[:-1])
    
    # Словарь сокращений имен
    name_abbreviations = {
        'дима': 'дмитрий',
        'дмитр': 'дмитрий',
        'саша': 'александр',
        'алекс': 'александр',
        'лёша': 'алексей',
        'леша': 'алексей',
        'макс': 'максим',
        'максимка': 'максим',
        'миша': 'михаил',
        'паша': 'павел',
        'женя': 'евгений',
        'вова': 'владимир',
        'володя': 'владимир',
        'коля': 'николай',
        'серёга': 'сергей',
        'серега': 'сергей',
        'андрюха': 'андрей',
        'влад': 'владислав',
        'юра': 'юрий',
        'катя': 'екатерина',
        'настя': 'анастасия',
        'маша': 'мария',
        'лена': 'елена',
        'оля': 'ольга',
        'таня': 'татьяна',
        'вика': 'виктория',
        'даша': 'дарья'
    }
    
    # Нормализуем имена через словарь сокращений
    firstname1_normalized = name_abbreviations.get(firstname1.lower(), firstname1.lower())
    firstname2_normalized = name_abbreviations.get(firstname2.lower(), firstname2.lower())
    
    # Сравниваем фамилии
    surname_similarity = SequenceMatcher(None, surname1, surname2).ratio()
    
    # Сравниваем имена (с учетом нормализации)
    firstname_similarity = SequenceMatcher(None, firstname1_normalized, firstname2_normalized).ratio()
    
    # Взвешенная сумма: фамилия 70%, имя 30%
    weighted_similarity = surname_similarity * 0.7 + firstname_similarity * 0.3
    
    return weighted_similarity


class ExcelProcessor:
    """Процессор для извлечения данных из Excel файлов"""
    
    @staticmethod
    def _parse_decimal(text: str) -> float:
        """Извлечение числа из текста"""
        try:
            # Удаляем все кроме цифр, точки и минуса
            cleaned = ''.join(c for c in str(text) if c.isdigit() or c in '.-,')
            cleaned = cleaned.replace(',', '.')
            return float(cleaned) if cleaned else 0.0
        except (ValueError, AttributeError):
            return 0.0
    
    def extract_notes_entries(self, file_content: bytes) -> Dict[str, List[Dict[str, Any]]]:
        """
        Извлечение блока «Примечание» из Excel файла
        
        Возвращает:
        {
            'безнал': [список записей без наличных],
            'нал': [список записей с наличными],
            'extra': [список дополнительных заметок]
        }
        """
        try:
            df = pd.read_excel(io.BytesIO(file_content), sheet_name=0, header=None, engine='openpyxl')
        except Exception as e:
            logger.error(f"Error reading Excel for notes block: {e}")
            return {}
        
        if df.empty:
            return {}
        
        # Ищем заголовок "Примечания" в любой колонке
        start_row = None
        notes_col = None
        
        for row_idx in range(len(df)):
            for col_idx in range(df.shape[1]):
                cell = df.iloc[row_idx, col_idx]
                if isinstance(cell, str) and 'примечан' in cell.strip().lower():
                    start_row = row_idx + 1
                    notes_col = col_idx
                    logger.info(f"Found 'Примечания' at row {row_idx}, col {col_idx}")
                    break
            if start_row is not None:
                break
        
        if start_row is None or notes_col is None:
            logger.info("Notes block header not found")
            return {}
        
        # Ищем строку с заголовками ("долг") или начинаем со следующей строки
        column_headers_row = None
        for row_idx in range(start_row, len(df)):
            left_cell = df.iloc[row_idx, notes_col] if df.shape[1] > notes_col else None
            right_cell = df.iloc[row_idx, notes_col + 1] if df.shape[1] > notes_col + 1 else None
            
            if left_cell is None and right_cell is None:
                continue
            
            left_text = str(left_cell).strip().lower() if left_cell is not None else ''
            right_text = str(right_cell).strip().lower() if right_cell is not None else ''
            
            if 'долг' in left_text or 'долг' in right_text:
                column_headers_row = row_idx
                start_row = row_idx + 1
                logger.info(f"Found debt headers at row {row_idx}, data starts at {start_row}")
                break
            else:
                column_headers_row = row_idx
                start_row = row_idx
                break
        
        without_cash: List[Dict[str, Any]] = []
        with_cash: List[Dict[str, Any]] = []
        extra_notes: List[str] = []
        
        left_done = False
        right_done = False
        
        for row_idx in range(start_row, len(df)):
            left_cell = df.iloc[row_idx, notes_col] if df.shape[1] > notes_col else None
            right_cell = df.iloc[row_idx, notes_col + 1] if df.shape[1] > notes_col + 1 else None
            
            if left_cell is None and right_cell is None:
                continue
            
            left_text = str(left_cell).strip() if left_cell is not None and not (isinstance(left_cell, float) and pd.isna(left_cell)) else ''
            right_text = str(right_cell).strip() if right_cell is not None and not (isinstance(right_cell, float) and pd.isna(right_cell)) else ''
            
            left_lower = left_text.lower()
            right_lower = right_text.lower()
            
            # Останавливаемся если встречаем слова "доход", "расход", "прибыль" - это итоговый баланс
            if any(word in left_lower or word in right_lower for word in ['доход', 'расход', 'прибыль']):
                logger.info(f"Found balance keywords at row {row_idx}, stopping notes parsing")
                break
            
            processed_left = False
            processed_right = False
            
            if left_text and not left_done:
                if left_lower.startswith('итого'):
                    amount = self._parse_decimal(left_text.split(':')[-1])
                    without_cash.append({
                        'category': 'безнал',
                        'entry_text': left_text,
                        'is_total': True,
                        'amount': amount
                    })
                    left_done = True
                    processed_left = True
                else:
                    without_cash.append({
                        'category': 'безнал',
                        'entry_text': left_text,
                        'is_total': False
                    })
                    processed_left = True
            
            if right_text and not right_done:
                if right_lower.startswith('итого'):
                    amount = self._parse_decimal(right_text.split(':')[-1])
                    with_cash.append({
                        'category': 'нал',
                        'entry_text': right_text,
                        'is_total': True,
                        'amount': amount
                    })
                    right_done = True
                    processed_right = True
                else:
                    with_cash.append({
                        'category': 'нал',
                        'entry_text': right_text,
                        'is_total': False
                    })
                    processed_right = True
            
            if left_done and right_done and not (processed_left or processed_right):
                combined = " ".join(part for part in [left_text, right_text] if part).strip()
                if combined:
                    extra_notes.append(combined)
            
            elif not processed_left and left_text:
                extra_notes.append(left_text)
            
            elif not processed_right and right_text:
                extra_notes.append(right_text)
        
        return {
            'безнал': without_cash,
            'нал': with_cash,
            'extra': extra_notes
        }
    
    def extract_payments_sheet(self, file_content: bytes, db, club: str, date: str) -> Dict:
        """
        Извлечение данных из листа 'ЛИСТ ВЫПЛАТ'
        
        Структура листа:
        - Столбец A: категория (Д, Оф, Г, Н, Б, К, Dj, М, А, Лм, СБ)
        - Столбец B: номер
        - Столбец C: имя
        - Столбец D: СТАВКА
        - Столбец E: 3% ЛМ
        - Столбец F: 5%
        - Столбец G: ПРОМО
        - Столбец H: CRAZY MENU
        - Столбец I: Консумация
        - Столбец J: ЧАЕВЫЕ
        - Столбец K: ШТРАФЫ
        - Столбец L: ИТОГО выплат на смене
        - Столбец M: ДОЛГ
        - Столбец N: ДОЛГ НАЛ
        - Столбец O: к выплате
        
        Args:
            file_content: содержимое Excel файла
            db: объект Database для проверки объединений сотрудников
            club: название клуба (Москвич/Анора)
            date: дата для проверки канонических имен
        
        Returns:
            {
                'payments': [...],
                'name_changes': [
                    {'code': 'Д1', 'old_name': 'Ольга', 'new_name': 'Юлия', 'similarity': 0.2}
                ]
            }
        """
        # Пытаемся найти лист с разным регистром
        sheet_name = None
        try:
            # Получаем список всех листов
            import pandas as pd
            excel_file = pd.ExcelFile(io.BytesIO(file_content), engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            # Ищем лист с названием содержащим "лист" и "выплат" (любой регистр)
            for name in sheet_names:
                name_lower = name.lower().strip()
                if 'лист' in name_lower and 'выплат' in name_lower:
                    sheet_name = name
                    break
            
            if not sheet_name:
                logger.error("Sheet with 'лист выплат' not found in file")
                return []
            
            df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, header=None, engine='openpyxl')
            
            # Печатаем первые 30 строк для отладки
            print("=== DEBUG: First 30 rows of sheet ===")
            for idx in range(min(30, len(df))):
                row_data = []
                for col_idx in range(min(5, df.shape[1])):
                    cell = df.iloc[idx, col_idx]
                    row_data.append(str(cell)[:20] if not pd.isna(cell) else "")
                print(f"Row {idx}: {row_data}")
            print("=== END DEBUG ===")
            
        except Exception as e:
            logger.error(f"Error reading payments sheet: {e}")
            return {'payments': [], 'name_changes': []}
        
        if df.empty:
            return {'payments': [], 'name_changes': []}
        
        payments = []
        name_changes = []
        
        # Проходим по строкам (пропускаем первые 2 строки с заголовками)
        for row_idx in range(2, len(df)):
            # ПРОВЕРКА НА ИТОГО И ПРОЧИЕ РАСХОДЫ - останавливаем парсинг ПЕРЕД try-except
            # Проверяем ВСЕ первые 5 столбцов на наличие стоп-слов
            should_stop = False
            for col_idx in range(min(5, df.shape[1])):
                cell = df.iloc[row_idx, col_idx]
                if not pd.isna(cell):
                    cell_str = str(cell).strip().lower()
                    for stop_word in ['итого', 'промоутер', 'такси', 'прочие', 'стилист', 'нал:', 'безнал:', '%', 'процент']:
                        if stop_word in cell_str:
                            print(f"Found stop word '{stop_word}' at row {row_idx}, col {col_idx}, stopping parsing")
                            should_stop = True
                            break
                    if should_stop:
                        break
            
            if should_stop:
                print(f"DEBUG: Breaking at row {row_idx} due to stop word")
                break  # ← Теперь break гарантированно сработает!
            
            try:
                # Столбцы A и B - код
                category = df.iloc[row_idx, 0]  # A
                number = df.iloc[row_idx, 1]    # B
                
                category = str(category).strip() if not pd.isna(category) else ""
                number = str(number).strip() if not pd.isna(number) else ""
                
                # Убираем .0 из номера если есть
                if number and '.' in number:
                    try:
                        number = str(int(float(number)))
                    except:
                        pass
                
                # ГЕНЕРАЦИЯ КОДА
                # 1. Есть категория + номер (и номер - это цифры)
                if category and number and number.replace('.', '').replace(',', '').isdigit():
                    code = f"{category}{number}"
                
                # 2. Есть категория, НЕТ номера
                elif category and not number:
                    # Берём ПОЛНОЕ имя из столбца C
                    name_full = df.iloc[row_idx, 2] if not pd.isna(df.iloc[row_idx, 2]) else ""
                    name_full = str(name_full).strip()
                    
                    if name_full:
                        code = f"{category}-{name_full}"
                    else:
                        # Нет имени - пропускаем
                        continue
                
                # 3. НЕТ категории (A пусто)
                elif not category:
                    name_full = df.iloc[row_idx, 2] if not pd.isna(df.iloc[row_idx, 2]) else ""
                    name_full = str(name_full).strip()
                    
                    if name_full:
                        # Для случая без категории (Уборщица) - используем ПОЛНОЕ имя как код
                        code = name_full
                    else:
                        # Нет имени - пропускаем
                        continue
                
                else:
                    # Не подходит ни под один вариант - пропускаем
                    continue
                
                # НОРМАЛИЗАЦИЯ КОДА ПЕРЕД ПРОВЕРКАМИ В БД
                code = DataParser.normalize_code(code)
                
                # Имя ВСЕГДА берём из столбца C (независимо от типа кода)
                name = df.iloc[row_idx, 2] if not pd.isna(df.iloc[row_idx, 2]) else ""
                name = str(name).strip()
                name_from_file = name  # Сохраняем оригинал
                
                # ПРОВЕРКА ИМЕНИ С ПРИОРИТЕТОМ:
                # 1. Каноническое имя (по дате)
                # 2. Объединённое имя (employee_merges)
                # 3. Проверка в employees с сравнением похожести
                # 4. Существующее имя в operations
                
                canonical = db.get_canonical_name(code, club, date)
                if canonical:
                    # Приоритет 1: Каноническое имя
                    name = canonical
                    print(f"DEBUG: Canonical name used for {code}: {name}")
                else:
                    # Приоритет 2: Объединения
                    merge_info = db.check_employee_merge(club, code, name)
                    if merge_info:
                        code = merge_info['merged_code']
                        # Нормализуем merged_code на всякий случай
                        code = DataParser.normalize_code(code)
                        name = merge_info['merged_name']
                        print(f"DEBUG: Merged name used for {code}: {name}")
                    else:
                        # Приоритет 3: Проверка в employees с сравнением похожести
                        # Импортируем функцию сравнения
                        from bot import name_similarity
                        
                        # Проверяем есть ли сотрудник в employees
                        conn = db.get_connection()
                        cursor = conn.cursor()
                        cursor.execute("""
                            SELECT full_name, is_active
                            FROM employees
                            WHERE club = ? AND code = ?
                            LIMIT 1
                        """, (club, code))
                        
                        row = cursor.fetchone()
                        conn.close()
                        
                        name_from_file = name  # Оригинальное имя из файла
                        
                        if row:
                            db_name, is_active = row
                            
                            # Сравниваем похожесть
                            similarity = name_similarity(name, db_name)
                            
                            if similarity >= 0.85:
                                # Опечатка - тихо исправляем
                                print(f"DEBUG: Autocorrected {code}: '{name}' → '{db_name}' (similarity: {similarity:.2f})")
                                name = db_name
                            else:
                                # Имя сильно изменилось - запоминаем
                                print(f"DEBUG: Name changed {code}: '{db_name}' → '{name}' (similarity: {similarity:.2f})")
                                name_changes.append({
                                    'code': code,
                                    'old_name': db_name,
                                    'new_name': name,
                                    'similarity': similarity
                                })
                                # Пока используем имя из файла
                        else:
                            # Новый сотрудник - ничего не делаем
                            print(f"DEBUG: New employee: {code} - {name}")
                            
                            # Проверяем operations как fallback
                            existing_names = db.get_employee_names_by_code(club, code)
                            if existing_names:
                                name = existing_names[0]
                
                # Извлекаем числовые данные
                stavka = self._parse_decimal(df.iloc[row_idx, 3])       # D
                lm_3 = self._parse_decimal(df.iloc[row_idx, 4])         # E
                percent_5 = self._parse_decimal(df.iloc[row_idx, 5])    # F
                promo = self._parse_decimal(df.iloc[row_idx, 6])        # G
                crz = self._parse_decimal(df.iloc[row_idx, 7])          # H
                cons = self._parse_decimal(df.iloc[row_idx, 8])         # I
                tips = self._parse_decimal(df.iloc[row_idx, 9])         # J
                fines = self._parse_decimal(df.iloc[row_idx, 10])       # K
                total_shift = self._parse_decimal(df.iloc[row_idx, 11]) # L
                debt = self._parse_decimal(df.iloc[row_idx, 12])        # M (Долг БН)
                debt_nal = self._parse_decimal(df.iloc[row_idx, 13])    # N (Долг НАЛ)
                to_pay = self._parse_decimal(df.iloc[row_idx, 14])      # O (получила на смене)
                
                # Пропускаем строки где все значения = 0
                if all(v == 0 for v in [stavka, lm_3, percent_5, promo, crz, cons, tips, total_shift]):
                    continue
                
                # Проверяем нет ли уже такого кода в списке
                if any(p['code'] == code and p['name'] == name for p in payments):
                    print(f"DEBUG: DUPLICATE found! Skipping code={code}, name={name}")
                    continue
                
                payments.append({
                    'code': code,
                    'name': name,
                    'stavka': stavka,
                    'lm_3': lm_3,
                    'percent_5': percent_5,
                    'promo': promo,
                    'crz': crz,
                    'cons': cons,
                    'tips': tips,
                    'fines': fines,
                    'total_shift': total_shift,
                    'debt': debt,
                    'debt_nal': debt_nal,
                    'to_pay': to_pay
                })
                print(f"DEBUG: Added payment row {row_idx}: code={code}, name={name}")
                
            except Exception as e:
                logger.error(f"Error parsing payment row {row_idx}: {e}")
                print(f"DEBUG: Exception at row {row_idx}: {e}")
                continue
        
        logger.info(f"Extracted {len(payments)} payment records from 'ЛИСТ ВЫПЛАТ'")
        print(f"DEBUG: Total payments extracted: {len(payments)}")
        print(f"DEBUG: Payment codes: {[p['code'] for p in payments]}")
        print(f"DEBUG: Name changes found: {len(name_changes)}")
        
        return {
            'payments': payments,
            'name_changes': name_changes
        }

