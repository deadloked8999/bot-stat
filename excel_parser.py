"""
Модуль для парсинга Excel файлов (извлечение блока Примечания)
"""
import io
import logging
from typing import Dict, List, Any
import pandas as pd

logger = logging.getLogger(__name__)


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

