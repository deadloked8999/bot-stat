"""
Модуль для работы с базой данных SQLite
"""
import sqlite3
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import config


class Database:
    """Класс для работы с базой данных"""
    
    def __init__(self, db_path: str = config.DATABASE_PATH):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Получить соединение с БД"""
        return sqlite3.connect(self.db_path)
    
    def init_database(self):
        """Инициализация базы данных"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица операций (основные данные)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                club TEXT NOT NULL,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                name_snapshot TEXT NOT NULL,
                channel TEXT NOT NULL,
                amount REAL NOT NULL,
                original_line TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(club, date, code, channel)
            )
        """)
        
        # Таблица объединений сотрудников
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employee_merges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                club TEXT NOT NULL,
                original_code TEXT NOT NULL,
                original_name TEXT NOT NULL,
                merged_code TEXT NOT NULL,
                merged_name TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        
        # Таблица журнала правок
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS edit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                club TEXT NOT NULL,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                channel TEXT NOT NULL,
                action TEXT NOT NULL,
                old_value REAL,
                new_value REAL,
                edited_at TEXT NOT NULL
            )
        """)
        
        # Таблица самозанятых
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS self_employed (
                code TEXT PRIMARY KEY,
                marked_at TEXT NOT NULL
            )
        """)
        
        # Индексы для быстрого поиска
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_operations_club_date 
            ON operations(club, date)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_operations_code 
            ON operations(code)
        """)
        
        conn.commit()
        conn.close()
    
    def add_or_update_operation(self, club: str, date: str, code: str, 
                                name: str, channel: str, amount: float, 
                                original_line: str, aggregate: bool = True) -> str:
        """
        Добавить или обновить операцию
        aggregate: если True - складывать суммы, если False - заменять
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Проверяем, существует ли запись
        cursor.execute("""
            SELECT amount FROM operations 
            WHERE club = ? AND date = ? AND code = ? AND channel = ?
        """, (club, date, code, channel))
        
        existing = cursor.fetchone()
        created_at = datetime.now().isoformat()
        
        if existing:
            old_amount = existing[0]
            if aggregate:
                new_amount = old_amount + amount
                action = f"Добавлено к существующей сумме: {old_amount} + {amount} = {new_amount}"
            else:
                new_amount = amount
                action = f"Заменено: {old_amount} → {new_amount}"
            
            # Обновляем запись
            cursor.execute("""
                UPDATE operations 
                SET amount = ?, name_snapshot = ?, original_line = ?, created_at = ?
                WHERE club = ? AND date = ? AND code = ? AND channel = ?
            """, (new_amount, name, original_line, created_at, club, date, code, channel))
            
            # Записываем в журнал
            cursor.execute("""
                INSERT INTO edit_log (club, date, code, channel, action, old_value, new_value, edited_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (club, date, code, channel, 'update' if aggregate else 'replace', 
                  old_amount, new_amount, created_at))
        else:
            # Вставляем новую запись
            cursor.execute("""
                INSERT INTO operations (club, date, code, name_snapshot, channel, amount, original_line, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (club, date, code, name, channel, amount, original_line, created_at))
            action = f"Добавлена новая запись: {amount}"
        
        conn.commit()
        conn.close()
        return action
    
    def get_operations_by_date(self, club: str, date: str) -> List[Dict]:
        """Получить все операции за дату по клубу"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT code, name_snapshot, channel, amount, original_line, created_at
            FROM operations
            WHERE club = ? AND date = ?
            ORDER BY code, channel
        """, (club, date))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'code': row[0],
                'name': row[1],
                'channel': row[2],
                'amount': row[3],
                'original_line': row[4],
                'created_at': row[5]
            }
            for row in rows
        ]
    
    def get_operations_by_period(self, club: str, date_from: str, date_to: str) -> List[Dict]:
        """Получить все операции за период по клубу"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT code, name_snapshot, channel, amount, date
            FROM operations
            WHERE club = ? AND date >= ? AND date <= ?
            ORDER BY date, code, channel
        """, (club, date_from, date_to))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'code': row[0],
                'name': row[1],
                'channel': row[2],
                'amount': row[3],
                'date': row[4]
            }
            for row in rows
        ]
    
    def update_operation(self, club: str, date: str, code: str, 
                        channel: str, new_amount: float) -> Tuple[bool, str]:
        """Исправить сумму операции"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем старое значение
        cursor.execute("""
            SELECT amount FROM operations
            WHERE club = ? AND date = ? AND code = ? AND channel = ?
        """, (club, date, code, channel))
        
        existing = cursor.fetchone()
        if not existing:
            conn.close()
            return False, f"Запись не найдена (club={club}, date={date}, code={code}, channel={channel})"
        
        old_amount = existing[0]
        created_at = datetime.now().isoformat()
        
        # Обновляем
        cursor.execute("""
            UPDATE operations
            SET amount = ?, created_at = ?
            WHERE club = ? AND date = ? AND code = ? AND channel = ?
        """, (new_amount, created_at, club, date, code, channel))
        
        # Журнал
        cursor.execute("""
            INSERT INTO edit_log (club, date, code, channel, action, old_value, new_value, edited_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (club, date, code, channel, 'manual_update', old_amount, new_amount, created_at))
        
        conn.commit()
        conn.close()
        return True, f"Готово: {code} {channel} {old_amount} → {new_amount} ({date})"
    
    def update_operation_name(self, club: str, date: str, code: str, 
                             channel: str, new_name: str) -> Tuple[bool, str]:
        """Обновить имя операции (для объединения дубликатов)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем старое значение
        cursor.execute("""
            SELECT name_snapshot FROM operations
            WHERE club = ? AND date = ? AND code = ? AND channel = ?
        """, (club, date, code, channel))
        
        existing = cursor.fetchone()
        if not existing:
            conn.close()
            return False, f"Запись не найдена"
        
        old_name = existing[0]
        created_at = datetime.now().isoformat()
        
        # Обновляем имя
        cursor.execute("""
            UPDATE operations
            SET name_snapshot = ?, created_at = ?
            WHERE club = ? AND date = ? AND code = ? AND channel = ?
        """, (new_name, created_at, club, date, code, channel))
        
        # Журнал (используем action для хранения старого и нового имени)
        cursor.execute("""
            INSERT INTO edit_log (club, date, code, channel, action, old_value, new_value, edited_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (club, date, code, channel, f'merge_name: "{old_name}" -> "{new_name}"', 
              0, 0, created_at))
        
        conn.commit()
        conn.close()
        return True, f"Обновлено имя: {code} {channel} '{old_name}' → '{new_name}' ({date})"
    
    def restore_sb_names_from_log(self) -> Tuple[int, List[str]]:
        """
        Восстановление имен СБ из журнала edit_log
        Возвращает: (количество восстановленных, список сообщений)
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Находим все записи в журнале с merge_name для СБ
        cursor.execute("""
            SELECT club, date, code, channel, action, edited_at
            FROM edit_log
            WHERE code = 'СБ' AND action LIKE 'merge_name:%'
            ORDER BY edited_at DESC
        """)
        
        rows = cursor.fetchall()
        restored_count = 0
        messages = []
        
        for row in rows:
            club, date, code, channel, action, edited_at = row
            
            # Извлекаем старое имя из action: 'merge_name: "{old_name}" -> "{new_name}"'
            match = re.search(r'merge_name: "([^"]+)" -> "([^"]+)"', action)
            if not match:
                continue
            
            old_name = match.group(1)
            new_name = match.group(2)
            
            # Восстанавливаем старое имя напрямую (без записи в журнал)
            cursor.execute("""
                UPDATE operations
                SET name_snapshot = ?
                WHERE club = ? AND date = ? AND code = ? AND channel = ?
            """, (old_name, club, date, code, channel))
            
            if cursor.rowcount > 0:
                restored_count += 1
                messages.append(f"✅ {club} {date} {code} {channel}: '{new_name}' → '{old_name}'")
            else:
                messages.append(f"❌ {club} {date} {code} {channel}: запись не найдена")
        
        conn.commit()
        conn.close()
        return restored_count, messages
    
    def delete_operation(self, club: str, date: str, code: str, channel: str) -> Tuple[bool, str]:
        """Удалить операцию"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Получаем значение перед удалением
        cursor.execute("""
            SELECT amount FROM operations
            WHERE club = ? AND date = ? AND code = ? AND channel = ?
        """, (club, date, code, channel))
        
        existing = cursor.fetchone()
        if not existing:
            conn.close()
            return False, "Запись не найдена"
        
        old_amount = existing[0]
        created_at = datetime.now().isoformat()
        
        # Удаляем
        cursor.execute("""
            DELETE FROM operations
            WHERE club = ? AND date = ? AND code = ? AND channel = ?
        """, (club, date, code, channel))
        
        # Журнал
        cursor.execute("""
            INSERT INTO edit_log (club, date, code, channel, action, old_value, new_value, edited_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (club, date, code, channel, 'delete', old_amount, None, created_at))
        
        conn.commit()
        conn.close()
        return True, f"Удалено: {code} {channel} {old_amount} ({date})"
    
    def delete_operations_by_period(self, club: str, date_from: str, date_to: str) -> int:
        """
        Массовое удаление операций за период по клубу
        Возвращает количество удалённых записей
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT code, channel, amount, date
            FROM operations
            WHERE club = ? AND date >= ? AND date <= ?
        """, (club, date_from, date_to))
        rows = cursor.fetchall()
        
        if not rows:
            conn.close()
            return 0
        
        deleted_count = len(rows)
        created_at = datetime.now().isoformat()
        
        # Журналируем удаление каждой записи
        for code, channel, amount, op_date in rows:
            cursor.execute("""
                INSERT INTO edit_log (club, date, code, channel, action, old_value, new_value, edited_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (club, op_date, code, channel, 'bulk_delete', amount, None, created_at))
        
        # Удаляем записи
        cursor.execute("""
            DELETE FROM operations
            WHERE club = ? AND date >= ? AND date <= ?
        """, (club, date_from, date_to))
        
        conn.commit()
        conn.close()
        return deleted_count
    
    def get_employee_payments(self, code: str, date_from: str, date_to: str, 
                             club: Optional[str] = None) -> List[Dict]:
        """
        Получить все выплаты сотрудника за период
        Если club не указан, ищет по всем клубам
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        if club:
            cursor.execute("""
                SELECT club, date, channel, amount, name_snapshot
                FROM operations
                WHERE code = ? AND date >= ? AND date <= ? AND club = ?
                ORDER BY date, channel
            """, (code, date_from, date_to, club))
        else:
            cursor.execute("""
                SELECT club, date, channel, amount, name_snapshot
                FROM operations
                WHERE code = ? AND date >= ? AND date <= ?
                ORDER BY club, date, channel
            """, (code, date_from, date_to))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'club': row[0],
                'date': row[1],
                'channel': row[2],
                'amount': row[3],
                'name': row[4]
            }
            for row in rows
        ]
    
    def get_edit_log(self, limit: int = 20, code: Optional[str] = None, 
                     date: Optional[str] = None) -> List[Dict]:
        """
        Получить журнал изменений
        limit: количество записей (по умолчанию 20)
        code: фильтр по коду сотрудника
        date: фильтр по дате
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = """
            SELECT club, date, code, channel, action, old_value, new_value, edited_at
            FROM edit_log
            WHERE 1=1
        """
        params = []
        
        if code:
            query += " AND code = ?"
            params.append(code)
        
        if date:
            query += " AND date = ?"
            params.append(date)
        
        query += " ORDER BY edited_at DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [
            {
                'club': row[0],
                'date': row[1],
                'code': row[2],
                'channel': row[3],
                'action': row[4],
                'old_value': row[5],
                'new_value': row[6],
                'edited_at': row[7]
            }
            for row in rows
        ]
    
    # ==================== Методы для самозанятых ====================
    
    def add_self_employed(self, code: str) -> Tuple[bool, str]:
        """
        Добавить код в список самозанятых
        Returns: (success, message)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Нормализуем код (приводим к верхнему регистру)
            code = code.upper().strip()
            
            # Проверяем, не добавлен ли уже
            cursor.execute("SELECT code FROM self_employed WHERE code = ?", (code,))
            if cursor.fetchone():
                conn.close()
                return False, f"Код {code} уже в списке самозанятых"
            
            # Добавляем
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(
                "INSERT INTO self_employed (code, marked_at) VALUES (?, ?)",
                (code, now)
            )
            conn.commit()
            conn.close()
            
            return True, f"✅ Код {code} добавлен в самозанятые"
        
        except Exception as e:
            return False, f"Ошибка: {str(e)}"
    
    def remove_self_employed(self, code: str) -> Tuple[bool, str]:
        """
        Удалить код из списка самозанятых
        Returns: (success, message)
        """
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            code = code.upper().strip()
            
            # Проверяем наличие
            cursor.execute("SELECT code FROM self_employed WHERE code = ?", (code,))
            if not cursor.fetchone():
                conn.close()
                return False, f"Код {code} не найден в списке самозанятых"
            
            # Удаляем
            cursor.execute("DELETE FROM self_employed WHERE code = ?", (code,))
            conn.commit()
            conn.close()
            
            return True, f"✅ Код {code} убран из самозанятых"
        
        except Exception as e:
            return False, f"Ошибка: {str(e)}"
    
    def is_self_employed(self, code: str) -> bool:
        """
        Проверить, является ли код самозанятым
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        code = code.upper().strip()
        cursor.execute("SELECT code FROM self_employed WHERE code = ?", (code,))
        result = cursor.fetchone() is not None
        
        conn.close()
        return result
    
    def get_all_self_employed(self) -> List[str]:
        """
        Получить список всех самозанятых кодов
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT code FROM self_employed ORDER BY code")
        codes = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return codes
    
    def init_self_employed_list(self, codes: List[str]) -> int:
        """
        Инициализация списка самозанятых (для первого запуска)
        Возвращает количество добавленных кодов
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        added = 0
        
        for code in codes:
            code = code.upper().strip()
            try:
                cursor.execute(
                    "INSERT OR IGNORE INTO self_employed (code, marked_at) VALUES (?, ?)",
                    (code, now)
                )
                if cursor.rowcount > 0:
                    added += 1
            except:
                continue
        
        conn.commit()
        conn.close()
        
        return added
    
    def get_all_employees(self, club: str) -> List[Dict]:
        """
        Получить список всех уникальных сотрудников клуба
        Возвращает список словарей с полями: code, name
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Получаем уникальные пары (код, имя)
            cursor.execute("""
                SELECT DISTINCT code, name_snapshot as name
                FROM operations
                WHERE club = ?
                ORDER BY code, name_snapshot
            """, (club,))
            
            rows = cursor.fetchall()
            employees = [{'code': row[0], 'name': row[1]} for row in rows]
            
            conn.close()
            return employees
        except Exception as e:
            print(f"Ошибка получения сотрудников: {e}")
            conn.close()
            return []
    
    def merge_employees(self, club: str, main_code: str, main_name: str, employees_to_merge: List[Dict]) -> int:
        """
        Объединить сотрудников в БД - перенести данные и объединить
        
        Args:
            club: название клуба
            main_code: код главного сотрудника
            main_name: имя главного сотрудника
            employees_to_merge: список сотрудников для объединения (словари с code, name)
        
        Returns:
            Количество обновлённых записей
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        total_updated = 0
        now = datetime.now().isoformat()
        
        try:
            for emp in employees_to_merge:
                # Записываем объединение в таблицу employee_merges
                cursor.execute("""
                    INSERT INTO employee_merges (club, original_code, original_name, merged_code, merged_name, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (club, emp['code'], emp['name'], main_code, main_name, now))
                
                # Получаем все записи сотрудника которого объединяем
                cursor.execute("""
                    SELECT date, channel, amount, original_line
                    FROM operations
                    WHERE club = ? AND code = ? AND name_snapshot = ?
                """, (club, emp['code'], emp['name']))
                
                records = cursor.fetchall()
                
                # Для каждой записи:
                # 1. Проверяем есть ли уже запись с (club, date, main_code, channel)
                # 2. Если есть - агрегируем (добавляем amount)
                # 3. Если нет - создаём новую с main_code
                for date, channel, amount, original_line in records:
                    # Проверяем существование
                    cursor.execute("""
                        SELECT id, amount FROM operations
                        WHERE club = ? AND date = ? AND code = ? AND channel = ?
                    """, (club, date, main_code, channel))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        # Агрегируем
                        new_amount = existing[1] + amount
                        cursor.execute("""
                            UPDATE operations
                            SET amount = ?, name_snapshot = ?
                            WHERE id = ?
                        """, (new_amount, main_name, existing[0]))
                    else:
                        # Создаём новую запись
                        cursor.execute("""
                            INSERT INTO operations (club, date, code, name_snapshot, channel, amount, original_line, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (club, date, main_code, main_name, channel, amount, original_line, now))
                    
                    total_updated += 1
                
                # Удаляем старые записи
                cursor.execute("""
                    DELETE FROM operations
                    WHERE club = ? AND code = ? AND name_snapshot = ?
                """, (club, emp['code'], emp['name']))
            
            conn.commit()
            conn.close()
            return total_updated
        except Exception as e:
            print(f"Ошибка объединения сотрудников: {e}")
            conn.rollback()
            conn.close()
            return 0
    
    def check_employee_merge(self, club: str, code: str, name: str) -> Optional[Dict]:
        """
        Проверить был ли сотрудник объединён ранее
        
        Returns:
            None если не объединён, или {'merged_code': ..., 'merged_name': ...}
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT merged_code, merged_name
                FROM employee_merges
                WHERE club = ? AND original_code = ? AND original_name = ?
                LIMIT 1
            """, (club, code, name))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {'merged_code': row[0], 'merged_name': row[1]}
            return None
        except Exception as e:
            print(f"Ошибка проверки объединения: {e}")
            conn.close()
            return None

