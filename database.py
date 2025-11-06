"""
Модуль для работы с базой данных SQLite
"""
import sqlite3
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

