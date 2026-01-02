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
        
        # Проверяем и создаём таблицу employees если нужно
        self.migrate_to_employees()
    
    def get_connection(self):
        """Получить соединение с БД"""
        return sqlite3.connect(self.db_path)
    
    @staticmethod
    def normalize_sb_code(code: str) -> str:
        """Нормализовать код СБ: СБ_{id} или СБ_{timestamp} -> СБ"""
        if code and code.startswith('СБ_'):
            return 'СБ'
        return code
    
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
        
        # Таблица расходов на стилистов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stylist_expenses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                club TEXT NOT NULL,
                period_from TEXT NOT NULL,
                period_to TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                amount REAL NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        
        # Таблица выплат (из ЛИСТА ВЫПЛАТ)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                club TEXT NOT NULL,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                stavka REAL DEFAULT 0,
                lm_3 REAL DEFAULT 0,
                percent_5 REAL DEFAULT 0,
                promo REAL DEFAULT 0,
                crz REAL DEFAULT 0,
                cons REAL DEFAULT 0,
                tips REAL DEFAULT 0,
                fines REAL DEFAULT 0,
                total_shift REAL DEFAULT 0,
                debt REAL DEFAULT 0,
                debt_nal REAL DEFAULT 0,
                to_pay REAL DEFAULT 0,
                created_at TEXT NOT NULL,
                UNIQUE(club, date, code)
            )
        """)
        
        # Таблица канонических имён сотрудников
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employee_canonical_names (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                canonical_name TEXT NOT NULL,
                club TEXT NOT NULL,
                valid_from TEXT NOT NULL,
                valid_to TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(code, club, valid_from)
            )
        """)
        
        # Таблица доступов сотрудников к боту
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS employee_access (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                club TEXT NOT NULL,
                telegram_user_id INTEGER UNIQUE NOT NULL,
                full_name TEXT,
                username TEXT,
                phone TEXT,
                birth_date TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT NOT NULL,
                UNIQUE(code, club)
            )
        """)
        
        # Таблица админов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_user_id INTEGER UNIQUE NOT NULL,
                name TEXT,
                added_at TEXT NOT NULL
            )
        """)
        
        # Добавляем админов если их ещё нет
        cursor.execute("SELECT COUNT(*) FROM admins")
        admin_count = cursor.fetchone()[0]
        
        if admin_count == 0:
            # Добавляем двух основных админов
            admins_to_add = [
                (1380211249, "Админ 1"),
                (7942920768, "Админ 2")
            ]
            
            for admin_id, admin_name in admins_to_add:
                cursor.execute("""
                    INSERT INTO admins (telegram_user_id, name, added_at)
                    VALUES (?, ?, ?)
                """, (admin_id, admin_name, datetime.now().isoformat()))
                print(f"[INIT] Добавлен админ: {admin_name} ({admin_id})")
        
        # Индексы для быстрого поиска
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_operations_club_date 
            ON operations(club, date)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_operations_code 
            ON operations(code)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_payments_club_date 
            ON payments(club, date)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_canonical_names_lookup 
            ON employee_canonical_names(code, club, valid_from, valid_to)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_employee_access_telegram 
            ON employee_access(telegram_user_id)
        """)
        
        conn.commit()
        conn.close()
    
    def ensure_employees_table(self):
        """Убедиться что таблица employees существует (для миграции)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Проверяем существование таблицы
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='employees'
            """)
            
            if cursor.fetchone():
                print("[INFO] Table employees already exists")
                conn.close()
                return
            
            print("[INFO] Creating table employees...")
            
            # Создаём таблицу
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    club TEXT NOT NULL,
                    full_name TEXT NOT NULL,
                    phone TEXT,
                    telegram_user_id INTEGER,
                    telegram_username TEXT,
                    birth_date TEXT,
                    hired_date TEXT,
                    fired_date TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    UNIQUE(code, club)
                )
            """)
            
            # Создаём индексы
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_employees_code 
                ON employees(code, club)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_employees_telegram 
                ON employees(telegram_user_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_employees_active 
                ON employees(is_active)
            """)
            
            # Создаём таблицу истории сотрудников
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS employee_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    club TEXT NOT NULL,
                    full_name TEXT NOT NULL,
                    hired_date TEXT,
                    fired_date TEXT,
                    created_at TEXT NOT NULL
                )
            """)
            
            conn.commit()
            print("[INFO] Table employees created successfully")
            conn.close()
            
        except Exception as e:
            print(f"[ERROR] Failed to create employees table: {e}")
            conn.rollback()
            conn.close()
    
    def migrate_to_employees(self):
        """Миграция данных из operations, payments и employee_access в employees"""
        
        # Убеждаемся что таблица существует
        self.ensure_employees_table()
        
        print("[MIGRATION] Starting employees migration...")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Проверяем: уже мигрировали?
            cursor.execute("SELECT COUNT(*) FROM employees")
            count = cursor.fetchone()[0]
            
            if count > 0:
                print(f"[MIGRATION] Employees already migrated ({count} records)")
                conn.close()
                return
            
            # Собираем уникальные коды из operations
            cursor.execute("""
                SELECT DISTINCT code, name_snapshot, club, MIN(date) as first_date
                FROM operations
                GROUP BY code, club
            """)
            operations_data = cursor.fetchall()
            
            # Собираем уникальные коды из payments
            cursor.execute("""
                SELECT DISTINCT code, name, club, MIN(date) as first_date
                FROM payments
                GROUP BY code, club
            """)
            payments_data = cursor.fetchall()
            
            # Собираем данные из employee_access
            cursor.execute("""
                SELECT code, club, telegram_user_id, full_name, username, phone
                FROM employee_access
                WHERE is_active = 1
            """)
            access_data = cursor.fetchall()
            
            # Создаём словарь для быстрого поиска
            access_dict = {}
            for row in access_data:
                key = f"{row[0]}_{row[1]}"  # code_club
                access_dict[key] = {
                    'telegram_user_id': row[2],
                    'full_name': row[3],
                    'username': row[4],
                    'phone': row[5]
                }
            
            # Объединяем данные
            employees_dict = {}
            
            # Из operations
            for code, name, club, first_date in operations_data:
                key = f"{code}_{club}"
                if key not in employees_dict:
                    employees_dict[key] = {
                        'code': code,
                        'club': club,
                        'full_name': name,
                        'hired_date': first_date
                    }
            
            # Из payments (если имя длиннее - используем его)
            for code, name, club, first_date in payments_data:
                key = f"{code}_{club}"
                if key in employees_dict:
                    # Обновляем имя если оно длиннее
                    if name and len(name) > len(employees_dict[key]['full_name']):
                        employees_dict[key]['full_name'] = name
                    # Обновляем дату если раньше
                    if first_date < employees_dict[key]['hired_date']:
                        employees_dict[key]['hired_date'] = first_date
                else:
                    employees_dict[key] = {
                        'code': code,
                        'club': club,
                        'full_name': name,
                        'hired_date': first_date
                    }
            
            # Добавляем данные из employee_access
            for key, emp in employees_dict.items():
                if key in access_dict:
                    acc = access_dict[key]
                    emp['telegram_user_id'] = acc['telegram_user_id']
                    emp['telegram_username'] = acc['username']
                    emp['phone'] = acc['phone']
                    # Если есть полное имя в access - используем его
                    if acc['full_name']:
                        emp['full_name'] = acc['full_name']
            
            # Вставляем в employees
            now = datetime.now().isoformat()
            inserted = 0
            
            for key, emp in employees_dict.items():
                cursor.execute("""
                    INSERT INTO employees 
                    (code, club, full_name, phone, telegram_user_id, telegram_username, 
                     hired_date, is_active, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                """, (
                    emp['code'],
                    emp['club'],
                    emp['full_name'],
                    emp.get('phone'),
                    emp.get('telegram_user_id'),
                    emp.get('telegram_username'),
                    emp['hired_date'],
                    now
                ))
                inserted += 1
            
            conn.commit()
            print(f"[MIGRATION] Successfully migrated {inserted} employees")
            conn.close()
            
        except Exception as e:
            print(f"[MIGRATION] Error: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            conn.close()
    
    def add_or_update_operation(self, club: str, date: str, code: str, 
                                name: str, channel: str, amount: float, 
                                original_line: str, aggregate: bool = True) -> str:
        """
        Добавить или обновить операцию
        aggregate: если True - складывать суммы, если False - заменять
        
        ВАЖНО: Для СБ проверка существования учитывает имя, чтобы разные СБ 
        с одинаковым кодом не объединялись в одну запись.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        created_at = datetime.now().isoformat()
        
        # Для СБ проверка существования должна учитывать имя
        # Для остальных кодов - только по (club, date, code, channel)
        if code == 'СБ' and name:
            # Проверяем существование с учетом имени
            cursor.execute("""
                SELECT id, amount, name_snapshot FROM operations 
                WHERE club = ? AND date = ? AND code = ? AND channel = ? AND name_snapshot = ?
            """, (club, date, code, channel, name))
            
            existing = cursor.fetchone()
            
            if existing:
                # Найдена запись с таким же именем - обновляем
                record_id, old_amount, old_name = existing
                if aggregate:
                    new_amount = old_amount + amount
                    action = f"Добавлено к существующей сумме: {old_amount} + {amount} = {new_amount}"
                else:
                    new_amount = amount
                    action = f"Заменено: {old_amount} → {new_amount}"
                
                # Обновляем запись (имя сохраняем существующее)
                cursor.execute("""
                    UPDATE operations 
                    SET amount = ?, original_line = ?, created_at = ?
                    WHERE id = ?
                """, (new_amount, original_line, created_at, record_id))
                
                # Записываем в журнал
                cursor.execute("""
                    INSERT INTO edit_log (club, date, code, channel, action, old_value, new_value, edited_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (club, date, code, channel, 'update' if aggregate else 'replace', 
                      old_amount, new_amount, created_at))
            else:
                # Записи с таким именем нет - проверяем, нет ли конфликта по UNIQUE constraint
                # (другой СБ с тем же кодом, датой и каналом, но другим именем)
                cursor.execute("""
                    SELECT id, amount, name_snapshot FROM operations 
                    WHERE club = ? AND date = ? AND code = ? AND channel = ?
                """, (club, date, code, channel))
                
                conflict = cursor.fetchone()
                
                if conflict:
                    # Есть конфликт по UNIQUE constraint - старая запись с другим именем
                    conflict_id, conflict_amount, conflict_name = conflict
                    
                    # ОБХОДНОЙ ПУТЬ: используем уникальный код для каждого СБ
                    # Меняем код старой записи на уникальный, чтобы обойти UNIQUE constraint
                    temp_code = f"СБ_{conflict_id}"
                    cursor.execute("""
                        UPDATE operations 
                        SET code = ?
                        WHERE id = ?
                    """, (temp_code, conflict_id))
                    
                    # Теперь можем вставить новую запись с уникальным кодом
                    import time
                    new_temp_code = f"СБ_{int(time.time() * 1000000)}"  # Уникальный код на основе timestamp
                    cursor.execute("""
                        INSERT INTO operations (club, date, code, name_snapshot, channel, amount, original_line, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (club, date, new_temp_code, name, channel, amount, original_line, created_at))
                    
                    action = f"Добавлена новая запись СБ: {name} - {amount} (сохранена отдельно от {conflict_name})"
                else:
                    # Конфликта нет - просто вставляем
                    cursor.execute("""
                        INSERT INTO operations (club, date, code, name_snapshot, channel, amount, original_line, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (club, date, code, name, channel, amount, original_line, created_at))
                    action = f"Добавлена новая запись: {amount}"
        else:
            # Для не-СБ или СБ без имени - стандартная логика
            cursor.execute("""
                SELECT id, amount, name_snapshot FROM operations 
                WHERE club = ? AND date = ? AND code = ? AND channel = ?
            """, (club, date, code, channel))
            
            existing = cursor.fetchone()
            
            if existing:
                record_id, old_amount, old_name = existing
                if aggregate:
                    new_amount = old_amount + amount
                    action = f"Добавлено к существующей сумме: {old_amount} + {amount} = {new_amount}"
                else:
                    new_amount = amount
                    action = f"Заменено: {old_amount} → {new_amount}"
                
                # Обновляем запись (имя обновляем только если оно изменилось)
                final_name = name if name else old_name
                cursor.execute("""
                    UPDATE operations 
                    SET amount = ?, name_snapshot = ?, original_line = ?, created_at = ?
                    WHERE id = ?
                """, (new_amount, final_name, original_line, created_at, record_id))
                
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
                'code': self.normalize_sb_code(row[0]),
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
                'code': self.normalize_sb_code(row[0]),
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
                'code': self.normalize_sb_code(row[2]),
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
        Инициализация списка самозанятых (ТОЛЬКО если таблица пустая)
        Возвращает количество добавленных кодов
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Проверяем: есть ли уже записи в таблице
        cursor.execute("SELECT COUNT(*) FROM self_employed")
        count = cursor.fetchone()[0]
        
        # Если таблица НЕ пустая - ничего не делаем
        if count > 0:
            conn.close()
            return 0
        
        # Таблица пустая - инициализируем список
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        added = 0
        
        for code in codes:
            code = code.upper().strip()
            try:
                cursor.execute(
                    "INSERT INTO self_employed (code, marked_at) VALUES (?, ?)",
                    (code, now)
                )
                added += 1
            except:
                continue
        
        conn.commit()
        conn.close()
        
        return added
    
    def get_all_employees(self, club: str) -> List[Dict[str, str]]:
        """
        Получить список ВСЕХ уникальных сотрудников для клуба
        Объединяет данные из employees, operations и payments
        Нормализует коды для объединения вариантов
        """
        from parser import DataParser
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            employees_dict = {}  # Ключ: нормализованный код
            
            # 1. Из employees (приоритет)
            cursor.execute("""
                SELECT code, full_name
                FROM employees
                WHERE club = ?
            """, (club,))
            
            for code, name in cursor.fetchall():
                normalized_code = DataParser.normalize_code(code)
                # Используем только код как ключ, чтобы объединить варианты
                if normalized_code not in employees_dict:
                    employees_dict[normalized_code] = {'code': normalized_code, 'name': name, 'source': 'employees'}
            
            # 2. Из operations
            cursor.execute("""
                SELECT DISTINCT code, name_snapshot
                FROM operations
                WHERE club = ?
            """, (club,))
            
            for code, name in cursor.fetchall():
                normalized_code = DataParser.normalize_code(code)
                # Добавляем только если нет в employees
                if normalized_code not in employees_dict:
                    employees_dict[normalized_code] = {'code': normalized_code, 'name': name, 'source': 'operations'}
            
            # 3. Из payments
            cursor.execute("""
                SELECT DISTINCT code, name
                FROM payments
                WHERE club = ?
            """, (club,))
            
            for code, name in cursor.fetchall():
                normalized_code = DataParser.normalize_code(code)
                # Добавляем только если нет в employees
                if normalized_code not in employees_dict:
                    employees_dict[normalized_code] = {'code': normalized_code, 'name': name, 'source': 'payments'}
            
            conn.close()
            
            # Возвращаем список (без информации об источнике)
            return [{'code': emp['code'], 'name': emp['name']} for emp in employees_dict.values()]
            
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
        print(f"DEBUG: merge_employees started")
        print(f"DEBUG: club={club}, main_code={main_code}, main_name={main_name}")
        print(f"DEBUG: employees_to_merge={employees_to_merge}")
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        total_updated = 0
        now = datetime.now().isoformat()
        
        try:
            for emp in employees_to_merge:
                print(f"DEBUG: Processing employee: {emp['code']} - {emp['name']}")
                # Записываем объединение в таблицу employee_merges
                cursor.execute("""
                    INSERT INTO employee_merges (club, original_code, original_name, merged_code, merged_name, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (club, emp['code'], emp['name'], main_code, main_name, now))
                print(f"DEBUG: Merge record inserted for {emp['code']} - {emp['name']}")
                
                # Получаем все записи из operations
                cursor.execute("""
                    SELECT date, channel, amount, original_line
                    FROM operations
                    WHERE club = ? AND code = ?
                """, (club, emp['code']))
                
                ops_records = cursor.fetchall()
                print(f"DEBUG: Found {len(ops_records)} operations records for {emp['code']}")
                
                # Получаем все записи из payments
                cursor.execute("""
                    SELECT date, 'ЗП' as channel, total_shift as amount, '' as original_line
                    FROM payments
                    WHERE club = ? AND code = ?
                """, (club, emp['code']))
                
                pay_records = cursor.fetchall()
                print(f"DEBUG: Found {len(pay_records)} payments records for {emp['code']}")
                
                # Объединяем
                records = list(ops_records) + list(pay_records)
                print(f"DEBUG: Total {len(records)} records to merge")
                
                # Для каждой записи:
                for date, channel, amount, original_line in records:
                    print(f"DEBUG: Merging record: date={date}, channel={channel}, amount={amount}")
                    
                    # Если это operations
                    if channel != 'ЗП':
                        # Проверяем существование
                        cursor.execute("""
                            SELECT id, amount FROM operations
                            WHERE club = ? AND date = ? AND code = ? AND channel = ?
                        """, (club, date, main_code, channel))
                        
                        existing = cursor.fetchone()
                        
                        if existing:
                            # Агрегируем
                            new_amount = existing[1] + amount
                            print(f"DEBUG: Aggregating operations: id={existing[0]}, new_amount={new_amount}")
                            cursor.execute("""
                                UPDATE operations
                                SET amount = ?, name_snapshot = ?
                                WHERE id = ?
                            """, (new_amount, main_name, existing[0]))
                        else:
                            # Создаём новую
                            print(f"DEBUG: Creating new operations record")
                            cursor.execute("""
                                INSERT INTO operations (club, date, code, name_snapshot, channel, amount, original_line, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            """, (club, date, main_code, main_name, channel, amount, original_line, now))
                        
                        total_updated += 1
                    
                    # Если это payments
                    else:
                        # Проверяем существование
                        cursor.execute("""
                            SELECT date FROM payments
                            WHERE club = ? AND date = ? AND code = ?
                        """, (club, date, main_code))
                        
                        existing = cursor.fetchone()
                        
                        if existing:
                            print(f"DEBUG: Payment already exists for date={date}, skipping")
                        else:
                            # Копируем всю строку
                            print(f"DEBUG: Copying payment record")
                            cursor.execute("""
                                INSERT INTO payments (club, date, code, name, stavka, lm_3, percent_5, promo, crz, cons, tips, fines, total_shift, debt, debt_nal, to_pay, created_at)
                                SELECT club, date, ?, name, stavka, lm_3, percent_5, promo, crz, cons, tips, fines, total_shift, debt, debt_nal, to_pay, created_at
                                FROM payments
                                WHERE club = ? AND date = ? AND code = ?
                            """, (main_code, club, date, emp['code']))
                        
                        total_updated += 1
                
                # Удаляем старые записи из operations
                cursor.execute("""
                    DELETE FROM operations
                    WHERE club = ? AND code = ?
                """, (club, emp['code']))
                
                # Удаляем старые записи из payments
                cursor.execute("""
                    DELETE FROM payments
                    WHERE club = ? AND code = ?
                """, (club, emp['code']))
                
                # Удаляем из employees
                cursor.execute("""
                    DELETE FROM employees
                    WHERE club = ? AND code = ?
                """, (club, emp['code']))
                
                print(f"DEBUG: Deleted old records for {emp['code']}")
            
            conn.commit()
            print(f"DEBUG: Commit successful, total_updated={total_updated}")
            conn.close()
            return total_updated
        except Exception as e:
            print(f"DEBUG: EXCEPTION in merge_employees: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
            conn.close()
            return 0
    
    def split_merged_employee(self, club: str, merged_code: str) -> int:
        """
        Разделить ошибочно объединённого сотрудника обратно по именам
        
        Например:
        СБ-ДЕНИС ЕРМАКОВ содержит записи с разными именами:
        - Александр Ромашкан → создаём СБ-Александр Ромашкан
        - Денис Ермаков → оставляем СБ-ДЕНИС ЕРМАКОВ
        - Дмитрий Васенёв → создаём СБ-Дмитрий Васенёв
        и т.д.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            from datetime import datetime
            now = datetime.now().isoformat()
            
            # 1. Получаем все уникальные имена для этого кода из operations
            cursor.execute("""
                SELECT DISTINCT name_snapshot
                FROM operations
                WHERE club = ? AND code = ?
            """, (club, merged_code))
            
            names_ops = [row[0] for row in cursor.fetchall()]
            
            # 2. Получаем все уникальные имена из payments
            cursor.execute("""
                SELECT DISTINCT name
                FROM payments
                WHERE club = ? AND code = ?
            """, (club, merged_code))
            
            names_pay = [row[0] for row in cursor.fetchall()]
            
            # Объединяем уникальные имена
            all_names = list(set(names_ops + names_pay))
            
            print(f"[SPLIT] Found {len(all_names)} unique names for {merged_code}")
            print(f"[SPLIT] Names: {all_names}")
            
            if len(all_names) <= 1:
                print("[SPLIT] Nothing to split (only one name)")
                return 0
            
            total_split = 0
            
            # 3. Для каждого имени создаём отдельный код
            for name in all_names:
                # Генерируем новый код
                if merged_code.startswith('СБ-'):
                    new_code = f"СБ-{name}"
                else:
                    new_code = f"{merged_code.split('-')[0]}-{name}"
                
                print(f"[SPLIT] Processing {name} → {new_code}")
                
                # 4. Переносим записи из operations
                cursor.execute("""
                    UPDATE operations
                    SET code = ?
                    WHERE club = ? AND code = ? AND name_snapshot = ?
                """, (new_code, club, merged_code, name))
                
                ops_updated = cursor.rowcount
                print(f"[SPLIT] Updated {ops_updated} operations records")
                
                # 5. Переносим записи из payments
                cursor.execute("""
                    UPDATE payments
                    SET code = ?
                    WHERE club = ? AND code = ? AND name = ?
                """, (new_code, club, merged_code, name))
                
                pay_updated = cursor.rowcount
                print(f"[SPLIT] Updated {pay_updated} payments records")
                
                # 6. Создаём/обновляем запись в employees
                cursor.execute("""
                    INSERT OR REPLACE INTO employees 
                    (code, club, full_name, is_active, hired_date, created_at)
                    VALUES (?, ?, ?, 1, ?, ?)
                """, (new_code, club, name, datetime.now().strftime('%Y-%m-%d'), now))
                
                total_split += ops_updated + pay_updated
            
            # 7. Удаляем старую запись объединённого сотрудника
            cursor.execute("""
                DELETE FROM employees
                WHERE club = ? AND code = ?
            """, (club, merged_code))
            
            conn.commit()
            print(f"[SPLIT] Complete! Total records updated: {total_split}")
            conn.close()
            return total_split
            
        except Exception as e:
            print(f"[SPLIT ERROR]: {e}")
            import traceback
            traceback.print_exc()
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
    
    # ============ Методы для работы с расходами на стилистов ============
    
    def add_stylist_expense(self, club: str, period_from: str, period_to: str, 
                           code: str, name: str, amount: float) -> bool:
        """
        Добавить расход на стилиста
        
        Args:
            club: Клуб (Москвич/Анора)
            period_from: Начало периода (YYYY-MM-DD)
            period_to: Конец периода (YYYY-MM-DD)
            code: Код сотрудника
            name: Имя сотрудника
            amount: Сумма расхода
        
        Returns:
            True если успешно
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO stylist_expenses 
                (club, period_from, period_to, code, name, amount, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (club, period_from, period_to, code, name, amount, 
                  datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка добавления расхода на стилиста: {e}")
            conn.rollback()
            conn.close()
            return False
    
    def get_stylist_expenses_for_period(self, club: str, date_from: str, date_to: str) -> List[Dict]:
        """
        Получить расходы на стилистов для периода с учетом пересечений
        
        Args:
            club: Клуб
            date_from: Начало периода отчета (YYYY-MM-DD)
            date_to: Конец периода отчета (YYYY-MM-DD)
        
        Returns:
            Список словарей с полями: code, name, total_amount
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Пересечение периодов: NOT (period_to < date_from OR period_from > date_to)
            cursor.execute("""
                SELECT code, name, SUM(amount) as total_amount
                FROM stylist_expenses
                WHERE club = ?
                  AND NOT (period_to < ? OR period_from > ?)
                GROUP BY code, name
            """, (club, date_from, date_to))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {
                    'code': row[0],
                    'name': row[1],
                    'amount': row[2]
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Ошибка получения расходов на стилистов: {e}")
            conn.close()
            return []
    
    def add_payment(self, club: str, date: str, code: str, name: str,
                    stavka: float = 0, lm_3: float = 0, percent_5: float = 0,
                    promo: float = 0, crz: float = 0, cons: float = 0,
                    tips: float = 0, fines: float = 0, total_shift: float = 0,
                    debt: float = 0, debt_nal: float = 0, to_pay: float = 0):
        """Добавить или обновить запись о выплате"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO payments 
            (club, date, code, name, stavka, lm_3, percent_5, promo, crz, cons, 
             tips, fines, total_shift, debt, debt_nal, to_pay, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (club, date, code, name, stavka, lm_3, percent_5, promo, crz, cons,
              tips, fines, total_shift, debt, debt_nal, to_pay, 
              datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        
        conn.commit()
        conn.close()

    def get_payments(self, club: str, date_from: str, date_to: str):
        """Получить все выплаты за период"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM payments
            WHERE club = ? AND date >= ? AND date <= ?
            ORDER BY date, code
        """, (club, date_from, date_to))
        
        rows = cursor.fetchall()
        conn.close()
        
        return rows
    
    def debug_payments(self, club: str, date: str):
        """Показать все записи payments для клуба и даты"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT code, name, total_shift, created_at
            FROM payments
            WHERE club = ? AND date = ?
            ORDER BY code
        """, (club, date))
        
        rows = cursor.fetchall()
        conn.close()
        
        print(f"\n=== PAYMENTS в БД для {club} за {date} ===")
        for row in rows:
            print(f"Код: {row[0]}, Имя: {row[1]}, ИТОГО: {row[2]}, Создано: {row[3]}")
        print(f"=== Всего записей: {len(rows)} ===\n")
        
        return rows
    
    def fix_payment_codes(self):
        """
        Исправить коды в таблице payments (убрать .0 из номеров)
        Например: Н8.0 → Н8, Оф12.0 → Оф12
        
        Returns:
            Количество исправленных записей
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Получаем все записи
            cursor.execute("SELECT id, code FROM payments")
            rows = cursor.fetchall()
            
            fixed_count = 0
            
            for row_id, code in rows:
                # Проверяем есть ли .0 в коде
                if '.0' in code or '.' in code:
                    # Разбиваем на категорию и номер
                    import re
                    match = re.match(r'([А-Яа-яA-Za-z]+)(.+)', code)
                    if match:
                        category = match.group(1)
                        number_str = match.group(2).strip()
                        
                        # Преобразуем номер
                        try:
                            # Убираем .0
                            if '.' in number_str:
                                number = int(float(number_str))
                                new_code = f"{category}{number}"
                                
                                # Обновляем в БД
                                cursor.execute(
                                    "UPDATE payments SET code = ? WHERE id = ?",
                                    (new_code, row_id)
                                )
                                fixed_count += 1
                                print(f"Исправлено: {code} → {new_code}")
                        except:
                            pass
            
            conn.commit()
            print(f"Всего исправлено записей: {fixed_count}")
            return fixed_count
            
        except Exception as e:
            print(f"Ошибка исправления кодов: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()
    
    def get_stylist_expenses_periods(self, club: str) -> List[Dict]:
        """
        Получить все периоды расходов на стилистов для клуба
        
        Returns:
            Список словарей с полями: period_from, period_to, count, total_amount
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT period_from, period_to, COUNT(*) as count, SUM(amount) as total
                FROM stylist_expenses
                WHERE club = ?
                GROUP BY period_from, period_to
                ORDER BY period_from DESC
            """, (club,))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {
                    'period_from': row[0],
                    'period_to': row[1],
                    'count': row[2],
                    'total_amount': row[3]
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Ошибка получения периодов расходов: {e}")
            conn.close()
            return []
    
    def get_stylist_expenses_by_period(self, club: str, period_from: str, period_to: str) -> List[Dict]:
        """
        Получить детальный список расходов за конкретный период
        
        Returns:
            Список словарей с полями: code, name, amount
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT code, name, amount
                FROM stylist_expenses
                WHERE club = ? AND period_from = ? AND period_to = ?
                ORDER BY code, name
            """, (club, period_from, period_to))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {
                    'code': row[0],
                    'name': row[1],
                    'amount': row[2]
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Ошибка получения расходов по периоду: {e}")
            conn.close()
            return []
    
    def delete_stylist_expenses_by_period(self, club: str, period_from: str, period_to: str) -> int:
        """
        Удалить все расходы на стилистов за конкретный период
        
        Returns:
            Количество удалённых записей
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                DELETE FROM stylist_expenses
                WHERE club = ? AND period_from = ? AND period_to = ?
            """, (club, period_from, period_to))
            
            deleted = cursor.rowcount
            conn.commit()
            conn.close()
            return deleted
        except Exception as e:
            print(f"Ошибка удаления расходов: {e}")
            conn.rollback()
            conn.close()
            return 0
    
    def get_employee_names_by_code(self, club: str, code: str) -> List[str]:
        """
        Получить все уникальные имена для кода в клубе
        
        Args:
            club: Клуб (Москвич/Анора)
            code: Код сотрудника (например, Д13)
        
        Returns:
            Список уникальных имен: ['Варя'] или ['Катя', 'Лена'] или []
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT DISTINCT name_snapshot
                FROM operations
                WHERE club = ? AND code = ? AND name_snapshot != ''
                ORDER BY name_snapshot
            """, (club, code))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [row[0] for row in rows if row[0]]
        except Exception as e:
            print(f"Ошибка получения имен сотрудника: {e}")
            conn.close()
            return []
    
    # ============ Методы для работы с каноническими именами ============
    
    def get_canonical_name(self, code: str, club: str, date: str) -> Optional[str]:
        """
        Получить каноническое имя сотрудника на указанную дату
        
        Args:
            code: Код сотрудника
            club: Клуб
            date: Дата в формате YYYY-MM-DD
        
        Returns:
            Каноническое имя или None если не найдено
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT canonical_name
                FROM employee_canonical_names
                WHERE code = ? AND club = ? 
                  AND valid_from <= ? 
                  AND (valid_to IS NULL OR valid_to >= ?)
                ORDER BY valid_from DESC
                LIMIT 1
            """, (code, club, date, date))
            
            row = cursor.fetchone()
            conn.close()
            
            return row[0] if row else None
        except Exception as e:
            print(f"Ошибка получения канонического имени: {e}")
            conn.close()
            return None
    
    def add_canonical_name(self, code: str, club: str, canonical_name: str, 
                          valid_from: str, valid_to: Optional[str] = None) -> bool:
        """
        Добавить каноническое имя сотрудника
        
        Args:
            code: Код сотрудника
            club: Клуб
            canonical_name: Каноническое имя
            valid_from: Дата начала действия (YYYY-MM-DD)
            valid_to: Дата окончания действия (YYYY-MM-DD) или None
        
        Returns:
            True если успешно добавлено
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO employee_canonical_names 
                (code, club, canonical_name, valid_from, valid_to, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (code, club, canonical_name, valid_from, valid_to, 
                  datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка добавления канонического имени: {e}")
            conn.rollback()
            conn.close()
            return False
    
    def update_canonical_name_period(self, id: int, valid_to: str) -> bool:
        """
        Закрыть период действия канонического имени (увольнение)
        
        Args:
            id: ID записи
            valid_to: Дата окончания действия (YYYY-MM-DD)
        
        Returns:
            True если успешно обновлено
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE employee_canonical_names
                SET valid_to = ?
                WHERE id = ?
            """, (valid_to, id))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка обновления периода: {e}")
            conn.rollback()
            conn.close()
            return False
    
    def get_all_canonical_names(self, club: str) -> List[Dict]:
        """
        Получить все канонические имена для клуба
        
        Args:
            club: Клуб
        
        Returns:
            Список словарей с полями: id, code, canonical_name, valid_from, valid_to, created_at
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, code, canonical_name, valid_from, valid_to, created_at
                FROM employee_canonical_names
                WHERE club = ?
                ORDER BY code, valid_from DESC
            """, (club,))
            
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {
                    'id': row[0],
                    'code': row[1],
                    'canonical_name': row[2],
                    'valid_from': row[3],
                    'valid_to': row[4],
                    'created_at': row[5]
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Ошибка получения канонических имён: {e}")
            conn.close()
            return []
    
    # ============ Методы для работы с доступами сотрудников ============
    
    def get_employee_by_telegram_id(self, telegram_user_id: int) -> Optional[Dict]:
        """
        Получить данные сотрудника по Telegram ID
        
        Args:
            telegram_user_id: ID пользователя в Telegram
        
        Returns:
            Словарь с данными или None
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, code, club, telegram_user_id, full_name, username, 
                       phone, birth_date, is_active, created_at
                FROM employee_access
                WHERE telegram_user_id = ? AND is_active = 1
            """, (telegram_user_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    'id': row[0],
                    'code': row[1],
                    'club': row[2],
                    'telegram_user_id': row[3],
                    'full_name': row[4],
                    'username': row[5],
                    'phone': row[6],
                    'birth_date': row[7],
                    'is_active': bool(row[8]),
                    'created_at': row[9]
                }
            return None
        except Exception as e:
            print(f"Ошибка получения сотрудника по Telegram ID: {e}")
            conn.close()
            return None
    
    def add_employee_access(self, code: str, club: str, telegram_user_id: int,
                           full_name: Optional[str] = None, username: Optional[str] = None,
                           phone: Optional[str] = None, birth_date: Optional[str] = None) -> bool:
        """
        Добавить доступ сотрудника к боту
        
        Args:
            code: Код сотрудника
            club: Клуб
            telegram_user_id: ID пользователя в Telegram
            full_name: Полное имя
            username: Username в Telegram
            phone: Телефон
            birth_date: Дата рождения (YYYY-MM-DD)
        
        Returns:
            True если успешно добавлено
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO employee_access 
                (code, club, telegram_user_id, full_name, username, phone, birth_date, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, club, telegram_user_id, full_name, username, phone, birth_date,
                  datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка добавления доступа: {e}")
            conn.rollback()
            conn.close()
            return False
    
    def update_employee_access(self, id: int, **kwargs) -> bool:
        """
        Обновить данные доступа сотрудника
        
        Args:
            id: ID записи
            **kwargs: Поля для обновления (code, club, full_name, username, phone, birth_date, is_active)
        
        Returns:
            True если успешно обновлено
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Формируем SET часть запроса
            allowed_fields = ['code', 'club', 'full_name', 'username', 'phone', 'birth_date', 'is_active']
            updates = []
            values = []
            
            for field, value in kwargs.items():
                if field in allowed_fields:
                    updates.append(f"{field} = ?")
                    values.append(value)
            
            if not updates:
                conn.close()
                return False
            
            values.append(id)
            query = f"UPDATE employee_access SET {', '.join(updates)} WHERE id = ?"
            
            cursor.execute(query, tuple(values))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка обновления доступа: {e}")
            conn.rollback()
            conn.close()
            return False
    
    def delete_employee_access(self, id: int) -> bool:
        """
        Удалить доступ сотрудника (или деактивировать)
        
        Args:
            id: ID записи
        
        Returns:
            True если успешно удалено/деактивировано
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Деактивируем вместо удаления
            cursor.execute("""
                UPDATE employee_access
                SET is_active = 0
                WHERE id = ?
            """, (id,))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка удаления доступа: {e}")
            conn.rollback()
            conn.close()
            return False
    
    def get_all_employee_access(self, club: Optional[str] = None) -> List[Dict]:
        """
        Получить список всех доступов сотрудников
        
        Args:
            club: Клуб (опционально, если None - все клубы)
        
        Returns:
            Список словарей с данными доступа
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            if club:
                cursor.execute("""
                    SELECT id, code, club, telegram_user_id, full_name, username, 
                           phone, birth_date, is_active, created_at
                    FROM employee_access
                    WHERE club = ?
                    ORDER BY code, created_at DESC
                """, (club,))
            else:
                cursor.execute("""
                    SELECT id, code, club, telegram_user_id, full_name, username, 
                           phone, birth_date, is_active, created_at
                    FROM employee_access
                    ORDER BY club, code, created_at DESC
                """)
            
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {
                    'id': row[0],
                    'code': row[1],
                    'club': row[2],
                    'telegram_user_id': row[3],
                    'full_name': row[4],
                    'username': row[5],
                    'phone': row[6],
                    'birth_date': row[7],
                    'is_active': bool(row[8]),
                    'created_at': row[9]
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Ошибка получения списка доступов: {e}")
            conn.close()
            return []
    
    def is_admin(self, telegram_user_id: int) -> bool:
        """Проверить является ли пользователь админом"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id FROM admins 
                WHERE telegram_user_id = ?
            """, (telegram_user_id,))
            
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            print(f"Ошибка проверки админа: {e}")
            conn.close()
            return False
    
    def add_admin(self, telegram_user_id: int, name: str = None) -> bool:
        """Добавить админа"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO admins (telegram_user_id, name, added_at)
                VALUES (?, ?, ?)
            """, (telegram_user_id, name, datetime.now().isoformat()))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Ошибка добавления админа: {e}")
            conn.close()
            return False
    
    def get_all_admins(self) -> List[Dict]:
        """Получить список всех админов"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT telegram_user_id, name, added_at 
                FROM admins
                ORDER BY added_at
            """)
            
            rows = cursor.fetchall()
            conn.close()
            
            return [
                {
                    'telegram_user_id': row[0],
                    'name': row[1],
                    'added_at': row[2]
                }
                for row in rows
            ]
        except Exception as e:
            print(f"Ошибка получения админов: {e}")
            conn.close()
            return []

