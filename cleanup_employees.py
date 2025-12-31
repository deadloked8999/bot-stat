from database import Database
from datetime import datetime

db = Database()
conn = db.get_connection()
cursor = conn.cursor()

print("=" * 60)
print("АВТОМАТИЧЕСКАЯ ОЧИСТКА БАЗЫ СОТРУДНИКОВ")
print("=" * 60)

# === 1. ОБЪЕДИНЕНИЕ ДУБЛЕЙ ПО РЕГИСТРУ ===
print("\n=== ШАГ 1: ОБЪЕДИНЕНИЕ ДУБЛЕЙ ПО РЕГИСТРУ ===\n")

# Определяем правила объединения
merge_rules = {
    'Анора': {
        'ДЖ12': ['Dj12'],
        'ДЖ17': ['Dj17'],
        'БСТ67': ['Бст67'],
        'БСТ5': ['БСТ5 Леша'],  # Леша → Алексей
        'ОФ0': ['Оф0'],
        'ОФ3': ['Оф3'],
        'ОФ4': ['Оф4', 'оф4'],  # Ника и Вероника - один человек
        'ОФ7': ['Оф7'],
        'УБОРЩИЦА': ['УБОЩИЦА', 'Уборщица', 'Уборщица Анора', 'уборщица']
    },
    'Москвич': {
        'УБОРЩИЦА': ['Уборщица', 'Уборщица Москвич', 'уборщица']
    }
}

for club, rules in merge_rules.items():
    print(f"\n--- Клуб: {club} ---\n")
    
    for main_code, variants in rules.items():
        for old_code in variants:
            cursor.execute("""
                SELECT code FROM employees 
                WHERE club = ? AND code = ?
            """, (club, old_code))
            
            if cursor.fetchone():
                print(f"Объединяю: {old_code} → {main_code}")
                
                # Обновляем operations
                cursor.execute("""
                    UPDATE operations
                    SET code = ?
                    WHERE club = ? AND code = ?
                """, (main_code, club, old_code))
                ops_updated = cursor.rowcount
                
                # Обновляем payments
                cursor.execute("""
                    UPDATE payments
                    SET code = ?
                    WHERE club = ? AND code = ?
                """, (main_code, club, old_code))
                pay_updated = cursor.rowcount
                
                # Удаляем из employees
                cursor.execute("""
                    DELETE FROM employees
                    WHERE club = ? AND code = ?
                """, (club, old_code))
                
                print(f"  Operations: {ops_updated}, Payments: {pay_updated}")

conn.commit()

# === 2. СПЕЦИАЛЬНАЯ ОБРАБОТКА ОФ4 (Ника → Вероника) ===
print("\n=== ШАГ 2: ОФ4 Ника → Вероника ===\n")

cursor.execute("""
    UPDATE employees
    SET full_name = 'Вероника'
    WHERE club = 'Анора' AND code = 'ОФ4' AND full_name = 'Ника'
""")

if cursor.rowcount > 0:
    print("Обновлено имя: ОФ4 Ника → Вероника")

cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Вероника'
    WHERE club = 'Анора' AND code = 'ОФ4' AND name_snapshot = 'Ника'
""")

cursor.execute("""
    UPDATE payments
    SET name = 'Вероника'
    WHERE club = 'Анора' AND code = 'ОФ4' AND name = 'Ника'
""")

conn.commit()

# === 3. СПЕЦИАЛЬНАЯ ОБРАБОТКА БСТ5 (Леша → Алексей) ===
print("\n=== ШАГ 3: БСТ5 Леша → Алексей ===\n")

cursor.execute("""
    UPDATE employees
    SET full_name = 'Алексей'
    WHERE club = 'Анора' AND code = 'БСТ5' AND full_name = 'Леша'
""")

if cursor.rowcount > 0:
    print("Обновлено имя: БСТ5 Леша → Алексей")

cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Алексей'
    WHERE club = 'Анора' AND code = 'БСТ5' AND name_snapshot = 'Леша'
""")

cursor.execute("""
    UPDATE payments
    SET name = 'Алексей'
    WHERE club = 'Анора' AND code = 'БСТ5' AND name = 'Леша'
""")

conn.commit()

# === 4. ОБЪЕДИНЕНИЕ СБ ===
print("\n=== ШАГ 4: ОБЪЕДИНЕНИЕ СБ ===\n")

sb_merge_rules = {
    'СБ-АЛЕКСАНДР ЧЕРНЫЙ': [
        'СБ - Александр Черный',
        'СБ-Александр Черный',
        'СБ-Александр Чёрный'
    ],
    'СБ-ВИЛЛИ': [
        'СБ - Вилли',
        'СБ-Вилли'
    ],
    'СБ-ГАСАН': [
        'СБ - Гасан',
        'СБ-Гасан'
    ],
    'СБ-ДЕНИС ЕРМАКОВ': [
        'СБ - Денис Ермаков',
        'СБ-Денис Ермаков'
    ],
    'СБ-ДМИТРИЙ ВАСЕНЁВ': [
        'СБ - Дмитрий Васенев',
        'СБ - Дмитрий Васенёв',
        'СБ-Дмитрий Васенев'
    ],
    'СБ-ДМИТРИЙ НЕМОВ': [
        'СБ - Дмитрий Немов',
        'СБ-Дима Немов',
        'СБ-Дмитрий Немов'
    ],
    'СБ-ДМИТРИЙ ПЕСКОВ': [
        'СБ - Дима Песков',
        'СБ - Дмитрий Песков',
        'СБ-Дима Песков',
        'СБ-Дмитрий Песков'
    ],
    'СБ-ЕВГЕНИЙ ЕГОРОВ': [
        'СБ - Евгений Егоров',
        'СБ - Женя Егоров',
        'СБ-Женя Егоров'
    ],
    'СБ-ИВАН КОРОЛЕВ': [
        'СБ - Иван Королев',
        'СБ-Иван Королев'
    ],
    'СБ-ОЛЕГ ЖУКЕВИЧ': [
        'СБ - Олег Жукевич',
        'СБ-Олег Жукевич'
    ]
}

for main_code, variants in sb_merge_rules.items():
    print(f"\nОбъединяю в {main_code}:")
    
    for old_code in variants:
        # Ищем во всех клубах
        cursor.execute("""
            SELECT DISTINCT club FROM employees WHERE code = ?
        """, (old_code,))
        
        clubs = [row[0] for row in cursor.fetchall()]
        
        for club in clubs:
            print(f"  {club}: {old_code}")
            
            # Обновляем operations
            cursor.execute("""
                UPDATE operations
                SET code = ?
                WHERE club = ? AND code = ?
            """, (main_code, club, old_code))
            
            # Обновляем payments
            cursor.execute("""
                UPDATE payments
                SET code = ?
                WHERE club = ? AND code = ?
            """, (main_code, club, old_code))
            
            # Удаляем из employees
            cursor.execute("""
                DELETE FROM employees
                WHERE club = ? AND code = ?
            """, (club, old_code))

conn.commit()

# === 5. УДАЛЕНИЕ МУСОРНЫХ КОДОВ ===
print("\n=== ШАГ 5: УДАЛЕНИЕ МУСОРНЫХ КОДОВ ===\n")

trash_codes = [
    'АНАР-ДЕПОЗИТ', 'АРАМ', 'АРТУР', 'БОНУС', 'ВХОД',
    'ДЕПОЗИТ,БОНУС', 'НАТИК', 'САМВЕЛ-БОНУС+ДЕПОЗИТ',
    'ТАКС', 'Ф1', 'ФАРИД-ВХОД+БОНУС+ДЕПОЗИТ'
]

for code in trash_codes:
    cursor.execute("""
        SELECT club FROM employees WHERE code = ?
    """, (code,))
    
    clubs = [row[0] for row in cursor.fetchall()]
    
    if clubs:
        print(f"Удаляю мусорный код: {code} ({', '.join(clubs)})")
        
        cursor.execute("""
            DELETE FROM employees WHERE code = ?
        """, (code,))

conn.commit()

# === 6. СОЗДАНИЕ ГЛАВНЫХ ЗАПИСЕЙ ===
print("\n=== ШАГ 6: СОЗДАНИЕ ГЛАВНЫХ ЗАПИСЕЙ ===\n")

# Убеждаемся что главные записи существуют
main_employees = [
    ('Анора', 'ОФ4', 'Вероника'),
    ('Анора', 'БСТ5', 'Алексей'),
    ('Анора', 'УБОРЩИЦА', 'Анора'),
    ('Москвич', 'УБОРЩИЦА', 'Москвич'),
]

for club, code, name in main_employees:
    cursor.execute("""
        INSERT OR IGNORE INTO employees
        (code, club, full_name, is_active, created_at)
        VALUES (?, ?, ?, 1, ?)
    """, (code, club, name, datetime.now().isoformat()))
    
    if cursor.rowcount > 0:
        print(f"Создана запись: {club} / {code} - {name}")

conn.commit()
conn.close()

print("\n" + "=" * 60)
print("✅ ОЧИСТКА ЗАВЕРШЕНА!")
print("=" * 60)

