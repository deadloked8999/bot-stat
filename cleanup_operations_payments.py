import shutil
from datetime import datetime
from database import Database
from parser import DataParser

# Создаём бэкап
backup_name = f"bot_data.db.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
shutil.copy('/root/bot-stat/bot_data.db', f'/root/bot-stat/{backup_name}')
print(f"Создан бэкап: {backup_name}\n")

db = Database()
conn = db.get_connection()
cursor = conn.cursor()

print("=" * 60)
print("ОЧИСТКА ТАБЛИЦ operations И payments")
print("=" * 60)

# === 1. НОРМАЛИЗАЦИЯ КОДОВ ===
print("\n=== ШАГ 1: НОРМАЛИЗАЦИЯ КОДОВ ===\n")

# Получаем все уникальные коды из operations
cursor.execute("SELECT DISTINCT code, club FROM operations")
operations_codes = cursor.fetchall()

print(f"Найдено уникальных кодов в operations: {len(operations_codes)}")

normalized_count = 0
for old_code, club in operations_codes:
    new_code = DataParser.normalize_code(old_code)
    
    if old_code != new_code:
        print(f"  {club}: {old_code} → {new_code}")
        
        # Обновляем operations
        cursor.execute("""
            UPDATE operations
            SET code = ?
            WHERE club = ? AND code = ?
        """, (new_code, club, old_code))
        
        normalized_count += cursor.rowcount

print(f"\nОбновлено записей в operations: {normalized_count}")

# Получаем все уникальные коды из payments
cursor.execute("SELECT DISTINCT code, club FROM payments")
payments_codes = cursor.fetchall()

print(f"\nНайдено уникальных кодов в payments: {len(payments_codes)}")

normalized_count = 0
for old_code, club in payments_codes:
    new_code = DataParser.normalize_code(old_code)
    
    if old_code != new_code:
        print(f"  {club}: {old_code} → {new_code}")
        
        # Обновляем payments
        cursor.execute("""
            UPDATE payments
            SET code = ?
            WHERE club = ? AND code = ?
        """, (new_code, club, old_code))
        
        normalized_count += cursor.rowcount

print(f"\nОбновлено записей в payments: {normalized_count}")
conn.commit()

# === 2. УДАЛЕНИЕ МУСОРНЫХ КОДОВ ===
print("\n=== ШАГ 2: УДАЛЕНИЕ МУСОРНЫХ КОДОВ ===\n")

trash_codes = [
    'АНАР-ДЕПОЗИТ', 'АРАМ', 'АРТУР', 'БОНУС', 'ВХОД',
    'ДЕПОЗИТ,БОНУС', 'НАТИК', 'САМВЕЛ-БОНУС+ДЕПОЗИТ',
    'ТАКС', 'Ф1', 'ФАРИД-ВХОД+БОНУС+ДЕПОЗИТ',
    'ДЕПОЗИТ', 'БОНУС+ДЕПОЗИТ', 'ВХОД+БОНУС'
]

# Нормализуем мусорные коды
trash_codes_normalized = [DataParser.normalize_code(code) for code in trash_codes]

deleted_ops = 0
deleted_pay = 0

for code in trash_codes_normalized:
    # Удаляем из operations
    cursor.execute("""
        DELETE FROM operations WHERE code = ?
    """, (code,))
    ops_deleted = cursor.rowcount
    deleted_ops += ops_deleted
    
    # Удаляем из payments
    cursor.execute("""
        DELETE FROM payments WHERE code = ?
    """, (code,))
    pay_deleted = cursor.rowcount
    deleted_pay += pay_deleted
    
    if ops_deleted > 0 or pay_deleted > 0:
        print(f"Удалено: {code} (operations: {ops_deleted}, payments: {pay_deleted})")

print(f"\nВсего удалено: operations={deleted_ops}, payments={deleted_pay}")
conn.commit()

# === 3. ОБЪЕДИНЕНИЕ ВАРИАНТОВ НАПИСАНИЯ ===
print("\n=== ШАГ 3: ОБЪЕДИНЕНИЕ ВАРИАНТОВ НАПИСАНИЯ ===\n")

merge_rules = {
    'Анора': {
        'ДЖ12': ['Dj12', 'ДЖ12'],
        'ДЖ17': ['Dj17', 'ДЖ17'],
        'БСТ67': ['Бст67', 'БСТ67'],
        'ОФ0': ['Оф0', 'ОФ0'],
        'ОФ3': ['Оф3', 'ОФ3'],
        'ОФ4': ['Оф4', 'оф4', 'ОФ4'],
        'ОФ7': ['Оф7', 'ОФ7'],
        'УБОРЩИЦА': ['УБОЩИЦА', 'Уборщица', 'Уборщица Анора', 'уборщица', 'УБОРЩИЦА']
    },
    'Москвич': {
        'УБОРЩИЦА': ['Уборщица', 'Уборщица Москвич', 'уборщица', 'УБОРЩИЦА']
    }
}

total_merged_ops = 0
total_merged_pay = 0

for club, rules in merge_rules.items():
    print(f"\n--- Клуб: {club} ---\n")
    
    for main_code, variants in rules.items():
        # Нормализуем главный код
        main_code = DataParser.normalize_code(main_code)
        
        for old_code in variants:
            # Нормализуем старый код
            old_code_normalized = DataParser.normalize_code(old_code)
            
            # Если после нормализации они совпадают - пропускаем
            if old_code_normalized == main_code:
                continue
            
            # Обновляем operations
            cursor.execute("""
                UPDATE operations
                SET code = ?
                WHERE club = ? AND code = ?
            """, (main_code, club, old_code_normalized))
            ops_updated = cursor.rowcount
            
            # Обновляем payments
            cursor.execute("""
                UPDATE payments
                SET code = ?
                WHERE club = ? AND code = ?
            """, (main_code, club, old_code_normalized))
            pay_updated = cursor.rowcount
            
            if ops_updated > 0 or pay_updated > 0:
                print(f"  {old_code_normalized} → {main_code} (operations: {ops_updated}, payments: {pay_updated})")
                total_merged_ops += ops_updated
                total_merged_pay += pay_updated

print(f"\nВсего объединено: operations={total_merged_ops}, payments={total_merged_pay}")
conn.commit()

# === 4. ИСПРАВЛЕНИЕ ИМЁН ===
print("\n=== ШАГ 4: ИСПРАВЛЕНИЕ ИМЁН ===\n")

# ОФ4: Ника → Вероника
cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Вероника'
    WHERE club = 'Анора' AND code = 'ОФ4' AND name_snapshot = 'Ника'
""")
ops_updated = cursor.rowcount

cursor.execute("""
    UPDATE payments
    SET name = 'Вероника'
    WHERE club = 'Анора' AND code = 'ОФ4' AND name = 'Ника'
""")
pay_updated = cursor.rowcount

if ops_updated > 0 or pay_updated > 0:
    print(f"ОФ4 Ника → Вероника (operations: {ops_updated}, payments: {pay_updated})")

# БСТ5: Леша → Алексей
cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Алексей'
    WHERE club = 'Анора' AND code = 'БСТ5' AND name_snapshot = 'Леша'
""")
ops_updated = cursor.rowcount

cursor.execute("""
    UPDATE payments
    SET name = 'Алексей'
    WHERE club = 'Анора' AND code = 'БСТ5' AND name = 'Леша'
""")
pay_updated = cursor.rowcount

if ops_updated > 0 or pay_updated > 0:
    print(f"БСТ5 Леша → Алексей (operations: {ops_updated}, payments: {pay_updated})")

conn.commit()

# === 5. ОБЪЕДИНЕНИЕ СБ ===
print("\n=== ШАГ 5: ОБЪЕДИНЕНИЕ СБ ===\n")

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

total_sb_ops = 0
total_sb_pay = 0

for main_code, variants in sb_merge_rules.items():
    # Нормализуем главный код
    main_code_normalized = DataParser.normalize_code(main_code)
    
    print(f"\nОбъединяю в {main_code_normalized}:")
    
    for old_code in variants:
        # Нормализуем старый код
        old_code_normalized = DataParser.normalize_code(old_code)
        
        # Если после нормализации они совпадают - пропускаем
        if old_code_normalized == main_code_normalized:
            continue
        
        # Ищем во всех клубах
        cursor.execute("""
            SELECT DISTINCT club FROM operations WHERE code = ?
        """, (old_code_normalized,))
        clubs_ops = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT DISTINCT club FROM payments WHERE code = ?
        """, (old_code_normalized,))
        clubs_pay = [row[0] for row in cursor.fetchall()]
        
        clubs = list(set(clubs_ops + clubs_pay))
        
        for club in clubs:
            # Обновляем operations
            cursor.execute("""
                UPDATE operations
                SET code = ?
                WHERE club = ? AND code = ?
            """, (main_code_normalized, club, old_code_normalized))
            ops_updated = cursor.rowcount
            total_sb_ops += ops_updated
            
            # Обновляем payments
            cursor.execute("""
                UPDATE payments
                SET code = ?
                WHERE club = ? AND code = ?
            """, (main_code_normalized, club, old_code_normalized))
            pay_updated = cursor.rowcount
            total_sb_pay += pay_updated
            
            if ops_updated > 0 or pay_updated > 0:
                print(f"  {club}: {old_code_normalized} → {main_code_normalized} (operations: {ops_updated}, payments: {pay_updated})")

print(f"\nВсего объединено СБ: operations={total_sb_ops}, payments={total_sb_pay}")
conn.commit()

conn.close()

print("\n" + "=" * 60)
print("✅ ОЧИСТКА ЗАВЕРШЕНА!")
print("=" * 60)

