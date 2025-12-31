import shutil
from datetime import datetime
from database import Database

# Бэкап
backup_name = f"bot_data.db.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
shutil.copy('/root/bot-stat/bot_data.db', f'/root/bot-stat/{backup_name}')
print(f"Создан бэкап: {backup_name}\n")

db = Database()
conn = db.get_connection()
cursor = conn.cursor()

print("=" * 60)
print("ОЧИСТКА operations И payments")
print("=" * 60)

# === ШАГ 1: ОБЪЕДИНЕНИЕ ДУБЛЕЙ ПО РЕГИСТРУ ===
print("\n=== ШАГ 1: ОБЪЕДИНЕНИЕ ДУБЛЕЙ ===\n")

merge_rules = {
    'Анора': {
        'ДЖ12': ['Dj12'],
        'ДЖ17': ['Dj17'],
        'БСТ67': ['Бст67'],
        'ОФ0': ['Оф0'],
        'ОФ3': ['Оф3'],
        'ОФ4': ['Оф4', 'оф4'],
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
            
            if ops_updated > 0 or pay_updated > 0:
                print(f"{old_code} → {main_code}: ops={ops_updated}, pay={pay_updated}")

conn.commit()

# === ШАГ 2: ОБЪЕДИНЕНИЕ СБ ===
print("\n=== ШАГ 2: ОБЪЕДИНЕНИЕ СБ ===\n")

sb_merge_rules = {
    'СБ-АЛЕКСАНДР ЧЕРНЫЙ': [
        'СБ - Александр Черный',
        'СБ-Александр Черный',
        'СБ-Александр Чёрный'
    ],
    'СБ-ВИЛЛИ': ['СБ - Вилли', 'СБ-Вилли'],
    'СБ-ГАСАН': ['СБ - Гасан', 'СБ-Гасан'],
    'СБ-ДЕНИС ЕРМАКОВ': ['СБ - Денис Ермаков', 'СБ-Денис Ермаков'],
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
    'СБ-ИВАН КОРОЛЕВ': ['СБ - Иван Королев', 'СБ-Иван Королев'],
    'СБ-ОЛЕГ ЖУКЕВИЧ': ['СБ - Олег Жукевич', 'СБ-Олег Жукевич']
}

for main_code, variants in sb_merge_rules.items():
    print(f"\nОбъединяю в {main_code}:")
    
    for old_code in variants:
        # Обновляем operations
        cursor.execute("""
            UPDATE operations
            SET code = ?
            WHERE code = ?
        """, (main_code, old_code))
        ops_updated = cursor.rowcount
        
        # Обновляем payments
        cursor.execute("""
            UPDATE payments
            SET code = ?
            WHERE code = ?
        """, (main_code, old_code))
        pay_updated = cursor.rowcount
        
        if ops_updated > 0 or pay_updated > 0:
            print(f"  {old_code}: ops={ops_updated}, pay={pay_updated}")

conn.commit()

# === ШАГ 3: УДАЛЕНИЕ МУСОРНЫХ КОДОВ ===
print("\n=== ШАГ 3: УДАЛЕНИЕ МУСОРА ===\n")

trash_codes = [
    'АНАР-ДЕПОЗИТ', 'АРАМ', 'АРТУР', 'БОНУС', 'ВХОД',
    'ДЕПОЗИТ,БОНУС', 'НАТИК', 'САМВЕЛ-БОНУС+ДЕПОЗИТ',
    'ТАКС', 'Ф1', 'ФАРИД-ВХОД+БОНУС+ДЕПОЗИТ'
]

for code in trash_codes:
    # Удаляем из operations
    cursor.execute("DELETE FROM operations WHERE code = ?", (code,))
    ops_deleted = cursor.rowcount
    
    # Удаляем из payments
    cursor.execute("DELETE FROM payments WHERE code = ?", (code,))
    pay_deleted = cursor.rowcount
    
    if ops_deleted > 0 or pay_deleted > 0:
        print(f"Удалено {code}: ops={ops_deleted}, pay={pay_deleted}")

conn.commit()

# === ШАГ 4: ИСПРАВЛЕНИЕ ИМЁН ===
print("\n=== ШАГ 4: ИСПРАВЛЕНИЕ ИМЁН ===\n")

# ОФ4 Ника → Вероника
cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Вероника'
    WHERE club = 'Анора' AND code = 'ОФ4' AND name_snapshot = 'Ника'
""")
print(f"ОФ4 Ника → Вероника (operations): {cursor.rowcount}")

cursor.execute("""
    UPDATE payments
    SET name = 'Вероника'
    WHERE club = 'Анора' AND code = 'ОФ4' AND name = 'Ника'
""")
print(f"ОФ4 Ника → Вероника (payments): {cursor.rowcount}")

# БСТ5 Леша → Алексей
cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Алексей'
    WHERE club = 'Анора' AND code = 'БСТ5' AND name_snapshot = 'Леша'
""")
print(f"БСТ5 Леша → Алексей (operations): {cursor.rowcount}")

cursor.execute("""
    UPDATE payments
    SET name = 'Алексей'
    WHERE club = 'Анора' AND code = 'БСТ5' AND name = 'Леша'
""")
print(f"БСТ5 Леша → Алексей (payments): {cursor.rowcount}")

conn.commit()
conn.close()

print("\n" + "=" * 60)
print("✅ ОЧИСТКА ЗАВЕРШЕНА!")
print("=" * 60)

