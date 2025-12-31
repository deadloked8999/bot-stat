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
print("ФИНАЛЬНАЯ ОЧИСТКА operations И payments")
print("=" * 60)

# === ШАГ 1: ОБЪЕДИНЕНИЕ ДУБЛЕЙ ПО РЕГИСТРУ ===
print("\n=== ШАГ 1: ОБЪЕДИНЕНИЕ ДУБЛЕЙ ===\n")

merge_rules = {
    'ДЖ12': ['Dj12', 'dj12', 'DJ12'],
    'ДЖ17': ['Dj17', 'dj17', 'DJ17'],
    'БСТ67': ['Бст67', 'бст67'],
    'ОФ0': ['Оф0', 'оф0'],
    'ОФ3': ['Оф3', 'оф3'],
    'ОФ4': ['Оф4', 'оф4'],
    'ОФ7': ['Оф7', 'оф7'],
    'УБОРЩИЦА': ['УБОЩИЦА', 'Уборщица', 'Уборщица Анора', 'Уборщица Москвич', 'уборщица']
}

for main_code, variants in merge_rules.items():
    total_ops = 0
    total_pay = 0
    
    for old_code in variants:
        # UPDATE operations (ВСЕ записи с этим кодом)
        cursor.execute("""
            UPDATE operations
            SET code = ?
            WHERE code = ?
        """, (main_code, old_code))
        total_ops += cursor.rowcount
        
        # UPDATE payments
        cursor.execute("""
            UPDATE payments
            SET code = ?
            WHERE code = ?
        """, (main_code, old_code))
        total_pay += cursor.rowcount
    
    if total_ops > 0 or total_pay > 0:
        print(f"{main_code}: обновлено ops={total_ops}, pay={total_pay}")

conn.commit()

# === ШАГ 2: ОБЪЕДИНЕНИЕ СБ ===
print("\n=== ШАГ 2: ОБЪЕДИНЕНИЕ СБ ===\n")

sb_merge_rules = {
    'СБ-АЛЕКСАНДР ЧЕРНЫЙ': [
        'СБ - Александр Черный',
        'СБ-Александр Черный',
        'СБ-Александр Чёрный',
        'СБ-александр черный',
        'Сб-Александр Черный'
    ],
    'СБ-ВИЛЛИ': [
        'СБ - Вилли',
        'СБ-Вилли',
        'СБ-вилли',
        'Сб-Вилли'
    ],
    'СБ-ГАСАН': [
        'СБ - Гасан',
        'СБ-Гасан',
        'СБ-гасан',
        'Сб-Гасан'
    ],
    'СБ-ДЕНИС ЕРМАКОВ': [
        'СБ - Денис Ермаков',
        'СБ-Денис Ермаков',
        'СБ-денис ермаков',
        'Сб-Денис Ермаков'
    ],
    'СБ-ДМИТРИЙ ВАСЕНЁВ': [
        'СБ - Дмитрий Васенев',
        'СБ - Дмитрий Васенёв',
        'СБ-Дмитрий Васенев',
        'СБ-Дмитрий Васенёв',
        'СБ-дмитрий васенев',
        'СБ-дмитрий васенёв',
        'Сб-Дмитрий Васенев'
    ],
    'СБ-ДМИТРИЙ НЕМОВ': [
        'СБ - Дмитрий Немов',
        'СБ - Дима Немов',
        'СБ-Дмитрий Немов',
        'СБ-Дима Немов',
        'СБ-дмитрий немов',
        'СБ-дима немов',
        'Сб-Дмитрий Немов'
    ],
    'СБ-ДМИТРИЙ ПЕСКОВ': [
        'СБ - Дима Песков',
        'СБ - Дмитрий Песков',
        'СБ-Дима Песков',
        'СБ-Дмитрий Песков',
        'СБ-дима песков',
        'СБ-дмитрий песков',
        'Сб-Дмитрий Песков'
    ],
    'СБ-ЕВГЕНИЙ ЕГОРОВ': [
        'СБ - Евгений Егоров',
        'СБ - Женя Егоров',
        'СБ-Евгений Егоров',
        'СБ-Женя Егоров',
        'СБ-евгений егоров',
        'СБ-женя егоров',
        'Сб-Евгений Егоров'
    ],
    'СБ-ИВАН КОРОЛЕВ': [
        'СБ - Иван Королев',
        'СБ-Иван Королев',
        'СБ-иван королев',
        'Сб-Иван Королев'
    ],
    'СБ-ОЛЕГ ЖУКЕВИЧ': [
        'СБ - Олег Жукевич',
        'СБ-Олег Жукевич',
        'СБ-олег жукевич',
        'Сб-Олег Жукевич'
    ]
}

for main_code, variants in sb_merge_rules.items():
    total_ops = 0
    total_pay = 0
    
    for old_code in variants:
        cursor.execute("""
            UPDATE operations
            SET code = ?
            WHERE code = ?
        """, (main_code, old_code))
        total_ops += cursor.rowcount
        
        cursor.execute("""
            UPDATE payments
            SET code = ?
            WHERE code = ?
        """, (main_code, old_code))
        total_pay += cursor.rowcount
    
    if total_ops > 0 or total_pay > 0:
        print(f"{main_code}: обновлено ops={total_ops}, pay={total_pay}")

conn.commit()

# === ШАГ 3: УДАЛЕНИЕ МУСОРНЫХ ЗАПИСЕЙ ===
print("\n=== ШАГ 3: УДАЛЕНИЕ МУСОРА ===\n")

trash_codes = [
    'АНАР-ДЕПОЗИТ', 'АРАМ', 'АРТУР', 'БОНУС', 'ВХОД',
    'ДЕПОЗИТ,БОНУС', 'НАТИК', 'САМВЕЛ-БОНУС+ДЕПОЗИТ',
    'ТАКС', 'Ф1', 'ФАРИД-ВХОД+БОНУС+ДЕПОЗИТ'
]

for code in trash_codes:
    cursor.execute("DELETE FROM operations WHERE code = ?", (code,))
    ops_deleted = cursor.rowcount
    
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
    WHERE code = 'ОФ4' AND name_snapshot = 'Ника'
""")
ops_updated = cursor.rowcount

cursor.execute("""
    UPDATE payments
    SET name = 'Вероника'
    WHERE code = 'ОФ4' AND name = 'Ника'
""")
pay_updated = cursor.rowcount

print(f"ОФ4 Ника → Вероника: ops={ops_updated}, pay={pay_updated}")

# БСТ5 Леша → Алексей
cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Алексей'
    WHERE code = 'БСТ5' AND name_snapshot = 'Леша'
""")
ops_updated = cursor.rowcount

cursor.execute("""
    UPDATE payments
    SET name = 'Алексей'
    WHERE code = 'БСТ5' AND name = 'Леша'
""")
pay_updated = cursor.rowcount

print(f"БСТ5 Леша → Алексей: ops={ops_updated}, pay={pay_updated}")

# СБ имена → нормализация
sb_name_fixes = {
    'СБ-ДМИТРИЙ НЕМОВ': ['Дима Немов'],
    'СБ-ДМИТРИЙ ПЕСКОВ': ['Дима Песков'],
    'СБ-ЕВГЕНИЙ ЕГОРОВ': ['Женя Егоров'],
    'СБ-ДМИТРИЙ ВАСЕНЁВ': ['Дмитрий Васенев']
}

for main_code, old_names in sb_name_fixes.items():
    # Извлекаем правильное имя из кода (после СБ-)
    correct_name = main_code.replace('СБ-', '').replace('ДМИТРИЙ', 'Дмитрий').replace('ЕВГЕНИЙ', 'Евгений').replace('ВАСЕНЁВ', 'Васенёв')
    
    for old_name in old_names:
        cursor.execute("""
            UPDATE operations
            SET name_snapshot = ?
            WHERE code = ? AND name_snapshot = ?
        """, (correct_name, main_code, old_name))
        
        cursor.execute("""
            UPDATE payments
            SET name = ?
            WHERE code = ? AND name = ?
        """, (correct_name, main_code, old_name))

conn.commit()
conn.close()

print("\n" + "=" * 60)
print("✅ ОЧИСТКА ЗАВЕРШЕНА!")
print("=" * 60)

