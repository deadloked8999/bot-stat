from database import Database
from datetime import datetime

db = Database()
conn = db.get_connection()
cursor = conn.cursor()

print("=" * 60)
print("ОБЪЕДИНЕНИЕ УБОРЩИЦ ПО КЛУБАМ")
print("=" * 60)

# 1. UPDATE operations - имя = клуб
print("\n=== ШАГ 1: operations ===\n")

cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Москвич'
    WHERE code = 'УБОРЩИЦА' AND club = 'Москвич'
""")
print(f"Москвич: обновлено {cursor.rowcount} записей")

cursor.execute("""
    UPDATE operations
    SET name_snapshot = 'Анора'
    WHERE code = 'УБОРЩИЦА' AND club = 'Анора'
""")
print(f"Анора: обновлено {cursor.rowcount} записей")

conn.commit()

# 2. UPDATE payments - имя = клуб
print("\n=== ШАГ 2: payments ===\n")

cursor.execute("""
    UPDATE payments
    SET name = 'Москвич'
    WHERE code = 'УБОРЩИЦА' AND club = 'Москвич'
""")
print(f"Москвич: обновлено {cursor.rowcount} записей")

cursor.execute("""
    UPDATE payments
    SET name = 'Анора'
    WHERE code = 'УБОРЩИЦА' AND club = 'Анора'
""")
print(f"Анора: обновлено {cursor.rowcount} записей")

conn.commit()

# 3. UPDATE employees - имя = клуб
print("\n=== ШАГ 3: employees ===\n")

cursor.execute("""
    UPDATE employees
    SET full_name = club
    WHERE code = 'УБОРЩИЦА'
""")
print(f"Обновлено {cursor.rowcount} записей в employees")

conn.commit()

# 4. UPDATE employee_history - имя = клуб
print("\n=== ШАГ 4: employee_history ===\n")

cursor.execute("""
    UPDATE employee_history
    SET full_name = club
    WHERE code = 'УБОРЩИЦА'
""")
print(f"Обновлено {cursor.rowcount} записей в employee_history")

conn.commit()

conn.close()

print("\n" + "=" * 60)
print("✅ ОБЪЕДИНЕНИЕ ЗАВЕРШЕНО!")
print("=" * 60)

