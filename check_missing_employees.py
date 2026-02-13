#!/usr/bin/env python3
"""
Скрипт для проверки сотрудников из payments, которых нет в employees
"""
import sqlite3
import sys

# Путь к базе данных
db_path = 'bot_data.db'

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=" * 60)
    print("ПРОВЕРКА СОТРУДНИКОВ: payments vs employees")
    print("=" * 60)
    print()
    
    # 1. Количество уникальных сотрудников в payments
    cursor.execute("""
        SELECT COUNT(DISTINCT code || '-' || club) as total_in_payments
        FROM payments
    """)
    total_payments = cursor.fetchone()[0]
    print(f"1. Уникальных сотрудников в payments: {total_payments}")
    print()
    
    # 2. Количество сотрудников в employees
    cursor.execute("""
        SELECT COUNT(*) as total_in_employees
        FROM employees
    """)
    total_employees = cursor.fetchone()[0]
    print(f"2. Сотрудников в employees: {total_employees}")
    print()
    
    # 3. Количество "потерянных" сотрудников
    cursor.execute("""
        SELECT COUNT(*) as missing_employees
        FROM (
            SELECT DISTINCT p.code, p.club
            FROM payments p
            LEFT JOIN employees e ON p.code = e.code AND p.club = e.club
            WHERE e.code IS NULL
        )
    """)
    missing_count = cursor.fetchone()[0]
    print(f"3. Сотрудников в payments, но НЕТ в employees: {missing_count}")
    print()
    
    # 4. Список "потерянных" сотрудников
    if missing_count > 0:
        print("4. Список сотрудников (первые 50):")
        print("-" * 60)
        cursor.execute("""
            SELECT DISTINCT p.code, p.club, p.name
            FROM payments p
            LEFT JOIN employees e ON p.code = e.code AND p.club = e.club
            WHERE e.code IS NULL
            ORDER BY p.club, p.code
            LIMIT 50
        """)
        
        rows = cursor.fetchall()
        for code, club, name in rows:
            print(f"  {club:10} | {code:10} | {name}")
        
        if missing_count > 50:
            print(f"\n  ... и ещё {missing_count - 50} сотрудников")
    else:
        print("4. Все сотрудники из payments есть в employees! [OK]")
    
    print()
    print("=" * 60)
    
    conn.close()
    
except Exception as e:
    print(f"Ошибка: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

