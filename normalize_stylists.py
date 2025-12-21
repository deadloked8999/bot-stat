"""
Скрипт для нормализации данных стилистов в БД
Приводит коды и имена к формату из operations
"""
import sqlite3
from database import Database
from parser import DataParser

def normalize_stylist_data():
    """Нормализация всех записей стилистов"""
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    # Получаем все записи стилистов
    cursor.execute("SELECT id, club, code, name, amount FROM stylist_expenses")
    stylist_records = cursor.fetchall()
    
    print(f"📊 Найдено записей стилистов: {len(stylist_records)}")
    print()
    
    updated_count = 0
    not_found_count = 0
    
    for record in stylist_records:
        record_id, club, old_code, old_name, amount = record
        
        # Нормализуем код
        normalized_code = DataParser.normalize_code(old_code)
        
        print(f"🔍 Обрабатываю: {club} | {old_code} ({old_name}) → {normalized_code}")
        
        # Ищем этот код в operations для данного клуба
        cursor.execute("""
            SELECT DISTINCT name_snapshot 
            FROM operations 
            WHERE club = ? AND code = ?
            LIMIT 1
        """, (club, normalized_code))
        
        result = cursor.fetchone()
        
        if result:
            correct_name = result[0]
            
            # Обновляем запись
            cursor.execute("""
                UPDATE stylist_expenses 
                SET code = ?, name = ?
                WHERE id = ?
            """, (normalized_code, correct_name, record_id))
            
            print(f"  ✅ Обновлено: {normalized_code} {correct_name}")
            updated_count += 1
        else:
            # Если не нашли в operations - просто нормализуем код
            cursor.execute("""
                UPDATE stylist_expenses 
                SET code = ?
                WHERE id = ?
            """, (normalized_code, record_id))
            
            print(f"  ⚠️ Код обновлен, но сотрудник не найден в operations: {normalized_code}")
            not_found_count += 1
    
    conn.commit()
    conn.close()
    
    print()
    print("=" * 60)
    print(f"✅ Обновлено записей: {updated_count}")
    print(f"⚠️ Не найдено в operations: {not_found_count}")
    print(f"📊 Всего обработано: {len(stylist_records)}")
    print("=" * 60)

if __name__ == "__main__":
    print("🚀 ЗАПУСК НОРМАЛИЗАЦИИ ДАННЫХ СТИЛИСТОВ")
    print("=" * 60)
    normalize_stylist_data()
    print("\n✅ ГОТОВО!")

