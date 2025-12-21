"""
Скрипт ОТКАТА - убирает проверку имени при добавлении расходов стилистов
Теперь расходы добавляются ТОЛЬКО по коду, имя не проверяется
"""
import sqlite3
from database import Database

def show_current_state():
    """Показать текущие данные стилистов"""
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT club, code, name, amount, period_from, period_to
        FROM stylist_expenses
        ORDER BY club, code
    """)
    
    records = cursor.fetchall()
    
    print("📊 ТЕКУЩИЕ ДАННЫЕ СТИЛИСТОВ В БД:")
    print("=" * 80)
    
    total_moskvich = 0
    total_anora = 0
    
    for club, code, name, amount, period_from, period_to in records:
        print(f"{club:10} | {code:5} | {name:20} | {amount:10.2f} | {period_from} - {period_to}")
        if club == 'Москвич':
            total_moskvich += amount
        else:
            total_anora += amount
    
    print("=" * 80)
    print(f"ИТОГО Москвич: {total_moskvich:.2f}")
    print(f"ИТОГО Анора:   {total_anora:.2f}")
    print(f"ВСЕГО:         {total_moskvich + total_anora:.2f}")
    print()
    
    conn.close()

if __name__ == "__main__":
    print("\n🔍 ПРОВЕРКА ДАННЫХ СТИЛИСТОВ\n")
    show_current_state()
    
    print("\n📝 РЕШЕНИЕ:")
    print("Имена изменились после нормализации.")
    print("Теперь нужно изменить код reports.py:")
    print("Убрать проверку имени в строке 97-101")
    print("\nРасходы стилистов будут добавляться ТОЛЬКО по коду (Д13, D14 и т.д.)")
    print("Имя не будет проверяться - главное совпадение кода!")

