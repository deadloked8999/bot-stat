"""
Модуль генерации отчетов
"""
from typing import List, Dict, Tuple
from collections import defaultdict
import csv
from io import StringIO
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment


class ReportGenerator:
    """Генератор отчетов"""
    
    @staticmethod
    def calculate_report(operations: List[Dict]) -> Tuple[List[Dict], Dict, Dict, bool]:
        """
        Расчет отчета по операциям
        Возвращает: (строки_отчета, итоги_по_строкам, итоги_пересчет, проверка_ок)
        """
        # Группируем по сотрудникам (код)
        employee_data = defaultdict(lambda: {
            'names': set(),
            'nal': 0.0,
            'beznal': 0.0
        })
        
        # Пересчет для проверки
        total_nal_raw = 0.0
        total_beznal_raw = 0.0
        
        for op in operations:
            code = op['code']
            name = op['name']
            channel = op['channel']
            amount = op['amount']
            
            employee_data[code]['names'].add(name)
            
            if channel == 'нал':
                employee_data[code]['nal'] += amount
                total_nal_raw += amount
            elif channel == 'безнал':
                employee_data[code]['beznal'] += amount
                total_beznal_raw += amount
        
        # Формируем строки отчета
        report_rows = []
        total_nal = 0.0
        total_beznal = 0.0
        total_minus10 = 0.0
        total_itog = 0.0
        
        for code in sorted(employee_data.keys()):
            data = employee_data[code]
            
            # Имя (если разные - берем первое и помечаем)
            names_list = list(data['names'])
            name = names_list[0]
            name_comment = " (⚠️ разные имена)" if len(names_list) > 1 else ""
            
            nal = round(data['nal'], 2)
            beznal = round(data['beznal'], 2)
            minus10 = round(beznal * 0.10, 2)
            itog = round(nal + (beznal - minus10), 2)
            
            report_rows.append({
                'name': name + name_comment,
                'code': code,
                'nal': nal,
                'beznal': beznal,
                'minus10': minus10,
                'itog': itog
            })
            
            total_nal += nal
            total_beznal += beznal
            total_minus10 += minus10
            total_itog += itog
        
        # Итоги по строкам
        totals_by_rows = {
            'nal': round(total_nal, 2),
            'beznal': round(total_beznal, 2),
            'minus10': round(total_minus10, 2),
            'itog': round(total_itog, 2)
        }
        
        # Пересчет для проверки
        recalc_minus10 = round(total_beznal_raw * 0.10, 2)
        recalc_itog = round(total_nal_raw + (total_beznal_raw - recalc_minus10), 2)
        
        totals_recalc = {
            'nal': round(total_nal_raw, 2),
            'beznal': round(total_beznal_raw, 2),
            'minus10': recalc_minus10,
            'itog': recalc_itog
        }
        
        # Проверка совпадения
        check_ok = (
            totals_by_rows['nal'] == totals_recalc['nal'] and
            totals_by_rows['beznal'] == totals_recalc['beznal'] and
            totals_by_rows['minus10'] == totals_recalc['minus10'] and
            totals_by_rows['itog'] == totals_recalc['itog']
        )
        
        return report_rows, totals_by_rows, totals_recalc, check_ok
    
    @staticmethod
    def format_report_text(report_rows: List[Dict], totals: Dict, 
                          check_ok: bool, totals_recalc: Dict,
                          club: str, period: str) -> str:
        """
        Форматирование отчета для вывода в Telegram
        """
        if not report_rows:
            return f"📊 Отчет по клубу {club} за {period}\n\nДанных нет."
        
        result = []
        result.append(f"📊 ОТЧЕТ")
        result.append(f"Клуб: {club}")
        result.append(f"Период: {period}")
        result.append("")
        
        # Заголовок таблицы
        result.append("```")
        result.append(f"{'Имя':<20} {'Код':<6} {'Нал':>10} {'Безнал':>10} {'10%':>10} {'Итог':>12}")
        result.append("-" * 80)
        
        # Строки отчета
        for row in report_rows:
            name_display = row['name'][:20]  # Обрезаем длинные имена
            result.append(
                f"{name_display:<20} {row['code']:<6} "
                f"{row['nal']:>10.2f} {row['beznal']:>10.2f} "
                f"{row['minus10']:>10.2f} {row['itog']:>12.2f}"
            )
        
        # Итоги
        result.append("-" * 80)
        result.append(
            f"{'ИТОГО':<20} {'':<6} "
            f"{totals['nal']:>10.2f} {totals['beznal']:>10.2f} "
            f"{totals['minus10']:>10.2f} {totals['itog']:>12.2f}"
        )
        result.append("```")
        result.append("")
        
        # Проверка
        if check_ok:
            result.append("✅ Сверка столбцов: совпадает")
        else:
            result.append("❗ Сверка столбцов: РАСХОЖДЕНИЕ")
            result.append("Пересчёт:")
            result.append(f"  Нал: {totals['nal']} vs {totals_recalc['nal']}")
            result.append(f"  Безнал: {totals['beznal']} vs {totals_recalc['beznal']}")
            result.append(f"  10%: {totals['minus10']} vs {totals_recalc['minus10']}")
            result.append(f"  Итог: {totals['itog']} vs {totals_recalc['itog']}")
        
        return '\n'.join(result)
    
    @staticmethod
    def generate_csv(report_rows: List[Dict], totals: Dict) -> str:
        """
        Генерация CSV
        """
        output = StringIO()
        writer = csv.writer(output)
        
        # Заголовок
        writer.writerow(['Имя', 'Код', 'Нал', 'Безнал', '10% от безнала', 'Итог (нал + безнал − 10%)'])
        
        # Данные
        for row in report_rows:
            writer.writerow([
                row['name'],
                row['code'],
                row['nal'],
                row['beznal'],
                row['minus10'],
                row['itog']
            ])
        
        # Итоги
        writer.writerow([
            'ИТОГО',
            '',
            totals['nal'],
            totals['beznal'],
            totals['minus10'],
            totals['itog']
        ])
        
        return output.getvalue()
    
    @staticmethod
    def generate_xlsx(report_rows: List[Dict], totals: Dict, 
                     club: str, period: str, filename: str) -> str:
        """
        Генерация XLSX файла
        """
        wb = Workbook()
        ws = wb.active
        ws.title = "Отчет"
        
        # Заголовок
        ws['A1'] = f"Отчет по клубу {club}"
        ws['A1'].font = Font(bold=True, size=14)
        ws['A2'] = f"Период: {period}"
        ws['A2'].font = Font(size=11)
        
        # Шапка таблицы
        headers = ['Имя', 'Код', 'Нал', 'Безнал', '10% от безнала', 'Итог (нал + безнал − 10%)']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=4, column=col, value=header)
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal='center')
        
        # Данные
        row_num = 5
        for row_data in report_rows:
            ws.cell(row=row_num, column=1, value=row_data['name'])
            ws.cell(row=row_num, column=2, value=row_data['code'])
            ws.cell(row=row_num, column=3, value=row_data['nal'])
            ws.cell(row=row_num, column=4, value=row_data['beznal'])
            ws.cell(row=row_num, column=5, value=row_data['minus10'])
            ws.cell(row=row_num, column=6, value=row_data['itog'])
            row_num += 1
        
        # Итоги
        ws.cell(row=row_num, column=1, value='ИТОГО').font = Font(bold=True)
        ws.cell(row=row_num, column=3, value=totals['nal']).font = Font(bold=True)
        ws.cell(row=row_num, column=4, value=totals['beznal']).font = Font(bold=True)
        ws.cell(row=row_num, column=5, value=totals['minus10']).font = Font(bold=True)
        ws.cell(row=row_num, column=6, value=totals['itog']).font = Font(bold=True)
        
        # Ширина столбцов
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 8
        ws.column_dimensions['C'].width = 12
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 15
        ws.column_dimensions['F'].width = 25
        
        # Сохраняем
        wb.save(filename)
        return filename

