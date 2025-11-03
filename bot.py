"""
Главный модуль Telegram бота для учета статистики
"""
import os
import re
from datetime import datetime
from typing import Dict, Optional

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)

import config
from database import Database
from parser import DataParser
from reports import ReportGenerator
from utils import (
    get_current_date,
    parse_date,
    parse_short_date,
    parse_date_range,
    get_week_range,
    parse_period,
    normalize_command,
    extract_club_from_text,
    format_operations_list
)


# Состояния пользователя
USER_STATES = {}

# Авторизованные пользователи
AUTHORIZED_USERS = set()

# Пин-код для доступа
PIN_CODE = "1664"


class UserState:
    """Класс для хранения состояния пользователя"""
    
    def __init__(self):
        self.club: Optional[str] = None
        self.mode: Optional[str] = None
        self.temp_nal_data: list = []
        self.temp_beznal_data: list = []
        self.current_date: str = get_current_date()
        
        # Для команды отчет
        self.report_club: Optional[str] = None
        
        # Для команды исправить
        self.edit_code: Optional[str] = None
        self.edit_date: Optional[str] = None
        self.edit_current_data: Optional[dict] = None
        
        # Для команды удалить
        self.delete_code: Optional[str] = None
        self.delete_date: Optional[str] = None
        self.delete_records: Optional[dict] = None
        
        # Для команды экспорт
        self.export_club: Optional[str] = None
        
        # Для сводного отчета
        self.merge_candidates: Optional[list] = None
        self.merge_period: Optional[tuple] = None
    
    def reset_input(self):
        """Сброс блочного ввода"""
        self.mode = None
        self.temp_nal_data = []
        self.temp_beznal_data = []
    
    def has_data(self) -> bool:
        """Проверка наличия данных"""
        return len(self.temp_nal_data) > 0 or len(self.temp_beznal_data) > 0


def get_user_state(user_id: int) -> UserState:
    """Получить состояние пользователя"""
    if user_id not in USER_STATES:
        USER_STATES[user_id] = UserState()
    return USER_STATES[user_id]


# Инициализация базы данных
db = Database()


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start и старт"""
    user_id = update.effective_user.id
    
    # Проверка авторизации
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("🔒 Введите пин-код для доступа:")
        return
    
    state = get_user_state(user_id)
    
    text = update.message.text.lower()
    
    # Определяем клуб
    club = None
    if 'москвич' in text:
        club = 'Москвич'
    elif 'анора' in text or 'anora' in text:
        club = 'Анора'
    
    if not club:
        await update.message.reply_text(
            "❌ Не указан клуб.\n\n"
            "Используйте:\n"
            "старт москвич\n"
            "старт анора"
        )
        return
    
    # Устанавливаем клуб и дату
    state.club = club
    state.current_date = get_current_date()
    state.reset_input()
    
    await update.message.reply_text(
        f"✅ Выбран клуб: {club}\n"
        f"📅 Дата: {state.current_date}\n\n"
        f"📋 Доступные команды:\n\n"
        f"💰 ВВОД ДАННЫХ:\n"
        f"• нал — начать ввод НАЛ\n"
        f"• безнал — начать ввод БЕЗНАЛ\n"
        f"• готово — завершить и сохранить\n\n"
        f"📊 ОТЧЁТЫ:\n"
        f"• отчет — получить отчёт\n"
        f"• выплаты КОД период — выплаты сотруднику\n\n"
        f"📝 РЕДАКТИРОВАНИЕ:\n"
        f"• список дата — просмотр записей\n"
        f"• исправить КОД дата — изменить\n"
        f"• удалить КОД дата — удалить\n\n"
        f"📤 ЭКСПОРТ:\n"
        f"• экспорт — экспорт в Excel\n\n"
        f"❓ СПРАВКА:\n"
        f"• помощь — все команды"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user_id = update.effective_user.id
    state = get_user_state(user_id)
    text = update.message.text.strip()
    text_lower = normalize_command(text)
    
    # Проверка авторизации
    if user_id not in AUTHORIZED_USERS:
        if text == PIN_CODE:
            AUTHORIZED_USERS.add(user_id)
            await update.message.reply_text(
                "✅ Доступ разрешён!\n\n"
                "Используйте команды:\n"
                "• старт москвич\n"
                "• старт анора"
            )
        else:
            await update.message.reply_text("🔒 Введите пин-код для доступа:")
        return
    
    # Команда "помощь"
    if text_lower in ['помощь', 'help']:
        await update.message.reply_text(
            "📋 СПИСОК КОМАНД:\n\n"
            "🏢 НАЧАЛО РАБОТЫ:\n"
            "• старт москвич / старт анора - выбор клуба\n\n"
            "💰 ВВОД ДАННЫХ:\n"
            "• нал - ввод НАЛ\n"
            "• безнал - ввод БЕЗНАЛ\n"
            "• готово - завершить ввод, указать дату и сохранить\n\n"
            "📊 ОТЧЁТЫ:\n"
            "• отчет - получить отчёт (выбор клуба + период)\n"
            "• выплаты КОД период - выплаты сотруднику\n\n"
            "📝 ПРОСМОТР И РЕДАКТИРОВАНИЕ:\n"
            "• список дата - показать записи за дату\n"
            "• исправить КОД дата - изменить данные сотрудника\n"
            "• удалить КОД дата - удалить данные сотрудника\n\n"
            "📤 ЭКСПОРТ:\n"
            "• экспорт - экспортировать отчёт в Excel\n\n"
            "📖 ФОРМАТЫ:\n"
            "• Дата: 30,10 или 30.10\n"
            "• Период: 10,06-11,08\n"
            "• Данные: Д7 Нади-6800 или Д1 Яна 2200"
        )
        return
    
    # Команда "старт москвич" или "старт анора" (обработка как текст)
    if text_lower.startswith('старт'):
        await start_command(update, context)
        return
    
    # Команда "нал"
    if text_lower == 'нал':
        if not state.club:
            await update.message.reply_text(
                "❌ Клуб не выбран.\n"
                "Используйте: старт москвич или старт анора"
            )
            return
        
        state.mode = 'нал'
        await update.message.reply_text(
            f"📝 Режим ввода: НАЛ\n"
            f"Клуб: {state.club}\n\n"
            f"Вставьте список данных.\n"
            f"После ввода всех данных (НАЛ и БЕЗНАЛ) напишите: готово"
        )
        return
    
    # Команда "безнал"
    if text_lower == 'безнал':
        if not state.club:
            await update.message.reply_text(
                "❌ Клуб не выбран.\n"
                "Используйте: старт москвич или старт анора"
            )
            return
        
        state.mode = 'безнал'
        await update.message.reply_text(
            f"📝 Режим ввода: БЕЗНАЛ\n"
            f"Клуб: {state.club}\n\n"
            f"Вставьте список данных.\n"
            f"После ввода всех данных (НАЛ и БЕЗНАЛ) напишите: готово"
        )
        return
    
    # Команда "готово"
    if text_lower == 'готово':
        if not state.has_data():
            await update.message.reply_text(
                "❌ Нет данных для обработки.\n"
                "Используйте команды: нал и безнал для ввода данных"
            )
            return
        
        # Показываем все принятые данные
        response_parts = []
        response_parts.append(f"📊 Принятые данные по клубу {state.club}:\n")
        
        total_nal = 0
        total_beznal = 0
        
        if state.temp_nal_data:
            response_parts.append("📗 НАЛ:")
            for item in state.temp_nal_data:
                response_parts.append(f"  {item['code']} {item['name']} — {item['amount']:.0f}")
                total_nal += item['amount']
            response_parts.append(f"  Итого НАЛ: {total_nal:.0f}\n")
        
        if state.temp_beznal_data:
            response_parts.append("📘 БЕЗНАЛ:")
            for item in state.temp_beznal_data:
                response_parts.append(f"  {item['code']} {item['name']} — {item['amount']:.0f}")
                total_beznal += item['amount']
            response_parts.append(f"  Итого БЕЗНАЛ: {total_beznal:.0f}\n")
        
        response_parts.append(f"💰 Всего: {total_nal + total_beznal:.0f}")
        response_parts.append("\n📅 Укажите дату (формат: 30,10 или 30.10):")
        
        await update.message.reply_text('\n'.join(response_parts))
        
        # Переходим в режим ожидания даты
        state.mode = 'awaiting_date'
        return
    
    # Обработка ввода даты после команды "готово"
    if state.mode == 'awaiting_date':
        success, parsed_date, error = parse_short_date(text)
        if not success:
            await update.message.reply_text(f"❌ {error}")
            return
        
        # Сохраняем все данные в БД
        saved_count = 0
        
        for item in state.temp_nal_data:
            db.add_or_update_operation(
                club=state.club,
                date=parsed_date,
                code=item['code'],
                name=item['name'],
                channel='нал',
                amount=item['amount'],
                original_line=item['original_line'],
                aggregate=True
            )
            saved_count += 1
        
        for item in state.temp_beznal_data:
            db.add_or_update_operation(
                club=state.club,
                date=parsed_date,
                code=item['code'],
                name=item['name'],
                channel='безнал',
                amount=item['amount'],
                original_line=item['original_line'],
                aggregate=True
            )
            saved_count += 1
        
        # Очищаем состояние
        state.reset_input()
        
        await update.message.reply_text(
            f"✅ Сохранено: клуб {state.club}, дата {parsed_date}\n"
            f"Записей: {saved_count}"
        )
        return
    
    # Блочный ввод данных
    if state.mode in ['нал', 'безнал']:
        # Парсим введенные данные
        successful, errors = DataParser.parse_block(text)
        
        if successful:
            # Сохраняем в соответствующий список
            if state.mode == 'нал':
                state.temp_nal_data.extend(successful)
            else:
                state.temp_beznal_data.extend(successful)
        
        if errors:
            error_msg = "⚠️ Ошибки при парсинге:\n" + '\n'.join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n... и ещё {len(errors) - 5} ошибок"
            await update.message.reply_text(error_msg)
        
        return
    
    # Команда "отчет"
    if text_lower == 'отчет':
        await update.message.reply_text(
            "Выберите клуб:\n"
            "• москвич\n"
            "• анора\n"
            "• оба"
        )
        state.mode = 'awaiting_report_club'
        return
    
    # Обработка выбора клуба для отчета
    if state.mode == 'awaiting_report_club':
        if text_lower in ['москвич', 'анора', 'оба']:
            state.report_club = text_lower
            await update.message.reply_text(
                "Укажите дату или период:\n"
                "• Одна дата: 12,12\n"
                "• Период: 10,06-11,08"
            )
            state.mode = 'awaiting_report_period'
        else:
            await update.message.reply_text(
                "❌ Неверный выбор. Выберите: москвич, анора или оба"
            )
        return
    
    # Обработка периода для отчета
    if state.mode == 'awaiting_report_period':
        # Проверяем, это одна дата или диапазон
        if '-' in text:
            # Диапазон дат: 10,06-11,08
            success, date_from, date_to, error = parse_date_range(text)
            if not success:
                await update.message.reply_text(f"❌ {error}")
                return
        else:
            # Одна дата: 12,12
            success, single_date, error = parse_short_date(text)
            if not success:
                await update.message.reply_text(f"❌ {error}")
                return
            date_from = single_date
            date_to = single_date
        
        # Генерируем отчет
        if state.report_club == 'оба':
            # Сначала отчеты по каждому клубу
            for club in ['Москвич', 'Анора']:
                await generate_and_send_report(update, club, date_from, date_to)
            
            # Затем проверяем возможность сводного отчета
            await prepare_merged_report(update, state, date_from, date_to)
        else:
            club = 'Москвич' if state.report_club == 'москвич' else 'Анора'
            await generate_and_send_report(update, club, date_from, date_to)
            state.mode = None
            state.report_club = None
        return
    
    # Обработка подтверждения объединения для сводного отчета
    if state.mode == 'awaiting_merge_confirm':
        await handle_merge_confirmation(update, state, text_lower)
        return
    
    # Команда "выплаты"
    if text_lower.startswith('выплаты'):
        await handle_payments_command(update, context, state, text)
        return
    
    # Команда "список"
    if text_lower.startswith('список'):
        await handle_list_command(update, context, state, text)
        return
    
    # Команда "исправить"
    if text_lower.startswith('исправить'):
        await handle_edit_command_new(update, context, state, text)
        return
    
    # Обработка ввода новых данных для исправления
    if state.mode == 'awaiting_edit_data':
        await handle_edit_input(update, context, state, text, text_lower)
        return
    
    # Команда "удалить"
    if text_lower.startswith('удалить'):
        await handle_delete_command_new(update, context, state, text)
        return
    
    # Обработка выбора что удалить
    if state.mode == 'awaiting_delete_choice':
        await handle_delete_choice(update, context, state, text_lower)
        return
    
    # Команда "экспорт"
    if text_lower == 'экспорт':
        await update.message.reply_text(
            "Выберите клуб для экспорта:\n"
            "• москвич\n"
            "• анора\n"
            "• оба"
        )
        state.mode = 'awaiting_export_club'
        return
    
    # Обработка выбора клуба для экспорта
    if state.mode == 'awaiting_export_club':
        if text_lower in ['москвич', 'анора', 'оба']:
            state.export_club = text_lower
            await update.message.reply_text(
                "Укажите дату или период:\n"
                "• 12,12\n"
                "• 10,06-11,08"
            )
            state.mode = 'awaiting_export_period'
        else:
            await update.message.reply_text(
                "❌ Неверный выбор. Выберите: москвич, анора или оба"
            )
        return
    
    # Обработка периода для экспорта
    if state.mode == 'awaiting_export_period':
        # Парсим период
        if '-' in text:
            success, date_from, date_to, error = parse_date_range(text)
            if not success:
                await update.message.reply_text(f"❌ {error}")
                return
        else:
            success, single_date, error = parse_short_date(text)
            if not success:
                await update.message.reply_text(f"❌ {error}")
                return
            date_from = single_date
            date_to = single_date
        
        # Экспортируем
        if state.export_club == 'оба':
            for club in ['Москвич', 'Анора']:
                await export_report(update, club, date_from, date_to)
        else:
            club = 'Москвич' if state.export_club == 'москвич' else 'Анора'
            await export_report(update, club, date_from, date_to)
        
        state.mode = None
        state.export_club = None
        return
    
    # Неизвестная команда
    await update.message.reply_text(
        "❓ Неизвестная команда.\n\n"
        "Доступные команды:\n"
        "• старт москвич | старт анора\n"
        "• нал — начать ввод НАЛ\n"
        "• безнал — начать ввод БЕЗНАЛ\n"
        "• готово — завершить ввод и записать\n"
        "• отчет — получить отчёт\n"
        "• выплаты КОД период — выплаты сотруднику\n"
        "• список дата — показать записи\n"
        "• исправить — редактировать\n"
        "• удалить — удалить запись"
    )


async def handle_save_command(update: Update, context: ContextTypes.DEFAULT_TYPE, state: UserState):
    """Обработка команды дата/записать"""
    if not state.club:
        await update.message.reply_text(
            "❌ Клуб не выбран.\n"
            "Используйте: старт москвич или старт анора"
        )
        return
    
    text = update.message.text.strip()
    parts = text.split()
    
    # Определяем дату
    target_date = state.current_date
    if len(parts) >= 2:
        potential_date = parts[-1]
        success, parsed_date, error = parse_date(potential_date)
        if success:
            target_date = parsed_date
    
    # Проверяем наличие данных для записи
    ready_nal = context.user_data.get('ready_нал', [])
    ready_beznal = context.user_data.get('ready_безнал', [])
    
    if not ready_nal and not ready_beznal:
        await update.message.reply_text(
            "❌ Нет данных для записи.\n"
            "Используйте: нал / безнал → вставьте данные → готово"
        )
        return
    
    # Записываем в БД
    saved_count = 0
    
    for item in ready_nal:
        db.add_or_update_operation(
            club=state.club,
            date=target_date,
            code=item['code'],
            name=item['name'],
            channel='нал',
            amount=item['amount'],
            original_line=item['original_line'],
            aggregate=True
        )
        saved_count += 1
    
    for item in ready_beznal:
        db.add_or_update_operation(
            club=state.club,
            date=target_date,
            code=item['code'],
            name=item['name'],
            channel='безнал',
            amount=item['amount'],
            original_line=item['original_line'],
            aggregate=True
        )
        saved_count += 1
    
    # Очищаем временные данные
    context.user_data['ready_нал'] = []
    context.user_data['ready_безнал'] = []
    
    await update.message.reply_text(
        f"✅ Сохранено: клуб {state.club}, дата {target_date}\n"
        f"Записей: {saved_count}\n\n"
        f"Можно вводить следующий день или запросить: прошу отчёт"
    )


async def handle_report_command(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                               state: UserState, text: str):
    """Обработка команды прошу отчёт"""
    # Определяем клуб
    club = extract_club_from_text(text)
    if not club:
        club = state.club
    
    if not club:
        await update.message.reply_text(
            "❌ Клуб не указан.\n"
            "Используйте: прошу отчёт москвич | прошу отчёт анора\n"
            "Или сначала выберите клуб: старт москвич"
        )
        return
    
    # Определяем период
    date_from, date_to = None, None
    
    # Поиск явного диапазона
    period_match = re.search(r'(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})', text)
    if period_match:
        date_from = period_match.group(1)
        date_to = period_match.group(2)
    elif 'неделя' in text or 'недел' in text:
        date_from, date_to = get_week_range()
    else:
        # По умолчанию - текущая неделя
        date_from, date_to = get_week_range()
    
    # Получаем данные
    operations = db.get_operations_by_period(club, date_from, date_to)
    
    if not operations:
        await update.message.reply_text(
            f"📊 Отчет по клубу {club}\n"
            f"Период: {date_from} .. {date_to}\n\n"
            f"Данных нет."
        )
        return
    
    # Генерируем отчет
    report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(operations)
    
    report_text = ReportGenerator.format_report_text(
        report_rows, totals, check_ok, totals_recalc, club, f"{date_from} .. {date_to}"
    )
    
    await update.message.reply_text(report_text, parse_mode='Markdown')


async def handle_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                              state: UserState, text: str):
    """Обработка команды список"""
    if not state.club:
        await update.message.reply_text(
            "❌ Клуб не выбран.\n"
            "Используйте: старт москвич или старт анора"
        )
        return
    
    parts = text.split()
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ Укажите дату.\n"
            "Пример: список 2025-11-03"
        )
        return
    
    date_str = parts[1]
    success, parsed_date, error = parse_date(date_str)
    
    if not success:
        await update.message.reply_text(f"❌ {error}")
        return
    
    operations = db.get_operations_by_date(state.club, parsed_date)
    
    response = format_operations_list(operations, parsed_date, state.club)
    await update.message.reply_text(response)


async def handle_edit_command_new(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                  state: UserState, text: str):
    """Новая интерактивная команда исправить"""
    if not state.club:
        await update.message.reply_text(
            "❌ Клуб не выбран.\n"
            "Используйте: старт москвич или старт анора"
        )
        return
    
    # Формат: исправить Д1 30,10
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text(
            "❌ Неверный формат.\n"
            "Пример: исправить Д1 30,10"
        )
        return
    
    code = DataParser.normalize_code(parts[1])
    date_str = parts[2]
    
    # Парсим дату
    success, parsed_date, error = parse_short_date(date_str)
    if not success:
        await update.message.reply_text(f"❌ {error}")
        return
    
    # Получаем текущие данные
    operations = db.get_operations_by_date(state.club, parsed_date)
    
    # Фильтруем по коду
    code_ops = [op for op in operations if op['code'] == code]
    
    if not code_ops:
        await update.message.reply_text(
            f"❌ Записи для {code} за {parsed_date} не найдены."
        )
        return
    
    # Показываем текущие данные
    response = [f"📊 Текущие данные {code} за {parsed_date}:"]
    current_data = {}
    
    for op in code_ops:
        response.append(f"• {op['channel'].upper()}: {op['amount']:.0f}")
        current_data[op['channel']] = op['amount']
    
    response.append("\nВведите новые значения:")
    response.append("Формат: нал 1000")
    response.append("        безнал 2000")
    response.append("        готово")
    
    await update.message.reply_text('\n'.join(response))
    
    # Сохраняем состояние
    state.edit_code = code
    state.edit_date = parsed_date
    state.edit_current_data = current_data
    state.mode = 'awaiting_edit_data'


async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                            state: UserState, text: str, text_lower: str):
    """Обработка ввода новых значений для исправления"""
    if text_lower == 'готово':
        # Сохраняем изменения (если были)
        if hasattr(state, 'edit_new_values') and state.edit_new_values:
            for channel, amount in state.edit_new_values.items():
                db.update_operation(state.club, state.edit_date, state.edit_code, channel, amount)
            
            await update.message.reply_text(
                f"✅ Данные {state.edit_code} за {state.edit_date} обновлены"
            )
            
            # Очищаем
            state.mode = None
            state.edit_new_values = {}
        else:
            await update.message.reply_text("❌ Не введено новых значений")
        return
    
    # Парсим ввод: нал 1000 или безнал 2000
    parts = text_lower.split()
    if len(parts) == 2 and parts[0] in ['нал', 'безнал']:
        channel = parts[0]
        success, amount, error = DataParser.parse_amount(parts[1])
        
        if success:
            if not hasattr(state, 'edit_new_values'):
                state.edit_new_values = {}
            state.edit_new_values[channel] = amount
            await update.message.reply_text(f"✓ {channel.upper()}: {amount:.0f}")
        else:
            await update.message.reply_text(f"❌ {error}")
    else:
        await update.message.reply_text(
            "❌ Неверный формат. Используйте:\n"
            "нал 1000\n"
            "безнал 2000\n"
            "готово"
        )


async def handle_delete_command_new(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                    state: UserState, text: str):
    """Новая интерактивная команда удалить"""
    if not state.club:
        await update.message.reply_text(
            "❌ Клуб не выбран.\n"
            "Используйте: старт москвич или старт анора"
        )
        return
    
    # Формат: удалить Д1 30,10
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text(
            "❌ Неверный формат.\n"
            "Пример: удалить Д1 30,10"
        )
        return
    
    code = DataParser.normalize_code(parts[1])
    date_str = parts[2]
    
    # Парсим дату
    success, parsed_date, error = parse_short_date(date_str)
    if not success:
        await update.message.reply_text(f"❌ {error}")
        return
    
    # Получаем данные
    operations = db.get_operations_by_date(state.club, parsed_date)
    code_ops = [op for op in operations if op['code'] == code]
    
    if not code_ops:
        await update.message.reply_text(
            f"❌ Записи для {code} за {parsed_date} не найдены."
        )
        return
    
    # Показываем записи
    response = [f"📊 Записи {code} за {parsed_date}:"]
    delete_records = {}
    
    for op in code_ops:
        response.append(f"• {op['channel'].upper()}: {op['amount']:.0f}")
        delete_records[op['channel']] = op['amount']
    
    response.append("\nЧто удалить?")
    response.append("• нал")
    response.append("• безнал")
    response.append("• обе")
    
    await update.message.reply_text('\n'.join(response))
    
    # Сохраняем состояние
    state.delete_code = code
    state.delete_date = parsed_date
    state.delete_records = delete_records
    state.mode = 'awaiting_delete_choice'


async def handle_delete_choice(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                               state: UserState, choice: str):
    """Обработка выбора что удалить"""
    if choice in ['нал', 'безнал']:
        # Удаляем один канал
        if choice in state.delete_records:
            db.delete_operation(state.club, state.delete_date, state.delete_code, choice)
            await update.message.reply_text(
                f"✅ Удалено: {state.delete_code} {choice.upper()} за {state.delete_date}"
            )
        else:
            await update.message.reply_text(f"❌ Записи {choice.upper()} нет")
    
    elif choice in ['обе', 'все']:
        # Удаляем оба канала
        deleted = []
        for channel in ['нал', 'безнал']:
            if channel in state.delete_records:
                db.delete_operation(state.club, state.delete_date, state.delete_code, channel)
                deleted.append(channel.upper())
        
        if deleted:
            await update.message.reply_text(
                f"✅ Удалено: {state.delete_code} {', '.join(deleted)} за {state.delete_date}"
            )
        else:
            await update.message.reply_text("❌ Нет записей для удаления")
    
    else:
        await update.message.reply_text(
            "❌ Неверный выбор. Выберите: нал, безнал или обе"
        )
        return
    
    # Очищаем состояние
    state.mode = None


async def export_report(update: Update, club: str, date_from: str, date_to: str):
    """Экспорт отчёта в XLSX"""
    operations = db.get_operations_by_period(club, date_from, date_to)
    
    if not operations:
        await update.message.reply_text(
            f"📊 Нет данных для экспорта\n"
            f"Клуб: {club}\n"
            f"Период: {date_from} .. {date_to}"
        )
        return
    
    # Генерируем отчет
    report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(operations)
    
    # Создаем XLSX
    club_translit = 'moskvich' if club == 'Москвич' else 'anora'
    filename = f"otchet_{club_translit}_{date_from}_{date_to}.xlsx"
    
    ReportGenerator.generate_xlsx(
        report_rows, totals, club, f"{date_from} .. {date_to}", filename
    )
    
    # Отправляем файл
    with open(filename, 'rb') as f:
        await update.message.reply_document(
            document=f,
            filename=filename,
            caption=f"📊 Экспорт: {club}\nПериод: {date_from} .. {date_to}"
        )
    
    # Удаляем временный файл
    os.remove(filename)


async def prepare_merged_report(update: Update, state: UserState, date_from: str, date_to: str):
    """Подготовка сводного отчета с проверкой совпадений"""
    # Получаем данные по обоим клубам
    ops_moskvich = db.get_operations_by_period('Москвич', date_from, date_to)
    ops_anora = db.get_operations_by_period('Анора', date_from, date_to)
    
    # Группируем по сотрудникам (код)
    from collections import defaultdict
    
    employees_m = defaultdict(lambda: {'names': set(), 'nal': 0, 'beznal': 0})
    employees_a = defaultdict(lambda: {'names': set(), 'nal': 0, 'beznal': 0})
    
    for op in ops_moskvich:
        code = op['code']
        employees_m[code]['names'].add(op['name'])
        if op['channel'] == 'нал':
            employees_m[code]['nal'] += op['amount']
        else:
            employees_m[code]['beznal'] += op['amount']
    
    for op in ops_anora:
        code = op['code']
        employees_a[code]['names'].add(op['name'])
        if op['channel'] == 'нал':
            employees_a[code]['nal'] += op['amount']
        else:
            employees_a[code]['beznal'] += op['amount']
    
    # Ищем совпадения по КОД+ИМЯ
    merge_candidates = []
    all_codes = set(employees_m.keys()) | set(employees_a.keys())
    
    for code in all_codes:
        if code in employees_m and code in employees_a:
            # Код есть в обоих клубах
            names_m = employees_m[code]['names']
            names_a = employees_a[code]['names']
            
            # Проверяем совпадение имён
            common_names = names_m & names_a
            
            if common_names:
                # Есть полное совпадение КОД+ИМЯ
                name = list(common_names)[0]
                merge_candidates.append({
                    'code': code,
                    'name': name,
                    'moskvich': {'nal': employees_m[code]['nal'], 'beznal': employees_m[code]['beznal']},
                    'anora': {'nal': employees_a[code]['nal'], 'beznal': employees_a[code]['beznal']}
                })
    
    if not merge_candidates:
        # Совпадений нет - генерируем сводный без объединения (просто все записи)
        await update.message.reply_text(
            "ℹ️ Совпадений не найдено. Генерируется сводный отчёт из всех записей...\n"
        )
        
        # Создаём сводный из всех операций
        all_ops = ops_moskvich + ops_anora
        
        if all_ops:
            report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(all_ops)
            report_text = ReportGenerator.format_report_text(
                report_rows, totals, check_ok, totals_recalc, 
                "📊 СВОДНЫЙ ОТЧЁТ (Москвич + Анора)", f"{date_from} .. {date_to}"
            )
            await update.message.reply_text(report_text, parse_mode='Markdown')
            
            # Экспорт
            filename = f"otchet_svodny_{date_from}_{date_to}.xlsx"
            ReportGenerator.generate_xlsx(
                report_rows, totals, "СВОДНЫЙ (Москвич + Анора)", f"{date_from} .. {date_to}", filename
            )
            with open(filename, 'rb') as f:
                await update.message.reply_document(
                    document=f, filename=filename,
                    caption=f"📊 СВОДНЫЙ ОТЧЁТ (Оба клуба)\nПериод: {date_from} .. {date_to}"
                )
            os.remove(filename)
        
        state.mode = None
        state.report_club = None
        return
    
    # Показываем список для подтверждения
    response = ["📋 Найдены совпадения для объединения:\n"]
    
    for i, candidate in enumerate(merge_candidates, 1):
        response.append(f"{i}. {candidate['name']} {candidate['code']}")
        response.append(f"   • Москвич: НАЛ {candidate['moskvich']['nal']:.0f}, БЕЗНАЛ {candidate['moskvich']['beznal']:.0f}")
        response.append(f"   • Анора: НАЛ {candidate['anora']['nal']:.0f}, БЕЗНАЛ {candidate['anora']['beznal']:.0f}")
        response.append("")
    
    response.append("Объединить? Напишите:")
    response.append("• ок - объединить все")
    response.append("• 1,2 - НЕ объединять строки (через запятую)")
    
    await update.message.reply_text('\n'.join(response))
    
    # Сохраняем кандидатов
    state.merge_candidates = merge_candidates
    state.merge_period = (date_from, date_to)
    state.mode = 'awaiting_merge_confirm'


async def handle_merge_confirmation(update: Update, state: UserState, choice: str):
    """Обработка подтверждения объединения"""
    if choice == 'ок' or choice == 'ok':
        # Объединяем все
        excluded = set()
    else:
        # Парсим список исключений: 1,2,3
        try:
            excluded = set(int(x.strip()) - 1 for x in choice.split(','))
        except:
            await update.message.reply_text(
                "❌ Неверный формат. Используйте: ок или 1,2,3"
            )
            return
    
    # Генерируем сводный отчет
    await generate_merged_report(update, state, excluded)
    
    # Очищаем
    state.mode = None
    state.report_club = None
    state.merge_candidates = None
    state.merge_period = None


async def generate_merged_report(update: Update, state: UserState, excluded: set):
    """Генерация сводного отчета из ОБОИХ клубов"""
    date_from, date_to = state.merge_period
    
    # Получаем ВСЕ данные обоих клубов
    ops_m = db.get_operations_by_period('Москвич', date_from, date_to)
    ops_a = db.get_operations_by_period('Анора', date_from, date_to)
    
    # Создаём объединённый список операций для СВОДНОГО отчёта
    merged_ops = []
    
    # Множество обработанных пар (код, имя)
    processed = set()
    
    # 1. Добавляем ОБЪЕДИНЁННЫЕ записи (которые пользователь подтвердил)
    for i, candidate in enumerate(state.merge_candidates):
        code = candidate['code']
        name = candidate['name']
        
        if i not in excluded:
            # ОБЪЕДИНЯЕМ - суммируем из обоих клубов
            total_nal = candidate['moskvich']['nal'] + candidate['anora']['nal']
            total_beznal = candidate['moskvich']['beznal'] + candidate['anora']['beznal']
            
            if total_nal > 0:
                merged_ops.append({
                    'code': code, 'name': name, 'channel': 'нал', 
                    'amount': total_nal, 'date': date_from
                })
            if total_beznal > 0:
                merged_ops.append({
                    'code': code, 'name': name, 'channel': 'безнал', 
                    'amount': total_beznal, 'date': date_from
                })
            
            processed.add((code, name))
        else:
            # НЕ объединяем - добавляем раздельно с пометкой клуба
            if candidate['moskvich']['nal'] > 0:
                merged_ops.append({
                    'code': code, 'name': f"{name} (Москвич)", 'channel': 'нал',
                    'amount': candidate['moskvich']['nal'], 'date': date_from
                })
            if candidate['moskvich']['beznal'] > 0:
                merged_ops.append({
                    'code': code, 'name': f"{name} (Москвич)", 'channel': 'безнал',
                    'amount': candidate['moskvich']['beznal'], 'date': date_from
                })
            if candidate['anora']['nal'] > 0:
                merged_ops.append({
                    'code': code, 'name': f"{name} (Анора)", 'channel': 'нал',
                    'amount': candidate['anora']['nal'], 'date': date_from
                })
            if candidate['anora']['beznal'] > 0:
                merged_ops.append({
                    'code': code, 'name': f"{name} (Анора)", 'channel': 'безнал',
                    'amount': candidate['anora']['beznal'], 'date': date_from
                })
            
            processed.add((code, name))
    
    # 2. Добавляем ВСЕ ОСТАЛЬНЫЕ записи (уникальные для каждого клуба)
    for op in ops_m + ops_a:
        if (op['code'], op['name']) not in processed:
            merged_ops.append(op)
    
    # Генерируем СВОДНЫЙ отчет
    if merged_ops:
        report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(merged_ops)
        report_text = ReportGenerator.format_report_text(
            report_rows, totals, check_ok, totals_recalc, 
            "📊 СВОДНЫЙ ОТЧЁТ (Москвич + Анора)", f"{date_from} .. {date_to}"
        )
        await update.message.reply_text(report_text, parse_mode='Markdown')
        
        # Экспорт сводного
        filename = f"otchet_svodny_{date_from}_{date_to}.xlsx"
        ReportGenerator.generate_xlsx(
            report_rows, totals, "СВОДНЫЙ (Москвич + Анора)", f"{date_from} .. {date_to}", filename
        )
        with open(filename, 'rb') as f:
            await update.message.reply_document(
                document=f, filename=filename,
                caption=f"📊 СВОДНЫЙ ОТЧЁТ (Оба клуба)\nПериод: {date_from} .. {date_to}"
            )
        os.remove(filename)
    else:
        await update.message.reply_text("ℹ️ Нет данных для сводного отчета")


async def generate_and_send_report(update: Update, club: str, date_from: str, date_to: str):
    """Генерация и отправка отчета"""
    operations = db.get_operations_by_period(club, date_from, date_to)
    
    if not operations:
        await update.message.reply_text(
            f"📊 Отчет по клубу {club}\n"
            f"Период: {date_from} .. {date_to}\n\n"
            f"Данных нет."
        )
        return
    
    # Генерируем отчет
    report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(operations)
    
    report_text = ReportGenerator.format_report_text(
        report_rows, totals, check_ok, totals_recalc, club, f"{date_from} .. {date_to}"
    )
    
    await update.message.reply_text(report_text, parse_mode='Markdown')
    
    # Создаем XLSX
    club_translit = 'moskvich' if club == 'Москвич' else 'anora'
    filename = f"otchet_{club_translit}_{date_from}_{date_to}.xlsx"
    
    ReportGenerator.generate_xlsx(
        report_rows, totals, club, f"{date_from} .. {date_to}", filename
    )
    
    # Отправляем файл
    with open(filename, 'rb') as f:
        await update.message.reply_document(
            document=f,
            filename=filename,
            caption=f"📊 Отчет по клубу {club}\nПериод: {date_from} .. {date_to}"
        )
    
    # Удаляем временный файл
    os.remove(filename)


async def handle_payments_command(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                  state: UserState, text: str):
    """Обработка команды выплаты"""
    # Формат: выплаты Д1 30,10-1,11
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text(
            "❌ Неверный формат.\n"
            "Пример: выплаты Д1 30,10-1,11"
        )
        return
    
    code = DataParser.normalize_code(parts[1])
    period_str = parts[2]
    
    # Парсим период (одна дата или диапазон)
    if '-' in period_str:
        # Диапазон: 10,06-11,08
        success, date_from, date_to, error = parse_date_range(period_str)
        if not success:
            await update.message.reply_text(f"❌ {error}")
            return
    else:
        # Одна дата: 12,12
        success, single_date, error = parse_short_date(period_str)
        if not success:
            await update.message.reply_text(f"❌ {error}")
            return
        date_from = single_date
        date_to = single_date
    
    # Получаем выплаты сотрудника (по всем клубам если не указан активный клуб)
    payments = db.get_employee_payments(code, date_from, date_to, state.club)
    
    if not payments:
        await update.message.reply_text(
            f"📊 Выплаты сотруднику {code}\n"
            f"Период: {date_from} .. {date_to}\n\n"
            f"Данных нет."
        )
        return
    
    # Формируем ответ
    response_parts = []
    response_parts.append(f"📊 Выплаты сотруднику {code}")
    response_parts.append(f"Период: {date_from} .. {date_to}\n")
    
    total = 0
    current_club = None
    
    for payment in payments:
        if current_club != payment['club']:
            if current_club is not None:
                response_parts.append("")
            response_parts.append(f"🏢 Клуб: {payment['club']}")
            current_club = payment['club']
        
        response_parts.append(
            f"  {payment['date']} | {payment['channel'].upper():7} | "
            f"{payment['name']:15} | {payment['amount']:.0f}"
        )
        total += payment['amount']
    
    response_parts.append("")
    response_parts.append(f"💰 Всего выплат: {total:.0f}")
    
    await update.message.reply_text('\n'.join(response_parts))


def main():
    """Запуск бота"""
    # Проверяем токен
    if config.BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        print("❌ Ошибка: не установлен токен бота!")
        print("Установите переменную окружения TELEGRAM_BOT_TOKEN")
        print("или измените значение в config.py")
        return
    
    # Создаем приложение
    app = Application.builder().token(config.BOT_TOKEN).build()
    
    # Регистрируем обработчики
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Запускаем бота
    print("🤖 Бот запущен!")
    print("Для остановки нажмите Ctrl+C")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()

