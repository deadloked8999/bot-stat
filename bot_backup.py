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


class UserState:
    """Класс для хранения состояния пользователя"""
    
    def __init__(self):
        self.club: Optional[str] = None
        self.mode: Optional[str] = None  # 'нал', 'безнал', 'awaiting_date', 'awaiting_report_club', 'awaiting_report_period', None
        self.temp_nal_data: list = []  # Временные данные НАЛ
        self.temp_beznal_data: list = []  # Временные данные БЕЗНАЛ
        self.current_date: str = get_current_date()
        self.report_club: Optional[str] = None  # Для команды отчет
    
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
        f"Доступные команды:\n"
        f"• нал — начать ввод НАЛ\n"
        f"• безнал — начать ввод БЕЗНАЛ\n"
        f"• готово — завершить ввод блока\n"
        f"• дата/записать — сохранить данные\n"
        f"• прошу отчёт — получить отчёт\n"
        f"• список ГГГГ-ММ-ДД — показать записи за дату\n"
        f"• исправить — редактировать запись\n"
        f"• удалить — удалить запись\n"
        f"• экспорт — экспортировать отчёт"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстовых сообщений"""
    user_id = update.effective_user.id
    state = get_user_state(user_id)
    text = update.message.text.strip()
    text_lower = normalize_command(text)
    
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
        if not state.mode:
            await update.message.reply_text(
                "❌ Не активирован режим ввода.\n"
                "Используйте: нал или безнал"
            )
            return
        
        if not state.temp_data:
            await update.message.reply_text(
                "❌ Нет данных для обработки.\n"
                "Вставьте список перед командой готово"
            )
            return
        
        # Обрабатываем накопленные данные
        accumulated_text = '\n'.join(state.temp_data)
        successful, errors = DataParser.parse_block(accumulated_text)
        
        # Формируем ответ
        response = DataParser.format_parse_result(
            successful, errors, state.mode, state.club
        )
        
        await update.message.reply_text(response)
        
        # Сохраняем успешные записи во временное хранилище
        # (они будут записаны в БД по команде "дата/записать")
        if successful:
            # Помечаем данные как готовые к записи
            context.user_data[f'ready_{state.mode}'] = successful
        
        # Сбрасываем режим ввода
        state.temp_data = []
        state.mode = None
        
        await update.message.reply_text(
            "\n💾 Для сохранения данных используйте:\n"
            "дата/записать\n"
            "или\n"
            "дата/записать ГГГГ-ММ-ДД"
        )
        return
    
    # Блочный ввод данных
    if state.mode in ['нал', 'безнал']:
        # Накапливаем данные
        state.temp_data.append(text)
        return
    
    # Команда "дата/записать"
    if text_lower.startswith('дата/записать') or text_lower.startswith('дата записать'):
        await handle_save_command(update, context, state)
        return
    
    # Команда "прошу отчёт"
    if 'прошу отчёт' in text_lower or 'прошу отчет' in text_lower:
        await handle_report_command(update, context, state, text_lower)
        return
    
    # Команда "список"
    if text_lower.startswith('список'):
        await handle_list_command(update, context, state, text)
        return
    
    # Команда "исправить"
    if text_lower.startswith('исправить'):
        await handle_edit_command(update, context, state, text)
        return
    
    # Команда "удалить"
    if text_lower.startswith('удалить'):
        await handle_delete_command(update, context, state, text)
        return
    
    # Команда "экспорт"
    if text_lower.startswith('экспорт'):
        await handle_export_command(update, context, state, text_lower)
        return
    
    # Неизвестная команда
    await update.message.reply_text(
        "❓ Неизвестная команда.\n\n"
        "Доступные команды:\n"
        "• старт москвич | старт анора\n"
        "• нал | безнал\n"
        "• готово\n"
        "• дата/записать [ГГГГ-ММ-ДД]\n"
        "• прошу отчёт [параметры]\n"
        "• список ГГГГ-ММ-ДД\n"
        "• исправить код канал сумма дата\n"
        "• удалить код канал дата\n"
        "• экспорт неделя [клуб]"
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


async def handle_edit_command(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                              state: UserState, text: str):
    """Обработка команды исправить"""
    if not state.club:
        await update.message.reply_text(
            "❌ Клуб не выбран.\n"
            "Используйте: старт москвич или старт анора"
        )
        return
    
    # Формат: исправить Д1 нал 2500 2025-11-03
    parts = text.split()
    if len(parts) < 5:
        await update.message.reply_text(
            "❌ Неверный формат.\n"
            "Пример: исправить Д1 нал 2500 2025-11-03"
        )
        return
    
    code = DataParser.normalize_code(parts[1])
    channel = parts[2].lower()
    amount_str = parts[3]
    date_str = parts[4]
    
    # Проверяем канал
    if channel not in ['нал', 'безнал']:
        await update.message.reply_text(
            "❌ Канал должен быть 'нал' или 'безнал'"
        )
        return
    
    # Парсим сумму
    success_amount, amount, error_amount = DataParser.parse_amount(amount_str)
    if not success_amount:
        await update.message.reply_text(f"❌ {error_amount}")
        return
    
    # Парсим дату
    success_date, parsed_date, error_date = parse_date(date_str)
    if not success_date:
        await update.message.reply_text(f"❌ {error_date}")
        return
    
    # Обновляем
    success, message = db.update_operation(state.club, parsed_date, code, channel, amount)
    
    if success:
        await update.message.reply_text(f"✅ {message}")
    else:
        await update.message.reply_text(f"❌ {message}")


async def handle_delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                state: UserState, text: str):
    """Обработка команды удалить"""
    if not state.club:
        await update.message.reply_text(
            "❌ Клуб не выбран.\n"
            "Используйте: старт москвич или старт анора"
        )
        return
    
    # Формат: удалить Д1 безнал 2025-11-03
    parts = text.split()
    if len(parts) < 4:
        await update.message.reply_text(
            "❌ Неверный формат.\n"
            "Пример: удалить Д1 безнал 2025-11-03"
        )
        return
    
    code = DataParser.normalize_code(parts[1])
    channel = parts[2].lower()
    date_str = parts[3]
    
    # Проверяем канал
    if channel not in ['нал', 'безнал']:
        await update.message.reply_text(
            "❌ Канал должен быть 'нал' или 'безнал'"
        )
        return
    
    # Парсим дату
    success_date, parsed_date, error_date = parse_date(date_str)
    if not success_date:
        await update.message.reply_text(f"❌ {error_date}")
        return
    
    # Удаляем
    success, message = db.delete_operation(state.club, parsed_date, code, channel)
    
    if success:
        await update.message.reply_text(f"✅ {message}")
    else:
        await update.message.reply_text(f"❌ {message}")


async def handle_export_command(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                state: UserState, text: str):
    """Обработка команды экспорт"""
    # Определяем клуб
    club = extract_club_from_text(text)
    if not club:
        club = state.club
    
    if not club:
        await update.message.reply_text(
            "❌ Клуб не указан.\n"
            "Используйте: экспорт неделя москвич | экспорт неделя анора"
        )
        return
    
    # Определяем период (по умолчанию - текущая неделя)
    date_from, date_to = get_week_range()
    
    # Получаем данные
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
            caption=f"📊 Отчет по клубу {club}\nПериод: {date_from} .. {date_to}"
        )
    
    # Удаляем временный файл
    os.remove(filename)


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

