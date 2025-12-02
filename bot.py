"""
Главный модуль Telegram бота для учета статистики
"""
import os
import re
import uuid
import tempfile
from datetime import datetime
from typing import Dict, Optional, Tuple, List
from openpyxl import Workbook
from difflib import SequenceMatcher

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters
)

import config
from database import Database
from parser import DataParser
from reports import ReportGenerator
from excel_parser import ExcelProcessor
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

# Авторизованные пользователи (в памяти - сбрасывается при перезапуске!)
AUTHORIZED_USERS = set()

# Пин-код для доступа
PIN_CODE = "1664"

# Пин-код для удаления всех данных
RESET_PIN_CODE = "6002147"


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
        self.delete_mass_club: Optional[str] = None
        self.delete_mass_date_from: Optional[str] = None
        self.delete_mass_date_to: Optional[str] = None
        self.delete_mass_preview: Optional[dict] = None
        
        # Для команды экспорт
        self.export_club: Optional[str] = None
        
        # Для команды список
        self.list_club: Optional[str] = None
        
        # Для сводного отчета
        self.merge_candidates: Optional[list] = None
        self.merge_period: Optional[tuple] = None
        
        # Для проверки дубликатов в отчёте
        self.duplicate_check_data: Optional[dict] = None
        
        # Для объединения СБ с похожими именами
        self.sb_merge_data: Optional[dict] = None
        
        # Для предпросмотра данных
        self.preview_date: Optional[str] = None
        self.preview_duplicates: Optional[list] = None
        self.edit_line_number: Optional[int] = None
        
        # Для загрузки файла Excel
        self.upload_file_club: Optional[str] = None
        self.upload_file_date: Optional[str] = None
        self.upload_file_data: Optional[dict] = None
        
        # ID сообщений бота для удаления
        self.bot_messages: list = []
    
    def reset_input(self):
        """Сброс блочного ввода"""
        self.mode = None
        self.temp_nal_data = []
        self.temp_beznal_data = []
        self.preview_date = None
        self.preview_duplicates = None
        self.edit_line_number = None
        self.delete_mass_club = None
        self.delete_mass_date_from = None
        self.delete_mass_date_to = None
        self.delete_mass_preview = None
    
    def has_data(self) -> bool:
        """Проверка наличия данных"""
        return len(self.temp_nal_data) > 0 or len(self.temp_beznal_data) > 0


def get_user_state(user_id: int) -> UserState:
    """Получить состояние пользователя"""
    if user_id not in USER_STATES:
        USER_STATES[user_id] = UserState()
    return USER_STATES[user_id]


async def send_and_save(update: Update, state: UserState, text: str, **kwargs):
    """Отправить сообщение и сохранить его ID для возможного удаления"""
    msg = await update.message.reply_text(text, **kwargs)
    state.bot_messages.append(msg.message_id)
    # Ограничиваем список (храним только последние 100 сообщений)
    if len(state.bot_messages) > 100:
        state.bot_messages = state.bot_messages[-100:]
    return msg


def get_main_keyboard():
    """Главная клавиатура с основными командами"""
    keyboard = [
        ['📥 НАЛ', '📥 БЕЗНАЛ'],
        ['📎 ЗАГРУЗИТЬ ФАЙЛ'],
        ['✅ ГОТОВО', '❌ ОТМЕНА'],
        ['📊 ОТЧЁТ', '💰 ВЫПЛАТЫ'],
        ['📋 СПИСОК', '📤 ЭКСПОРТ'],
        ['✏️ ИСПРАВИТЬ', '🗑️ УДАЛИТЬ'],
        ['📜 ЖУРНАЛ', '👔 САМОЗАНЯТЫЕ'],
        ['❓ ПОМОЩЬ', '🚪 ЗАВЕРШИТЬ']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_club_keyboard():
    """Клавиатура для выбора клуба (Inline кнопки)"""
    keyboard = [
        [InlineKeyboardButton("🏢 Москвич", callback_data='club_moskvich')],
        [InlineKeyboardButton("🏢 Анора", callback_data='club_anora')]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_club_choice_keyboard():
    """Постоянная клавиатура для выбора клуба (Reply кнопки)"""
    keyboard = [
        ['🏢 СТАРТ МОСКВИЧ'],
        ['🏢 СТАРТ АНОРА']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_club_report_keyboard():
    """Клавиатура для выбора клуба в отчёте"""
    keyboard = [
        [InlineKeyboardButton("🏢 Москвич", callback_data='report_club_moskvich')],
        [InlineKeyboardButton("🏢 Анора", callback_data='report_club_anora')],
        [InlineKeyboardButton("🏢🏢 ОБА", callback_data='report_club_both')]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_delete_keyboard():
    """Клавиатура для выбора что удалить"""
    keyboard = [
        [InlineKeyboardButton("📗 НАЛ", callback_data='delete_nal')],
        [InlineKeyboardButton("📘 БЕЗНАЛ", callback_data='delete_beznal')],
        [InlineKeyboardButton("🗑️ ОБЕ", callback_data='delete_both')]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_delete_mode_keyboard():
    """Клавиатура выбора режима удаления"""
    keyboard = [
        [InlineKeyboardButton("🧍 Удалить сотрудника", callback_data='delete_mode_employee')],
        [InlineKeyboardButton("🗑️ Удалить все", callback_data='delete_mode_mass')]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_delete_mass_confirm_keyboard():
    """Клавиатура подтверждения массового удаления"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Да, удалить", callback_data='delete_mass_confirm_yes'),
            InlineKeyboardButton("❌ Нет", callback_data='delete_mass_confirm_no')
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_self_employed_action_keyboard():
    """Клавиатура для управления самозанятыми"""
    keyboard = [
        [
            InlineKeyboardButton("➕ Добавить код", callback_data='self_employed_add'),
            InlineKeyboardButton("➖ Удалить код", callback_data='self_employed_remove')
        ],
        [InlineKeyboardButton("❌ Закрыть", callback_data='self_employed_close')]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_merge_confirmation_keyboard():
    """Клавиатура для подтверждения объединения совпадений"""
    keyboard = [
        [
            InlineKeyboardButton("✅ Объединить все", callback_data='merge_all'),
            InlineKeyboardButton("❌ Не объединять", callback_data='merge_none')
        ],
        [InlineKeyboardButton("📄 Показать список", callback_data='merge_show_list')]
    ]
    return InlineKeyboardMarkup(keyboard)


def make_processed_key(code: str, name: Optional[str]) -> Tuple[str, str]:
    """Нормализованный ключ для отслеживания уже обработанных записей"""
    return code, (name or "").strip()


# Инициализация базы данных
db = Database()


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка команды /start и старт"""
    user_id = update.effective_user.id
    
    # Проверка авторизации
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text(
            "🔒 Введите пин-код для доступа:",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    state = get_user_state(user_id)
    
    # Получаем текст команды
    if update.message:
        text = update.message.text.lower()
    else:
        text = ""
    
    # Если команда /start без параметров - показываем выбор клуба
    if text.strip() == '/start':
        await update.message.reply_text(
            "Выберите клуб:",
            reply_markup=get_club_choice_keyboard()
        )
        return
    
    # Определяем клуб
    club = None
    if 'москвич' in text:
        club = 'Москвич'
    elif 'анора' in text or 'anora' in text:
        club = 'Анора'
    
    if not club:
        await update.message.reply_text(
            "Выберите клуб, нажав на кнопку ниже:",
            reply_markup=get_club_choice_keyboard()
        )
        return
    
    # Устанавливаем клуб и дату
    state.club = club
    state.current_date = get_current_date()
    state.reset_input()
    
    await update.message.reply_text(
        f"✅ Выбран клуб: {club}\n"
        f"📅 Текущая дата: {state.current_date}\n\n"
        f"🎯 ЧТО ДАЛЬШЕ?\n\n"
        f"📥 Для ввода данных:\n"
        f"   • Нажмите НАЛ или БЕЗНАЛ\n"
        f"   • Вставьте список данных\n"
        f"   • Нажмите ГОТОВО\n\n"
        f"📊 Для просмотра отчётов:\n"
        f"   • Нажмите ОТЧЁТ, ВЫПЛАТЫ или СПИСОК\n\n"
        f"❓ Полная справка: нажмите ПОМОЩЬ\n\n"
        f"Используйте кнопки меню ⬇️",
        reply_markup=get_main_keyboard()
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
                "Выберите клуб, нажав на кнопку ниже:",
                reply_markup=get_club_choice_keyboard()
            )
        else:
            await update.message.reply_text("🔒 Введите пин-код для доступа:")
        return
    
    # УНИВЕРСАЛЬНАЯ КНОПКА ОТМЕНА - работает на ЛЮБОМ этапе!
    # Проверяем ПЕРЕД всеми режимами
    if text_lower == 'отмена' or text_lower == '❌ отмена':
        # Список режимов где ОТМЕНА должна работать
        cancelable_modes = [
            'awaiting_preview_date', 'awaiting_preview_action', 'awaiting_edit_line_number', 'awaiting_edit_line_data',
            'awaiting_edit_params', 'awaiting_edit_data', 'awaiting_delete_choice',
            'awaiting_report_club', 'awaiting_report_period', 'awaiting_duplicate_confirm', 'awaiting_sb_merge_confirm',
            'awaiting_export_club', 'awaiting_export_period',
            'awaiting_merge_confirm', 'awaiting_list_club', 'awaiting_list_date', 'awaiting_payments_input',
            'awaiting_delete_mass_club', 'awaiting_delete_mass_period', 'awaiting_delete_mass_confirm',
            'awaiting_delete_employee_input',
            'awaiting_upload_club', 'awaiting_upload_date', 'awaiting_upload_file', 'awaiting_upload_confirm',
            'нал', 'безнал'
        ]
        
        if state.mode in cancelable_modes or state.has_data():
            # Полная очистка (но клуб остаётся!)
            saved_club = state.club  # Сохраняем клуб
            state.reset_input()
            state.mode = None
            state.duplicate_check_data = None
            state.sb_merge_data = None
            state.report_club = None
            state.export_club = None
            state.list_club = None
            state.edit_code = None
            state.edit_date = None
            state.edit_current_data = None
            state.delete_code = None
            state.delete_date = None
            state.delete_records = None
            state.merge_candidates = None
            state.merge_period = None
            state.upload_file_club = None
            state.upload_file_date = None
            state.upload_file_data = None
            state.club = saved_club  # Восстанавливаем клуб
            
            await update.message.reply_text(
                f"❌ Операция отменена\n\n"
                f"🏢 Клуб: {state.club}\n"
                f"Используйте кнопки меню:",
                reply_markup=get_main_keyboard()
            )
            return
    
    if state.mode == 'awaiting_delete_mass_club':
        await handle_delete_mass_club_input(update, state, text, text_lower)
        return
    
    if state.mode == 'awaiting_delete_mass_period':
        await handle_delete_mass_period_input(update, state, text, text_lower)
        return
    
    if state.mode == 'awaiting_delete_mass_confirm':
        await handle_delete_mass_confirm_text(update, state, text_lower)
        return

    if state.mode == 'awaiting_delete_employee_input':
        await handle_delete_employee_input(update, context, state, text)
        return
    
    # Обработка ввода даты для загрузки файла
    if state.mode == 'awaiting_upload_date':
        success, parsed_date, error = parse_short_date(text)
        if success:
            state.upload_file_date = parsed_date
            await update.message.reply_text(
                f"📎 ЗАГРУЗКА ФАЙЛА\n"
                f"🏢 Клуб: {state.upload_file_club}\n"
                f"📅 Дата: {parsed_date}\n\n"
                f"📄 Теперь отправьте Excel файл"
            )
            state.mode = 'awaiting_upload_file'
        else:
            await update.message.reply_text(
                f"❌ {error}\n\n"
                f"Введите дату (формат: 30,10) или напишите: отмена"
            )
        return
    
    # НОВАЯ ЛОГИКА: обработка предпросмотра и ввода даты
    if state.mode == 'awaiting_preview_date':
        # Пытаемся распарсить дату
        success, parsed_date, error = parse_short_date(text)
        if success:
            # Сохраняем дату и показываем финальный предпросмотр
            state.preview_date = parsed_date
            await show_data_preview(update, state, show_duplicates=True)
            
            # Переходим в режим ожидания действия (ЗАПИСАТЬ/ИЗМЕНИТЬ/ОТМЕНА)
            state.mode = 'awaiting_preview_action'
            return
        else:
            await update.message.reply_text(
                f"❌ {error}\n\n"
                f"Введите дату (формат: 30,10) или напишите: отмена"
            )
            return
    
    # Обработка действий в режиме предпросмотра
    if state.mode == 'awaiting_preview_action':
        await handle_preview_action(update, state, text, text_lower)
        return
    
    # Обработка ввода номера строки для редактирования
    if state.mode == 'awaiting_edit_line_number':
        await handle_edit_line_number(update, state, text)
        return
    
    # Обработка ввода новых данных для строки
    if state.mode == 'awaiting_edit_line_data':
        await handle_edit_line_data(update, state, text)
        return
    
    # Команда "обнулить"
    if text_lower == 'обнулить':
        await update.message.reply_text(
            "⚠️ ВНИМАНИЕ! Будут удалены ВСЕ данные из базы!\n\n"
            "Для подтверждения введите пин-код:"
        )
        state.mode = 'awaiting_reset_pin'
        return
    
    # Обработка пина для обнуления
    if state.mode == 'awaiting_reset_pin':
        if text == RESET_PIN_CODE:
            # Удаляем все данные
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM operations")
            cursor.execute("DELETE FROM edit_log")
            conn.commit()
            conn.close()
            
            state.mode = None
            await update.message.reply_text(
                "✅ Все данные удалены из базы.\n"
                "База данных обнулена."
            )
        else:
            state.mode = None
            await update.message.reply_text(
                "❌ Неверный пин-код. Операция отменена."
            )
        return
    
    # Команда "завершить" - выход из сессии
    if text_lower == 'завершить' or text_lower == '🚪 завершить':
        # Удаляем сообщения бота (последние сохранённые)
        chat_id = update.effective_chat.id
        deleted_count = 0
        
        for msg_id in state.bot_messages[-50:]:  # Последние 50 сообщений
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
                deleted_count += 1
            except:
                pass  # Игнорируем ошибки (сообщение уже удалено или старое)
        
        # Очищаем состояние
        AUTHORIZED_USERS.discard(user_id)
        state.reset_input()
        state.club = None
        state.bot_messages = []
        
        await update.message.reply_text(
            "👋 Сессия завершена.\n"
            f"Удалено сообщений: {deleted_count}\n\n"
            "Для повторного входа введите пин-код.",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    # Сопоставление кнопок с командами
    button_commands = {
        '🏢 старт москвич': 'старт москвич',
        '🏢 старт анора': 'старт анора',
        '📥 нал': 'нал',
        '📥 безнал': 'безнал',
        '📎 загрузить файл': 'загрузить файл',
        '✅ готово': 'готово',
        '❌ отмена': 'отмена',
        '📊 отчёт': 'отчет',
        '📊 отчет': 'отчет',
        '💰 выплаты': 'выплаты',
        '📋 список': 'список',
        '📤 экспорт': 'экспорт',
        '✏️ исправить': 'исправить',
        '🗑️ удалить': 'удалить',
        '📜 журнал': 'журнал',
        '❓ помощь': 'помощь',
        '🚪 завершить': 'завершить'
    }
    
    # Если нажата кнопка - преобразуем в команду
    if text_lower in button_commands:
        text_lower = button_commands[text_lower]
    
    # Команда "кнопки" - показать клавиатуру
    if text_lower == 'кнопки':
        if state.club:
            await update.message.reply_text(
                "Клавиатура:",
                reply_markup=get_main_keyboard()
            )
        else:
            await update.message.reply_text(
                "Сначала выберите клуб:",
                reply_markup=get_club_keyboard()
            )
        return
    
    # Команда "помощь"
    if text_lower in ['помощь', 'help']:
        await update.message.reply_text(
            "📋 ПОЛНАЯ СПРАВКА ПО КОМАНДАМ\n\n"
            "🏢 НАЧАЛО РАБОТЫ:\n"
            "• Выберите клуб: СТАРТ МОСКВИЧ / СТАРТ АНОРА\n"
            "• После выбора используйте кнопки меню\n\n"
            "💰 ВВОД ДАННЫХ:\n"
            "1️⃣ Нажмите НАЛ или БЕЗНАЛ\n"
            "2️⃣ Вставьте список данных\n"
            "3️⃣ Нажмите ГОТОВО → предпросмотр\n"
            "4️⃣ Укажите дату (например: 3,10)\n"
            "5️⃣ Проверьте данные в предпросмотре\n"
            "6️⃣ ЗАПИСАТЬ - сохранить в базу\n\n"
            "🔍 ПРЕДПРОСМОТР:\n"
            "После ГОТОВО вы увидите все данные с номерами строк\n"
            "• ЗАПИСАТЬ → сохранить данные\n"
            "• ИЗМЕНИТЬ → редактировать строку (укажите номер)\n"
            "• ОТМЕНА → отменить ввод\n"
            "• Если есть дубликаты → команды для объединения\n\n"
            "🔄 ОБЪЕДИНЕНИЕ ДУБЛИКАТОВ:\n"
            "Если найдены записи с одним кодом (с именем и без):\n"
            "• ОК → объединить все\n"
            "• ОК 1 → объединить только пункт 1\n"
            "• ОК 1 2 → объединить пункты 1 и 2\n"
            "• НЕ 1 → НЕ объединять пункт 1 (остальные да)\n"
            "• НЕ 1 2 → НЕ объединять пункты 1 и 2\n\n"
            "📊 ОТЧЁТЫ:\n"
            "• ОТЧЁТ → выбрать клуб → указать период\n"
            "• ВЫПЛАТЫ → код + период (Д7 3,10-5,11)\n"
            "• Получите Excel файл с отчётом\n\n"
            "📝 ПРОСМОТР И РЕДАКТИРОВАНИЕ:\n"
            "• СПИСОК → клуб → дата (посмотреть все записи)\n"
            "• ИСПРАВИТЬ → код + дата (Д7 3,10)\n"
            "• УДАЛИТЬ → код + дата (Д7 3,10)\n"
            "• УДАЛИТЬ ВСЕ → клуб → дата/период (массовое удаление)\n\n"
            "📤 ЭКСПОРТ:\n"
            "• ЭКСПОРТ → клуб → период → Excel файл\n\n"
            "📜 ЖУРНАЛ ИЗМЕНЕНИЙ:\n"
            "• ЖУРНАЛ → последние 20 изменений\n"
            "• ЖУРНАЛ 50 → последние 50 изменений\n"
            "• ЖУРНАЛ Д7 → все изменения по коду Д7\n"
            "• ЖУРНАЛ 3,10 → все изменения за дату\n"
            "Показывает: объединения, исправления, удаления\n\n"
            "🔧 ДОПОЛНИТЕЛЬНО:\n"
            "• ОБНУЛИТЬ → удалить все данные (нужен пин)\n"
            "• ЗАВЕРШИТЬ → выход (очистка истории)\n\n"
            "📖 ФОРМАТЫ ДАТ:\n"
            "• 3,10 = 03.10.2025\n"
            "• 30,10 = 30.10.2025\n"
            "• 3,10-5,11 = период с 3.10 по 5.11\n\n"
            "📝 ФОРМАТЫ ДАННЫХ:\n"
            "• Д7 Надя 6800 или Д7 Надя-6800\n"
            "• Юля Д17 1000\n"
            "• СБ Дмитрий 4000\n"
            "• Уборщица-2000\n"
            "• Суммы: 40,000 или 40.000 → 40000 ✅\n\n"
            "✨ АВТОМАТИЧЕСКАЯ ОЧИСТКА:\n"
            "• Дубли из Excel очищаются автоматически\n"
            "• Разделители тысяч (точки/запятые) удаляются\n"
            "• В предпросмотре видно что было изменено"
        )
        return
    
    # Обработка подтверждения объединения дубликатов
    if state.mode == 'awaiting_duplicate_confirm':
        await handle_duplicate_confirmation(update, context, state, text, text_lower)
        return
    
    if state.mode == 'awaiting_sb_merge_confirm':
        await handle_sb_merge_confirmation(update, context, state, text, text_lower)
        return
    
    # Обработка подтверждения загрузки файла
    if state.mode == 'awaiting_upload_confirm':
        if text_lower == 'отмена' or text_lower == '❌ отмена':
            state.upload_file_club = None
            state.upload_file_date = None
            state.upload_file_data = None
            state.mode = None
            await update.message.reply_text(
                "❌ Загрузка файла отменена\n\n"
                "Используйте кнопки меню:",
                reply_markup=get_main_keyboard()
            )
            return
        elif text_lower.startswith('записать'):
            # Парсим команду
            # Варианты: "записать", "записать 1 2", "записать без 3"
            selected_merges = None  # None = все, [] = без объединений, [1,2] = только указанные
            
            if text_lower == 'записать':
                # Применить все объединения
                selected_merges = None
            elif 'без' in text_lower:
                # ЗАПИСАТЬ БЕЗ 1 2 3 - исключить указанные
                parts = text_lower.replace('записать', '').replace('без', '').strip().split()
                excluded = []
                for part in parts:
                    try:
                        excluded.append(int(part))
                    except ValueError:
                        pass
                
                if excluded:
                    # Получаем все ID объединений
                    data = state.upload_file_data
                    beznal_analysis = data.get('beznal_analysis', {})
                    nal_analysis = data.get('nal_analysis', {})
                    
                    all_merge_ids = []
                    for merge in beznal_analysis.get('merges', []):
                        if 'merge_id' in merge:
                            all_merge_ids.append(merge['merge_id'])
                    for merge in nal_analysis.get('merges', []):
                        if 'merge_id' in merge:
                            all_merge_ids.append(merge['merge_id'])
                    
                    # Все кроме исключенных
                    selected_merges = [mid for mid in all_merge_ids if mid not in excluded]
                else:
                    selected_merges = None  # Нет исключений - все
            else:
                # ЗАПИСАТЬ 1 2 3 - только указанные
                parts = text_lower.replace('записать', '').strip().split()
                selected = []
                for part in parts:
                    try:
                        selected.append(int(part))
                    except ValueError:
                        pass
                
                if selected:
                    selected_merges = selected
                else:
                    selected_merges = None  # Не смогли распарсить - применяем все
            
            # Сохраняем выбор и сохраняем данные
            state.upload_file_data['selected_merges'] = selected_merges
            await save_file_data(update, state)
            return
        else:
            await update.message.reply_text(
                "⚠️ Неверная команда. Введите:\n"
                "  • ЗАПИСАТЬ - применить все\n"
                "  • ЗАПИСАТЬ 1 2 - только [1] и [2]\n"
                "  • ЗАПИСАТЬ БЕЗ 3 - все кроме [3]\n"
                "  • ОТМЕНА - отменить"
            )
            return
    
    # Команда "старт москвич" или "старт анора" (обработка как текст)
    if text_lower.startswith('старт'):
        # Если в режиме ввода данных - предупреждение
        if state.has_data() and state.mode != 'awaiting_date':
            await update.message.reply_text(
                "⚠️ У вас есть несохранённые данные!\n"
                "Завершите ввод командой: готово\n"
                "Или отмените: отмена"
            )
            return
        await start_command(update, context)
        return
    
    # Команда "нал"
    if text_lower == 'нал':
        if not state.club:
            await update.message.reply_text(
                "❌ Клуб не выбран.\n"
                "Используйте: старт москвич или старт анора"
            )
        else:
            state.mode = 'нал'
            await update.message.reply_text(
                f"📥 РЕЖИМ ВВОДА: НАЛ\n\n"
                f"🏢 Клуб: {state.club}\n\n"
                f"📝 Вставьте список данных:\n"
                f"Примеры форматов:\n"
                f"  • Д7 Юля 1000\n"
                f"  • Д7 Юля-1000\n"
                f"  • Юля Д7 1000\n\n"
                f"⏭️ После ввода всех данных (НАЛ и БЕЗНАЛ)\n"
                f"   нажмите: ГОТОВО"
            )
        return
    
    # Команда "безнал"
    if text_lower == 'безнал':
        if not state.club:
            await update.message.reply_text(
                "❌ Клуб не выбран.\n"
                "Используйте: старт москвич или старт анора"
            )
        else:
            state.mode = 'безнал'
            await update.message.reply_text(
                f"📥 РЕЖИМ ВВОДА: БЕЗНАЛ\n\n"
                f"🏢 Клуб: {state.club}\n\n"
                f"📝 Вставьте список данных:\n"
                f"Примеры форматов:\n"
                f"  • Д7 Юля 1000\n"
                f"  • Д7 Юля-1000\n"
                f"  • Юля Д7 1000\n\n"
                f"⏭️ После ввода всех данных (НАЛ и БЕЗНАЛ)\n"
                f"   нажмите: ГОТОВО"
            )
        return
    
    # Команда "загрузить файл"
    if text_lower == 'загрузить файл':
        if state.has_data():
            await update.message.reply_text(
                "⚠️ У вас есть несохранённые данные!\n"
                "Завершите ввод командой: готово\n"
                "Или отмените: отмена"
            )
            return
        
        await update.message.reply_text(
            "📎 ЗАГРУЗКА EXCEL ФАЙЛА\n\n"
            "Выберите клуб:",
            reply_markup=get_club_keyboard()
        )
        state.mode = 'awaiting_upload_club'
        return
    
    # Команда "готово"
    if text_lower == 'готово':
        if not state.has_data():
            await update.message.reply_text(
                "❌ Нет данных для обработки.\n"
                "Используйте команды: нал и безнал для ввода данных"
            )
            return
        
        # Показываем предпросмотр данных
        await show_data_preview(update, state, show_duplicates=True)
        
        # Переходим в режим ожидания даты (сначала нужно указать дату)
        state.mode = 'awaiting_preview_date'
        return
    
    # Блочный ввод данных (но проверяем сначала - это не команда/кнопка!)
    if state.mode in ['нал', 'безнал']:
        # Проверяем - это команда или кнопка?
        # Если текст начинается с emoji кнопок или это известная команда - НЕ парсим как данные
        emoji_buttons = ['📥', '✅', '❌', '📊', '💰', '📋', '📤', '✏️', '🗑️', '❓', '🚪']
        is_button = any(text.startswith(emoji) for emoji in emoji_buttons)
        
        if is_button or text_lower in ['отмена', 'готово', 'отчет', 'список', 'экспорт', 'помощь']:
            # Это команда/кнопка - НЕ парсим как данные, пропускаем дальше
            pass
        else:
            # Это данные - парсим
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
            "Выберите клуб:",
            reply_markup=get_club_report_keyboard()
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
                await generate_and_send_report(update, club, date_from, date_to, state)
                # Если generate_and_send_report установил режим awaiting_duplicate_confirm или awaiting_sb_merge_confirm - выходим
                if state.mode in ['awaiting_duplicate_confirm', 'awaiting_sb_merge_confirm']:
                    return
            
            # Затем проверяем возможность сводного отчета
            await prepare_merged_report(update, state, date_from, date_to)
            
            # НЕ сбрасываем режим если ждём подтверждения объединения!
            if state.mode != 'awaiting_merge_confirm':
                state.mode = None
                state.report_club = None
        else:
            club = 'Москвич' if state.report_club == 'москвич' else 'Анора'
            await generate_and_send_report(update, club, date_from, date_to, state)
            
            # НЕ сбрасываем режим если ждём подтверждения дубликатов!
            if state.mode not in ['awaiting_duplicate_confirm', 'awaiting_sb_merge_confirm']:
                state.mode = None
                state.report_club = None
        return
    
    # Обработка подтверждения объединения для сводного отчета
    if state.mode == 'awaiting_merge_confirm':
        await handle_merge_confirmation(update, state, text_lower)
        return
    
    # Команда "выплаты"
    if text_lower.startswith('выплаты') or text_lower == 'выплаты':
        if text_lower == 'выплаты':
            # Нажата кнопка - переходим в режим ожидания
            await update.message.reply_text(
                "Выплаты за период\n\n"
                "Введите сотрудника и период:\n\n"
                "Примеры:\n"
                "• Д7 12,12\n"
                "• Д7 10,06-11,08"
            )
            state.mode = 'awaiting_payments_input'
        else:
            await handle_payments_command(update, context, state, text)
        return
    
    # Обработка ввода для выплат (после кнопки)
    if state.mode == 'awaiting_payments_input':
        await handle_payments_command(update, context, state, text)
        state.mode = None
        return
    
    # Команда "список"
    if text_lower.startswith('список') or text_lower == 'список':
        if text_lower == 'список':
            await update.message.reply_text(
                "📋 Выберите клуб для просмотра записей:",
                reply_markup=get_club_report_keyboard()
            )
            state.mode = 'awaiting_list_club'
        else:
            await handle_list_command(update, context, state, text)
        return
    
    # Обработка выбора клуба для списка
    if state.mode == 'awaiting_list_club':
        club_choice = text_lower
        if club_choice in ['москвич', 'анора', 'оба']:
            state.list_club = club_choice
            await update.message.reply_text(
                "📅 Введите дату:\n\n"
                "Примеры:\n"
                "• 3,11\n"
                "• 30,10"
            )
            state.mode = 'awaiting_list_date'
        else:
            await update.message.reply_text("❌ Выберите: москвич, анора или оба")
        return
    
    # Обработка ввода даты для списка
    if state.mode == 'awaiting_list_date':
        success, parsed_date, error = parse_short_date(text)
        if success:
            if state.list_club == 'оба':
                # Показываем списки для обоих клубов
                for club in ['Москвич', 'Анора']:
                    operations = db.get_operations_by_date(club, parsed_date)
                    response = format_operations_list(operations, parsed_date, club)
                    await update.message.reply_text(response)
            else:
                # Показываем список для одного клуба
                club = 'Москвич' if state.list_club == 'москвич' else 'Анора'
                operations = db.get_operations_by_date(club, parsed_date)
                response = format_operations_list(operations, parsed_date, club)
                await update.message.reply_text(response)
            
            state.mode = None
            state.list_club = None
        else:
            await update.message.reply_text(f"❌ {error}")
        return
    
    # Команда "исправить"
    if text_lower.startswith('исправить') or text_lower == 'исправить':
        if text_lower == 'исправить':
            await update.message.reply_text(
                "📝 Введите код и дату:\n\n"
                "Примеры:\n"
                "• Д7 3,11\n"
                "• Д1 30,10"
            )
            state.mode = 'awaiting_edit_params'
        else:
            await handle_edit_command_new(update, context, state, text)
        return
    
    # Обработка ввода параметров для исправления
    if state.mode == 'awaiting_edit_params':
        await handle_edit_command_new(update, context, state, text)
        return
    
    # Обработка ввода новых данных для исправления
    if state.mode == 'awaiting_edit_data':
        await handle_edit_input(update, context, state, text, text_lower)
        return
    
    # Команда "удалить"
    if text_lower.startswith('удалить') or text_lower == 'удалить':
        if text_lower == 'удалить':
            await update.message.reply_text(
                "Формат: удалить КОД дата\n\n"
                "Примеры:\n"
                "• удалить Д7 12,12\n"
                "• удалить Д1 30,10\n\n"
                "Массовое удаление:\n"
                "• удалить все"
            , reply_markup=get_delete_mode_keyboard())
        else:
            await handle_delete_command_new(update, context, state, text)
        return
    
    # Обработка выбора что удалить
    if state.mode == 'awaiting_delete_choice':
        await handle_delete_choice(update, context, state, text_lower)
        return
    
    # Команда "журнал"
    if text_lower.startswith('журнал') or text_lower == 'журнал' or text_lower == '📜 журнал':
        await handle_journal_command(update, context, state, text)
        return
    
    # Команда "самозанятые"
    if text_lower in ['самозанятые', '👔 самозанятые']:
        await handle_self_employed_command(update, context, state)
        return
    
    # Обработка режима добавления самозанятого
    if state.mode == 'awaiting_self_employed_add':
        await handle_self_employed_add(update, state, text)
        return
    
    # Обработка режима удаления самозанятого
    if state.mode == 'awaiting_self_employed_remove':
        await handle_self_employed_remove(update, state, text)
        return
    
    # Команда "экспорт"
    if text_lower == 'экспорт':
        await update.message.reply_text(
            "Выберите клуб для экспорта:",
            reply_markup=get_club_report_keyboard()  # Используем ту же клавиатуру
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
    
    # Если дошли сюда - либо данные в режиме ввода, либо неизвестная команда
    # В режиме ввода данные уже обработаны выше, поэтому просто игнорируем
    if state.mode in ['нал', 'безнал']:
        return
    
    # Неизвестная команда (только если не в режиме ввода)
    await update.message.reply_text(
        "❓ Команда не распознана\n\n"
        "📋 ОСНОВНЫЕ КОМАНДЫ:\n\n"
        "💰 Ввод данных:\n"
        "  • НАЛ / БЕЗНАЛ → ввод данных\n"
        "  • ГОТОВО → завершить и сохранить\n\n"
        "📊 Просмотр:\n"
        "  • ОТЧЁТ → отчёт по периоду\n"
        "  • ВЫПЛАТЫ → выплаты сотруднику\n"
        "  • СПИСОК → записи за дату\n\n"
        "📤 Другое:\n"
        "  • ЭКСПОРТ → экспорт в Excel\n"
        "  • ИСПРАВИТЬ → редактировать запись\n"
        "  • УДАЛИТЬ → удалить запись\n\n"
        "💡 Используйте кнопки меню ⬇️\n"
        "📖 Полная справка: ПОМОЩЬ"
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
    
    # Формат: Д1 30,10 (без слова "исправить")
    # Убираем "исправить" если есть (для обратной совместимости)
    parts = text.split()
    if parts[0].lower() in ['исправить', 'ispravit']:
        parts = parts[1:]  # Убираем первое слово
    
    if len(parts) < 2:
        await update.message.reply_text(
            "❌ Неверный формат.\n"
            "Пример: Д1 30,10"
        )
        return
    
    code = DataParser.normalize_code(parts[0])
    date_str = parts[1]
    
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
    response.append("Примеры:")
    response.append("• нал 1100")
    response.append("• безнал 2500")
    response.append("• нал 1100 безнал 2500")
    
    await update.message.reply_text('\n'.join(response))
    
    # Сохраняем состояние
    state.edit_code = code
    state.edit_date = parsed_date
    state.edit_current_data = current_data
    state.mode = 'awaiting_edit_data'


async def handle_edit_input(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                            state: UserState, text: str, text_lower: str):
    """Обработка ввода новых значений для исправления"""
    # Парсим ввод: нал 1100 или безнал 2500 или нал 1100 безнал 2500
    parts = text_lower.split()
    
    # Ищем пары: канал + сумма
    updates = []
    i = 0
    while i < len(parts):
        if parts[i] in ['нал', 'безнал']:
            if i + 1 < len(parts):
                channel = parts[i]
                success, amount, error = DataParser.parse_amount(parts[i + 1])
                
                if success:
                    updates.append((channel, amount))
                    i += 2
                else:
                    await update.message.reply_text(f"❌ {error}")
                    return
            else:
                await update.message.reply_text(f"❌ Не указана сумма для {parts[i]}")
                return
        else:
            await update.message.reply_text(
                f"❌ Неверный формат.\n\n"
                f"Примеры:\n"
                f"• нал 1100\n"
                f"• безнал 2500\n"
                f"• нал 1100 безнал 2500"
            )
            return
    
    if not updates:
        await update.message.reply_text(
            "❌ Не указаны данные для обновления.\n\n"
            "Примеры:\n"
            "• нал 1100\n"
            "• безнал 2500\n"
            "• нал 1100 безнал 2500"
        )
        return
    
    # Сохраняем изменения СРАЗУ
    updated_channels = []
    for channel, amount in updates:
        success, msg = db.update_operation(state.club, state.edit_date, state.edit_code, channel, amount)
        if success:
            updated_channels.append(f"{channel.upper()}: {amount:.0f}")
        else:
            await update.message.reply_text(f"❌ Ошибка обновления {channel}: {msg}")
            return
    
    # Показываем результат
    await update.message.reply_text(
        f"✅ Данные {state.edit_code} за {state.edit_date} обновлены:\n" +
        "\n".join(f"• {ch}" for ch in updated_channels)
    )
    
    # Очищаем состояние
    state.mode = None
    state.edit_code = None
    state.edit_date = None
    state.edit_current_data = None


async def handle_delete_command_new(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                    state: UserState, text: str):
    """Новая интерактивная команда удалить"""
    if not state.club:
        await update.message.reply_text(
            "❌ Клуб не выбран.\n"
            "Используйте: старт москвич или старт анора"
        )
        return
    
    text_lower = normalize_command(text)
    if text_lower in ['удалить все', 'удалить всё']:
        await update.message.reply_text(
            "🏢 Выберите клуб для удаления данных:",
            reply_markup=get_club_report_keyboard()
        )
        state.mode = 'awaiting_delete_mass_club'
        return
    
    # Формат: удалить Д1 30,10
    parts = text.split()
    if len(parts) < 3:
        await update.message.reply_text(
            "❌ Неверный формат.\n"
            "Пример: удалить Д1 30,10\n"
            "Или напишите: удалить все"
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
    
    await update.message.reply_text('\n'.join(response), reply_markup=get_delete_keyboard())
    
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


async def handle_delete_employee_input(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       state: UserState, text: str):
    """Обработка ввода для удаления конкретного сотрудника"""
    cleaned = text.strip()
    if not cleaned:
        await update.message.reply_text(
            "❌ Введите код и дату.\n"
            "Пример: Д1 30,10"
        )
        return
    
    # Используем существующий обработчик, добавляя ключевое слово
    await handle_delete_command_new(update, context, state, f"удалить {cleaned}")


def _summarize_operations_for_delete(operations: list) -> Dict:
    """Возвращает агрегаты по списку операций"""
    total_nal = sum(op['amount'] for op in operations if op['channel'] == 'нал')
    total_beznal = sum(op['amount'] for op in operations if op['channel'] == 'безнал')
    return {
        'count': len(operations),
        'total_nal': total_nal,
        'total_beznal': total_beznal
    }


def _format_delete_preview_lines(club_label: str, date_from: str, date_to: str,
                                 operations: list) -> Tuple[str, Dict]:
    """Формирует текст предпросмотра удаления и возвращает агрегаты"""
    summary = _summarize_operations_for_delete(operations)
    lines = []
    lines.append(f"🏢 {club_label}")
    lines.append(f"📅 Период: {date_from} .. {date_to}")
    lines.append(f"🧾 Записей: {summary['count']}")
    lines.append(f"💵 НАЛ: {summary['total_nal']:,.0f}".replace(',', ' '))
    lines.append(f"💳 БЕЗНАЛ: {summary['total_beznal']:,.0f}".replace(',', ' '))
    
    if operations:
        lines.append("Первые записи:")
        for op in operations[:5]:
            code = op['code']
            name = op['name'] or "(без имени)"
            channel = op['channel'].upper()
            amount = f"{op['amount']:,.0f}".replace(',', ' ')
            lines.append(f" • {op['date']} | {code} {name} | {channel} {amount}")
        if len(operations) > 5:
            lines.append(f" • ... и ещё {len(operations) - 5} записей")
    else:
        lines.append("Нет записей за выбранный период.")
    
    return '\n'.join(lines), summary


def create_delete_preview_excel(preview_data: List[Dict], filename: str):
    """Создаёт Excel-файл с данными для удаления"""
    wb = Workbook()
    ws = wb.active
    ws.title = "К удалению"
    ws.append(["Клуб", "Дата", "Код", "Имя", "Канал", "Сумма"])
    
    for item in preview_data:
        club = item['club']
        for op in item.get('operations', []):
            ws.append([
                club,
                op['date'],
                op['code'],
                op['name'] or "",
                op['channel'],
                op['amount']
            ])
    
    for column_cells in ws.columns:
        max_length = 0
        column = column_cells[0].column_letter
        for cell in column_cells:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[column].width = max_length + 2
    
    wb.save(filename)


async def handle_delete_mass_club_input(update: Update, state: UserState,
                                        text: str, text_lower: str):
    """Выбор клуба для массового удаления"""
    club_map = {
        'москвич': 'москвич',
        'анора': 'анора',
        'оба': 'оба'
    }
    
    normalized = text_lower
    if normalized in ['🏢 москвич', 'москвич']:
        selection = 'москвич'
    elif normalized in ['🏢 анора', 'анора', 'anora']:
        selection = 'анора'
    elif normalized in ['🏢🏢 оба', 'оба']:
        selection = 'оба'
    else:
        await update.message.reply_text(
            "❌ Неверный выбор. Напишите: москвич, анора или оба"
        )
        return
    
    state.delete_mass_club = selection
    state.delete_mass_date_from = None
    state.delete_mass_date_to = None
    state.delete_mass_preview = None
    await update.message.reply_text(
        "📅 Укажите дату или период для удаления:\n\n"
        "Примеры:\n"
        "• 5,11\n"
        "• 2,11-5,11"
    )
    state.mode = 'awaiting_delete_mass_period'


async def handle_delete_mass_period_input(update: Update, state: UserState,
                                          text: str, text_lower: str):
    """Обработка ввода даты/периода для массового удаления"""
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
    
    selection = state.delete_mass_club
    club_labels = []
    if selection == 'оба':
        club_labels = [('Москвич', 'Москвич'), ('Анора', 'Анора')]
    else:
        label = 'Москвич' if selection == 'москвич' else 'Анора'
        club_labels = [(label, label)]
    
    preview_sections = []
    preview_data = []
    total_records = 0
    
    for club_key, club_label in club_labels:
        operations = db.get_operations_by_period(club_label, date_from, date_to)
        if operations:
            section_text, summary = _format_delete_preview_lines(club_label, date_from, date_to, operations)
            preview_sections.append(section_text)
            preview_data.append({
                'club': club_label,
                'summary': summary,
                'operations': operations
            })
            total_records += summary['count']
        else:
            preview_sections.append(
                f"🏢 {club_label}\n"
                f"📅 Период: {date_from} .. {date_to}\n"
                "Нет записей за выбранный период."
            )
    
    if total_records == 0:
        await update.message.reply_text(
            "ℹ️ За указанный период данных нет.\n"
            "Удаление не требуется."
        )
        # Сброс
        state.mode = None
        state.delete_mass_club = None
        return
    
    # Сохраняем параметры
    state.delete_mass_date_from = date_from
    state.delete_mass_date_to = date_to
    state.delete_mass_preview = {
        'clubs': preview_data,
        'total_records': total_records
    }
    
    await update.message.reply_text(
        "📊 Предпросмотр удаления:\n\n" + '\n\n'.join(preview_sections)
    )
    
    # Отправляем Excel с деталями
    filename = f"delete_preview_{uuid.uuid4().hex}.xlsx"
    create_delete_preview_excel(preview_data, filename)
    with open(filename, 'rb') as f:
        await update.message.reply_document(
            document=f,
            filename=f"preview_delete_{date_from}_{date_to}.xlsx",
            caption="📄 Excel с данными для удаления"
        )
    os.remove(filename)
    
    await update.message.reply_text(
        "❗ Подтвердите удаление всех записей за этот период.",
        reply_markup=get_delete_mass_confirm_keyboard()
    )
    state.mode = 'awaiting_delete_mass_confirm'


async def handle_delete_mass_confirm_message(message, state: UserState, confirmed: bool):
    """Подтверждение массового удаления (message может быть update.message или query.message)"""
    if confirmed:
        selection = state.delete_mass_club
        date_from = state.delete_mass_date_from
        date_to = state.delete_mass_date_to
        preview = state.delete_mass_preview or {}
        
        results = []
        total_deleted = 0
        
        clubs_to_process = []
        if selection == 'оба':
            clubs_to_process = ['Москвич', 'Анора']
        else:
            clubs_to_process = ['Москвич' if selection == 'москвич' else 'Анора']
        
        for club in clubs_to_process:
            deleted = db.delete_operations_by_period(club, date_from, date_to)
            total_deleted += deleted
            summary = None
            if preview:
                for item in preview.get('clubs', []):
                    if item['club'] == club:
                        summary = item['summary']
                        break
            results.append({
                'club': club,
                'deleted': deleted,
                'summary': summary
            })
        
        lines = []
        if total_deleted == 0:
            lines.append("ℹ️ Записей для удаления не найдено.")
        else:
            lines.append("🗑️ Удаление завершено!")
            lines.append(f"📅 Период: {date_from} .. {date_to}")
            lines.append(f"🧾 Удалено записей: {total_deleted}")
            lines.append("")
            for item in results:
                summary = item['summary']
                lines.append(f"🏢 {item['club']}")
                lines.append(f"Удалено записей: {item['deleted']}")
                if summary:
                    lines.append(f"НАЛ: {summary['total_nal']:,.0f}".replace(',', ' '))
                    lines.append(f"БЕЗНАЛ: {summary['total_beznal']:,.0f}".replace(',', ' '))
                lines.append("")
            lines.append("📜 История доступна в ЖУРНАЛ.")
        
        await message.reply_text('\n'.join(line for line in lines if line))
        
        # Сброс
        state.mode = None
        state.delete_mass_club = None
        state.delete_mass_date_from = None
        state.delete_mass_date_to = None
        state.delete_mass_preview = None
        return
    
    await message.reply_text("✅ Удаление отменено.")
    state.mode = None
    state.delete_mass_club = None
    state.delete_mass_date_from = None
    state.delete_mass_date_to = None
    state.delete_mass_preview = None


async def handle_delete_mass_confirm_text(update: Update, state: UserState, text_lower: str):
    """Фолбэк на текстовое подтверждение"""
    if text_lower in ['да', 'ok', 'ок', 'yes', 'y']:
        await handle_delete_mass_confirm_message(update.message, state, True)
    elif text_lower in ['нет', 'no', 'n', 'отмена', 'cancel']:
        await handle_delete_mass_confirm_message(update.message, state, False)
    else:
        await update.message.reply_text(
            "❓ Не понял. Напишите: да / нет\n"
            "Для отмены напишите: отмена"
        )
        return

        state.mode = None
        state.delete_mass_club = None
        state.delete_mass_date_from = None
        state.delete_mass_date_to = None
        state.delete_mass_preview = None
        return
    
    await update.message.reply_text(
        "❓ Не понял. Напишите: да / нет\n"
        "Для отмены напишите: отмена"
    )


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
        report_rows, totals, club, f"{date_from} .. {date_to}", filename, db
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
                    'anora': {'nal': employees_a[code]['nal'], 'beznal': employees_a[code]['beznal']},
                    'names_m': list(names_m),
                    'names_a': list(names_a)
                })
    
    if not merge_candidates:
        # Совпадений нет - генерируем сводный без объединения (просто все записи)
        await update.message.reply_text(
            "ℹ️ Совпадений не найдено\n"
            "Генерируется сводный отчёт из всех записей..."
        )
        
        # Создаём сводный из всех операций
        all_ops = ops_moskvich + ops_anora
        
        if all_ops:
            report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(all_ops)
            
            # Краткая сводка
            summary = format_report_summary(
                totals, 
                "СВОДНЫЙ (Москвич + Анора)", 
                f"{date_from} .. {date_to}",
                len(report_rows)
            )
            await update.message.reply_text(summary)
            
            # Экспорт
            filename = f"otchet_svodny_{date_from}_{date_to}.xlsx"
            ReportGenerator.generate_xlsx(
                report_rows, totals, "СВОДНЫЙ (Москвич + Анора)", f"{date_from} .. {date_to}", filename, db
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
    
    # Создаем текстовый файл со списком совпадений
    file_content = ["📋 НАЙДЕНЫ СОВПАДЕНИЯ ДЛЯ ОБЪЕДИНЕНИЯ\n"]
    file_content.append(f"Период: {date_from} .. {date_to}\n")
    file_content.append("=" * 50 + "\n\n")
    
    for i, candidate in enumerate(merge_candidates, 1):
        file_content.append(f"{i}. {candidate['name']} {candidate['code']}\n")
        file_content.append(f"   • Москвич: НАЛ {candidate['moskvich']['nal']:.0f}, БЕЗНАЛ {candidate['moskvich']['beznal']:.0f}\n")
        file_content.append(f"   • Анора: НАЛ {candidate['anora']['nal']:.0f}, БЕЗНАЛ {candidate['anora']['beznal']:.0f}\n")
        file_content.append("\n")
    
    file_content.append("=" * 50 + "\n")
    file_content.append("\n🔄 ОБЪЕДИНЕНИЕ ДЛЯ СВОДНОГО ОТЧЁТА:\n")
    file_content.append("• ОК → объединить все\n")
    file_content.append("• ОК 1 → объединить только пункт 1\n")
    file_content.append("• ОК 1 2 → объединить пункты 1 и 2\n")
    file_content.append("• НЕ 1 → НЕ объединять пункт 1 (остальные да)\n")
    file_content.append("• НЕ 1 2 → НЕ объединять пункты 1 и 2\n")
    file_content.append("\nℹ️ Примечание: объединение ТОЛЬКО для отчёта\n")
    file_content.append("(данные в БД не изменяются)\n")
    
    # Сохраняем во временный файл
    temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.txt', delete=False)
    temp_file.write(''.join(file_content))
    temp_file.close()
    
    # Отправляем короткое сообщение с кнопками
    count = len(merge_candidates)
    short_message = (
        f"📋 Найдено совпадений: {count}\n\n"
        f"🔄 Объединение для сводного отчёта:\n"
        f"• Используйте кнопки ниже\n"
        f"• Или введите: ОК / ОК 1 / НЕ 1\n\n"
        f"📄 Детальный список в файле ⬇️\n\n"
        f"ℹ️ Объединение ТОЛЬКО для отчёта\n"
        f"(данные в БД не изменяются)"
    )
    
    # Отправляем файл и сообщение с кнопками
    with open(temp_file.name, 'rb') as f:
        await update.message.reply_document(
            document=f,
            filename=f"sovpadeniya_{date_from}_{date_to}.txt",
            caption=short_message,
            reply_markup=get_merge_confirmation_keyboard()
        )
    
    # Удаляем временный файл
    os.remove(temp_file.name)
    
    # Сохраняем кандидатов
    state.merge_candidates = merge_candidates
    state.merge_period = (date_from, date_to)
    state.mode = 'awaiting_merge_confirm'


async def handle_merge_confirmation(update: Update, state: UserState, choice: str, message=None):
    """Обработка подтверждения объединения для сводного отчёта"""
    # Используем message если передан, иначе update.message
    msg = message if message else update.message
    
    # Обработка ответа с новой логикой
    indices_to_merge = set()
    
    # Убираем знаки препинания для удобства парсинга: "не1,2" -> "не 1 2"
    normalized_text = choice.replace(',', ' ').replace('.', ' ')
    parts = normalized_text.split()
    
    if not parts:
        await msg.reply_text("❌ Неверный формат. Используйте: ок, ок 1, ок 1 2, не 1, не 1 2")
        return
    
    command = parts[0]
    
    if command in ['ок', 'ok']:
        # "ок" без номеров -> объединить ВСЕ
        if len(parts) == 1:
            indices_to_merge = set(range(len(state.merge_candidates)))
        else:
            # "ок 1 2" -> объединить ТОЛЬКО указанные
            try:
                indices_to_merge = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            except:
                await msg.reply_text("❌ Неверный формат номеров. Используйте: ок 1 2")
                return
    elif command in ['не', 'net', 'нет']:
        # "не 1 2" -> НЕ объединять указанные (объединить остальные)
        try:
            exclude_indices = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            indices_to_merge = set(range(len(state.merge_candidates))) - exclude_indices
        except:
            await msg.reply_text("❌ Неверный формат номеров. Используйте: не 1 2")
            return
    else:
        await msg.reply_text(
            "❌ Неверная команда.\n\n"
            "Используйте:\n"
            "• ок - объединить все\n"
            "• ок 1 - объединить только пункт 1\n"
            "• ок 1 2 - объединить пункты 1 и 2\n"
            "• не 1 - НЕ объединять пункт 1 (остальные объединить)\n"
            "• не 1 2 - НЕ объединять пункты 1 и 2"
        )
        return
    
    # Преобразуем в формат excluded (для совместимости с generate_merged_report)
    excluded = set(range(len(state.merge_candidates))) - indices_to_merge
    
    # Уведомление о начале генерации
    merged_count = len(indices_to_merge)
    await msg.reply_text(
        f"⏳ Генерация сводного отчёта...\n"
        f"Объединяется: {merged_count} из {len(state.merge_candidates)}"
    )
    
    # Генерируем сводный отчет (используем message если передан)
    await generate_merged_report(update, state, excluded, message)
    
    # Очищаем
    state.mode = None
    state.report_club = None
    state.merge_candidates = None
    state.merge_period = None


async def generate_merged_report(update: Update, state: UserState, excluded: set, message=None):
    """Генерация сводного отчета из ОБОИХ клубов"""
    # Используем message если передан, иначе update.message
    msg = message if message else update.message
    
    try:
        date_from, date_to = state.merge_period
        
        # Получаем ВСЕ данные обоих клубов
        ops_m = db.get_operations_by_period('Москвич', date_from, date_to)
        ops_a = db.get_operations_by_period('Анора', date_from, date_to)
    except Exception as e:
        await msg.reply_text(f"❌ Ошибка получения данных: {str(e)}")
        return
    
    # Создаём объединённый список операций для СВОДНОГО отчёта
    merged_ops = []
    
    # Множество обработанных пар (код, имя)
    processed = set()
    
    # 1. Добавляем ОБЪЕДИНЁННЫЕ записи (которые пользователь подтвердил)
    for i, candidate in enumerate(state.merge_candidates):
        code = candidate['code']
        name = candidate['name']
        names_m = candidate.get('names_m', [])
        names_a = candidate.get('names_a', [])
        name_variants = set(names_m + names_a)
        name_variants.add(name)
        
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
            
            for variant in name_variants:
                processed.add(make_processed_key(code, variant))
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
            
            for variant in name_variants:
                processed.add(make_processed_key(code, variant))
    
    # 2. Добавляем ВСЕ ОСТАЛЬНЫЕ записи (уникальные для каждого клуба)
    for op in ops_m + ops_a:
        if make_processed_key(op['code'], op['name']) not in processed:
            merged_ops.append(op)
    
    # Генерируем СВОДНЫЙ отчет
    if merged_ops:
        try:
            report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(merged_ops)
            
            # Краткая сводка вместо полного отчёта
            merged_count = len(state.merge_candidates) - len(excluded) if state.merge_candidates else 0
            summary = format_report_summary(
                totals, 
                "СВОДНЫЙ (Москвич + Анора)", 
                f"{date_from} .. {date_to}",
                len(report_rows),
                merged_count
            )
            await msg.reply_text(summary)
        except Exception as e:
            await msg.reply_text(f"❌ Ошибка генерации отчёта: {str(e)}")
            return
        
        # Экспорт сводного
        try:
            filename = f"otchet_svodny_{date_from}_{date_to}.xlsx"
            ReportGenerator.generate_xlsx(
                report_rows, totals, "СВОДНЫЙ (Москвич + Анора)", f"{date_from} .. {date_to}", filename, db
            )
            with open(filename, 'rb') as f:
                await msg.reply_document(
                    document=f, filename=filename,
                    caption=f"📊 СВОДНЫЙ ОТЧЁТ (Оба клуба)\nПериод: {date_from} .. {date_to}"
                )
            os.remove(filename)
        except Exception as e:
            await msg.reply_text(f"⚠️ Ошибка создания Excel: {str(e)}")
    else:
        await msg.reply_text("ℹ️ Нет данных для сводного отчета")


def find_code_duplicates(operations: list) -> list:
    """
    Поиск дубликатов: один код, но одна запись с именем, другая без
    """
    from collections import defaultdict
    
    by_code = defaultdict(lambda: {'with_name': [], 'without_name': []})
    
    for op in operations:
        code = op['code']
        if op['name']:
            by_code[code]['with_name'].append(op)
        else:
            by_code[code]['without_name'].append(op)
    
    # Ищем коды где есть И с именем И без имени
    duplicates = []
    for code, data in by_code.items():
        if data['with_name'] and data['without_name']:
            duplicates.append({
                'code': code,
                'with_name': data['with_name'],
                'without_name': data['without_name']
            })
    
    return duplicates


def name_similarity(name1: str, name2: str) -> float:
    """Вычисление похожести двух имен (0.0 - 1.0)"""
    if not name1 or not name2:
        return 0.0
    return SequenceMatcher(None, name1.lower().strip(), name2.lower().strip()).ratio()


def normalize_name_variants(name: str) -> List[str]:
    """Создание вариантов имени в разных порядках (ФИО, ИОФ, ОИФ)"""
    if not name:
        return ['']
    
    name = name.strip()
    parts = name.split()
    
    if len(parts) <= 1:
        return [name]
    
    # Убираем лишние пробелы и приводим к единому формату
    normalized = ' '.join(part.strip() for part in parts if part.strip())
    
    # Если 2 части - только 2 варианта
    if len(parts) == 2:
        return [normalized, f"{parts[1]} {parts[0]}"]
    
    # Если 3+ части - создаем основные варианты
    variants = set()
    variants.add(normalized)  # Оригинальный порядок
    
    if len(parts) >= 2:
        variants.add(f"{parts[1]} {parts[0]}")  # ИОФ
        if len(parts) >= 3:
            variants.add(f"{parts[0]} {parts[2]} {parts[1]}")  # ФОИ
            variants.add(f"{parts[1]} {parts[0]} {parts[2]}")  # ИОФ (полный)
    
    return list(variants)


def find_sb_name_duplicates(operations: list, similarity_threshold: float = 0.75) -> list:
    """
    Поиск СБ сотрудников с похожими именами для объединения
    similarity_threshold: порог похожести (0.75 = 75%)
    """
    from collections import defaultdict
    
    # Фильтруем только СБ
    sb_operations = [op for op in operations if op['code'] == 'СБ' and op.get('name')]
    
    if len(sb_operations) < 2:
        return []
    
    # Группируем по именам
    by_name = defaultdict(list)
    for op in sb_operations:
        name = op['name'].strip()
        if name:
            by_name[name].append(op)
    
    # Находим похожие имена
    name_groups = []
    processed_names = set()
    
    names_list = list(by_name.keys())
    
    for i, name1 in enumerate(names_list):
        if name1 in processed_names:
            continue
        
        # Создаем варианты для name1
        variants1 = normalize_name_variants(name1)
        
        # Ищем похожие имена
        similar_names = [name1]
        group_max_similarity = 0.0
        
        for j, name2 in enumerate(names_list[i+1:], i+1):
            if name2 in processed_names:
                continue
            
            # Создаем варианты для name2
            variants2 = normalize_name_variants(name2)
            
            # Сравниваем все варианты
            max_similarity = 0.0
            for v1 in variants1:
                for v2 in variants2:
                    similarity = name_similarity(v1, v2)
                    max_similarity = max(max_similarity, similarity)
            
            # Если похожесть выше порога - добавляем в группу
            if max_similarity >= similarity_threshold:
                similar_names.append(name2)
                processed_names.add(name2)
                group_max_similarity = max(group_max_similarity, max_similarity)
        
        # Если нашли похожие - создаем группу
        if len(similar_names) > 1:
            processed_names.add(name1)
            
            # Собираем все операции для этой группы
            group_operations = []
            for name in similar_names:
                group_operations.extend(by_name[name])
            
            # Вычисляем суммы
            total_nal = sum(op['amount'] for op in group_operations if op['channel'] == 'нал')
            total_beznal = sum(op['amount'] for op in group_operations if op['channel'] == 'безнал')
            
            # Определяем основное имя (самое длинное или первое)
            main_name = max(similar_names, key=len)
            
            name_groups.append({
                'names': similar_names,
                'main_name': main_name,
                'operations': group_operations,
                'total_nal': total_nal,
                'total_beznal': total_beznal,
                'similarity': group_max_similarity if group_max_similarity > 0 else 1.0
            })
    
    return name_groups


async def handle_duplicate_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                       state: UserState, text: str, text_lower: str):
    """Обработка подтверждения объединения дубликатов"""
    if not state.duplicate_check_data:
        await update.message.reply_text("❌ Ошибка: данные не найдены")
        state.mode = None
        return
    
    data = state.duplicate_check_data
    duplicates = data['duplicates']
    operations = data['operations']
    
    # Обработка ответа с новой логикой
    indices_to_merge = set()
    
    # Убираем знаки препинания для удобства парсинга: "не1,2" -> "не 1 2"
    normalized_text = text_lower.replace(',', ' ').replace('.', ' ')
    parts = normalized_text.split()
    
    if not parts:
        await update.message.reply_text("❌ Неверный формат. Используйте: ок, ок 1, ок 1 2, не 1, не 1 2")
        return
    
    command = parts[0]
    
    if command in ['ок', 'ok']:
        # "ок" без номеров -> объединить ВСЕ
        if len(parts) == 1:
            indices_to_merge = set(range(len(duplicates)))
        else:
            # "ок 1 2" -> объединить ТОЛЬКО указанные
            try:
                indices_to_merge = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            except:
                await update.message.reply_text("❌ Неверный формат номеров. Используйте: ок 1 2")
                return
    elif command in ['не', 'net', 'нет']:
        # "не 1 2" -> НЕ объединять указанные (объединить остальные)
        try:
            exclude_indices = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            indices_to_merge = set(range(len(duplicates))) - exclude_indices
        except:
            await update.message.reply_text("❌ Неверный формат номеров. Используйте: не 1 2")
            return
    else:
        await update.message.reply_text(
            "❌ Неверная команда.\n\n"
            "Используйте:\n"
            "• ок - объединить все\n"
            "• ок 1 - объединить только пункт 1\n"
            "• ок 1 2 - объединить пункты 1 и 2\n"
            "• не 1 - НЕ объединять пункт 1 (остальные объединить)\n"
            "• не 1 2 - НЕ объединять пункты 1 и 2"
        )
        return
    
    # СОХРАНЯЕМ ОБЪЕДИНЕНИЕ В БД!
    updated_count = 0
    
    for i, dup in enumerate(duplicates):
        if i in indices_to_merge:
            code = dup['code']
            
            # Берём имя из записи с именем
            if dup['with_name']:
                merged_name = dup['with_name'][0]['name']
                
                # Обновляем ВСЕ записи БЕЗ имени для этого кода в БД
                for op_without_name in dup['without_name']:
                    # Обновляем в БД
                    success, msg = db.update_operation_name(
                        club=data['club'],
                        date=op_without_name['date'],
                        code=code,
                        channel=op_without_name['channel'],
                        new_name=merged_name
                    )
                    if success:
                        updated_count += 1
    
    # Получаем ОБНОВЛЁННЫЕ данные из БД
    updated_operations = db.get_operations_by_period(data['club'], data['date_from'], data['date_to'])
    
    # Проверяем СБ с похожими именами после обработки дубликатов кода
    sb_duplicates = find_sb_name_duplicates(updated_operations)
    if sb_duplicates:
        # Показываем запрос на объединение СБ
        await prepare_sb_merge(update, state, data['club'], data['date_from'], data['date_to'], updated_operations, sb_duplicates)
        return
    
    # Генерируем отчёт с объединёнными данными
    report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(updated_operations)
    
    # Краткая сводка с информацией об объединении
    summary = format_report_summary(
        totals, 
        data['club'], 
        f"{data['date_from']} .. {data['date_to']}",
        len(report_rows),
        updated_count
    )
    
    await update.message.reply_text(summary)
    
    # Создаем XLSX
    club_translit = 'moskvich' if data['club'] == 'Москвич' else 'anora'
    filename = f"otchet_{club_translit}_{data['date_from']}_{data['date_to']}.xlsx"
    
    ReportGenerator.generate_xlsx(report_rows, totals, data['club'], 
                                  f"{data['date_from']} .. {data['date_to']}", filename, db)
    
    with open(filename, 'rb') as f:
        await update.message.reply_document(
            document=f,
            filename=filename,
            caption=f"📊 Отчет {data['club']} ({data['date_from']} .. {data['date_to']})"
        )
    
    os.remove(filename)
    
    # Очищаем состояние
    state.mode = None
    state.duplicate_check_data = None
    state.sb_merge_data = None
    state.report_club = None


async def handle_sb_merge_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                      state: UserState, text: str, text_lower: str):
    """Обработка подтверждения объединения СБ с похожими именами"""
    if not state.sb_merge_data:
        await update.message.reply_text("❌ Ошибка: данные не найдены")
        state.mode = None
        return
    
    data = state.sb_merge_data
    sb_duplicates = data['sb_duplicates']
    
    # Обработка ответа
    indices_to_merge = set()
    
    # Убираем знаки препинания
    normalized_text = text_lower.replace(',', ' ').replace('.', ' ')
    parts = normalized_text.split()
    
    if not parts:
        await update.message.reply_text("❌ Неверный формат. Используйте: ок, ок 1, ок 1 2, не 1, не 1 2")
        return
    
    command = parts[0]
    
    if command in ['ок', 'ok']:
        if len(parts) == 1:
            indices_to_merge = set(range(len(sb_duplicates)))
        else:
            try:
                indices_to_merge = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            except:
                await update.message.reply_text("❌ Неверный формат номеров. Используйте: ок 1 2")
                return
    elif command in ['не', 'net', 'нет']:
        try:
            exclude_indices = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            indices_to_merge = set(range(len(sb_duplicates))) - exclude_indices
        except:
            await update.message.reply_text("❌ Неверный формат номеров. Используйте: не 1 2")
            return
    else:
        await update.message.reply_text(
            "❌ Неверная команда.\n\n"
            "Используйте:\n"
            "• ок - объединить все\n"
            "• ок 1 - объединить только пункт 1\n"
            "• ок 1 2 - объединить пункты 1 и 2\n"
            "• не 1 - НЕ объединять пункт 1 (остальные объединить)\n"
            "• не 1 2 - НЕ объединять пункты 1 и 2"
        )
        return
    
    # Создаем словарь объединений (ТОЛЬКО для отчета, БД не изменяем!)
    sb_name_merges = {}
    
    for i, group in enumerate(sb_duplicates):
        if i in indices_to_merge:
            main_name = group['main_name']
            
            # Для всех похожих имен указываем основное имя
            for name in group['names']:
                if name != main_name:
                    sb_name_merges[name] = main_name
    
    # Получаем данные из БД (БЕЗ изменений!)
    operations = db.get_operations_by_period(data['club'], data['date_from'], data['date_to'])
    
    # Генерируем отчёт с объединёнными данными (только для отчета)
    report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(
        operations, 
        sb_name_merges=sb_name_merges if sb_name_merges else None
    )
    
    # Краткая сводка
    summary = format_report_summary(
        totals, 
        data['club'], 
        f"{data['date_from']} .. {data['date_to']}",
        len(report_rows),
        0  # Объединение только для отчета, не сохраняется в БД
    )
    
    await update.message.reply_text(summary)
    
    # Создаем XLSX
    club_translit = 'moskvich' if data['club'] == 'Москвич' else 'anora'
    filename = f"otchet_{club_translit}_{data['date_from']}_{data['date_to']}.xlsx"
    
    ReportGenerator.generate_xlsx(report_rows, totals, data['club'], 
                                  f"{data['date_from']} .. {data['date_to']}", filename, db)
    
    with open(filename, 'rb') as f:
        await update.message.reply_document(
            document=f,
            filename=filename,
            caption=f"📊 Отчет {data['club']} ({data['date_from']} .. {data['date_to']})"
        )
    
    os.remove(filename)
    
    # Проверяем, был ли выбран "оба" клуба - если да, продолжаем обработку
    if state.report_club == 'оба':
        # Продолжаем обработку для второго клуба и сводного отчета
        processed_club = data['club']
        remaining_clubs = ['Москвич', 'Анора']
        remaining_clubs.remove(processed_club)
        
        # Обрабатываем оставшийся клуб через generate_and_send_report
        for club in remaining_clubs:
            await generate_and_send_report(update, club, data['date_from'], data['date_to'], state)
            # Если установлен режим ожидания - выходим
            if state.mode in ['awaiting_duplicate_confirm', 'awaiting_sb_merge_confirm']:
                return
        
        # Если нет дубликатов - генерируем сводный отчет
        await prepare_merged_report(update, state, data['date_from'], data['date_to'])
        
        # НЕ сбрасываем режим если ждём подтверждения объединения!
        if state.mode != 'awaiting_merge_confirm':
            state.mode = None
            state.report_club = None
    else:
        # Очищаем состояние
        state.mode = None
        state.sb_merge_data = None
        state.report_club = None


async def prepare_sb_merge(update: Update, state: UserState, club: str, date_from: str,
                           date_to: str, operations: list, sb_duplicates: list):
    """Подготовка объединения СБ с похожими именами"""
    # Создаем текстовый файл со списком СБ кандидатов
    file_content = ["📋 НАЙДЕНЫ СБ С ПОХОЖИМИ ИМЕНАМИ\n"]
    file_content.append(f"Клуб: {club}\n")
    file_content.append(f"Период: {date_from} .. {date_to}\n")
    file_content.append("=" * 50 + "\n\n")
    
    for i, group in enumerate(sb_duplicates, 1):
        similarity_pct = int(group['similarity'] * 100)
        file_content.append(f"{i}. Группа: {group['main_name']}\n")
        file_content.append(f"   Похожесть: {similarity_pct}%\n")
        
        # Группируем операции по именам для отображения
        by_name = {}
        for op in group['operations']:
            name = op['name']
            if name not in by_name:
                by_name[name] = {'nal': 0, 'beznal': 0}
            if op['channel'] == 'нал':
                by_name[name]['nal'] += op['amount']
            else:
                by_name[name]['beznal'] += op['amount']
        
        for name in group['names']:
            if name in by_name:
                file_content.append(f"   • {name}: НАЛ {by_name[name]['nal']:.0f}, БЕЗНАЛ {by_name[name]['beznal']:.0f}\n")
        
        file_content.append(f"   ИТОГО: НАЛ {group['total_nal']:.0f}, БЕЗНАЛ {group['total_beznal']:.0f}\n")
        file_content.append("\n")
    
    file_content.append("=" * 50 + "\n")
    file_content.append("\n🔄 ОБЪЕДИНЕНИЕ СБ:\n")
    file_content.append("• ОК → объединить все\n")
    file_content.append("• ОК 1 → объединить только пункт 1\n")
    file_content.append("• ОК 1 2 → объединить пункты 1 и 2\n")
    file_content.append("• НЕ 1 → НЕ объединять пункт 1 (остальные да)\n")
    file_content.append("• НЕ 1 2 → НЕ объединять пункты 1 и 2\n")
    file_content.append("\nℹ️ Примечание: объединение сохраняется в БД\n")
    
    # Сохраняем во временный файл
    temp_file = tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', suffix='.txt', delete=False)
    temp_file.write(''.join(file_content))
    temp_file.close()
    
    # Отправляем короткое сообщение с кнопками
    count = len(sb_duplicates)
    short_message = (
        f"📋 Найдено групп СБ: {count}\n\n"
        f"🔄 Объединение СБ с похожими именами:\n"
        f"• Используйте кнопки ниже\n"
        f"• Или введите: ОК / ОК 1 / НЕ 1\n\n"
        f"📄 Детальный список в файле ⬇️\n\n"
        f"ℹ️ Объединение сохраняется в БД"
    )
    
    # Отправляем файл и сообщение с кнопками
    with open(temp_file.name, 'rb') as f:
        await update.message.reply_document(
            document=f,
            filename=f"sb_merge_{club}_{date_from}_{date_to}.txt",
            caption=short_message,
            reply_markup=get_merge_confirmation_keyboard()
        )
    
    # Удаляем временный файл
    os.remove(temp_file.name)
    
    # Сохраняем данные для обработки
    state.sb_merge_data = {
        'club': club,
        'date_from': date_from,
        'date_to': date_to,
        'operations': operations,
        'sb_duplicates': sb_duplicates
    }
    state.mode = 'awaiting_sb_merge_confirm'


async def generate_and_send_report(update: Update, club: str, date_from: str, date_to: str, 
                                  state: UserState = None, check_duplicates: bool = True):
    """Генерация и отправка отчета"""
    operations = db.get_operations_by_period(club, date_from, date_to)
    
    if not operations:
        await update.message.reply_text(
            f"📊 Отчет по клубу {club}\n"
            f"Период: {date_from} .. {date_to}\n\n"
            f"Данных нет."
        )
        return
    
    # Проверка на дубликаты (одинаковый код, но с именем и без)
    if check_duplicates and state:
        duplicates = find_code_duplicates(operations)
        
        if duplicates:
            # Показываем запрос на объединение
            response = [f"⚠️ Найдены записи с одинаковым кодом:\n"]
            
            for i, dup in enumerate(duplicates, 1):
                response.append(f"{i}. Код: {dup['code']}")
                
                # С именем
                names_with = set(op['name'] for op in dup['with_name'])
                for name in names_with:
                    ops = [op for op in dup['with_name'] if op['name'] == name]
                    total_nal = sum(op['amount'] for op in ops if op['channel'] == 'нал')
                    total_bez = sum(op['amount'] for op in ops if op['channel'] == 'безнал')
                    response.append(f"   • {name}: НАЛ {total_nal:.0f}, БЕЗНАЛ {total_bez:.0f}")
                
                # Без имени
                total_nal_no = sum(op['amount'] for op in dup['without_name'] if op['channel'] == 'нал')
                total_bez_no = sum(op['amount'] for op in dup['without_name'] if op['channel'] == 'безнал')
                response.append(f"   • (без имени): НАЛ {total_nal_no:.0f}, БЕЗНАЛ {total_bez_no:.0f}")
                response.append("")
            
            response.append("─" * 35)
            response.append("\n🔄 ОБЪЕДИНЕНИЕ ДУБЛИКАТОВ:\n")
            response.append("• ОК → объединить все")
            response.append("• ОК 1 → объединить только пункт 1")
            response.append("• ОК 1 2 → объединить пункты 1 и 2")
            response.append("• НЕ 1 → НЕ объединять пункт 1 (остальные да)")
            response.append("• НЕ 1 2 → НЕ объединять пункты 1 и 2")
            
            await update.message.reply_text('\n'.join(response))
            
            # Сохраняем данные для обработки
            state.duplicate_check_data = {
                'club': club,
                'date_from': date_from,
                'date_to': date_to,
                'operations': operations,
                'duplicates': duplicates
            }
            state.mode = 'awaiting_duplicate_confirm'
            return
    
    # Проверка на СБ с похожими именами (после проверки дубликатов кода)
    if check_duplicates and state:
        sb_duplicates = find_sb_name_duplicates(operations)
        
        if sb_duplicates:
            # Показываем запрос на объединение СБ с инлайн-кнопками и файлом
            await prepare_sb_merge(update, state, club, date_from, date_to, operations, sb_duplicates)
            return
    
    # Генерируем отчет (без дубликатов или после подтверждения)
    report_rows, totals, totals_recalc, check_ok = ReportGenerator.calculate_report(operations)
    
    # Краткая сводка вместо полного отчёта
    summary = format_report_summary(
        totals, 
        club, 
        f"{date_from} .. {date_to}",
        len(report_rows)
    )
    await update.message.reply_text(summary)
    
    # Создаем XLSX
    club_translit = 'moskvich' if club == 'Москвич' else 'anora'
    filename = f"otchet_{club_translit}_{date_from}_{date_to}.xlsx"
    
    ReportGenerator.generate_xlsx(
        report_rows, totals, club, f"{date_from} .. {date_to}", filename, db
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
    parts = text.split()
    
    # Определяем формат ввода
    if parts[0].lower() == 'выплаты':
        # Формат: выплаты Д1 30,10-1,11
        if len(parts) < 3:
            await update.message.reply_text(
                "❌ Неверный формат.\n"
                "Пример: выплаты Д1 30,10-1,11"
            )
            return
        code = DataParser.normalize_code(parts[1])
        period_str = parts[2]
    else:
        # Упрощённый формат (после кнопки): Д1 30,10-1,11
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ Неверный формат.\n"
                "Пример: Д1 30,10-1,11"
            )
            return
        code = DataParser.normalize_code(parts[0])
        period_str = parts[1]
    
    # Парсим период (одна дата или диапазон)
    if '-' in period_str:
        success, date_from, date_to, error = parse_date_range(period_str)
        if not success:
            await update.message.reply_text(f"❌ {error}")
            return
    else:
        success, single_date, error = parse_short_date(period_str)
        if not success:
            await update.message.reply_text(f"❌ {error}")
            return
        date_from = single_date
        date_to = single_date
    
    # Получаем выплаты ПО ВСЕМ КЛУБАМ
    payments = db.get_employee_payments(code, date_from, date_to, None)
    
    if not payments:
        await update.message.reply_text(
            f"📊 Выплаты сотруднику {code}\n"
            f"Период: {date_from} .. {date_to}\n\n"
            f"Данных нет."
        )
        return
    
    # Формируем ответ с группировкой по клубам
    response_parts = []
    response_parts.append(f"📊 Выплаты сотруднику {code}")
    response_parts.append(f"Период: {date_from} .. {date_to}\n")
    
    # Группируем по клубам
    from collections import defaultdict
    by_club = defaultdict(lambda: {'nal': 0, 'beznal': 0, 'payments': []})
    
    for payment in payments:
        club = payment['club']
        by_club[club]['payments'].append(payment)
        
        if payment['channel'] == 'нал':
            by_club[club]['nal'] += payment['amount']
        else:
            by_club[club]['beznal'] += payment['amount']
    
    # Общие итоги
    total_nal = 0
    total_beznal = 0
    
    # Выводим по каждому клубу
    for club in sorted(by_club.keys()):
        data = by_club[club]
        response_parts.append(f"🏢 Клуб: {club}")
        
        for payment in data['payments']:
            if payment['channel'] == 'нал':
                response_parts.append(
                    f"  {payment['date']} | НАЛ     | {payment['name']:15} | {payment['amount']:.0f}"
                )
            else:
                # БЕЗНАЛ - показываем полную сумму и к выплате (минус 10%)
                to_pay = payment['amount'] * 0.9
                response_parts.append(
                    f"  {payment['date']} | БЕЗНАЛ  | {payment['name']:15} | {payment['amount']:.0f} (к выплате: {to_pay:.0f})"
                )
        
        # Итог по клубу
        club_total = data['nal'] + (data['beznal'] * 0.9)
        response_parts.append(f"  Итого {club}: {club_total:.0f}\n")
        
        total_nal += data['nal']
        total_beznal += data['beznal']
    
    # Общий итог по всем клубам
    total_minus10 = total_beznal * 0.1
    total_itog = total_nal + (total_beznal - total_minus10)
    
    response_parts.append("💰 ИТОГО ПО ВСЕМ КЛУБАМ:")
    response_parts.append(f"  НАЛ: {total_nal:.0f}")
    response_parts.append(f"  БЕЗНАЛ: {total_beznal:.0f}")
    response_parts.append(f"  10% от безнала: {total_minus10:.0f}")
    response_parts.append(f"  ИТОГО к выплате: {total_itog:.0f}")
    
    await update.message.reply_text('\n'.join(response_parts))


async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на inline кнопки"""
    query = update.callback_query
    
    user_id = update.effective_user.id
    
    # Проверка авторизации
    if user_id not in AUTHORIZED_USERS:
        await query.answer("🔒 Требуется авторизация", show_alert=True)
        await query.message.reply_text(
            "🔒 Введите пин-код для доступа:",
            reply_markup=ReplyKeyboardRemove()
        )
        return
    
    await query.answer()
    state = get_user_state(user_id)
    
    # Выбор клуба при старте
    if query.data == 'club_moskvich':
        # Проверяем режим - загрузка файла или обычный старт
        if state.mode == 'awaiting_upload_club':
            state.upload_file_club = 'Москвич'
            await query.edit_message_text(
                f"📎 ЗАГРУЗКА ФАЙЛА\n"
                f"🏢 Клуб: Москвич\n\n"
                f"📅 Введите дату для этих данных:\n"
                f"Формат: 3,11 или 30,10"
            )
            state.mode = 'awaiting_upload_date'
        else:
            state.club = 'Москвич'
            state.current_date = get_current_date()
            state.reset_input()
            
            await query.edit_message_text(
                f"✅ Выбран клуб: Москвич\n"
                f"📅 Дата: {state.current_date}"
            )
            await query.message.reply_text(
                "Используйте кнопки ниже для работы:",
                reply_markup=get_main_keyboard()
            )
    
    elif query.data == 'club_anora':
        # Проверяем режим - загрузка файла или обычный старт
        if state.mode == 'awaiting_upload_club':
            state.upload_file_club = 'Анора'
            await query.edit_message_text(
                f"📎 ЗАГРУЗКА ФАЙЛА\n"
                f"🏢 Клуб: Анора\n\n"
                f"📅 Введите дату для этих данных:\n"
                f"Формат: 3,11 или 30,10"
            )
            state.mode = 'awaiting_upload_date'
        else:
            state.club = 'Анора'
            state.current_date = get_current_date()
            state.reset_input()
            
            await query.edit_message_text(
                f"✅ Выбран клуб: Анора\n"
                f"📅 Дата: {state.current_date}"
            )
            await query.message.reply_text(
                "Используйте кнопки ниже для работы:",
                reply_markup=get_main_keyboard()
            )
    
    # Выбор режима удаления
    elif query.data == 'delete_mode_employee':
        state.mode = 'awaiting_delete_employee_input'
        await query.edit_message_text(
            "✏️ Введите код и дату сотрудника для удаления:\n"
            "Пример: Д1 30,10"
        )
    elif query.data == 'delete_mode_mass':
        state.delete_mass_club = None
        state.delete_mass_date_from = None
        state.delete_mass_date_to = None
        state.delete_mass_preview = None
        state.mode = 'awaiting_delete_mass_club'
        await query.edit_message_text(
            "🏢 Выберите клуб для удаления:",
            reply_markup=get_club_report_keyboard()
        )
    
    # Выбор клуба для отчёта / экспорта / списка
    elif query.data in ['report_club_moskvich', 'report_club_anora', 'report_club_both']:
        club_map = {
            'report_club_moskvich': 'москвич',
            'report_club_anora': 'анора',
            'report_club_both': 'оба'
        }
        
        # Определяем режим (отчёт, экспорт или список)
        if state.mode == 'awaiting_export_club':
            state.export_club = club_map[query.data]
            await query.edit_message_text(
                f"Экспорт: {state.export_club}\n\n"
                f"Укажите дату или период:\n"
                f"• Одна дата: 12,12\n"
                f"• Период: 10,06-11,08"
            )
            state.mode = 'awaiting_export_period'
        elif state.mode == 'awaiting_list_club':
            state.list_club = club_map[query.data]
            await query.edit_message_text(
                f"📋 Список: {state.list_club}\n\n"
                f"📅 Введите дату:\n"
                f"• 3,11\n"
                f"• 30,10"
            )
            state.mode = 'awaiting_list_date'
        elif state.mode == 'awaiting_delete_mass_club':
            state.delete_mass_club = club_map[query.data]
            state.delete_mass_date_from = None
            state.delete_mass_date_to = None
            state.delete_mass_preview = None
            await query.edit_message_text(
                f"Удаление ({state.delete_mass_club})\n\n"
                f"📅 Укажите дату или период для удаления:\n"
                f"• 5,11\n"
                f"• 2,11-5,11"
            )
            state.mode = 'awaiting_delete_mass_period'
        else:
            state.report_club = club_map[query.data]
            await query.edit_message_text(
                f"Клуб: {state.report_club}\n\n"
                f"Укажите дату или период:\n"
                f"• Одна дата: 12,12\n"
                f"• Период: 10,06-11,08"
            )
            state.mode = 'awaiting_report_period'
    
    # Выбор что удалить
    elif query.data in ['delete_nal', 'delete_beznal', 'delete_both']:
        channel_map = {
            'delete_nal': 'нал',
            'delete_beznal': 'безнал',
            'delete_both': 'обе'
        }
        choice = channel_map[query.data]
        
        await query.edit_message_text(f"Удаление: {choice.upper()}...")
        
        # Обработка удаления
        if choice in ['нал', 'безнал']:
            if choice in state.delete_records:
                db.delete_operation(state.club, state.delete_date, state.delete_code, choice)
                await query.message.reply_text(
                    f"✅ Удалено: {state.delete_code} {choice.upper()} за {state.delete_date}"
                )
            else:
                await query.message.reply_text(f"❌ Записи {choice.upper()} нет")
        
        elif choice == 'обе':
            deleted = []
            for channel in ['нал', 'безнал']:
                if channel in state.delete_records:
                    db.delete_operation(state.club, state.delete_date, state.delete_code, channel)
                    deleted.append(channel.upper())
            
            if deleted:
                await query.message.reply_text(
                    f"✅ Удалено: {state.delete_code} {', '.join(deleted)} за {state.delete_date}"
                )
            else:
                await query.message.reply_text("❌ Нет записей для удаления")
        
        state.mode = None

    elif query.data == 'delete_mass_confirm_yes':
        await query.edit_message_reply_markup(None)
        await handle_delete_mass_confirm_message(query.message, state, True)
    elif query.data == 'delete_mass_confirm_no':
        await query.edit_message_reply_markup(None)
        await handle_delete_mass_confirm_message(query.message, state, False)
    
    # Управление самозанятыми
    elif query.data == 'self_employed_add':
        await query.edit_message_text("➕ Введите код для добавления в самозанятые:\n\nПример: Д7")
        state.mode = 'awaiting_self_employed_add'
    elif query.data == 'self_employed_remove':
        await query.edit_message_text("➖ Введите код для удаления из самозанятых:\n\nПример: Д7")
        state.mode = 'awaiting_self_employed_remove'
    elif query.data == 'self_employed_close':
        await query.edit_message_text("✅ Закрыто")
        state.mode = None
    
    # Обработка объединения совпадений
    elif query.data == 'merge_all':
        if state.mode == 'awaiting_merge_confirm' and state.merge_candidates:
            await query.edit_message_reply_markup(None)
            await handle_merge_confirmation(update, state, 'ок', message=query.message)
        elif state.mode == 'awaiting_sb_merge_confirm' and state.sb_merge_data:
            await query.edit_message_reply_markup(None)
            await handle_sb_merge_confirmation(update, context, state, 'ок', 'ок')
        else:
            await query.answer("❌ Ошибка: данные не найдены", show_alert=True)
    
    elif query.data == 'merge_none':
        if state.mode == 'awaiting_merge_confirm' and state.merge_candidates:
            # Формируем команду "не" со всеми номерами
            all_numbers = ' '.join(str(i+1) for i in range(len(state.merge_candidates)))
            await query.edit_message_reply_markup(None)
            await handle_merge_confirmation(update, state, f'не {all_numbers}', message=query.message)
        elif state.mode == 'awaiting_sb_merge_confirm' and state.sb_merge_data:
            # Формируем команду "не" со всеми номерами
            sb_duplicates = state.sb_merge_data['sb_duplicates']
            all_numbers = ' '.join(str(i+1) for i in range(len(sb_duplicates)))
            await query.edit_message_reply_markup(None)
            await handle_sb_merge_confirmation(update, context, state, f'не {all_numbers}', f'не {all_numbers}')
        else:
            await query.answer("❌ Ошибка: данные не найдены", show_alert=True)
    
    elif query.data == 'merge_show_list':
        if state.mode == 'awaiting_merge_confirm' and state.merge_candidates:
            # Показываем список частями (по 15 записей)
            await query.answer("📄 Отправляю список...")
            candidates = state.merge_candidates
            chunk_size = 15
            
            for chunk_start in range(0, len(candidates), chunk_size):
                chunk = candidates[chunk_start:chunk_start + chunk_size]
                response = [f"📋 Совпадения ({chunk_start + 1}-{min(chunk_start + chunk_size, len(candidates))} из {len(candidates)}):\n"]
                
                for i, candidate in enumerate(chunk, chunk_start + 1):
                    response.append(f"{i}. {candidate['name']} {candidate['code']}")
                    response.append(f"   • Москвич: НАЛ {candidate['moskvich']['nal']:.0f}, БЕЗНАЛ {candidate['moskvich']['beznal']:.0f}")
                    response.append(f"   • Анора: НАЛ {candidate['anora']['nal']:.0f}, БЕЗНАЛ {candidate['anora']['beznal']:.0f}")
                    response.append("")
                
                await query.message.reply_text('\n'.join(response))
        elif state.mode == 'awaiting_sb_merge_confirm' and state.sb_merge_data:
            # Показываем список СБ частями
            await query.answer("📄 Отправляю список...")
            sb_duplicates = state.sb_merge_data['sb_duplicates']
            chunk_size = 15
            
            for chunk_start in range(0, len(sb_duplicates), chunk_size):
                chunk = sb_duplicates[chunk_start:chunk_start + chunk_size]
                response = [f"📋 СБ с похожими именами ({chunk_start + 1}-{min(chunk_start + chunk_size, len(sb_duplicates))} из {len(sb_duplicates)}):\n"]
                
                for i, group in enumerate(chunk, chunk_start + 1):
                    similarity_pct = int(group['similarity'] * 100)
                    response.append(f"{i}. Группа: {group['main_name']} (Похожесть: {similarity_pct}%)")
                    
                    # Группируем операции по именам
                    by_name = {}
                    for op in group['operations']:
                        name = op['name']
                        if name not in by_name:
                            by_name[name] = {'nal': 0, 'beznal': 0}
                        if op['channel'] == 'нал':
                            by_name[name]['nal'] += op['amount']
                        else:
                            by_name[name]['beznal'] += op['amount']
                    
                    for name in group['names']:
                        if name in by_name:
                            response.append(f"   • {name}: НАЛ {by_name[name]['nal']:.0f}, БЕЗНАЛ {by_name[name]['beznal']:.0f}")
                    response.append(f"   ИТОГО: НАЛ {group['total_nal']:.0f}, БЕЗНАЛ {group['total_beznal']:.0f}")
                    response.append("")
                
                await query.message.reply_text('\n'.join(response))
        else:
            await query.answer("❌ Ошибка: данные не найдены", show_alert=True)


def format_report_summary(totals: Dict, club_name: str, period: str, 
                         employee_count: int, merged_count: int = 0) -> str:
    """
    Форматирование краткой сводки отчёта
    totals: словарь с итогами
    club_name: название клуба или "СВОДНЫЙ"
    period: период отчёта
    employee_count: количество сотрудников
    merged_count: количество объединённых дубликатов (если есть)
    """
    lines = []
    lines.append("✅ ОТЧЁТ ГОТОВ!\n")
    lines.append(f"🏢 Клуб: {club_name}")
    lines.append(f"📅 Период: {period}")
    lines.append(f"👥 Сотрудников: {employee_count}")
    
    if merged_count > 0:
        lines.append(f"🔄 Объединено дубликатов: {merged_count}")
    
    lines.append("\n💰 ИТОГО:")
    lines.append(f"   НАЛ:      {totals['nal']:,.0f}".replace(',', ' '))
    lines.append(f"   БЕЗНАЛ:   {totals['beznal']:,.0f}".replace(',', ' '))
    lines.append(f"   10%:      {totals['minus10']:,.0f}".replace(',', ' '))
    lines.append(f"   {'─' * 25}")
    lines.append(f"   ИТОГО:    {totals['itog']:,.0f}".replace(',', ' '))
    lines.append("\n📄 Детальный отчёт в Excel файле ⬇️")
    
    return '\n'.join(lines)


async def handle_self_employed_command(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       state: UserState):
    """Команда управления самозанятыми"""
    codes = db.get_all_self_employed()
    
    if not codes:
        message = "📋 Список самозанятых пуст."
    else:
        message = f"👔 САМОЗАНЯТЫЕ ({len(codes)} чел.):\n\n"
        message += ", ".join(codes)
    
    await update.message.reply_text(
        message,
        reply_markup=get_self_employed_action_keyboard()
    )


async def handle_self_employed_add(update: Update, state: UserState, code: str):
    """Добавление кода в самозанятые"""
    from parser import DataParser
    
    code = code.strip()
    
    # Проверка формата кода
    if not DataParser.is_code(code):
        await update.message.reply_text(
            "❌ Неверный формат кода.\n"
            "Примеры: Д7, Р1, Б52, К21"
        )
        return
    
    # Нормализуем код
    normalized_code = DataParser.normalize_code(code)
    
    # Добавляем в БД
    success, message = db.add_self_employed(normalized_code)
    
    await update.message.reply_text(message)
    
    # Сбрасываем режим
    state.mode = None


async def handle_self_employed_remove(update: Update, state: UserState, code: str):
    """Удаление кода из самозанятых"""
    from parser import DataParser
    
    code = code.strip()
    
    # Проверка формата кода
    if not DataParser.is_code(code):
        await update.message.reply_text(
            "❌ Неверный формат кода.\n"
            "Примеры: Д7, Р1, Б52, К21"
        )
        return
    
    # Нормализуем код
    normalized_code = DataParser.normalize_code(code)
    
    # Удаляем из БД
    success, message = db.remove_self_employed(normalized_code)
    
    await update.message.reply_text(message)
    
    # Сбрасываем режим
    state.mode = None


async def restore_sb_names_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для восстановления имен СБ из журнала"""
    user_id = update.effective_user.id
    
    # Проверка авторизации
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("🔒 Требуется авторизация")
        return
    
    await update.message.reply_text("⏳ Восстанавливаю имена СБ из журнала...")
    
    restored_count, messages = db.restore_sb_names_from_log()
    
    if restored_count > 0:
        response = [f"✅ Восстановлено записей: {restored_count}\n"]
        response.extend(messages[:20])  # Показываем первые 20
        if len(messages) > 20:
            response.append(f"\n... и ещё {len(messages) - 20} записей")
        await update.message.reply_text('\n'.join(response))
    else:
        await update.message.reply_text("ℹ️ Записей для восстановления не найдено")


async def handle_journal_command(update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                 state: UserState, text: str):
    """Обработка команды журнал"""
    parts = text.split()
    
    limit = 20  # По умолчанию 20 записей
    code = None
    date = None
    
    # Парсим параметры
    # Формат: журнал [число] [код] [дата]
    # Примеры: журнал, журнал 50, журнал Д7, журнал 3,10, журнал Д7 3,10
    
    if len(parts) >= 2:
        # Проверяем второй параметр
        if parts[1].isdigit():
            limit = int(parts[1])
            if limit > 100:
                limit = 100  # Максимум 100
        else:
            # Это может быть код или дата
            from parser import DataParser
            from utils import parse_short_date
            
            # Пробуем как код
            if DataParser.is_code(parts[1]):
                code = DataParser.normalize_code(parts[1])
            else:
                # Пробуем как дату
                success, parsed_date, error = parse_short_date(parts[1])
                if success:
                    date = parsed_date
    
    if len(parts) >= 3:
        # Третий параметр
        from parser import DataParser
        from utils import parse_short_date
        
        if DataParser.is_code(parts[2]):
            code = DataParser.normalize_code(parts[2])
        else:
            success, parsed_date, error = parse_short_date(parts[2])
            if success:
                date = parsed_date
    
    # Получаем журнал
    logs = db.get_edit_log(limit=limit, code=code, date=date)
    
    if not logs:
        filter_info = []
        if code:
            filter_info.append(f"код: {code}")
        if date:
            filter_info.append(f"дата: {date}")
        
        filter_str = f" ({', '.join(filter_info)})" if filter_info else ""
        
        await update.message.reply_text(
            f"📜 Журнал изменений{filter_str}\n\n"
            f"Записей не найдено."
        )
        return
    
    # Форматируем журнал
    response_parts = []
    response_parts.append("📜 ЖУРНАЛ ИЗМЕНЕНИЙ\n")
    
    if code:
        response_parts.append(f"Фильтр: код {code}")
    if date:
        response_parts.append(f"Фильтр: дата {date}")
    
    response_parts.append(f"Показано: {len(logs)} из {limit}\n")
    
    for log in logs:
        # Форматируем дату и время
        edited_at = log['edited_at'][:16].replace('T', ' ')  # 2025-11-06T22:30:00 -> 2025-11-06 22:30
        
        action_type = log['action']
        
        # Определяем иконку по типу действия
        if 'merge' in action_type:
            icon = "🔄"
            action_text = log['action'].replace('merge_name: ', '')
        elif action_type == 'delete':
            icon = "🗑️"
            action_text = f"Удалено: {log['old_value']:.0f}"
        elif action_type == 'manual_update':
            icon = "✏️"
            action_text = f"Исправлено: {log['old_value']:.0f} → {log['new_value']:.0f}"
        elif action_type == 'update':
            icon = "➕"
            action_text = f"Добавлено: {log['old_value']:.0f} + ... = {log['new_value']:.0f}"
        elif action_type == 'replace':
            icon = "🔄"
            action_text = f"Заменено: {log['old_value']:.0f} → {log['new_value']:.0f}"
        else:
            icon = "📝"
            action_text = action_type
        
        response_parts.append(
            f"{icon} {edited_at}\n"
            f"   {log['club']} | {log['code']} | {log['channel'].upper()}\n"
            f"   {action_text}\n"
        )
    
    response_parts.append("─" * 35)
    response_parts.append(f"\n💡 Команды:")
    response_parts.append(f"• журнал 50 - показать 50 записей")
    response_parts.append(f"• журнал Д7 - по коду Д7")
    response_parts.append(f"• журнал 3,10 - за дату 03.10")
    
    await update.message.reply_text('\n'.join(response_parts))


def check_internal_duplicates(nal_data: list, beznal_data: list) -> list:
    """
    Проверка дубликатов внутри вводимых данных
    Возвращает список дубликатов (один код с именем и без имени)
    """
    from collections import defaultdict
    
    all_data = nal_data + beznal_data
    by_code = defaultdict(lambda: {'with_name': [], 'without_name': []})
    
    for item in all_data:
        code = item['code']
        if item['name']:
            by_code[code]['with_name'].append(item)
        else:
            by_code[code]['without_name'].append(item)
    
    # Ищем коды где есть И с именем И без имени
    duplicates = []
    for code, data in by_code.items():
        if data['with_name'] and data['without_name']:
            duplicates.append({
                'code': code,
                'with_name': data['with_name'],
                'without_name': data['without_name']
            })
    
    return duplicates


async def show_data_preview(update: Update, state: UserState, show_duplicates: bool = True):
    """Показать предпросмотр данных перед записью"""
    response_parts = []
    response_parts.append(f"📋 ПРЕДПРОСМОТР ДАННЫХ\n")
    response_parts.append(f"Клуб: {state.club}")
    
    if state.preview_date:
        response_parts.append(f"Дата: {state.preview_date}\n")
    
    # Показываем все данные с номерами строк
    line_num = 1
    total_nal = 0
    total_beznal = 0
    
    if state.temp_nal_data:
        response_parts.append("📗 НАЛ:")
        for item in state.temp_nal_data:
            response_parts.append(f"  {line_num}. {item['code']} {item['name']} — {item['amount']:.0f}")
            total_nal += item['amount']
            line_num += 1
        response_parts.append(f"  Итого НАЛ: {total_nal:.0f}\n")
    
    if state.temp_beznal_data:
        response_parts.append("📘 БЕЗНАЛ:")
        for item in state.temp_beznal_data:
            response_parts.append(f"  {line_num}. {item['code']} {item['name']} — {item['amount']:.0f}")
            total_beznal += item['amount']
            line_num += 1
        response_parts.append(f"  Итого БЕЗНАЛ: {total_beznal:.0f}\n")
    
    response_parts.append(f"💰 Всего: {total_nal + total_beznal:.0f}\n")
    
    # Проверка на дубликаты
    if show_duplicates:
        duplicates = check_internal_duplicates(state.temp_nal_data, state.temp_beznal_data)
        
        if duplicates:
            response_parts.append("⚠️ ВНИМАНИЕ! Найдены возможные дубликаты:\n")
            for i, dup in enumerate(duplicates, 1):
                response_parts.append(f"{i}. Код: {dup['code']}")
                
                # С именем
                names_with = set(item['name'] for item in dup['with_name'])
                for name in names_with:
                    items = [item for item in dup['with_name'] if item['name'] == name]
                    nal_sum = sum(item['amount'] for item in items if item in state.temp_nal_data)
                    bez_sum = sum(item['amount'] for item in items if item in state.temp_beznal_data)
                    response_parts.append(f"   • {name}: НАЛ {nal_sum:.0f}, БЕЗНАЛ {bez_sum:.0f}")
                
                # Без имени
                nal_no = sum(item['amount'] for item in dup['without_name'] if item in state.temp_nal_data)
                bez_no = sum(item['amount'] for item in dup['without_name'] if item in state.temp_beznal_data)
                response_parts.append(f"   • (без имени): НАЛ {nal_no:.0f}, БЕЗНАЛ {bez_no:.0f}")
                response_parts.append("")
            
            state.preview_duplicates = duplicates
    
    # Команды для пользователя
    response_parts.append("─" * 35)
    
    if not state.preview_date:
        response_parts.append("\n⏭️ СЛЕДУЮЩИЙ ШАГ:")
        response_parts.append("📅 Укажите дату в формате: 30,10 или 3,10")
        response_parts.append("\nПримеры:")
        response_parts.append("  • 3,10 → 03.10.2025")
        response_parts.append("  • 30,10 → 30.10.2025")
    else:
        response_parts.append("\n⏭️ ВЫБЕРИТЕ ДЕЙСТВИЕ:")
        response_parts.append("")
        response_parts.append("✅ ЗАПИСАТЬ")
        response_parts.append("   Сохранить данные в базу")
        response_parts.append("")
        response_parts.append("✏️ ИЗМЕНИТЬ")
        response_parts.append("   Редактировать строку по номеру")
        response_parts.append("")
        response_parts.append("❌ ОТМЕНА")
        response_parts.append("   Отменить весь ввод данных")
        
        if state.preview_duplicates:
            response_parts.append("")
            response_parts.append("─" * 35)
            response_parts.append("\n🔄 ОБЪЕДИНЕНИЕ ДУБЛИКАТОВ:")
            response_parts.append("")
            response_parts.append("• ОК → объединить все")
            response_parts.append("• ОК 1 → объединить только пункт 1")
            response_parts.append("• ОК 1 2 → объединить пункты 1 и 2")
            response_parts.append("• НЕ 1 → НЕ объединять пункт 1")
            response_parts.append("• НЕ 1 2 → НЕ объединять пункты 1 и 2")
    
    await update.message.reply_text('\n'.join(response_parts))


async def handle_preview_action(update: Update, state: UserState, text: str, text_lower: str):
    """Обработка действий в режиме предпросмотра"""
    
    # Проверяем команды объединения дубликатов
    if state.preview_duplicates and (text_lower.startswith('ок') or text_lower.startswith('не')):
        await handle_preview_duplicates(update, state, text_lower)
        return
    
    # ЗАПИСАТЬ - сохранение данных
    if text_lower == 'записать':
        await save_preview_data(update, state)
        return
    
    # ИЗМЕНИТЬ - редактирование строки
    if text_lower == 'изменить':
        total_lines = len(state.temp_nal_data) + len(state.temp_beznal_data)
        await update.message.reply_text(
            f"📝 Введите номер строки для редактирования:\n\n"
            f"📊 Доступно строк: 1-{total_lines}\n\n"
            f"Например: 1"
        )
        state.mode = 'awaiting_edit_line_number'
        return
    
    # Неизвестная команда
    await update.message.reply_text(
        "❓ Используйте команды:\n"
        "• ЗАПИСАТЬ\n"
        "• ИЗМЕНИТЬ\n"
        "• ОТМЕНА"
    )


async def handle_preview_duplicates(update: Update, state: UserState, text_lower: str):
    """Обработка объединения дубликатов в предпросмотре"""
    duplicates = state.preview_duplicates
    
    if not duplicates:
        await update.message.reply_text("❌ Дубликаты не найдены")
        return
    
    # Парсим команду
    normalized_text = text_lower.replace(',', ' ').replace('.', ' ')
    parts = normalized_text.split()
    
    if not parts:
        await update.message.reply_text("❌ Неверный формат")
        return
    
    command = parts[0]
    indices_to_merge = set()
    
    if command in ['ок', 'ok']:
        if len(parts) == 1:
            # Объединить все
            indices_to_merge = set(range(len(duplicates)))
        else:
            # Объединить указанные
            try:
                indices_to_merge = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            except:
                await update.message.reply_text("❌ Неверный формат номеров")
                return
    elif command in ['не', 'нет']:
        # Не объединять указанные
        try:
            exclude_indices = set(int(x) - 1 for x in parts[1:] if x.isdigit())
            indices_to_merge = set(range(len(duplicates))) - exclude_indices
        except:
            await update.message.reply_text("❌ Неверный формат номеров")
            return
    
    # Объединяем дубликаты
    for i, dup in enumerate(duplicates):
        if i in indices_to_merge:
            code = dup['code']
            # Берём имя из записи с именем
            if dup['with_name']:
                merged_name = dup['with_name'][0]['name']
                
                # Обновляем записи без имени
                for item in dup['without_name']:
                    item['name'] = merged_name
    
    # Убираем дубликаты из списка
    state.preview_duplicates = None
    
    await update.message.reply_text(
        "✅ Дубликаты объединены!\n\n"
        "📋 Обновлённый предпросмотр:"
    )
    
    # Показываем обновлённый предпросмотр
    await show_data_preview(update, state, show_duplicates=True)


async def handle_edit_line_number(update: Update, state: UserState, text: str):
    """Обработка ввода номера строки для редактирования"""
    try:
        line_num = int(text.strip())
        
        # Проверяем диапазон
        total_lines = len(state.temp_nal_data) + len(state.temp_beznal_data)
        
        if line_num < 1 or line_num > total_lines:
            await update.message.reply_text(
                f"❌ Неверный номер строки. Введите число от 1 до {total_lines}"
            )
            return
        
        # Находим строку
        if line_num <= len(state.temp_nal_data):
            item = state.temp_nal_data[line_num - 1]
            channel = 'нал'
            index = line_num - 1
        else:
            item = state.temp_beznal_data[line_num - len(state.temp_nal_data) - 1]
            channel = 'безнал'
            index = line_num - len(state.temp_nal_data) - 1
        
        # Показываем текущие данные
        await update.message.reply_text(
            f"✏️ Редактирование строки {line_num}\n\n"
            f"📌 Текущие данные:\n"
            f"   Код: {item['code']}\n"
            f"   Имя: {item['name']}\n"
            f"   Сумма: {item['amount']:.0f}\n"
            f"   Канал: {channel.upper()}\n\n"
            f"📝 Введите новые данные в формате:\n"
            f"   КОД ИМЯ СУММА\n\n"
            f"💡 Пример: Д7 Юля 10000"
        )
        
        state.edit_line_number = line_num
        state.mode = 'awaiting_edit_line_data'
        
    except ValueError:
        await update.message.reply_text("❌ Введите корректный номер строки")


async def handle_edit_line_data(update: Update, state: UserState, text: str):
    """Обработка ввода новых данных для строки"""
    from parser import DataParser
    
    # Парсим новую строку
    success, data, error = DataParser.parse_line(text, 1)
    
    if not success:
        await update.message.reply_text(f"❌ {error}\n\nПопробуйте ещё раз")
        return
    
    # Определяем в каком списке находится строка
    line_num = state.edit_line_number
    
    if line_num <= len(state.temp_nal_data):
        # Обновляем в НАЛ
        state.temp_nal_data[line_num - 1] = data
    else:
        # Обновляем в БЕЗНАЛ
        index = line_num - len(state.temp_nal_data) - 1
        state.temp_beznal_data[index] = data
    
    await update.message.reply_text(
        "✅ Строка успешно обновлена!\n\n"
        "📋 Обновлённый предпросмотр:"
    )
    
    # Очищаем редактирование
    state.edit_line_number = None
    state.mode = 'awaiting_preview_action'
    
    # Показываем обновлённый предпросмотр
    await show_data_preview(update, state, show_duplicates=True)


async def save_preview_data(update: Update, state: UserState):
    """Сохранение данных из предпросмотра в БД"""
    if not state.preview_date:
        await update.message.reply_text("❌ Дата не указана")
        return
    
    saved_count = 0
    
    for item in state.temp_nal_data:
        db.add_or_update_operation(
            club=state.club,
            date=state.preview_date,
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
            date=state.preview_date,
            code=item['code'],
            name=item['name'],
            channel='безнал',
            amount=item['amount'],
            original_line=item['original_line'],
            aggregate=True
        )
        saved_count += 1
    
    state.reset_input()
    
    await update.message.reply_text(
        f"✅ ДАННЫЕ УСПЕШНО СОХРАНЕНЫ!\n\n"
        f"🏢 Клуб: {state.club}\n"
        f"📅 Дата: {state.preview_date}\n"
        f"📊 Записей: {saved_count}\n\n"
        f"🎯 Что дальше?\n"
        f"• Введите новые данные: НАЛ / БЕЗНАЛ\n"
        f"• Посмотрите отчёт: ОТЧЁТ\n"
        f"• Или используйте другие команды ⬇️",
        reply_markup=get_main_keyboard()
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка загруженных документов (Excel файлы)"""
    user_id = update.effective_user.id
    state = get_user_state(user_id)
    
    # Проверка авторизации
    if user_id not in AUTHORIZED_USERS:
        await update.message.reply_text("🔒 Требуется авторизация")
        return
    
    # Проверяем что мы в режиме ожидания файла
    if state.mode != 'awaiting_upload_file':
        return
    
    document = update.message.document
    
    # Проверяем что это Excel файл
    if not (document.file_name.endswith('.xlsx') or document.file_name.endswith('.xls')):
        await update.message.reply_text(
            "❌ Поддерживаются только Excel файлы (.xlsx, .xls)\n"
            "Отправьте правильный файл или напишите: отмена"
        )
        return
    
    await update.message.reply_text("⏳ Обрабатываю файл...")
    
    try:
        # Скачиваем файл
        file = await context.bot.get_file(document.file_id)
        file_bytes = await file.download_as_bytearray()
        
        # Парсим Excel
        excel_processor = ExcelProcessor()
        notes_data = excel_processor.extract_notes_entries(bytes(file_bytes))
        
        if not notes_data or (not notes_data.get('безнал') and not notes_data.get('нал')):
            await update.message.reply_text(
                "❌ Не найден блок 'Примечания' в файле\n"
                "или он пустой.\n\n"
                "Проверьте файл и попробуйте снова\n"
                "или напишите: отмена"
            )
            return
        
        # Обрабатываем данные через DataParser
        beznal_entries = notes_data.get('безнал', [])
        nal_entries = notes_data.get('нал', [])
        
        parsed_beznal = []
        parsed_nal = []
        errors = []
        
        # Парсим безнал
        for idx, entry in enumerate(beznal_entries, 1):
            if entry.get('is_total'):
                continue  # Пропускаем строку ИТОГО
            
            entry_text = entry.get('entry_text', '').strip()
            if not entry_text:
                continue
            
            success, data, error = DataParser.parse_line(entry_text, idx)
            if success:
                parsed_beznal.append(data)
            elif error and 'Пустая строка' not in error:
                errors.append(f"БЕЗНАЛ строка {idx}: {error}")
        
        # Парсим нал
        for idx, entry in enumerate(nal_entries, 1):
            if entry.get('is_total'):
                continue  # Пропускаем строку ИТОГО
            
            entry_text = entry.get('entry_text', '').strip()
            if not entry_text:
                continue
            
            success, data, error = DataParser.parse_line(entry_text, idx)
            if success:
                parsed_nal.append(data)
            elif error and 'Пустая строка' not in error:
                errors.append(f"НАЛ строка {idx}: {error}")
        
        if not parsed_beznal and not parsed_nal:
            await update.message.reply_text(
                "❌ Не удалось извлечь данные из файла\n\n"
                "Ошибки:\n" + "\n".join(errors[:5]) if errors else "Нет валидных строк"
            )
            state.mode = None
            return
        
        # Проверяем доплаты (строки начинающиеся с %)
        # ВАЖНО: Анализируем НАЛ и БЕЗНАЛ ОТДЕЛЬНО!
        beznal_analysis = DataParser.find_additional_payments(parsed_beznal)
        nal_analysis = DataParser.find_additional_payments(parsed_nal)
        
        # Сохраняем данные в состояние
        state.upload_file_data = {
            'beznal': parsed_beznal,
            'nal': parsed_nal,
            'errors': errors,
            'beznal_analysis': beznal_analysis,
            'nal_analysis': nal_analysis
        }
        
        # Показываем предпросмотр
        await show_file_preview(update, state)
        state.mode = 'awaiting_upload_confirm'
        
    except Exception as e:
        await update.message.reply_text(
            f"❌ Ошибка обработки файла: {str(e)}\n\n"
            f"Попробуйте снова или напишите: отмена"
        )
        state.mode = None


async def show_file_preview(update: Update, state: UserState):
    """Показать предпросмотр данных из файла"""
    data = state.upload_file_data
    beznal_list = data.get('beznal', [])
    nal_list = data.get('nal', [])
    errors = data.get('errors', [])
    beznal_analysis = data.get('beznal_analysis', {})
    nal_analysis = data.get('nal_analysis', {})
    
    # Заголовок
    header = []
    header.append("📎 ПРЕДПРОСМОТР ДАННЫХ ИЗ ФАЙЛА")
    header.append("")
    header.append(f"🏢 Клуб: {state.upload_file_club}")
    header.append(f"📅 Дата: {state.upload_file_date}")
    header.append("")
    
    # БЕЗНАЛ - формируем текст
    beznal_text = []
    if beznal_list:
        beznal_text.append(f"📘 БЕЗНАЛ ({len(beznal_list)} записей):")
        total_beznal = 0
        for idx, item in enumerate(beznal_list, 1):
            beznal_text.append(f"  {idx}. {item['code']} {item['name']} — {item['amount']:.0f}")
            total_beznal += item['amount']
        beznal_text.append(f"  💰 Итого безнал: {total_beznal:.0f}")
        beznal_text.append("")
    
    # НАЛ - формируем текст
    nal_text = []
    if nal_list:
        nal_text.append(f"📗 НАЛ ({len(nal_list)} записей):")
        total_nal = 0
        for idx, item in enumerate(nal_list, 1):
            nal_text.append(f"  {idx}. {item['code']} {item['name']} — {item['amount']:.0f}")
            total_nal += item['amount']
        nal_text.append(f"  💰 Итого нал: {total_nal:.0f}")
        nal_text.append("")
    
    # Ошибки
    errors_text = []
    if errors:
        errors_text.append(f"⚠️ Ошибок при парсинге: {len(errors)}")
        for error in errors[:5]:
            errors_text.append(f"  • {error}")
        if len(errors) > 5:
            errors_text.append(f"  ... и ещё {len(errors) - 5} ошибок")
        errors_text.append("")
    
    # Доплаты (строки с %) - ОТДЕЛЬНО ДЛЯ БЕЗНАЛ И НАЛ
    additional_text = []
    merge_counter = 0  # Сквозная нумерация для всех объединений
    
    # БЕЗНАЛ
    if beznal_analysis:
        beznal_merges = beznal_analysis.get('merges', [])
        beznal_not_found = beznal_analysis.get('not_found', [])
        beznal_no_code = beznal_analysis.get('no_code', [])
        
        if beznal_merges:
            additional_text.append("🔀 ДОПЛАТЫ БЕЗНАЛ:")
            additional_text.append("")
            for merge in beznal_merges:
                merge_counter += 1
                merge['merge_id'] = merge_counter  # Присваиваем ID
                code = merge['code']
                main_items = merge['main_items']
                add_item = merge['additional_item']
                total = merge['total_amount']
                
                additional_text.append(f"[{merge_counter}] Код: {code}")
                for main in main_items:
                    additional_text.append(f"     Основная: {main['name']} — {main['amount']:.0f}")
                additional_text.append(f"     Доплата: {add_item['original_line']} — {add_item['amount']:.0f}")
                additional_text.append(f"     ИТОГО: {total:.0f}")
                additional_text.append("")
        
        if beznal_not_found:
            additional_text.append("⚠️ БЕЗНАЛ - Доплаты без основной записи:")
            for item in beznal_not_found:
                additional_text.append(f"  • {item['original_line']} (код {item['code']} не найден)")
            additional_text.append("")
            
        if beznal_no_code:
            additional_text.append("❓ БЕЗНАЛ - Доплаты без кода:")
            for item in beznal_no_code:
                additional_text.append(f"  • {item['original_line']}")
            additional_text.append("")
    
    # НАЛ
    if nal_analysis:
        nal_merges = nal_analysis.get('merges', [])
        nal_not_found = nal_analysis.get('not_found', [])
        nal_no_code = nal_analysis.get('no_code', [])
        
        if nal_merges:
            additional_text.append("🔀 ДОПЛАТЫ НАЛ:")
            additional_text.append("")
            for merge in nal_merges:
                merge_counter += 1
                merge['merge_id'] = merge_counter  # Присваиваем ID
                code = merge['code']
                main_items = merge['main_items']
                add_item = merge['additional_item']
                total = merge['total_amount']
                
                additional_text.append(f"[{merge_counter}] Код: {code}")
                for main in main_items:
                    additional_text.append(f"     Основная: {main['name']} — {main['amount']:.0f}")
                additional_text.append(f"     Доплата: {add_item['original_line']} — {add_item['amount']:.0f}")
                additional_text.append(f"     ИТОГО: {total:.0f}")
                additional_text.append("")
        
        if nal_not_found:
            additional_text.append("⚠️ НАЛ - Доплаты без основной записи:")
            for item in nal_not_found:
                additional_text.append(f"  • {item['original_line']} (код {item['code']} не найден)")
            additional_text.append("")
            
        if nal_no_code:
            additional_text.append("❓ НАЛ - Доплаты без кода:")
            for item in nal_no_code:
                additional_text.append(f"  • {item['original_line']}")
            additional_text.append("")
    
    # Финал
    footer = []
    if additional_text:
        footer.append("⚠️ ВНИМАНИЕ! Обнаружены доплаты.")
        footer.append("Проверьте объединения выше.")
        footer.append("")
        footer.append("✅ Введите команду:")
        footer.append("  • ЗАПИСАТЬ - применить ВСЕ объединения")
        footer.append("  • ЗАПИСАТЬ 1 2 - применить только [1] и [2]")
        footer.append("  • ЗАПИСАТЬ БЕЗ 3 - применить все кроме [3]")
        footer.append("  • ОТМЕНА - отменить загрузку")
    else:
        footer.append("✅ Всё верно? Введите:")
        footer.append("  • ЗАПИСАТЬ - сохранить в базу")
        footer.append("  • ОТМЕНА - отменить")
    
    # Объединяем весь текст
    full_text = '\n'.join(header + beznal_text + nal_text + errors_text + additional_text + footer)
    
    # Разбиваем на части по 4000 символов если нужно
    max_length = 4000
    if len(full_text) <= max_length:
        await update.message.reply_text(full_text)
    else:
        # Разбиваем на куски
        parts = []
        current_part = []
        
        for line in (header + beznal_text + nal_text + errors_text + additional_text + footer):
            test_part = '\n'.join(current_part + [line])
            if len(test_part) > max_length and current_part:
                # Сохраняем текущую часть и начинаем новую
                parts.append('\n'.join(current_part))
                current_part = [line]
            else:
                current_part.append(line)
        
        # Добавляем последнюю часть
        if current_part:
            parts.append('\n'.join(current_part))
        
        # Отправляем все части
        for part in parts:
            await update.message.reply_text(part)


async def save_file_data(update: Update, state: UserState):
    """Сохранение данных из файла в БД с учетом объединений доплат"""
    data = state.upload_file_data
    beznal_list = data.get('beznal', [])
    nal_list = data.get('nal', [])
    beznal_analysis = data.get('beznal_analysis', {})
    nal_analysis = data.get('nal_analysis', {})
    selected_merges = data.get('selected_merges')  # None = все, [1,2] = только указанные
    
    # Сохраняем значения до очистки
    club = state.upload_file_club
    date = state.upload_file_date
    
    # Создаем словари для объединений ОТДЕЛЬНО ДЛЯ БЕЗНАЛ И НАЛ
    beznal_merge_dict = {}
    nal_merge_dict = {}
    
    # БЕЗНАЛ - собираем объединения которые нужно применить
    beznal_merges = beznal_analysis.get('merges', [])
    for merge in beznal_merges:
        merge_id = merge.get('merge_id')
        # Проверяем, нужно ли применять это объединение
        should_apply = False
        if selected_merges is None:
            should_apply = True  # Применяем все
        elif merge_id in selected_merges:
            should_apply = True  # Применяем только выбранные
        
        if should_apply:
            code = merge['code']
            beznal_merge_dict[code] = {
                'amount': merge['total_amount'],
                'name': merge['main_items'][0]['name'] if merge['main_items'] else ''
            }
    
    # НАЛ - собираем объединения которые нужно применить
    nal_merges = nal_analysis.get('merges', [])
    for merge in nal_merges:
        merge_id = merge.get('merge_id')
        # Проверяем, нужно ли применять это объединение
        should_apply = False
        if selected_merges is None:
            should_apply = True  # Применяем все
        elif merge_id in selected_merges:
            should_apply = True  # Применяем только выбранные
        
        if should_apply:
            code = merge['code']
            nal_merge_dict[code] = {
                'amount': merge['total_amount'],
                'name': merge['main_items'][0]['name'] if merge['main_items'] else ''
            }
    
    saved_count = 0
    
    # Сохраняем безнал
    for item in beznal_list:
        # Пропускаем доплаты (is_additional=True) - они уже учтены
        if item.get('is_additional', False):
            continue
            
        code = item['code']
        # Если код объединяется - используем итоговую сумму
        if code in beznal_merge_dict:
            amount = beznal_merge_dict[code]['amount']
            name = beznal_merge_dict[code]['name']
        else:
            amount = item['amount']
            name = item['name']
            
        db.add_or_update_operation(
            club=club,
            date=date,
            code=code,
            name=name,
            channel='безнал',
            amount=amount,
            original_line=item['original_line'],
            aggregate=True
        )
        saved_count += 1
    
    # Сохраняем нал
    for item in nal_list:
        # Пропускаем доплаты (is_additional=True) - они уже учтены
        if item.get('is_additional', False):
            continue
            
        code = item['code']
        # Если код объединяется - используем итоговую сумму
        if code in nal_merge_dict:
            amount = nal_merge_dict[code]['amount']
            name = nal_merge_dict[code]['name']
        else:
            amount = item['amount']
            name = item['name']
            
        db.add_or_update_operation(
            club=club,
            date=date,
            code=code,
            name=name,
            channel='нал',
            amount=amount,
            original_line=item['original_line'],
            aggregate=True
        )
        saved_count += 1
    
    # Очищаем состояние
    state.upload_file_club = None
    state.upload_file_date = None
    state.upload_file_data = None
    state.mode = None
    
    await update.message.reply_text(
        f"✅ ДАННЫЕ ИЗ ФАЙЛА СОХРАНЕНЫ!\n\n"
        f"🏢 Клуб: {club}\n"
        f"📅 Дата: {date}\n"
        f"📊 Записей: {saved_count}\n\n"
        f"Используйте кнопки меню ⬇️",
        reply_markup=get_main_keyboard()
    )


def main():
    """Запуск бота"""
    # Проверяем токен
    if config.BOT_TOKEN == 'YOUR_BOT_TOKEN_HERE':
        print("❌ Ошибка: не установлен токен бота!")
        print("Установите переменную окружения TELEGRAM_BOT_TOKEN")
        print("или измените значение в config.py")
        return
    
    # Инициализация списка самозанятых (только при первом запуске)
    initial_self_employed = [
        'Д4', 'Д5', 'Д11', 'Д15', 'Д18', 'Д20', 'Д23', 'Д33', 'Д35', 'Д38',
        'Д66', 'ОФ1', 'ОФ3', 'ОФ4', 'Б13', 'Б52', 'К2', 'К4', 'К21'
    ]
    added = db.init_self_employed_list(initial_self_employed)
    if added > 0:
        print(f"[OK] Инициализирован список самозанятых: {added} кодов")
    
    # Создаем приложение
    app = Application.builder().token(config.BOT_TOKEN).build()
    
    # Регистрируем обработчики
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("restore_sb", restore_sb_names_command))
    app.add_handler(CallbackQueryHandler(handle_callback_query))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Запускаем бота
    print("[BOT] Бот запущен!")
    print("Для остановки нажмите Ctrl+C")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    main()

