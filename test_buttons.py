"""Тест работы кнопок"""
import asyncio
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

# Тестовый токен
try:
    from config_local import TELEGRAM_BOT_TOKEN
    TOKEN = TELEGRAM_BOT_TOKEN
except:
    import config
    TOKEN = config.BOT_TOKEN

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тест команды /start"""
    # Inline кнопки
    keyboard = [
        [InlineKeyboardButton("Кнопка 1", callback_data='btn1')],
        [InlineKeyboardButton("Кнопка 2", callback_data='btn2')]
    ]
    await update.message.reply_text(
        "Выберите кнопку:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия на кнопку"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text(f"Вы нажали: {query.data}")
    
    # Постоянная клавиатура
    keyboard = [
        ['Кнопка А', 'Кнопка Б'],
        ['Кнопка В']
    ]
    await query.message.reply_text(
        "Теперь постоянные кнопки внизу:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текста"""
    await update.message.reply_text(f"Вы написали: {update.message.text}")

def main():
    print("Запуск теста кнопок...")
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT, handle_text))
    
    print("Тест запущен! Отправьте /start боту")
    app.run_polling()

if __name__ == '__main__':
    main()

