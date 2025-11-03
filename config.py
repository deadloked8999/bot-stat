# Конфигурационный файл для бота
import os

# Токен бота (установите свой токен через переменную окружения)
# Или замените 'YOUR_BOT_TOKEN_HERE' на ваш токен
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', 'YOUR_BOT_TOKEN_HERE')

# Попытка загрузить токен из локального файла (если есть)
try:
    from config_local import TELEGRAM_BOT_TOKEN
    BOT_TOKEN = TELEGRAM_BOT_TOKEN
except ImportError:
    pass

# Временная зона
TIMEZONE = 'Europe/Riga'

# Клубы
CLUBS = {
    'москвич': 'Москвич',
    'анора': 'Анора'
}

# Каналы
CHANNELS = {
    'нал': 'нал',
    'безнал': 'безнал'
}

# База данных
DATABASE_PATH = 'bot_data.db'

