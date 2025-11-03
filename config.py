# Конфигурационный файл для бота
import os

# Токен бота (установите свой токен через переменную окружения)
BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '8529542965:AAG2hgRXjWSCBbWVGx57fknqSfZuTumE2bs')

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

