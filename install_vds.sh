#!/bin/bash
# Автоматическая установка Telegram бота на VDS (Linux)

echo "========================================"
echo "   УСТАНОВКА TELEGRAM БОТА НА VDS"
echo "========================================"
echo ""

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Проверка root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}[ERROR]${NC} Запустите с правами root: sudo bash install_vds.sh"
    exit 1
fi

echo -e "${GREEN}[1/8]${NC} Обновление системы..."
apt update && apt upgrade -y

echo -e "${GREEN}[2/8]${NC} Установка Python и Git..."
apt install python3 python3-pip git nano -y

echo -e "${GREEN}[3/8]${NC} Проверка версии Python..."
python3 --version

echo -e "${GREEN}[4/8]${NC} Клонирование проекта с GitHub..."
cd /opt
if [ -d "bot-stat" ]; then
    echo "Папка bot-stat уже существует. Обновление..."
    cd bot-stat
    git pull origin main
else
    git clone https://github.com/deadloked8999/bot-stat.git
    cd bot-stat
fi

echo -e "${GREEN}[5/8]${NC} Установка зависимостей Python..."
pip3 install -r requirements.txt

echo -e "${GREEN}[6/8]${NC} Настройка токена бота..."
echo ""
echo -e "${YELLOW}ВВЕДИТЕ ТОКЕН БОТА:${NC}"
read -p "Токен: " BOT_TOKEN

cat > config_local.py << EOF
# Локальная конфигурация VDS
TELEGRAM_BOT_TOKEN = '$BOT_TOKEN'
EOF

echo -e "${GREEN}[7/8]${NC} Создание systemd службы..."
cat > /etc/systemd/system/telegram-bot.service << EOF
[Unit]
Description=Telegram Statistics Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/bot-stat
ExecStart=/usr/bin/python3 /opt/bot-stat/bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo -e "${GREEN}[8/8]${NC} Запуск бота..."
systemctl daemon-reload
systemctl enable telegram-bot
systemctl start telegram-bot

sleep 2

echo ""
echo "========================================"
echo -e "${GREEN}   УСТАНОВКА ЗАВЕРШЕНА!${NC}"
echo "========================================"
echo ""

# Проверка статуса
if systemctl is-active --quiet telegram-bot; then
    echo -e "${GREEN}✓${NC} Бот запущен и работает!"
else
    echo -e "${RED}✗${NC} Ошибка запуска. Проверьте логи:"
    echo "   journalctl -u telegram-bot -n 50"
fi

echo ""
echo "УПРАВЛЕНИЕ БОТОМ:"
echo "  Статус:       systemctl status telegram-bot"
echo "  Остановить:   systemctl stop telegram-bot"
echo "  Запустить:    systemctl start telegram-bot"
echo "  Перезапуск:   systemctl restart telegram-bot"
echo "  Логи:         journalctl -u telegram-bot -f"
echo ""
echo "ФАЙЛЫ:"
echo "  Проект:       /opt/bot-stat/"
echo "  База данных:  /opt/bot-stat/bot_data.db"
echo "  Конфигурация: /opt/bot-stat/config_local.py"
echo ""

