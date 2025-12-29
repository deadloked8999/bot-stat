#!/bin/bash

echo "🚀 Deploying bot..."

# Добавляем все изменения
git add .

# Коммит с сообщением (или используем параметр)
if [ -z "$1" ]; then
    git commit -m "Auto deploy: $(date '+%Y-%m-%d %H:%M:%S')"
else
    git commit -m "$1"
fi

# Пушим на GitHub
git push origin main

# Обновляем на сервере
echo "📡 Updating server..."
ssh root@185.245.34.167 "cd ~/bot-stat && git pull origin main && systemctl restart telegram-bot"

echo "✅ Deploy complete!"

