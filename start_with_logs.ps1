# Запуск бота с сохранением логов в файл
Write-Host "Запуск Telegram бота с логированием..." -ForegroundColor Green
Write-Host "Логи сохраняются в bot_logs.txt" -ForegroundColor Yellow
Write-Host "Для остановки нажмите Ctrl+C" -ForegroundColor Yellow
Write-Host ""

# Запускаем бота с перенаправлением вывода
python bot.py *> bot_logs.txt

Write-Host ""
Write-Host "Бот остановлен. Логи сохранены в bot_logs.txt" -ForegroundColor Green

