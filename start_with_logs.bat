@echo off
chcp 65001 >nul
title Telegram Bot - Moskvich Money Bot (with logs)
color 0A

echo ========================================
echo     TELEGRAM BOT ZAPUSK (S LOGAMI)
echo ========================================
echo.
echo Bot zapuskaetsya...
echo Logi sohranyayutsya v bot_logs.txt
echo.

REM Запускаем бота с перенаправлением вывода в файл
python bot.py > bot_logs.txt 2>&1

echo.
echo ========================================
echo Bot ostanovlen
echo Logi sohraneny v bot_logs.txt
echo ========================================
pause

