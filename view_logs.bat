@echo off
chcp 65001 >nul
title Просмотр логов бота
color 0B

echo ========================================
echo     ПРОСМОТР ЛОГОВ БОТА
echo ========================================
echo.

if not exist bot_logs.txt (
    echo Файл bot_logs.txt не найден!
    echo Запустите бота через start_with_logs.bat для создания логов
    pause
    exit
)

echo Показываю последние 50 строк логов:
echo ========================================
echo.

REM Показываем последние 50 строк
powershell -Command "Get-Content bot_logs.txt -Tail 50"

echo.
echo ========================================
echo.
echo Для просмотра всех логов откройте bot_logs.txt в блокноте
echo Или используйте: type bot_logs.txt ^| more
echo.
pause

