# Просмотр логов бота
Write-Host "Просмотр логов бота" -ForegroundColor Green
Write-Host ""

if (-not (Test-Path "bot_logs.txt")) {
    Write-Host "Файл bot_logs.txt не найден!" -ForegroundColor Red
    Write-Host "Запустите бота через start_with_logs.ps1 для создания логов" -ForegroundColor Yellow
    Read-Host "Нажмите Enter для выхода"
    exit
}

Write-Host "Последние 50 строк логов:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Показываем последние 50 строк
Get-Content bot_logs.txt -Tail 50

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Для просмотра всех логов используйте:" -ForegroundColor Yellow
Write-Host "  Get-Content bot_logs.txt | more" -ForegroundColor White
Write-Host "  или откройте bot_logs.txt в блокноте" -ForegroundColor White
Write-Host ""

Read-Host "Нажмите Enter для выхода"

