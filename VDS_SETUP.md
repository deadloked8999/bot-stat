# 🌐 Развертывание бота на VDS

## 📋 ЧТО НУЖНО ОТ VDS:

- **ОС:** Linux (Ubuntu 20.04/22.04 рекомендуется) или Windows Server
- **RAM:** Минимум 512 MB (рекомендуется 1 GB)
- **Диск:** 1 GB свободного места
- **Python:** 3.8 или выше

---

## 🚀 ПОШАГОВАЯ ИНСТРУКЦИЯ (Linux)

### ШАГ 1: Подключитесь к VDS

**Через SSH (если у вас Linux VDS):**

```bash
ssh root@ВАШ_IP_АДРЕС
```

Введите пароль от VDS.

---

### ШАГ 2: Установите необходимое ПО

```bash
# Обновление системы
apt update && apt upgrade -y

# Установка Python и pip
apt install python3 python3-pip git -y

# Проверка версии Python
python3 --version
```

Должно быть: Python 3.8+

---

### ШАГ 3: Клонируйте проект с GitHub

```bash
# Переход в домашнюю директорию
cd ~

# Клонирование проекта
git clone https://github.com/deadloked8999/bot-stat.git

# Переход в папку проекта
cd bot-stat
```

---

### ШАГ 4: Установите зависимости

```bash
pip3 install -r requirements.txt
```

---

### ШАГ 5: Настройте токен бота

**Создайте файл config_local.py:**

```bash
nano config_local.py
```

Вставьте:
```python
TELEGRAM_BOT_TOKEN = '8529542965:AAG2hgRXjWSCBbWVGx57fknqSfZuTumE2bs'
```

Сохраните: `Ctrl+O`, `Enter`, `Ctrl+X`

---

### ШАГ 6: Проверьте запуск

```bash
python3 bot.py
```

Должно появиться:
```
🤖 Бот запущен!
Для остановки нажмите Ctrl+C
```

Проверьте в Telegram - бот должен отвечать!

Остановите: `Ctrl+C`

---

### ШАГ 7: Запустите бота как службу (постоянно)

**Создайте systemd сервис:**

```bash
nano /etc/systemd/system/telegram-bot.service
```

Вставьте:
```ini
[Unit]
Description=Telegram Statistics Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/bot-stat
ExecStart=/usr/bin/python3 /root/bot-stat/bot.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Сохраните: `Ctrl+O`, `Enter`, `Ctrl+X`

---

### ШАГ 8: Запустите службу

```bash
# Перезагрузка конфигурации
systemctl daemon-reload

# Запуск бота
systemctl start telegram-bot

# Включение автозапуска при перезагрузке
systemctl enable telegram-bot

# Проверка статуса
systemctl status telegram-bot
```

Должно быть: **Active: active (running)**

---

### ШАГ 9: Управление ботом

**Просмотр логов:**
```bash
journalctl -u telegram-bot -f
```

**Остановить бота:**
```bash
systemctl stop telegram-bot
```

**Перезапустить бота:**
```bash
systemctl restart telegram-bot
```

**Статус:**
```bash
systemctl status telegram-bot
```

---

## 🔄 ОБНОВЛЕНИЕ БОТА НА VDS

Когда вы внесли изменения локально и запушили на GitHub:

```bash
# Подключитесь к VDS
ssh root@ВАШ_IP

# Перейдите в папку проекта
cd ~/bot-stat

# Остановите бота
systemctl stop telegram-bot

# Получите обновления
git pull origin main

# Установите новые зависимости (если есть)
pip3 install -r requirements.txt

# Запустите бота
systemctl start telegram-bot

# Проверьте статус
systemctl status telegram-bot
```

---

## 🪟 ЕСЛИ У ВАС WINDOWS SERVER

### Вариант 1: Автозапуск через Task Scheduler

1. Откройте **Task Scheduler**
2. Create Basic Task
3. Name: `Telegram Bot`
4. Trigger: **When the computer starts**
5. Action: **Start a program**
6. Program: `C:\Python313\python.exe`
7. Arguments: `C:\путь\до\bot-stat\bot.py`
8. Finish

### Вариант 2: NSSM (рекомендуется)

```cmd
# Скачайте NSSM
# https://nssm.cc/download

# Установите службу
nssm install TelegramBot "C:\Python313\python.exe" "C:\путь\до\bot-stat\bot.py"

# Запустите
nssm start TelegramBot
```

---

## 📊 МОНИТОРИНГ

### Проверка работы бота:

```bash
# Логи (Linux)
journalctl -u telegram-bot -n 50

# Размер базы данных
ls -lh ~/bot-stat/bot_data.db

# Процессы Python
ps aux | grep python
```

---

## 🆘 РЕШЕНИЕ ПРОБЛЕМ

### Бот не запускается:

```bash
# Проверьте логи
journalctl -u telegram-bot -n 100

# Проверьте токен
cat ~/bot-stat/config_local.py

# Проверьте зависимости
pip3 list | grep telegram
```

### Бот не отвечает:

1. Проверьте интернет на VDS: `ping 8.8.8.8`
2. Проверьте статус: `systemctl status telegram-bot`
3. Перезапустите: `systemctl restart telegram-bot`

### NetworkError:

- Проверьте файрвол: `ufw status`
- Разрешите исходящие соединения
- Проверьте DNS: `cat /etc/resolv.conf`

---

## ✅ ПРЕИМУЩЕСТВА VDS

- ✅ Бот работает 24/7
- ✅ Не зависит от вашего компьютера
- ✅ Автозапуск при перезагрузке
- ✅ Логи и мониторинг

---

## 📝 КРАТКАЯ ШПАРГАЛКА

```bash
# Подключение к VDS
ssh root@IP

# Переход в проект
cd ~/bot-stat

# Обновление с GitHub
git pull origin main

# Перезапуск бота
systemctl restart telegram-bot

# Просмотр логов
journalctl -u telegram-bot -f

# Статус
systemctl status telegram-bot
```

---

## 🔐 БЕЗОПАСНОСТЬ

1. Создайте отдельного пользователя (не root):
```bash
adduser botuser
su - botuser
```

2. Настройте файрвол:
```bash
ufw allow ssh
ufw enable
```

3. Регулярно делайте бэкап БД:
```bash
cp ~/bot-stat/bot_data.db ~/backup/bot_data_$(date +%Y%m%d).db
```

---

**Какая ОС у вас на VDS: Linux или Windows?**

Я подготовлю точную инструкцию! 🚀

