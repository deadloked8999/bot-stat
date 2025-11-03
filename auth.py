"""Модуль авторизации пользователей"""
import json
import os

AUTH_FILE = 'authorized_users.json'


def load_authorized_users():
    """Загрузка списка авторизованных пользователей"""
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE, 'r') as f:
                data = json.load(f)
                return set(data)
        except:
            return set()
    return set()


def save_authorized_users(users_set):
    """Сохранение списка авторизованных пользователей"""
    with open(AUTH_FILE, 'w') as f:
        json.dump(list(users_set), f)


def add_user(user_id):
    """Добавить пользователя в авторизованные"""
    users = load_authorized_users()
    users.add(user_id)
    save_authorized_users(users)
    return users


def remove_user(user_id):
    """Удалить пользователя из авторизованных"""
    users = load_authorized_users()
    users.discard(user_id)
    save_authorized_users(users)
    return users


def is_authorized(user_id):
    """Проверка авторизации пользователя"""
    users = load_authorized_users()
    return user_id in users

