"""Модуль авторизации пользователей"""
import json
import os

AUTH_FILE = 'authorized_users.json'

# Кэш авторизованных пользователей в памяти
_authorized_cache = None


def _ensure_cache():
    """Инициализация кэша"""
    global _authorized_cache
    if _authorized_cache is None:
        _authorized_cache = load_authorized_users()
    return _authorized_cache


def load_authorized_users():
    """Загрузка списка авторизованных пользователей"""
    if os.path.exists(AUTH_FILE):
        try:
            with open(AUTH_FILE, 'r') as f:
                data = json.load(f)
                return set(data)
        except Exception as e:
            print(f"Ошибка загрузки авторизации: {e}")
            return set()
    return set()


def save_authorized_users(users_set):
    """Сохранение списка авторизованных пользователей"""
    try:
        with open(AUTH_FILE, 'w') as f:
            json.dump(list(users_set), f, indent=2)
        print(f"Авторизация сохранена: {list(users_set)}")
    except Exception as e:
        print(f"Ошибка сохранения авторизации: {e}")


def add_user(user_id):
    """Добавить пользователя в авторизованные"""
    global _authorized_cache
    cache = _ensure_cache()
    cache.add(user_id)
    save_authorized_users(cache)
    _authorized_cache = cache
    return cache


def remove_user(user_id):
    """Удалить пользователя из авторизованных"""
    global _authorized_cache
    cache = _ensure_cache()
    cache.discard(user_id)
    save_authorized_users(cache)
    _authorized_cache = cache
    return cache


def is_authorized(user_id):
    """Проверка авторизации пользователя"""
    cache = _ensure_cache()
    return user_id in cache


def get_all_authorized():
    """Получить всех авторизованных пользователей"""
    return _ensure_cache()

