"""Абсолютные пути проекта.

Все модули должны строить пути через эти константы, чтобы запуск работал
из любой рабочей директории (см. пункт "Почти везде используются относительные пути").
"""
import os

# Корень проекта = родительский каталог для пакета core/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", "config.yaml")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")
MODULES_DIR = os.path.join(PROJECT_ROOT, "modules")
