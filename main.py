"""Точка входа приложения «Обработчик регистров 1С 6-в-1».

Вся логика — в пакетах ``ui`` (интерфейс) и ``flatregister`` (ядро);
здесь только создание и запуск приложения (требование сборки Nuitka —
``main.py`` остаётся в корне).
"""
from ui.app import FlatRegister1CApp

if __name__ == "__main__":
    app = FlatRegister1CApp()
    app.run()
