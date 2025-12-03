import sqlite3
import time
from flask import Flask, request, jsonify
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Создание Flask приложения
app = Flask(__name__)

# База данных (SQLite)
DB_NAME = 'numbers.db'


# Функции для работы с базой данных
def init_db():
    """Инициализация базы данных и создание таблицы, если её нет"""
    try:
        conn = sqlite3.connect(DB_NAME,
                               check_same_thread=False)  # Разрешаем использование одного соединения в нескольких потоках
        c = conn.cursor()

        # Включаем режим WAL для уменьшения блокировок
        c.execute('PRAGMA journal_mode=WAL;')
        c.execute('CREATE TABLE IF NOT EXISTS numbers (number INTEGER PRIMARY KEY)')
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Ошибка при инициализации базы данных: {e}")


def get_max_number():
    """Получение максимального обработанного числа"""
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT MAX(number) FROM numbers')
        result = c.fetchone()
        conn.close()
        return result[0] if result[0] is not None else -1
    except Exception as e:
        logger.error(f"Ошибка при получении максимального числа: {e}")
        return -1


def check_number_exists(number):
    """Проверка, было ли уже обработано данное число"""
    try:
        conn = sqlite3.connect(DB_NAME, check_same_thread=False)
        c = conn.cursor()
        c.execute('SELECT COUNT(*) FROM numbers WHERE number = ?', (number,))
        result = c.fetchone()
        conn.close()
        return result[0] > 0
    except Exception as e:
        logger.error(f"Ошибка при проверке числа: {e}")
        return False


def add_number(number):
    """Добавление числа в базу данных с повторной попыткой в случае блокировки"""
    retries = 3  # Уменьшаем количество попыток
    for attempt in range(retries):
        try:
            conn = sqlite3.connect(DB_NAME, check_same_thread=False)
            c = conn.cursor()

            # Включаем режим WAL для уменьшения блокировок
            c.execute('PRAGMA journal_mode=WAL;')

            c.execute('INSERT INTO numbers (number) VALUES (?)', (number,))
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError as e:
            # Обработка ошибки вставки дублирующего числа (уже существует)
            logger.warning(f"Ошибка уникальности: число {number} уже существует.")
            return False
        except sqlite3.OperationalError as e:
            # В случае блокировки, ждем и пробуем снова
            if 'database is locked' in str(e):
                logger.warning(f"Ошибка блокировки базы данных, повторная попытка {attempt + 1}/{retries}")
                time.sleep(1)  # Увеличиваем задержку между попытками
            else:
                logger.error(f"Ошибка при добавлении числа в базу данных: {e}")
                return False
    return False


# Логика обработки числа
def process_number(number):
    """Обработка числа"""
    max_number = get_max_number()

    # Проверка 1: если число уже обработано
    if check_number_exists(number):
        logger.warning(f"Число {number} уже обработано.")
        return f"Ошибка: число {number} уже обработано.", False

    # Проверка 2: если число на 1 меньше максимального обработанного числа
    if number == max_number + 1:
        logger.warning(f"Число {number} на 1 меньше уже обработанного числа {max_number}.")
        return f"Ошибка: число {number} на 1 меньше уже обработанного числа {max_number}.", False

    # Добавление числа в базу данных
    if add_number(number + 1):
        logger.info(f"Число {number + 1} добавлено в базу данных.")
        return f"Число обработано: {number + 1}", True
    else:
        logger.error(f"Не удалось добавить число {number + 1} в базу данных.")
        return f"Ошибка при добавлении числа {number + 1}. Попробуйте позже.", False


# API маршрут для обработки POST запроса
@app.route('/process_number', methods=['POST'])
def handle_request():
    """Обработка POST запроса"""
    try:
        data = request.get_json()
        if 'number' not in data:
            return jsonify({"error": "Поле 'number' не найдено в запросе."}), 400

        number = data['number']

        # Проверка на правильность числа
        if not isinstance(number, int) or number < 0:
            return jsonify({"error": "Число должно быть натуральным."}), 400

        message, success = process_number(number)

        if success:
            return jsonify({"message": message}), 200
        else:
            return jsonify({"error": message}), 400
    except Exception as e:
        logger.error(f"Ошибка при обработке запроса: {e}")
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500


# Инициализация базы данных
init_db()

# Запуск приложения
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
