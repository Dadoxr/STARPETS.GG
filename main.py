import logging
from flask import Flask, request, jsonify
import sqlite3
import requests
import threading
import random
import time
import dotenv
import os

dotenv.load_dotenv()

app = Flask(__name__)
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

class User:
    def __init__(self, user_id: int, username: str, balance: int):
        self.id = user_id
        self.username = username
        self.balance = balance

    @classmethod
    def drop_table(cls):
        """Создает таблицу пользователей в базе данных, если она не существует."""
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute('''DROP TABLE IF EXISTS users''')
            conn.commit()

    @classmethod
    def create_table(cls):
        """Создает таблицу пользователей в базе данных, если она не существует."""
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute('DROP TABLE IF EXISTS users;')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL,
                    balance INTEGER NOT NULL
                )
            ''')
            conn.commit()

    @classmethod
    def add_user(cls, username: str, balance: int):
        """Добавляет нового пользователя в базу данных."""
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO users (username, balance) VALUES (?, ?)', (username, balance))
            conn.commit()

    @classmethod
    def update_balance(cls, user_id: int, amount: float):
        """Обновляет баланс пользователя."""
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE users SET balance = balance + ? WHERE id = ?', (amount, user_id))
            conn.commit()

    @classmethod
    def get_balance(cls, user_id: int):
        """Возвращает текущий баланс пользователя."""
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT balance FROM users WHERE id = ?', (user_id,))
            result = cursor.fetchone()
            return result[0] if result else None
    
    @classmethod
    def get_users(cls):
        """Возвращает текущий баланс пользователя."""
        with sqlite3.connect('users.db') as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users')
            result = cursor.fetchall()
        return result
        

def fetch_weather(city: str) -> float:
    """Получает текущую температуру в указанном городе с использованием API Гисметео."""

    api_key = os.getenv('API_KEY')  
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'
    response = requests.get(url)

    if response.status_code == 200:
        try:
            data = response.json()
            data_list = data.get('list', [])
            if data_list:
                temperature = data_list[0].get('main', {}).get('temp', None)
                if temperature is None:
                    raise ValueError("Температура не найдена")
                return temperature
            else:
                temperature = random.choice([1, 23, 32, 1, 7])
        except (KeyError, ValueError) as e:
            logger.error(f"Ошибка при извлечении температуры для города {city}: {e}")
            temperature = random.choice([1, 23, 32, 1, 7])
    else:
        logger.error(f"Ошибка ответа сервера: {response.status_code}")
        temperature = random.choice([1, 23, 32, 1, 7])

    return temperature


def update_balance_thread(user_id: int, city: str):
    """Обновляет баланс пользователя с использованием температуры в указанном городе."""
    try:
        temperature = fetch_weather(city)
        if not int(User.get_balance(user_id)) - int(temperature) < 0:
            User.update_balance(user_id, temperature)
    except Exception as e:
        logger.error(f"Ошибка при обновлении баланса для пользователя {user_id} в городе {city}: {e}")

@app.route('/update_balance', methods=['POST'])
def update_balance():
    """Обработчик запроса на обновление баланса пользователя."""
    user_id = int(request.json.get('userId'))
    city = request.json.get('city')

    threading.Thread(target=update_balance_thread, args=(user_id, city)).start()

    return jsonify({'status': 'success'})


if __name__ == '__main__':
    User.drop_table()
    User.create_table()

    amount_of_users = 5

    for i in range(1, amount_of_users+1):
        User.add_user(f'user{i}', 5000 + i * 1000)

    for i in range(1200):
        user_id = 1 + i % amount_of_users
        city = random.choice(['Moscow', 'Moscow', 'Moscow', 'Moscow', 'Moscow'])
        threading.Thread(target=update_balance_thread, args=(user_id, city)).start()
        time.sleep(0.1)

    app.run(debug=True)
 