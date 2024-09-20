#!/usr/bin/env python3
import pymysql
import asyncio
from telegram import Bot
from telegram.error import TelegramError
from telegram.ext import Application, CommandHandler
import textwrap
import pandas as pd

# Замените TOKEN на токен вашего бота
TOKEN = ''
# Замените CHAT_ID на ID вашего чата или канала
CHAT_ID = ''

"""
async def send_message_to_telegram(price_comparison_list):
    bot = Bot(token=TOKEN)
    
    # Формирование таблицы в моноширинном шрифте
    header = "Название          | Цена       | Старая     | Разница   \n" + "-" * 45
    table_rows = ""
    
    for comparison in price_comparison_list:
        row = (f"{comparison['title']:<20} | {comparison['latest_price']:<10} | "
               f"{comparison['previous_price']:<10} | {comparison['difference']:<10}\n")
        table_rows += row
    
    message = f"```\n{header}\n{table_rows}```"
    
    MAX_MESSAGE_LENGTH = 4096  # Максимальная длина сообщения для Telegram
    messages = textwrap.wrap(message, MAX_MESSAGE_LENGTH)
    
    # Отправьте каждую часть сообщения
    for msg in messages:
        try:
            await bot.send_message(chat_id=CHAT_ID, text=msg)
            print("Message sent successfully!")
        except TelegramError as e:
            print(f"Error occurred: {e}")
          
"""

EXCEL_FILE = 'price_comparison.xlsx'

def save_to_excel(price_comparison_list):
    # Создание DataFrame из списка словарей
    df = pd.DataFrame(price_comparison_list)
    # заголовки столбцов
    df.columns = ['Товар', 'Новая цена', 'Старая цена', 'Дельта']
    
    # Сохранение DataFrame в Excel файл
    df.to_excel(EXCEL_FILE, index=False)

async def send_document_to_telegram():
    bot = Bot(token=TOKEN)
    # Отправка Excel файла
    try:
        with open(EXCEL_FILE, 'rb') as file:
            await bot.send_document(chat_id=CHAT_ID, document=file, caption="Отчет по сравнению цен")
            print("Документ отправлен")
    except TelegramError as e:
        print(f"Error occurred: {e}")

async def main():
    # Установка соединения с базой данных
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='airflow',
        db='airflow',
        port=33061
    )

    # Создание объекта курсора для выполнения запросов
    cursor = conn.cursor()

    # Получение последней даты из таблицы
    cursor.execute("SELECT MAX(data) FROM price")
    latest_date = cursor.fetchone()[0]

    # Получение предыдущей даты из таблицы
    cursor.execute("SELECT MAX(data) FROM price WHERE data < %s", (latest_date,))
    previous_date = cursor.fetchone()[0]

    # Получение цен для последней и предыдущей дат
    cursor.execute("""
        SELECT title, price, data
        FROM price
        WHERE data IN (%s, %s)
    """, (latest_date, previous_date))

    prices = cursor.fetchall()

    # Создание словаря для хранения сравнений цен
    price_comparisons = {}

    # Итерация по ценам и сравнение последней и предыдущей цен
    for title, price, data in prices:
        if data == latest_date:
            latest_price = price
        elif data == previous_date:
            previous_price = price
        if title not in price_comparisons:
            price_comparisons[title] = {}
        price_comparisons[title][data] = price

    # Вывод сравнений цен
    price_comparison_list = []
    for title, prices in price_comparisons.items():
        latest_price = prices.get(latest_date)
        previous_price = prices.get(previous_date)
        if latest_price is not None and previous_price is not None and latest_price != previous_price:
            price_comparison = {
                'title': title,
                'latest_price': latest_price,
                'previous_price': previous_price,
                'difference': latest_price - previous_price
            }
            price_comparison_list.append(price_comparison)

    # Отправка сообщения в Telegram
    save_to_excel(price_comparison_list)
    await send_document_to_telegram()
    #await send_message_to_telegram(price_comparison_list)
    # Закрытие курсора и соединения
    cursor.close()
    conn.close()

# Запуск основной асинхронной функции

asyncio.run(main())
